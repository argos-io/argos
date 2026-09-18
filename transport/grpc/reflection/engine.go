/*
 *
 * Copyright 2024 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Engine implements gRPC server reflection request handling (logic aligned with
// google.golang.org/grpc/reflection/internal).
package reflection

import (
	"fmt"
	"sort"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	v1reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
)

// ExtensionResolver is the interface used to query details about extensions.
// This interface is satisfied by protoregistry.GlobalTypes.
type ExtensionResolver interface {
	protoregistry.ExtensionTypeResolver
	RangeExtensionsByMessage(message protoreflect.FullName, f func(protoreflect.ExtensionType) bool)
}

// Engine serves reflection requests against registered service names and file
// descriptors.
type Engine struct {
	Services     []string
	DescResolver protodesc.Resolver
	ExtResolver  ExtensionResolver
}

// FileDescWithDependencies returns a slice of serialized fileDescriptors in
// wire format ([]byte). The fileDescriptors will include fd and all the
// transitive dependencies of fd with names not in sentFileDescriptors.
func (s *Engine) FileDescWithDependencies(fd protoreflect.FileDescriptor, sentFileDescriptors map[string]bool) ([][]byte, error) {
	if fd.IsPlaceholder() {
		// If the given root file is a placeholder, treat it
		// as missing instead of serializing it.
		return nil, protoregistry.NotFound
	}
	var r [][]byte
	queue := []protoreflect.FileDescriptor{fd}
	for len(queue) > 0 {
		currentfd := queue[0]
		queue = queue[1:]
		if currentfd.IsPlaceholder() {
			// Skip any missing files in the dependency graph.
			continue
		}
		if sent := sentFileDescriptors[currentfd.Path()]; len(r) == 0 || !sent {
			sentFileDescriptors[currentfd.Path()] = true
			fdProto := protodesc.ToFileDescriptorProto(currentfd)
			currentfdEncoded, err := proto.Marshal(fdProto)
			if err != nil {
				return nil, err
			}
			r = append(r, currentfdEncoded)
		}
		for i := 0; i < currentfd.Imports().Len(); i++ {
			queue = append(queue, currentfd.Imports().Get(i))
		}
	}
	return r, nil
}

// FileDescEncodingContainingSymbol finds the file descriptor containing the
// given symbol, finds all of its previously unsent transitive dependencies,
// does marshalling on them, and returns the marshalled result. The given symbol
// can be a type, a service or a method.
func (s *Engine) FileDescEncodingContainingSymbol(name string, sentFileDescriptors map[string]bool) ([][]byte, error) {
	d, err := s.DescResolver.FindDescriptorByName(protoreflect.FullName(name))
	if err != nil {
		return nil, err
	}
	return s.FileDescWithDependencies(d.ParentFile(), sentFileDescriptors)
}

// FileDescEncodingContainingExtension finds the file descriptor containing
// given extension, finds all of its previously unsent transitive dependencies,
// does marshalling on them, and returns the marshalled result.
func (s *Engine) FileDescEncodingContainingExtension(typeName string, extNum int32, sentFileDescriptors map[string]bool) ([][]byte, error) {
	xt, err := s.ExtResolver.FindExtensionByNumber(protoreflect.FullName(typeName), protoreflect.FieldNumber(extNum))
	if err != nil {
		return nil, err
	}
	return s.FileDescWithDependencies(xt.TypeDescriptor().ParentFile(), sentFileDescriptors)
}

// AllExtensionNumbersForTypeName returns all extension numbers for the given type.
func (s *Engine) AllExtensionNumbersForTypeName(name string) ([]int32, error) {
	var numbers []int32
	s.ExtResolver.RangeExtensionsByMessage(protoreflect.FullName(name), func(xt protoreflect.ExtensionType) bool {
		numbers = append(numbers, int32(xt.TypeDescriptor().Number()))
		return true
	})
	sort.Slice(numbers, func(i, j int) bool {
		return numbers[i] < numbers[j]
	})
	if len(numbers) == 0 {
		// maybe return an error if given type name is not known
		if _, err := s.DescResolver.FindDescriptorByName(protoreflect.FullName(name)); err != nil {
			return nil, err
		}
	}
	return numbers, nil
}

// ListServices returns the names of services this server exposes.
func (s *Engine) ListServices() []*v1reflectionpb.ServiceResponse {
	resp := make([]*v1reflectionpb.ServiceResponse, 0, len(s.Services))
	for _, svc := range s.Services {
		resp = append(resp, &v1reflectionpb.ServiceResponse{Name: svc})
	}
	sort.Slice(resp, func(i, j int) bool {
		return resp[i].Name < resp[j].Name
	})
	return resp
}

// ProcessV1 handles one v1 reflection request.
func (s *Engine) ProcessV1(in *v1reflectionpb.ServerReflectionRequest, sentFileDescriptors map[string]bool) (*v1reflectionpb.ServerReflectionResponse, error) {
	if in == nil {
		return nil, protoregistry.NotFound
	}
	out := &v1reflectionpb.ServerReflectionResponse{
		ValidHost:       in.Host,
		OriginalRequest: in,
	}
	switch req := in.MessageRequest.(type) {
	case *v1reflectionpb.ServerReflectionRequest_FileByFilename:
		var b [][]byte
		fd, err := s.DescResolver.FindFileByPath(req.FileByFilename)
		if err == nil {
			b, err = s.FileDescWithDependencies(fd, sentFileDescriptors)
		}
		if err != nil {
			out.MessageResponse = &v1reflectionpb.ServerReflectionResponse_ErrorResponse{
				ErrorResponse: &v1reflectionpb.ErrorResponse{
					ErrorCode:    int32(codes.NotFound),
					ErrorMessage: err.Error(),
				},
			}
		} else {
			out.MessageResponse = &v1reflectionpb.ServerReflectionResponse_FileDescriptorResponse{
				FileDescriptorResponse: &v1reflectionpb.FileDescriptorResponse{FileDescriptorProto: b},
			}
		}
	case *v1reflectionpb.ServerReflectionRequest_FileContainingSymbol:
		b, err := s.FileDescEncodingContainingSymbol(req.FileContainingSymbol, sentFileDescriptors)
		if err != nil {
			out.MessageResponse = &v1reflectionpb.ServerReflectionResponse_ErrorResponse{
				ErrorResponse: &v1reflectionpb.ErrorResponse{
					ErrorCode:    int32(codes.NotFound),
					ErrorMessage: err.Error(),
				},
			}
		} else {
			out.MessageResponse = &v1reflectionpb.ServerReflectionResponse_FileDescriptorResponse{
				FileDescriptorResponse: &v1reflectionpb.FileDescriptorResponse{FileDescriptorProto: b},
			}
		}
	case *v1reflectionpb.ServerReflectionRequest_FileContainingExtension:
		typeName := req.FileContainingExtension.ContainingType
		extNum := req.FileContainingExtension.ExtensionNumber
		b, err := s.FileDescEncodingContainingExtension(typeName, extNum, sentFileDescriptors)
		if err != nil {
			out.MessageResponse = &v1reflectionpb.ServerReflectionResponse_ErrorResponse{
				ErrorResponse: &v1reflectionpb.ErrorResponse{
					ErrorCode:    int32(codes.NotFound),
					ErrorMessage: err.Error(),
				},
			}
		} else {
			out.MessageResponse = &v1reflectionpb.ServerReflectionResponse_FileDescriptorResponse{
				FileDescriptorResponse: &v1reflectionpb.FileDescriptorResponse{FileDescriptorProto: b},
			}
		}
	case *v1reflectionpb.ServerReflectionRequest_AllExtensionNumbersOfType:
		extNums, err := s.AllExtensionNumbersForTypeName(req.AllExtensionNumbersOfType)
		if err != nil {
			out.MessageResponse = &v1reflectionpb.ServerReflectionResponse_ErrorResponse{
				ErrorResponse: &v1reflectionpb.ErrorResponse{
					ErrorCode:    int32(codes.NotFound),
					ErrorMessage: err.Error(),
				},
			}
		} else {
			out.MessageResponse = &v1reflectionpb.ServerReflectionResponse_AllExtensionNumbersResponse{
				AllExtensionNumbersResponse: &v1reflectionpb.ExtensionNumberResponse{
					BaseTypeName:    req.AllExtensionNumbersOfType,
					ExtensionNumber: extNums,
				},
			}
		}
	case *v1reflectionpb.ServerReflectionRequest_ListServices:
		out.MessageResponse = &v1reflectionpb.ServerReflectionResponse_ListServicesResponse{
			ListServicesResponse: &v1reflectionpb.ListServiceResponse{
				Service: s.ListServices(),
			},
		}
	default:
		return nil, invalidRequest(in.MessageRequest)
	}
	return out, nil
}

func invalidRequest(req any) error {
	return fmt.Errorf("invalid MessageRequest: %v", req)
}
