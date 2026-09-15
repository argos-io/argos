package ir

import (
	"testing"

	"github.com/argos-io/argos/descriptor"
)

func TestValidateNamesRejectsMethodSymbolCollisionWithMessage(t *testing.T) {
	file := File{
		GoPackage: "service",
		InputBase: "service",
		Messages:  []Message{{GoName: "EchoService_Echo"}},
		Services: []Service{{
			GoName: "EchoService",
			Methods: []Method{{
				GoName: "Echo", InputType: "Request", OutputType: "Response",
			}},
		}},
	}
	if err := ValidateNames(&file); err == nil {
		t.Fatal("ValidateNames succeeded for method descriptor colliding with message")
	}
}

func TestValidateNamesRejectsServiceDescCollisionWithMessage(t *testing.T) {
	file := File{
		GoPackage: "service",
		Messages:  []Message{{GoName: "FooServiceDesc"}},
		Services: []Service{{
			GoName: "FooService",
			Methods: []Method{{
				GoName: "Bar", InputType: "Request", OutputType: "Response",
			}},
		}},
	}
	if err := ValidateNames(&file); err == nil {
		t.Fatal("ValidateNames succeeded for service descriptor colliding with message")
	}
}

func TestValidateNamesAllowsSameMethodNameAcrossServices(t *testing.T) {
	file := File{
		GoPackage: "service",
		InputBase: "service",
		Services: []Service{
			{GoName: "AlphaService", Methods: []Method{{
				GoName: "Echo", InputType: "Request", OutputType: "Response",
			}}},
			{GoName: "BetaService", Methods: []Method{{
				GoName: "Echo", InputType: "Request", OutputType: "Response",
			}}},
		},
	}
	if err := ValidateNames(&file); err != nil {
		t.Fatalf("ValidateNames: %v", err)
	}
}

func TestValidateNamesRejectsInvalidIdentifier(t *testing.T) {
	file := File{
		GoPackage: "service",
		Services: []Service{{
			GoName:  "EchoService",
			Methods: []Method{{GoName: "Call", InputType: "Request.Type", OutputType: "Response"}},
		}},
	}
	if err := ValidateNames(&file); err == nil {
		t.Fatal("ValidateNames succeeded for qualified Go type")
	}
}

func TestNormalizeDescriptorSetDefaultsToProtoMessages(t *testing.T) {
	file := File{
		GoPackage:         "service",
		InputBase:         "echo",
		FileDescriptorSet: []byte{1},
	}
	file.Normalize(false)
	if got, want := file.Source, SourceProto; got != want {
		t.Fatalf("Source = %q, want %q", got, want)
	}
	if got, want := file.MessagesName(), "echo.pb.go"; got != want {
		t.Fatalf("MessagesName = %q, want %q", got, want)
	}
}

func TestNormalizeFillsFullNameAndShape(t *testing.T) {
	file := File{
		GoPackage:    "echov1",
		ProtoPackage: "echo.v1",
		Services: []Service{{
			GoName: "EchoService",
			Methods: []Method{
				{GoName: "Echo", InputType: "EchoRequest", OutputType: "EchoResponse"},
				{GoName: "Watch", InputType: "WatchRequest", OutputType: "Event", ServerStream: true},
				{GoName: "Legacy", FullName: "echo.v1.EchoService/Legacy", InputType: "EchoRequest", OutputType: "EchoResponse"},
			},
		}},
	}
	file.Normalize(false)
	svc := file.Services[0]
	if got, want := svc.FullName, "echo.v1.EchoService"; got != want {
		t.Fatalf("Service.FullName = %q, want %q", got, want)
	}
	if got, want := svc.Methods[0].FullName, "echo.v1.EchoService.Echo"; got != want {
		t.Fatalf("Echo.FullName = %q, want %q", got, want)
	}
	if got, want := svc.Methods[0].Shape, Unary; got != want {
		t.Fatalf("Echo.Shape = %v, want %v", got, want)
	}
	if got, want := svc.Methods[1].Shape, ServerStreaming; got != want {
		t.Fatalf("Watch.Shape = %v, want %v", got, want)
	}
	if !svc.Methods[1].ServerStream || svc.Methods[1].ClientStream {
		t.Fatalf("Watch stream flags = client=%v server=%v", svc.Methods[1].ClientStream, svc.Methods[1].ServerStream)
	}
	if got, want := svc.Methods[2].FullName, "echo.v1.EchoService.Legacy"; got != want {
		t.Fatalf("Legacy.FullName = %q, want %q (slash migrated)", got, want)
	}
}

func TestShapeAlignedWithDescriptor(t *testing.T) {
	cases := []struct {
		ir Shape
		d  descriptor.Shape
	}{
		{Unary, descriptor.Unary},
		{ServerStreaming, descriptor.ServerStreaming},
		{ClientStreaming, descriptor.ClientStreaming},
		{BidiStreaming, descriptor.BidiStreaming},
	}
	for _, tc := range cases {
		if tc.ir != Shape(tc.d) || uint8(tc.ir) != uint8(tc.d) {
			t.Errorf("ir.Shape %v not aligned with descriptor.Shape %v", tc.ir, tc.d)
		}
	}
}

func TestValidateNamesRejectsGRPCPathFullName(t *testing.T) {
	file := File{
		GoPackage: "service",
		Services: []Service{{
			GoName:   "EchoService",
			FullName: "echo.v1.EchoService",
			Methods: []Method{{
				GoName: "Echo", FullName: "echo.v1.EchoService/Echo",
				InputType: "Request", OutputType: "Response",
			}},
		}},
	}
	if err := ValidateNames(&file); err == nil {
		t.Fatal("ValidateNames succeeded for gRPC path FullName")
	}
}
