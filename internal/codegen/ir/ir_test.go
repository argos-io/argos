package ir

import "testing"

func TestValidateNamesRejectsGeneratedCollisions(t *testing.T) {
	file := File{
		GoPackage: "example.com/service;service",
		InputBase: "service",
		Services: []Service{
			{GoName: "AlphaService", Methods: []Method{{
				GoName: "Call", InputType: "Request", OutputType: "Response",
			}}},
			{GoName: "Alpha", Methods: []Method{{
				GoName: "Other", InputType: "Request", OutputType: "Response",
			}}},
		},
	}
	if err := ValidateNames(&file); err == nil {
		t.Fatal("ValidateNames succeeded for colliding service prefixes")
	}
}

func TestValidateNamesRejectsConcatenatedGeneratedCollision(t *testing.T) {
	file := File{
		GoPackage: "service",
		Services: []Service{
			{GoName: "FooService", Methods: []Method{{
				GoName: "Bar", InputType: "Request", OutputType: "Response",
			}}},
			{GoName: "FooBService", Methods: []Method{{
				GoName: "ar", InputType: "Request", OutputType: "Response",
			}}},
		},
	}
	if err := ValidateNames(&file); err == nil {
		t.Fatal("ValidateNames succeeded for colliding generated method constants")
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
