package httpunary

import "github.com/argos-io/argos/descriptor"

// Binding maps one RPC method to an HTTP verb and path template.
// Template segments use {name} for path variables; client OpenCall supplies
// values via outgoing metadata keys PathVarMetadataPrefix + name.
type Binding struct {
	Method  descriptor.Method
	Verb    string
	Pattern string
}

// RESTConfig configures REST-style routing (TransportOption WithREST).
type RESTConfig struct {
	Bindings []Binding
	// ContentType overrides default ContentType for success responses; nil uses ContentType.
	ContentType func(codecName string) string
	// EncodeError overrides the default JSON status envelope for error Finish; nil uses EncodeErrorBody.
	EncodeError func(err error) (contentType string, body []byte)
}
