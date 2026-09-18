// Package teststack registers named transport and codec factories for tests.
package teststack

import (
	"fmt"
	"strings"
	"testing"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/transport"
)

func sanitizeTestName(name string) string {
	r := strings.NewReplacer("/", "_", " ", "_", "(", "", ")", "", ",", "_")
	return r.Replace(name)
}

// TransportName registers tr under a name derived from t and returns that name.
func TransportName(t *testing.T, tr transport.Transport) string {
	name := fmt.Sprintf("test-transport-%s-%p", sanitizeTestName(t.Name()), tr)
	transport.Register(name, func() (transport.Transport, error) { return tr, nil })
	return name
}

// CodecName registers cd under a name derived from t and returns that name.
func CodecName(t *testing.T, cd codec.Codec) string {
	name := fmt.Sprintf("test-codec-%s-%p", sanitizeTestName(t.Name()), cd)
	codec.Register(name, func() (codec.Codec, error) { return cd, nil })
	return name
}
