package argos

import "fmt"

// ResolveService fills zero factories from the registries on c. An axis with
// both a factory and a name uses the factory.
func (c *Config) ResolveService(sc ServiceConfig) (ServiceConfig, error) {
	out := sc
	if out.Transport == nil {
		if sc.TransportName == "" {
			return ServiceConfig{}, fmt.Errorf("argos: transport not set and no transport name")
		}
		if c.transportReg == nil {
			return ServiceConfig{}, fmt.Errorf("argos: transport %q: no transport registry on Config", sc.TransportName)
		}
		fn, err := c.transportReg.Lookup(sc.TransportName)
		if err != nil {
			return ServiceConfig{}, err
		}
		out.Transport = TransportFunc(fn)
	}
	if out.Framing == nil {
		if sc.FramingName == "" {
			return ServiceConfig{}, fmt.Errorf("argos: framing not set and no framing name")
		}
		if c.framingReg == nil {
			return ServiceConfig{}, fmt.Errorf("argos: framing %q: no framing registry on Config", sc.FramingName)
		}
		fn, err := c.framingReg.Lookup(sc.FramingName)
		if err != nil {
			return ServiceConfig{}, err
		}
		out.Framing = FramingFunc(fn)
	}
	if out.Codec == nil {
		if sc.CodecName == "" {
			return ServiceConfig{}, fmt.Errorf("argos: codec not set and no codec name")
		}
		if c.codecReg == nil {
			return ServiceConfig{}, fmt.Errorf("argos: codec %q: no codec registry on Config", sc.CodecName)
		}
		fn, err := c.codecReg.Lookup(sc.CodecName)
		if err != nil {
			return ServiceConfig{}, err
		}
		out.Codec = CodecFunc(fn)
	}
	return out, nil
}
