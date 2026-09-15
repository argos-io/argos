package argos

// ServiceConfig holds per-service overrides stored by WithService (§6).
type ServiceConfig struct {
	Binding BindingFunc
	Target  string
}

// ServiceOption configures one ServiceConfig entry during WithService.
type ServiceOption interface {
	applyService(*ServiceConfig)
}

type serviceOptionFunc func(*ServiceConfig)

func (f serviceOptionFunc) applyService(sc *ServiceConfig) { f(sc) }

// ServiceBinding sets the per-service BindingFunc factory.
func ServiceBinding(fn BindingFunc) ServiceOption {
	return serviceOptionFunc(func(sc *ServiceConfig) { sc.Binding = fn })
}

// ServiceTarget sets the per-service address target (e.g. ip://host:port).
func ServiceTarget(target string) ServiceOption {
	return serviceOptionFunc(func(sc *ServiceConfig) { sc.Target = target })
}
