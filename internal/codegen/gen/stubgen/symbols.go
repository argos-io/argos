package stubgen

import "github.com/argos-io/argos/internal/codegen/ir"

// Symbols returns package-level identifiers emitted into *.argos.go for file.
func Symbols(file ir.File) []string {
	var out []string
	seen := make(map[string]struct{})
	add := func(name string) {
		if name == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}

	for _, service := range file.Services {
		add(service.GoName)
		add(service.GoName + "Desc")
		add("Register" + service.GoName)
		add(service.GoName + "Client")
		add("New" + service.GoName + "Client")
		add(lowerFirst(service.GoName) + "Client")
		for _, method := range service.Methods {
			add(service.GoName + "_" + method.GoName)
			if !method.ClientStream && !method.ServerStream {
				continue
			}
			add(service.GoName + "_" + method.GoName + "Server")
			add(lowerFirst(service.GoName) + method.GoName + "Server")
			add(service.GoName + "_" + method.GoName + "Client")
			add(lowerFirst(service.GoName) + method.GoName + "Client")
		}
	}
	return out
}
