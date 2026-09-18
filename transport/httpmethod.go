package transport

import "strings"

// PrefaceHTTPMethod returns the HTTP method for OpenStream. Empty means POST.
func PrefaceHTTPMethod(p RequestPreface) string {
	if p.Method == "" {
		return "POST"
	}
	return strings.ToUpper(p.Method)
}
