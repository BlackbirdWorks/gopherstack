// Package httptarget holds the pieces shared by services that deliver events to HTTP-style
// targets (API Gateway REST APIs and EventBridge API destinations).
package httptarget

import (
	"net/http"
	"net/url"
	"strings"
)

const (
	arnFields     = 6
	routeParts    = 3
	wildcard      = "*"
	executeAPISvc = "execute-api"
)

// Route is a parsed execute-api target ARN, arn:aws:execute-api:{region}:{account}:{apiId}/{stage}/{METHOD}/{path}.
type Route struct {
	APIID  string
	Stage  string
	Method string
	Path   string
	Region string
}

// ParseExecuteAPIARN splits an API Gateway target ARN. A "*" method means any and is sent as POST.
func ParseExecuteAPIARN(arn string) (Route, bool) {
	parts := strings.SplitN(arn, ":", arnFields)
	if len(parts) < arnFields || parts[0] != "arn" || parts[2] != executeAPISvc {
		return Route{}, false
	}

	segs := strings.SplitN(parts[5], "/", routeParts+1)
	if len(segs) < routeParts || segs[0] == "" || segs[1] == "" || segs[2] == "" {
		return Route{}, false
	}

	r := Route{APIID: segs[0], Stage: segs[1], Method: strings.ToUpper(segs[2]), Region: parts[3], Path: "/"}
	if len(segs) == routeParts+1 {
		r.Path = "/" + segs[3]
	}

	if r.Method == wildcard {
		r.Method = http.MethodPost
	}

	return r, true
}

// FillWildcards replaces each "*" in s with the next value, path-escaped; surplus wildcards stay as-is.
func FillWildcards(s string, values []string) string {
	if len(values) == 0 || !strings.Contains(s, wildcard) {
		return s
	}

	var b strings.Builder

	i := 0

	for _, r := range s {
		if r == '*' && i < len(values) {
			b.WriteString(url.PathEscape(values[i]))

			i++

			continue
		}

		b.WriteRune(r)
	}

	return b.String()
}
