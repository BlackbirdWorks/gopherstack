package lambda

import (
	"net"
	"strings"
)

const (
	functionURLIDLen      = 32
	functionURLHostInfix  = ".lambda-url."
	functionURLHostSuffix = ".on.aws"
	functionURLLocalZone  = ".localhost"
)

// parseFunctionURLHost extracts the url-id and region from a function URL host:
// <id>.lambda-url.<region>.on.aws or <id>.lambda-url.<region>.localhost (port optional).
func parseFunctionURLHost(host string) (string, string, bool) {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	host = strings.ToLower(strings.TrimSuffix(host, "."))

	rest, ok := strings.CutSuffix(host, functionURLHostSuffix)
	if !ok {
		rest, ok = strings.CutSuffix(host, functionURLLocalZone)
	}

	if !ok {
		return "", "", false
	}

	id, region, found := strings.Cut(rest, functionURLHostInfix)
	if !found || id == "" || region == "" || strings.Contains(id, ".") || strings.Contains(region, ".") {
		return "", "", false
	}

	return id, region, true
}

// IsFunctionURLHost reports whether host names a Lambda function URL.
func IsFunctionURLHost(host string) bool {
	_, _, ok := parseFunctionURLHost(host)

	return ok
}
