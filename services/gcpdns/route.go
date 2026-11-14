package gcpdns

import (
	"net/http"
	"net/url"
	"strings"
)

const (
	apiPrefix      = "/dns/v1/"
	apiPrefixBeta2 = "/dns/v1beta2/"
	apiPrefixV2    = "/dns/v2/"
	globalLocation = "global"
)

const (
	ruleCollectionLen = 2
	ruleItemLen       = 3
	rrsetItemLen      = 2
)

type route struct {
	op      string
	project string
	zone    string
	id      string
	rrName  string
	rrType  string
}

// splitPath returns the unescaped path segments below the API prefix, with the v2 locations segment removed.
func splitPath(path string) ([]string, bool) {
	var raw string

	v2 := false

	switch {
	case strings.HasPrefix(path, apiPrefix):
		raw = strings.TrimPrefix(path, apiPrefix)
	case strings.HasPrefix(path, apiPrefixBeta2):
		raw = strings.TrimPrefix(path, apiPrefixBeta2)
	case strings.HasPrefix(path, apiPrefixV2):
		raw, v2 = strings.TrimPrefix(path, apiPrefixV2), true
	default:
		return nil, false
	}

	parts := strings.Split(strings.Trim(raw, "/"), "/")
	for i, p := range parts {
		u, err := url.PathUnescape(p)
		if err != nil {
			return nil, false
		}

		parts[i] = u
	}

	if len(parts) < 2 || parts[0] != "projects" || parts[1] == "" {
		return nil, false
	}

	if v2 {
		if len(parts) < 4 || parts[2] != "locations" || parts[3] != globalLocation {
			return nil, false
		}

		parts = append(parts[:2:2], parts[4:]...)
	}

	return parts, true
}

func parseRoute(r *http.Request) route {
	parts, ok := splitPath(r.URL.EscapedPath())
	if !ok {
		return route{}
	}

	rt := route{project: parts[1]}
	rest := parts[2:]

	switch {
	case len(rest) == 0:
		rt.op = methodOp(r.Method, "Projects", "Get")
	case rest[0] == "managedZones":
		rt = zoneRoute(r.Method, rt, rest[1:])
	case rest[0] == "policies":
		rt = simpleRoute(r.Method, rt, "Policies", rest[1:])
	case rest[0] == "responsePolicies":
		rt = responseRoute(r.Method, rt, rest[1:])
	}

	return rt
}

func simpleRoute(method string, rt route, family string, rest []string) route {
	switch len(rest) {
	case 0:
		rt.op = collectionOp(method, family)
	case 1:
		rt.id = rest[0]
		rt.op = itemOp(method, family)
	}

	return rt
}

func responseRoute(method string, rt route, rest []string) route {
	if len(rest) < 2 || rest[1] != "rules" {
		return simpleRoute(method, rt, "ResponsePolicies", rest)
	}

	rt.zone = rest[0]

	switch len(rest) {
	case ruleCollectionLen:
		rt.op = collectionOp(method, "ResponsePolicyRules")
	case ruleItemLen:
		rt.id = rest[2]
		rt.op = itemOp(method, "ResponsePolicyRules")
	}

	return rt
}

func zoneRoute(method string, rt route, rest []string) route {
	if len(rest) == 0 {
		rt.op = collectionOp(method, "ManagedZones")

		return rt
	}

	rt.zone = rest[0]
	sub := rest[1:]

	if name, verb, ok := strings.Cut(rt.zone, ":"); ok && len(sub) == 0 && method == http.MethodPost {
		rt.zone = name
		rt.op = iamOp(verb)

		return rt
	}

	if len(sub) == 0 {
		rt.op = itemOp(method, "ManagedZones")

		return rt
	}

	switch sub[0] {
	case "changes":
		return childRoute(method, rt, "Changes", sub[1:])
	case "dnsKeys":
		return childRoute(method, rt, "DnsKeys", sub[1:])
	case "operations":
		return childRoute(method, rt, "ManagedZoneOperations", sub[1:])
	case "rrsets":
		return rrsetRoute(method, rt, sub[1:])
	}

	return rt
}

func childRoute(method string, rt route, family string, rest []string) route {
	switch len(rest) {
	case 0:
		rt.op = collectionOp(method, family)
	case 1:
		rt.id = rest[0]
		rt.op = itemOp(method, family)
	}

	return rt
}

func rrsetRoute(method string, rt route, rest []string) route {
	switch len(rest) {
	case 0:
		rt.op = collectionOp(method, "ResourceRecordSets")
	case rrsetItemLen:
		rt.rrName, rt.rrType = rest[0], rest[1]
		rt.op = itemOp(method, "ResourceRecordSets")
	}

	return rt
}

func methodOp(method, family, verb string) string {
	if method == http.MethodGet {
		return family + "." + verb
	}

	return ""
}

func collectionOp(method, family string) string {
	switch method {
	case http.MethodGet:
		return family + ".List"
	case http.MethodPost:
		return family + ".Create"
	}

	return ""
}

func itemOp(method, family string) string {
	switch method {
	case http.MethodGet:
		return family + ".Get"
	case http.MethodDelete:
		return family + ".Delete"
	case http.MethodPatch:
		return family + ".Patch"
	case http.MethodPut:
		if family == "ResourceRecordSets" || family == "Changes" || family == "DnsKeys" ||
			family == "ManagedZoneOperations" {
			return ""
		}

		return family + ".Update"
	}

	return ""
}

func iamOp(verb string) string {
	switch verb {
	case "getIamPolicy":
		return "ManagedZones.GetIamPolicy"
	case "setIamPolicy":
		return "ManagedZones.SetIamPolicy"
	case "testIamPermissions":
		return "ManagedZones.TestIamPermissions"
	}

	return ""
}
