package gcpdns

import (
	"net/http"
	"net/url"
	"strings"
)

const apiPrefix = "/dns/v1/"

type route struct {
	op      string
	project string
	zone    string
	id      string
	rrName  string
	rrType  string
}

func parseRoute(r *http.Request) route {
	raw := strings.TrimPrefix(r.URL.EscapedPath(), apiPrefix)
	if raw == r.URL.EscapedPath() {
		return route{}
	}

	parts := strings.Split(strings.Trim(raw, "/"), "/")
	for i, p := range parts {
		u, err := url.PathUnescape(p)
		if err != nil {
			return route{}
		}

		parts[i] = u
	}

	if len(parts) < 2 || parts[0] != "projects" || parts[1] == "" {
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
	case 2:
		rt.op = collectionOp(method, "ResponsePolicyRules")
	case 3:
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
	case 2:
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
