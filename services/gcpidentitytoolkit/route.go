package gcpidentitytoolkit

import (
	"slices"
	"net/http"
	"net/url"
	"strings"
)

var hostPrefixes = []string{"/identitytoolkit.googleapis.com", "/securetoken.googleapis.com"} //nolint:gochecknoglobals // immutable

type route struct {
	op      string
	project string
	tenant  string
	id      string
	version string
}

func splitPath(r *http.Request) []string {
	p := r.URL.EscapedPath()

	for _, pre := range hostPrefixes {
		p = strings.TrimPrefix(p, pre)
	}

	parts := strings.Split(strings.Trim(p, "/"), "/")
	for i, s := range parts {
		u, err := url.PathUnescape(s)
		if err != nil {
			return nil
		}

		parts[i] = u
	}

	return parts
}

func parseRoute(r *http.Request) route {
	parts := splitPath(r)
	if len(parts) == 0 {
		return route{}
	}

	switch parts[0] {
	case "emulator":
		return emulatorRoute(r, parts)
	case "robot", "service_accounts", "identitytoolkit":
		return route{op: "PublicKeys"}
	case "v1", "v2":
		rt := route{version: parts[0]}

		return versionedRoute(r, rt, parts[1:])
	}

	return route{}
}

func emulatorRoute(r *http.Request, parts []string) route {
	if len(parts) == 5 && parts[1] == "v1" && parts[2] == "projects" {
		rt := route{project: parts[3], tenant: r.URL.Query().Get("tenantId")}

		switch {
		case parts[4] == "oobCodes" && r.Method == http.MethodGet:
			rt.op = "ListOobCodes"
		case parts[4] == "accounts" && r.Method == http.MethodDelete:
			rt.op = "ClearAccounts"
		}

		return rt
	}

	return route{}
}

func versionedRoute(r *http.Request, rt route, rest []string) route {
	if len(rest) == 0 {
		return route{}
	}

	if rt.version == "v1" && rest[0] == "token" && len(rest) == 1 {
		rt.op = "Token"

		return rt
	}

	if rest[0] != "projects" {
		if rt.version == "v1" && len(rest) == 1 {
			rt.op = accountVerb(r, rest[0])
		}

		return rt
	}

	if len(rest) < 2 {
		return route{}
	}

	proj := rest[1]
	rt.project = proj
	tail := rest[2:]

	if before, verb, ok := strings.Cut(proj, ":"); ok {
		rt.project = before

		if r.Method == http.MethodPost && rt.version == "v1" {
			switch verb {
			case "createSessionCookie":
				rt.op = "CreateSessionCookie"
			case "queryAccounts":
				rt.op = "query"
			}
		}

		return rt
	}

	if rt.version == "v2" {
		return v2Route(r, rt, tail)
	}

	if len(tail) >= 2 && tail[0] == "tenants" {
		rt.tenant = tail[1]
		tail = tail[2:]

		if before, verb, ok := strings.Cut(rt.tenant, ":"); ok {
			rt.tenant = before

			if verb == "createSessionCookie" && r.Method == http.MethodPost {
				rt.op = "CreateSessionCookie"
			}

			return rt
		}
	}

	if len(tail) == 1 {
		rt.op = projectVerb(r, tail[0])
	}

	return rt
}

func projectVerb(r *http.Request, seg string) string {
	switch {
	case seg == "accounts" && r.Method == http.MethodPost:
		return "AdminCreate"
	case strings.HasPrefix(seg, "accounts:"):
		return accountVerb(r, seg)
	}

	return ""
}

func accountVerb(r *http.Request, seg string) string {
	verb, ok := strings.CutPrefix(seg, "accounts:")
	if !ok {
		return ""
	}

	if verb == "batchGet" {
		if r.Method == http.MethodGet {
			return "BatchGet"
		}

		return ""
	}

	if r.Method != http.MethodPost {
		return ""
	}

	if slices.Contains(accountVerbs, verb) {
		return verb
	}

	return ""
}

//nolint:gochecknoglobals // immutable lookup table
var accountVerbs = []string{
	"signUp", "signInWithPassword", "signInWithCustomToken", "signInWithEmailLink", "lookup", "update", "delete",
	"sendOobCode", "resetPassword", "createAuthUri", "batchCreate", "batchDelete", "query",
}

func v2Route(r *http.Request, rt route, tail []string) route {
	switch {
	case len(tail) == 1 && tail[0] == "config":
		switch r.Method {
		case http.MethodGet:
			rt.op = "GetConfig"
		case http.MethodPatch:
			rt.op = "UpdateConfig"
		}
	case len(tail) == 1 && tail[0] == "tenants":
		switch r.Method {
		case http.MethodGet:
			rt.op = "ListTenants"
		case http.MethodPost:
			rt.op = "CreateTenant"
		}
	case len(tail) == 2 && tail[0] == "tenants":
		rt.id = tail[1]

		switch r.Method {
		case http.MethodGet:
			rt.op = "GetTenant"
		case http.MethodPatch:
			rt.op = "UpdateTenant"
		case http.MethodDelete:
			rt.op = "DeleteTenant"
		}
	}

	return rt
}
