package gcpidentitytoolkit

import (
	"net/http"
	"net/url"
	"slices"
	"strings"
)

//nolint:gochecknoglobals // immutable lookup tables
var (
	hostPrefixes = []string{"/identitytoolkit.googleapis.com", "/securetoken.googleapis.com"}
	accountVerbs = []string{
		"signUp", "signInWithPassword", "signInWithCustomToken", "signInWithEmailLink", "lookup", "update", "delete",
		"sendOobCode", opResetPassword, "createAuthUri", opBatchCreate, opBatchDelete, opQuery,
	}
)

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
		return route{op: opPublicKeys}
	case "v1", "v2":
		return versionedRoute(r, route{version: parts[0]}, parts[1:])
	}

	return route{}
}

func emulatorRoute(r *http.Request, parts []string) route {
	const emulatorPathLen = 5

	if len(parts) != emulatorPathLen || parts[1] != "v1" || parts[2] != "projects" {
		return route{}
	}

	rt := route{project: parts[3], tenant: r.URL.Query().Get("tenantId")}

	switch {
	case parts[4] == "oobCodes" && r.Method == http.MethodGet:
		rt.op = opListOobCodes
	case parts[4] == "accounts" && r.Method == http.MethodDelete:
		rt.op = opClearAccounts
	}

	return rt
}

func versionedRoute(r *http.Request, rt route, rest []string) route {
	switch {
	case len(rest) == 0:
		return route{}
	case rt.version == "v1" && rest[0] == "token" && len(rest) == 1:
		rt.op = opToken
	case rest[0] == "projects" && len(rest) >= 2:
		return projectRoute(r, rt, rest[1], rest[2:])
	case rt.version == "v1" && len(rest) == 1:
		rt.op = accountVerb(r, rest[0])
	}

	return rt
}

func projectRoute(r *http.Request, rt route, proj string, tail []string) route {
	rt.project = proj

	if before, verb, ok := strings.Cut(proj, ":"); ok {
		rt.project = before
		rt.op = projectColonVerb(r, rt, verb)

		return rt
	}

	if rt.version == "v2" {
		return v2Route(r, rt, tail)
	}

	if len(tail) >= 2 && tail[0] == segTenants {
		rt.tenant = tail[1]
		tail = tail[2:]

		if before, verb, ok := strings.Cut(rt.tenant, ":"); ok {
			rt.tenant = before
			rt.op = projectColonVerb(r, rt, verb)

			return rt
		}
	}

	if len(tail) == 1 {
		rt.op = projectVerb(r, tail[0])
	}

	return rt
}

func projectColonVerb(r *http.Request, rt route, verb string) string {
	if r.Method != http.MethodPost || rt.version != "v1" {
		return ""
	}

	switch verb {
	case "createSessionCookie":
		return opCreateSessionCookie
	case "queryAccounts":
		return opQuery
	}

	return ""
}

func projectVerb(r *http.Request, seg string) string {
	if seg == "accounts" && r.Method == http.MethodPost {
		return opAdminCreate
	}

	return accountVerb(r, seg)
}

func accountVerb(r *http.Request, seg string) string {
	verb, ok := strings.CutPrefix(seg, "accounts:")
	if !ok {
		return ""
	}

	if verb == "batchGet" {
		if r.Method == http.MethodGet {
			return opBatchGet
		}

		return ""
	}

	if r.Method == http.MethodPost && slices.Contains(accountVerbs, verb) {
		return verb
	}

	return ""
}

func v2Route(r *http.Request, rt route, tail []string) route {
	switch {
	case len(tail) == 1 && tail[0] == "config":
		rt.op = pick(r.Method, map[string]string{http.MethodGet: opGetConfig, http.MethodPatch: opUpdateConfig})
	case len(tail) == 1 && tail[0] == segTenants:
		rt.op = pick(r.Method, map[string]string{http.MethodGet: opListTenants, http.MethodPost: opCreateTenant})
	case len(tail) == 2 && tail[0] == segTenants:
		rt.id = tail[1]
		rt.op = pick(r.Method, map[string]string{
			http.MethodGet: opGetTenant, http.MethodPatch: opUpdateTenant, http.MethodDelete: opDeleteTenant,
		})
	}

	return rt
}

func pick(method string, ops map[string]string) string { return ops[method] }
