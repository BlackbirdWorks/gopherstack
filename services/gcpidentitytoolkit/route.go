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
		"signUp",
		"signInWithPassword",
		"signInWithCustomToken",
		"signInWithEmailLink",
		"lookup",
		"update",
		"delete",
		"sendOobCode",
		"sendVerificationCode",
		"signInWithPhoneNumber",
		opResetPassword,
		"createAuthUri",
		opSignInWithIdp,
		opBatchCreate,
		opBatchDelete,
		opQuery,
	}
)

//nolint:gochecknoglobals // immutable lookup table
var mfaOps = []string{opMfaEnrollStart, opMfaEnrollFinalize, opMfaEnrollWithdraw, opMfaSignInStart, opMfaSignInFinalize}

type route struct {
	op      string
	project string
	tenant  string
	id      string
	res     string
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

	if len(parts) == 2 && parts[1] == "action" && r.Method == http.MethodGet {
		return route{op: opEmulatorAction}
	}

	if len(parts) != emulatorPathLen || parts[1] != "v1" || parts[2] != "projects" {
		return route{}
	}

	rt := route{project: parts[3], tenant: r.URL.Query().Get("tenantId")}

	switch {
	case parts[4] == "oobCodes" && r.Method == http.MethodGet:
		rt.op = opListOobCodes
	case parts[4] == "verificationCodes" && r.Method == http.MethodGet:
		rt.op = opListVerificationCodes
	case parts[4] == segAccounts && r.Method == http.MethodDelete:
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
	case rt.version == "v2" && len(rest) == 1:
		return v2ClientRoute(r, rt, rest[0])
	case rt.version == "v2" && len(rest) == 2 && rest[0] == segAccounts:
		rt.op = mfaVerb(r, rest[1])
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

func v2ClientRoute(r *http.Request, rt route, seg string) route {
	if r.Method != http.MethodGet {
		return rt
	}

	rt.tenant = r.URL.Query().Get("tenantId")

	switch seg {
	case "passwordPolicy":
		rt.op = opGetPasswordPolicy
	case "recaptchaConfig":
		rt.op = opGetRecaptchaConfig
	}

	return rt
}

const idpItemLen = 2

func mfaVerb(r *http.Request, verb string) string {
	if r.Method == http.MethodPost && slices.Contains(mfaOps, verb) {
		return verb
	}

	return ""
}

func idpCollection(seg string) bool {
	return seg == collDefaultIdp || seg == collOAuthIdp || seg == collSamlIdp
}

func idpRoute(r *http.Request, rt route, tail []string) route {
	rt.res, rt.op = tail[0], opIdpConfigs

	switch len(tail) {
	case 1:
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			rt.op = ""
		}
	case idpItemLen:
		rt.id = tail[1]

		if r.Method != http.MethodGet && r.Method != http.MethodPatch && r.Method != http.MethodDelete {
			rt.op = ""
		}
	default:
		rt.op = ""
	}

	return rt
}

func v2ExtraRoute(r *http.Request, rt route, tail []string) (route, bool) {
	switch {
	case len(tail) >= 3 && tail[0] == segTenants && idpCollection(tail[2]):
		rt.tenant = tail[1]

		return idpRoute(r, rt, tail[2:]), true
	case len(tail) >= 1 && idpCollection(tail[0]):
		return idpRoute(r, rt, tail), true
	case len(tail) == 2 && tail[0] == segAccounts:
		rt.op = mfaVerb(r, tail[1])

		return rt, true
	}

	return rt, false
}

func v2Route(r *http.Request, rt route, tail []string) route {
	if ext, ok := v2ExtraRoute(r, rt, tail); ok {
		return ext
	}

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
