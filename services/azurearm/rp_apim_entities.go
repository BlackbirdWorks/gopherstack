package azurearm

import (
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	apimMaxName       = 300
	apimMaxSubName    = 100
	apimMaxNamedName  = 256
	apimMaxNamedValue = 4096
	apimMaxURL        = 2000
	apimMaxPath       = 400
	apimMaxOpMethod   = 20
	apimMaxOpTemplate = 1000
	apimMaxOps        = 1000
	apimMaxKeyLen     = 256
	apimKeyBytes      = 16
	apimHeaderSub     = "Ocp-Apim-Subscription-Key"
	apimQuerySub      = "subscription-key"
	apimStateActive   = "active"
)

var (
	apimNamedNameRE = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
	apimTemplateRE  = regexp.MustCompile(`\{(\*?)([^{}/?&=]+)\}`)
)

func newAPIMKey() string {
	var b [apimKeyBytes]byte

	_, _ = rand.Read(b[:])

	return hex.EncodeToString(b[:])
}

func optString(in map[string]any, key string, minLen, maxLen int, required bool, what string) (string, error) {
	raw, has := in[key]
	if !has || raw == nil {
		if required {
			return "", apimInvalid("The %s is required.", what)
		}

		return "", nil
	}

	v, ok := raw.(string)
	if !ok {
		return "", apimInvalid("The %s must be a string.", what)
	}

	if n := utf8.RuneCountInString(v); n < minLen || n > maxLen {
		return "", apimInvalid("The %s must be %d to %d characters long.", what, minLen, maxLen)
	}

	return v, nil
}

func optBool(in map[string]any, key string, def bool) (bool, error) {
	raw, has := in[key]
	if !has || raw == nil {
		return def, nil
	}

	v, ok := raw.(bool)
	if !ok {
		return false, apimInvalid("The %s must be true or false.", key)
	}

	return v, nil
}

func validAbsURL(raw string, schemes ...string) bool {
	u, err := url.Parse(raw)

	return err == nil && u.Host != "" && slices.Contains(schemes, strings.ToLower(u.Scheme))
}

func templateParams(tpl string) []string {
	var out []string

	for _, m := range apimTemplateRE.FindAllStringSubmatch(tpl, -1) {
		if !slices.Contains(out, m[2]) {
			out = append(out, m[2])
		}
	}

	return out
}

func canonicalTemplate(tpl string) string {
	path, _, _ := strings.Cut(tpl, "?")
	path = strings.ToLower(strings.Trim(path, "/"))

	return apimTemplateRE.ReplaceAllString(path, "{}")
}

func normAPI(s *apimService, _, id string, in map[string]any, _ *apimEntity) (map[string]any, error) {
	out := cloneAnyMap(in)

	var imported []apimImportedOp

	if format := strOf(in, "format"); format != "" {
		spec, err := parseAPISpec(format, strOf(in, fieldValue))
		if err != nil {
			return nil, err
		}

		spec.fillDefaults(out)
		imported = spec.ops
	}

	for _, k := range []string{
		"format", fieldValue, "wsdlSelector", "soapApiType", "translateRequiredQueryParametersConduct", "isOnline",
		"sourceApiId", "apiType",
	} {
		delete(out, k)
	}

	if err := apiFields(s, id, in, out); err != nil {
		return nil, err
	}

	out["isOnline"] = true

	cur, err := optBool(out, "isCurrent", true)
	if err != nil {
		return nil, err
	}

	out["isCurrent"] = cur

	if _, has := out["apiRevision"]; !has {
		out["apiRevision"] = "1"
	}

	if len(imported) > 0 {
		s.importOps(id, imported)
	}

	return out, nil
}

func apiFields(s *apimService, id string, in, out map[string]any) error {
	if _, err := optString(out, "displayName", 1, apimMaxName, true, "API displayName"); err != nil {
		return err
	}

	if _, has := out["path"]; !has {
		return apimInvalid("The API path is required.")
	}

	path, err := optString(out, "path", 0, apimMaxPath, true, "API path")
	if err != nil {
		return err
	}

	path = strings.Trim(path, "/")
	out["path"] = path

	for _, other := range s.APIs {
		if !strings.EqualFold(other.ID, id) && strings.EqualFold(strOf(other.Props, "path"), path) {
			return apimInvalid("The API path %q is already used by another API.", path)
		}
	}

	if err = apiProtocols(out); err != nil {
		return err
	}

	if su, serr := optString(out, "serviceUrl", 0, apimMaxURL, false, "API serviceUrl"); serr != nil {
		return serr
	} else if su != "" && !validAbsURL(su, "http", "https", "ws", "wss") {
		return apimInvalid("The API serviceUrl must be an absolute http, https, ws or wss URL.")
	}

	if _, err = optBool(out, "subscriptionRequired", true); err != nil {
		return err
	}

	if _, has := out["subscriptionRequired"]; !has {
		out["subscriptionRequired"] = true
	}

	return apiKeyNamesAndType(in, out)
}

func apiProtocols(out map[string]any) error {
	raw, has := out["protocols"]
	if !has || raw == nil {
		out["protocols"] = []any{"https"}

		return nil
	}

	arr, ok := raw.([]any)
	if !ok || len(arr) == 0 {
		return apimInvalid("The API protocols must be a non-empty list.")
	}

	for _, e := range arr {
		if v, isStr := e.(string); !isStr || !slices.Contains([]string{"http", "https", "ws", "wss"}, strings.ToLower(v)) {
			return apimInvalid("The API protocols must be among http, https, ws and wss.")
		}
	}

	return nil
}

func apiKeyNamesAndType(in, out map[string]any) error {
	names := map[string]any{"header": apimHeaderSub, "query": apimQuerySub}

	if given := mapOf(out["subscriptionKeyParameterNames"]); given != nil {
		for _, k := range []string{"header", "query"} {
			if v, has := given[k]; has {
				str, ok := v.(string)
				if !ok || str == "" {
					return apimInvalid("The subscriptionKeyParameterNames %s must be a non-empty string.", k)
				}

				names[k] = str
			}
		}
	}

	out["subscriptionKeyParameterNames"] = names

	typ := strOf(in, "apiType")
	if typ == "" {
		typ = strOf(in, "type")
	}

	if typ == "" {
		typ = "http"
	}

	if !slices.Contains([]string{"http", "soap", "websocket", "graphql"}, typ) {
		return apimInvalid("The API type must be http, soap, websocket or graphql.")
	}

	out["type"] = typ

	return nil
}

func delAPI(s *apimService, _, id string, _ url.Values) error {
	lid := strings.ToLower(id)

	for k, o := range s.Ops {
		if strings.EqualFold(o.Parent, id) {
			delete(s.Ops, k)
			delete(s.Policies, entKey("apis/"+lid+"/operations/"+o.ID, apimPolicyID))
		}
	}

	delete(s.Policies, entKey("apis/"+lid, apimPolicyID))

	for k := range s.ProductAPIs {
		if strings.HasSuffix(k, "/"+lid) {
			delete(s.ProductAPIs, k)
		}
	}

	for k, sub := range s.Subs {
		if strings.EqualFold(strOf(sub.Props, "scope"), s.armBase()+"/apis/"+id) {
			delete(s.Subs, k)
		}
	}

	return nil
}

func (s *apimService) importOps(apiID string, ops []apimImportedOp) {
	for _, o := range ops {
		key := entKey(apiID, o.id)
		etag := uint64(1)

		if prev, ok := s.Ops[key]; ok {
			etag = prev.Etag + 1
		}

		s.Ops[key] = &apimEntity{ID: o.id, Parent: apiID, Props: o.props, Etag: etag}
	}
}

func opParams(in map[string]any, tpl string) ([]any, error) {
	declared := map[string]bool{}
	params := []any{}

	inTpl := templateParams(tpl)

	if raw, has := in["templateParameters"]; has && raw != nil {
		arr, ok := raw.([]any)
		if !ok {
			return nil, apimInvalid("The templateParameters must be a list.")
		}

		for _, e := range arr {
			m := mapOf(e)
			name, typ := strOf(m, fieldName), strOf(m, fieldType)

			if m == nil || name == "" || typ == "" {
				return nil, apimInvalid("Every template parameter requires a name and a type.")
			}

			if !slices.Contains(inTpl, name) {
				return nil, apimInvalid("The template parameter %q does not appear in the urlTemplate.", name)
			}

			declared[name] = true

			params = append(params, m)
		}
	}

	for _, name := range inTpl {
		if !declared[name] {
			params = append(params, map[string]any{fieldName: name, "required": true, fieldType: "string"})
		}
	}

	return params, nil
}

func normOperation(s *apimService, parent, id string, in map[string]any, _ *apimEntity) (map[string]any, error) {
	out := cloneAnyMap(in)
	delete(out, "policies")

	if _, err := optString(out, "displayName", 1, apimMaxName, true, "operation displayName"); err != nil {
		return nil, err
	}

	method, err := optString(out, "method", 1, apimMaxOpMethod, true, "operation method")
	if err != nil {
		return nil, err
	}

	method = strings.ToUpper(method)
	out["method"] = method

	tpl, err := optString(out, "urlTemplate", 1, apimMaxOpTemplate, true, "operation urlTemplate")
	if err != nil {
		return nil, err
	}

	for _, other := range s.Ops {
		if strings.EqualFold(other.Parent, parent) && !strings.EqualFold(other.ID, id) &&
			strOf(other.Props, "method") == method &&
			canonicalTemplate(strOf(other.Props, "urlTemplate")) == canonicalTemplate(tpl) {
			return nil, apimInvalid(
				"An operation with method %s and the same URL template already exists in this API.", method,
			)
		}
	}

	if countOps(s, parent) >= apimMaxOps {
		return nil, apimErr(409, apimCodeConflict, "Too many operations in the API.")
	}

	params, err := opParams(out, tpl)
	if err != nil {
		return nil, err
	}

	out["templateParameters"] = params

	return out, nil
}

func countOps(s *apimService, api string) int {
	n := 0

	for _, o := range s.Ops {
		if strings.EqualFold(o.Parent, api) {
			n++
		}
	}

	return n
}

func delOperation(s *apimService, parent, id string, _ url.Values) error {
	delete(s.Policies, entKey("apis/"+strings.ToLower(parent)+"/operations/"+id, apimPolicyID))

	return nil
}

func normProduct(_ *apimService, _, _ string, in map[string]any, _ *apimEntity) (map[string]any, error) {
	out := cloneAnyMap(in)

	if _, err := optString(out, "displayName", 1, apimMaxName, true, "product displayName"); err != nil {
		return nil, err
	}

	state := strOf(out, "state")
	if state == "" {
		state = "notPublished"
	}

	if state != "published" && state != "notPublished" {
		return nil, apimInvalid("The product state must be published or notPublished.")
	}

	out["state"] = state

	req, err := optBool(out, "subscriptionRequired", true)
	if err != nil {
		return nil, err
	}

	out["subscriptionRequired"] = req

	if _, err = optBool(out, "approvalRequired", false); err != nil {
		return nil, err
	}

	if lim, has := out["subscriptionsLimit"]; has && lim != nil && intOf(lim) < 0 {
		return nil, apimInvalid("The subscriptionsLimit must not be negative.")
	}

	return out, nil
}

func delProduct(s *apimService, _, id string, q url.Values) error {
	scope := s.armBase() + "/products/" + id

	var subs []string

	for k, sub := range s.Subs {
		if strings.EqualFold(strOf(sub.Props, "scope"), scope) {
			subs = append(subs, k)
		}
	}

	if len(subs) > 0 && q.Get("deleteSubscriptions") != "true" {
		return apimErr(409, apimCodeConflict,
			"The product has subscriptions; delete them first or pass deleteSubscriptions=true.")
	}

	for _, k := range subs {
		delete(s.Subs, k)
	}

	prefix := strings.ToLower(id) + "/"
	for k := range s.ProductAPIs {
		if strings.HasPrefix(k, prefix) {
			delete(s.ProductAPIs, k)
		}
	}

	return nil
}

func (s *apimService) normScope(scope string) (string, error) {
	base := s.armBase()
	rel := scope

	if len(rel) >= len(base) && strings.EqualFold(rel[:len(base)], base) {
		rel = rel[len(base):]
	}

	parts := strings.Split(strings.Trim(rel, "/"), "/")
	if parts[0] == "" {
		parts = nil
	}

	switch {
	case len(parts) == 0:
		return base + "/", nil
	case len(parts) == 1 && strings.EqualFold(parts[0], "apis"):
		return base + "/apis", nil
	case len(parts) == 2 && strings.EqualFold(parts[0], "apis"):
		if e, ok := s.APIs[entKey("", parts[1])]; ok {
			return base + "/apis/" + e.ID, nil
		}

		return "", apimInvalid("The scope refers to the API %q, which does not exist.", parts[1])
	case len(parts) == 2 && strings.EqualFold(parts[0], "products"):
		if e, ok := s.Products[entKey("", parts[1])]; ok {
			return base + "/products/" + e.ID, nil
		}

		return "", apimInvalid("The scope refers to the product %q, which does not exist.", parts[1])
	}

	return "", apimInvalid("The scope must be /apis, /apis/{apiId} or /products/{productId}.")
}

func normSubscription(s *apimService, _, _ string, in map[string]any, prev *apimEntity) (map[string]any, error) {
	out := cloneAnyMap(in)

	if _, err := optString(out, "displayName", 1, apimMaxSubName, true, "subscription displayName"); err != nil {
		return nil, err
	}

	rawScope, err := optString(out, "scope", 1, apimMaxURL, true, "subscription scope")
	if err != nil {
		return nil, err
	}

	if out["scope"], err = s.normScope(rawScope); err != nil {
		return nil, err
	}

	state := strOf(out, "state")
	if state == "" {
		state = "submitted"
	}

	if !slices.Contains([]string{apimStateActive, "suspended", "submitted", "rejected", "cancelled", "expired"}, state) {
		return nil, apimInvalid("The subscription state is not valid.")
	}

	out["state"] = state

	for _, k := range []string{"primaryKey", "secondaryKey"} {
		v, kerr := optString(out, k, 1, apimMaxKeyLen, false, "subscription "+k)
		if kerr != nil {
			return nil, kerr
		}

		switch {
		case v != "":
		case prev != nil:
			out[k] = prev.Props[k]
		default:
			out[k] = newAPIMKey()
		}
	}

	if _, err = optBool(out, "allowTracing", false); err != nil {
		return nil, err
	}

	stampSubscription(s, out, prev)

	return out, nil
}

func stampSubscription(s *apimService, out map[string]any, prev *apimEntity) {
	now := time.Now()
	if s.now != nil {
		now = s.now()
	}

	stamp := now.UTC().Format(time.RFC3339)

	if prev != nil {
		out["createdDate"] = prev.Props["createdDate"]
		if v, ok := prev.Props["startDate"]; ok {
			out["startDate"] = v
		}
	} else {
		out["createdDate"] = stamp
	}

	if _, has := out["startDate"]; !has && out["state"] == apimStateActive {
		out["startDate"] = stamp
	}
}

func viewSubscription(props map[string]any) map[string]any {
	out := cloneAnyMap(props)
	delete(out, "primaryKey")
	delete(out, "secondaryKey")

	return out
}

func normNamedValue(s *apimService, _, id string, in map[string]any, _ *apimEntity) (map[string]any, error) {
	out := cloneAnyMap(in)

	name, err := optString(out, "displayName", 1, apimMaxNamedName, true, "named value displayName")
	if err != nil {
		return nil, err
	}

	if !apimNamedNameRE.MatchString(name) {
		return nil, apimInvalid("The named value displayName may contain only letters, digits, period, dash and underscore.")
	}

	for _, other := range s.Named {
		if !strings.EqualFold(other.ID, id) && strings.EqualFold(strOf(other.Props, "displayName"), name) {
			return nil, apimInvalid("A named value with the displayName %q already exists.", name)
		}
	}

	if out["keyVault"] != nil {
		return nil, apimInvalid("Key Vault backed named values are not supported; supply the value directly.")
	}

	delete(out, "keyVault")

	val, err := optString(out, fieldValue, 1, apimMaxNamedValue, true, "named value value")
	if err != nil {
		return nil, err
	}

	if strings.TrimSpace(val) == "" {
		return nil, apimInvalid("The named value value must not be empty or whitespace.")
	}

	if strings.HasPrefix(val, "@(") || strings.HasPrefix(val, "@{") {
		return nil, apimInvalid("Policy expressions in named values are not supported.")
	}

	if _, err = optBool(out, "secret", false); err != nil {
		return nil, err
	}

	if _, has := out["secret"]; !has {
		out["secret"] = false
	}

	return out, nil
}

func viewNamedValue(props map[string]any) map[string]any {
	out := cloneAnyMap(props)
	if secret, _ := out["secret"].(bool); secret {
		delete(out, fieldValue)
	}

	return out
}

func normBackend(_ *apimService, _, _ string, in map[string]any, _ *apimEntity) (map[string]any, error) {
	out := cloneAnyMap(in)

	u, err := optString(out, "url", 1, apimMaxURL, true, "backend url")
	if err != nil {
		return nil, err
	}

	if !validAbsURL(u, "http", "https") {
		return nil, apimInvalid("The backend url must be an absolute http or https URL.")
	}

	if proto := strOf(out, "protocol"); proto != "http" && proto != "soap" {
		return nil, apimInvalid("The backend protocol must be http or soap.")
	}

	if _, err = optString(out, "title", 0, apimMaxName, false, "backend title"); err != nil {
		return nil, err
	}

	if _, err = optString(out, "description", 0, apimMaxURL, false, "backend description"); err != nil {
		return nil, err
	}

	if err = checkBackendCredentials(mapOf(out["credentials"])); err != nil {
		return nil, err
	}

	return out, nil
}

func checkBackendCredentials(c map[string]any) error {
	for _, k := range []string{"header", "query"} {
		for name, v := range mapOf(c[k]) {
			vals, ok := v.([]any)
			if !ok || name == "" {
				return apimInvalid("The backend credentials %s must map names to lists of values.", k)
			}

			for _, e := range vals {
				if _, isStr := e.(string); !isStr {
					return apimInvalid("The backend credentials %s values must be strings.", k)
				}
			}
		}
	}

	if a := mapOf(c["authorization"]); a != nil && (strOf(a, "scheme") == "" || strOf(a, "parameter") == "") {
		return apimInvalid("The backend authorization credentials need a scheme and a parameter.")
	}

	return nil
}

func viewBackend(props map[string]any) map[string]any {
	out := cloneAnyMap(props)

	if c := mapOf(out["credentials"]); c != nil {
		red := cloneAnyMap(c)
		delete(red, "authorization")

		for _, k := range []string{"header", "query"} {
			names := map[string]any{}
			for name := range mapOf(c[k]) {
				names[name] = []any{}
			}

			if len(names) > 0 {
				red[k] = names
			}
		}

		out["credentials"] = red
	}

	if px := mapOf(out["proxy"]); px != nil {
		red := cloneAnyMap(px)
		delete(red, "password")
		out["proxy"] = red
	}

	return out
}
