package gcpapigateway

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	translateAppend   = "APPEND_PATH_TO_ADDRESS"
	translateConstant = "CONSTANT_ADDRESS"
	defaultDeadline   = 15 * time.Second
	maxDeadlineSec    = 3600
	quotaUnit         = "1/min/{project}"
	maxMetricName     = 40
	kindAPIKey        = "apiKey"
	kindJWT           = "jwt"
	kindOther         = "other"
	keyIn             = "in"
	inQuery           = "query"
	inHeader          = "header"
	keyBackend        = "x-google-backend"
	keyResponses      = "responses"
)

//nolint:gochecknoglobals // immutable lookup table
var httpMethods = []string{"get", "put", "post", "delete", "options", "head", "patch"}

type seg struct {
	lit   string
	param string
}

type backendCfg struct {
	Address     string
	Translation string
	Deadline    time.Duration
	DisableAuth bool
}

type secDef struct {
	Name      string
	Kind      string
	KeyName   string
	KeyIn     string
	Issuer    string
	JWKSURI   string
	Audiences []string
}

type quotaLimit struct {
	Name   string
	Metric string
	PerMin int64
}

type route struct {
	Backend  *backendCfg
	Costs    map[string]int64
	Method   string
	Template string
	Segs     []seg
	Security [][]string
}

type compiledSpec struct {
	Defs     map[string]*secDef
	Limits   map[string]quotaLimit
	BasePath string
	Routes   []*route
}

type compiler struct {
	doc     map[string]any
	defs    map[string]*secDef
	metrics map[string]bool
	limits  map[string]quotaLimit
	diags   []diagnostic
	rootSec [][]string
	v3      bool
}

func asMap(v any) map[string]any {
	switch m := v.(type) {
	case map[string]any:
		return m
	case map[any]any:
		out := make(map[string]any, len(m))
		for k, e := range m {
			out[fmt.Sprint(k)] = e
		}

		return out
	default:
		return nil
	}
}

func asList(v any) []any {
	l, _ := v.([]any)

	return l
}

func str(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := m[k].(string); ok {
			return s
		}
	}

	return ""
}

func num(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case uint64:
		return float64(n), true
	default:
		return 0, false
	}
}

func first(m map[string]any, keys ...string) (any, bool) {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			return v, true
		}
	}

	return nil, false
}

func (c *compiler) failf(loc, format string, a ...any) {
	c.diags = append(c.diags, diagnostic{Location: loc, Message: fmt.Sprintf(format, a...)})
}

// compileSpec parses and validates an API config's OpenAPI document (YAML or JSON).
func compileSpec(raw []byte) (*compiledSpec, []diagnostic) {
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil || doc == nil {
		return nil, []diagnostic{{Message: "Unable to parse the OpenAPI document: it is not valid YAML or JSON."}}
	}

	c := &compiler{doc: doc, defs: map[string]*secDef{}, metrics: map[string]bool{}, limits: map[string]quotaLimit{}}

	if !c.version() {
		return nil, c.diags
	}

	base := c.basePath()
	c.securityDefs()
	c.management()
	c.rootSecurity()

	root := c.rootBackend()
	routes := c.paths(root)

	if len(c.diags) > 0 {
		return nil, c.diags
	}

	return &compiledSpec{Defs: c.defs, Limits: c.limits, BasePath: base, Routes: routes}, nil
}

func (c *compiler) version() bool {
	if s, ok := c.doc["swagger"]; ok {
		if n, isNum := num(s); (isNum && n == 2) || fmt.Sprint(s) == "2.0" {
			return true
		}

		c.failf("swagger", "Unsupported Swagger version %v: use \"2.0\".", s)

		return false
	}

	if s, ok := c.doc["openapi"].(string); ok && (strings.HasPrefix(s, "3.0") || strings.HasPrefix(s, "3.1")) {
		c.v3 = true

		return true
	}

	c.failf("", "Unsupported document: expected an OpenAPI 2.0 (swagger: \"2.0\") or 3.x (openapi: 3.0.x or 3.1.x) "+
		"specification.")

	return false
}

func (c *compiler) basePath() string {
	var base string

	if c.v3 {
		servers := asList(c.doc["servers"])
		if len(servers) > 0 {
			if u, err := url.Parse(str(asMap(servers[0]), "url")); err == nil {
				base = u.Path
			}
		}
	} else {
		base = str(c.doc, "basePath")
	}

	if base != "" && !strings.HasPrefix(base, "/") {
		c.failf("basePath", "basePath %q must start with '/'.", base)
	}

	return strings.TrimRight(base, "/")
}

func (c *compiler) securityDefs() {
	defs := asMap(c.doc["securityDefinitions"])
	if c.v3 {
		defs = asMap(asMap(c.doc["components"])["securitySchemes"])
	}

	issuers := map[string]string{}

	for _, name := range sortedKeys(defs) {
		m := asMap(defs[name])
		loc := "securityDefinitions." + name
		d := &secDef{Name: name, Kind: kindOther}
		c.defs[name] = d

		switch {
		case str(m, "type") == "apiKey":
			d.Kind, d.KeyName, d.KeyIn = kindAPIKey, str(m, "name"), str(m, keyIn)
			if d.KeyName == "" || (d.KeyIn != inQuery && d.KeyIn != inHeader) {
				c.failf(loc, "apiKey security definition %q needs a name and 'in' of query or header.", name)
			}
		case str(m, "x-google-issuer") != "":
			d.Kind, d.Issuer, d.JWKSURI = kindJWT, str(m, "x-google-issuer"), str(m, "x-google-jwks_uri")
			d.Audiences = audiences(m["x-google-audiences"])

			if !httpURL(d.JWKSURI) {
				c.failf(loc, "x-google-jwks_uri is required and must be an http or https URL.")
			}

			if prev, dup := issuers[d.Issuer]; dup {
				c.failf(loc, "x-google-issuer %q is already used by security definition %q.", d.Issuer, prev)
			}

			issuers[d.Issuer] = name
		}
	}
}

func httpURL(s string) bool {
	u, err := url.Parse(s)

	return s != "" && err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}

func audiences(v any) []string {
	switch t := v.(type) {
	case string:
		var out []string

		for a := range strings.SplitSeq(t, ",") {
			if a = strings.TrimSpace(a); a != "" {
				out = append(out, a)
			}
		}

		return out
	case []any:
		var out []string
		for _, a := range t {
			out = append(out, fmt.Sprint(a))
		}

		return out
	default:
		return nil
	}
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	return keys
}

func (c *compiler) management() {
	mg, ok := first(c.doc, "x-google-management", "x-google-api-management")
	if !ok {
		return
	}

	m := asMap(mg)

	for i, e := range asList(m["metrics"]) {
		em := asMap(e)
		loc := fmt.Sprintf("x-google-management.metrics[%d]", i)
		name := str(em, "name")

		switch {
		case name == "":
			c.failf(loc, "Metric name is required.")
		case len(str(em, "displayName")) > maxMetricName:
			c.failf(loc, "Metric displayName must be at most %d characters.", maxMetricName)
		case str(em, "valueType") != "INT64":
			c.failf(loc, "Metric %q: valueType must be INT64.", name)
		case str(em, "metricKind") != "DELTA":
			c.failf(loc, "Metric %q: metricKind must be DELTA.", name)
		default:
			c.metrics[name] = true
		}
	}

	limits := asList(asMap(m["quota"])["limits"])
	for i, e := range limits {
		c.limit(fmt.Sprintf("x-google-management.quota.limits[%d]", i), asMap(e))
	}
}

func (c *compiler) limit(loc string, m map[string]any) {
	name, metric := str(m, "name"), str(m, "metric")

	switch {
	case name == "" || metric == "":
		c.failf(loc, "Quota limits need a name and a metric.")

		return
	case !c.metrics[metric]:
		c.failf(loc, "Quota limit %q refers to undefined metric %q.", name, metric)

		return
	case str(m, "unit") != quotaUnit:
		c.failf(loc, "Quota limit %q: only the unit %q is supported.", name, quotaUnit)

		return
	}

	n, ok := num(asMap(m["values"])["STANDARD"])
	if !ok || n < 0 || n != float64(int64(n)) {
		c.failf(loc, "Quota limit %q: values.STANDARD must be a non-negative integer.", name)

		return
	}

	c.limits[metric] = quotaLimit{Name: name, Metric: metric, PerMin: int64(n)}
}

func (c *compiler) rootSecurity() {
	if sec, ok := c.doc["security"]; ok {
		c.rootSec = c.requirements("security", sec)
	}
}

func (c *compiler) requirements(loc string, v any) [][]string {
	list := asList(v)
	out := make([][]string, 0, len(list))

	for _, e := range list {
		req := []string{}

		for _, name := range sortedKeys(asMap(e)) {
			d, ok := c.defs[name]

			switch {
			case !ok:
				c.failf(loc, "Security requirement %q is not defined in the security definitions.", name)
			case d.Kind == kindOther:
				c.failf(
					loc,
					"Security definition %q is unsupported: use an apiKey or an x-google-issuer definition.",
					name,
				)
			default:
				req = append(req, name)
			}
		}

		out = append(out, req)
	}

	return out
}

// rootBackend returns the top-level x-google-backend, or nil.
func (c *compiler) rootBackend() *backendCfg {
	v, ok := c.doc[keyBackend]
	if !ok {
		return nil
	}

	return c.backend(keyBackend, v, translateAppend)
}

func (c *compiler) backend(loc string, v any, defTranslation string) *backendCfg {
	m := asMap(v)

	if name, ok := v.(string); ok && c.v3 {
		m = asMap(asMap(asMap(c.doc["x-google-api-management"])["backends"])[name])
		if m == nil {
			c.failf(loc, "Backend %q is not defined in x-google-api-management.backends.", name)

			return nil
		}
	}

	if m == nil {
		c.failf(loc, "x-google-backend must be an object.")

		return nil
	}

	b := &backendCfg{Address: str(m, "address"), Translation: defTranslation, Deadline: defaultDeadline}

	if !httpURL(b.Address) {
		c.failf(loc, "x-google-backend.address is required and must be an http or https URL.")
	}

	if t := str(m, "path_translation", "pathTranslation"); t != "" {
		if t != translateAppend && t != translateConstant {
			c.failf(loc, "Unsupported path_translation %q: use %s or %s.", t, translateAppend, translateConstant)
		}

		b.Translation = t
	}

	c.backendLimits(loc, m, b)

	return b
}

func (c *compiler) backendLimits(loc string, m map[string]any, b *backendCfg) {
	if d, ok := m["deadline"]; ok {
		n, isNum := num(d)
		if !isNum || n < 0 || n > maxDeadlineSec {
			c.failf(loc, "x-google-backend.deadline must be between 0 and %d seconds.", maxDeadlineSec)
		} else if n > 0 {
			b.Deadline = time.Duration(n * float64(time.Second))
		}
	}

	if p := str(m, "protocol"); p != "" && p != "http/1.1" && p != "h2" {
		c.failf(loc, "Unsupported x-google-backend.protocol %q: use http/1.1 or h2.", p)
	}

	disable, _ := first(m, "disable_auth", "disableAuth")
	b.DisableAuth, _ = disable.(bool)

	if _, aud := first(m, "jwt_audience", "jwtAudience"); aud && b.DisableAuth {
		c.failf(loc, "Only one of jwt_audience and disable_auth may be set.")
	}
}

func (c *compiler) paths(root *backendCfg) []*route {
	paths := asMap(c.doc["paths"])
	if len(paths) == 0 {
		c.failf("paths", "The document defines no paths.")

		return nil
	}

	var routes []*route

	seen := map[string]string{}

	for _, p := range sortedKeys(paths) {
		item := asMap(paths[p])
		segs, ok := c.template(p)

		for _, method := range httpMethods {
			opv, has := item[method]
			if !has {
				continue
			}

			loc := "paths." + p + "." + method
			r := c.operation(loc, asMap(opv), root)
			r.Method, r.Template, r.Segs = strings.ToUpper(method), p, segs

			if ok {
				key := r.Method + " " + normalised(segs)
				if prev, dup := seen[key]; dup {
					c.failf(loc, "Path %q is ambiguous with %q.", p, prev)
				}

				seen[key] = p
			}

			routes = append(routes, r)
		}
	}

	if len(routes) == 0 {
		c.failf("paths", "The document defines no operations.")
	}

	return routes
}

func (c *compiler) operation(loc string, op map[string]any, root *backendCfg) *route {
	r := &route{Backend: root}

	if _, ok := op[keyResponses]; !ok {
		c.failf(loc, "Operation is missing the required 'responses' field.")
	}

	if bv, ok := op[keyBackend]; ok {
		r.Backend = c.backend(loc+"."+keyBackend, bv, translateConstant)
	} else if root == nil {
		c.failf(loc, "Operation has no x-google-backend and the document defines none at the top level.")
	}

	r.Security = c.rootSec
	if sec, ok := op["security"]; ok {
		r.Security = c.requirements(loc+".security", sec)
	}

	if q, ok := op["x-google-quota"]; ok {
		r.Costs = map[string]int64{}
		costs := asMap(asMap(q)["metricCosts"])

		for _, metric := range sortedKeys(costs) {
			n, isNum := num(costs[metric])

			switch {
			case !c.metrics[metric]:
				c.failf(loc+".x-google-quota", "metricCosts refers to undefined metric %q.", metric)
			case !isNum || n < 0 || n != float64(int64(n)):
				c.failf(loc+".x-google-quota", "metricCosts[%q] must be a non-negative integer.", metric)
			default:
				r.Costs[metric] = int64(n)
			}
		}
	}

	return r
}

func (c *compiler) template(p string) ([]seg, bool) {
	if !strings.HasPrefix(p, "/") {
		c.failf("paths."+p, "Path %q must start with '/'.", p)

		return nil, false
	}

	var segs []seg

	for s := range strings.SplitSeq(strings.Trim(p, "/"), "/") {
		switch {
		case strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}") && !strings.ContainsAny(s[1:len(s)-1], "{}=*"):
			segs = append(segs, seg{param: s[1 : len(s)-1]})
		case strings.ContainsAny(s, "{}"):
			c.failf("paths."+p, "Path %q: a template variable must fill a whole segment as {name}.", p)

			return nil, false
		default:
			segs = append(segs, seg{lit: s})
		}
	}

	return segs, true
}

func normalised(segs []seg) string {
	parts := make([]string, len(segs))

	for i, s := range segs {
		if s.param != "" {
			parts[i] = "{}"
		} else {
			parts[i] = s.lit
		}
	}

	return strings.Join(parts, "/")
}

// match finds the route for a request method and path, with the path template variables.
func (s *compiledSpec) match(method, p string) (*route, map[string]string) {
	rest, ok := strings.CutPrefix(p, s.BasePath)
	if !ok || (rest != "" && !strings.HasPrefix(rest, "/")) {
		return nil, nil
	}

	parts := strings.Split(strings.Trim(rest, "/"), "/")

	var (
		best     *route
		bestVars map[string]string
		bestLits = -1
	)

	for _, r := range s.Routes {
		vars, lits, fits := r.fit(parts)
		if !fits {
			continue
		}

		if r.Method == method && lits > bestLits {
			best, bestVars, bestLits = r, vars, lits
		}
	}

	return best, bestVars
}

func (r *route) fit(parts []string) (map[string]string, int, bool) {
	if len(parts) != len(r.Segs) {
		return nil, 0, false
	}

	vars := map[string]string{}
	lits := 0

	for i, sg := range r.Segs {
		switch {
		case sg.param != "":
			if parts[i] == "" {
				return nil, 0, false
			}

			vars[sg.param] = parts[i]
		case sg.lit == parts[i]:
			lits++
		default:
			return nil, 0, false
		}
	}

	return vars, lits, true
}
