package cloudfront

import (
	"fmt"
	"slices"
	"sort"
)

const (
	maxReadTimeout     = 60
	maxKeepAlive       = 60
	maxConnTimeout     = 10
	maxConnAttempts    = 3
	maxPort            = 65535
	protoHTTP          = "http"
	protoHTTPS         = "https"
	protoMatchViewer   = "match-viewer"
	criteriaDefault    = "default"
	criteriaMediaScore = "media-quality-score"
)

// originParams are the validated arguments of updateRequestOrigin/selectRequestOriginById.
type originParams struct {
	Path         *string
	OACEnabled   *bool
	Headers      map[string]string
	Domain       string
	Protocol     string
	IPAddrType   string
	ShieldRegion string
	SSLProtocols []string
	ReadTimeout  int
	KeepAlive    int
	ConnTimeout  int
	Attempts     int
	Port         int
	Shield       bool
}

func objField(m map[string]any, key string) (map[string]any, bool, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return nil, false, nil
	}

	o, isObj := v.(map[string]any)
	if !isObj {
		return nil, false, fnError(key + " must be an object")
	}

	return o, true, nil
}

func intField(m map[string]any, key string, hi int) (int, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return 0, nil
	}

	n, isNum := numToInt(v)
	if !isNum || n < 1 || n > hi {
		return 0, fnError(fmt.Sprintf("%s must be an integer between 1 and %d", key, hi))
	}

	return n, nil
}

func numToInt(v any) (int, bool) {
	switch t := v.(type) {
	case int64:
		return int(t), true
	case int:
		return t, true
	case float64:
		if t == float64(int(t)) {
			return int(t), true
		}
	}

	return 0, false
}

func strField(m map[string]any, key string, allowed ...string) (string, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return "", nil
	}

	s, isStr := v.(string)
	if !isStr || s == "" || (len(allowed) > 0 && !slices.Contains(allowed, s)) {
		return "", fnError(fmt.Sprintf("%s is invalid", key))
	}

	return s, nil
}

func boolField(m map[string]any, key string) (*bool, error) {
	v, ok := m[key]
	if !ok || v == nil {
		return nil, nil //nolint:nilnil // absent optional
	}

	b, isBool := v.(bool)
	if !isBool {
		return nil, fnError(key + " must be a boolean")
	}

	return &b, nil
}

// parseOriginParams validates the params object of the origin helpers.
func parseOriginParams(m map[string]any) (*originParams, error) {
	p := &originParams{}

	steps := []func(map[string]any, *originParams) error{
		parseBasicParams, parseTimeouts, parseCustomOriginConfig, parseOACConfig, parseShieldConfig,
	}
	for _, step := range steps {
		if err := step(m, p); err != nil {
			return nil, err
		}
	}

	return p, nil
}

func parseBasicParams(m map[string]any, p *originParams) error {
	var err error

	if p.Domain, err = strField(m, "domainName"); err != nil {
		return err
	}

	if v, ok := m["originPath"]; ok && v != nil {
		s, isStr := v.(string)
		if !isStr {
			return fnError("originPath must be a string")
		}

		p.Path = &s
	}

	if p.Attempts, err = intField(m, "connectionAttempts", maxConnAttempts); err != nil {
		return err
	}

	hs, ok, err := objField(m, "customHeaders")
	if err != nil || !ok {
		return err
	}

	p.Headers = make(map[string]string, len(hs))

	for k, v := range hs {
		s, isStr := v.(string)
		if !isStr {
			return fnError("customHeaders values must be strings")
		}

		p.Headers[k] = s
	}

	return nil
}

func parseTimeouts(m map[string]any, p *originParams) error {
	t, ok, err := objField(m, "timeouts")
	if err != nil || !ok {
		return err
	}

	if p.ReadTimeout, err = intField(t, "readTimeout", maxReadTimeout); err != nil {
		return err
	}

	if p.KeepAlive, err = intField(t, "keepAliveTimeout", maxKeepAlive); err != nil {
		return err
	}

	p.ConnTimeout, err = intField(t, "connectionTimeout", maxConnTimeout)

	return err
}

func parseCustomOriginConfig(m map[string]any, p *originParams) error {
	c, ok, err := objField(m, "customOriginConfig")
	if err != nil || !ok {
		return err
	}

	if p.Port, err = intField(c, "port", maxPort); err != nil {
		return err
	}

	if p.Protocol, err = strField(c, "protocol", protoHTTP, protoHTTPS, protoMatchViewer); err != nil {
		return err
	}

	if p.IPAddrType, err = strField(c, "ipAddressType", "ipv4", "ipv6", "dualstack"); err != nil {
		return err
	}

	if v, has := c["sslProtocols"]; has && v != nil {
		p.SSLProtocols, err = enumList(v, "sslProtocols", "SSLv3", "TLSv1", "TLSv1.1", "TLSv1.2")
	}

	return err
}

func enumList(v any, name string, allowed ...string) ([]string, error) {
	arr, ok := v.([]any)
	if !ok {
		return nil, fnError(name + " must be an array")
	}

	out := make([]string, 0, len(arr))

	for _, e := range arr {
		s, isStr := e.(string)
		if !isStr || (len(allowed) > 0 && !slices.Contains(allowed, s)) {
			return nil, fnError(name + " contains an invalid value")
		}

		out = append(out, s)
	}

	return out, nil
}

func parseOACConfig(m map[string]any, p *originParams) error {
	c, ok, err := objField(m, "originAccessControlConfig")
	if err != nil || !ok {
		return err
	}

	if p.OACEnabled, err = boolField(c, "enabled"); err != nil {
		return err
	}

	if _, err = strField(c, "signingBehavior", "always", "never", "no-override"); err != nil {
		return err
	}

	if _, err = strField(c, "signingProtocol", "sigv4"); err != nil {
		return err
	}

	_, err = strField(c, "originType", "s3", "mediastore", "mediapackagev2", "lambda")

	return err
}

func parseShieldConfig(m map[string]any, p *originParams) error {
	c, ok, err := objField(m, "originShield")
	if err != nil || !ok {
		return err
	}

	en, err := boolField(c, "enabled")
	if err != nil {
		return err
	}

	p.Shield = en != nil && *en

	if p.ShieldRegion, err = strField(c, "region"); err != nil {
		return err
	}

	if p.Shield && p.ShieldRegion == "" {
		return fnError("originShield.region is required when originShield is enabled")
	}

	return nil
}

// groupMember is one origin of a created request origin group.
type groupMember struct {
	Over *originParams
	ID   string
}

// groupSpec is the validated argument of createRequestOriginGroup.
type groupSpec struct {
	Criteria string
	Members  []groupMember
	Failover []int
}

var defaultFailoverCodes = []int{500, 502, 503, 504} //nolint:gochecknoglobals // static default

// parseGroupSpec validates createRequestOriginGroup params against the distribution's origins.
func parseGroupSpec(m map[string]any, cat *originCatalog) (*groupSpec, error) {
	g := &groupSpec{Failover: slices.Clone(defaultFailoverCodes)}

	var err error

	if g.Criteria, err = strField(m, "selectionCriteria", criteriaDefault, criteriaMediaScore); err != nil {
		return nil, err
	}

	if g.Criteria == "" {
		g.Criteria = criteriaDefault
	}

	if g.Members, err = parseGroupMembers(m["originIds"], cat); err != nil {
		return nil, err
	}

	fc, ok, err := objField(m, "failoverCriteria")
	if err != nil {
		return nil, err
	}

	if ok {
		if g.Failover, err = parseStatusCodes(fc["statusCodes"]); err != nil {
			return nil, err
		}
	}

	return g, nil
}

func parseGroupMembers(v any, cat *originCatalog) ([]groupMember, error) {
	arr, ok := v.([]any)
	if !ok || len(arr) != groupSize {
		return nil, fnError("originIds must be an array of exactly two origins")
	}

	out := make([]groupMember, 0, len(arr))
	seen := map[string]bool{}

	for _, e := range arr {
		mem, err := parseGroupMember(e, cat)
		if err != nil {
			return nil, err
		}

		if seen[mem.ID] {
			return nil, fnError("originIds must be distinct")
		}

		seen[mem.ID] = true

		out = append(out, mem)
	}

	return out, nil
}

const groupSize = 2

func parseGroupMember(e any, cat *originCatalog) (groupMember, error) {
	var mem groupMember

	switch t := e.(type) {
	case string:
		mem.ID = t
	case map[string]any:
		id, err := strField(t, "originId")
		if err != nil || id == "" {
			return mem, fnError("originIds entries need an originId")
		}

		mem.ID = id

		if ov, has, oerr := objField(t, "originOverrides"); oerr != nil {
			return mem, oerr
		} else if has {
			if mem.Over, err = parseOriginParams(ov); err != nil {
				return mem, err
			}
		}
	default:
		return mem, fnError("originIds entries must be strings or objects")
	}

	if !cat.has(mem.ID) {
		return mem, fnError(fmt.Sprintf("origin %q does not exist in the distribution", mem.ID))
	}

	return mem, nil
}

func parseStatusCodes(v any) ([]int, error) {
	arr, ok := v.([]any)
	if !ok || len(arr) == 0 {
		return nil, fnError("failoverCriteria.statusCodes must be a non-empty array")
	}

	out := make([]int, 0, len(arr))

	for _, e := range arr {
		n, isNum := numToInt(e)
		if !isNum || n < 400 || n > 599 {
			return nil, fnError("failoverCriteria.statusCodes must be HTTP error status codes")
		}

		out = append(out, n)
	}

	sort.Ints(out)

	return out, nil
}

// originDirective is the origin routing a viewer-request function asked for.
type originDirective struct {
	Params   *originParams
	Group    *groupSpec
	Selected string
}

// originCatalog lists the distribution's origin ids; a nil catalog accepts any id (TestFunction).
type originCatalog struct{ ids map[string]struct{} }

func newOriginCatalog(ids []string) *originCatalog {
	c := &originCatalog{ids: make(map[string]struct{}, len(ids))}
	for _, id := range ids {
		c.ids[id] = struct{}{}
	}

	return c
}

func (c *originCatalog) has(id string) bool {
	if c == nil {
		return true
	}

	_, ok := c.ids[id]

	return ok
}
