package cloudfront

import (
	"encoding/xml"
	"net/http"
	"strings"
)

const (
	defaultTTLSeconds = int64(86400)
	maxTTLSeconds     = int64(31536000)

	policyAllowAll      = "allow-all"
	policyHTTPSOnly     = "https-only"
	policyRedirectHTTPS = "redirect-to-https"
	originPolicyHTTP    = "http-only"
	originPolicyHTTPS   = "https-only"
	originPolicyMatch   = "match-viewer"

	selNone       = "none"
	selAll        = "all"
	selAllViewer  = "allviewer"
	selAllViewerC = "allviewerandwhitelistcloudfront"
	selWhitelist  = "whitelist"
	selAllExcept  = "allexcept"
)

type dpDistConfig struct {
	XMLName           xml.Name          `xml:"DistributionConfig"`
	DefaultRootObject string            `xml:"DefaultRootObject"`
	Origins           []dpOrigin        `xml:"Origins>Items>Origin"`
	Groups            []dpOriginGroup   `xml:"OriginGroups>Items>OriginGroup"`
	Behaviors         []dpBehavior      `xml:"CacheBehaviors>Items>CacheBehavior"`
	ErrorResponses    []dpErrorResponse `xml:"CustomErrorResponses>Items>CustomErrorResponse"`
	Default           dpBehavior        `xml:"DefaultCacheBehavior"`
	Enabled           bool              `xml:"Enabled"`
}

type dpOrigin struct {
	S3      *dpS3Origin     `xml:"S3OriginConfig"`
	Custom  *dpCustomOrigin `xml:"CustomOriginConfig"`
	ID      string          `xml:"Id"`
	Domain  string          `xml:"DomainName"`
	Path    string          `xml:"OriginPath"`
	OACID   string          `xml:"OriginAccessControlId"`
	Headers []dpHeaderPair  `xml:"CustomHeaders>Items>OriginCustomHeader"`

	ConnAttempts int `xml:"ConnectionAttempts"`
	ConnTimeout  int `xml:"ConnectionTimeout"`
}

type dpOriginGroup struct {
	ID      string   `xml:"Id"`
	Members []string `xml:"Members>Items>OriginGroupMember>OriginId"`
	Codes   []int    `xml:"FailoverCriteria>StatusCodes>Items>StatusCode"`
}

type dpHeaderPair struct {
	Name  string `xml:"HeaderName"`
	Value string `xml:"HeaderValue"`
}

type dpS3Origin struct {
	OAI string `xml:"OriginAccessIdentity"`
}

type dpCustomOrigin struct {
	Policy    string `xml:"OriginProtocolPolicy"`
	HTTPPort  int    `xml:"HTTPPort"`
	HTTPSPort int    `xml:"HTTPSPort"`

	ReadTimeout int `xml:"OriginReadTimeout"`
	KeepAlive   int `xml:"OriginKeepaliveTimeout"`
}

type dpTrustedKeyGroups struct {
	Groups  []string `xml:"Items>KeyGroup"`
	Enabled bool     `xml:"Enabled"`
}

type dpBehavior struct {
	MinTTL                *int64             `xml:"MinTTL"`
	DefaultTTL            *int64             `xml:"DefaultTTL"`
	MaxTTL                *int64             `xml:"MaxTTL"`
	Forwarded             *dpForwarded       `xml:"ForwardedValues"`
	ViewerProtocolPolicy  string             `xml:"ViewerProtocolPolicy"`
	TargetOriginID        string             `xml:"TargetOriginId"`
	PathPattern           string             `xml:"PathPattern"`
	CachePolicyID         string             `xml:"CachePolicyId"`
	OriginRequestPolicyID string             `xml:"OriginRequestPolicyId"`
	ResponseHeadersID     string             `xml:"ResponseHeadersPolicyId"`
	Methods               []string           `xml:"AllowedMethods>Items>Method"`
	CachedMethods         []string           `xml:"AllowedMethods>CachedMethods>Items>Method"`
	Functions             []dpFunctionAssoc  `xml:"FunctionAssociations>Items>FunctionAssociation"`
	Edge                  []dpEdgeAssoc      `xml:"LambdaFunctionAssociations>Items>LambdaFunctionAssociation"`
	KeyGroups             dpTrustedKeyGroups `xml:"TrustedKeyGroups"`
}

type dpForwarded struct {
	Forward   string   `xml:"Cookies>Forward"`
	QueryKeys []string `xml:"QueryStringCacheKeys>Items>Name"`
	Headers   []string `xml:"Headers>Items>Name"`
	Whitelist []string `xml:"Cookies>WhitelistedNames>Items>Name"`
	QueryStr  bool     `xml:"QueryString"`
}

type dpErrorResponse struct {
	PagePath     string `xml:"ResponsePagePath"`
	ResponseCode string `xml:"ResponseCode"`
	ErrorCode    int    `xml:"ErrorCode"`
}

func parseDistConfig(raw []byte) (*dpDistConfig, error) {
	var cfg dpDistConfig
	if err := xml.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *dpDistConfig) origin(id string) *dpOrigin {
	for i := range c.Origins {
		if c.Origins[i].ID == id {
			return &c.Origins[i]
		}
	}

	return nil
}

// behaviorFor picks the first cache behavior whose PathPattern matches, else the default.
func (c *dpDistConfig) behaviorFor(reqPath string) *dpBehavior {
	for i := range c.Behaviors {
		if matchPathPattern(c.Behaviors[i].PathPattern, reqPath) {
			return &c.Behaviors[i]
		}
	}

	return &c.Default
}

func (b *dpBehavior) allows(method string) bool {
	allowed := b.Methods
	if len(allowed) == 0 {
		allowed = []string{http.MethodGet, http.MethodHead}
	}

	for _, m := range allowed {
		if strings.EqualFold(m, method) {
			return true
		}
	}

	return false
}

func (b *dpBehavior) caches(method string) bool {
	cached := b.CachedMethods
	if len(cached) == 0 {
		cached = []string{http.MethodGet, http.MethodHead}
	}

	for _, m := range cached {
		if strings.EqualFold(m, method) {
			return true
		}
	}

	return false
}

func matchPathPattern(pattern, reqPath string) bool {
	pattern = strings.TrimPrefix(pattern, "/")
	reqPath = strings.TrimPrefix(reqPath, "/")

	return globMatchCS(pattern, reqPath)
}

// globMatchCS matches '*' (any run, including '/') and '?' (one char), case-sensitively.
func globMatchCS(pattern, s string) bool {
	var (
		px, sx         int
		nextPx, nextSx = -1, 0
	)

	for sx < len(s) || px < len(pattern) {
		if px < len(pattern) {
			switch pattern[px] {
			case '*':
				nextPx, nextSx = px, sx+1
				px++

				continue
			case '?':
				if sx < len(s) {
					px++
					sx++

					continue
				}
			default:
				if sx < len(s) && pattern[px] == s[sx] {
					px++
					sx++

					continue
				}
			}
		}

		if nextPx == -1 || nextSx > len(s) {
			return false
		}

		px, sx = nextPx+1, nextSx
		nextSx++
	}

	return true
}

// selector decides which names of one kind (header, cookie, query string) are used.
type selector struct {
	behavior string
	names    []string
}

func (s selector) selects(name string, fold bool) bool {
	in := func() bool {
		for _, n := range s.names {
			if n == name || (fold && strings.EqualFold(n, name)) {
				return true
			}
		}

		return false
	}

	switch s.behavior {
	case selAll, selAllViewer, selAllViewerC:
		return true
	case selWhitelist:
		return in()
	case selAllExcept:
		return !in()
	default:
		return false
	}
}

type dpTTL struct{ min, def, max int64 }

type dpPolicy struct {
	cacheHeaders  selector
	cacheCookies  selector
	cacheQuery    selector
	originHeaders selector
	originCookies selector
	originQuery   selector
	ttl           dpTTL
	gzip          bool
	cloudFrontHdr bool
}

func (p *dpPolicy) forwardsHeader(name string) bool {
	return p.cacheHeaders.selects(name, true) || p.originHeaders.selects(name, true)
}

func (p *dpPolicy) forwardsCookie(name string) bool {
	return p.cacheCookies.selects(name, false) || p.originCookies.selects(name, false)
}

func (p *dpPolicy) forwardsQuery(name string) bool {
	return p.cacheQuery.selects(name, false) || p.originQuery.selects(name, false)
}

func (p *dpPolicy) forwardsAnyQuery() bool {
	return p.cacheQuery.behavior != "" && p.cacheQuery.behavior != selNone ||
		p.originQuery.behavior != "" && p.originQuery.behavior != selNone
}

func lower(s string) string { return strings.ToLower(s) }

func legacyPolicy(b *dpBehavior) dpPolicy {
	p := dpPolicy{ttl: dpTTL{def: defaultTTLSeconds, max: maxTTLSeconds}}
	if b.MinTTL != nil {
		p.ttl.min = *b.MinTTL
	}

	if b.DefaultTTL != nil {
		p.ttl.def = *b.DefaultTTL
	}

	if b.MaxTTL != nil {
		p.ttl.max = *b.MaxTTL
	}

	if p.ttl.max < p.ttl.def {
		p.ttl.max = p.ttl.def
	}

	f := b.Forwarded
	if f == nil {
		return p
	}

	if f.QueryStr {
		if len(f.QueryKeys) > 0 {
			p.cacheQuery = selector{behavior: selWhitelist, names: f.QueryKeys}
		} else {
			p.cacheQuery = selector{behavior: selAll}
		}
	}

	p.cacheCookies = selector{behavior: lower(f.Forward), names: f.Whitelist}
	if len(f.Headers) == 1 && f.Headers[0] == "*" {
		p.cacheHeaders = selector{behavior: selAll}
	} else if len(f.Headers) > 0 {
		p.cacheHeaders = selector{behavior: selWhitelist, names: f.Headers}
	}

	return p
}
