package ociloadbalancer

import (
	"net"
	"net/http"
	"net/netip"
	"regexp"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	maxNameLen      = 255
	maxPort         = 65535
	maxCookieAge    = 31536000
	defaultReturn   = http.StatusOK
	defaultRetries  = 3
	defaultTimeout  = 3000
	defaultInterval = 10000
	maxRetries      = 10
)

var (
	nameRE     = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	hostnameRE = regexp.MustCompile(
		`^(\*\.)?([A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?)(\.[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?)*(\.\*)?$`,
	)
	tokenRE = regexp.MustCompile("^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
)

func checkName(kind, name string) *APIError {
	if name == "" {
		return errMissing(kind + " name is required")
	}

	if len(name) > maxNameLen || !nameRE.MatchString(name) {
		return errParam(kind + " name may contain only letters, digits, dashes and underscores")
	}

	return nil
}

type backendIn struct {
	Port           *int   `json:"port"`
	Weight         *int   `json:"weight"`
	MaxConnections *int   `json:"maxConnections"`
	Backup         *bool  `json:"backup"`
	Drain          *bool  `json:"drain"`
	Offline        *bool  `json:"offline"`
	IPAddress      string `json:"ipAddress"`
}

type healthCheckerIn struct {
	URLPath           *string `json:"urlPath"`
	Port              *int    `json:"port"`
	ReturnCode        *int    `json:"returnCode"`
	Retries           *int    `json:"retries"`
	TimeoutInMillis   *int    `json:"timeoutInMillis"`
	IntervalInMillis  *int    `json:"intervalInMillis"`
	ResponseBodyRegex *string `json:"responseBodyRegex"`
	IsForcePlainText  *bool   `json:"isForcePlainText"`
	Protocol          string  `json:"protocol"`
}

type backendSetIn struct {
	HealthChecker         *healthCheckerIn     `json:"healthChecker"`
	BackendMaxConnections *int                 `json:"backendMaxConnections"`
	SSL                   *SSLConfig           `json:"sslConfiguration"`
	SessionPersistence    *SessionPersistence  `json:"sessionPersistenceConfiguration"`
	LBCookiePersistence   *LBCookiePersistence `json:"lbCookieSessionPersistenceConfiguration"`
	Name                  string               `json:"name"`
	Policy                string               `json:"policy"`
	Backends              []backendIn          `json:"backends"`
}

func intOr(v *int, def int) int {
	if v == nil {
		return def
	}

	return *v
}

func boolOr(v *bool) bool { return v != nil && *v }

func backendName(ip string, port int) string { return net.JoinHostPort(ip, itoa(port)) }

func buildBackend(in backendIn) (Backend, *APIError) {
	addr, err := netip.ParseAddr(in.IPAddress)
	if err != nil {
		return Backend{}, errParam("backend ipAddress " + in.IPAddress + " is not a valid IP address")
	}

	if in.Port == nil || *in.Port < 1 || *in.Port > maxPort {
		return Backend{}, errParam("backend port must be between 1 and 65535")
	}

	w := intOr(in.Weight, 1)
	if w < 1 {
		return Backend{}, errParam("backend weight must be at least 1")
	}

	if in.MaxConnections != nil && *in.MaxConnections < 1 {
		return Backend{}, errParam("backend maxConnections must be at least 1")
	}

	ip := addr.String()

	return Backend{
		Name: backendName(ip, *in.Port), IPAddress: ip, Port: *in.Port, Weight: w, MaxConnections: in.MaxConnections,
		Backup: boolOr(in.Backup), Drain: boolOr(in.Drain), Offline: boolOr(in.Offline),
	}, nil
}

func buildHealthChecker(in *healthCheckerIn) (HealthChecker, *APIError) {
	if in == nil {
		return HealthChecker{}, errMissing("healthChecker is required")
	}

	hc := HealthChecker{
		Protocol: strings.ToUpper(
			in.Protocol,
		),
		Port:             intOr(in.Port, 0),
		ReturnCode:       intOr(in.ReturnCode, defaultReturn),
		Retries:          intOr(in.Retries, defaultRetries),
		TimeoutInMillis:  intOr(in.TimeoutInMillis, defaultTimeout),
		IntervalInMillis: intOr(in.IntervalInMillis, defaultInterval),
		IsForcePlainText: boolOr(in.IsForcePlainText),
	}

	if in.ResponseBodyRegex != nil {
		hc.ResponseBodyRegex = *in.ResponseBodyRegex
	}

	if hc.Protocol != protoHTTP && hc.Protocol != protoTCP {
		return HealthChecker{}, errParam("healthChecker protocol must be HTTP or TCP")
	}

	if hc.validate() != nil {
		return HealthChecker{}, hc.validate()
	}

	if hc.Protocol == protoHTTP {
		hc.URLPath = "/"
		if in.URLPath != nil {
			hc.URLPath = *in.URLPath
		}
	}

	return hc, nil
}

// validate reports why a health checker's numeric fields are out of range.
func (hc HealthChecker) validate() *APIError {
	switch {
	case hc.Port < 0 || hc.Port > maxPort:
		return errParam("healthChecker port must be between 0 and 65535")
	case hc.Retries < 0 || hc.Retries > maxRetries:
		return errParam("healthChecker retries must be between 0 and 10")
	case hc.TimeoutInMillis < 1 || hc.IntervalInMillis < 1:
		return errParam("healthChecker timeoutInMillis and intervalInMillis must be positive")
	case hc.ReturnCode < http.StatusContinue || hc.ReturnCode > 599:
		return errParam("healthChecker returnCode must be an HTTP status code")
	}

	if hc.ResponseBodyRegex != "" {
		if _, err := regexp.Compile(hc.ResponseBodyRegex); err != nil {
			return errParam("healthChecker responseBodyRegex is not a valid regular expression")
		}
	}

	return nil
}

func buildBackendSet(name string, in *backendSetIn) (*BackendSet, *APIError) {
	if err := checkName("backend set", name); err != nil {
		return nil, err
	}

	if !slices.Contains(policyNames, in.Policy) {
		return nil, errParam("policy must be one of " + strings.Join(policyNames, ", "))
	}

	hc, err := buildHealthChecker(in.HealthChecker)
	if err != nil {
		return nil, err
	}

	if in.BackendMaxConnections != nil && *in.BackendMaxConnections < 1 {
		return nil, errParam("backendMaxConnections must be at least 1")
	}

	if in.SessionPersistence != nil && in.LBCookiePersistence != nil {
		return nil, errParam(
			"sessionPersistenceConfiguration and lbCookieSessionPersistenceConfiguration are mutually exclusive",
		)
	}

	if err = checkPersistence(in); err != nil {
		return nil, err
	}

	bs := &BackendSet{
		Name: name, Policy: in.Policy, HealthChecker: hc, BackendMaxConnections: in.BackendMaxConnections,
		SSL: in.SSL, SessionPersistence: in.SessionPersistence, LBCookiePersistence: in.LBCookiePersistence,
		Backends: []Backend{},
	}

	for _, bi := range in.Backends {
		be, berr := buildBackend(bi)
		if berr != nil {
			return nil, berr
		}

		if slices.ContainsFunc(bs.Backends, func(o Backend) bool { return o.Name == be.Name }) {
			return nil, errParam("duplicate backend " + be.Name)
		}

		bs.Backends = append(bs.Backends, be)
	}

	return bs, nil
}

func checkPersistence(in *backendSetIn) *APIError {
	if sp := in.SessionPersistence; sp != nil && sp.CookieName == "" {
		return errMissing("sessionPersistenceConfiguration cookieName is required")
	}

	if lc := in.LBCookiePersistence; lc != nil {
		if lc.MaxAgeInSeconds != nil && (*lc.MaxAgeInSeconds < 0 || *lc.MaxAgeInSeconds > maxCookieAge) {
			return errParam("maxAgeInSeconds is out of range")
		}
	}

	return nil
}

type listenerIn struct {
	SSL        *SSLConfig        `json:"sslConfiguration"`
	Connection *ConnectionConfig `json:"connectionConfiguration"`
	Name       string            `json:"name"`
	DefaultSet string            `json:"defaultBackendSetName"`
	Protocol   string            `json:"protocol"`
	PathRoutes string            `json:"pathRouteSetName"`
	Policy     string            `json:"routingPolicyName"`
	Port       *int              `json:"port"`
	Hostnames  []string          `json:"hostnameNames"`
	RuleSets   []string          `json:"ruleSetNames"`
}

func buildListener(name string, in *listenerIn) (*Listener, *APIError) {
	if err := checkName("listener", name); err != nil {
		return nil, err
	}

	proto := strings.ToUpper(in.Protocol)
	if !slices.Contains(protocolNames, proto) {
		return nil, errParam("protocol must be one of " + strings.Join(protocolNames, ", "))
	}

	if in.Port == nil || *in.Port < 1 || *in.Port > maxPort {
		return nil, errParam("listener port must be between 1 and 65535")
	}

	if in.DefaultSet == "" {
		return nil, errMissing("defaultBackendSetName is required")
	}

	l := &Listener{
		Name: name, DefaultSet: in.DefaultSet, Protocol: proto, Port: *in.Port, Hostnames: in.Hostnames,
		PathRoutes: in.PathRoutes, Policy: in.Policy, RuleSets: in.RuleSets, SSL: in.SSL, Connection: in.Connection,
	}

	if err := checkListenerShape(l); err != nil {
		return nil, err
	}

	return l, nil
}

func checkListenerShape(l *Listener) *APIError {
	if l.Protocol == protoHTTP2 && l.SSL == nil {
		return errParam("an HTTP2 listener requires sslConfiguration")
	}

	if l.Protocol == protoTCP && (len(l.Hostnames) > 0 || l.PathRoutes != "" || l.Policy != "" || len(l.RuleSets) > 0) {
		return errParam(
			"hostnameNames, pathRouteSetName, routingPolicyName and ruleSetNames apply to HTTP listeners only",
		)
	}

	if l.PathRoutes != "" && l.Policy != "" {
		return errParam("a listener cannot use both pathRouteSetName and routingPolicyName")
	}

	if l.SSL != nil {
		if err := checkListenerSSL(l.SSL); err != nil {
			return err
		}
	}

	return checkConnection(l.Connection)
}

func checkConnection(c *ConnectionConfig) *APIError {
	if c == nil {
		return nil
	}

	if c.IdleTimeout < 1 {
		return errParam("connectionConfiguration idleTimeout must be positive")
	}

	if c.BackendTCPProxyProtocolVersion != nil || len(c.BackendTCPProxyProtocolOptions) > 0 {
		return errUnsupported("backend TCP proxy protocol")
	}

	return nil
}

func checkListenerSSL(s *SSLConfig) *APIError {
	if len(s.CertificateIDs) > 0 || len(s.TrustedCertificateAuthorityIDs) > 0 {
		return errUnsupported("certificateIds and trustedCertificateAuthorityIds (OCI Certificates service)")
	}

	if s.CertificateName == "" {
		return errMissing("sslConfiguration certificateName is required")
	}

	return checkTLSVersions(s)
}

func checkTLSVersions(s *SSLConfig) *APIError {
	for _, p := range s.Protocols {
		if p != "TLSv1.2" && p != "TLSv1.3" {
			return errUnsupported("TLS protocol " + p + " (only TLSv1.2 and TLSv1.3)")
		}
	}

	return nil
}

type hostnameIn struct {
	Name     string `json:"name"`
	Hostname string `json:"hostname"`
}

func checkHostname(h string) *APIError {
	if h == "" || len(h) > maxNameLen || !hostnameRE.MatchString(h) {
		return errParam("hostname " + h + " is not a valid virtual hostname")
	}

	return nil
}

type pathRouteSetIn struct {
	Name       string      `json:"name"`
	PathRoutes []PathRoute `json:"pathRoutes"`
}

func buildPathRouteSet(name string, in *pathRouteSetIn) (*PathRouteSet, *APIError) {
	if err := checkName("path route set", name); err != nil {
		return nil, err
	}

	if len(in.PathRoutes) == 0 {
		return nil, errMissing("pathRoutes is required")
	}

	for _, r := range in.PathRoutes {
		if r.Path == "" || r.BackendSetName == "" {
			return nil, errMissing("each path route needs path and backendSetName")
		}

		if !slices.Contains([]string{matchExact, matchLongest, matchPrefix, matchSuffix}, r.PathMatchType.MatchType) {
			return nil, errParam(
				"pathMatchType.matchType must be EXACT_MATCH, FORCE_LONGEST_PREFIX_MATCH, PREFIX_MATCH or SUFFIX_MATCH",
			)
		}
	}

	return &PathRouteSet{Name: name, PathRoutes: in.PathRoutes}, nil
}

type routingPolicyIn struct {
	Name                     string        `json:"name"`
	ConditionLanguageVersion string        `json:"conditionLanguageVersion"`
	Rules                    []RoutingRule `json:"rules"`
}

func buildRoutingPolicy(name string, in *routingPolicyIn, keepVersion string) (*RoutingPolicy, *APIError) {
	if err := checkName("routing policy", name); err != nil {
		return nil, err
	}

	version := in.ConditionLanguageVersion
	if version == "" {
		version = keepVersion
	}

	if version != "V1" {
		return nil, errParam("conditionLanguageVersion must be V1")
	}

	if len(in.Rules) == 0 {
		return nil, errMissing("rules is required")
	}

	seen := map[string]bool{}

	for _, r := range in.Rules {
		if err := checkName("routing rule", r.Name); err != nil {
			return nil, err
		}

		if seen[strs.Fold(r.Name)] {
			return nil, errParam("duplicate routing rule " + r.Name)
		}

		seen[strs.Fold(r.Name)] = true

		if _, err := parseCondition(r.Condition); err != nil {
			return nil, errParam("rule " + r.Name + ": " + err.Error())
		}

		if len(r.Actions) != 1 || r.Actions[0].Name != "FORWARD_TO_BACKENDSET" || r.Actions[0].BackendSetName == "" {
			return nil, errParam(
				"rule " + r.Name + " needs exactly one FORWARD_TO_BACKENDSET action with backendSetName",
			)
		}
	}

	return &RoutingPolicy{Name: name, ConditionLanguageVersion: version, Rules: in.Rules}, nil
}

type ruleSetIn struct {
	Name  string `json:"name"`
	Items []Rule `json:"items"`
}

func buildRuleSet(name string, in *ruleSetIn) (*RuleSet, *APIError) {
	if err := checkName("rule set", name); err != nil {
		return nil, err
	}

	if len(in.Items) == 0 {
		return nil, errMissing("items is required")
	}

	items := make([]Rule, 0, len(in.Items))

	for _, r := range in.Items {
		nr, err := normalizeRule(r)
		if err != nil {
			return nil, err
		}

		items = append(items, nr)
	}

	return &RuleSet{Name: name, Items: items}, nil
}

type cipherSuiteIn struct {
	Name    string   `json:"name"`
	Ciphers []string `json:"ciphers"`
}

func buildCipherSuite(name string, in *cipherSuiteIn) (*SSLCipherSuite, *APIError) {
	if err := checkName("SSL cipher suite", name); err != nil {
		return nil, err
	}

	if strings.HasPrefix(name, "oci-") {
		return nil, errParam("the oci- prefix is reserved for built-in cipher suites")
	}

	if len(in.Ciphers) == 0 {
		return nil, errMissing("ciphers is required")
	}

	for _, c := range in.Ciphers {
		if _, ok := cipherIDs[c]; !ok {
			return nil, errParam("cipher " + c + " is not supported by this emulator")
		}
	}

	return &SSLCipherSuite{Name: name, Ciphers: in.Ciphers}, nil
}
