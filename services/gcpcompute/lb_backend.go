package gcpcompute

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"slices"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	schemeExternal        = "EXTERNAL"
	schemeExternalManaged = "EXTERNAL_MANAGED"
	schemeInternal        = "INTERNAL"
	schemeInternalManaged = "INTERNAL_MANAGED"
	schemeSelfManaged     = "INTERNAL_SELF_MANAGED"
	protoHTTP             = "HTTP"
	protoHTTPS            = "HTTPS"
	protoHTTP2            = "HTTP2"
	protoGRPC             = "GRPC"
	protoUDP              = "UDP"
	protoTCP              = "TCP"
	affinityNone          = "NONE"
	modeUtilization       = "UTILIZATION"
	modeRate              = "RATE"
	modeConnection        = "CONNECTION"
	defTimeoutSec         = 30
	maxTimeoutSec         = 86400
	defDrainingSec        = 300
	defMaxUtilization     = 0.8
	maxSignedKeys         = 3
	signedKeyBytes        = 16
	maxBucketHeaders      = 5
)

var (
	bsProtocols = map[string]bool{ //nolint:gochecknoglobals // read-only lookup table
		protoHTTP: true, protoHTTPS: true, protoHTTP2: true, "H2C": true, protoGRPC: true,
		protoTCP: true, "SSL": true, protoUDP: true, "UNSPECIFIED": true,
	}
	bsHTTPProtocols = map[string]bool{ //nolint:gochecknoglobals // read-only lookup table
		protoHTTP: true, protoHTTPS: true, protoHTTP2: true, "H2C": true, protoGRPC: true,
	}
	bsAffinities = map[string]bool{ //nolint:gochecknoglobals // read-only lookup table
		affinityNone: true, "CLIENT_IP": true, "CLIENT_IP_PROTO": true, "CLIENT_IP_PORT_PROTO": true,
		"GENERATED_COOKIE": true, "HEADER_FIELD": true, "HTTP_COOKIE": true, "STRONG_COOKIE_AFFINITY": true,
	}
	cacheModes = map[string]bool{ //nolint:gochecknoglobals // read-only lookup table
		"": true, "USE_ORIGIN_HEADERS": true, "FORCE_CACHE_ALL": true, "CACHE_ALL_STATIC": true,
	}
	bucketNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$`)
	keyValueRE   = regexp.MustCompile(`^[A-Za-z0-9_-]{22}={0,2}$`)
)

func bsSpec() lbSpec[computeapi.BackendService] {
	return lbSpec[computeapi.BackendService]{
		coll: collBackendSvc, kind: "compute#backendService", noun: "backend_service",
		m: func(b *InMemoryBackend) map[string]*computeapi.BackendService { return b.backendSvcs },
		meta: func(v *computeapi.BackendService) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink,
				region: &v.Region, fp: &v.Fingerprint, id: &v.Id,
			}
		},
		prep: prepBackendService,
		view: func(v *computeapi.BackendService) {
			for _, be := range v.Backends {
				be.ForceSendFields = []string{"CapacityScaler"}
			}
		},
		refs: func(v *computeapi.BackendService) []string {
			out := slices.Clone(v.HealthChecks)
			for _, be := range v.Backends {
				out = append(out, be.Group)
			}

			if v.SecurityPolicy != "" {
				out = append(out, v.SecurityPolicy)
			}

			return out
		},
	}
}

func hcLbSpec() lbSpec[computeapi.HealthCheck] {
	return lbSpec[computeapi.HealthCheck]{
		coll: collHealth, kind: "compute#healthCheck", noun: "health_check",
		m: func(b *InMemoryBackend) map[string]*computeapi.HealthCheck { return b.healths },
		meta: func(v *computeapi.HealthCheck) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink,
				region: &v.Region, id: &v.Id,
			}
		},
	}
}

func bucketSpec() lbSpec[computeapi.BackendBucket] {
	return lbSpec[computeapi.BackendBucket]{
		coll: collBackendBucket, kind: "compute#backendBucket", noun: "backend_bucket",
		m: func(b *InMemoryBackend) map[string]*computeapi.BackendBucket { return b.backendBuckets },
		meta: func(v *computeapi.BackendBucket) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink,
				region: &v.Region, id: &v.Id,
			}
		},
		prep: prepBackendBucket,
	}
}

func validBackendScheme(c lbCtx, scheme string) bool {
	switch scheme {
	case schemeExternal, schemeExternalManaged:
		return true
	case schemeSelfManaged:
		return c.scope == scopeGlobal
	case schemeInternal, schemeInternalManaged:
		return c.scope == scopeRegion
	}

	return false
}

func prepBackendService(b *InMemoryBackend, c lbCtx, in *computeapi.BackendService, raw map[string]any) *APIError {
	if in.LoadBalancingScheme == "" {
		in.LoadBalancingScheme = schemeExternal
	}

	if !validBackendScheme(c, in.LoadBalancingScheme) {
		return errField("resource.loadBalancingScheme", in.LoadBalancingScheme, "Not supported for a "+c.scope+
			" backend service. Global: EXTERNAL, EXTERNAL_MANAGED, INTERNAL_SELF_MANAGED; "+
			"regional: EXTERNAL, EXTERNAL_MANAGED, INTERNAL, INTERNAL_MANAGED.")
	}

	if err := normalizeBSProtocol(in); err != nil {
		return err
	}

	if err := normalizeBSScalars(in); err != nil {
		return err
	}

	hcs, err := lbResolveAll(b, hcLbSpec(), c, "resource.healthChecks", in.HealthChecks)
	if err != nil {
		return err
	}

	if len(hcs) > 1 {
		return errField(
			"resource.healthChecks",
			strings.Join(in.HealthChecks, ","),
			"At most one health check is allowed.",
		)
	}

	in.HealthChecks = hcs

	if err = normalizeBSExtras(in); err != nil {
		return err
	}

	if err = b.resolveBSSecurityPolicyLocked(c, in); err != nil {
		return err
	}

	return b.prepBackendsLocked(c, in, raw)
}

func normalizeBSProtocol(in *computeapi.BackendService) *APIError {
	if in.Protocol == "" {
		in.Protocol = protoHTTP
		if in.LoadBalancingScheme == schemeInternal {
			in.Protocol = protoTCP
		}
	}

	if !bsProtocols[in.Protocol] {
		return errField("resource.protocol", in.Protocol, "")
	}

	if in.LoadBalancingScheme == schemeInternal && in.Protocol != protoTCP &&
		in.Protocol != protoUDP && in.Protocol != "UNSPECIFIED" {
		return errField(
			"resource.protocol",
			in.Protocol,
			"An INTERNAL backend service supports TCP, UDP and UNSPECIFIED.",
		)
	}

	if in.Port < 0 || in.Port > hcMaxPort {
		return errField("resource.port", "", "Port must be in the range 1-65535.")
	}

	return nil
}

func normalizeBSScalars(in *computeapi.BackendService) *APIError {
	if in.TimeoutSec == 0 {
		in.TimeoutSec = defTimeoutSec
	}

	if in.TimeoutSec < 1 || in.TimeoutSec > maxTimeoutSec {
		return errInvalid("Invalid value for field 'resource.timeoutSec': '%d'. Must be between 1 and %d.",
			in.TimeoutSec, maxTimeoutSec)
	}

	if in.SessionAffinity == "" {
		in.SessionAffinity = affinityNone
	}

	if !bsAffinities[in.SessionAffinity] {
		return errField("resource.sessionAffinity", in.SessionAffinity, "")
	}

	if in.AffinityCookieTtlSec < 0 || in.AffinityCookieTtlSec > maxTimeoutSec {
		return errField("resource.affinityCookieTtlSec", "", "Must be between 0 and 86400.")
	}

	if in.ConnectionDraining == nil {
		in.ConnectionDraining = &computeapi.ConnectionDraining{DrainingTimeoutSec: defDrainingSec}
	}

	in.ConnectionDraining.ForceSendFields = []string{"DrainingTimeoutSec"}

	return nil
}

func normalizeBSExtras(in *computeapi.BackendService) *APIError {
	if in.EnableCDN && (!bsHTTPProtocols[in.Protocol] || in.LoadBalancingScheme == schemeInternal ||
		in.LoadBalancingScheme == schemeSelfManaged) {
		return errInvalid("Invalid value for field 'resource.enableCDN': Cloud CDN needs an HTTP(S) protocol " +
			"and an EXTERNAL or EXTERNAL_MANAGED load balancing scheme.")
	}

	if p := in.CdnPolicy; p != nil {
		if !cacheModes[p.CacheMode] {
			return errField("resource.cdnPolicy.cacheMode", p.CacheMode, "")
		}

		if p.ClientTtl < 0 || p.DefaultTtl < 0 || p.MaxTtl < 0 {
			return errField("resource.cdnPolicy", "", "TTL values cannot be negative.")
		}
	}

	if i := in.Iap; i != nil && i.Enabled {
		if i.Oauth2ClientId == "" {
			return errRequired("resource.iap.oauth2ClientId")
		}

		if i.Oauth2ClientSecret != "" {
			sum := sha256.Sum256([]byte(i.Oauth2ClientSecret))
			i.Oauth2ClientSecretSha256, i.Oauth2ClientSecret = hex.EncodeToString(sum[:]), ""
		}
	}

	return nil
}

type backendGroup struct {
	neg     *computeapi.NetworkEndpointGroup
	link    string
	negKey  string
	named   []*computeapi.NamedPort
	members []string
}

func (b *InMemoryBackend) resolveBackendGroupLocked(c lbCtx, raw string) (backendGroup, *APIError) {
	const field = "resource.backends.group"

	parts := strings.Split(trimURL(raw), "/")
	if len(parts) < nameInRegionOrZone {
		return backendGroup{}, errField(field, raw, "The URL is malformed.")
	}

	switch parts[len(parts)-2] {
	case collGroups:
		return b.resolveInstanceGroupBackendLocked(c, field, raw)
	case collNEG:
		return b.resolveNEGBackendLocked(c, field, raw)
	}

	return backendGroup{}, errField(field, raw, "Must be an instance group or a network endpoint group.")
}

func (b *InMemoryBackend) resolveInstanceGroupBackendLocked(c lbCtx, field, raw string) (backendGroup, *APIError) {
	r, ok := parseRef(raw, collGroups, c.project, "")
	if !ok || r.loc == "" || r.scope != scopeZone && r.scope != scopeRegion {
		return backendGroup{}, errField(field, raw, "The URL is malformed.")
	}

	g, members, err := b.findGroupLocked(r.project, r.scope, r.loc, r.name)
	if err != nil {
		return backendGroup{}, err
	}

	region := r.loc
	if r.scope == scopeZone {
		region = regionOfZone(r.loc)
	}

	if c.scope == scopeRegion && region != c.loc {
		return backendGroup{}, errField(field, raw, "The group is not in the backend service's region.")
	}

	return backendGroup{link: g.SelfLink, named: g.NamedPorts, members: members}, nil
}

func (b *InMemoryBackend) resolveNEGBackendLocked(c lbCtx, field, raw string) (backendGroup, *APIError) {
	r, ok := parseRef(raw, collNEG, c.project, "")
	if !ok || r.scope == "" {
		return backendGroup{}, errField(field, raw, "The URL is malformed.")
	}

	lk := r.loc
	if r.scope == scopeGlobal {
		lk = scopeGlobal
	}

	neg, found := b.negs[key(r.project, lk, r.name)]
	if !found {
		return backendGroup{}, errNotFound(relPath(r.project, r.scope, r.loc, collNEG, r.name))
	}

	region := r.loc
	if r.scope == scopeZone {
		region = regionOfZone(r.loc)
	}

	if c.scope == scopeRegion && (r.scope == scopeGlobal || region != c.loc) {
		return backendGroup{}, errField(field, raw, "The group is not in the backend service's region.")
	}

	return backendGroup{link: neg.SelfLink, neg: neg, negKey: key(r.project, lk, r.name)}, nil
}

func rawBackends(raw map[string]any) []any {
	list, _ := raw["backends"].([]any)

	return list
}

func rawHas(list []any, i int, field string) bool {
	if i >= len(list) {
		return false
	}

	m, _ := list[i].(map[string]any)
	_, ok := m[field]

	return ok
}

func internetNEG(n *computeapi.NetworkEndpointGroup) bool {
	return n != nil && (n.NetworkEndpointType == negInternetIPPort || n.NetworkEndpointType == negInternetFQDN)
}

func (b *InMemoryBackend) prepBackendsLocked(c lbCtx, in *computeapi.BackendService, raw map[string]any) *APIError {
	rb := rawBackends(raw)
	keep := rb == nil
	seen := map[string]bool{}
	needsHC := false

	for i, be := range in.Backends {
		if be.Group == "" {
			return errRequired("resource.backends.group")
		}

		g, err := b.resolveBackendGroupLocked(c, be.Group)
		if err != nil {
			return err
		}

		if seen[g.link] {
			return errField("resource.backends.group", be.Group, "Duplicate backend.")
		}

		seen[g.link], be.Group = true, g.link

		serverless := g.neg != nil && g.neg.NetworkEndpointType == negServerless
		needsHC = needsHC || !serverless

		if internetNEG(g.neg) && c.scope != scopeGlobal {
			return errField("resource.backends.group", be.Group, "Internet NEGs need a global backend service.")
		}

		if err = normalizeBackend(in, be, g, serverless, keep || rawHas(rb, i, "capacityScaler")); err != nil {
			return err
		}
	}

	if needsHC && len(in.HealthChecks) == 0 {
		return errRequired("resource.healthChecks")
	}

	return nil
}

func checkBalancingMode(be *computeapi.Backend, g backendGroup) *APIError {
	switch be.BalancingMode {
	case modeUtilization:
		if g.neg != nil {
			return errField("resource.backends.balancingMode", be.BalancingMode, "NEG backends use RATE or CONNECTION.")
		}

		if be.MaxUtilization == 0 {
			be.MaxUtilization = defMaxUtilization
		}

		if be.MaxUtilization < 0 || be.MaxUtilization > 1 {
			return errField("resource.backends.maxUtilization", "", "Must be in (0, 1].")
		}
	case modeRate:
		if be.MaxRate <= 0 && be.MaxRatePerInstance <= 0 && be.MaxRatePerEndpoint <= 0 {
			return errInvalid("Invalid value for field 'resource.backends': RATE balancing mode needs maxRate, " +
				"maxRatePerInstance or maxRatePerEndpoint.")
		}
	case modeConnection:
	default:
		return errField("resource.backends.balancingMode", be.BalancingMode, "")
	}

	return nil
}

func normalizeBackend(
	in *computeapi.BackendService,
	be *computeapi.Backend,
	g backendGroup,
	serverless, scalerSet bool,
) *APIError {
	if serverless {
		if be.BalancingMode != "" {
			return errField(
				"resource.backends.balancingMode",
				be.BalancingMode,
				"Serverless NEG backends take no balancing mode.",
			)
		}

		return nil
	}

	if be.BalancingMode == "" {
		be.BalancingMode = defaultBalancingMode(in, g)
	}

	if err := checkBalancingMode(be, g); err != nil {
		return err
	}

	if !scalerSet && be.CapacityScaler == 0 {
		be.CapacityScaler = 1
	}

	if be.CapacityScaler < 0 || be.CapacityScaler > 1 {
		return errField("resource.backends.capacityScaler", "", "Must be between 0.0 and 1.0.")
	}

	be.ForceSendFields = []string{"CapacityScaler"}

	return nil
}

func defaultBalancingMode(in *computeapi.BackendService, g backendGroup) string {
	if bsHTTPProtocols[in.Protocol] {
		if g.neg != nil {
			return modeRate
		}

		return modeUtilization
	}

	return modeConnection
}

func prepBackendBucket(_ *InMemoryBackend, _ lbCtx, in *computeapi.BackendBucket, _ map[string]any) *APIError {
	if in.BucketName == "" {
		return errRequired("resource.bucketName")
	}

	if !bucketNameRE.MatchString(in.BucketName) {
		return errField("resource.bucketName", in.BucketName, "Not a valid Cloud Storage bucket name.")
	}

	if len(in.CustomResponseHeaders) > maxBucketHeaders {
		return errField("resource.customResponseHeaders", "", "At most 5 headers are allowed.")
	}

	for _, h := range in.CustomResponseHeaders {
		if !strings.Contains(h, ":") {
			return errField("resource.customResponseHeaders", h, "Must be formatted as 'Header-Name: value'.")
		}
	}

	switch in.CompressionMode {
	case "", "AUTOMATIC", "DISABLED":
	default:
		return errField("resource.compressionMode", in.CompressionMode, "")
	}

	if in.CdnPolicy != nil && !cacheModes[in.CdnPolicy.CacheMode] {
		return errField("resource.cdnPolicy.cacheMode", in.CdnPolicy.CacheMode, "")
	}

	return nil
}

func checkSignedKey(names []string, key *computeapi.SignedUrlKey) *APIError {
	if err := validName("keyName", key.KeyName); err != nil {
		return err
	}

	if !keyValueRE.MatchString(key.KeyValue) {
		return errField("keyValue", "", "Must be a base64url encoded 128-bit key.")
	}

	if slices.Contains(names, key.KeyName) {
		return errInvalid(
			"Invalid value for field 'keyName': '%s'. A signed URL key with this name exists.",
			key.KeyName,
		)
	}

	if len(names) >= maxSignedKeys {
		return errInvalid("At most %d signed URL keys are allowed.", maxSignedKeys)
	}

	return nil
}

func (b *InMemoryBackend) addBackendServiceKey(
	c lbCtx, name string, key *computeapi.SignedUrlKey,
) (*computeapi.Operation, error) {
	return lbMutate(
		b,
		bsSpec(),
		c,
		name,
		"addSignedUrlKey",
		nil,
		func(cur *computeapi.BackendService) (*computeapi.BackendService, *APIError) {
			if cur.CdnPolicy == nil {
				cur.CdnPolicy = &computeapi.BackendServiceCdnPolicy{}
			}

			if err := checkSignedKey(cur.CdnPolicy.SignedUrlKeyNames, key); err != nil {
				return nil, err
			}

			cur.CdnPolicy.SignedUrlKeyNames = append(cur.CdnPolicy.SignedUrlKeyNames, key.KeyName)

			return cur, nil
		},
	)
}

func (b *InMemoryBackend) deleteBackendServiceKey(c lbCtx, name, keyName string) (*computeapi.Operation, error) {
	return lbMutate(
		b,
		bsSpec(),
		c,
		name,
		"deleteSignedUrlKey",
		nil,
		func(cur *computeapi.BackendService) (*computeapi.BackendService, *APIError) {
			if cur.CdnPolicy == nil || !slices.Contains(cur.CdnPolicy.SignedUrlKeyNames, keyName) {
				return nil, errInvalid("Invalid value for field 'keyName': '%s'. No such signed URL key.", keyName)
			}

			cur.CdnPolicy.SignedUrlKeyNames = slices.DeleteFunc(
				cur.CdnPolicy.SignedUrlKeyNames,
				func(s string) bool { return s == keyName },
			)

			return cur, nil
		},
	)
}

func (b *InMemoryBackend) addBackendBucketKey(
	c lbCtx, name string, key *computeapi.SignedUrlKey,
) (*computeapi.Operation, error) {
	return lbMutate(
		b,
		bucketSpec(),
		c,
		name,
		"addSignedUrlKey",
		nil,
		func(cur *computeapi.BackendBucket) (*computeapi.BackendBucket, *APIError) {
			if cur.CdnPolicy == nil {
				cur.CdnPolicy = &computeapi.BackendBucketCdnPolicy{}
			}

			if err := checkSignedKey(cur.CdnPolicy.SignedUrlKeyNames, key); err != nil {
				return nil, err
			}

			cur.CdnPolicy.SignedUrlKeyNames = append(cur.CdnPolicy.SignedUrlKeyNames, key.KeyName)

			return cur, nil
		},
	)
}

func (b *InMemoryBackend) deleteBackendBucketKey(c lbCtx, name, keyName string) (*computeapi.Operation, error) {
	return lbMutate(
		b,
		bucketSpec(),
		c,
		name,
		"deleteSignedUrlKey",
		nil,
		func(cur *computeapi.BackendBucket) (*computeapi.BackendBucket, *APIError) {
			if cur.CdnPolicy == nil || !slices.Contains(cur.CdnPolicy.SignedUrlKeyNames, keyName) {
				return nil, errInvalid("Invalid value for field 'keyName': '%s'. No such signed URL key.", keyName)
			}

			cur.CdnPolicy.SignedUrlKeyNames = slices.DeleteFunc(
				cur.CdnPolicy.SignedUrlKeyNames,
				func(s string) bool { return s == keyName },
			)

			return cur, nil
		},
	)
}
