package gcpcompute

import (
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	collHealth         = "healthChecks"
	hcMaxPort          = 65535
	portHTTP           = 80
	portHTTPS          = 443
	hcMaxSec           = 300
	hcMaxThreshold     = 10
	hcDefaultSec       = 5
	hcDefaultThreshold = 2
	portFixed          = "USE_FIXED_PORT"
	portNamed          = "USE_NAMED_PORT"
	portServing        = "USE_SERVING_PORT"
)

type hcSpec struct {
	port      *int64
	portName  *string
	portSpec  *string
	proxy     *string
	path      *string
	typ       string
	field     string
	defPort   int64
	httpStyle bool
}

func hcSpecs(h *computeapi.HealthCheck) []hcSpec {
	var out []hcSpec

	if s := h.HttpHealthCheck; s != nil {
		out = append(out, hcSpec{
			typ:       "HTTP",
			field:     "httpHealthCheck",
			defPort:   portHTTP,
			httpStyle: true,
			port:      &s.Port,
			portName:  &s.PortName,
			portSpec:  &s.PortSpecification,
			proxy:     &s.ProxyHeader,
			path:      &s.RequestPath,
		})
	}

	if s := h.HttpsHealthCheck; s != nil {
		out = append(out, hcSpec{
			typ:       "HTTPS",
			field:     "httpsHealthCheck",
			defPort:   portHTTPS,
			httpStyle: true,
			port:      &s.Port,
			portName:  &s.PortName,
			portSpec:  &s.PortSpecification,
			proxy:     &s.ProxyHeader,
			path:      &s.RequestPath,
		})
	}

	if s := h.Http2HealthCheck; s != nil {
		out = append(out, hcSpec{
			typ:       "HTTP2",
			field:     "http2HealthCheck",
			defPort:   portHTTPS,
			httpStyle: true,
			port:      &s.Port,
			portName:  &s.PortName,
			portSpec:  &s.PortSpecification,
			proxy:     &s.ProxyHeader,
			path:      &s.RequestPath,
		})
	}

	if s := h.TcpHealthCheck; s != nil {
		out = append(out, hcSpec{
			typ: "TCP", field: "tcpHealthCheck", defPort: portHTTP,
			port: &s.Port, portName: &s.PortName, portSpec: &s.PortSpecification, proxy: &s.ProxyHeader,
		})
	}

	if s := h.SslHealthCheck; s != nil {
		out = append(out, hcSpec{
			typ: "SSL", field: "sslHealthCheck", defPort: portHTTPS,
			port: &s.Port, portName: &s.PortName, portSpec: &s.PortSpecification, proxy: &s.ProxyHeader,
		})
	}

	if s := h.GrpcHealthCheck; s != nil {
		out = append(out, hcSpec{
			typ: "GRPC", field: "grpcHealthCheck",
			port: &s.Port, portName: &s.PortName, portSpec: &s.PortSpecification,
		})
	}

	return out
}

func hcExactlyOne() *APIError {
	return errInvalid("Invalid value for field 'resource': ''. Exactly one of httpHealthCheck, httpsHealthCheck, " +
		"http2HealthCheck, tcpHealthCheck, sslHealthCheck or grpcHealthCheck must be specified.")
}

func validateSpec(sp hcSpec) *APIError {
	f := "resource." + sp.field

	if *sp.portName != "" && *sp.portSpec == "" {
		*sp.portSpec = portNamed
	} else if *sp.portSpec == "" {
		*sp.portSpec = portFixed
	}

	if err := validatePortSpec(sp, f); err != nil {
		return err
	}

	if *sp.port < 0 || *sp.port > hcMaxPort {
		return errField(f+".port", "", "Port must be in the range 1-65535.")
	}

	if sp.proxy != nil {
		if *sp.proxy == "" {
			*sp.proxy = "NONE"
		}

		if *sp.proxy != "NONE" && *sp.proxy != "PROXY_V1" {
			return errField(f+".proxyHeader", *sp.proxy, "")
		}
	}

	if sp.httpStyle {
		if *sp.path == "" {
			*sp.path = "/"
		}

		if !strings.HasPrefix(*sp.path, "/") {
			return errField(f+".requestPath", *sp.path, "Must start with '/'.")
		}
	}

	return nil
}

func validatePortSpec(sp hcSpec, f string) *APIError {
	switch *sp.portSpec {
	case portFixed:
		if *sp.portName != "" {
			return errField(f+".portName", *sp.portName, "portName is only valid with USE_NAMED_PORT.")
		}

		if *sp.port == 0 {
			if sp.defPort == 0 {
				return errRequired(f + ".port")
			}

			*sp.port = sp.defPort
		}
	case portNamed:
		if *sp.portName == "" {
			return errRequired(f + ".portName")
		}

		if *sp.port != 0 {
			return errField(f+".port", "", "port is not valid with USE_NAMED_PORT.")
		}
	case portServing:
		if *sp.port != 0 || *sp.portName != "" {
			return errField(
				f+".portSpecification",
				portServing,
				"port and portName are not valid with USE_SERVING_PORT.",
			)
		}
	default:
		return errField(f+".portSpecification", *sp.portSpec, "")
	}

	return nil
}

func hcRange(field string, v *int64, def, hi int64) *APIError {
	if *v == 0 {
		*v = def
	}

	if *v < 1 || *v > hi {
		return errInvalid("Invalid value for field '%s': '%d'. Must be between 1 and %d.", field, *v, hi)
	}

	return nil
}

func normalizeHealthCheck(h *computeapi.HealthCheck) *APIError {
	specs := hcSpecs(h)
	if len(specs) != 1 {
		return hcExactlyOne()
	}

	sp := specs[0]
	if h.Type == "" {
		h.Type = sp.typ
	}

	if h.Type != sp.typ {
		return errField("resource.type", h.Type, "Does not match the "+sp.field+" that was supplied.")
	}

	if err := validateSpec(sp); err != nil {
		return err
	}

	for _, e := range []*APIError{
		hcRange("resource.checkIntervalSec", &h.CheckIntervalSec, hcDefaultSec, hcMaxSec),
		hcRange("resource.timeoutSec", &h.TimeoutSec, hcDefaultSec, hcMaxSec),
		hcRange("resource.healthyThreshold", &h.HealthyThreshold, hcDefaultThreshold, hcMaxThreshold),
		hcRange("resource.unhealthyThreshold", &h.UnhealthyThreshold, hcDefaultThreshold, hcMaxThreshold),
	} {
		if e != nil {
			return e
		}
	}

	if h.TimeoutSec > h.CheckIntervalSec {
		return errInvalid("Invalid value for field 'resource.timeoutSec': '%d'. "+
			"Timeout sec must be less than or equal to check interval sec.", h.TimeoutSec)
	}

	return nil
}

func hcLoc(region string) string {
	if region == "" {
		return scopeGlobal
	}

	return region
}

func hcPath(project, region, name string) string {
	if region == "" {
		return relPath(project, scopeGlobal, "", collHealth, name)
	}

	return relPath(project, scopeRegion, region, collHealth, name)
}

func hcTarget(project, region string, h *computeapi.HealthCheck) target {
	t := target{project: project, scope: scopeGlobal, collection: collHealth, name: h.Name, id: h.Id}
	if region != "" {
		t.scope, t.loc = scopeRegion, region
	}

	return t
}

func (b *InMemoryBackend) insertHealthCheck(
	project, region string,
	in *computeapi.HealthCheck,
) (*computeapi.Operation, error) {
	b.mu.Lock("InsertHealthCheck")
	defer b.mu.Unlock()

	if err := validName("name", in.Name); err != nil {
		return nil, err
	}

	if _, dup := b.healths[key(project, hcLoc(region), in.Name)]; dup {
		return nil, errExists(hcPath(project, region, in.Name))
	}

	if err := normalizeHealthCheck(in); err != nil {
		return nil, err
	}

	in.Kind, in.Id, in.CreationTimestamp = "compute#healthCheck", newID(), b.stamp()

	if region == "" {
		in.SelfLink = globalLink(project, collHealth, in.Name)
	} else {
		in.Region = scopeURL(project, scopeRegion, region)
		in.SelfLink = regionLink(project, region, collHealth, in.Name)
	}

	b.healths[key(project, hcLoc(region), in.Name)] = in

	return b.newOpLocked("insert", hcTarget(project, region, in), b.timing.Done), nil
}

func (b *InMemoryBackend) getHealthCheck(project, region, name string) (*computeapi.HealthCheck, error) {
	b.mu.RLock("GetHealthCheck")
	defer b.mu.RUnlock()

	h, ok := b.healths[key(project, hcLoc(region), name)]
	if !ok {
		return nil, errNotFound(hcPath(project, region, name))
	}

	return clone(h), nil
}

func (b *InMemoryBackend) listHealthChecks(project, scope, loc string) []entry {
	b.mu.RLock("ListHealthChecks")
	defer b.mu.RUnlock()

	var out []entry

	for k, h := range b.healths {
		if !strings.HasPrefix(k, project+"/") {
			continue
		}

		hr := lastSegment(h.Region)
		if scope == scopeGlobal && hr != "" || scope == scopeRegion && hr != loc {
			continue
		}

		es := scopeGlobal
		if hr != "" {
			es = scopeRegion + "/" + hr
		}

		out = append(out, entry{scope: es, item: clone(h)})
	}

	return out
}

var hcSpecKeys = []string{ //nolint:gochecknoglobals // read-only lookup table
	"httpHealthCheck", "httpsHealthCheck", "http2HealthCheck", "tcpHealthCheck", "sslHealthCheck", "grpcHealthCheck",
}

func (b *InMemoryBackend) updateHealthCheck(
	project, region, name, verb string,
	raw map[string]any,
	replace *computeapi.HealthCheck,
) (*computeapi.Operation, error) {
	b.mu.Lock("UpdateHealthCheck")
	defer b.mu.Unlock()

	cur, ok := b.healths[key(project, hcLoc(region), name)]
	if !ok {
		return nil, errNotFound(hcPath(project, region, name))
	}

	next := replace
	if next == nil {
		doc := toDoc(cur)

		for _, k := range hcSpecKeys {
			if _, has := raw[k]; has {
				for _, other := range hcSpecKeys {
					delete(doc, other)
				}

				delete(doc, "type")

				break
			}
		}

		mergePatch(doc, raw)

		merged, err := fromDoc[computeapi.HealthCheck](doc)
		if err != nil {
			return nil, errInvalid("Invalid JSON payload received: %v", err)
		}

		next = merged
	}

	if next.Name != "" && next.Name != cur.Name {
		return nil, errField("resource.name", next.Name, "The name of a health check cannot be changed.")
	}

	next.Name, next.Id, next.Kind = cur.Name, cur.Id, cur.Kind
	next.CreationTimestamp, next.SelfLink, next.Region = cur.CreationTimestamp, cur.SelfLink, cur.Region

	if err := normalizeHealthCheck(next); err != nil {
		return nil, err
	}

	b.healths[key(project, hcLoc(region), name)] = next

	return b.newOpLocked(verb, hcTarget(project, region, next), b.timing.Done), nil
}

func (b *InMemoryBackend) deleteHealthCheck(project, region, name string) (*computeapi.Operation, error) {
	b.mu.Lock("DeleteHealthCheck")
	defer b.mu.Unlock()

	h, ok := b.healths[key(project, hcLoc(region), name)]
	if !ok {
		return nil, errNotFound(hcPath(project, region, name))
	}

	for _, m := range b.migs {
		for _, p := range m.Mgr.AutoHealingPolicies {
			if p.HealthCheck == h.SelfLink {
				return nil, errInUse("healthCheck", trimURL(h.SelfLink), trimURL(m.Mgr.SelfLink))
			}
		}
	}

	delete(b.healths, key(project, hcLoc(region), name))

	return b.newOpLocked("delete", hcTarget(project, region, h), b.timing.Done), nil
}
