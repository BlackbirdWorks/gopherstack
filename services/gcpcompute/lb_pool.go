package gcpcompute

import (
	"slices"
	"strconv"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	collTargetPools     = "targetPools"
	collTargetInstances = "targetInstances"
	collHTTPHealth      = "httpHealthChecks"
	collHTTPSHealth     = "httpsHealthChecks"
	affinityClientIP    = "CLIENT_IP"
	affinityClientProto = "CLIENT_IP_PROTO"
	natNoNat            = "NO_NAT"
	legacyHCPath        = "/"
	legacyHCSeconds     = 5
	legacyHCThreshold   = 2
)

func targetPoolSpec() lbSpec[computeapi.TargetPool] {
	return lbSpec[computeapi.TargetPool]{
		coll: collTargetPools, kind: "compute#targetPool", noun: "target_pool",
		m: func(b *InMemoryBackend) map[string]*computeapi.TargetPool { return b.targetPools },
		meta: func(v *computeapi.TargetPool) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink,
				region: &v.Region, id: &v.Id,
			}
		},
		prep: prepTargetPool,
		refs: func(v *computeapi.TargetPool) []string {
			out := append(slices.Clone(v.HealthChecks), v.BackupPool)
			if v.SecurityPolicy != "" {
				out = append(out, v.SecurityPolicy)
			}

			return out
		},
	}
}

func targetInstanceSpec() lbSpec[computeapi.TargetInstance] {
	return lbSpec[computeapi.TargetInstance]{
		coll: collTargetInstances, kind: "compute#targetInstance", noun: "target_instance",
		m: func(b *InMemoryBackend) map[string]*computeapi.TargetInstance { return b.targetInstances },
		meta: func(v *computeapi.TargetInstance) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink,
				zone: &v.Zone, id: &v.Id,
			}
		},
		prep: prepTargetInstance,
	}
}

func legacyHealthSpec[T any](
	coll, kind, noun string,
	m func(b *InMemoryBackend) map[string]*T,
	meta func(v *T) lbMeta,
	prep func(v *T) *APIError,
) lbSpec[T] {
	return lbSpec[T]{
		coll: coll, kind: kind, noun: noun, m: m, meta: meta,
		prep: func(_ *InMemoryBackend, _ lbCtx, in *T, _ map[string]any) *APIError { return prep(in) },
	}
}

func httpHealthSpec() lbSpec[computeapi.HttpHealthCheck] {
	return legacyHealthSpec(collHTTPHealth, "compute#httpHealthCheck", "http_health_check",
		func(b *InMemoryBackend) map[string]*computeapi.HttpHealthCheck { return b.httpHealths },
		func(v *computeapi.HttpHealthCheck) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink, id: &v.Id,
			}
		},
		func(in *computeapi.HttpHealthCheck) *APIError {
			return prepLegacyHealth(&in.Port, portHTTP, &in.RequestPath, &in.CheckIntervalSec, &in.TimeoutSec,
				&in.HealthyThreshold, &in.UnhealthyThreshold)
		})
}

func httpsHealthSpec() lbSpec[computeapi.HttpsHealthCheck] {
	return legacyHealthSpec(collHTTPSHealth, "compute#httpsHealthCheck", "https_health_check",
		func(b *InMemoryBackend) map[string]*computeapi.HttpsHealthCheck { return b.httpsHealths },
		func(v *computeapi.HttpsHealthCheck) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink, id: &v.Id,
			}
		},
		func(in *computeapi.HttpsHealthCheck) *APIError {
			return prepLegacyHealth(&in.Port, portHTTPS, &in.RequestPath, &in.CheckIntervalSec, &in.TimeoutSec,
				&in.HealthyThreshold, &in.UnhealthyThreshold)
		})
}

func prepLegacyHealth(
	port *int64,
	defPort int64,
	path *string,
	interval, timeout, healthy, unhealthy *int64,
) *APIError {
	if *port == 0 {
		*port = defPort
	}

	if *port < 0 || *port > hcMaxPort {
		return errField("resource.port", "", "Port must be in the range 1-65535.")
	}

	if *path == "" {
		*path = legacyHCPath
	}

	if !strings.HasPrefix(*path, "/") {
		return errField("resource.requestPath", *path, "Must start with '/'.")
	}

	for field, p := range map[string]*int64{
		"checkIntervalSec": interval, "timeoutSec": timeout,
		"healthyThreshold": healthy, "unhealthyThreshold": unhealthy,
	} {
		if *p < 0 {
			return errField("resource."+field, "", "Must not be negative.")
		}
	}

	setDefault(interval, legacyHCSeconds)
	setDefault(timeout, legacyHCSeconds)
	setDefault(healthy, legacyHCThreshold)
	setDefault(unhealthy, legacyHCThreshold)

	if *timeout > *interval {
		return errInvalid("Invalid value for field 'resource.timeoutSec': timeoutSec cannot be greater than " +
			"checkIntervalSec.")
	}

	return nil
}

const (
	instanceTailSegs = 4
	healthTailSegs   = 2
)

// sameTail compares two references by their trailing path segments so partial and full URLs match.
func sameTail(a, b string, n int) bool {
	tail := func(s string) string {
		parts := strings.Split(trimURL(s), "/")

		return strings.Join(parts[max(0, len(parts)-n):], "/")
	}

	return tail(a) == tail(b)
}

func setDefault(p *int64, def int64) {
	if *p == 0 {
		*p = def
	}
}

// resolveZonalInstanceLocked resolves an instance reference to its selfLink; region, when set, bounds the zone.
func (b *InMemoryBackend) resolveZonalInstanceLocked(
	project, region, field, raw string,
) (string, *instRec, *APIError) {
	r, err := refLocated(raw, "instances", scopeZone, project, "")
	if err != nil {
		return "", nil, errField(field, raw, "The URL is malformed.")
	}

	if region != "" && regionOfZone(r.loc) != region {
		return "", nil, errField(field, raw, "The instance must be in a zone of region "+region+".")
	}

	rec, ok := b.instances[key(r.project, r.loc, r.name)]
	if !ok {
		return "", nil, errNotFound(relPath(r.project, scopeZone, r.loc, "instances", r.name))
	}

	return rec.Inst.SelfLink, rec, nil
}

func (b *InMemoryBackend) resolveLegacyHealthLocked(c lbCtx, field, raw string) (string, *APIError) {
	g := lbCtx{project: c.project, scope: scopeGlobal}

	return lbResolve(b, httpHealthSpec(), g, field, raw)
}

func prepTargetPool(b *InMemoryBackend, c lbCtx, in *computeapi.TargetPool, _ map[string]any) *APIError {
	switch in.SessionAffinity {
	case "":
		in.SessionAffinity = affinityNone
	case affinityNone, affinityClientIP, affinityClientProto:
	default:
		return errField("resource.sessionAffinity", in.SessionAffinity, "")
	}

	if in.FailoverRatio < 0 || in.FailoverRatio > 1 {
		return errField("resource.failoverRatio", "", "The value must be in [0, 1].")
	}

	if in.FailoverRatio != 0 && in.BackupPool == "" {
		return errField("resource.failoverRatio", "", "backupPool must be set together with failoverRatio.")
	}

	if len(in.HealthChecks) > 1 {
		return errField("resource.healthChecks", "", "Only one health check may be specified.")
	}

	for i, hc := range in.HealthChecks {
		l, err := b.resolveLegacyHealthLocked(c, "resource.healthChecks", hc)
		if err != nil {
			return err
		}

		in.HealthChecks[i] = l
	}

	links := make([]string, 0, len(in.Instances))

	for _, raw := range in.Instances {
		l, _, err := b.resolveZonalInstanceLocked(c.project, c.loc, "resource.instances", raw)
		if err != nil {
			return err
		}

		if slices.Contains(links, l) {
			return errField("resource.instances", raw, "Duplicate reference.")
		}

		links = append(links, l)
	}

	in.Instances = links

	return b.prepBackupPoolLocked(c, in)
}

func (b *InMemoryBackend) prepBackupPoolLocked(c lbCtx, in *computeapi.TargetPool) *APIError {
	if in.BackupPool == "" {
		return nil
	}

	l, err := lbResolve(b, targetPoolSpec(), c, "resource.backupPool", in.BackupPool)
	if err != nil {
		return err
	}

	if l == in.SelfLink {
		return errField("resource.backupPool", in.BackupPool, "A target pool cannot be its own backup pool.")
	}

	in.BackupPool = l

	return nil
}

func prepTargetInstance(
	b *InMemoryBackend, c lbCtx, in *computeapi.TargetInstance, _ map[string]any,
) *APIError {
	if in.NatPolicy == "" {
		in.NatPolicy = natNoNat
	}

	if in.NatPolicy != natNoNat {
		return errField("resource.natPolicy", in.NatPolicy, "Must have a value of NO_NAT.")
	}

	if in.Instance == "" {
		return errRequired("resource.instance")
	}

	l, rec, err := b.resolveZonalInstanceLocked(c.project, "", "resource.instance", in.Instance)
	if err != nil {
		return err
	}

	if lastSegment(rec.Inst.Zone) != c.loc {
		return errField("resource.instance", in.Instance, "The instance must be in zone "+c.loc+".")
	}

	in.Instance = l

	netLink := ""
	if len(rec.Inst.NetworkInterfaces) > 0 {
		netLink = rec.Inst.NetworkInterfaces[0].Network
	}

	if in.Network == "" {
		in.Network = netLink

		return nil
	}

	n, nerr := b.resolveNetworkLocked(c.project, in.Network)
	if nerr != nil {
		return nerr
	}

	in.Network = n.SelfLink

	return nil
}

func (b *InMemoryBackend) targetPoolHealth(
	c lbCtx,
	name, instance string,
) (*computeapi.TargetPoolInstanceHealth, error) {
	b.mu.RLock("GetTargetPoolHealth")
	defer b.mu.RUnlock()

	pool, ok := b.targetPools[key(c.project, c.loc, name)]
	if !ok {
		return nil, errNotFound(relPath(c.project, c.scope, c.loc, collTargetPools, name))
	}

	link, rec, err := b.resolveZonalInstanceLocked(c.project, c.loc, "instance", instance)
	if err != nil {
		return nil, err
	}

	if !slices.Contains(pool.Instances, link) {
		return nil, errInvalid("Invalid value for field 'instance': '%s'. The instance is not in target pool '%s'.",
			instance, name)
	}

	hs := &computeapi.HealthStatus{
		Instance:    link,
		Port:        legacyPoolPort(b, pool),
		HealthState: healthStateFor(len(pool.HealthChecks) > 0, instRunning(rec, b.now().UTC(), b.timing)),
	}

	if len(rec.Inst.NetworkInterfaces) > 0 {
		hs.IpAddress = rec.Inst.NetworkInterfaces[0].NetworkIP
	}

	return &computeapi.TargetPoolInstanceHealth{
		Kind: "compute#targetPoolInstanceHealth", HealthStatus: []*computeapi.HealthStatus{hs},
	}, nil
}

func legacyPoolPort(b *InMemoryBackend, pool *computeapi.TargetPool) int64 {
	if len(pool.HealthChecks) == 0 {
		return portHTTP
	}

	if hc := lbFind(b, httpHealthSpec(), pool.HealthChecks[0]); hc != nil {
		return hc.Port
	}

	return portHTTP
}

type poolInstancesRequest struct {
	Instances []*computeapi.InstanceReference `json:"instances"`
}

type poolHealthChecksRequest struct {
	HealthChecks []*computeapi.HealthCheckReference `json:"healthChecks"`
}

func (b *InMemoryBackend) poolAddInstances(
	c lbCtx,
	name string,
	in *poolInstancesRequest,
) (*computeapi.Operation, error) {
	return lbMutate(b, targetPoolSpec(), c, name, "addInstance", nil,
		func(cur *computeapi.TargetPool) (*computeapi.TargetPool, *APIError) {
			if len(in.Instances) == 0 {
				return nil, errRequired("instances")
			}

			for _, r := range in.Instances {
				if slices.ContainsFunc(
					cur.Instances,
					func(s string) bool { return sameTail(s, r.Instance, instanceTailSegs) },
				) {
					return nil, errField("instances", r.Instance, "The instance is already in the target pool.")
				}

				cur.Instances = append(cur.Instances, r.Instance)
			}

			return cur, nil
		})
}

func (b *InMemoryBackend) poolRemoveInstances(
	c lbCtx, name string, in *poolInstancesRequest,
) (*computeapi.Operation, error) {
	return lbMutate(b, targetPoolSpec(), c, name, "removeInstance", nil,
		func(cur *computeapi.TargetPool) (*computeapi.TargetPool, *APIError) {
			for _, r := range in.Instances {
				i := slices.IndexFunc(
					cur.Instances,
					func(s string) bool { return sameTail(s, r.Instance, instanceTailSegs) },
				)
				if i < 0 {
					return nil, errNotFound(trimURL(r.Instance))
				}

				cur.Instances = slices.Delete(cur.Instances, i, i+1)
			}

			return cur, nil
		})
}

func (b *InMemoryBackend) poolAddHealthChecks(
	c lbCtx, name string, in *poolHealthChecksRequest,
) (*computeapi.Operation, error) {
	return lbMutate(b, targetPoolSpec(), c, name, "addHealthCheck", nil,
		func(cur *computeapi.TargetPool) (*computeapi.TargetPool, *APIError) {
			for _, r := range in.HealthChecks {
				cur.HealthChecks = append(cur.HealthChecks, r.HealthCheck)
			}

			return cur, nil
		})
}

func (b *InMemoryBackend) poolRemoveHealthChecks(
	c lbCtx, name string, in *poolHealthChecksRequest,
) (*computeapi.Operation, error) {
	return lbMutate(b, targetPoolSpec(), c, name, "removeHealthCheck", nil,
		func(cur *computeapi.TargetPool) (*computeapi.TargetPool, *APIError) {
			for _, r := range in.HealthChecks {
				i := slices.IndexFunc(
					cur.HealthChecks,
					func(s string) bool { return sameTail(s, r.HealthCheck, healthTailSegs) },
				)
				if i < 0 {
					return nil, errNotFound(trimURL(r.HealthCheck))
				}

				cur.HealthChecks = slices.Delete(cur.HealthChecks, i, i+1)
			}

			return cur, nil
		})
}

func (b *InMemoryBackend) poolSetBackup(
	c lbCtx, name, ratio string, in *computeapi.TargetReference,
) (*computeapi.Operation, error) {
	return lbMutate(b, targetPoolSpec(), c, name, "setBackup", nil,
		func(cur *computeapi.TargetPool) (*computeapi.TargetPool, *APIError) {
			cur.BackupPool = in.Target
			cur.FailoverRatio = 0

			if ratio != "" {
				f, err := strconv.ParseFloat(ratio, 64)
				if err != nil {
					return nil, errField("failoverRatio", ratio, "")
				}

				cur.FailoverRatio = f
			}

			return cur, nil
		})
}

func (h *Handler) registerPoolOps() {
	regional := []string{scopeRegion}

	registerLB(h, targetPoolSpec(), lbOpts[computeapi.TargetPool]{scopes: regional, agg: regionScopes()})
	registerLB(
		h,
		targetInstanceSpec(),
		lbOpts[computeapi.TargetInstance]{scopes: []string{scopeZone}, agg: zoneScopes()},
	)
	registerLB(h, httpHealthSpec(), lbOpts[computeapi.HttpHealthCheck]{
		scopes: []string{scopeGlobal}, patch: true, put: true,
	})
	registerLB(h, httpsHealthSpec(), lbOpts[computeapi.HttpsHealthCheck]{
		scopes: []string{scopeGlobal}, patch: true, put: true,
	})

	h.reg(collTargetPools, scopeRegion, "addInstance", poolAction((*InMemoryBackend).poolAddInstances))
	h.reg(collTargetPools, scopeRegion, "removeInstance", poolAction((*InMemoryBackend).poolRemoveInstances))
	h.reg(collTargetPools, scopeRegion, "addHealthCheck", poolAction((*InMemoryBackend).poolAddHealthChecks))
	h.reg(collTargetPools, scopeRegion, "removeHealthCheck", poolAction((*InMemoryBackend).poolRemoveHealthChecks))
	h.reg(collTargetPools, scopeRegion, "setBackup", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.TargetReference](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.poolSetBackup(ctxOf(rq), rq.rt.name, rq.q.Get("failoverRatio"), in))
	})
	h.reg(collTargetPools, scopeRegion, "setSecurityPolicy", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.SecurityPolicyReference](rq)
		if err != nil {
			return nil, err
		}

		return opResult(h.Backend.poolSetSecurityPolicy(ctxOf(rq), rq.rt.name, in.SecurityPolicy))
	})
	h.reg(collTargetPools, scopeRegion, "getHealth", func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[computeapi.InstanceReference](rq)
		if err != nil {
			return nil, err
		}

		return h.Backend.targetPoolHealth(ctxOf(rq), rq.rt.name, in.Instance)
	})
}

func poolAction[R any](
	fn func(b *InMemoryBackend, c lbCtx, name string, in *R) (*computeapi.Operation, error),
) handlerFn {
	return func(h *Handler, rq *request) (any, error) {
		in, _, err := decode[R](rq)
		if err != nil {
			return nil, err
		}

		return opResult(fn(h.Backend, ctxOf(rq), rq.rt.name, in))
	}
}

func (b *InMemoryBackend) poolSetSecurityPolicy(c lbCtx, name, policy string) (*computeapi.Operation, error) {
	return lbMutate(b, targetPoolSpec(), c, name, "setSecurityPolicy", nil,
		func(cur *computeapi.TargetPool) (*computeapi.TargetPool, *APIError) {
			if policy == "" {
				cur.SecurityPolicy = ""

				return cur, nil
			}

			self, err := lbResolve(b, secPolicySpec(), lbCtx{project: c.project, scope: scopeGlobal},
				"securityPolicyReference.securityPolicy", policy)
			if err != nil {
				return nil, err
			}

			cur.SecurityPolicy = self

			return cur, nil
		})
}
