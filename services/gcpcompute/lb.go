package gcpcompute

import (
	"slices"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	collBackendSvc    = "backendServices"
	collBackendBucket = "backendBuckets"
	collURLMaps       = "urlMaps"
	collHTTPProxy     = "targetHttpProxies"
	collHTTPSProxy    = "targetHttpsProxies"
	collTCPProxy      = "targetTcpProxies"
	collSSLProxy      = "targetSslProxies"
	collSSLCerts      = "sslCertificates"
	collSSLPolicies   = "sslPolicies"
	collFwdRules      = "forwardingRules"
	collNEG           = "networkEndpointGroups"
	collAddresses     = "addresses"
	opPrefix          = "compute."
)

// lbCtx is the location a load-balancing resource lives in.
type lbCtx struct {
	project string
	scope   string
	loc     string
}

func (c lbCtx) locKey() string {
	if c.scope == scopeGlobal {
		return scopeGlobal
	}

	return c.loc
}

func (c lbCtx) entryScope() string {
	if c.scope == scopeGlobal {
		return scopeGlobal
	}

	return c.scope + "/" + c.loc
}

// lbMeta points at the identity fields every load-balancing resource carries.
type lbMeta struct {
	name    *string
	kind    *string
	created *string
	self    *string
	region  *string
	zone    *string
	fp      *string
	id      *uint64
}

type lbSpec[T any] struct {
	m    func(b *InMemoryBackend) map[string]*T
	meta func(v *T) lbMeta
	prep func(b *InMemoryBackend, c lbCtx, in *T, raw map[string]any) *APIError
	refs func(v *T) []string
	view func(v *T)
	coll string
	kind string
	noun string
}

func lbTargetOf(c lbCtx, coll, name string, id uint64) target {
	return target{project: c.project, scope: c.scope, loc: c.loc, collection: coll, name: name, id: id}
}

func lbStamp(b *InMemoryBackend, c lbCtx, sp lbMeta, coll, name, kind string) {
	*sp.kind, *sp.id, *sp.created = kind, newID(), b.stamp()
	*sp.name = name
	*sp.self = link(c.project, c.scope, c.loc, coll, name)

	switch c.scope {
	case scopeRegion:
		if sp.region != nil {
			*sp.region = scopeURL(c.project, scopeRegion, c.loc)
		}
	case scopeZone:
		if sp.zone != nil {
			*sp.zone = scopeURL(c.project, scopeZone, c.loc)
		}
	}
}

func lbInsert[T any](
	b *InMemoryBackend,
	sp lbSpec[T],
	c lbCtx,
	in *T,
	raw map[string]any,
) (*computeapi.Operation, error) {
	b.mu.Lock("Insert" + sp.coll)
	defer b.mu.Unlock()

	mt := sp.meta(in)
	name := *mt.name

	if err := validName("name", name); err != nil {
		return nil, err
	}

	m := sp.m(b)
	if _, dup := m[key(c.project, c.locKey(), name)]; dup {
		return nil, errExists(relPath(c.project, c.scope, c.loc, sp.coll, name))
	}

	lbStamp(b, c, mt, sp.coll, name, sp.kind)

	if mt.fp != nil {
		*mt.fp = newFingerprint()
	}

	if err := sp.prep(b, c, in, raw); err != nil {
		return nil, err
	}

	m[key(c.project, c.locKey(), name)] = in

	return b.newOpLocked(opPrefix+sp.coll+".insert", lbTargetOf(c, sp.coll, name, *mt.id), b.timing.Done), nil
}

func lbGet[T any](b *InMemoryBackend, sp lbSpec[T], c lbCtx, name string) (*T, error) {
	b.mu.RLock("Get" + sp.coll)
	defer b.mu.RUnlock()

	v, ok := sp.m(b)[key(c.project, c.locKey(), name)]
	if !ok {
		return nil, errNotFound(relPath(c.project, c.scope, c.loc, sp.coll, name))
	}

	return lbView(sp, v), nil
}

func lbView[T any](sp lbSpec[T], v *T) *T {
	out := clone(v)
	if sp.view != nil {
		sp.view(out)
	}

	return out
}

// lbList returns the project's resources; an empty scope lists every location (aggregatedList).
func lbList[T any](b *InMemoryBackend, sp lbSpec[T], project, scope, loc string) []entry {
	b.mu.RLock("List" + sp.coll)
	defer b.mu.RUnlock()

	var out []entry

	for k, v := range sp.m(b) {
		if !strings.HasPrefix(k, project+"/") {
			continue
		}

		mt := sp.meta(v)
		es, eloc := lbLocOf(mt)

		if scope != "" && (es != scope || eloc != loc) {
			continue
		}

		if es != scopeGlobal {
			es += "/" + eloc
		}

		out = append(out, entry{scope: es, item: lbView(sp, v)})
	}

	return out
}

func lbLocOf(mt lbMeta) (string, string) {
	switch {
	case mt.zone != nil && *mt.zone != "":
		return scopeZone, lastSegment(*mt.zone)
	case mt.region != nil && *mt.region != "":
		return scopeRegion, lastSegment(*mt.region)
	}

	return scopeGlobal, ""
}

func lbDelete[T any](
	b *InMemoryBackend,
	sp lbSpec[T],
	c lbCtx,
	name string,
	extra func(b *InMemoryBackend, v *T) *APIError,
) (*computeapi.Operation, error) {
	b.mu.Lock("Delete" + sp.coll)
	defer b.mu.Unlock()

	m := sp.m(b)

	v, ok := m[key(c.project, c.locKey(), name)]
	if !ok {
		return nil, errNotFound(relPath(c.project, c.scope, c.loc, sp.coll, name))
	}

	mt := sp.meta(v)

	if err := b.lbInUseLocked(sp.noun, *mt.self); err != nil {
		return nil, err
	}

	if extra != nil {
		if err := extra(b, v); err != nil {
			return nil, err
		}
	}

	delete(m, key(c.project, c.locKey(), name))

	return b.newOpLocked(opPrefix+sp.coll+".delete", lbTargetOf(c, sp.coll, name, *mt.id), b.timing.Done), nil
}

// lbMutate applies edit to a copy of the stored resource, re-runs the validation and stores the result.
func lbMutate[T any](
	b *InMemoryBackend,
	sp lbSpec[T],
	c lbCtx,
	name, verb string,
	raw map[string]any,
	edit func(cur *T) (*T, *APIError),
) (*computeapi.Operation, error) {
	b.mu.Lock("Update" + sp.coll)
	defer b.mu.Unlock()

	m := sp.m(b)

	cur, ok := m[key(c.project, c.locKey(), name)]
	if !ok {
		return nil, errNotFound(relPath(c.project, c.scope, c.loc, sp.coll, name))
	}

	old := sp.meta(cur)

	next, err := edit(clone(cur))
	if err != nil {
		return nil, err
	}

	mt := sp.meta(next)
	if *mt.name != "" && *mt.name != *old.name {
		return nil, errField("resource.name", *mt.name, "The name of the resource cannot be changed.")
	}

	*mt.name, *mt.id, *mt.kind, *mt.created, *mt.self = *old.name, *old.id, *old.kind, *old.created, *old.self

	if mt.region != nil {
		*mt.region = *old.region
	}

	if mt.zone != nil {
		*mt.zone = *old.zone
	}

	if mt.fp != nil {
		*mt.fp = newFingerprint()
	}

	if perr := sp.prep(b, c, next, raw); perr != nil {
		return nil, perr
	}

	m[key(c.project, c.locKey(), name)] = next

	return b.newOpLocked(opPrefix+sp.coll+"."+verb, lbTargetOf(c, sp.coll, name, *mt.id), b.timing.Done), nil
}

func lbPatch[T any](
	b *InMemoryBackend,
	sp lbSpec[T],
	c lbCtx,
	name string,
	raw map[string]any,
) (*computeapi.Operation, error) {
	return lbMutate(b, sp, c, name, "patch", raw, func(cur *T) (*T, *APIError) {
		return mergeInto(cur, raw)
	})
}

// lbReplace is the PUT verb; resources with a fingerprint require the current one.
func lbReplace[T any](
	b *InMemoryBackend,
	sp lbSpec[T],
	c lbCtx,
	name string,
	in *T,
	raw map[string]any,
) (*computeapi.Operation, error) {
	return lbMutate(b, sp, c, name, "update", raw, func(cur *T) (*T, *APIError) {
		if fp := sp.meta(in).fp; fp != nil && *fp != *sp.meta(cur).fp {
			return nil, errPrecondition("Supplied fingerprint does not match current fingerprint.")
		}

		return in, nil
	})
}

// lbResolve resolves a same-scope reference to an existing resource and returns its selfLink.
func lbResolve[T any](b *InMemoryBackend, sp lbSpec[T], c lbCtx, field, raw string) (string, *APIError) {
	r, ok := parseRef(raw, sp.coll, c.project, c.loc)
	if !ok {
		return "", errField(field, raw, "The URL is malformed.")
	}

	switch c.scope {
	case scopeGlobal:
		if r.scope != "" && r.scope != scopeGlobal {
			return "", errField(field, raw, "Must be a global "+sp.noun+".")
		}
	default:
		if r.scope != "" && r.scope != c.scope || r.loc != c.loc {
			return "", errField(field, raw, "Must be a "+sp.noun+" in "+c.loc+".")
		}
	}

	v, found := sp.m(b)[key(r.project, c.locKey(), r.name)]
	if !found {
		return "", errNotFound(relPath(r.project, c.scope, c.loc, sp.coll, r.name))
	}

	return *sp.meta(v).self, nil
}

func lbResolveAll[T any](
	b *InMemoryBackend,
	sp lbSpec[T],
	c lbCtx,
	field string,
	raws []string,
) ([]string, *APIError) {
	out := make([]string, 0, len(raws))

	for _, raw := range raws {
		l, err := lbResolve(b, sp, c, field, raw)
		if err != nil {
			return nil, err
		}

		if slices.Contains(out, l) {
			return nil, errField(field, raw, "Duplicate reference.")
		}

		out = append(out, l)
	}

	return out, nil
}

func lbCollectUsers[T any](b *InMemoryBackend, sp lbSpec[T], self string, out *[]string) {
	if sp.refs == nil {
		return
	}

	for _, v := range sp.m(b) {
		if slices.Contains(sp.refs(v), self) {
			*out = append(*out, *sp.meta(v).self)
		}
	}
}

// lbUsersLocked returns the selfLinks of every resource that references self, sorted.
func (b *InMemoryBackend) lbUsersLocked(self string) []string {
	var users []string

	lbCollectUsers(b, bsSpec(), self, &users)
	lbCollectUsers(b, urlMapSpec(), self, &users)
	lbCollectUsers(b, httpProxySpec(), self, &users)
	lbCollectUsers(b, httpsProxySpec(), self, &users)
	lbCollectUsers(b, tcpProxySpec(), self, &users)
	lbCollectUsers(b, sslProxySpec(), self, &users)
	lbCollectUsers(b, fwdRuleSpec(), self, &users)

	for _, m := range b.migs {
		for _, p := range m.Mgr.AutoHealingPolicies {
			if p.HealthCheck == self {
				users = append(users, m.Mgr.SelfLink)
			}
		}
	}

	slices.Sort(users)

	return users
}

func (b *InMemoryBackend) lbInUseLocked(noun, self string) *APIError {
	if users := b.lbUsersLocked(self); len(users) > 0 {
		return errInUse(noun, trimURL(self), trimURL(users[0]))
	}

	return nil
}
