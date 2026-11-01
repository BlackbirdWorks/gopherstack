package azurearm

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpscale"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
)

// acaHostTarget is what an ingress host name resolves to: a fixed revision, a label, or (both empty) the app's
// traffic split.
type acaHostTarget struct {
	AppKey   string
	Revision string
	Label    string
}

type acaRoute struct {
	revision string
	weight   int
}

// routesOf returns the active revisions that receive traffic and their weights.
func (p *ContainerAppsProvider) routesOf(a *storedACAApp) []acaRoute {
	latest := a.latest()
	if latest == nil {
		return nil
	}

	if a.mode() == acaModeSingle {
		if r := p.readyRevision(a); r != "" {
			return []acaRoute{{r, acaPercent}}
		}

		return nil
	}

	entries := acaTrafficOf(a.Config)
	if len(entries) == 0 {
		entries = []acaTrafficEntry{{latest: true, weight: acaPercent}}
	}

	var out []acaRoute

	for _, e := range entries {
		name := e.revision

		switch {
		case e.latest:
			name = latest.Name
		case name == "":
			name = acaLabelRevision(a, e.label)
		}

		if r := a.revision(name); r != nil && r.Active && e.weight > 0 {
			out = append(out, acaRoute{r.Name, e.weight})
		}
	}

	return out
}

func acaLabelRevision(a *storedACAApp, label string) string {
	for _, e := range acaTrafficOf(a.Config) {
		if e.label != label {
			continue
		}

		switch {
		case e.latest && a.latest() != nil:
			return a.latest().Name
		case e.revision != "":
			return e.revision
		}
	}

	return ""
}

func (p *ContainerAppsProvider) rebuildHostsLocked() {
	hosts := map[string]acaHostTarget{}

	for key, a := range p.apps {
		env, in := p.envByID(a.EnvironmentID), a.ingress()
		if env == nil || in == nil {
			continue
		}

		ext := acaBool(in, "external")
		hosts[acaHostFor(a.Name, env.DefaultDomain, ext)] = acaHostTarget{AppKey: key}

		for _, r := range a.Revisions {
			if r.Active {
				hosts[acaHostFor(r.Name, env.DefaultDomain, ext)] = acaHostTarget{AppKey: key, Revision: r.Name}
			}
		}

		for _, e := range acaTrafficOf(a.Config) {
			if e.label != "" {
				hosts[acaHostFor(a.Name+"---"+e.label, env.DefaultDomain, ext)] = acaHostTarget{
					AppKey: key,
					Label:  e.label,
				}
			}
		}
	}

	p.hosts = hosts
}

// ServeIngress proxies a request addressed to a container app host name; it reports false for any other host.
func (p *ContainerAppsProvider) ServeIngress(w http.ResponseWriter, r *http.Request) bool {
	host := strings.ToLower(hostFromHostHeader(r.Host))

	p.mu.RLock("ServeIngress")
	t, ok := p.hosts[host]
	p.mu.RUnlock()

	if !ok {
		return false
	}

	rev, status, msg := p.pickRevision(t)
	if rev == "" {
		http.Error(w, msg, status)

		return true
	}

	key := acaRevKey(p.appByKey(t.AppKey), rev)

	rep, release, err := p.rt.acquire(r.Context(), key, func() (*acaRevSpec, error) { return p.specFor(t.AppKey, rev) })
	if err != nil {
		status = http.StatusServiceUnavailable
		if errors.Is(err, gcpscale.ErrSaturated) {
			status = http.StatusTooManyRequests
		}

		http.Error(w, "no replica could serve the request: "+err.Error(), status)

		return true
	}
	defer release()

	if err = rep.waitReady(); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)

		return true
	}

	gcpserve.Proxy(rep.Port, p.rt.transport).ServeHTTP(w, r)

	return true
}

func (p *ContainerAppsProvider) appByKey(key string) *storedACAApp {
	p.mu.RLock("appByKey")
	defer p.mu.RUnlock()

	if a := p.apps[key]; a != nil {
		return a
	}

	return &storedACAApp{}
}

func (p *ContainerAppsProvider) specFor(appKey, rev string) (*acaRevSpec, error) {
	p.mu.RLock("specFor")
	defer p.mu.RUnlock()

	a := p.apps[appKey]
	if a == nil {
		return nil, ErrResourceNotFound
	}

	r := a.revision(rev)
	if r == nil || !r.Active || a.Stopped {
		return nil, ErrResourceNotFound
	}

	return p.buildSpec(a, r)
}

// pickRevision chooses the revision that serves a request to t, drawing against the traffic weights.
func (p *ContainerAppsProvider) pickRevision(t acaHostTarget) (string, int, string) {
	p.mu.RLock("pickRevision")
	a := p.apps[t.AppKey]

	if a == nil || a.Stopped {
		p.mu.RUnlock()

		return "", http.StatusNotFound, "The container app is not running."
	}

	rev := t.Revision

	if t.Label != "" {
		rev = acaLabelRevision(a, t.Label)
	}

	var routes []acaRoute
	if rev == "" {
		routes = p.routesOf(a)
	}
	p.mu.RUnlock()

	if rev != "" {
		return rev, 0, ""
	}

	total := 0
	for _, r := range routes {
		total += r.weight
	}

	if total == 0 {
		return "", http.StatusServiceUnavailable, "The container app has no active revision receiving traffic."
	}

	n := p.rt.draw(total)
	for _, r := range routes {
		if n < r.weight {
			return r.revision, 0, ""
		}

		n -= r.weight
	}

	return routes[len(routes)-1].revision, 0, ""
}

func (p *ContainerAppsProvider) revisionGet(id ResourceID, t acaTarget) (map[string]any, error) {
	a, ok := p.apps[acaKey(id.ResourceGroup, t.names[0])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	r := a.revision(t.names[1])
	if r == nil {
		return nil, ErrResourceNotFound
	}

	if t.kind == acaKindRevision {
		return p.revisionBody(id, a, r), nil
	}

	for _, rep := range p.rt.replicasOf(acaRevKey(a, r.Name)) {
		if strings.EqualFold(rep.Name, t.names[2]) {
			return p.replicaBody(id, a, r, rep), nil
		}
	}

	return nil, ErrResourceNotFound
}

func (p *ContainerAppsProvider) listRevisions(id ResourceID, t acaTarget) ([]map[string]any, error) {
	a, ok := p.apps[acaKey(id.ResourceGroup, t.names[0])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	out := make([]map[string]any, 0, len(a.Revisions))

	if t.kind == acaKindRevision {
		for _, r := range a.Revisions {
			out = append(out, p.revisionBody(id, a, r))
		}

		return out, nil
	}

	r := a.revision(t.names[1])
	if r == nil {
		return nil, ErrResourceNotFound
	}

	for _, rep := range p.rt.replicasOf(acaRevKey(a, r.Name)) {
		out = append(out, p.replicaBody(id, a, r, rep))
	}

	return out, nil
}

func (p *ContainerAppsProvider) revisionBody(id ResourceID, a *storedACAApp, r *storedACARevision) map[string]any {
	key := acaRevKey(a, r.Name)
	failure := p.rt.failureOf(key)
	replicas := len(p.rt.replicasOf(key))

	weight := 0

	for _, rt := range p.routesOf(a) {
		if strings.EqualFold(rt.revision, r.Name) {
			weight = rt.weight
		}
	}

	prov, running, health := "Provisioned", aciStateRunning, "Healthy"

	switch {
	case !r.Active:
		prov, running, health = "Deprovisioned", aciStateStopped, acaHealthNone
	case a.Stopped:
		running, health = aciStateStopped, acaHealthNone
	case failure != "":
		prov, running, health = aciStateFailed, aciStateFailed, "Unhealthy"
	}

	props := map[string]any{
		"active": r.Active, "createdTime": r.Created.Format(time.RFC3339Nano), "provisioningState": prov,
		acaFieldRunning: running, "healthState": health, "replicas": replicas, "trafficWeight": weight,
		fieldTemplate: acaTemplateOut(r.Template),
	}

	if r.Active {
		props["lastActiveTime"] = r.LastActive.Format(time.RFC3339Nano)
	}

	if failure != "" {
		props["provisioningError"] = failure
	}

	if env, in := p.envByID(a.EnvironmentID), a.ingress(); env != nil && in != nil {
		props["fqdn"] = acaHostFor(r.Name, env.DefaultDomain, acaBool(in, "external"))
	}

	rid := acaResourceID(
		id.SubscriptionID,
		a.ResourceGroup,
		[]string{acaAppType, acaRevisionType},
		[]string{a.Name, r.Name},
	)

	return map[string]any{
		"id":               rid.ARMID(),
		fieldName:          r.Name,
		fieldType:          namespaceMicrosoftApp + "/" + acaAppType + "/" + acaRevisionType,
		fieldProperties:    props,
		acaFieldSystemData: acaSystemData(r.Created, r.Created),
	}
}

func (p *ContainerAppsProvider) replicaBody(
	id ResourceID, a *storedACAApp, r *storedACARevision, rep *acaReplica,
) map[string]any {
	view, live := p.rt.engine.View(rep.Key)

	state, details := aciStateRunning, ""

	conts := make([]map[string]any, 0, len(view.Containers))

	for _, c := range view.Containers {
		ready := c.Cur.State == aciStateRunning
		if !ready && state == aciStateRunning {
			state, details = "NotRunning", fmt.Sprintf("container %s is %s", c.Name, strings.ToLower(c.Cur.State))
		}

		conts = append(conts, map[string]any{
			fieldName: c.Name, "ready": ready, "started": ready, "restartCount": c.Restarts,
			acaFieldRunning: c.Cur.State, "runningStateDetails": c.Cur.Detail,
		})
	}

	if !live {
		state, details = "Unknown", "the container engine has no record of this replica"
	}

	props := map[string]any{
		"createdTime": rep.Created.Format(time.RFC3339Nano), acaFieldRunning: state, fieldContainers: conts,
	}

	if details != "" {
		props["runningStateDetails"] = details
	}

	rid := acaResourceID(id.SubscriptionID, a.ResourceGroup,
		[]string{acaAppType, acaRevisionType, acaReplicaType}, []string{a.Name, r.Name, rep.Name})

	return map[string]any{
		"id": rid.ARMID(), fieldName: rep.Name,
		fieldType:       namespaceMicrosoftApp + "/" + acaAppType + "/" + acaRevisionType + "/" + acaReplicaType,
		fieldProperties: props, acaFieldSystemData: acaSystemData(rep.Created, rep.Created),
	}
}
