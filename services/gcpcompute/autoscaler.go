package gcpcompute

import (
	"slices"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	collScalers      = "autoscalers"
	opScaler         = "compute.autoscalers."
	defaultCoolDown  = 60
	defaultCPUTarget = 0.6
	scalerActive     = "ACTIVE"
)

var scalerModes = []string{"ON", "OFF", "ONLY_UP", "ONLY_SCALE_OUT"} //nolint:gochecknoglobals // read-only lookup table

func scalerPath(project, scope, loc, name string) string {
	return relPath(project, scope, loc, collScalers, name)
}

func scalerScope(a *computeapi.Autoscaler) (string, string) {
	if a.Region != "" {
		return scopeRegion, lastSegment(a.Region)
	}

	return scopeZone, lastSegment(a.Zone)
}

func scalerTarget(project string, a *computeapi.Autoscaler) target {
	scope, loc := scalerScope(a)

	return target{project: project, scope: scope, loc: loc, collection: collScalers, name: a.Name, id: a.Id}
}

func normalizePolicy(p *computeapi.AutoscalingPolicy) *APIError {
	if p == nil {
		return errRequired("resource.autoscalingPolicy")
	}

	if p.MaxNumReplicas < 1 {
		return errField("resource.autoscalingPolicy.maxNumReplicas", "", "Must be at least 1.")
	}

	if p.MinNumReplicas == 0 {
		p.MinNumReplicas = 1
	}

	if p.MinNumReplicas < 0 || p.MinNumReplicas > p.MaxNumReplicas {
		return errInvalid("Invalid value for field 'resource.autoscalingPolicy.minNumReplicas': " +
			"must be between 0 and maxNumReplicas.")
	}

	if p.CoolDownPeriodSec == 0 {
		p.CoolDownPeriodSec = defaultCoolDown
	}

	if p.CoolDownPeriodSec < 0 {
		return errField("resource.autoscalingPolicy.coolDownPeriodSec", "", "Must not be negative.")
	}

	if p.Mode == "" {
		p.Mode = "ON"
	}

	if !slices.Contains(scalerModes, p.Mode) {
		return errField("resource.autoscalingPolicy.mode", p.Mode, "")
	}

	return validateSignals(p)
}

func validateSignals(p *computeapi.AutoscalingPolicy) *APIError {
	if c := p.CpuUtilization; c != nil && (c.UtilizationTarget < 0 || c.UtilizationTarget > 1) {
		return errField("resource.autoscalingPolicy.cpuUtilization.utilizationTarget", "", "Must be in (0, 1].")
	}

	if l := p.LoadBalancingUtilization; l != nil && (l.UtilizationTarget <= 0 || l.UtilizationTarget > 1) {
		return errField("resource.autoscalingPolicy.loadBalancingUtilization.utilizationTarget", "", "Must be in (0, 1].")
	}

	for _, m := range p.CustomMetricUtilizations {
		if m.Metric == "" {
			return errRequired("resource.autoscalingPolicy.customMetricUtilizations.metric")
		}

		if m.UtilizationTarget <= 0 && m.SingleInstanceAssignment <= 0 {
			return errField("resource.autoscalingPolicy.customMetricUtilizations.utilizationTarget", m.Metric, "Must be positive.")
		}

		if t := m.UtilizationTargetType; t != "" && !slices.Contains([]string{"GAUGE", "DELTA_PER_SECOND", "DELTA_PER_MINUTE"}, t) {
			return errField("resource.autoscalingPolicy.customMetricUtilizations.utilizationTargetType", t, "")
		}
	}

	if p.CpuUtilization == nil && p.LoadBalancingUtilization == nil && len(p.CustomMetricUtilizations) == 0 &&
		len(p.ScalingSchedules) == 0 {
		p.CpuUtilization = &computeapi.AutoscalingPolicyCpuUtilization{UtilizationTarget: defaultCPUTarget}
	}

	return nil
}

func (b *InMemoryBackend) scalerForLocked(target string) *computeapi.Autoscaler {
	for _, a := range b.scalers {
		if a.Target == target {
			return a
		}
	}

	return nil
}

// applyScalerLocked clamps the group's target size into the autoscaler's replica bounds. There is no metric
// source, so utilization signals never move the size.
func (b *InMemoryBackend) applyScalerLocked(rec *migRec) {
	a := b.scalerForLocked(rec.Mgr.SelfLink)
	if a == nil || a.AutoscalingPolicy.Mode == "OFF" {
		return
	}

	p := a.AutoscalingPolicy
	rec.Mgr.TargetSize = min(max(rec.Mgr.TargetSize, p.MinNumReplicas), p.MaxNumReplicas)
	a.RecommendedSize = rec.Mgr.TargetSize
}

func (b *InMemoryBackend) resolveScalerTargetLocked(project, scope, loc string, a *computeapi.Autoscaler) (*migRec, *APIError) {
	if a.Target == "" {
		return nil, errRequired("resource.target")
	}

	r, ok := parseRef(a.Target, collMIG, project, loc)
	if !ok || r.name == "" || (r.scope != "" && r.scope != scope) || r.loc != loc {
		return nil, errField("resource.target", a.Target, "The URL is malformed or names a different scope.")
	}

	rec, err := b.getMIGLocked(r.project, scope, loc, r.name)
	if err != nil {
		return nil, err
	}

	return rec, nil
}

func (b *InMemoryBackend) finishScalerLocked(project, scope, loc string, a *computeapi.Autoscaler, rec *migRec) *APIError {
	a.Target = rec.Mgr.SelfLink
	a.Status = scalerActive
	a.RecommendedSize = rec.Mgr.TargetSize

	if scope == scopeRegion {
		a.Region, a.Zone = scopeURL(project, scopeRegion, loc), ""
	} else {
		a.Zone, a.Region = scopeURL(project, scopeZone, loc), ""
	}

	b.scalers[key(project, loc, a.Name)] = a

	next := clone(rec.Mgr)
	next.Fingerprint = newFingerprint()

	b.applyScalerLocked(&migRec{Project: rec.Project, Mgr: next, Members: rec.Members})

	old := rec.Mgr
	rec.Mgr = next

	if err := b.reconcileLocked(rec); err != nil {
		rec.Mgr = old

		return err
	}

	return nil
}

func (b *InMemoryBackend) insertScaler(project, scope, loc string, in *computeapi.Autoscaler) (*computeapi.Operation, error) {
	b.mu.Lock("InsertAutoscaler")
	defer b.mu.Unlock()

	b.sweepGroupsLocked()

	if err := validName("name", in.Name); err != nil {
		return nil, err
	}

	if _, dup := b.scalers[key(project, loc, in.Name)]; dup {
		return nil, errExists(scalerPath(project, scope, loc, in.Name))
	}

	rec, err := b.resolveScalerTargetLocked(project, scope, loc, in)
	if err != nil {
		return nil, err
	}

	if existing := b.scalerForLocked(rec.Mgr.SelfLink); existing != nil {
		return nil, errInvalid("Invalid value for field 'resource.target': '%s'. The instance group manager already has autoscaler '%s'.",
			in.Target, existing.Name)
	}

	if err = normalizePolicy(in.AutoscalingPolicy); err != nil {
		return nil, err
	}

	in.Kind, in.Id, in.CreationTimestamp = "compute#autoscaler", newID(), b.stamp()
	in.SelfLink = link(project, scope, loc, collScalers, in.Name)

	if err = b.finishScalerLocked(project, scope, loc, in, rec); err != nil {
		delete(b.scalers, key(project, loc, in.Name))

		return nil, err
	}

	return b.newOpLocked(opScaler+"insert", scalerTarget(project, in), b.timing.Done), nil
}

func (b *InMemoryBackend) getScaler(project, scope, loc, name string) (*computeapi.Autoscaler, error) {
	b.mu.RLock("GetAutoscaler")
	defer b.mu.RUnlock()

	a, ok := b.scalers[key(project, loc, name)]
	if !ok {
		return nil, errNotFound(scalerPath(project, scope, loc, name))
	}

	return clone(a), nil
}

func (b *InMemoryBackend) listScalers(project, scope, loc string) []entry {
	b.mu.RLock("ListAutoscalers")
	defer b.mu.RUnlock()

	var out []entry

	for k, a := range b.scalers {
		sc, l := scalerScope(a)
		if !strings.HasPrefix(k, project+"/") || (scope != "" && (sc != scope || l != loc)) {
			continue
		}

		out = append(out, entry{scope: sc + "/" + l, item: clone(a)})
	}

	return out
}

func (b *InMemoryBackend) deleteScaler(project, scope, loc, name string) (*computeapi.Operation, error) {
	b.mu.Lock("DeleteAutoscaler")
	defer b.mu.Unlock()

	a, ok := b.scalers[key(project, loc, name)]
	if !ok {
		return nil, errNotFound(scalerPath(project, scope, loc, name))
	}

	delete(b.scalers, key(project, loc, name))

	return b.newOpLocked(opScaler+"delete", scalerTarget(project, a), b.timing.Done), nil
}

func (b *InMemoryBackend) updateScaler(
	project, scope, loc, name, verb string,
	raw map[string]any,
	replace *computeapi.Autoscaler,
) (*computeapi.Operation, error) {
	b.mu.Lock("UpdateAutoscaler")
	defer b.mu.Unlock()

	b.sweepGroupsLocked()

	cur, ok := b.scalers[key(project, loc, name)]
	if !ok {
		return nil, errNotFound(scalerPath(project, scope, loc, name))
	}

	next := replace
	if next == nil {
		merged, err := mergeInto(cur, raw)
		if err != nil {
			return nil, err
		}

		next = merged
	}

	if next.Name != "" && next.Name != cur.Name {
		return nil, errField("resource.name", next.Name, "The name of an autoscaler cannot be changed.")
	}

	next.Name, next.Id, next.Kind, next.CreationTimestamp, next.SelfLink = cur.Name, cur.Id, cur.Kind, cur.CreationTimestamp, cur.SelfLink

	if next.Target == "" {
		next.Target = cur.Target
	}

	rec, err := b.resolveScalerTargetLocked(project, scope, loc, next)
	if err != nil {
		return nil, err
	}

	if rec.Mgr.SelfLink != cur.Target {
		return nil, errField("resource.target", next.Target, "The target of an autoscaler cannot be changed.")
	}

	if err = normalizePolicy(next.AutoscalingPolicy); err != nil {
		return nil, err
	}

	if err = b.finishScalerLocked(project, scope, loc, next, rec); err != nil {
		b.scalers[key(project, loc, name)] = cur

		return nil, err
	}

	return b.newOpLocked(opScaler+verb, scalerTarget(project, next), b.timing.Done), nil
}
