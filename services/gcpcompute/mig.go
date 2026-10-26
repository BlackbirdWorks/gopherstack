package gcpcompute

import (
	"slices"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	collMIG           = "instanceGroupManagers"
	collGroups        = "instanceGroups"
	maxBaseNameLen    = 58
	maxInitialDelay   = 3600
	updateProactive   = "PROACTIVE"
	updateOpportunist = "OPPORTUNISTIC"
	opMIG             = "compute.instanceGroupManagers."
	actNone           = "NONE"
	actCreating       = "CREATING"
	actDeleting       = "DELETING"
	actRecreating     = "RECREATING"
	maxPercent        = 100
)

func migPath(project, scope, loc, name string) string {
	return relPath(project, scope, loc, collMIG, name)
}

func migScopeOf(m *computeapi.InstanceGroupManager) (string, string) {
	if m.Region != "" {
		return scopeRegion, lastSegment(m.Region)
	}

	return scopeZone, lastSegment(m.Zone)
}

func migOpTarget(project string, m *computeapi.InstanceGroupManager) target {
	scope, loc := migScopeOf(m)

	return target{project: project, scope: scope, loc: loc, collection: collMIG, name: m.Name, id: m.Id}
}

func migUsesTemplate(m *computeapi.InstanceGroupManager, tmpl string) bool {
	if m.InstanceTemplate == tmpl {
		return true
	}

	for _, v := range m.Versions {
		if v.InstanceTemplate == tmpl {
			return true
		}
	}

	return false
}

func zonesInRegion(region string) []string {
	var out []string

	for _, z := range allZones() {
		if regionOfZone(z) == region {
			out = append(out, z)
		}
	}

	return out
}

func validNamedPorts(ports []*computeapi.NamedPort) *APIError {
	seen := map[string]bool{}

	for _, p := range ports {
		if err := validName("namedPorts.name", p.Name); err != nil {
			return err
		}

		if seen[p.Name] {
			return errField("namedPorts.name", p.Name, "Duplicate named port.")
		}

		if p.Port < 1 || p.Port > hcMaxPort {
			return errField("namedPorts.port", "", "Port must be in the range 1-65535.")
		}

		seen[p.Name] = true
	}

	return nil
}

func (b *InMemoryBackend) normalizeVersionsLocked(
	project, region string,
	m *computeapi.InstanceGroupManager,
) *APIError {
	if err := seedVersions(m); err != nil {
		return err
	}

	unsized := 0

	for _, v := range m.Versions {
		if err := b.resolveVersionLocked(project, region, v); err != nil {
			return err
		}

		if v.TargetSize == nil {
			unsized++
		}
	}

	if len(m.Versions) > 1 && unsized > 1 {
		return errInvalid("Invalid value for field 'resource.versions': at most one version may omit targetSize.")
	}

	if len(m.Versions) == 1 {
		m.InstanceTemplate = m.Versions[0].InstanceTemplate
	} else {
		m.InstanceTemplate = ""
	}

	return nil
}

func seedVersions(m *computeapi.InstanceGroupManager) *APIError {
	switch {
	case m.InstanceTemplate != "" && len(m.Versions) == 1 && m.Versions[0].InstanceTemplate == m.InstanceTemplate:
	case m.InstanceTemplate != "" && len(m.Versions) > 0:
		return errInvalid(
			"Invalid value for field 'resource.versions': instanceTemplate and versions are mutually exclusive.",
		)
	case m.InstanceTemplate != "":
		m.Versions = []*computeapi.InstanceGroupManagerVersion{{InstanceTemplate: m.InstanceTemplate}}
	case len(m.Versions) == 0:
		return errRequired("resource.instanceTemplate")
	}

	return nil
}

func (b *InMemoryBackend) resolveVersionLocked(
	project, region string,
	v *computeapi.InstanceGroupManagerVersion,
) *APIError {
	t, err := b.resolveTemplateLocked(project, v.InstanceTemplate, "resource.versions.instanceTemplate")
	if err != nil {
		return err
	}

	if tr := lastSegment(t.Region); tr != "" && tr != region {
		return errField(
			"resource.versions.instanceTemplate",
			v.InstanceTemplate,
			"The template is in a different region.",
		)
	}

	v.InstanceTemplate = t.SelfLink

	ts := v.TargetSize
	if ts == nil {
		return nil
	}

	if ts.Fixed < 0 || ts.Percent < 0 || ts.Percent > maxPercent || (ts.Fixed != 0 && ts.Percent != 0) {
		return errInvalid("Invalid value for field 'resource.versions.targetSize': either fixed or percent in 0-100.")
	}

	v.TargetSize = &computeapi.FixedOrPercent{Fixed: ts.Fixed, Percent: ts.Percent}

	return nil
}

func (b *InMemoryBackend) resolveHealthLocked(project, raw string) (*computeapi.HealthCheck, *APIError) {
	r, ok := parseRef(raw, collHealth, project, "")
	if !ok || (r.scope != "" && r.scope != scopeGlobal && r.scope != scopeRegion) {
		return nil, errField("resource.autoHealingPolicies.healthCheck", raw, "The URL is malformed.")
	}

	loc := ""
	if r.scope == scopeRegion {
		loc = r.loc
	}

	h, found := b.healths[key(r.project, hcLoc(loc), r.name)]
	if !found {
		return nil, errNotFound(hcPath(r.project, loc, r.name))
	}

	return h, nil
}

func (b *InMemoryBackend) normalizeHealingLocked(project string, m *computeapi.InstanceGroupManager) *APIError {
	if len(m.AutoHealingPolicies) > 1 {
		return errInvalid("Invalid value for field 'resource.autoHealingPolicies': at most one policy is allowed.")
	}

	for _, p := range m.AutoHealingPolicies {
		if p.InitialDelaySec < 0 || p.InitialDelaySec > maxInitialDelay {
			return errField("resource.autoHealingPolicies.initialDelaySec", "", "Must be between 0 and 3600.")
		}

		h, err := b.resolveHealthLocked(project, p.HealthCheck)
		if err != nil {
			return err
		}

		p.HealthCheck = h.SelfLink
	}

	return nil
}

func normalizeUpdatePolicy(
	p *computeapi.InstanceGroupManagerUpdatePolicy,
	zones int,
) (*computeapi.InstanceGroupManagerUpdatePolicy, *APIError) {
	if p == nil {
		p = &computeapi.InstanceGroupManagerUpdatePolicy{}
	}

	if p.Type == "" {
		p.Type = updateOpportunist
	}

	if p.Type != updateProactive && p.Type != updateOpportunist {
		return nil, errField("resource.updatePolicy.type", p.Type, "")
	}

	def := func(v **computeapi.FixedOrPercent) {
		if *v == nil {
			*v = &computeapi.FixedOrPercent{Fixed: int64(zones)}
		}
	}

	def(&p.MaxSurge)
	def(&p.MaxUnavailable)

	if p.MinimalAction == "" {
		p.MinimalAction = "REPLACE"
	}

	if p.ReplacementMethod == "" {
		p.ReplacementMethod = "SUBSTITUTE"
	}

	return p, nil
}

func (b *InMemoryBackend) normalizeDistribution(m *computeapi.InstanceGroupManager, region string) *APIError {
	dp := m.DistributionPolicy
	if region == "" {
		if dp != nil {
			return errInvalid("Invalid value for field 'resource.distributionPolicy': only valid for regional groups.")
		}

		return nil
	}

	if dp == nil {
		dp = &computeapi.DistributionPolicy{}
	}

	if dp.TargetShape == "" {
		dp.TargetShape = "EVEN"
	}

	if !slices.Contains([]string{"EVEN", "BALANCED", "ANY", "ANY_SINGLE_ZONE"}, dp.TargetShape) {
		return errField("resource.distributionPolicy.targetShape", dp.TargetShape, "")
	}

	seen := map[string]bool{}

	for _, z := range dp.Zones {
		zn := lastSegment(z.Zone)
		if !zoneExists(zn) || regionOfZone(zn) != region || seen[zn] {
			return errField(
				"resource.distributionPolicy.zones.zone",
				z.Zone,
				"Not a distinct zone of the group's region.",
			)
		}

		seen[zn] = true
	}

	m.DistributionPolicy = dp

	return nil
}

func (b *InMemoryBackend) normalizeMIGLocked(project, scope, loc string, m *computeapi.InstanceGroupManager) *APIError {
	region := loc
	if scope == scopeZone {
		region = regionOfZone(loc)
	}

	if err := validName("name", m.Name); err != nil {
		return err
	}

	if err := validName(
		"resource.baseInstanceName",
		m.BaseInstanceName,
	); err != nil ||
		len(m.BaseInstanceName) > maxBaseNameLen {
		if err == nil {
			err = errField("resource.baseInstanceName", m.BaseInstanceName, "Must be at most 58 characters.")
		}

		return err
	}

	if m.TargetSize < 0 {
		return errField("resource.targetSize", "", "Must not be negative.")
	}

	if err := firstErr(
		b.normalizeVersionsLocked(project, region, m),
		validNamedPorts(m.NamedPorts),
		b.normalizeHealingLocked(project, m),
	); err != nil {
		return err
	}

	zones := 1

	if scope == scopeRegion {
		if err := b.normalizeDistribution(m, loc); err != nil {
			return err
		}

		zones = len(zonesInRegion(loc))
		if n := len(m.DistributionPolicy.Zones); n > 0 {
			zones = n
		}
	} else if m.DistributionPolicy != nil {
		return errInvalid("Invalid value for field 'resource.distributionPolicy': only valid for regional groups.")
	}

	up, err := normalizeUpdatePolicy(m.UpdatePolicy, zones)
	if err != nil {
		return err
	}

	m.UpdatePolicy = up

	return nil
}

func (b *InMemoryBackend) insertMIG(
	project, scope, loc string,
	in *computeapi.InstanceGroupManager,
) (*computeapi.Operation, error) {
	b.mu.Lock("InsertInstanceGroupManager")
	defer b.mu.Unlock()

	b.sweepGroupsLocked()

	if err := validName("name", in.Name); err != nil {
		return nil, err
	}

	if scope == scopeZone && !zoneExists(loc) || scope == scopeRegion && regionMissing(loc) {
		return nil, errNotFound(scopePath(project, scope, loc))
	}

	if _, dup := b.migs[key(project, loc, in.Name)]; dup {
		return nil, errExists(migPath(project, scope, loc, in.Name))
	}

	if _, dup := b.groups[key(project, loc, in.Name)]; dup {
		return nil, errExists(relPath(project, scope, loc, collGroups, in.Name))
	}

	if err := b.normalizeMIGLocked(project, scope, loc, in); err != nil {
		return nil, err
	}

	in.Kind, in.Id, in.CreationTimestamp = "compute#instanceGroupManager", newID(), b.stamp()
	in.Fingerprint = newFingerprint()
	in.Status = nil
	in.CurrentActions = nil
	in.SelfLink = link(project, scope, loc, collMIG, in.Name)
	in.InstanceGroup = link(project, scope, loc, collGroups, in.Name)

	if scope == scopeRegion {
		in.Region = scopeURL(project, scopeRegion, loc)
		for _, z := range in.DistributionPolicy.Zones {
			z.Zone = scopeURL(project, scopeZone, lastSegment(z.Zone))
		}
	} else {
		in.Zone = scopeURL(project, scopeZone, loc)
	}

	rec := &migRec{Project: project, Mgr: in}

	if err := b.reconcileLocked(rec); err != nil {
		return nil, err
	}

	b.migs[key(project, loc, in.Name)] = rec

	return b.newOpLocked(opMIG+"insert", migOpTarget(project, in), b.timing.Done), nil
}

func regionMissing(region string) bool {
	_, ok := findRegion(region)

	return !ok
}

func (b *InMemoryBackend) getMIGLocked(project, scope, loc, name string) (*migRec, *APIError) {
	r, ok := b.migs[key(project, loc, name)]
	if !ok {
		return nil, errNotFound(migPath(project, scope, loc, name))
	}

	return r, nil
}

func (b *InMemoryBackend) getMIG(project, scope, loc, name string) (*computeapi.InstanceGroupManager, error) {
	b.mu.RLock("GetInstanceGroupManager")
	defer b.mu.RUnlock()

	r, err := b.getMIGLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	return b.migViewLocked(r), nil
}

func (b *InMemoryBackend) listMIGs(project, scope, loc string) []entry {
	b.mu.RLock("ListInstanceGroupManagers")
	defer b.mu.RUnlock()

	var out []entry

	for _, r := range b.migs {
		sc, l := migScopeOf(r.Mgr)
		if r.Project != project || (scope != "" && (sc != scope || l != loc)) {
			continue
		}

		out = append(out, entry{scope: sc + "/" + l, item: b.migViewLocked(r)})
	}

	return out
}

func (b *InMemoryBackend) deleteMIG(project, scope, loc, name string) (*computeapi.Operation, error) {
	b.mu.Lock("DeleteInstanceGroupManager")
	defer b.mu.Unlock()

	b.sweepGroupsLocked()

	r, err := b.getMIGLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	for _, m := range liveMembers(r) {
		b.removeMemberInstanceLocked(project, m)
	}

	for k, a := range b.scalers {
		if a.Target == r.Mgr.SelfLink {
			delete(b.scalers, k)
		}
	}

	delete(b.migs, key(project, loc, name))

	return b.newOpLocked(opMIG+"delete", migOpTarget(project, r.Mgr), b.timing.Done), nil
}

func (b *InMemoryBackend) updateMIG(
	project, scope, loc, name, verb string,
	raw map[string]any,
	replace *computeapi.InstanceGroupManager,
) (*computeapi.Operation, error) {
	b.mu.Lock("UpdateInstanceGroupManager")
	defer b.mu.Unlock()

	b.sweepGroupsLocked()

	rec, err := b.getMIGLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	next := replace
	if next == nil {
		doc := toDoc(rec.Mgr)
		if _, ok := raw["versions"]; ok {
			delete(doc, "instanceTemplate")
		}

		if _, ok := raw["instanceTemplate"]; ok {
			delete(doc, "versions")
		}

		mergePatch(doc, raw)

		merged, derr := fromDoc[computeapi.InstanceGroupManager](doc)
		if derr != nil {
			return nil, errInvalid("Invalid JSON payload received: %v", derr)
		}

		next = merged
	} else if next.TargetSize == 0 && next.Name == "" {
		next.TargetSize = rec.Mgr.TargetSize
	}

	if next.BaseInstanceName != rec.Mgr.BaseInstanceName && next.BaseInstanceName != "" {
		return nil, errField(
			"resource.baseInstanceName",
			next.BaseInstanceName,
			"The base instance name cannot be changed.",
		)
	}

	next.Name, next.BaseInstanceName = rec.Mgr.Name, rec.Mgr.BaseInstanceName

	if replace != nil && replace.DistributionPolicy == nil {
		next.DistributionPolicy = rec.Mgr.DistributionPolicy
	}

	if err = b.normalizeMIGLocked(project, scope, loc, next); err != nil {
		return nil, err
	}

	next.Kind, next.Id, next.CreationTimestamp = rec.Mgr.Kind, rec.Mgr.Id, rec.Mgr.CreationTimestamp
	next.SelfLink, next.InstanceGroup = rec.Mgr.SelfLink, rec.Mgr.InstanceGroup
	next.Zone, next.Region = rec.Mgr.Zone, rec.Mgr.Region
	next.Status, next.CurrentActions, next.Fingerprint = nil, nil, newFingerprint()

	if next.DistributionPolicy != nil {
		for _, z := range next.DistributionPolicy.Zones {
			z.Zone = scopeURL(project, scopeZone, lastSegment(z.Zone))
		}
	}

	return b.commitMIGLocked(project, rec, next, verb)
}

// commitMIGLocked swaps in next, rolls the group's instances to match and restores the old config on failure.
func (b *InMemoryBackend) commitMIGLocked(
	project string,
	rec *migRec,
	next *computeapi.InstanceGroupManager,
	verb string,
) (*computeapi.Operation, error) {
	old := rec.Mgr
	rec.Mgr = next

	b.applyScalerLocked(rec)

	if next.UpdatePolicy.Type == updateProactive {
		b.rollOutdatedLocked(project, rec)
	}

	if err := b.reconcileLocked(rec); err != nil {
		rec.Mgr = old

		return nil, err
	}

	return b.newOpLocked(opMIG+verb, migOpTarget(project, next), b.timing.Done), nil
}

func (b *InMemoryBackend) resizeMIG(project, scope, loc, name string, size int64) (*computeapi.Operation, error) {
	b.mu.Lock("ResizeInstanceGroupManager")
	defer b.mu.Unlock()

	b.sweepGroupsLocked()

	rec, err := b.getMIGLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	if size < 0 {
		return nil, errField("size", "", "Must not be negative.")
	}

	next := clone(rec.Mgr)
	next.TargetSize, next.Fingerprint = size, newFingerprint()

	return b.commitMIGLocked(project, rec, next, "resize")
}

type templateRequest struct {
	InstanceTemplate string `json:"instanceTemplate"`
}

func (b *InMemoryBackend) setMIGTemplate(project, scope, loc, name, tmpl string) (*computeapi.Operation, error) {
	b.mu.Lock("SetInstanceTemplate")
	defer b.mu.Unlock()

	b.sweepGroupsLocked()

	rec, err := b.getMIGLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	if tmpl == "" {
		return nil, errRequired("instanceTemplate")
	}

	next := clone(rec.Mgr)
	next.InstanceTemplate, next.Versions = tmpl, nil

	region := loc
	if scope == scopeZone {
		region = regionOfZone(loc)
	}

	if err = b.normalizeVersionsLocked(project, region, next); err != nil {
		return nil, err
	}

	next.Fingerprint = newFingerprint()

	return b.commitMIGLocked(project, rec, next, "setInstanceTemplate")
}

type targetPoolsRequest struct {
	Fingerprint string   `json:"fingerprint"`
	TargetPools []string `json:"targetPools"`
}

func (b *InMemoryBackend) setMIGTargetPools(
	project, scope, loc, name string,
	in targetPoolsRequest,
) (*computeapi.Operation, error) {
	b.mu.Lock("SetTargetPools")
	defer b.mu.Unlock()

	b.sweepGroupsLocked()

	rec, err := b.getMIGLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	if in.Fingerprint != "" && in.Fingerprint != rec.Mgr.Fingerprint {
		return nil, errPrecondition("Supplied fingerprint does not match current fingerprint.")
	}

	for _, p := range in.TargetPools {
		if strings.TrimSpace(p) == "" {
			return nil, errField("targetPools", p, "")
		}
	}

	rec.Mgr.TargetPools, rec.Mgr.Fingerprint = in.TargetPools, newFingerprint()

	return b.newOpLocked(opMIG+"setTargetPools", migOpTarget(project, rec.Mgr), b.timing.Done), nil
}
