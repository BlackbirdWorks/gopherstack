package gcpcompute

import (
	"slices"

	computeapi "google.golang.org/api/compute/v1"
)

type perInstanceConfigsRequest struct {
	PerInstanceConfigs []*computeapi.PerInstanceConfig `json:"perInstanceConfigs"`
}

type namesRequest struct {
	Names []string `json:"names"`
}

type createInstancesRequest struct {
	Instances []*computeapi.PerInstanceConfig `json:"instances"`
}

func (b *InMemoryBackend) listPerInstanceConfigs(project, scope, loc, name string) ([]entry, error) {
	b.mu.RLock("ListPerInstanceConfigs")
	defer b.mu.RUnlock()

	rec, err := b.getMIGLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	out := make([]entry, 0, len(rec.Configs))

	for _, c := range rec.Configs {
		out = append(out, entry{scope: scopeGlobal, item: b.picViewLocked(c)})
	}

	return out, nil
}

// stageConfigsLocked validates every config against its member and returns the merged result without storing it.
func (b *InMemoryBackend) stageConfigsLocked(
	rec *migRec, cfgs []*computeapi.PerInstanceConfig, merge bool,
) ([]*computeapi.PerInstanceConfig, *APIError) {
	if len(cfgs) == 0 {
		return nil, errRequired("perInstanceConfigs")
	}

	if len(cfgs) > maxMIGBatch {
		return nil, errField("perInstanceConfigs", "", "At most 1000 per-instance configs are allowed per request.")
	}

	out := make([]*computeapi.PerInstanceConfig, 0, len(cfgs))
	seen := map[string]bool{}

	for _, in := range cfgs {
		if err := validName("resource.perInstanceConfigs.name", in.Name); err != nil {
			return nil, err
		}

		if seen[in.Name] {
			return nil, errField("resource.perInstanceConfigs.name", in.Name, "Duplicate per-instance config.")
		}

		seen[in.Name] = true

		m := rec.memberNamed(in.Name)
		if m == nil || m.Deleting {
			return nil, errField("resource.perInstanceConfigs.name", in.Name,
				"The instance does not exist or is not a member of the group.")
		}

		staged, err := b.stageConfigLocked(rec, m, in, merge)
		if err != nil {
			return nil, err
		}

		out = append(out, staged)
	}

	return out, nil
}

func (b *InMemoryBackend) stageConfigLocked(
	rec *migRec, m *memberRec, in *computeapi.PerInstanceConfig, merge bool,
) (*computeapi.PerInstanceConfig, *APIError) {
	cur := rec.config(in.Name)

	if cur != nil && in.Fingerprint != "" && in.Fingerprint != cur.Config.Fingerprint {
		return nil, errPrecondition("Supplied fingerprint does not match current per-instance config fingerprint.")
	}

	if err := b.validatePreservedStateLocked(rec.Project, m.Zone, in.PreservedState); err != nil {
		return nil, err
	}

	out := &computeapi.PerInstanceConfig{Name: in.Name, PreservedState: clone(in.PreservedState)}
	if merge && cur != nil {
		out.PreservedState = mergePreserved(clone(cur.Config.PreservedState), in.PreservedState)
	}

	return out, nil
}

func (b *InMemoryBackend) commitConfigsLocked(rec *migRec, cfgs []*computeapi.PerInstanceConfig) {
	now := b.now().UTC()

	for _, c := range cfgs {
		c.Fingerprint = newFingerprint()
		rec.Configs = slices.DeleteFunc(rec.Configs, func(o *picRec) bool { return o.Config.Name == c.Name })
		rec.Configs = append(rec.Configs, &picRec{Since: now, Config: c})

		if c.PreservedState != nil && len(c.PreservedState.Metadata) > 0 {
			b.applyConfigMetadataLocked(rec, c)
		}
	}

	slices.SortFunc(rec.Configs, func(a, c *picRec) int { return cmpString(a.Config.Name, c.Config.Name) })
}

func cmpString(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}

	return 0
}

func (b *InMemoryBackend) applyConfigMetadataLocked(rec *migRec, c *computeapi.PerInstanceConfig) {
	m := rec.memberNamed(c.Name)
	if m == nil {
		return
	}

	r, ok := b.instances[key(rec.Project, m.Zone, m.Name)]
	if !ok {
		return
	}

	if r.Inst.Metadata == nil {
		r.Inst.Metadata = &computeapi.Metadata{}
	}

	setMetadata(r.Inst.Metadata, c.PreservedState.Metadata)
	r.Inst.Metadata.Fingerprint = metadataFP(r.Inst.Metadata.Items)
	b.touchLocked(r)
}

func (b *InMemoryBackend) upsertPerInstanceConfigs(
	project, scope, loc, name, verb string, req *perInstanceConfigsRequest,
) (*computeapi.Operation, error) {
	b.mu.Lock("UpsertPerInstanceConfigs")
	defer b.mu.Unlock()

	b.sweepGroupsLocked()

	rec, err := b.getMIGLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	staged, serr := b.stageConfigsLocked(rec, req.PerInstanceConfigs, verb == "patchPerInstanceConfigs")
	if serr != nil {
		return nil, serr
	}

	b.commitConfigsLocked(rec, staged)

	return b.newOpLocked(opMIG+verb, migOpTarget(project, rec.Mgr), b.timing.Done), nil
}

func (b *InMemoryBackend) deletePerInstanceConfigs(
	project, scope, loc, name string, req *namesRequest,
) (*computeapi.Operation, error) {
	b.mu.Lock("DeletePerInstanceConfigs")
	defer b.mu.Unlock()

	b.sweepGroupsLocked()

	rec, err := b.getMIGLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	if len(req.Names) == 0 {
		return nil, errRequired("names")
	}

	for _, n := range req.Names {
		if rec.config(n) == nil {
			return nil, errNotFound(migPath(project, scope, loc, name) + "/perInstanceConfigs/" + n)
		}
	}

	rec.Configs = slices.DeleteFunc(
		rec.Configs,
		func(c *picRec) bool { return slices.Contains(req.Names, c.Config.Name) },
	)

	return b.newOpLocked(opMIG+"deletePerInstanceConfigs", migOpTarget(project, rec.Mgr), b.timing.Done), nil
}

// createMIGInstances adds instances with per-instance configs and raises targetSize by their number.
func (b *InMemoryBackend) createMIGInstances(
	project, scope, loc, name string, req *createInstancesRequest,
) (*computeapi.Operation, error) {
	b.mu.Lock("CreateInstances")
	defer b.mu.Unlock()

	b.sweepGroupsLocked()

	rec, err := b.getMIGLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	if len(req.Instances) == 0 {
		return nil, errRequired("instances")
	}

	if len(req.Instances) > maxMIGBatch {
		return nil, errField("instances", "", "At most 1000 instances are allowed per request.")
	}

	if err = b.createNamedLocked(rec, req.Instances, ""); err != nil {
		return nil, err
	}

	return b.newOpLocked(opMIG+"createInstances", migOpTarget(project, rec.Mgr), b.timing.Done), nil
}

// createNamedLocked creates one instance per config, all or none, and grows targetSize by the count.
func (b *InMemoryBackend) createNamedLocked(
	rec *migRec, cfgs []*computeapi.PerInstanceConfig, request string,
) *APIError {
	project := rec.Project
	seen := map[string]bool{}

	for _, c := range cfgs {
		if err := validName("resource.instances.name", c.Name); err != nil {
			return err
		}

		if seen[c.Name] || rec.memberNamed(c.Name) != nil {
			return errExists(trimURL(rec.Mgr.SelfLink) + "/instances/" + c.Name)
		}

		seen[c.Name] = true
	}

	prev, prevConfigs := len(rec.Members), slices.Clone(rec.Configs)
	tc, zc := templateCounts(activeMembers(rec)), zoneCounts(rec)
	added := make([]*memberRec, 0, len(cfgs))

	fail := func(err *APIError) *APIError {
		for _, m := range added {
			b.removeMemberInstanceLocked(project, rec, m, true)
		}

		rec.Members, rec.Configs = rec.Members[:prev], prevConfigs

		return err
	}

	for _, c := range cfgs {
		zone := b.pickZoneLocked(rec, zc)

		if _, dup := b.instances[key(project, zone, c.Name)]; dup {
			return fail(errExists(relPath(project, scopeZone, zone, collInstances, c.Name)))
		}

		if err := b.registerConfigLocked(rec, zone, c); err != nil {
			return fail(err)
		}

		tmpl := b.pickVersion(rec, tc)
		m := &memberRec{Name: c.Name, Zone: zone, Template: tmpl, Request: request}
		rec.Members = append(rec.Members, m)

		if err := b.createInstanceForLocked(project, rec, zone, c.Name, tmpl); err != nil {
			rec.Members = dropMember(rec.Members, m)

			return fail(err)
		}

		added = append(added, m)
		tc[tmpl]++
		zc[zone]++
	}

	rec.Mgr.TargetSize += int64(len(cfgs))
	rec.Mgr.Fingerprint = newFingerprint()
	b.applyScalerLocked(rec)

	return nil
}

func (b *InMemoryBackend) registerConfigLocked(rec *migRec, zone string, c *computeapi.PerInstanceConfig) *APIError {
	if c.PreservedState == nil {
		return nil
	}

	if err := b.validatePreservedStateLocked(rec.Project, zone, c.PreservedState); err != nil {
		return err
	}

	b.commitConfigsLocked(rec, []*computeapi.PerInstanceConfig{
		{Name: c.Name, PreservedState: clone(c.PreservedState)},
	})

	return nil
}
