package gcpcompute

import (
	"slices"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const opGroup = "compute.instanceGroups."

type groupInstancesRequest struct {
	Instances []*computeapi.InstanceReference `json:"instances"`
}

type listGroupInstancesRequest struct {
	InstanceState string `json:"instanceState"`
}

type namedPortsRequest struct {
	Fingerprint string                  `json:"fingerprint"`
	NamedPorts  []*computeapi.NamedPort `json:"namedPorts"`
}

func groupOpTarget(project, scope, loc string, g *computeapi.InstanceGroup) target {
	return target{project: project, scope: scope, loc: loc, collection: collGroups, name: g.Name, id: g.Id}
}

func (b *InMemoryBackend) dropFromGroupsLocked(selfLink string) {
	for _, g := range b.groups {
		g.Members = slices.DeleteFunc(g.Members, func(s string) bool { return s == selfLink })
	}
}

func (b *InMemoryBackend) instanceNetworkLocked(selfLink string) (string, string) {
	for _, r := range b.instances {
		if r.Inst.SelfLink == selfLink && len(r.Inst.NetworkInterfaces) > 0 {
			n := r.Inst.NetworkInterfaces[0]

			return n.Network, n.Subnetwork
		}
	}

	return "", ""
}

func (b *InMemoryBackend) groupViewLocked(g *groupRec) *computeapi.InstanceGroup {
	out := clone(g.Group)
	out.Size = int64(len(g.Members))
	out.ForceSendFields = []string{"Size"}

	return out
}

// derivedGroupLocked is the instance group every managed group owns.
func (b *InMemoryBackend) derivedGroupLocked(rec *migRec) *computeapi.InstanceGroup {
	m := rec.Mgr
	g := &computeapi.InstanceGroup{
		Kind: "compute#instanceGroup", Id: m.Id + 1, CreationTimestamp: m.CreationTimestamp, Name: m.Name,
		Description: m.Description, NamedPorts: m.NamedPorts, SelfLink: m.InstanceGroup, Zone: m.Zone, Region: m.Region,
		Fingerprint: fingerprint(m.Fingerprint, "group"),
	}

	for _, mem := range liveMembers(rec) {
		g.Size++

		if g.Network == "" {
			g.Network, g.Subnetwork = b.instanceNetworkLocked(zoneLink(rec.Project, mem.Zone, "instances", mem.Name))
		}
	}

	g.ForceSendFields = []string{"Size"}

	return g
}

func (b *InMemoryBackend) insertGroup(
	project, zone string,
	in *computeapi.InstanceGroup,
) (*computeapi.Operation, error) {
	b.mu.Lock("InsertInstanceGroup")
	defer b.mu.Unlock()

	b.sweepGroupsLocked()

	if err := firstErr(validName("name", in.Name), validNamedPorts(in.NamedPorts)); err != nil {
		return nil, err
	}

	if _, dup := b.groups[key(project, zone, in.Name)]; dup {
		return nil, errExists(relPath(project, scopeZone, zone, collGroups, in.Name))
	}

	if _, dup := b.migs[key(project, zone, in.Name)]; dup {
		return nil, errExists(relPath(project, scopeZone, zone, collGroups, in.Name))
	}

	g := &computeapi.InstanceGroup{
		Kind: "compute#instanceGroup", Id: newID(), CreationTimestamp: b.stamp(), Name: in.Name,
		Description: in.Description, NamedPorts: in.NamedPorts, Zone: scopeURL(project, scopeZone, zone),
		SelfLink: zoneLink(project, zone, collGroups, in.Name), Fingerprint: newFingerprint(),
		Network: in.Network, Subnetwork: in.Subnetwork,
	}

	b.groups[key(project, zone, in.Name)] = &groupRec{Group: g}

	return b.newOpLocked(opGroup+"insert", groupOpTarget(project, scopeZone, zone, g), b.timing.Done), nil
}

func (b *InMemoryBackend) findGroupLocked(
	project, scope, loc, name string,
) (*computeapi.InstanceGroup, []string, *APIError) {
	if scope == scopeZone {
		if g, ok := b.groups[key(project, loc, name)]; ok {
			return b.groupViewLocked(g), g.Members, nil
		}
	}

	if rec, ok := b.migs[key(project, loc, name)]; ok {
		var members []string

		for _, m := range liveMembers(rec) {
			members = append(members, zoneLink(project, m.Zone, "instances", m.Name))
		}

		return b.derivedGroupLocked(rec), members, nil
	}

	return nil, nil, errNotFound(relPath(project, scope, loc, collGroups, name))
}

func (b *InMemoryBackend) getGroup(project, scope, loc, name string) (*computeapi.InstanceGroup, error) {
	b.mu.RLock("GetInstanceGroup")
	defer b.mu.RUnlock()

	g, _, err := b.findGroupLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	return g, nil
}

func (b *InMemoryBackend) listGroups(project, scope, loc string) []entry {
	b.mu.RLock("ListInstanceGroups")
	defer b.mu.RUnlock()

	var out []entry

	for _, g := range b.groups {
		z := lastSegment(g.Group.Zone)
		if strings.HasPrefix(g.Group.SelfLink, apiBase+"projects/"+project+"/") &&
			(scope == "" || scope == scopeZone && z == loc) {
			out = append(out, entry{scope: scopeZone + "/" + z, item: b.groupViewLocked(g)})
		}
	}

	for _, rec := range b.migs {
		sc, l := migScopeOf(rec.Mgr)
		if rec.Project == project && (scope == "" || sc == scope && l == loc) {
			out = append(out, entry{scope: sc + "/" + l, item: b.derivedGroupLocked(rec)})
		}
	}

	return out
}

func (b *InMemoryBackend) deleteGroup(project, zone, name string) (*computeapi.Operation, error) {
	b.mu.Lock("DeleteInstanceGroup")
	defer b.mu.Unlock()

	g, ok := b.groups[key(project, zone, name)]
	if !ok {
		if _, managed := b.migs[key(project, zone, name)]; managed {
			return nil, errInvalid(
				"The instance group '%s' is managed by an instance group manager and cannot be deleted directly.",
				name,
			)
		}

		return nil, errNotFound(relPath(project, scopeZone, zone, collGroups, name))
	}

	delete(b.groups, key(project, zone, name))

	return b.newOpLocked(opGroup+"delete", groupOpTarget(project, scopeZone, zone, g.Group), b.timing.Done), nil
}

func (b *InMemoryBackend) resolveGroupInstanceLocked(
	project, zone string,
	ref *computeapi.InstanceReference,
) (*instRec, *APIError) {
	r, err := refLocated(ref.Instance, "instances", scopeZone, project, zone)
	if err != nil || r.loc != zone {
		return nil, errField("instances", ref.Instance, "The URL is malformed or names a different zone.")
	}

	inst, ok := b.instances[key(r.project, zone, r.name)]
	if !ok {
		return nil, errNotFound(relPath(r.project, scopeZone, zone, "instances", r.name))
	}

	return inst, nil
}

func (b *InMemoryBackend) addGroupInstances(
	project, zone, name string,
	in groupInstancesRequest,
) (*computeapi.Operation, error) {
	b.mu.Lock("AddInstances")
	defer b.mu.Unlock()

	g, err := b.unmanagedLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	if len(in.Instances) == 0 {
		return nil, errRequired("instances")
	}

	staged := slices.Clone(g.Members)
	network, subnet := g.Group.Network, g.Group.Subnetwork

	for _, ref := range in.Instances {
		inst, rerr := b.resolveGroupInstanceLocked(project, zone, ref)
		if rerr != nil {
			return nil, rerr
		}

		if slices.Contains(staged, inst.Inst.SelfLink) {
			return nil, errInvalid(
				"Instance '%s' is already a member of the instance group '%s'.",
				trimURL(inst.Inst.SelfLink),
				name,
			)
		}

		if len(inst.Inst.NetworkInterfaces) > 0 {
			n := inst.Inst.NetworkInterfaces[0]
			if network != "" && n.Network != network {
				return nil, errInvalid(
					"Instance '%s' is not in network '%s' of the instance group.",
					trimURL(inst.Inst.SelfLink),
					trimURL(network),
				)
			}

			network, subnet = n.Network, n.Subnetwork
		}

		staged = append(staged, inst.Inst.SelfLink)
	}

	g.Members = staged
	g.Group.Network, g.Group.Subnetwork = network, subnet

	return b.newOpLocked(opGroup+"addInstances", groupOpTarget(project, scopeZone, zone, g.Group), b.timing.Done), nil
}

func (b *InMemoryBackend) unmanagedLocked(project, zone, name string) (*groupRec, *APIError) {
	g, ok := b.groups[key(project, zone, name)]
	if ok {
		return g, nil
	}

	if _, managed := b.migs[key(project, zone, name)]; managed {
		return nil, errInvalid(
			"The instance group '%s' is managed; use the instance group manager to change its members.",
			name,
		)
	}

	return nil, errNotFound(relPath(project, scopeZone, zone, collGroups, name))
}

func (b *InMemoryBackend) removeGroupInstances(
	project, zone, name string,
	in groupInstancesRequest,
) (*computeapi.Operation, error) {
	b.mu.Lock("RemoveInstances")
	defer b.mu.Unlock()

	g, err := b.unmanagedLocked(project, zone, name)
	if err != nil {
		return nil, err
	}

	if len(in.Instances) == 0 {
		return nil, errRequired("instances")
	}

	staged := slices.Clone(g.Members)

	for _, ref := range in.Instances {
		r, perr := refLocated(ref.Instance, "instances", scopeZone, project, zone)
		if perr != nil {
			return nil, errField("instances", ref.Instance, "The URL is malformed.")
		}

		self := zoneLink(r.project, r.loc, "instances", r.name)
		if !slices.Contains(staged, self) {
			return nil, errInvalid("Instance '%s' is not a member of the instance group '%s'.", trimURL(self), name)
		}

		staged = slices.DeleteFunc(staged, func(s string) bool { return s == self })
	}

	g.Members = staged

	return b.newOpLocked(
		opGroup+"removeInstances",
		groupOpTarget(project, scopeZone, zone, g.Group),
		b.timing.Done,
	), nil
}

func (b *InMemoryBackend) setGroupNamedPorts(
	project, scope, loc, name string,
	in namedPortsRequest,
) (*computeapi.Operation, error) {
	b.mu.Lock("SetNamedPorts")
	defer b.mu.Unlock()

	if err := validNamedPorts(in.NamedPorts); err != nil {
		return nil, err
	}

	if scope == scopeZone {
		if g, ok := b.groups[key(project, loc, name)]; ok {
			if in.Fingerprint != "" && in.Fingerprint != g.Group.Fingerprint {
				return nil, errPrecondition("Supplied fingerprint does not match current fingerprint.")
			}

			g.Group.NamedPorts, g.Group.Fingerprint = in.NamedPorts, newFingerprint()

			return b.newOpLocked(
				opGroup+"setNamedPorts",
				groupOpTarget(project, scope, loc, g.Group),
				b.timing.Done,
			), nil
		}
	}

	rec, ok := b.migs[key(project, loc, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scope, loc, collGroups, name))
	}

	rec.Mgr.NamedPorts, rec.Mgr.Fingerprint = in.NamedPorts, newFingerprint()
	g := b.derivedGroupLocked(rec)

	return b.newOpLocked(opGroup+"setNamedPorts", groupOpTarget(project, scope, loc, g), b.timing.Done), nil
}

func (b *InMemoryBackend) listGroupInstances(project, scope, loc, name, state string) ([]entry, error) {
	b.mu.RLock("ListGroupInstances")
	defer b.mu.RUnlock()

	if state != "" && state != "ALL" && state != "RUNNING" {
		return nil, errField("instanceState", state, "")
	}

	g, members, err := b.findGroupLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	out := make([]entry, 0, len(members))
	now := b.now().UTC()

	for _, m := range members {
		ref, _ := parseRef(m, "instances", project, "")

		r, ok := b.instances[key(ref.project, ref.loc, ref.name)]
		if !ok {
			continue
		}

		st := r.status(now, b.timing)
		if state == "RUNNING" && st != stRunning {
			continue
		}

		out = append(out, entry{scope: scopeGlobal, item: &computeapi.InstanceWithNamedPorts{
			Instance: m, Status: st, NamedPorts: g.NamedPorts,
		}})
	}

	return out, nil
}
