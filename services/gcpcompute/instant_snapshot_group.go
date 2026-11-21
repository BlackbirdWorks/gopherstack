package gcpcompute

import (
	"slices"
	"strconv"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	collISGroups   = "instantSnapshotGroups"
	maxResNameLen  = 63
	diskConsistent = "diskConsistencyGroupPolicy"
)

func instantSnapshotGroupSpec() lbSpec[computeapi.InstantSnapshotGroup] {
	return lbSpec[computeapi.InstantSnapshotGroup]{
		coll: collISGroups, kind: "compute#instantSnapshotGroup", noun: "instant_snapshot_group",
		m: func(b *InMemoryBackend) map[string]*computeapi.InstantSnapshotGroup { return b.instSnapGroups },
		meta: func(v *computeapi.InstantSnapshotGroup) lbMeta {
			return lbMeta{
				name: &v.Name, kind: &v.Kind, created: &v.CreationTimestamp, self: &v.SelfLink,
				region: &v.Region, zone: &v.Zone, id: &v.Id,
			}
		},
		prep: prepInstantSnapshotGroup,
	}
}

func memberSnapshotName(group, disk string) string {
	name := group + "-" + disk
	if len(name) <= maxResNameLen {
		return name
	}

	keep := max(maxResNameLen-len(disk)-1, 1)

	return strings.TrimRight(group[:min(keep, len(group))]+"-"+disk, "-")[:maxResNameLen]
}

func prepInstantSnapshotGroup(
	b *InMemoryBackend, c lbCtx, in *computeapi.InstantSnapshotGroup, _ map[string]any,
) *APIError {
	if c.scope == scopeZone && !zoneExists(c.loc) || c.scope == scopeRegion && regionMissing(c.loc) {
		return errNotFound(scopePath(c.project, c.scope, c.loc))
	}

	if in.SourceConsistencyGroup == "" {
		return errRequired("sourceConsistencyGroup")
	}

	region := c.loc
	if c.scope == scopeZone {
		region = regionOfZone(c.loc)
	}

	pc := lbCtx{project: c.project, scope: scopeRegion, loc: region}

	self, err := lbResolve(b, resPolicySpec(), pc, "sourceConsistencyGroup", in.SourceConsistencyGroup)
	if err != nil {
		return err
	}

	pol := lbFind(b, resPolicySpec(), self)
	if resPolicyKind(pol) != diskConsistent {
		return errField("sourceConsistencyGroup", in.SourceConsistencyGroup, "Not a disk consistency group policy.")
	}

	members := b.consistencyMembersLocked(c, self)
	if len(members) == 0 {
		return errInvalid("Invalid value for field 'sourceConsistencyGroup': '%s'. "+
			"The consistency group has no disks in %s.", in.SourceConsistencyGroup, c.loc)
	}

	snaps := make([]*computeapi.InstantSnapshot, 0, len(members))

	for _, d := range members {
		name := memberSnapshotName(in.Name, d.Name)
		if _, dup := b.instantSnaps[key(c.project, c.loc, name)]; dup {
			return errExists(relPath(c.project, c.scope, c.loc, collInstantSnaps, name))
		}

		s := b.newInstantSnapshot(c.project, c.scope, c.loc, name, d)
		s.SourceInstantSnapshotGroup = in.SelfLink
		s.SourceInstantSnapshotGroupId = strconv.FormatUint(in.Id, 10)
		snaps = append(snaps, s)
	}

	for _, s := range snaps {
		b.instantSnaps[key(c.project, c.loc, s.Name)] = s
	}

	in.SourceConsistencyGroup = self
	in.Status = statusReady
	in.ResourceStatus = &computeapi.InstantSnapshotGroupResourceStatus{
		ConsistencyMembershipResolutionTime: b.stamp(),
		SourceInfo: &computeapi.InstantSnapshotGroupSourceInfo{
			ConsistencyGroup: self, ConsistencyGroupId: strconv.FormatUint(pol.Id, 10),
		},
	}
	in.SelfLinkWithId = link(c.project, c.scope, c.loc, collISGroups, strconv.FormatUint(in.Id, 10))

	return nil
}

// consistencyMembersLocked returns the disks of the scope that carry the consistency group policy, sorted by name.
func (b *InMemoryBackend) consistencyMembersLocked(c lbCtx, policy string) []*computeapi.Disk {
	var out []*computeapi.Disk

	for _, d := range b.disks {
		if !strings.HasPrefix(d.SelfLink, apiBase+"projects/"+c.project+"/") || diskScope(d) != c.scope {
			continue
		}

		loc := lastSegment(d.Zone)
		if c.scope == scopeRegion {
			loc = lastSegment(d.Region)
		}

		if loc == c.loc && slices.Contains(d.ResourcePolicies, policy) {
			out = append(out, d)
		}
	}

	slices.SortFunc(out, func(a, z *computeapi.Disk) int { return strings.Compare(a.Name, z.Name) })

	return out
}

func (b *InMemoryBackend) deleteInstantSnapshotGroup(c lbCtx, name string) (*computeapi.Operation, error) {
	return lbDelete(b, instantSnapshotGroupSpec(), c, name,
		func(b *InMemoryBackend, g *computeapi.InstantSnapshotGroup) *APIError {
			for k, s := range b.instantSnaps {
				if s.SourceInstantSnapshotGroup == g.SelfLink {
					delete(b.instantSnaps, k)
				}
			}

			return nil
		})
}

func (h *Handler) registerInstantSnapshotGroupOps() {
	sc := []string{scopeZone, scopeRegion}

	registerLB(h, instantSnapshotGroupSpec(), lbOpts[computeapi.InstantSnapshotGroup]{
		scopes: sc, noAgg: true, noInsert: true,
		del: func(b *InMemoryBackend, c lbCtx, name string) (*computeapi.Operation, error) {
			return b.deleteInstantSnapshotGroup(c, name)
		},
	})

	for _, scope := range sc {
		h.reg(collISGroups, scope, verbInsert, func(h *Handler, rq *request) (any, error) {
			in, raw, err := decode[computeapi.InstantSnapshotGroup](rq)
			if err != nil {
				return nil, err
			}

			if q := rq.q.Get("sourceConsistencyGroup"); q != "" {
				in.SourceConsistencyGroup = q
			}

			return opResult(lbInsert(h.Backend, instantSnapshotGroupSpec(), ctxOf(rq), in, raw))
		})
	}
}
