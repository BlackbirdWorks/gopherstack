package gcpcompute

import (
	computeapi "google.golang.org/api/compute/v1"
)

type migInstancesRequest struct {
	Instances                      []string `json:"instances"`
	SkipInstancesOnValidationError bool     `json:"skipInstancesOnValidationError"`
}

type migMemberOp func(
	b *InMemoryBackend, project, scope, loc, name, verb string, in migInstancesRequest,
) (*computeapi.Operation, error)

func (b *InMemoryBackend) resolveMembersLocked(project string, rec *migRec, in migInstancesRequest) ([]*memberRec, *APIError) {
	if len(in.Instances) == 0 {
		return nil, errRequired("instances")
	}

	_, loc := migScopeOf(rec.Mgr)
	out := make([]*memberRec, 0, len(in.Instances))
	seen := map[*memberRec]bool{}

	for _, raw := range in.Instances {
		var found *memberRec

		if r, ok := parseRef(raw, "instances", project, loc); ok {
			for _, m := range liveMembers(rec) {
				if m.Name == r.name && (r.loc == "" || r.loc == m.Zone || rec.Mgr.Region == "" && r.loc == loc) {
					found = m
				}
			}
		}

		switch {
		case found == nil && in.SkipInstancesOnValidationError:
			continue
		case found == nil:
			return nil, errInvalid("Invalid value for field 'instances': '%s'. The instance is not a member of the group.", raw)
		case !seen[found]:
			seen[found] = true
			out = append(out, found)
		}
	}

	return out, nil
}

func (b *InMemoryBackend) memberOp(
	project, scope, loc, name, verb string,
	in migInstancesRequest,
	apply func(rec *migRec, ms []*memberRec) *APIError,
) (*computeapi.Operation, error) {
	b.mu.Lock("MIGInstances")
	defer b.mu.Unlock()

	b.sweepGroupsLocked()

	rec, err := b.getMIGLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	ms, err := b.resolveMembersLocked(project, rec, in)
	if err != nil {
		return nil, err
	}

	if err = apply(rec, ms); err != nil {
		return nil, err
	}

	return b.newOpLocked(opMIG+verb, migOpTarget(project, rec.Mgr), b.timing.Done), nil
}

func (b *InMemoryBackend) shrinkTarget(project string, rec *migRec, n int) *APIError {
	rec.Mgr.TargetSize = max(rec.Mgr.TargetSize-int64(n), 0)
	rec.Mgr.Fingerprint = newFingerprint()
	b.applyScalerLocked(rec)

	return b.reconcileLocked(rec)
}

func (b *InMemoryBackend) recreateMIGInstances(project, scope, loc, name, verb string, in migInstancesRequest) (*computeapi.Operation, error) {
	return b.memberOp(project, scope, loc, name, verb, in, func(rec *migRec, ms []*memberRec) *APIError {
		for _, m := range ms {
			if err := b.recreateMemberLocked(project, rec, m); err != nil {
				return err
			}
		}

		return nil
	})
}

func (b *InMemoryBackend) deleteMIGInstances(project, scope, loc, name, verb string, in migInstancesRequest) (*computeapi.Operation, error) {
	return b.memberOp(project, scope, loc, name, verb, in, func(rec *migRec, ms []*memberRec) *APIError {
		for _, m := range ms {
			b.markDeletingLocked(project, rec, m)
		}

		return b.shrinkTarget(project, rec, len(ms))
	})
}

func (b *InMemoryBackend) abandonMIGInstances(project, scope, loc, name, verb string, in migInstancesRequest) (*computeapi.Operation, error) {
	return b.memberOp(project, scope, loc, name, verb, in, func(rec *migRec, ms []*memberRec) *APIError {
		for _, m := range ms {
			rec.Members = dropMember(rec.Members, m)
		}

		return b.shrinkTarget(project, rec, len(ms))
	})
}

func (b *InMemoryBackend) listManagedInstances(project, scope, loc, name string) ([]entry, error) {
	b.mu.RLock("ListManagedInstances")
	defer b.mu.RUnlock()

	rec, err := b.getMIGLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	mis := b.managedInstancesLocked(rec)
	out := make([]entry, len(mis))

	for i, mi := range mis {
		out[i] = entry{scope: scopeGlobal, item: mi}
	}

	return out, nil
}
