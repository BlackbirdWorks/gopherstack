package gcpcompute

import (
	"slices"

	computeapi "google.golang.org/api/compute/v1"
)

var (
	diskPolicyKinds     = []string{"snapshotSchedulePolicy", "diskConsistencyGroupPolicy"}             //nolint:gochecknoglobals // lookup
	instancePolicyKinds = []string{"instanceSchedulePolicy", "groupPlacementPolicy", "workloadPolicy"} //nolint:gochecknoglobals // lookup
)

type resPoliciesRequest struct {
	ResourcePolicies []string `json:"resourcePolicies"`
}

// resolveResPoliciesLocked resolves refs to regional resource policies of the allowed kinds, one per kind.
func (b *InMemoryBackend) resolveResPoliciesLocked(
	project, region string, refs []string, field string, allowed []string,
) ([]string, *APIError) {
	c := lbCtx{project: project, scope: scopeRegion, loc: region}
	out := make([]string, 0, len(refs))
	kinds := map[string]bool{}

	for _, raw := range refs {
		self, err := lbResolve(b, resPolicySpec(), c, field, raw)
		if err != nil {
			return nil, err
		}

		kind := resPolicyKind(lbFind(b, resPolicySpec(), self))
		if !slices.Contains(allowed, kind) {
			return nil, errField(field, raw, "A resource policy of this type cannot be attached to this resource.")
		}

		if kinds[kind] {
			return nil, errField(field, raw, "At most one resource policy of this type can be attached.")
		}

		kinds[kind] = true

		if slices.Contains(out, self) {
			return nil, errField(field, raw, "Duplicate reference.")
		}

		out = append(out, self)
	}

	return out, nil
}

func (b *InMemoryBackend) addResPoliciesLocked(
	project, region string, have, add []string, allowed []string,
) ([]string, *APIError) {
	if len(add) == 0 {
		return nil, errRequired("resourcePolicies")
	}

	merged, err := b.resolveResPoliciesLocked(project, region, add, "resourcePolicies", allowed)
	if err != nil {
		return nil, err
	}

	for _, self := range merged {
		if slices.Contains(have, self) {
			return nil, errInvalid("The resource policy '%s' is already attached to the resource.", trimURL(self))
		}
	}

	return b.resolveResPoliciesLocked(project, region, append(slices.Clone(have), merged...), "resourcePolicies", allowed)
}

func (b *InMemoryBackend) removeResPoliciesLocked(project, region string, have, drop []string) ([]string, *APIError) {
	if len(drop) == 0 {
		return nil, errRequired("resourcePolicies")
	}

	c := lbCtx{project: project, scope: scopeRegion, loc: region}
	out := slices.Clone(have)

	for _, raw := range drop {
		self, err := lbResolve(b, resPolicySpec(), c, "resourcePolicies", raw)
		if err != nil {
			return nil, err
		}

		i := slices.Index(out, self)
		if i < 0 {
			return nil, errInvalid("The resource policy '%s' is not attached to the resource.", trimURL(self))
		}

		out = slices.Delete(out, i, i+1)
	}

	return out, nil
}

func (b *InMemoryBackend) changeDiskPolicies(
	project, zone, name, verb string, refs []string,
) (*computeapi.Operation, error) {
	b.mu.Lock("DiskResourcePolicies")
	defer b.mu.Unlock()

	d, ok := b.disks[key(project, zone, name)]
	if !ok {
		return nil, errNotFound(relPath(project, scopeZone, zone, "disks", name))
	}

	var (
		next []string
		err  *APIError
	)

	if verb == "addResourcePolicies" {
		next, err = b.addResPoliciesLocked(project, regionOfZone(zone), d.ResourcePolicies, refs, diskPolicyKinds)
	} else {
		next, err = b.removeResPoliciesLocked(project, regionOfZone(zone), d.ResourcePolicies, refs)
	}

	if err != nil {
		return nil, err
	}

	d.ResourcePolicies = next

	return b.newOpLocked(verb, diskTarget(project, zone, d), b.timing.Done), nil
}

func (b *InMemoryBackend) changeInstancePolicies(
	project, zone, name, verb string, refs []string,
) (*computeapi.Operation, error) {
	b.mu.Lock("InstanceResourcePolicies")
	defer b.mu.Unlock()

	r, aerr := b.getInstLocked(project, zone, name)
	if aerr != nil {
		return nil, aerr
	}

	var (
		next []string
		err  *APIError
	)

	if verb == "addResourcePolicies" {
		next, err = b.addResPoliciesLocked(project, regionOfZone(zone), r.Inst.ResourcePolicies, refs, instancePolicyKinds)
	} else {
		next, err = b.removeResPoliciesLocked(project, regionOfZone(zone), r.Inst.ResourcePolicies, refs)
	}

	if err != nil {
		return nil, err
	}

	r.Inst.ResourcePolicies = next
	b.touchLocked(r)

	return b.newOpLocked(verb, instTarget(project, zone, r.Inst), b.timing.Done), nil
}

// resPolicyUserLocked names a disk or instance that references the policy, or "".
func (b *InMemoryBackend) resPolicyUserLocked(self string) string {
	for _, d := range b.disks {
		if slices.Contains(d.ResourcePolicies, self) {
			return trimURL(d.SelfLink)
		}
	}

	for _, r := range b.instances {
		if slices.Contains(r.Inst.ResourcePolicies, self) {
			return trimURL(r.Inst.SelfLink)
		}
	}

	return ""
}

func (h *Handler) registerResourcePolicyOps() {
	r := scopeRegion
	registerLB(h, resPolicySpec(), lbOpts[computeapi.ResourcePolicy]{
		scopes: []string{r}, agg: regionScopes(), patch: true,
		del: func(b *InMemoryBackend, c lbCtx, name string) (*computeapi.Operation, error) {
			return lbDelete(b, resPolicySpec(), c, name, func(b *InMemoryBackend, v *computeapi.ResourcePolicy) *APIError {
				if by := b.resPolicyUserLocked(v.SelfLink); by != "" {
					return errInUse("resource_policy", trimURL(v.SelfLink), by)
				}

				return nil
			})
		},
	})

	for _, verb := range []string{"addResourcePolicies", "removeResourcePolicies"} {
		h.reg("disks", scopeZone, verb, func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[resPoliciesRequest](rq)
			if err != nil {
				return nil, err
			}

			return opResult(h.Backend.changeDiskPolicies(rq.rt.project, rq.rt.loc, rq.rt.name, verb, in.ResourcePolicies))
		})
		h.reg("instances", scopeZone, verb, func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[resPoliciesRequest](rq)
			if err != nil {
				return nil, err
			}

			return opResult(h.Backend.changeInstancePolicies(rq.rt.project, rq.rt.loc, rq.rt.name, verb, in.ResourcePolicies))
		})
	}
}
