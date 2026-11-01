package lambdaweb

import (
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func (b *InMemoryBackend) newEndpointLocked(f *Function, in *EndpointInput, now time.Time) *Endpoint {
	e := &Endpoint{
		Name: in.Name, ARN: endpointARN(f.Region, f.AccountID, f.Name, in.Name), Region: f.Region,
		AccountID: f.AccountID, FunctionName: f.Name, Description: in.Description, Type: in.Type,
		AuthType: in.AuthType, AutoDeploymentMode: in.AutoDeploymentMode, Regions: slices.Clone(in.Regions),
		Weights: slices.Clone(in.Weights), State: statePending, StateReason: "The endpoint is being created.",
		CreatedAt: now, UpdatedAt: now, PendingUntil: now.Add(b.dwell(endpointActivationDelay)),
		RegionDomains: map[string]string{},
	}

	if in.Type == endpointMultiRegion {
		e.DomainName = globalDomain()
	} else {
		for _, r := range e.Regions {
			e.RegionDomains[r] = regionalDomain(r)
		}

		e.DomainName = e.RegionDomains[f.Region]
	}

	applyScaling(e, in.Scaling)
	applyThrottle(e, in.Throttle)

	return e
}

func applyScaling(e *Endpoint, s *ScalingInput) {
	if s == nil {
		return
	}

	e.MaxEnvironments = nil

	if s.MaxEnvironments != nil {
		v := *s.MaxEnvironments
		e.MaxEnvironments = &v
	}
}

func applyThrottle(e *Endpoint, t *ThrottleInput) {
	if t == nil {
		return
	}

	e.RateLimit = nil

	if t.RateLimit != nil {
		v := *t.RateLimit
		e.RateLimit = &v
	}
}

// endpointView returns a copy of e carrying its effective routing table.
func (b *InMemoryBackend) endpointView(e *Endpoint) *Endpoint {
	cp := e.clone()
	cp.Weights = b.effectiveWeightsLocked(e)

	return cp
}

// resolveWeightsLocked validates revision weights and returns them with revision ARNs resolved to ids.
func (b *InMemoryBackend) resolveWeightsLocked(
	region, fn, endpointType string, weights []RevisionWeight,
) ([]RevisionWeight, error) {
	if len(weights) == 0 || len(weights) > maxWeightedRevisions {
		return nil, validationError("revisionWeights must list one or two revisions.")
	}

	out := make([]RevisionWeight, 0, len(weights))
	total := int32(0)

	for _, w := range weights {
		if w.Weight < 1 || w.Weight > percentTotal {
			return nil, validationError("Each revision weight must be between 1 and %d.", percentTotal)
		}

		rev, err := b.revisionLocked(region, fn, w.RevisionID)
		if err != nil {
			return nil, err
		}

		if slices.ContainsFunc(out, func(x RevisionWeight) bool { return x.RevisionID == rev.ID }) {
			return nil, validationError("Revision %s appears more than once in revisionWeights.", rev.ID)
		}

		if rev.State != stateActive {
			return nil, conflictError(
				resRevision,
				rev.ID,
				"Revision %s is %s; only Active revisions can receive traffic.",
				rev.ID,
				rev.State,
			)
		}

		if rev.ServiceConfig.EgressNetworkConnectorArn != "" && endpointType != endpointHomeRegion {
			return nil, validationError("Revision %s uses a network connector and can only serve %s endpoints.",
				rev.ID, endpointHomeRegion)
		}

		total += w.Weight

		out = append(out, RevisionWeight{RevisionID: rev.ID, Weight: w.Weight})
	}

	if total != percentTotal {
		return nil, validationError("Revision weights must sum to %d, got %d.", percentTotal, total)
	}

	return out, nil
}

// CreateWebFunctionEndpoint adds an endpoint to a function.
func (b *InMemoryBackend) CreateWebFunctionEndpoint(region, ref string, in EndpointInput) (*Endpoint, error) {
	r, name, err := b.functionRef(ref, region)
	if err != nil {
		return nil, err
	}

	if err = normalizeEndpoint(&in, r); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateWebFunctionEndpoint")
	defer b.mu.Unlock()

	b.advanceLocked()

	f, err := b.liveFunctionLocked(r, name)
	if err != nil {
		return nil, err
	}

	existing := b.endpointsOfLocked(r, name)
	if slices.ContainsFunc(existing, func(e *Endpoint) bool { return e.Name == in.Name }) {
		return nil, conflictError(resEndpoint, in.Name, "Endpoint %s already exists.", in.Name)
	}

	if len(existing) >= maxEndpointsPerFunction {
		return nil, quotaError(
			resFunction,
			name,
			"A web function can have at most %d endpoints.",
			maxEndpointsPerFunction,
		)
	}

	if err = b.prepareWeightsLocked(&in, r, name); err != nil {
		return nil, err
	}

	e := b.newEndpointLocked(f, &in, b.now().UTC())
	b.endpoints.Put(e)

	return b.endpointView(e), nil
}

func (b *InMemoryBackend) prepareWeightsLocked(in *EndpointInput, region, fn string) error {
	if in.AutoDeploymentMode == modeLatestRevision {
		if len(in.Weights) > 0 {
			return validationError("revisionWeights can't be set while autoDeploymentMode is %s.", modeLatestRevision)
		}

		return nil
	}

	weights, err := b.resolveWeightsLocked(region, fn, in.Type, in.Weights)
	if err != nil {
		return err
	}

	in.Weights = weights

	return nil
}

func (b *InMemoryBackend) endpointLocked(region, fn, ref string) (*Endpoint, error) {
	name, err := b.childRef(ref, resEndpoint, region, fn)
	if err != nil {
		return nil, err
	}

	e, ok := b.endpoints.Get(endpointKey(region, fn, name))
	if !ok {
		return nil, notFoundError(resEndpoint, name)
	}

	return e, nil
}

// GetWebFunctionEndpoint returns one endpoint.
func (b *InMemoryBackend) GetWebFunctionEndpoint(region, fnRef, epRef string) (*Endpoint, error) {
	r, name, err := b.functionRef(fnRef, region)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("GetWebFunctionEndpoint")
	defer b.mu.Unlock()

	b.advanceLocked()

	if _, err = b.liveFunctionLocked(r, name); err != nil {
		return nil, err
	}

	e, err := b.endpointLocked(r, name, epRef)
	if err != nil {
		return nil, err
	}

	return b.endpointView(e), nil
}

// ListWebFunctionEndpoints lists a function's endpoints ordered by name.
func (b *InMemoryBackend) ListWebFunctionEndpoints(
	region, ref string, opts ListOptions,
) (page.Page[*Endpoint], error) {
	r, name, err := b.functionRef(ref, region)
	if err != nil {
		return page.Page[*Endpoint]{}, err
	}

	if err = validateFilters(
		opts.Filters,
		"autoDeploymentMode",
		"endpointType",
		filterState,
		"updateStatus",
	); err != nil {
		return page.Page[*Endpoint]{}, err
	}

	if err = validateListOptions(opts); err != nil {
		return page.Page[*Endpoint]{}, err
	}

	b.mu.Lock("ListWebFunctionEndpoints")
	defer b.mu.Unlock()

	b.advanceLocked()

	if _, err = b.liveFunctionLocked(r, name); err != nil {
		return page.Page[*Endpoint]{}, err
	}

	var all []*Endpoint

	for _, e := range b.endpointsOfLocked(r, name) {
		attrs := map[string]string{
			"autoDeploymentMode": e.AutoDeploymentMode, "endpointType": e.Type, filterState: e.State,
			"updateStatus": e.UpdateStatus,
		}
		if matchesFilters(opts.Filters, attrs) {
			all = append(all, b.endpointView(e))
		}
	}

	return page.New(all, opts.NextToken, opts.MaxResults, defaultListLimit), nil
}

// UpdateWebFunctionEndpoint applies a partial endpoint update; the change is in effect once updateStatus is Successful.
func (b *InMemoryBackend) UpdateWebFunctionEndpoint(
	region, fnRef, epRef string, up EndpointUpdate,
) (*Endpoint, error) {
	r, name, err := b.functionRef(fnRef, region)
	if err != nil {
		return nil, err
	}

	if err = validateUpdate(up); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateWebFunctionEndpoint")
	defer b.mu.Unlock()

	b.advanceLocked()

	if _, err = b.liveFunctionLocked(r, name); err != nil {
		return nil, err
	}

	e, err := b.endpointLocked(r, name, epRef)
	if err != nil {
		return nil, err
	}

	if e.State != stateActive || e.UpdateStatus == updateInProgress {
		return nil, conflictError(resEndpoint, e.Name,
			"Endpoint %s can't be updated while it is %s or has an update in progress.", e.Name, e.State)
	}

	weights, mode, err := b.planUpdateLocked(e, up)
	if err != nil {
		return nil, err
	}

	e.Weights, e.AutoDeploymentMode = weights, mode

	if up.AuthType != "" {
		e.AuthType = up.AuthType
	}

	if up.Description != nil {
		e.Description = *up.Description
	}

	applyScaling(e, up.Scaling)
	applyThrottle(e, up.Throttle)

	now := b.now().UTC()
	e.UpdatedAt, e.UpdateStatus, e.UpdateReason = now, updateInProgress, "The endpoint update is in progress."
	e.UpdatePendingUntil = now.Add(b.dwell(endpointUpdateDelay))

	return b.endpointView(e), nil
}

func validateUpdate(up EndpointUpdate) error {
	if up.AuthType != "" {
		if err := validateAuthType(up.AuthType); err != nil {
			return err
		}
	}

	if up.AutoDeploymentMode != "" {
		if err := validateMode(up.AutoDeploymentMode); err != nil {
			return err
		}
	}

	if err := validateScaling(up.Scaling); err != nil {
		return err
	}

	return validateThrottle(up.Throttle)
}

// planUpdateLocked computes the stored weights and mode an update results in.
func (b *InMemoryBackend) planUpdateLocked(e *Endpoint, up EndpointUpdate) ([]RevisionWeight, string, error) {
	mode := e.AutoDeploymentMode
	if up.AutoDeploymentMode != "" {
		mode = up.AutoDeploymentMode
	}

	if mode == modeLatestRevision {
		return b.planLatestLocked(e, up)
	}

	if up.WeightsSet {
		w, err := b.resolveWeightsLocked(e.Region, e.FunctionName, e.Type, up.Weights)

		return w, mode, err
	}

	if e.AutoDeploymentMode == modeLatestRevision {
		return b.effectiveWeightsLocked(e), mode, nil
	}

	return slices.Clone(e.Weights), mode, nil
}

func (b *InMemoryBackend) planLatestLocked(
	e *Endpoint,
	up EndpointUpdate,
) ([]RevisionWeight, string, error) {
	if up.WeightsSet {
		return nil, "", validationError(
			"revisionWeights can't be set while autoDeploymentMode is %s.",
			modeLatestRevision,
		)
	}

	if e.Type != endpointHomeRegion {
		return nil, "", validationError("autoDeploymentMode %s is supported only for %s endpoints.",
			modeLatestRevision, endpointHomeRegion)
	}

	if e.AutoDeploymentMode == modeLatestRevision {
		return nil, modeLatestRevision, nil
	}

	revs := b.revisionsOfLocked(e.Region, e.FunctionName)
	if len(revs) == 0 || revs[0].State != stateActive {
		return nil, "", conflictError(resEndpoint, e.Name,
			"The latest revision must be Active before switching to %s.", modeLatestRevision)
	}

	return nil, modeLatestRevision, nil
}

// DeleteWebFunctionEndpoint deletes an endpoint; its domain stops serving immediately.
func (b *InMemoryBackend) DeleteWebFunctionEndpoint(region, fnRef, epRef string) error {
	r, name, err := b.functionRef(fnRef, region)
	if err != nil {
		return err
	}

	b.mu.Lock("DeleteWebFunctionEndpoint")
	defer b.mu.Unlock()

	b.advanceLocked()

	if _, err = b.liveFunctionLocked(r, name); err != nil {
		return err
	}

	e, err := b.endpointLocked(r, name, epRef)
	if err != nil {
		return err
	}

	if e.State == stateDeleting {
		return notFoundError(resEndpoint, e.Name)
	}

	now := b.now().UTC()
	e.State, e.StateReason, e.UpdatedAt = stateDeleting, "The endpoint is being deleted.", now
	e.PendingUntil = now.Add(b.dwell(deletionDelay))

	return nil
}
