package lambdaweb

import (
	"context"
	"maps"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	defaultListLimit = 50
	maxListLimit     = 50
)

// CreateWebFunction creates a function with its optional initial revision and endpoint.
func (b *InMemoryBackend) CreateWebFunction(
	ctx context.Context, region string, in CreateFunctionInput,
) (*CreatedFunction, error) {
	if err := b.validateCreateFunction(&in, region); err != nil {
		return nil, err
	}

	if in.Revision != nil {
		if err := b.pinCode(ctx, &in.Revision.S3); err != nil {
			return nil, err
		}

		if err := b.checkKey(ctx, region, in.Revision.KmsKeyArn); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("CreateWebFunction")
	defer b.mu.Unlock()

	b.advanceLocked()

	if _, ok := b.functions.Get(functionKey(region, in.Name)); ok {
		return nil, conflictError(resFunction, in.Name, "Web function %s already exists.", in.Name)
	}

	now := b.now().UTC()
	f := &Function{
		Name: in.Name, ARN: functionARN(region, b.accountID, in.Name), Region: region, AccountID: b.accountID,
		State: statePending, StateReason: "The function is being created.", CreatedAt: now, UpdatedAt: now,
		PendingUntil: now.Add(b.dwell(functionActivationDelay)), Tags: maps.Clone(in.Tags),
	}

	if f.Tags == nil {
		f.Tags = map[string]string{}
	}

	b.functions.Put(f)

	out := &CreatedFunction{Function: f.clone()}

	if in.Revision == nil {
		return out, nil
	}

	rev := b.newRevisionLocked(f, in.Revision, now)
	b.revisions.Put(rev)
	out.Revision = rev.clone()

	if in.Endpoint != nil {
		in.Endpoint.Weights = nil
		if in.Endpoint.AutoDeploymentMode == modeDisabled {
			in.Endpoint.Weights = []RevisionWeight{{RevisionID: rev.ID, Weight: percentTotal}}
		}

		ep := b.newEndpointLocked(f, in.Endpoint, now)
		b.endpoints.Put(ep)
		out.Endpoint = b.endpointView(ep)
	}

	return out, nil
}

func (b *InMemoryBackend) validateCreateFunction(in *CreateFunctionInput, region string) error {
	if err := validateName("web function", in.Name); err != nil {
		return err
	}

	if err := validateTags(in.Tags); err != nil {
		return err
	}

	if in.Endpoint != nil && in.Revision == nil {
		return validationError("An initial endpoint requires revisionConfig.")
	}

	if in.Revision != nil {
		if err := normalizeRevision(in.Revision); err != nil {
			return err
		}
	}

	if in.Endpoint != nil {
		if err := normalizeEndpoint(in.Endpoint, region); err != nil {
			return err
		}

		if len(in.Endpoint.Weights) > 0 {
			return validationError(
				"revisionWeights can't be set in endpointConfig; the initial revision gets all traffic.",
			)
		}

		return b.checkEgressLocked(in.Revision, in.Endpoint.Type)
	}

	return nil
}

func (b *InMemoryBackend) checkEgressLocked(rev *RevisionInput, endpointType string) error {
	if rev != nil && rev.ServiceConfig.EgressNetworkConnectorArn != "" && endpointType != endpointHomeRegion {
		return validationError("Revisions with an egress network connector support only %s endpoints.",
			endpointHomeRegion)
	}

	return nil
}

// GetWebFunction returns one function.
func (b *InMemoryBackend) GetWebFunction(region, ref string) (*Function, error) {
	r, name, err := b.functionRef(ref, region)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("GetWebFunction")
	defer b.mu.Unlock()

	b.advanceLocked()

	f, err := b.functionLocked(r, name)
	if err != nil {
		return nil, err
	}

	return f.clone(), nil
}

// ListWebFunctions lists the Region's functions ordered by name.
func (b *InMemoryBackend) ListWebFunctions(region string, opts ListOptions) (page.Page[*Function], error) {
	if err := validateFilters(opts.Filters, filterState); err != nil {
		return page.Page[*Function]{}, err
	}

	if err := validateListOptions(opts); err != nil {
		return page.Page[*Function]{}, err
	}

	b.mu.Lock("ListWebFunctions")
	defer b.mu.Unlock()

	b.advanceLocked()

	var all []*Function

	for _, f := range b.functions.All() {
		if f.Region == region && matchesFilters(opts.Filters, map[string]string{filterState: f.State}) {
			all = append(all, f.clone())
		}
	}

	slices.SortFunc(all, func(a, c *Function) int { return strings.Compare(a.Name, c.Name) })

	return page.New(all, opts.NextToken, opts.MaxResults, defaultListLimit), nil
}

func validateListOptions(opts ListOptions) error {
	if opts.MaxResults < 0 || opts.MaxResults > maxListLimit {
		return validationError("maxResults must be between 1 and %d.", maxListLimit)
	}

	if err := page.ValidateToken(opts.NextToken); err != nil {
		return validationError("Invalid nextToken.")
	}

	return nil
}

// DeleteWebFunction deletes a function with its revisions and endpoints.
func (b *InMemoryBackend) DeleteWebFunction(region, ref string) error {
	r, name, err := b.functionRef(ref, region)
	if err != nil {
		return err
	}

	b.mu.Lock("DeleteWebFunction")
	defer b.mu.Unlock()

	b.advanceLocked()

	f, err := b.liveFunctionLocked(r, name)
	if err != nil {
		return err
	}

	now := b.now().UTC()
	until := now.Add(b.dwell(deletionDelay))

	f.State, f.StateReason, f.PendingUntil, f.UpdatedAt = stateDeleting, "The function is being deleted.", until, now

	for _, e := range b.endpointsOfLocked(r, name) {
		e.State, e.StateReason, e.PendingUntil = stateDeleting, "The function is being deleted.", until
	}

	for _, rev := range b.revisionsOfLocked(r, name) {
		b.revisions.Delete(revisionKey(rev.Region, rev.FunctionName, rev.ID))
	}

	b.run.stopFunction(r, name)

	return nil
}

// AccountSettings reports the Region's quotas and function count.
func (b *InMemoryBackend) AccountSettings(region string) AccountSettings {
	b.mu.Lock("AccountSettings")
	defer b.mu.Unlock()

	b.advanceLocked()

	var n int32

	for _, f := range b.functions.All() {
		if f.Region == region {
			n++
		}
	}

	return AccountSettings{
		MaxEndpointsPerFunction: maxEndpointsPerFunction,
		MaxRevisionsPerFunction: maxRevisionsPerFunction,
		MaxTotalArmVCpus:        maxTotalArmVCpus,
		MaxTotalRateLimit:       maxTotalRateLimit,
		FunctionCount:           n,
	}
}
