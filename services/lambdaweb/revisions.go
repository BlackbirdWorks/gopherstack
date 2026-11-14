package lambdaweb

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func (b *InMemoryBackend) newRevisionLocked(f *Function, in *RevisionInput, now time.Time) *Revision {
	id := newRevisionID()
	for _, ok := b.revisions.Get(revisionKey(f.Region, f.Name, id)); ok; _, ok = b.revisions.Get(
		revisionKey(f.Region, f.Name, id)) {
		id = newRevisionID()
	}

	sc := in.ServiceConfig
	sc.EnvironmentVariables = maps.Clone(in.ServiceConfig.EnvironmentVariables)

	if sc.Logging != nil {
		lc := *sc.Logging
		if lc.LogGroup == "" {
			lc.LogGroup = "/aws/lambda/web/" + f.Name
		}

		sc.Logging = &lc
	}

	return &Revision{
		ID: id, ARN: revisionARN(f.Region, f.AccountID, f.Name, id), Region: f.Region, AccountID: f.AccountID,
		FunctionName: f.Name, Description: in.Description, KmsKeyArn: in.KmsKeyArn, Runtime: in.Runtime, S3: in.S3,
		ServiceConfig: sc, State: statePending, StateReason: "The revision is being created.", CreatedAt: now,
		PendingUntil: now.Add(b.dwell(revisionActivationDelay)),
	}
}

// CreateWebFunctionRevision creates an immutable revision.
func (b *InMemoryBackend) CreateWebFunctionRevision(
	ctx context.Context, region, ref string, in RevisionInput,
) (*Revision, error) {
	r, name, err := b.functionRef(ref, region)
	if err != nil {
		return nil, err
	}

	if err = normalizeRevision(&in); err != nil {
		return nil, err
	}

	if err = b.pinCode(ctx, &in.S3); err != nil {
		return nil, err
	}

	if err = b.checkKey(ctx, r, in.KmsKeyArn); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateWebFunctionRevision")
	defer b.mu.Unlock()

	b.advanceLocked()

	f, err := b.liveFunctionLocked(r, name)
	if err != nil {
		return nil, err
	}

	rev := b.newRevisionLocked(f, &in, b.now().UTC())
	b.revisions.Put(rev)

	return rev.clone(), nil
}

func (b *InMemoryBackend) revisionLocked(region, fn, ref string) (*Revision, error) {
	id, err := b.childRef(ref, resRevision, region, fn)
	if err != nil {
		return nil, err
	}

	rev, ok := b.revisions.Get(revisionKey(region, fn, id))
	if !ok {
		return nil, notFoundError(resRevision, id)
	}

	return rev, nil
}

// GetWebFunctionRevision returns one revision.
func (b *InMemoryBackend) GetWebFunctionRevision(region, fnRef, revRef string) (*Revision, error) {
	r, name, err := b.functionRef(fnRef, region)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("GetWebFunctionRevision")
	defer b.mu.Unlock()

	b.advanceLocked()

	if _, err = b.liveFunctionLocked(r, name); err != nil {
		return nil, err
	}

	rev, err := b.revisionLocked(r, name, revRef)
	if err != nil {
		return nil, err
	}

	return rev.clone(), nil
}

// ListWebFunctionRevisions lists a function's revisions, newest first.
func (b *InMemoryBackend) ListWebFunctionRevisions(
	region, ref string, opts ListOptions,
) (page.Page[*Revision], error) {
	r, name, err := b.functionRef(ref, region)
	if err != nil {
		return page.Page[*Revision]{}, err
	}

	if err = validateFilters(opts.Filters, filterState); err != nil {
		return page.Page[*Revision]{}, err
	}

	if err = validateListOptions(opts); err != nil {
		return page.Page[*Revision]{}, err
	}

	b.mu.Lock("ListWebFunctionRevisions")
	defer b.mu.Unlock()

	b.advanceLocked()

	if _, err = b.liveFunctionLocked(r, name); err != nil {
		return page.Page[*Revision]{}, err
	}

	var all []*Revision

	for _, rev := range b.revisionsOfLocked(r, name) {
		if matchesFilters(opts.Filters, map[string]string{filterState: rev.State}) {
			all = append(all, rev.clone())
		}
	}

	return page.New(all, opts.NextToken, opts.MaxResults, defaultListLimit), nil
}

// DeleteWebFunctionRevision deletes a revision no endpoint routes traffic to.
func (b *InMemoryBackend) DeleteWebFunctionRevision(region, fnRef, revRef string) error {
	r, name, err := b.functionRef(fnRef, region)
	if err != nil {
		return err
	}

	b.mu.Lock("DeleteWebFunctionRevision")
	defer b.mu.Unlock()

	b.advanceLocked()

	if _, err = b.liveFunctionLocked(r, name); err != nil {
		return err
	}

	rev, err := b.revisionLocked(r, name, revRef)
	if err != nil {
		return err
	}

	if b.revisionsInUseLocked(r, name)[rev.ID] {
		return conflictError(resRevision, rev.ID,
			"Revision %s receives traffic from an endpoint; remove it from the revision weights first.", rev.ID)
	}

	b.revisions.Delete(revisionKey(r, name, rev.ID))
	b.run.stopRevision(r, name, rev.ID)

	return nil
}

// failRevision marks a revision Failed after its code can no longer be read.
func (b *InMemoryBackend) failRevision(region, fn, id, reason string) {
	b.mu.Lock("failRevision")
	defer b.mu.Unlock()

	rev, ok := b.revisions.Get(revisionKey(region, fn, id))
	if !ok || rev.State == stateFailed {
		return
	}

	rev.State, rev.StateReason, rev.PendingUntil = stateFailed, reason, time.Time{}
	rev.Errors = slices.Concat(rev.Errors, []RevisionError{{
		Attribute: "buildConfig.codeConfig.s3Object", ErrorCode: "CodeUnavailable", ErrorMessage: reason,
	}})
}
