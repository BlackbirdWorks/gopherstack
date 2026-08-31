package omics

import (
	"fmt"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ────────────────────────────────────────────────────────────────────────────
// Workflow
// ────────────────────────────────────────────────────────────────────────────

// CreateWorkflow creates a new workflow. StorageCapacity/StorageType are
// stored/echoed exactly as given, with no fabricated default: unlike
// StartRunInput.StorageType/.StorageCapacity (which do state fixed defaults,
// applied in startRunDefaults), CreateWorkflowInput's own doc comments for
// these two fields only describe what the value means for runs that inherit
// it -- neither states what CreateWorkflow itself defaults to when the
// field is omitted, so no value is invented here. ParameterTemplate is
// likewise stored/echoed only when explicitly supplied -- see the doc
// comment on Workflow.ParameterTemplate.
func (b *InMemoryBackend) CreateWorkflow(input CreateWorkflowInput) (*Workflow, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock("CreateWorkflow")
	defer b.mu.Unlock()

	id := newID()
	wf := &Workflow{
		ID:                id,
		Name:              input.Name,
		Description:       input.Description,
		Engine:            input.Engine,
		Type:              workflowTypePrivate,
		StorageType:       input.StorageType,
		StorageCapacity:   input.StorageCapacity,
		ParameterTemplate: input.ParameterTemplate,
		UUID:              newID(),
		Status:            statusCreating,
		Tags:              copyTags(input.Tags),
		CreationTime:      time.Now().UTC(),
	}
	wf.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "workflow/"+id)

	b.workflows.Put(wf)

	if input.Tags != nil {
		b.tags[wf.Arn] = copyTags(input.Tags)
	}

	result := *wf

	return &result, nil
}

// DeleteWorkflow deletes a workflow.
func (b *InMemoryBackend) DeleteWorkflow(id string) error {
	b.mu.Lock("DeleteWorkflow")
	defer b.mu.Unlock()

	wf, ok := b.workflows.Get(id)
	if !ok {
		return fmt.Errorf("%w: workflow %s not found", ErrNotFound, id)
	}

	delete(b.tags, wf.Arn)
	b.workflows.Delete(id)

	for _, v := range slices.Clone(b.workflowVersionsByWorkflow.Get(id)) {
		b.workflowVersions.Delete(parentKey(id, v.VersionName))
	}

	return nil
}

// GetWorkflow retrieves a workflow, advancing CREATING→ACTIVE on first poll
// (real WorkflowActiveWaiter clients poll GetWorkflow until Status == ACTIVE).
func (b *InMemoryBackend) GetWorkflow(id string) (*Workflow, error) {
	b.mu.Lock("GetWorkflow")
	defer b.mu.Unlock()

	wf, ok := b.workflows.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: workflow %s not found", ErrNotFound, id)
	}

	if wf.Status == statusCreating {
		wf.pollCount++
		if wf.pollCount >= 1 {
			wf.Status = statusActive
		}
	}

	result := *wf

	return &result, nil
}

// ListWorkflows lists workflows, optionally filtered by name/type (real AWS
// ListWorkflowsInput query parameters).
func (b *InMemoryBackend) ListWorkflows(
	filter *WorkflowFilter,
	maxResults int,
	nextToken string,
) ([]*Workflow, string, error) {
	b.mu.RLock("ListWorkflows")
	defer b.mu.RUnlock()

	all := b.workflows.All()
	ids := make([]string, 0, len(all))

	for _, wf := range all {
		if filter != nil {
			if filter.Name != "" && wf.Name != filter.Name {
				continue
			}

			if filter.Type != "" && wf.Type != filter.Type {
				continue
			}
		}

		ids = append(ids, wf.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.workflows.Get)

	return result, outToken, nil
}

// UpdateWorkflow updates a workflow.
func (b *InMemoryBackend) UpdateWorkflow(id, name, description, storageType string, storageCapacity *int) error {
	b.mu.Lock("UpdateWorkflow")
	defer b.mu.Unlock()

	wf, ok := b.workflows.Get(id)
	if !ok {
		return fmt.Errorf("%w: workflow %s not found", ErrNotFound, id)
	}

	if name != "" {
		wf.Name = name
	}

	if description != "" {
		wf.Description = description
	}

	if storageType != "" {
		wf.StorageType = storageType
	}

	if storageCapacity != nil {
		wf.StorageCapacity = storageCapacity
	}

	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// WorkflowVersion
// ────────────────────────────────────────────────────────────────────────────

// CreateWorkflowVersion creates a new workflow version.
// CreateWorkflowVersion creates a new workflow version. StorageCapacity/
// StorageType/ParameterTemplate are stored/echoed exactly as given -- see
// the doc comment on CreateWorkflow for why no default is fabricated.
func (b *InMemoryBackend) CreateWorkflowVersion(input CreateWorkflowVersionInput) (*WorkflowVersion, error) {
	b.mu.Lock("CreateWorkflowVersion")
	defer b.mu.Unlock()

	wf, ok := b.workflows.Get(input.WorkflowID)
	if !ok {
		return nil, fmt.Errorf("%w: workflow %s not found", ErrNotFound, input.WorkflowID)
	}

	if b.workflowVersions.Has(parentKey(input.WorkflowID, input.VersionName)) {
		return nil, fmt.Errorf(
			"%w: workflow version %s already exists",
			ErrAlreadyExists,
			input.VersionName,
		)
	}

	wv := &WorkflowVersion{
		WorkflowID:        input.WorkflowID,
		VersionName:       input.VersionName,
		Description:       input.Description,
		Engine:            wf.Engine,
		Type:              wf.Type,
		StorageType:       input.StorageType,
		StorageCapacity:   input.StorageCapacity,
		ParameterTemplate: input.ParameterTemplate,
		Status:            statusCreating,
		Tags:              copyTags(input.Tags),
		CreationTime:      time.Now().UTC(),
	}
	wv.Arn = arn.Build(
		"omics",
		b.defaultRegion,
		b.accountID,
		fmt.Sprintf("workflow/%s/version/%s", input.WorkflowID, input.VersionName),
	)

	b.workflowVersions.Put(wv)

	if input.Tags != nil {
		b.tags[wv.Arn] = copyTags(input.Tags)
	}

	result := *wv

	return &result, nil
}

// DeleteWorkflowVersion deletes a workflow version.
func (b *InMemoryBackend) DeleteWorkflowVersion(workflowID, versionName string) error {
	b.mu.Lock("DeleteWorkflowVersion")
	defer b.mu.Unlock()

	if !b.workflows.Has(workflowID) {
		return fmt.Errorf("%w: workflow %s not found", ErrNotFound, workflowID)
	}

	wv, ok := b.workflowVersions.Get(parentKey(workflowID, versionName))
	if !ok {
		return fmt.Errorf("%w: workflow version %s not found", ErrNotFound, versionName)
	}

	delete(b.tags, wv.Arn)
	b.workflowVersions.Delete(parentKey(workflowID, versionName))

	return nil
}

// GetWorkflowVersion retrieves a workflow version, advancing CREATING→ACTIVE
// on first poll (real WorkflowVersionActiveWaiter clients poll until Status
// == ACTIVE).
func (b *InMemoryBackend) GetWorkflowVersion(
	workflowID, versionName string,
) (*WorkflowVersion, error) {
	b.mu.Lock("GetWorkflowVersion")
	defer b.mu.Unlock()

	if !b.workflows.Has(workflowID) {
		return nil, fmt.Errorf("%w: workflow %s not found", ErrNotFound, workflowID)
	}

	wv, ok := b.workflowVersions.Get(parentKey(workflowID, versionName))
	if !ok {
		return nil, fmt.Errorf("%w: workflow version %s not found", ErrNotFound, versionName)
	}

	if wv.Status == statusCreating {
		wv.pollCount++
		if wv.pollCount >= 1 {
			wv.Status = statusActive
		}
	}

	result := *wv

	return &result, nil
}

// ListWorkflowVersions lists versions of a workflow, optionally filtered by
// type (real AWS ListWorkflowVersionsInput "type" query parameter).
//
//nolint:dupl // structurally-identical parent-scoped List op (already deduped via listChildFiltered)
func (b *InMemoryBackend) ListWorkflowVersions(
	workflowID string,
	filter *WorkflowVersionFilter,
	maxResults int,
	nextToken string,
) ([]*WorkflowVersion, string, error) {
	b.mu.RLock("ListWorkflowVersions")
	defer b.mu.RUnlock()

	if !b.workflows.Has(workflowID) {
		return nil, "", fmt.Errorf("%w: workflow %s not found", ErrNotFound, workflowID)
	}

	group := b.workflowVersionsByWorkflow.Get(workflowID)
	result, outToken := listChildFiltered(
		group,
		func(wv *WorkflowVersion) string { return wv.VersionName },
		func(wv *WorkflowVersion) bool { return filter == nil || filter.Type == "" || wv.Type == filter.Type },
		nextToken, maxResults,
		func(id string) (*WorkflowVersion, bool) { return b.workflowVersions.Get(parentKey(workflowID, id)) },
	)

	return result, outToken, nil
}

// UpdateWorkflowVersion updates a workflow version.
func (b *InMemoryBackend) UpdateWorkflowVersion(
	workflowID, versionName, description, storageType string, storageCapacity *int,
) error {
	b.mu.Lock("UpdateWorkflowVersion")
	defer b.mu.Unlock()

	if !b.workflows.Has(workflowID) {
		return fmt.Errorf("%w: workflow %s not found", ErrNotFound, workflowID)
	}

	wv, ok := b.workflowVersions.Get(parentKey(workflowID, versionName))
	if !ok {
		return fmt.Errorf("%w: workflow version %s not found", ErrNotFound, versionName)
	}

	if description != "" {
		wv.Description = description
	}

	if storageType != "" {
		wv.StorageType = storageType
	}

	if storageCapacity != nil {
		wv.StorageCapacity = storageCapacity
	}

	return nil
}
