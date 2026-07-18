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

// CreateWorkflow creates a new workflow.
func (b *InMemoryBackend) CreateWorkflow(
	name, description, _ /* definitionZip */, _ /* definitionURI */, engine string,
	tags map[string]string,
) (*Workflow, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	b.mu.Lock("CreateWorkflow")
	defer b.mu.Unlock()

	id := newID()
	wf := &Workflow{
		ID:           id,
		Name:         name,
		Description:  description,
		Engine:       engine,
		Type:         "PRIVATE",
		Status:       statusCreating,
		Tags:         copyTags(tags),
		CreationTime: time.Now().UTC(),
	}
	wf.Arn = arn.Build("omics", b.defaultRegion, b.accountID, "workflow/"+id)

	b.workflows.Put(wf)

	if tags != nil {
		b.tags[wf.Arn] = copyTags(tags)
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

// ListWorkflows lists workflows.
func (b *InMemoryBackend) ListWorkflows(
	maxResults int,
	nextToken string,
) ([]*Workflow, string, error) {
	b.mu.RLock("ListWorkflows")
	defer b.mu.RUnlock()

	all := b.workflows.All()
	ids := make([]string, 0, len(all))

	for _, wf := range all {
		ids = append(ids, wf.ID)
	}

	result, outToken := paginatedCopies(ids, nextToken, maxResults, b.workflows.Get)

	return result, outToken, nil
}

// UpdateWorkflow updates a workflow.
func (b *InMemoryBackend) UpdateWorkflow(id, name, description string) error {
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

	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// WorkflowVersion
// ────────────────────────────────────────────────────────────────────────────

// CreateWorkflowVersion creates a new workflow version.
func (b *InMemoryBackend) CreateWorkflowVersion(
	workflowID, versionName, description string,
	tags map[string]string,
) (*WorkflowVersion, error) {
	b.mu.Lock("CreateWorkflowVersion")
	defer b.mu.Unlock()

	wf, ok := b.workflows.Get(workflowID)
	if !ok {
		return nil, fmt.Errorf("%w: workflow %s not found", ErrNotFound, workflowID)
	}

	if b.workflowVersions.Has(parentKey(workflowID, versionName)) {
		return nil, fmt.Errorf(
			"%w: workflow version %s already exists",
			ErrAlreadyExists,
			versionName,
		)
	}

	wv := &WorkflowVersion{
		WorkflowID:   workflowID,
		VersionName:  versionName,
		Description:  description,
		Engine:       wf.Engine,
		Type:         wf.Type,
		Status:       statusCreating,
		Tags:         copyTags(tags),
		CreationTime: time.Now().UTC(),
	}
	wv.Arn = arn.Build(
		"omics",
		b.defaultRegion,
		b.accountID,
		fmt.Sprintf("workflow/%s/version/%s", workflowID, versionName),
	)

	b.workflowVersions.Put(wv)

	if tags != nil {
		b.tags[wv.Arn] = copyTags(tags)
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

// ListWorkflowVersions lists versions of a workflow.
func (b *InMemoryBackend) ListWorkflowVersions(
	workflowID string,
	maxResults int,
	nextToken string,
) ([]*WorkflowVersion, string, error) {
	b.mu.RLock("ListWorkflowVersions")
	defer b.mu.RUnlock()

	if !b.workflows.Has(workflowID) {
		return nil, "", fmt.Errorf("%w: workflow %s not found", ErrNotFound, workflowID)
	}

	group := b.workflowVersionsByWorkflow.Get(workflowID)
	names := make([]string, 0, len(group))

	for _, wv := range group {
		names = append(names, wv.VersionName)
	}

	result, outToken := paginatedCopies(names, nextToken, maxResults, func(id string) (*WorkflowVersion, bool) {
		return b.workflowVersions.Get(parentKey(workflowID, id))
	})

	return result, outToken, nil
}

// UpdateWorkflowVersion updates a workflow version.
func (b *InMemoryBackend) UpdateWorkflowVersion(workflowID, versionName, description string) error {
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

	return nil
}
