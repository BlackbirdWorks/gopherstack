package codebuild

import (
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// AddSandboxInternal seeds a Sandbox directly into the backend (test helper).
func (b *InMemoryBackend) AddSandboxInternal(s *Sandbox) {
	b.mu.Lock("AddSandboxInternal")
	defer b.mu.Unlock()

	b.sandboxes.Put(s)
}

// BatchGetSandboxes returns sandboxes by ID or ARN. Missing IDs are returned separately.
func (b *InMemoryBackend) BatchGetSandboxes(ids []string) ([]*Sandbox, []string) {
	b.mu.RLock("BatchGetSandboxes")
	defer b.mu.RUnlock()

	found := make([]*Sandbox, 0, len(ids))
	notFound := make([]string, 0, len(ids))

	for _, id := range ids {
		if s, ok := b.sandboxes.Get(id); ok {
			out := *s
			found = append(found, &out)
		} else {
			notFound = append(notFound, id)
		}
	}

	return found, notFound
}

// ListSandboxes returns all sandbox IDs in sorted order.
func (b *InMemoryBackend) ListSandboxes() []string {
	b.mu.RLock("ListSandboxes")
	defer b.mu.RUnlock()

	items := b.sandboxes.Snapshot()
	ids := make([]string, len(items))

	for i, s := range items {
		ids[i] = s.ID
	}

	return ids
}

// StartSandbox creates a new sandbox for a project.
func (b *InMemoryBackend) StartSandbox(projectName string) (*Sandbox, error) {
	b.mu.Lock("StartSandbox")
	defer b.mu.Unlock()

	if !b.projects.Has(projectName) {
		return nil, ErrNotFound
	}

	id := uuid.NewString()
	sandboxArn := arn.Build("codebuild", b.region, b.accountID, "sandbox/"+id)
	sb := &Sandbox{
		ID:          id,
		Arn:         sandboxArn,
		ProjectName: projectName,
		Status:      "READY",
		StartTime:   float64(time.Now().Unix()),
	}
	b.sandboxes.Put(sb)

	out := *sb

	return &out, nil
}

// StopSandbox marks a sandbox as STOPPED.
func (b *InMemoryBackend) StopSandbox(id string) (*Sandbox, error) {
	b.mu.Lock("StopSandbox")
	defer b.mu.Unlock()

	sb, ok := b.sandboxes.Get(id)
	if !ok {
		return nil, ErrNotFound
	}

	sb.Status = buildStatusStopped
	sb.EndTime = float64(time.Now().Unix())
	out := *sb

	return &out, nil
}

// ListSandboxesForProject returns all sandbox IDs for a project in sorted order.
func (b *InMemoryBackend) ListSandboxesForProject(projectName string) ([]string, error) {
	b.mu.RLock("ListSandboxesForProject")
	defer b.mu.RUnlock()

	if !b.projects.Has(projectName) {
		return nil, ErrNotFound
	}

	group := b.sandboxesByProject.Get(projectName)
	ids := make([]string, len(group))

	for i, s := range group {
		ids[i] = s.ID
	}

	sort.Strings(ids)

	return ids, nil
}
