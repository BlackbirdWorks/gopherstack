package codebuild

import (
	"sort"
	"time"

	"github.com/google/uuid"
)

// AddCommandExecutionInternal seeds a CommandExecution directly into the backend (test helper).
func (b *InMemoryBackend) AddCommandExecutionInternal(ce *CommandExecution) {
	b.mu.Lock("AddCommandExecutionInternal")
	defer b.mu.Unlock()

	b.commandExecutions.Put(ce)
}

// BatchGetCommandExecutions returns command executions by ID within a sandbox.
// Missing IDs are returned separately.
func (b *InMemoryBackend) BatchGetCommandExecutions(sandboxID string, ids []string) ([]*CommandExecution, []string) {
	b.mu.RLock("BatchGetCommandExecutions")
	defer b.mu.RUnlock()

	found := make([]*CommandExecution, 0, len(ids))
	notFound := make([]string, 0, len(ids))

	for _, id := range ids {
		ce, ok := b.commandExecutions.Get(id)
		if ok && ce.SandboxID == sandboxID {
			out := *ce
			found = append(found, &out)
		} else {
			notFound = append(notFound, id)
		}
	}

	return found, notFound
}

// StartCommandExecution creates a new command execution in a sandbox.
func (b *InMemoryBackend) StartCommandExecution(sandboxID, command, execType string) (*CommandExecution, error) {
	b.mu.Lock("StartCommandExecution")
	defer b.mu.Unlock()

	if !b.sandboxes.Has(sandboxID) {
		return nil, ErrNotFound
	}

	id := uuid.NewString()
	now := float64(time.Now().Unix())
	ce := &CommandExecution{
		ID:        id,
		SandboxID: sandboxID,
		Command:   command,
		Type:      execType,
		Status:    buildStatusSucceeded,
		ExitCode:  "0",
		StartTime: now,
		EndTime:   now,
	}
	b.commandExecutions.Put(ce)

	out := *ce

	return &out, nil
}

// ListCommandExecutionsForSandbox returns all command executions for a sandbox.
// Real AWS returns full CommandExecution objects, not just IDs.
func (b *InMemoryBackend) ListCommandExecutionsForSandbox(sandboxID string) ([]*CommandExecution, error) {
	b.mu.RLock("ListCommandExecutionsForSandbox")
	defer b.mu.RUnlock()

	if !b.sandboxes.Has(sandboxID) {
		return nil, ErrNotFound
	}

	group := b.commandExecutionsBySandbox.Get(sandboxID)
	out := make([]*CommandExecution, len(group))

	for i, ce := range group {
		cp := *ce
		out[i] = &cp
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out, nil
}
