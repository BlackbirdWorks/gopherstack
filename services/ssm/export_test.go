package ssm

import "time"

// Exported wrappers for internal state used in tests.

// DefaultJanitorInterval exposes the package default janitor interval for testing.
const DefaultJanitorInterval = defaultSSMJanitorInterval

// DefaultCommandExpirySecs exposes the package default command expiry seconds for testing.
const DefaultCommandExpirySecs = defaultCommandExpirySecs

// MaxHistoryCap exposes the history cap constant for test assertions.
const MaxHistoryCap = maxHistoryCap

// MaxDocumentVersionCap exposes the document version cap constant for test assertions.
const MaxDocumentVersionCap = maxDocumentVersionCap

// HistoryLen returns the number of history entries stored for the given parameter name.
func (b *InMemoryBackend) HistoryLen(name string) int {
	b.mu.RLock("HistoryLen")
	defer b.mu.RUnlock()

	return len(b.history[name])
}

// CommandCount returns the number of commands currently stored.
func (b *InMemoryBackend) CommandCount() int {
	b.mu.RLock("CommandCount")
	defer b.mu.RUnlock()

	return len(b.commands)
}

// CommandInvocationCount returns the number of command invocation sets stored.
func (b *InMemoryBackend) CommandInvocationCount() int {
	b.mu.RLock("CommandInvocationCount")
	defer b.mu.RUnlock()

	return len(b.commandInvocations)
}

// SetCommandExpiresAfter overrides the ExpiresAfter timestamp of the given command.
// Used in tests to force a command into an expired state.
func (b *InMemoryBackend) SetCommandExpiresAfter(cmdID string, expiresAfter float64) {
	b.mu.Lock("SetCommandExpiresAfter")
	defer b.mu.Unlock()

	if cmd, ok := b.commands[cmdID]; ok {
		cmd.ExpiresAfter = expiresAfter
		b.commands[cmdID] = cmd
	}
}

// HasTagEntry reports whether the tags map contains an entry for the given parameter name.
// Returns false when the parameter's tag entry has been cleaned up (nil or absent).
func (b *InMemoryBackend) HasTagEntry(name string) bool {
	b.mu.RLock("HasTagEntry")
	defer b.mu.RUnlock()

	return b.tags[name] != nil
}

// DocumentVersionCount returns the number of versions stored for the given document.
func (b *InMemoryBackend) DocumentVersionCount(name string) int {
	b.mu.RLock("DocumentVersionCount")
	defer b.mu.RUnlock()

	return len(b.documentVersions[name])
}

// GetJanitorInterval returns the Interval configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the interval.
func (h *Handler) GetJanitorInterval() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.Interval
}

// GetJanitorTaskTimeout returns the TaskTimeout configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the timeout.
func (h *Handler) GetJanitorTaskTimeout() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.TaskTimeout
}

// GetCommandExpirySecs returns the commandExpirySecs configured on the backend.
// Used in tests to verify WithCommandTTL correctly propagates the TTL.
func (b *InMemoryBackend) GetCommandExpirySecs() float64 {
b.mu.RLock("GetCommandExpirySecs")
defer b.mu.RUnlock()

return b.commandExpirySecs
}
