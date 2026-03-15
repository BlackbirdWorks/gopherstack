package ssm

// Exported wrappers for internal state used in tests.

// MaxHistoryCap exposes the history cap constant for test assertions.
const MaxHistoryCap = maxHistoryCap

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
