package azurefile

import "sort"

// Accounts lists the storage accounts that currently hold at least one share.
func (b *InMemoryBackend) Accounts() []string {
	b.mu.RLock("Accounts")
	defer b.mu.RUnlock()

	out := make([]string, 0, len(b.accounts))

	for name, a := range b.accounts {
		if len(a.Shares) > 0 {
			out = append(out, name)
		}
	}

	sort.Strings(out)

	return out
}
