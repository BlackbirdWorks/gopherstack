package azuresql

import "strings"

// ServerIDByName returns the id of the server whose resource name is name.
func (b *InMemoryBackend) ServerIDByName(name string) (string, bool) {
	b.mu.RLock("ServerIDByName")
	defer b.mu.RUnlock()

	suffix := "/servers/" + strings.ToLower(name)

	for key := range b.servers {
		if strings.HasSuffix(key, suffix) {
			return key, true
		}
	}

	return "", false
}
