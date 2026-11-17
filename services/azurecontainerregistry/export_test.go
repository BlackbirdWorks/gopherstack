package azurecontainerregistry

// SetPlannedHook runs fn once the retention sweep or blob collection has decided what to delete, with the
// delete gate held.
func (b *InMemoryBackend) SetPlannedHook(fn func()) { b.onPlanned = fn }

// GateWaiters reports how many goroutines wait on the registry's gate.
func (b *InMemoryBackend) GateWaiters(reg string) int {
	g := b.gateOf(reg)
	if g == nil {
		return 0
	}

	return int(g.ReadWaiters() + g.WriteWaiters())
}
