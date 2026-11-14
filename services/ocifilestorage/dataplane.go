package ocifilestorage

// dataPlane holds file system data and serves mount targets. It is nil in stub mode.
type dataPlane interface {
	CreateFS(id string) error
	RemoveFS(id string)
	Usage(id string) int64
	TakeSnapshot(fsID, name string) error
	DropSnapshot(fsID, name string)
	CloneFrom(srcFS, snapshot, dstFS string) error
	ReplicateSnapshot(srcFS, snapshot, dstFS string) error
	StartMount(mtID string, exports func(mtID string) []ExportMount) error
	StopMount(mtID string)
	StopMounts()
	Endpoints() map[string]string
	Reset()
	Close()
}

// SetDataPlane enables the NFS data plane.
func (b *InMemoryBackend) SetDataPlane(dp dataPlane) {
	b.mu.Lock("SetDataPlane")
	b.dp = dp
	b.mu.Unlock()
}

// NFSEndpoints maps each mount target id to the loopback host:port its NFS server listens on.
func (b *InMemoryBackend) NFSEndpoints() map[string]string {
	b.mu.RLock("NFSEndpoints")
	dp := b.dp
	b.mu.RUnlock()

	if dp == nil {
		return map[string]string{}
	}

	return dp.Endpoints()
}

func (b *InMemoryBackend) nicIDs() []string {
	var out []string

	for _, e := range b.mts {
		if e.W.Int != nil && e.W.Int.VnicID != "" && e.W.State != stDeleted {
			out = append(out, e.W.Int.VnicID)
		}
	}

	return out
}

func (b *InMemoryBackend) dropNics(ids []string) {
	if b.net == nil || b.net.DeleteNic == nil {
		return
	}

	for _, id := range ids {
		b.net.DeleteNic(id)
	}
}
