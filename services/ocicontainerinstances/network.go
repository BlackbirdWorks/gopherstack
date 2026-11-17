package ocicontainerinstances

// VnicRequest asks the network for the VNIC that backs a container instance.
type VnicRequest struct {
	IsPublic      *bool
	CompartmentID string
	AD            string
	SubnetID      string
	DisplayName   string
	HostnameLabel string
	PrivateIP     string
	NsgIDs        []string
}

// VnicAllocation is the VNIC created for an instance.
type VnicAllocation struct {
	VnicID    string
	PrivateIP string
	PublicIPs []string
}

// Network backs instance VNICs with OCI Core.
type Network struct {
	Allocate func(VnicRequest) (VnicAllocation, error)
	Release  func(vnicID string)
}

// SetNetwork wires instance VNICs to OCI Core.
func (b *InMemoryBackend) SetNetwork(n Network) {
	b.mu.Lock("SetNetwork")
	b.network = &n
	b.mu.Unlock()
}

// allocateVnics creates the VNIC of a create request in OCI Core; nil when no network is wired or the request
// has no usable VNIC entry (validation reports that later).
func (b *InMemoryBackend) allocateVnics(in createIn) ([]VnicAllocation, error) {
	b.mu.RLock("allocateVnics")
	n := b.network
	b.mu.RUnlock()

	if n == nil || n.Allocate == nil || len(in.Vnics) != maxVnics || in.Vnics[0].SubnetID == "" {
		return nil, nil
	}

	v := in.Vnics[0]

	alloc, err := n.Allocate(VnicRequest{
		CompartmentID: in.Compartment, AD: in.AD, SubnetID: v.SubnetID, DisplayName: v.DisplayName,
		HostnameLabel: v.HostnameLabel, PrivateIP: v.PrivateIP, NsgIDs: v.NsgIDs, IsPublic: v.IsPublic,
	})
	if err != nil {
		return nil, err
	}

	return []VnicAllocation{alloc}, nil
}

func (b *InMemoryBackend) releaseVnics(ids []string) {
	b.mu.RLock("releaseVnics")
	n := b.network
	b.mu.RUnlock()

	if n == nil || n.Release == nil {
		return
	}

	for _, id := range ids {
		n.Release(id)
	}
}

func allocIDs(a []VnicAllocation) []string {
	out := make([]string, 0, len(a))
	for _, v := range a {
		out = append(out, v.VnicID)
	}

	return out
}
