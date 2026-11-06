package ocicore

// ServiceNicIn describes a VNIC created on behalf of another OCI service (for example a File Storage mount target).
type ServiceNicIn struct {
	CompartmentID string
	AD            string
	SubnetID      string
	DisplayName   string
	HostnameLabel string
	PrivateIP     string
	NsgIDs        []string
}

// ServiceNic is the VNIC and primary private IP created by CreateServiceNic.
type ServiceNic struct {
	VnicID      string
	PrivateIPID string
	IPAddress   string
}

// CreateServiceNic creates a VNIC without a public IP and returns its primary private IP.
func (b *InMemoryBackend) CreateServiceNic(in ServiceNicIn) (ServiceNic, error) {
	b.mu.Lock("CreateServiceNic")
	defer b.mu.Unlock()

	no := false

	v, aerr := b.newVnic(CreateVnicIn{
		AssignPublicIP: &no, DisplayName: in.DisplayName, HostnameLabel: in.HostnameLabel, PrivateIP: in.PrivateIP,
		SubnetID: in.SubnetID, NsgIDs: in.NsgIDs,
	}, in.CompartmentID, in.AD, false)
	if aerr != nil {
		return ServiceNic{}, aerr
	}

	out := ServiceNic{VnicID: v.ID, IPAddress: v.PrivateIP}
	if ips := liveOf(b, func(x *PrivateIP) bool { return x.VnicID == v.ID && x.IsPrimary }); len(ips) > 0 {
		out.PrivateIPID = ips[0].ID
	}

	return out, nil
}

// DeleteServiceNic terminates a VNIC made by CreateServiceNic; unknown ids are ignored.
func (b *InMemoryBackend) DeleteServiceNic(vnicID string) {
	b.mu.Lock("DeleteServiceNic")
	defer b.mu.Unlock()

	if v, ok := b.res[vnicID].(*Vnic); ok && live(v) {
		b.endVnic(v)
	}
}
