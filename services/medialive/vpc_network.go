package medialive

// VPCNetwork resolves subnets and manages the network interfaces behind a channel's VPC output.
type VPCNetwork interface {
	SubnetAZ(subnetID string) (az string, ok bool)
	CreateNetworkInterface(subnetID, description string) (string, error)
	DeleteNetworkInterface(id string)
}

// SetVPCNetwork wires the EC2 accessor used to resolve Channel.Vpc availabilityZones and networkInterfaceIds.
func (b *InMemoryBackend) SetVPCNetwork(v VPCNetwork) {
	b.mu.Lock("SetVPCNetwork")
	defer b.mu.Unlock()

	b.vpcNetwork = v
}

// provisionVpcLocked fills AvailabilityZones and creates one network interface per resolvable subnet.
func (b *InMemoryBackend) provisionVpcLocked(channelID string, vpc ChannelVpcSettings) ChannelVpcSettings {
	vpc.AvailabilityZones, vpc.NetworkInterfaceIDs = nil, nil

	if b.vpcNetwork == nil {
		return vpc
	}

	for _, subnetID := range vpc.SubnetIDs {
		az, ok := b.vpcNetwork.SubnetAZ(subnetID)
		if !ok {
			continue
		}

		vpc.AvailabilityZones = append(vpc.AvailabilityZones, az)

		eni, err := b.vpcNetwork.CreateNetworkInterface(subnetID, "MediaLive channel "+channelID)
		if err == nil {
			vpc.NetworkInterfaceIDs = append(vpc.NetworkInterfaceIDs, eni)
		}
	}

	return vpc
}

func (b *InMemoryBackend) releaseVpcLocked(vpc ChannelVpcSettings) {
	if b.vpcNetwork == nil {
		return
	}

	for _, id := range vpc.NetworkInterfaceIDs {
		b.vpcNetwork.DeleteNetworkInterface(id)
	}
}
