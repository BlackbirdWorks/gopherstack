package main

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	docdbbackend "github.com/blackbirdworks/gopherstack/services/docdb"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	medialivebackend "github.com/blackbirdworks/gopherstack/services/medialive"
	neptunebackend "github.com/blackbirdworks/gopherstack/services/neptune"
)

// ec2SubnetAdapter serves subnet lookups for services that derive VPC, AZ and network-type data from EC2.
type ec2SubnetAdapter struct {
	regions ec2Regions
}

func (a ec2SubnetAdapter) find(id string) (ec2backend.Backend, *ec2backend.Subnet) {
	for _, b := range a.regions.handler.RegionBackends() {
		if subnets := b.DescribeSubnets([]string{id}); len(subnets) > 0 {
			return b, subnets[0]
		}
	}

	return nil, nil
}

// SubnetNetwork implements docdb.SubnetResolver and neptune.SubnetResolver.
func (a ec2SubnetAdapter) SubnetNetwork(id string) (string, bool, bool) {
	b, sn := a.find(id)
	if sn == nil {
		return "", false, false
	}

	return sn.VPCID, !sn.Ipv6Native && b.SubnetHasIPv6Block(id), true
}

// SubnetAZ implements medialive.VPCNetwork.
func (a ec2SubnetAdapter) SubnetAZ(id string) (string, bool) {
	_, sn := a.find(id)
	if sn == nil {
		return "", false
	}

	return sn.AvailabilityZone, true
}

// CreateNetworkInterface implements medialive.VPCNetwork.
func (a ec2SubnetAdapter) CreateNetworkInterface(subnetID, description string) (string, error) {
	b, sn := a.find(subnetID)
	if sn == nil {
		return "", ec2backend.ErrSubnetNotFound
	}

	eni, err := b.CreateNetworkInterface(subnetID, description)
	if err != nil {
		return "", err
	}

	return eni.ID, nil
}

// DeleteNetworkInterface implements medialive.VPCNetwork.
func (a ec2SubnetAdapter) DeleteNetworkInterface(id string) {
	for _, b := range a.regions.handler.RegionBackends() {
		if b.DeleteNetworkInterface(id) == nil {
			return
		}
	}
}

// wireSubnetLookups gives DocumentDB, Neptune and MediaLive their EC2 subnet accessors.
func wireSubnetLookups(byName map[string]service.Registerable) {
	ec2H, ok := byName["EC2"].(*ec2backend.Handler)
	if !ok {
		return
	}

	adapter := ec2SubnetAdapter{regions: ec2Regions{handler: ec2H}}

	if h, hok := byName["DocDB"].(*docdbbackend.Handler); hok {
		h.Backend.SetSubnetResolver(adapter)
	}

	if h, hok := byName["Neptune"].(*neptunebackend.Handler); hok {
		if bk, bok := h.Backend.(*neptunebackend.InMemoryBackend); bok {
			bk.SetSubnetResolver(adapter)
		}
	}

	if h, hok := byName["MediaLive"].(*medialivebackend.Handler); hok {
		if bk, bok := h.Backend.(*medialivebackend.InMemoryBackend); bok {
			bk.SetVPCNetwork(adapter)
		}
	}
}
