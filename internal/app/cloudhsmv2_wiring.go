package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cloudhsmv2backend "github.com/blackbirdworks/gopherstack/services/cloudhsmv2"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

// cloudHSMNetwork backs CloudHSM clusters with EC2 subnets, security groups and HSM network interfaces.
type cloudHSMNetwork struct{ regions ec2Regions }

func (n cloudHSMNetwork) backend(region string) ec2backend.Backend {
	return n.regions.handler.BackendFor(region)
}

func (n cloudHSMNetwork) SubnetInfo(region string, ids []string) map[string]cloudhsmv2backend.Subnet {
	out := map[string]cloudhsmv2backend.Subnet{}

	b := n.backend(region)
	if b == nil {
		return out
	}

	for _, sn := range b.DescribeSubnets(ids) {
		out[sn.ID] = cloudhsmv2backend.Subnet{VpcID: sn.VPCID, Zone: sn.AvailabilityZone}
	}

	return out
}

func (n cloudHSMNetwork) CreateSecurityGroup(region, name, description, vpcID string) (string, error) {
	sg, err := n.backend(region).CreateSecurityGroup(name, description, vpcID)
	if err != nil {
		return "", err
	}

	return sg.ID, nil
}

func (n cloudHSMNetwork) DeleteSecurityGroup(region, id string) {
	if b := n.backend(region); b != nil {
		_ = b.DeleteSecurityGroup(id)
	}
}

func (n cloudHSMNetwork) CreateENI(region, subnetID, ip, securityGroupID, description string) (string, string, error) {
	opts := ec2backend.NetworkInterfaceOptions{Description: description, PrivateIP: ip}
	if securityGroupID != "" {
		opts.SecurityGroupIDs = []string{securityGroupID}
	}

	eni, err := n.backend(region).CreateNetworkInterfaceWithOptions(subnetID, opts)
	if err != nil {
		return "", "", err
	}

	return eni.ID, eni.PrivateIP, nil
}

func (n cloudHSMNetwork) DeleteENI(region, id string) {
	if b := n.backend(region); b != nil {
		_ = b.DeleteNetworkInterface(id)
	}
}

// wireCloudHSMNetwork gives CloudHSM v2 its EC2 subnet, security group and ENI accessors.
func wireCloudHSMNetwork(byName map[string]service.Registerable) {
	ec2H, ok := byName["EC2"].(*ec2backend.Handler)
	if !ok {
		return
	}

	if h, hok := byName["CloudHSMV2"].(*cloudhsmv2backend.Handler); hok {
		h.Backend.SetNetwork(cloudHSMNetwork{regions: ec2Regions{handler: ec2H}})
	}
}
