package main

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	vpclatticebackend "github.com/blackbirdworks/gopherstack/services/vpclattice"
)

type latticeEndpointDirectory struct {
	ec2 func(region string) *ec2backend.InMemoryBackend
}

func latticeEndpointRefs(eps []*ec2backend.VpcEndpoint) []vpclatticebackend.VpcEndpointRef {
	out := make([]vpclatticebackend.VpcEndpointRef, 0, len(eps))
	for _, ep := range eps {
		out = append(out, vpclatticebackend.VpcEndpointRef{
			CreatedAt: ep.CreateTime, ID: ep.ID, VpcID: ep.VPCID, OwnerID: ep.OwnerID, State: ep.State,
		})
	}

	return out
}

func (d *latticeEndpointDirectory) ServiceNetworkEndpoints(region, arn string) []vpclatticebackend.VpcEndpointRef {
	return latticeEndpointRefs(d.ec2(region).VpcEndpointsByServiceNetworkArn(arn))
}

func (d *latticeEndpointDirectory) ResourceConfigurationEndpoints(
	region, arn string,
) []vpclatticebackend.VpcEndpointRef {
	return latticeEndpointRefs(d.ec2(region).VpcEndpointsByResourceConfigurationArn(arn))
}

func (d *latticeEndpointDirectory) DisassociateResourceConfiguration(region, endpointID string) error {
	return d.ec2(region).DisassociateVpcEndpointResourceConfiguration(endpointID)
}

// wireVPCLatticeEndpoints feeds EC2 Resource and ServiceNetwork VPC endpoints into VPC Lattice's
// endpoint association lists.
func wireVPCLatticeEndpoints(latticeReg, ec2Reg service.Registerable) {
	latticeH, ok := latticeReg.(*vpclatticebackend.Handler)
	if !ok {
		return
	}

	latticeBk, ok := latticeH.Backend.(*vpclatticebackend.InMemoryBackend)
	if !ok {
		return
	}

	ec2H, ok := ec2Reg.(*ec2backend.Handler)
	if !ok {
		return
	}

	home, ok := ec2H.Backend.(*ec2backend.InMemoryBackend)
	if !ok {
		return
	}

	latticeBk.SetEndpointDirectory(&latticeEndpointDirectory{ec2: regionalEC2Backend(ec2H, home)})
}
