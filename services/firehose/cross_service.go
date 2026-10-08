package firehose

import (
	"fmt"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/service"

	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

type siblingServices interface {
	GetEC2Handler() service.Registerable
}

// SetAppConfig records the AppContext.Config so EC2 can be resolved lazily.
func (b *InMemoryBackend) SetAppConfig(cfg any) { b.appConfig = cfg }

func (b *InMemoryBackend) ec2Backend(region string) (ec2backend.Backend, bool) {
	s, ok := b.appConfig.(siblingServices)
	if !ok {
		return nil, false
	}

	h, ok := s.GetEC2Handler().(*ec2backend.Handler)
	if !ok || h == nil {
		return nil, false
	}

	return h.BackendFor(region), true
}

// resolveVpcConfiguration validates a destination's subnets and security groups against EC2
// and fills in the VpcId they belong to. Unwired EC2 leaves the description untouched.
func (b *InMemoryBackend) resolveVpcConfiguration(region string, d *VpcConfigurationDescription) error {
	if d == nil {
		return nil
	}

	bk, ok := b.ec2Backend(region)
	if !ok {
		return nil
	}

	var vpcID string

	for _, id := range d.SubnetIDs {
		subnets := bk.DescribeSubnets([]string{id})
		if len(subnets) == 0 {
			return fmt.Errorf("%w: VpcConfiguration subnet %s does not exist", ErrValidation, id)
		}

		if vpcID != "" && subnets[0].VPCID != vpcID {
			return fmt.Errorf("%w: VpcConfiguration subnets must all belong to the same VPC", ErrValidation)
		}

		vpcID = subnets[0].VPCID
	}

	for _, id := range d.SecurityGroupIDs {
		groups := bk.DescribeSecurityGroups([]string{id})
		if len(groups) == 0 {
			return fmt.Errorf("%w: VpcConfiguration security group %s does not exist", ErrValidation, id)
		}

		if vpcID != "" &&
			!slices.ContainsFunc(groups, func(g *ec2backend.SecurityGroup) bool { return g.VPCID == vpcID }) {
			return fmt.Errorf("%w: VpcConfiguration security group %s is not in VPC %s", ErrValidation, id, vpcID)
		}
	}

	d.VpcID = vpcID

	return nil
}

func (b *InMemoryBackend) resolveVpcConfigurations(region string, input *CreateDeliveryStreamInput) error {
	if d := input.OpenSearchDestination; d != nil {
		if err := b.resolveVpcConfiguration(region, d.VpcConfigurationDescription); err != nil {
			return err
		}
	}

	if d := input.ElasticsearchDestination; d != nil {
		return b.resolveVpcConfiguration(region, d.VpcConfigurationDescription)
	}

	return nil
}
