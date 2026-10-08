package autoscaling

import (
	"context"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

// InstanceAttributes are the EC2 instance attributes a launch configuration derives from InstanceId.
type InstanceAttributes struct {
	ImageID        string
	InstanceType   string
	KeyName        string
	UserData       string
	SecurityGroups []string
}

// EC2Lookup reads subnets and instances from the EC2 backend.
type EC2Lookup interface {
	SubnetAvailabilityZone(ctx context.Context, subnetID string) (string, bool)
	InstanceAttributes(ctx context.Context, instanceID string) (InstanceAttributes, bool)
}

// instanceAttributes resolves an EC2 instance's launch attributes.
func (b *InMemoryBackend) instanceAttributes(instanceID string) (InstanceAttributes, bool) {
	if b.ec2Lookup == nil {
		return InstanceAttributes{}, false
	}

	return b.ec2Lookup.InstanceAttributes(b.crossServiceContext(), instanceID)
}

// SetEC2Lookup wires the EC2 reader; nil leaves subnets and instances unresolved.
func (b *InMemoryBackend) SetEC2Lookup(l EC2Lookup) {
	b.mu.Lock("SetEC2Lookup")
	defer b.mu.Unlock()
	b.ec2Lookup = l
}

type ec2Siblings interface {
	GetEC2Handler() service.Registerable
}

// ec2LookupAdapter resolves subnets through the sibling EC2 backend in the caller's region.
type ec2LookupAdapter struct {
	cfg any
}

func (l ec2LookupAdapter) backend(ctx context.Context) (ec2backend.Backend, bool) {
	s, ok := l.cfg.(ec2Siblings)
	if !ok {
		return nil, false
	}

	h, ok := s.GetEC2Handler().(*ec2backend.Handler)
	if !ok || h == nil || h.Backend == nil {
		return nil, false
	}

	return h.BackendFor(awsmeta.Region(ctx)), true
}

func (l ec2LookupAdapter) InstanceAttributes(ctx context.Context, instanceID string) (InstanceAttributes, bool) {
	bk, ok := l.backend(ctx)
	if !ok {
		return InstanceAttributes{}, false
	}

	for _, inst := range bk.DescribeInstances([]string{instanceID}, "") {
		if inst.State.Name == "terminated" {
			continue
		}

		return InstanceAttributes{
			ImageID:        inst.ImageID,
			InstanceType:   inst.InstanceType,
			KeyName:        inst.KeyName,
			UserData:       inst.UserData,
			SecurityGroups: slices.Clone(inst.SecurityGroups),
		}, true
	}

	return InstanceAttributes{}, false
}

func (l ec2LookupAdapter) SubnetAvailabilityZone(ctx context.Context, subnetID string) (string, bool) {
	bk, ok := l.backend(ctx)
	if !ok {
		return "", false
	}

	subnets := bk.DescribeSubnets([]string{subnetID})
	if len(subnets) == 0 {
		return "", false
	}

	return subnets[0].AvailabilityZone, subnets[0].AvailabilityZone != ""
}

// subnetZone resolves subnetID's zone via the wired lookup.
func (b *InMemoryBackend) subnetZone(subnetID string) (string, bool) {
	if b.ec2Lookup == nil {
		return "", false
	}

	return b.ec2Lookup.SubnetAvailabilityZone(b.crossServiceContext(), subnetID)
}
