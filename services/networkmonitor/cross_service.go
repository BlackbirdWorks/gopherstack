package networkmonitor

import (
	"strings"

	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

// siblingServices is matched structurally against *CLI, which cannot be
// imported here without a cycle.
type siblingServices interface {
	GetEC2Handler() service.Registerable
}

// SetAppConfig records service.AppContext.Config so sibling backends can be
// resolved lazily, after every provider has been constructed.
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

// subnetVpcID returns the VPC that owns the subnet named by sourceArn, or ""
// when EC2 is not wired or the subnet is unknown.
func (b *InMemoryBackend) subnetVpcID(sourceArn string) string {
	parsed, err := awsarn.Parse(sourceArn)
	if err != nil || parsed.Service != "ec2" {
		return ""
	}

	subnetID, ok := strings.CutPrefix(parsed.Resource, "subnet/")
	if !ok {
		return ""
	}

	ec2Bk, ok := b.ec2Backend(parsed.Region)
	if !ok {
		return ""
	}

	for _, s := range ec2Bk.DescribeSubnets([]string{subnetID}) {
		if s.ID == subnetID {
			return s.VPCID
		}
	}

	return ""
}
