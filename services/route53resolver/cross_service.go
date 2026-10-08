package route53resolver

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

type ec2HandlerProvider interface {
	GetEC2Handler() service.Registerable
}

// SetAppConfig records the AppContext.Config so the EC2 backend is resolved lazily.
func (b *InMemoryBackend) SetAppConfig(cfg any) {
	b.appConfig = cfg
}

// subnetVPC returns the VPC of the first IP address whose subnet EC2 knows, or "".
func (b *InMemoryBackend) subnetVPC(ctx context.Context, ips []IPAddress) string {
	p, ok := b.appConfig.(ec2HandlerProvider)
	if !ok {
		return ""
	}

	h, ok := p.GetEC2Handler().(*ec2backend.Handler)
	if !ok || h == nil {
		return ""
	}

	bk := h.BackendFor(getRegion(ctx, b.region))

	for _, ip := range ips {
		if ip.SubnetID == "" {
			continue
		}

		if subs := bk.DescribeSubnets([]string{ip.SubnetID}); len(subs) > 0 {
			return subs[0].VPCID
		}
	}

	return ""
}
