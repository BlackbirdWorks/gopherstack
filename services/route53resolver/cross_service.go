package route53resolver

import (
	"context"
	"net"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
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

type route53HandlerProvider interface {
	GetRoute53Handler() service.Registerable
}

type logsHandlerProvider interface {
	GetCloudWatchLogsHandler() service.Registerable
}

// VPCLocator maps a query's source address to the VPC it came from.
type VPCLocator func(src net.IP) (region, vpcID string, ok bool)

// SetVPCLocator overrides the EC2-backed source-address lookup.
func (b *InMemoryBackend) SetVPCLocator(l VPCLocator) {
	b.mu.Lock("SetVPCLocator")
	b.locator = l
	b.mu.Unlock()
}

func (b *InMemoryBackend) ec2Handler() *ec2backend.Handler {
	p, ok := b.appConfig.(ec2HandlerProvider)
	if !ok {
		return nil
	}

	h, _ := p.GetEC2Handler().(*ec2backend.Handler)

	return h
}

func (b *InMemoryBackend) route53Backend() route53backend.StorageBackend {
	p, ok := b.appConfig.(route53HandlerProvider)
	if !ok {
		return nil
	}

	if h, hok := p.GetRoute53Handler().(*route53backend.Handler); hok && h != nil {
		return h.Backend
	}

	return nil
}

func (b *InMemoryBackend) logsBackend() cwlogsbackend.StorageBackend {
	p, ok := b.appConfig.(logsHandlerProvider)
	if !ok {
		return nil
	}

	if h, hok := p.GetCloudWatchLogsHandler().(*cwlogsbackend.Handler); hok && h != nil {
		return h.Backend
	}

	return nil
}

// locateVPC resolves src to a VPC by matching ENI and instance private
// addresses, then subnet CIDRs, across every EC2 region backend.
func (b *InMemoryBackend) locateVPC(src net.IP) (string, string, bool) {
	if src == nil {
		return "", "", false
	}

	b.mu.RLock("locateVPC")
	loc := b.locator
	b.mu.RUnlock()

	if loc != nil {
		return loc(src)
	}

	h := b.ec2Handler()
	if h == nil {
		return "", "", false
	}

	for _, bk := range h.RegionBackends() {
		mem, ok := bk.(*ec2backend.InMemoryBackend)
		if !ok {
			continue
		}

		if vpc := vpcOfAddress(mem, src); vpc != "" {
			return mem.Region, vpc, true
		}
	}

	return "", "", false
}

func vpcOfAddress(bk ec2backend.Backend, src net.IP) string {
	for _, eni := range bk.DescribeNetworkInterfaces(nil) {
		if sameIP(eni.PrivateIP, src) || anyIP(eni.SecondaryPrivateIPs, src) {
			return eni.VPCID
		}
	}

	for _, inst := range bk.DescribeInstances(nil, "") {
		if sameIP(inst.PrivateIP, src) {
			return inst.VPCID
		}
	}

	for _, sn := range bk.DescribeSubnets(nil) {
		if _, cidr, err := net.ParseCIDR(sn.CIDRBlock); err == nil && cidr.Contains(src) {
			return sn.VPCID
		}
	}

	return ""
}

func sameIP(s string, ip net.IP) bool {
	p := net.ParseIP(s)

	return p != nil && p.Equal(ip)
}

func anyIP(list []string, ip net.IP) bool {
	for _, s := range list {
		if sameIP(s, ip) {
			return true
		}
	}

	return false
}
