package app

import (
	"context"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	networkfirewallbackend "github.com/blackbirdworks/gopherstack/services/networkfirewall"
	resourcegroupsbackend "github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

const maxGroupMemberPages = 1000

// firewallReferenceResolver resolves resource group and ECS cluster IP set references for Network Firewall.
type firewallReferenceResolver struct {
	ec2SubnetAdapter
	groups resourcegroupsbackend.StorageBackend
	ecs    *ecsbackend.Handler
}

var _ networkfirewallbackend.ReferenceResolver = firewallReferenceResolver{}

type taskIPLister interface {
	RunningTaskIPv4s(cluster string, attrFilters map[string]string) ([]string, error)
}

// ResourceGroupCIDRCount counts the distinct private and public addresses of the EC2 instances and
// network interfaces in a resource group (network-firewall rule-groups-ip-set-references).
func (r firewallReferenceResolver) ResourceGroupCIDRCount(groupARN string) (int, bool) {
	if r.groups == nil {
		return 0, false
	}

	region := arnRegion(groupARN)
	ctx := resourcegroupsbackend.ContextWithRegion(context.Background(), region)
	addrs := map[string]struct{}{}
	token := ""

	for range maxGroupMemberPages {
		members, next, err := r.groups.ListGroupResources(ctx, groupARN, nil, token, 0)
		if err != nil {
			return 0, false
		}

		for _, m := range members {
			r.collectMemberIPs(region, m.ResourceArn, addrs)
		}

		if next == "" {
			break
		}

		token = next
	}

	return len(addrs), true
}

func (r firewallReferenceResolver) collectMemberIPs(region, resourceARN string, addrs map[string]struct{}) {
	ec2b := r.regions.handler.BackendFor(region)

	if _, id, ok := strings.Cut(resourceARN, ":instance/"); ok {
		for _, in := range ec2b.DescribeInstances([]string{id}, "") {
			addIPs(addrs, in.PrivateIP, in.PublicIPAddress)
		}

		return
	}

	if _, id, ok := strings.Cut(resourceARN, ":network-interface/"); ok {
		for _, ni := range ec2b.DescribeNetworkInterfaces([]string{id}) {
			addIPs(addrs, append([]string{ni.PrivateIP}, ni.SecondaryPrivateIPs...)...)
		}
	}
}

// ECSClusterCIDRCount counts the awsvpc task addresses of an ECS cluster.
func (r firewallReferenceResolver) ECSClusterCIDRCount(clusterARN string, filters map[string]string) (int, bool) {
	if r.ecs == nil {
		return 0, false
	}

	lister, ok := r.ecs.BackendFor(arnRegion(clusterARN)).(taskIPLister)
	if !ok {
		return 0, false
	}

	ips, err := lister.RunningTaskIPv4s(clusterARN, filters)
	if err != nil {
		return 0, false
	}

	return len(ips), true
}

func addIPs(set map[string]struct{}, ips ...string) {
	for _, ip := range ips {
		if ip != "" {
			set[ip] = struct{}{}
		}
	}
}

func firewallReferences(adapter ec2SubnetAdapter, byName map[string]service.Registerable) firewallReferenceResolver {
	r := firewallReferenceResolver{ec2SubnetAdapter: adapter}

	if h, ok := byName["ResourceGroups"].(*resourcegroupsbackend.Handler); ok {
		r.groups = h.Backend
	}

	if h, ok := byName["ECS"].(*ecsbackend.Handler); ok {
		r.ecs = h
	}

	return r
}
