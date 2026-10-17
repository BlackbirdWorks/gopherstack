package arczonalshift

import (
	"encoding/json"
	"slices"
	"strings"

	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	asgbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
)

type (
	asgSiblings interface{ GetAutoscalingHandler() service.Registerable }
	eksSiblings interface{ GetEKSHandler() service.Registerable }
	ec2Siblings interface{ GetEC2Handler() service.Registerable }
)

func (b *InMemoryBackend) asgHandler() (*asgbackend.Handler, bool) {
	s, ok := b.appConfig.(asgSiblings)
	if !ok {
		return nil, false
	}

	h, ok := s.GetAutoscalingHandler().(*asgbackend.Handler)

	return h, ok && h != nil
}

func (b *InMemoryBackend) eksHandler() (*eksbackend.Handler, bool) {
	s, ok := b.appConfig.(eksSiblings)
	if !ok {
		return nil, false
	}

	h, ok := s.GetEKSHandler().(*eksbackend.Handler)

	return h, ok && h != nil
}

func (b *InMemoryBackend) ec2Backend(region string) (ec2backend.Backend, bool) {
	s, ok := b.appConfig.(ec2Siblings)
	if !ok {
		return nil, false
	}

	h, ok := s.GetEC2Handler().(*ec2backend.Handler)
	if !ok || h == nil {
		return nil, false
	}

	return h.BackendFor(region), true
}

func managedFromZones(arn, name string, zoneNames []string) managedResource {
	m := managedResource{ARN: arn, Name: name, ZoneNames: zoneNames}
	for _, z := range zoneNames {
		m.ZoneIDs = append(m.ZoneIDs, zoneID(z))
	}

	return m
}

func (b *InMemoryBackend) managedGroupOrCluster(arn string) (managedResource, bool, error) {
	p, ok := parseResourceARN(arn)
	if !ok {
		return managedResource{}, false, nil
	}

	switch {
	case p.Service == "autoscaling" && strings.Contains(p.Resource, "autoScalingGroupName/"):
		m, err := b.managedGroup(arn, p)

		return m, true, err
	case p.Service == "eks" && strings.HasPrefix(p.Resource, "cluster/"):
		m, err := b.managedCluster(arn, p)

		return m, true, err
	default:
		return managedResource{}, false, nil
	}
}

func parseResourceARN(arn string) (awsarn.ARN, bool) {
	p, err := awsarn.Parse(arn)

	return p, err == nil
}

func notManaged(arn string) error {
	return restroute.NotFound("Managed resource %s was not found", arn)
}

func (b *InMemoryBackend) managedGroup(arn string, p awsarn.ARN) (managedResource, error) {
	h, ok := b.asgHandler()
	if !ok {
		return managedResource{}, notManaged(arn)
	}

	_, name, _ := strings.Cut(p.Resource, "autoScalingGroupName/")

	groups, err := h.BackendFor(p.Region).DescribeAutoScalingGroups([]string{name}, nil)
	if err != nil || len(groups) == 0 || !groupZonalShiftEnabled(&groups[0]) {
		return managedResource{}, notManaged(arn)
	}

	return managedFromZones(groups[0].AutoScalingGroupARN, name, b.groupZones(p.Region, &groups[0])), nil
}

func groupZonalShiftEnabled(g *asgbackend.AutoScalingGroup) bool {
	return g.AvailabilityZoneImpairmentPolicy != nil && g.AvailabilityZoneImpairmentPolicy.ZonalShiftEnabled
}

func (b *InMemoryBackend) groupZones(region string, g *asgbackend.AutoScalingGroup) []string {
	if len(g.AvailabilityZones) > 0 {
		return append([]string{}, g.AvailabilityZones...)
	}

	return b.subnetZones(region, strings.Split(g.VPCZoneIdentifier, ","))
}

func (b *InMemoryBackend) subnetZones(region string, subnetIDs []string) []string {
	ec2b, ok := b.ec2Backend(region)
	if !ok {
		return nil
	}

	ids := make([]string, 0, len(subnetIDs))

	for _, id := range subnetIDs {
		if id = strings.TrimSpace(id); id != "" {
			ids = append(ids, id)
		}
	}

	if len(ids) == 0 {
		return nil
	}

	var zones []string

	for _, s := range ec2b.DescribeSubnets(ids) {
		if s.AvailabilityZone != "" && !slices.Contains(zones, s.AvailabilityZone) {
			zones = append(zones, s.AvailabilityZone)
		}
	}

	return zones
}

func clusterZonalShiftEnabled(c *eksbackend.Cluster) bool {
	raw := c.ConfigBlocks["zonalShiftConfig"]
	if len(raw) == 0 {
		return false
	}

	var cfg struct {
		Enabled bool `json:"enabled"`
	}

	return json.Unmarshal(raw, &cfg) == nil && cfg.Enabled
}

func (b *InMemoryBackend) managedCluster(arn string, p awsarn.ARN) (managedResource, error) {
	h, ok := b.eksHandler()
	if !ok {
		return managedResource{}, notManaged(arn)
	}

	name := strings.TrimPrefix(p.Resource, "cluster/")

	c, err := h.BackendFor(p.Region).DescribeCluster(name)
	if err != nil || c == nil || !clusterZonalShiftEnabled(c) {
		return managedResource{}, notManaged(arn)
	}

	return managedFromZones(c.ARN, c.Name, b.clusterZones(p.Region, c)), nil
}

func (b *InMemoryBackend) clusterZones(region string, c *eksbackend.Cluster) []string {
	if c.VpcConfig == nil {
		return nil
	}

	return b.subnetZones(region, c.VpcConfig.SubnetIDs)
}

func (b *InMemoryBackend) listGroupsAndClusters(region string) []managedResource {
	var out []managedResource

	if h, ok := b.asgHandler(); ok {
		groups, err := h.BackendFor(region).DescribeAutoScalingGroups(nil, nil)
		if err == nil {
			for i := range groups {
				if groupZonalShiftEnabled(&groups[i]) {
					g := &groups[i]
					out = append(
						out,
						managedFromZones(g.AutoScalingGroupARN, g.AutoScalingGroupName, b.groupZones(region, g)),
					)
				}
			}
		}
	}

	if h, ok := b.eksHandler(); ok {
		eb := h.BackendFor(region)

		for _, name := range eb.ListClusters(false) {
			if c, err := eb.DescribeCluster(name); err == nil && clusterZonalShiftEnabled(c) {
				out = append(out, managedFromZones(c.ARN, c.Name, b.clusterZones(region, c)))
			}
		}
	}

	return out
}
