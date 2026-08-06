package resiliencehub

import (
	"context"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"

	cloudformationbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
	resourcegroupsbackend "github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

// siblingServices is the subset of *CLI's method set this backend needs to
// reach the CloudFormation/Resource Groups/EKS backends, so
// ResolveAppVersionResources can materialize CfnStack/ResourceGroup/EKS
// resource mappings against real cross-service state instead of leaving them
// unresolved. Matched structurally against *CLI (no import of the top-level
// package, which would cycle) -- same pattern as
// services/grafana/cross_service.go and services/mgn/cross_service.go.
type siblingServices interface {
	GetCloudFormationHandler() service.Registerable
	GetResourceGroupsHandler() service.Registerable
	GetEKSHandler() service.Registerable
}

// SetAppConfig records the service.AppContext.Config value Provider.Init
// received, so this backend can resolve sibling service handlers on demand.
//
// It cannot resolve them at Init time: gopherstack's startup sequence
// constructs every service provider independently in one pass (see cli.go's
// initIndependentServices) and only wires each provider's own CLI-struct
// field afterward, once every provider has returned. Capturing the *CLI
// pointer now and calling its Get*Handler methods lazily -- on the first
// real resolution, well after startup has finished -- gets around that
// ordering without needing a second, cross-service-aware init phase.
func (b *InMemoryBackend) SetAppConfig(cfg any) {
	b.appConfig = cfg
}

func (b *InMemoryBackend) siblings() (siblingServices, bool) {
	s, ok := b.appConfig.(siblingServices)

	return s, ok
}

// cloudformationBackend returns the emulator's CloudFormation backend, if wired.
func (b *InMemoryBackend) cloudformationBackend() (cloudformationbackend.StorageBackend, bool) {
	s, ok := b.siblings()
	if !ok {
		return nil, false
	}

	h, ok := s.GetCloudFormationHandler().(*cloudformationbackend.Handler)
	if !ok || h == nil {
		return nil, false
	}

	return h.Backend, true
}

// resourcegroupsBackend returns the emulator's Resource Groups backend, if wired.
func (b *InMemoryBackend) resourcegroupsBackend() (resourcegroupsbackend.StorageBackend, bool) {
	s, ok := b.siblings()
	if !ok {
		return nil, false
	}

	h, ok := s.GetResourceGroupsHandler().(*resourcegroupsbackend.Handler)
	if !ok || h == nil {
		return nil, false
	}

	return h.Backend, true
}

// eksBackend returns the emulator's EKS backend, if wired.
func (b *InMemoryBackend) eksBackend() (*eksbackend.InMemoryBackend, bool) {
	s, ok := b.siblings()
	if !ok {
		return nil, false
	}

	h, ok := s.GetEKSHandler().(*eksbackend.Handler)
	if !ok || h == nil {
		return nil, false
	}

	return h.Backend, true
}

// resolveCfnStackMappingLocked materializes every resource CloudFormation
// reports for m.LogicalStackName into a real, discovered PhysicalResource --
// a genuine cross-service lookup (services/cloudformation.DescribeStackResources),
// not a fabricated resource list. A no-op when CloudFormation isn't wired or
// the named stack doesn't exist. Callers must hold b.mu.
func (b *InMemoryBackend) resolveCfnStackMappingLocked(v *AppVersion, m ResourceMapping) {
	cfnBk, ok := b.cloudformationBackend()
	if !ok {
		return
	}

	stackResources, err := cfnBk.DescribeStackResources(m.LogicalStackName)
	if err != nil {
		return
	}

	for _, sr := range stackResources {
		loc := findResourceLocator{physicalID: sr.PhysicalID}
		if existing, _ := findResource(v, loc); existing != nil {
			continue
		}

		v.Resources = append(v.Resources, &PhysicalResource{
			LogicalResourceID:  &LogicalResourceID{LogicalStackName: m.LogicalStackName, Identifier: sr.LogicalID},
			PhysicalResourceID: &PhysicalResourceID{Identifier: sr.PhysicalID, Type: physicalIDTypeFor(sr.Type)},
			ResourceType:       sr.Type,
			ResourceName:       sr.LogicalID,
			SourceType:         ResourceSourceDiscovered,
		})
	}
}

// resolveResourceGroupMappingLocked materializes every resource Resource
// Groups reports as a member of m.ResourceGroupName into a real, discovered
// PhysicalResource -- a genuine cross-service lookup
// (services/resourcegroups.ListGroupResources), not a fabricated resource
// list. A no-op when Resource Groups isn't wired or the named group doesn't
// exist. Callers must hold b.mu.
func (b *InMemoryBackend) resolveResourceGroupMappingLocked(v *AppVersion, m ResourceMapping) {
	rgBk, ok := b.resourcegroupsBackend()
	if !ok {
		return
	}

	members, _, err := rgBk.ListGroupResources(context.Background(), m.ResourceGroupName, nil, "", 0)
	if err != nil {
		return
	}

	for _, member := range members {
		loc := findResourceLocator{physicalID: member.ResourceArn}
		if existing, _ := findResource(v, loc); existing != nil {
			continue
		}

		v.Resources = append(v.Resources, &PhysicalResource{
			LogicalResourceID: &LogicalResourceID{
				ResourceGroupName: m.ResourceGroupName,
				Identifier:        member.ResourceArn,
			},
			PhysicalResourceID: &PhysicalResourceID{Identifier: member.ResourceArn, Type: PhysicalIDTypeArn},
			ResourceType:       member.ResourceType,
			SourceType:         ResourceSourceDiscovered,
		})
	}
}

// resolveEKSMappingLocked materializes the EKS cluster named by
// m.EksSourceName (format "cluster-name/namespace", per PARITY.md) into a
// real, discovered PhysicalResource -- a genuine cross-service lookup
// (services/eks.DescribeCluster), not a fabricated resource. The namespace
// segment has no gopherstack-local semantics (no Kubernetes workload
// emulation exists in this tree), so only the cluster itself is resolved. A
// no-op when EKS isn't wired or the named cluster doesn't exist. Callers
// must hold b.mu.
func (b *InMemoryBackend) resolveEKSMappingLocked(v *AppVersion, m ResourceMapping) {
	eksBk, ok := b.eksBackend()
	if !ok {
		return
	}

	clusterName, _, _ := strings.Cut(m.EksSourceName, "/")

	cluster, err := eksBk.DescribeCluster(clusterName)
	if err != nil {
		return
	}

	loc := findResourceLocator{physicalID: cluster.ARN}
	if existing, _ := findResource(v, loc); existing != nil {
		return
	}

	v.Resources = append(v.Resources, &PhysicalResource{
		LogicalResourceID:  &LogicalResourceID{EksSourceName: m.EksSourceName, Identifier: cluster.Name},
		PhysicalResourceID: &PhysicalResourceID{Identifier: cluster.ARN, Type: PhysicalIDTypeArn},
		ResourceType:       eksClusterResourceType,
		ResourceName:       cluster.Name,
		SourceType:         ResourceSourceDiscovered,
	})
}
