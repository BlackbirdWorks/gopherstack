package app

import (
	"context"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	organizationsbackend "github.com/blackbirdworks/gopherstack/services/organizations"
	resourceexplorer2backend "github.com/blackbirdworks/gopherstack/services/resourceexplorer2"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

// allResourceLister is the uncached enumeration of every tagging provider.
type allResourceLister interface {
	AllResources(ctx context.Context) []rgtapibackend.TaggedResource
}

// taggingResourceSource feeds Resource Explorer from every resource provider of the tagging aggregation,
// tagged or not, so any service wired there is searchable and new resources appear at once.
type taggingResourceSource struct{ tagging allResourceLister }

func (s *taggingResourceSource) Resources(ctx context.Context) []resourceexplorer2backend.SourceResource {
	all := s.tagging.AllResources(ctx)
	out := make([]resourceexplorer2backend.SourceResource, 0, len(all))

	for _, r := range all {
		out = append(out, resourceexplorer2backend.SourceResource{ARN: r.ResourceARN, Tags: r.Tags})
	}

	return out
}

// ec2ResourceSource enumerates the EC2 resources the tagging aggregation does not carry.
type ec2ResourceSource struct {
	backend func(region string) *ec2backend.InMemoryBackend
}

func (s *ec2ResourceSource) Resources(ctx context.Context) []resourceexplorer2backend.SourceResource {
	bk := s.backend(awsmeta.Region(ctx))
	if bk == nil {
		return nil
	}

	out := make([]resourceexplorer2backend.SourceResource, 0)

	add := func(kind, id string) {
		out = append(out, resourceexplorer2backend.SourceResource{
			ARN: arn.Build("ec2", bk.Region, bk.AccountID, kind+"/"+id), Tags: bk.TagsForResource(id),
		})
	}

	for _, i := range bk.DescribeInstances(nil, "") {
		if i.State.Name != ec2backend.StateTerminated.Name {
			add("instance", i.ID)
		}
	}

	for _, v := range bk.DescribeVpcs(nil) {
		add("vpc", v.ID)
	}

	for _, v := range bk.DescribeSubnets(nil) {
		add("subnet", v.ID)
	}

	for _, v := range bk.DescribeSecurityGroups(nil) {
		add("security-group", v.ID)
	}

	for _, v := range bk.DescribeVolumes(nil) {
		add("volume", v.ID)
	}

	return out
}

// multiResourceSource concatenates several sources.
type multiResourceSource []resourceexplorer2backend.ResourceSource

func (m multiResourceSource) Resources(ctx context.Context) []resourceexplorer2backend.SourceResource {
	parts := make([][]resourceexplorer2backend.SourceResource, 0, len(m))
	for _, s := range m {
		parts = append(parts, s.Resources(ctx))
	}

	return slices.Concat(parts...)
}

type serviceAccessLister interface {
	ListAWSServiceAccessForOrganization() ([]organizationsbackend.EnabledServicePrincipal, error)
}

// organizationsAccess answers Resource Explorer's org-access question from the Organizations backend.
type organizationsAccess struct{ lister serviceAccessLister }

func (o *organizationsAccess) ServiceAccessEnabled(principal string) bool {
	enabled, err := o.lister.ListAWSServiceAccessForOrganization()
	if err != nil {
		return false
	}

	for _, e := range enabled {
		if e.ServicePrincipal == principal {
			return true
		}
	}

	return false
}

// wireResourceExplorer connects Resource Explorer to the tagging aggregation and Organizations.
func wireResourceExplorer(byName map[string]service.Registerable) {
	reH, ok := byName["ResourceExplorer2"].(*resourceexplorer2backend.Handler)
	if !ok {
		return
	}

	var sources multiResourceSource

	if rgtH, isRGT := byName["ResourceGroupsTaggingAPI"].(*rgtapibackend.Handler); isRGT {
		if lister, canList := rgtH.Backend.(allResourceLister); canList {
			sources = append(sources, &taggingResourceSource{tagging: lister})
		}
	}

	if ec2H, isEC2 := byName["EC2"].(*ec2backend.Handler); isEC2 {
		if home, homeOK := ec2H.Backend.(*ec2backend.InMemoryBackend); homeOK {
			sources = append(sources, &ec2ResourceSource{backend: regionalEC2Backend(ec2H, home)})
		}
	}

	reH.Backend.SetResourceSource(sources)

	if orgH, isOrg := byName["Organizations"].(*organizationsbackend.Handler); isOrg {
		if l, canList := orgH.Backend.(serviceAccessLister); canList {
			reH.Backend.SetOrgAccess(&organizationsAccess{lister: l})
		}
	}
}
