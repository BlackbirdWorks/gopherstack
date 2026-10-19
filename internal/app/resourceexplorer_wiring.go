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

const resourceExplorerPageSize = 100

// taggingResourceSource feeds Resource Explorer from the Resource Groups Tagging API aggregation.
// The service filters keep GetResources off its 30s unfiltered cache so new resources are searchable at once.
type taggingResourceSource struct {
	tagging rgtapibackend.StorageBackend
	filters []string
}

func (s *taggingResourceSource) Resources(ctx context.Context) []resourceexplorer2backend.SourceResource {
	pageSize := int32(resourceExplorerPageSize)
	in := &rgtapibackend.GetResourcesInput{ResourceTypeFilters: s.filters, ResourcesPerPage: &pageSize}

	var out []resourceexplorer2backend.SourceResource

	for {
		pg, err := s.tagging.GetResources(ctx, in)
		if err != nil || pg == nil {
			return out
		}

		for _, m := range pg.ResourceTagMappingList {
			tags := make(map[string]string, len(m.Tags))
			for _, t := range m.Tags {
				tags[t.Key] = t.Value
			}

			out = append(out, resourceexplorer2backend.SourceResource{ARN: m.ResourceARN, Tags: tags})
		}

		if pg.PaginationToken == nil || *pg.PaginationToken == "" {
			return out
		}

		in.PaginationToken = *pg.PaginationToken
	}
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
		sources = append(sources, &taggingResourceSource{
			tagging: rgtH.Backend, filters: resourceexplorer2backend.SupportedServices(),
		})
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
