package main

import (
	"context"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	fisbackend "github.com/blackbirdworks/gopherstack/services/fis"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

const fisEC2InstanceType = "aws:ec2:instance"

// fisTargetResolver resolves FIS resourceTags and filters: EC2 instances from the EC2 backend,
// every other type through the Resource Groups Tagging API (tags only, no filter paths).
type fisTargetResolver struct {
	ec2     func(region string) *ec2backend.InMemoryBackend
	tagging rgtapibackend.StorageBackend
}

func (r *fisTargetResolver) ResolveTargets(
	ctx context.Context,
	resourceType string,
	tags map[string]string,
	filters []fisbackend.ExperimentTemplateTargetFilter,
) []string {
	if resourceType == fisEC2InstanceType {
		return r.ec2Instances(awsmeta.Region(ctx), tags, filters)
	}

	if len(filters) > 0 || r.tagging == nil {
		return nil
	}

	return r.tagged(ctx, strings.TrimPrefix(resourceType, "aws:"), tags)
}

func (r *fisTargetResolver) ec2Instances(
	region string, tags map[string]string, filters []fisbackend.ExperimentTemplateTargetFilter,
) []string {
	bk := r.ec2(region)
	if bk == nil {
		return nil
	}

	var out []string

	for _, inst := range bk.DescribeInstances(nil, "") {
		if inst.State.Name == ec2backend.StateTerminated.Name {
			continue
		}

		if !tagsMatch(bk.TagsForResource(inst.ID), tags) || !ec2FiltersMatch(inst, filters) {
			continue
		}

		out = append(out, arn.Build("ec2", bk.Region, bk.AccountID, "instance/"+inst.ID))
	}

	return out
}

func (r *fisTargetResolver) tagged(ctx context.Context, taggingType string, tags map[string]string) []string {
	in := &rgtapibackend.GetResourcesInput{ResourceTypeFilters: []string{taggingType}}
	for k, v := range tags {
		in.TagFilters = append(in.TagFilters, rgtapibackend.TagFilter{Key: k, Values: []string{v}})
	}

	var out []string

	for {
		page, err := r.tagging.GetResources(ctx, in)
		if err != nil || page == nil {
			return out
		}

		for _, m := range page.ResourceTagMappingList {
			out = append(out, m.ResourceARN)
		}

		if page.PaginationToken == nil || *page.PaginationToken == "" {
			return out
		}

		in.PaginationToken = *page.PaginationToken
	}
}

func tagsMatch(have, want map[string]string) bool {
	for k, v := range want {
		if got, ok := have[k]; !ok || got != v {
			return false
		}
	}

	return true
}

// ec2FiltersMatch evaluates FIS target filter paths against an instance; an unknown path never matches.
func ec2FiltersMatch(inst *ec2backend.Instance, filters []fisbackend.ExperimentTemplateTargetFilter) bool {
	fields := map[string]string{
		"State.Name":                 inst.State.Name,
		"Placement.AvailabilityZone": inst.Placement.AvailabilityZone,
		"Placement.Tenancy":          inst.Placement.Tenancy,
		"VpcId":                      inst.VPCID,
		"SubnetId":                   inst.SubnetID,
		"InstanceType":               inst.InstanceType,
		"ImageId":                    inst.ImageID,
		"KeyName":                    inst.KeyName,
		"PrivateIpAddress":           inst.PrivateIP,
		"PublicIpAddress":            inst.PublicIPAddress,
	}

	for _, f := range filters {
		got, known := fields[f.Path]
		if !known || got == "" {
			return false
		}

		if !slices.Contains(f.Values, got) {
			return false
		}
	}

	return true
}

// wireFISTargetResolver lets FIS resolve resourceTags and filters to the ARNs they select.
func wireFISTargetResolver(byName map[string]service.Registerable) {
	fisH, ok := byName["FIS"].(*fisbackend.Handler)
	if !ok {
		return
	}

	r := &fisTargetResolver{}

	if ec2H, isEC2 := byName["EC2"].(*ec2backend.Handler); isEC2 {
		if home, homeOK := ec2H.Backend.(*ec2backend.InMemoryBackend); homeOK {
			r.ec2 = regionalEC2Backend(ec2H, home)
		}
	}

	if rgtH, isRGT := byName["ResourceGroupsTaggingAPI"].(*rgtapibackend.Handler); isRGT {
		r.tagging = rgtH.Backend
	}

	if r.ec2 == nil {
		r.ec2 = func(string) *ec2backend.InMemoryBackend { return nil }
	}

	fisH.SetTargetResolver(r)
}
