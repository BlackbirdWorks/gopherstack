package app

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cfnbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
	globalacceleratorbackend "github.com/blackbirdworks/gopherstack/services/globalaccelerator"
	organizationsbackend "github.com/blackbirdworks/gopherstack/services/organizations"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

// globalAcceleratorEndpoints verifies Global Accelerator endpoints against ELBv2 and EC2.
type globalAcceleratorEndpoints struct {
	elb *elbv2backend.Handler
	ec2 ec2Regions
}

// EndpointExists implements globalaccelerator.EndpointResolver.
func (r globalAcceleratorEndpoints) EndpointExists(_, endpointID string) bool {
	switch {
	case strings.HasPrefix(endpointID, "arn:"):
		return r.loadBalancerExists(endpointID)
	case strings.HasPrefix(endpointID, "i-"):
		return r.ec2.any(func(b ec2backend.Backend) bool {
			return len(b.DescribeInstances([]string{endpointID}, "")) > 0
		})
	case strings.HasPrefix(endpointID, "eipalloc-"):
		return r.ec2.any(func(b ec2backend.Backend) bool {
			return len(b.DescribeAddresses([]string{endpointID})) > 0
		})
	}

	return true
}

func (r globalAcceleratorEndpoints) loadBalancerExists(lbARN string) bool {
	if r.elb == nil {
		return true
	}

	lbs, err := r.elb.BackendFor(arnRegion(lbARN)).DescribeLoadBalancers([]string{lbARN}, nil)

	return err == nil && len(lbs) > 0
}

// globalAcceleratorOrgBackend is the slice of the Organizations backend the directory needs.
type globalAcceleratorOrgBackend interface {
	DescribeOrganization() (*organizationsbackend.Organization, error)
	ListAccounts() ([]*organizationsbackend.Account, error)
	ResolveAccountIDsUnderParent(parentID string) ([]string, error)
}

// globalAcceleratorOrgDirectory implements globalaccelerator.OrgDirectory over the Organizations backend.
type globalAcceleratorOrgDirectory struct{ org globalAcceleratorOrgBackend }

func (d globalAcceleratorOrgDirectory) OrganizationAccountIDs(orgID string) []string {
	o, err := d.org.DescribeOrganization()
	if err != nil || o.ID != orgID {
		return nil
	}

	accts, err := d.org.ListAccounts()
	if err != nil {
		return nil
	}

	ids := make([]string, 0, len(accts))
	for _, a := range accts {
		ids = append(ids, a.ID)
	}

	return ids
}

func (d globalAcceleratorOrgDirectory) OUAccountIDs(ouID string) []string {
	ids, err := d.org.ResolveAccountIDsUnderParent(ouID)
	if err != nil {
		return nil
	}

	return ids
}

// wireGlobalAcceleratorIntegrations gives Global Accelerator its ELBv2/EC2 endpoint lookups.
func wireGlobalAcceleratorIntegrations(byName map[string]service.Registerable) {
	gaH, ok := byName["GlobalAccelerator"].(*globalacceleratorbackend.Handler)
	if !ok {
		return
	}

	if orgH, ook := byName["Organizations"].(*organizationsbackend.Handler); ook {
		if orgs, bok := orgH.Backend.(globalAcceleratorOrgBackend); bok {
			gaH.Backend.SetOrgDirectory(globalAcceleratorOrgDirectory{org: orgs})
		}
	}

	if ec2H, eok := byName["EC2"].(*ec2backend.Handler); eok {
		elbH, _ := byName["ELBv2"].(*elbv2backend.Handler)
		gaH.Backend.SetEndpointResolver(globalAcceleratorEndpoints{elb: elbH, ec2: ec2Regions{handler: ec2H}})
	}
}

// wireGlobalAcceleratorCloudFormation lets stacks provision AWS::GlobalAccelerator::* resources.
func wireGlobalAcceleratorCloudFormation(gaReg, cfnReg service.Registerable) {
	gaH, gok := gaReg.(*globalacceleratorbackend.Handler)
	cfnH, cok := cfnReg.(*cfnbackend.Handler)

	if !gok || !cok {
		return
	}

	if cfnBk, ok := cfnH.Backend.(*cfnbackend.InMemoryBackend); ok {
		cfnBk.SetGlobalAccelerator(gaH)
	}
}

func wireTaggingGlobalAccelerator(bk rgtapibackend.StorageBackend, reg service.Registerable) {
	h, ok := reg.(*globalacceleratorbackend.Handler)
	if !ok {
		return
	}

	wireTaggingARNResources(
		bk, "globalaccelerator",
		func(arn string) string { return resourceTypeFromARN(arn, "globalaccelerator") },
		func() []taggedARNEntry {
			items := h.Backend.TaggedResources()
			out := make([]taggedARNEntry, 0, len(items))

			for _, it := range items {
				out = append(out, taggedARNEntry{ARN: it.ARN, Tags: it.Tags})
			}

			return out
		},
		h.Backend.TagResource,
		h.Backend.UntagResource,
	)
}
