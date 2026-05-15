package ec2

import (
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
)

// CreateImage creates an AMI from an instance.
func (b *InMemoryBackend) CreateImage(instanceID, name, description string) (*AMIStub, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateImage")
	defer b.mu.Unlock()

	if _, ok := b.instances[instanceID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	if name == "" {
		name = "gopherstack-image"
	}

	imageID := "ami-" + uuid.New().String()[:17]
	image := &AMIStub{
		ImageID:        imageID,
		Name:           name,
		Description:    description,
		Architecture:   archX8664,
		RootDeviceName: "/dev/xvda",
	}
	b.images[imageID] = image
	b.imageUsageReports[imageID] = &ImageUsageReport{
		ImageID:        imageID,
		State:          stateAvailable,
		GenerationDate: time.Now().UTC().Format(time.RFC3339),
	}

	cp := *image

	return &cp, nil
}

// DescribeImageUsageReports returns synthetic image usage reports.
func (b *InMemoryBackend) DescribeImageUsageReports() []*ImageUsageReport {
	b.mu.RLock("DescribeImageUsageReports")
	defer b.mu.RUnlock()

	reports := make([]*ImageUsageReport, 0, len(b.imageUsageReports))
	for _, report := range b.imageUsageReports {
		cp := *report
		reports = append(reports, &cp)
	}

	return reports
}

// CreateLaunchTemplate creates a launch template.
func (b *InMemoryBackend) CreateLaunchTemplate(
	name, imageID, instanceType string,
) (*LaunchTemplate, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: LaunchTemplateName is required", ErrInvalidParameter)
	}

	if imageID == "" {
		return nil, fmt.Errorf("%w: ImageId is required", ErrInvalidParameter)
	}

	if instanceType == "" {
		instanceType = instanceTypeT3Micro
	}

	b.mu.Lock("CreateLaunchTemplate")
	defer b.mu.Unlock()

	for _, lt := range b.launchTemplates {
		if lt.Name == name {
			return nil, fmt.Errorf(
				"%w: duplicate launch template name %s",
				ErrInvalidParameter,
				name,
			)
		}
	}

	template := &LaunchTemplate{
		ID:                   "lt-" + uuid.New().String()[:17],
		Name:                 name,
		ImageID:              imageID,
		InstanceType:         instanceType,
		CreatedBy:            b.AccountID,
		CreateTime:           time.Now().UTC(),
		DefaultVersionNumber: 1,
		LatestVersionNumber:  1,
	}
	b.launchTemplates[template.ID] = template
	cp := *template

	return &cp, nil
}

// DescribeLaunchTemplates returns launch templates, optionally filtered by names.
func (b *InMemoryBackend) DescribeLaunchTemplates(names []string) []*LaunchTemplate {
	b.mu.RLock("DescribeLaunchTemplates")
	defer b.mu.RUnlock()

	templates := make([]*LaunchTemplate, 0, len(b.launchTemplates))
	for _, lt := range b.launchTemplates {
		if len(names) > 0 && !slices.Contains(names, lt.Name) {
			continue
		}

		cp := *lt
		templates = append(templates, &cp)
	}

	return templates
}

// DescribeNetworkAcls returns one default ACL per VPC, optionally filtered by VPC IDs.
func (b *InMemoryBackend) DescribeNetworkAcls(vpcIDs []string) []*NetworkACL {
	b.mu.RLock("DescribeNetworkAcls")
	defer b.mu.RUnlock()

	allowed := make(map[string]bool, len(vpcIDs))
	for _, id := range vpcIDs {
		allowed[id] = true
	}

	networkACLs := make([]*NetworkACL, 0, len(b.vpcs))
	for _, vpc := range b.vpcs {
		if len(allowed) > 0 && !allowed[vpc.ID] {
			continue
		}

		assocIDs := make([]string, 0, len(b.subnets))
		for _, subnet := range b.subnets {
			if subnet.VPCID == vpc.ID {
				assocIDs = append(assocIDs, "aclassoc-"+subnet.ID)
			}
		}

		networkACLs = append(networkACLs, &NetworkACL{
			ID:             "acl-default-" + vpc.ID,
			VPCID:          vpc.ID,
			IsDefault:      true,
			AssociationIDs: assocIDs,
		})
	}

	return networkACLs
}

// CreateVpcEndpoint creates a VPC endpoint.
func (b *InMemoryBackend) CreateVpcEndpoint(
	vpcID, serviceName, endpointType string,
	subnetIDs []string,
) (*VpcEndpoint, error) {
	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	if serviceName == "" {
		return nil, fmt.Errorf("%w: ServiceName is required", ErrInvalidParameter)
	}

	if endpointType == "" {
		endpointType = "Interface"
	}

	b.mu.Lock("CreateVpcEndpoint")
	defer b.mu.Unlock()

	if _, ok := b.vpcs[vpcID]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	for _, subnetID := range subnetIDs {
		subnet, ok := b.subnets[subnetID]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrSubnetNotFound, subnetID)
		}

		if subnet.VPCID != vpcID {
			return nil, fmt.Errorf(
				"%w: subnet %s does not belong to VPC %s",
				ErrInvalidParameter,
				subnetID,
				vpcID,
			)
		}
	}

	endpoint := &VpcEndpoint{
		ID:              "vpce-" + uuid.New().String()[:17],
		VPCID:           vpcID,
		ServiceName:     serviceName,
		State:           stateAvailable,
		VpcEndpointType: endpointType,
		SubnetIDs:       append([]string(nil), subnetIDs...),
		CreateTime:      time.Now().UTC(),
	}
	b.vpcEndpoints[endpoint.ID] = endpoint
	cp := *endpoint
	cp.SubnetIDs = append([]string(nil), endpoint.SubnetIDs...)

	return &cp, nil
}

// DescribeVpcEndpoints returns VPC endpoints, optionally filtered by IDs.
func (b *InMemoryBackend) DescribeVpcEndpoints(ids []string) []*VpcEndpoint {
	b.mu.RLock("DescribeVpcEndpoints")
	defer b.mu.RUnlock()

	allowed := make(map[string]bool, len(ids))
	for _, id := range ids {
		allowed[id] = true
	}

	endpoints := make([]*VpcEndpoint, 0, len(b.vpcEndpoints))
	for _, ep := range b.vpcEndpoints {
		if len(allowed) > 0 && !allowed[ep.ID] {
			continue
		}

		cp := *ep
		cp.SubnetIDs = append([]string(nil), ep.SubnetIDs...)
		endpoints = append(endpoints, &cp)
	}

	return endpoints
}
