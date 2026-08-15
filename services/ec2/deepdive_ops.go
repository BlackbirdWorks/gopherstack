package ec2

import (
	"fmt"
	"slices"
	"time"
)

// CreateImage creates an AMI from an instance.
func (b *InMemoryBackend) CreateImage(instanceID, name, description string) (*AMIStub, error) {
	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateImage")
	defer b.mu.Unlock()

	if _, ok := b.instances.Get(instanceID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	if name == "" {
		name = "gopherstack-image"
	}

	imageID := newAMIID()
	image := &AMIStub{
		ImageID:        imageID,
		Name:           name,
		Description:    description,
		Architecture:   archX8664,
		RootDeviceName: "/dev/xvda",
		State:          stateAvailable,
	}
	b.images.Put(image)
	b.imageUsageReports.Put(&ImageUsageReport{
		ImageID:        imageID,
		State:          stateAvailable,
		GenerationDate: time.Now().UTC().Format(time.RFC3339),
	})

	cp := *image

	return &cp, nil
}

// DescribeImageUsageReports returns synthetic image usage reports.
func (b *InMemoryBackend) DescribeImageUsageReports() []*ImageUsageReport {
	b.mu.RLock("DescribeImageUsageReports")
	defer b.mu.RUnlock()

	reports := make([]*ImageUsageReport, 0, b.imageUsageReports.Len())
	for _, report := range b.imageUsageReports.All() {
		cp := *report
		reports = append(reports, &cp)
	}

	return reports
}

// CreateLaunchTemplate creates a launch template.
func (b *InMemoryBackend) CreateLaunchTemplate(
	name, imageID, instanceType string,
	tags map[string]string,
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

	for _, lt := range b.launchTemplates.All() {
		if lt.Name == name {
			return nil, fmt.Errorf(
				"%w: duplicate launch template name %s",
				ErrInvalidParameter,
				name,
			)
		}
	}

	template := &LaunchTemplate{
		ID:                   newLaunchTemplateID(),
		Name:                 name,
		ImageID:              imageID,
		InstanceType:         instanceType,
		CreatedBy:            b.AccountID,
		CreateTime:           time.Now().UTC(),
		DefaultVersionNumber: 1,
		LatestVersionNumber:  1,
	}
	b.launchTemplates.Put(template)
	b.setTagsLocked(template.ID, tags)
	cp := *template

	return &cp, nil
}

// DescribeLaunchTemplates returns launch templates, optionally filtered by names.
func (b *InMemoryBackend) DescribeLaunchTemplates(names []string) []*LaunchTemplate {
	b.mu.RLock("DescribeLaunchTemplates")
	defer b.mu.RUnlock()

	templates := make([]*LaunchTemplate, 0, b.launchTemplates.Len())
	for _, lt := range b.launchTemplates.All() {
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

	networkACLs := make([]*NetworkACL, 0, b.vpcs.Len())
	for _, vpc := range b.vpcs.All() {
		if len(allowed) > 0 && !allowed[vpc.ID] {
			continue
		}

		assocIDs := make([]string, 0, b.subnets.Len())
		for _, subnet := range b.subnets.All() {
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
	return b.CreateVpcEndpointWithRouteTableIDs(vpcID, serviceName, endpointType, subnetIDs, nil)
}

// CreateVpcEndpointWithRouteTableIDs creates a VPC endpoint with optional route table associations.
func (b *InMemoryBackend) CreateVpcEndpointWithRouteTableIDs(
	vpcID, serviceName, endpointType string,
	subnetIDs, routeTableIDs []string,
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

	if _, ok := b.vpcs.Get(vpcID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrVPCNotFound, vpcID)
	}

	for _, subnetID := range subnetIDs {
		subnet, ok := b.subnets.Get(subnetID)
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

	for _, rtID := range routeTableIDs {
		if _, ok := b.routeTables.Get(rtID); !ok {
			return nil, fmt.Errorf("%w: route table %s not found", ErrRouteTableNotFound, rtID)
		}
	}

	endpoint := &VpcEndpoint{
		ID:              newVPCEndpointID(),
		VPCID:           vpcID,
		ServiceName:     serviceName,
		State:           stateAvailable,
		VpcEndpointType: endpointType,
		OwnerID:         b.AccountID,
		SubnetIDs:       append([]string(nil), subnetIDs...),
		RouteTableIDs:   append([]string(nil), routeTableIDs...),
		CreateTime:      time.Now().UTC(),
	}
	b.vpcEndpoints.Put(endpoint)
	cp := *endpoint
	cp.SubnetIDs = append([]string(nil), endpoint.SubnetIDs...)
	cp.RouteTableIDs = append([]string(nil), endpoint.RouteTableIDs...)

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

	endpoints := make([]*VpcEndpoint, 0, b.vpcEndpoints.Len())
	for _, ep := range b.vpcEndpoints.All() {
		if len(allowed) > 0 && !allowed[ep.ID] {
			continue
		}

		cp := *ep
		cp.SubnetIDs = append([]string(nil), ep.SubnetIDs...)
		cp.RouteTableIDs = append([]string(nil), ep.RouteTableIDs...)
		endpoints = append(endpoints, &cp)
	}

	return endpoints
}
