package cloudformation

import (
	"errors"
	"fmt"
	"strings"
)

var errNoEC2Instances = errors.New("create EC2 instance: no instances returned")

// ---- EC2 Instance ----

func (rc *ResourceCreator) createEC2Instance(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	imageID := strProp(props, "ImageId", params, physicalIDs)
	if imageID == "" {
		imageID = "ami-00000000"
	}

	instanceType := strProp(props, "InstanceType", params, physicalIDs)
	if instanceType == "" {
		instanceType = "t3.micro"
	}

	subnetID := strProp(props, "SubnetId", params, physicalIDs)

	instances, err := rc.backends.EC2.Backend.RunInstances(imageID, instanceType, subnetID, 1)
	if err != nil {
		return "", fmt.Errorf("create EC2 instance: %w", err)
	}

	if len(instances) == 0 {
		return "", errNoEC2Instances
	}

	return instances[0].ID, nil
}

func (rc *ResourceCreator) deleteEC2Instance(id string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	_, err := rc.backends.EC2.Backend.TerminateInstances([]string{id})

	return err
}

// ---- EC2 VPCGatewayAttachment ----

func (rc *ResourceCreator) createEC2VPCGatewayAttachment(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	vpcID := strProp(props, "VpcId", params, physicalIDs)
	igwID := strProp(props, "InternetGatewayId", params, physicalIDs)

	if igwID != "" {
		if err := rc.backends.EC2.Backend.AttachInternetGateway(igwID, vpcID); err != nil {
			return "", fmt.Errorf("attach internet gateway %s to VPC %s: %w", igwID, vpcID, err)
		}
	}

	return igwID + ":" + vpcID, nil
}

func (rc *ResourceCreator) deleteEC2VPCGatewayAttachment(physicalID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	const splitParts = 2
	parts := strings.SplitN(physicalID, ":", splitParts)
	if len(parts) < splitParts {
		return nil
	}

	igwID := parts[0]
	vpcID := parts[1]

	if igwID == "" || vpcID == "" {
		return nil
	}

	return rc.backends.EC2.Backend.DetachInternetGateway(igwID, vpcID)
}

// ---- EC2 SubnetRouteTableAssociation ----

func (rc *ResourceCreator) createEC2SubnetRouteTableAssociation(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	rtID := strProp(props, "RouteTableId", params, physicalIDs)
	subnetID := strProp(props, "SubnetId", params, physicalIDs)

	assocID, err := rc.backends.EC2.Backend.AssociateRouteTable(rtID, subnetID)
	if err != nil {
		return "", fmt.Errorf("associate route table %s with subnet %s: %w", rtID, subnetID, err)
	}

	return assocID, nil
}

func (rc *ResourceCreator) deleteEC2SubnetRouteTableAssociation(assocID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DisassociateRouteTable(assocID)
}
