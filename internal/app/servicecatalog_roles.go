package app

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	servicecatalogbackend "github.com/blackbirdworks/gopherstack/services/servicecatalog"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

const (
	scAttrCIDR   = "CidrBlock"
	scAttrVPC    = "VpcId"
	scAttrSubnet = "SubnetId"
)

// serviceCatalogRoles checks launch roles trust servicecatalog.amazonaws.com through the STS emulator.
type serviceCatalogRoles struct{ sts *stsbackend.InMemoryBackend }

func (r serviceCatalogRoles) CheckLaunchRole(_ context.Context, roleARN string) error {
	if err := r.sts.CheckServiceRoleTrust(servicecatalogbackend.ServicePrincipal(), roleARN); err != nil {
		return fmt.Errorf("sts assume launch role: %w", err)
	}

	return nil
}

// wireServiceCatalogRoles makes Service Catalog assume launch roles under --enforce-iam.
func wireServiceCatalogRoles(cli *CLI, byName map[string]service.Registerable) {
	if cli == nil || !cli.EnforceIAM {
		return
	}

	scH, ok := byName["ServiceCatalog"].(*servicecatalogbackend.Handler)
	if !ok {
		return
	}

	if stsH, isSTS := byName["STS"].(*stsbackend.Handler); isSTS {
		if bk, isBk := stsH.Backend.(*stsbackend.InMemoryBackend); isBk {
			scH.Backend.SetRoleChecker(serviceCatalogRoles{sts: bk})
		}
	}
}

// serviceCatalogResources answers TEMPLATE rule functions from the EC2 backend.
type serviceCatalogResources struct{ ec2 ec2backend.Backend }

func (r serviceCatalogResources) ResourceIDs(_ context.Context, paramType string) ([]string, bool) {
	var ids []string

	switch paramType {
	case "AWS::EC2::VPC::Id":
		for _, v := range r.ec2.DescribeVpcs(nil) {
			ids = append(ids, v.ID)
		}
	case "AWS::EC2::Subnet::Id":
		for _, s := range r.ec2.DescribeSubnets(nil) {
			ids = append(ids, s.ID)
		}
	case "AWS::EC2::SecurityGroup::Id":
		for _, g := range r.ec2.DescribeSecurityGroups(nil) {
			ids = append(ids, g.ID)
		}
	case "AWS::EC2::Instance::Id":
		for _, i := range r.ec2.DescribeInstances(nil, "") {
			ids = append(ids, i.ID)
		}
	default:
		return nil, false
	}

	return ids, true
}

func (r serviceCatalogResources) ResourceAttribute(_ context.Context, paramType, id, attr string) (string, bool) {
	switch paramType {
	case "AWS::EC2::VPC::Id":
		return r.vpcAttribute(id, attr)
	case "AWS::EC2::Subnet::Id":
		return r.subnetAttribute(id, attr)
	case "AWS::EC2::SecurityGroup::Id":
		return r.groupAttribute(id, attr)
	case "AWS::EC2::Instance::Id":
		return r.instanceAttribute(id, attr)
	}

	return "", false
}

func (r serviceCatalogResources) vpcAttribute(id, attr string) (string, bool) {
	for _, v := range r.ec2.DescribeVpcs([]string{id}) {
		if attr == scAttrCIDR {
			return v.CIDRBlock, true
		}
	}

	return "", false
}

func (r serviceCatalogResources) subnetAttribute(id, attr string) (string, bool) {
	for _, s := range r.ec2.DescribeSubnets([]string{id}) {
		switch attr {
		case scAttrVPC:
			return s.VPCID, true
		case "AvailabilityZone":
			return s.AvailabilityZone, true
		case scAttrCIDR:
			return s.CIDRBlock, true
		}
	}

	return "", false
}

func (r serviceCatalogResources) groupAttribute(id, attr string) (string, bool) {
	for _, g := range r.ec2.DescribeSecurityGroups([]string{id}) {
		switch attr {
		case scAttrVPC:
			return g.VPCID, true
		case "GroupName":
			return g.Name, true
		}
	}

	return "", false
}

func (r serviceCatalogResources) instanceAttribute(id, attr string) (string, bool) {
	for _, i := range r.ec2.DescribeInstances([]string{id}, "") {
		switch attr {
		case scAttrVPC:
			return i.VPCID, true
		case scAttrSubnet:
			return i.SubnetID, true
		case "InstanceType":
			return i.InstanceType, true
		}
	}

	return "", false
}
