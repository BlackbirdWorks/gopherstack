package app

import (
	"context"
	"slices"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
)

const (
	scAttrCIDR   = "CidrBlock"
	scAttrVPC    = "VpcId"
	scAttrSubnet = "SubnetId"
	scAttrAZ     = "AvailabilityZone"

	scTypeVPC       = "AWS::EC2::VPC::Id"
	scTypeSubnet    = "AWS::EC2::Subnet::Id"
	scTypeGroupID   = "AWS::EC2::SecurityGroup::Id"
	scTypeGroupName = "AWS::EC2::SecurityGroup::GroupName"
	scTypeInstance  = "AWS::EC2::Instance::Id"
	scTypeImage     = "AWS::EC2::Image::Id"
	scTypeKeyPair   = "AWS::EC2::KeyPair::KeyName"
	scTypeVolume    = "AWS::EC2::Volume::Id"
	scTypeAZ        = "AWS::EC2::AvailabilityZone::Name"
	scTypeZone      = "AWS::Route53::HostedZone::Id"

	scMaxZonePage = 100
)

// serviceCatalogResources answers TEMPLATE rule functions from the EC2 and Route 53 backends.
// Attribute names are the member names of the matching Describe* response.
type serviceCatalogResources struct {
	ec2     ec2backend.Backend
	route53 route53backend.StorageBackend
}

func (r serviceCatalogResources) ResourceIDs(ctx context.Context, paramType string) ([]string, bool) {
	var ids []string

	switch paramType {
	case scTypeVPC:
		for _, v := range r.ec2.DescribeVpcs(nil) {
			ids = append(ids, v.ID)
		}
	case scTypeSubnet:
		for _, s := range r.ec2.DescribeSubnets(nil) {
			ids = append(ids, s.ID)
		}
	case scTypeGroupID:
		for _, g := range r.ec2.DescribeSecurityGroups(nil) {
			ids = append(ids, g.ID)
		}
	case scTypeGroupName:
		for _, g := range r.ec2.DescribeSecurityGroups(nil) {
			ids = append(ids, g.Name)
		}
	case scTypeInstance:
		for _, i := range r.ec2.DescribeInstances(nil, "") {
			ids = append(ids, i.ID)
		}
	case scTypeImage:
		for _, i := range r.ec2.DescribeImages() {
			ids = append(ids, i.ImageID)
		}
	case scTypeKeyPair:
		for _, k := range r.ec2.DescribeKeyPairs(nil) {
			ids = append(ids, k.Name)
		}
	case scTypeVolume:
		for _, v := range r.ec2.DescribeVolumes(nil) {
			ids = append(ids, v.ID)
		}
	case scTypeAZ:
		ids = r.ec2.DescribeAvailabilityZones(awsmeta.Region(ctx))
	case scTypeZone:
		zones, ok := r.hostedZones()
		if !ok {
			return nil, false
		}

		for _, z := range zones {
			ids = append(ids, z.ID)
		}
	default:
		return nil, false
	}

	return ids, true
}

func (r serviceCatalogResources) hostedZones() ([]route53backend.HostedZone, bool) {
	if r.route53 == nil {
		return nil, false
	}

	var (
		out   []route53backend.HostedZone
		token string
	)

	for {
		pg, err := r.route53.ListHostedZones(token, scMaxZonePage, "", "")
		if err != nil {
			return nil, false
		}

		out = append(out, pg.Data...)

		if pg.Next == "" {
			return out, true
		}

		token = pg.Next
	}
}

func (r serviceCatalogResources) ResourceAttribute(
	ctx context.Context, paramType, id, attr string,
) (string, bool) {
	switch paramType {
	case scTypeVPC:
		return r.vpcAttribute(id, attr)
	case scTypeSubnet:
		return r.subnetAttribute(id, attr)
	case scTypeGroupID:
		return r.groupAttribute(r.ec2.DescribeSecurityGroups([]string{id}), attr)
	case scTypeGroupName:
		return r.groupAttribute(r.groupsNamed(id), attr)
	case scTypeInstance:
		return r.instanceAttribute(id, attr)
	case scTypeImage:
		return r.imageAttribute(id, attr)
	case scTypeKeyPair:
		return r.keyPairAttribute(id, attr)
	case scTypeVolume:
		return r.volumeAttribute(id, attr)
	case scTypeAZ:
		return r.zoneNameAttribute(ctx, id, attr)
	case scTypeZone:
		return r.hostedZoneAttribute(id, attr)
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
		case scAttrAZ:
			return s.AvailabilityZone, true
		case scAttrCIDR:
			return s.CIDRBlock, true
		}
	}

	return "", false
}

func (r serviceCatalogResources) groupsNamed(name string) []*ec2backend.SecurityGroup {
	var out []*ec2backend.SecurityGroup

	for _, g := range r.ec2.DescribeSecurityGroups(nil) {
		if g.Name == name {
			out = append(out, g)
		}
	}

	return out
}

func (r serviceCatalogResources) groupAttribute(groups []*ec2backend.SecurityGroup, attr string) (string, bool) {
	for _, g := range groups {
		switch attr {
		case scAttrVPC:
			return g.VPCID, true
		case "GroupName":
			return g.Name, true
		case "GroupId":
			return g.ID, true
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

func (r serviceCatalogResources) imageAttribute(id, attr string) (string, bool) {
	for _, i := range r.ec2.DescribeImages() {
		if i.ImageID != id {
			continue
		}

		switch attr {
		case "Name":
			return i.Name, true
		case "Architecture":
			return i.Architecture, true
		case "OwnerId":
			return i.OwnerID, true
		case "State":
			return i.State, true
		case "VirtualizationType":
			return i.VirtualizationType, true
		}
	}

	return "", false
}

func (r serviceCatalogResources) keyPairAttribute(name, attr string) (string, bool) {
	for _, k := range r.ec2.DescribeKeyPairs([]string{name}) {
		switch attr {
		case "KeyFingerprint":
			return k.Fingerprint, true
		case "KeyType":
			return k.KeyType, true
		case "KeyPairId":
			return k.KeyPairID, true
		}
	}

	return "", false
}

func (r serviceCatalogResources) volumeAttribute(id, attr string) (string, bool) {
	for _, v := range r.ec2.DescribeVolumes([]string{id}) {
		switch attr {
		case scAttrAZ:
			return v.AZ, true
		case "Size":
			return strconv.Itoa(v.Size), true
		case "VolumeType":
			return v.VolumeType, true
		case "State":
			return v.State, true
		case "Encrypted":
			return strconv.FormatBool(v.Encrypted), true
		}
	}

	return "", false
}

func (r serviceCatalogResources) zoneNameAttribute(ctx context.Context, name, attr string) (string, bool) {
	region := awsmeta.Region(ctx)
	if !slices.Contains(r.ec2.DescribeAvailabilityZones(region), name) {
		return "", false
	}

	switch attr {
	case "ZoneName":
		return name, true
	case "RegionName":
		return region, true
	case "State":
		return "available", true
	}

	return "", false
}

func (r serviceCatalogResources) hostedZoneAttribute(id, attr string) (string, bool) {
	zones, ok := r.hostedZones()
	if !ok {
		return "", false
	}

	for _, z := range zones {
		if z.ID != id {
			continue
		}

		switch attr {
		case "Name":
			return z.Name, true
		case "Comment":
			return z.Comment, true
		case "CallerReference":
			return z.CallerReference, true
		}
	}

	return "", false
}
