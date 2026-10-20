package app

import (
	"context"
	"errors"
	"fmt"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	s3filesbackend "github.com/blackbirdworks/gopherstack/services/s3files"
)

type s3FilesBuckets struct{ s3 *s3backend.S3Handler }

func (r s3FilesBuckets) BucketExists(ctx context.Context, bucket string) bool {
	_, err := r.s3.Backend.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: &bucket})

	return err == nil
}

type s3FilesSubnets struct{ regions ec2Regions }

func (r s3FilesSubnets) Subnet(id string) (s3filesbackend.SubnetInfo, bool) {
	for _, b := range r.regions.handler.RegionBackends() {
		if subnets := b.DescribeSubnets([]string{id}); len(subnets) > 0 {
			sn := subnets[0]

			return s3filesbackend.SubnetInfo{
				VPCID:              sn.VPCID,
				AvailabilityZoneID: ec2backend.AvailabilityZoneID(sn.AvailabilityZone),
				CIDR:               sn.CIDRBlock,
			}, true
		}
	}

	return s3filesbackend.SubnetInfo{}, false
}

type s3FilesNetwork struct{ regions ec2Regions }

func (n s3FilesNetwork) CreateENI(req s3filesbackend.ENIRequest) (s3filesbackend.ENI, error) {
	for _, b := range n.regions.handler.RegionBackends() {
		if len(b.DescribeSubnets([]string{req.SubnetID})) == 0 {
			continue
		}

		eni, err := b.CreateNetworkInterfaceWithOptions(req.SubnetID, ec2backend.NetworkInterfaceOptions{
			Description:      req.Description,
			PrivateIP:        req.IPv4Address,
			SecurityGroupIDs: req.SecurityGroups,
		})
		if err != nil {
			return s3filesbackend.ENI{}, s3FilesEC2Error(err)
		}

		return s3filesbackend.ENI{ID: eni.ID, PrivateIP: eni.PrivateIP, SecurityGroups: eni.SecurityGroupIDs}, nil
	}

	return s3filesbackend.ENI{}, awserr.New("Subnet "+req.SubnetID+" not found", s3filesbackend.ErrNotFound)
}

func (n s3FilesNetwork) SetENISecurityGroups(eniID string, sgs []string) error {
	for _, b := range n.regions.handler.RegionBackends() {
		enis := b.DescribeNetworkInterfaces([]string{eniID})
		if len(enis) == 0 {
			continue
		}

		if err := s3FilesCheckGroupsVPC(b, enis[0].VPCID, sgs); err != nil {
			return err
		}

		return s3FilesEC2Error(b.SetNetworkInterfaceSecurityGroups(eniID, sgs))
	}

	return nil
}

func s3FilesCheckGroupsVPC(b ec2backend.Backend, vpcID string, sgs []string) error {
	for _, id := range sgs {
		found := b.DescribeSecurityGroups([]string{id})
		if len(found) == 0 {
			return awserr.New("Security group "+id+" not found", s3filesbackend.ErrNotFound)
		}

		if found[0].VPCID != vpcID {
			return awserr.New("Security group "+id+" does not belong to the subnet's VPC", s3filesbackend.ErrValidation)
		}
	}

	return nil
}

func (n s3FilesNetwork) DeleteENI(eniID string) {
	for _, b := range n.regions.handler.RegionBackends() {
		if len(b.DescribeNetworkInterfaces([]string{eniID})) > 0 {
			_ = b.DeleteNetworkInterface(eniID)

			return
		}
	}
}

// s3FilesEC2Error maps EC2 failures onto the S3 Files error set.
func s3FilesEC2Error(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ec2backend.ErrAddressInUse):
		return awserr.New(err.Error(), s3filesbackend.ErrConflict)
	case errors.Is(err, ec2backend.ErrSecurityGroupNotFound), errors.Is(err, ec2backend.ErrSubnetNotFound):
		return awserr.New(err.Error(), s3filesbackend.ErrNotFound)
	default:
		return awserr.New(err.Error(), s3filesbackend.ErrValidation)
	}
}

type s3FilesRoles struct{ iam iambackend.StorageBackend }

func (r s3FilesRoles) RoleExists(roleARN string) bool {
	_, err := r.iam.GetRoleByArn(roleARN)

	return err == nil
}

var (
	errS3FilesKeyMissing  = errors.New("KMS key does not exist")
	errS3FilesKeyDisabled = errors.New("KMS key is not enabled")
)

type s3FilesKeys struct{ kms kmsbackend.StorageBackend }

func (k s3FilesKeys) CheckKey(ctx context.Context, keyID string) error {
	out, err := k.kms.DescribeKey(ctx, &kmsbackend.DescribeKeyInput{KeyID: keyID})
	if err != nil {
		return fmt.Errorf("%w: %s", errS3FilesKeyMissing, keyID)
	}

	if out.KeyMetadata.KeyState != kmsbackend.KeyStateEnabled {
		return fmt.Errorf("%w: %s is %s", errS3FilesKeyDisabled, keyID, out.KeyMetadata.KeyState)
	}

	return nil
}

// wireS3Files lets S3 Files check buckets (S3), subnets, security groups and ENIs (EC2), roles (IAM) and keys (KMS).
func wireS3Files(byName map[string]service.Registerable) {
	h, ok := byName["S3Files"].(*s3filesbackend.Handler)
	if !ok {
		return
	}

	if s3h, isS3 := byName["S3"].(*s3backend.S3Handler); isS3 {
		h.Backend.SetBucketResolver(s3FilesBuckets{s3: s3h})
	}

	if ec2h, isEC2 := byName["EC2"].(*ec2backend.Handler); isEC2 {
		regions := ec2Regions{handler: ec2h}
		h.Backend.SetSubnetResolver(s3FilesSubnets{regions: regions})
		h.Backend.SetNetworkResolver(s3FilesNetwork{regions: regions})
	}

	if iamH, isIAM := byName["IAM"].(*iambackend.Handler); isIAM {
		h.Backend.SetRoleResolver(s3FilesRoles{iam: iamH.Backend})
	}

	if kmsH, isKMS := byName["KMS"].(*kmsbackend.Handler); isKMS {
		h.Backend.SetKeyResolver(s3FilesKeys{kms: kmsH.Backend})
	}
}
