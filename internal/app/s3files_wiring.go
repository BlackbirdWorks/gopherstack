package app

import (
	"context"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
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

// wireS3Files lets S3 Files validate bucket ARNs against S3 and mount-target subnets against EC2.
func wireS3Files(byName map[string]service.Registerable) {
	h, ok := byName["S3Files"].(*s3filesbackend.Handler)
	if !ok {
		return
	}

	if s3h, isS3 := byName["S3"].(*s3backend.S3Handler); isS3 {
		h.Backend.SetBucketResolver(s3FilesBuckets{s3: s3h})
	}

	if ec2h, isEC2 := byName["EC2"].(*ec2backend.Handler); isEC2 {
		h.Backend.SetSubnetResolver(s3FilesSubnets{regions: ec2Regions{handler: ec2h}})
	}
}
