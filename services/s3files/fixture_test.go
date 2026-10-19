package s3files_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	s3filessdk "github.com/aws/aws-sdk-go-v2/service/s3files"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/s3files"
)

const (
	testAccount = "123456789012"
	testRegion  = "us-east-1"
	testBucket  = "arn:aws:s3:::data-bucket"
	testRole    = "arn:aws:iam::123456789012:role/s3files"
	testSubnet  = "subnet-aaaa"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

type fakeBuckets map[string]bool

func (f fakeBuckets) BucketExists(_ context.Context, bucket string) bool { return f[bucket] }

type fakeSubnets map[string]s3files.SubnetInfo

func (f fakeSubnets) Subnet(id string) (s3files.SubnetInfo, bool) {
	info, ok := f[id]

	return info, ok
}

type fixture struct {
	client  *s3filessdk.Client
	backend *s3files.InMemoryBackend
	clock   *fakeClock
	url     string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	return newFixtureWith(t, s3files.NewInMemoryBackend(testAccount, testRegion), testRegion)
}

func newFixtureWith(t *testing.T, backend *s3files.InMemoryBackend, region string) *fixture {
	t.Helper()

	clock := &fakeClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	backend.SetClock(clock.now)
	backend.SetBucketResolver(fakeBuckets{"data-bucket": true, "other": true})
	backend.SetSubnetResolver(fakeSubnets{
		testSubnet:    {VPCID: "vpc-1", AvailabilityZoneID: "use1-az1", CIDR: "10.0.1.0/24"},
		"subnet-bbbb": {VPCID: "vpc-1", AvailabilityZoneID: "use1-az2", CIDR: "10.0.2.0/24"},
	})

	url := sdktest.Serve(t, s3files.NewHandler(backend))

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := s3filessdk.NewFromConfig(cfg, func(o *s3filessdk.Options) { o.BaseEndpoint = aws.String(url) })

	return &fixture{client: client, backend: backend, clock: clock, url: url}
}

func (f *fixture) fileSystem(t *testing.T) string {
	t.Helper()

	out, err := f.client.CreateFileSystem(t.Context(), &s3filessdk.CreateFileSystemInput{
		Bucket: aws.String(testBucket), RoleArn: aws.String(testRole),
	})
	require.NoError(t, err)

	return aws.ToString(out.FileSystemId)
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}
