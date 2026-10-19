package timestreaminfluxdb_test

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb"
	"github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/timestreaminfluxdb"
)

const testAccount = "123456789012"

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

type fakeSubnets map[string]string

func (f fakeSubnets) SubnetCIDR(_, id string) (string, bool) {
	c, ok := f[id]

	return c, ok
}

type fixture struct {
	client  *sdk.Client
	backend *timestreaminfluxdb.InMemoryBackend
	clock   *fakeClock
	url     string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	b := timestreaminfluxdb.NewInMemoryBackend(testAccount, "us-east-1")
	t.Cleanup(b.Close)

	return newFixtureFromBackend(t, b, &fakeClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)})
}

func newFixtureFromBackend(t *testing.T, b *timestreaminfluxdb.InMemoryBackend, clock *fakeClock) *fixture {
	t.Helper()

	b.SetClock(clock.now)

	url := sdktest.Serve(t, timestreaminfluxdb.NewHandler(b))

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return &fixture{
		client: sdk.NewFromConfig(
			cfg,
			func(o *sdk.Options) { o.BaseEndpoint = aws.String(url) },
		),
		backend: b,
		clock:   clock,
		url:     url,
	}
}

func (f *fixture) settle() { f.clock.advance(time.Minute) }

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

func instanceInput(name string) *sdk.CreateDbInstanceInput {
	return &sdk.CreateDbInstanceInput{
		Name: aws.String(
			name,
		),
		Password:       aws.String("Passw0rdPassw0rd"),
		DbInstanceType: types.DbInstanceTypeDbInfluxMedium,
		AllocatedStorage: aws.Int32(
			100,
		),
		VpcSubnetIds:        []string{"subnet-aaa111"},
		VpcSecurityGroupIds: []string{"sg-bbb222"},
	}
}

func clusterInput(name string) *sdk.CreateDbClusterInput {
	return &sdk.CreateDbClusterInput{
		Name: aws.String(
			name,
		),
		Password:            aws.String("Passw0rdPassw0rd"),
		DbInstanceType:      types.DbInstanceTypeDbInfluxLarge,
		VpcSubnetIds:        []string{"subnet-aaa111", "subnet-bbb222"},
		VpcSecurityGroupIds: []string{"sg-bbb222"},
		DeploymentType:      types.ClusterDeploymentTypeMultiNodeReadReplicas,
	}
}

func (f *fixture) instance(t *testing.T, name string) *sdk.CreateDbInstanceOutput {
	t.Helper()

	out, err := f.client.CreateDbInstance(t.Context(), instanceInput(name))
	require.NoError(t, err)
	f.settle()

	return out
}

func (f *fixture) cluster(t *testing.T, name string) string {
	t.Helper()

	out, err := f.client.CreateDbCluster(t.Context(), clusterInput(name))
	require.NoError(t, err)
	f.settle()

	return aws.ToString(out.DbClusterId)
}

func (f *fixture) regionClient(t *testing.T, region string) *sdk.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return sdk.NewFromConfig(cfg, func(o *sdk.Options) { o.BaseEndpoint = aws.String(f.url) })
}
