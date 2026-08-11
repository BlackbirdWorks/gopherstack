package ec2_test

import (
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/aws/aws-sdk-go-v2/service/outposts/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/blackbirdworks/gopherstack/services/outposts"
)

const crossServiceTestAccountID = "123456789012"

const crossServiceTestRegion = "us-east-1"

// fakeSiblingServices satisfies cross_service.go's siblingServices
// interface (the subset of *CLI's method set ec2 needs), letting a unit
// test wire an ec2.InMemoryBackend to a real outposts.InMemoryBackend
// without standing up the whole CLI -- matching real AWS's own
// RunInstances/TerminateInstances-to-Outposts coupling that
// cross_service.go implements.
type fakeSiblingServices struct {
	outpostsHandler service.Registerable
}

func (f fakeSiblingServices) GetOutpostsHandler() service.Registerable { return f.outpostsHandler }

// newWiredBackends returns an ec2 backend wired (via SetAppConfig) to a
// real outposts backend, plus a real aws-sdk-go-v2 outposts client against
// that backend for setting up Outpost/capacity fixtures the way a real
// caller would -- outposts' own request/response wire types are
// unexported, so its backend methods can't be called with hand-built
// structs from outside the package.
func newWiredBackends(t *testing.T) (*ec2.InMemoryBackend, *outpostssdk.Client) {
	t.Helper()

	ec2Bk := ec2.NewInMemoryBackend(crossServiceTestAccountID, crossServiceTestRegion)

	outpostsBk := outposts.NewInMemoryBackend(t.Context(), crossServiceTestAccountID, crossServiceTestRegion)
	t.Cleanup(outpostsBk.Close)
	outpostsHandler := outposts.NewHandler(outpostsBk)

	ec2Bk.SetAppConfig(fakeSiblingServices{outpostsHandler: outpostsHandler})

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(outpostsHandler))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(crossServiceTestRegion),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := outpostssdk.NewFromConfig(cfg, func(o *outpostssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	return ec2Bk, client
}

// setupOutpostWithCapacity creates a Site/Outpost via the real outposts SDK
// client and configures instanceType capacity on its seeded Asset,
// returning the Outpost's ARN.
func setupOutpostWithCapacity(t *testing.T, client *outpostssdk.Client, instanceType string, count int32) string {
	t.Helper()

	site, err := client.CreateSite(t.Context(), &outpostssdk.CreateSiteInput{
		Name: aws.String("ec2-cross-service-test-site"),
	})
	require.NoError(t, err)

	outpost, err := client.CreateOutpost(t.Context(), &outpostssdk.CreateOutpostInput{
		Name:                  aws.String("ec2-cross-service-test-outpost"),
		SiteId:                site.Site.SiteId,
		SupportedHardwareType: types.SupportedHardwareTypeRack,
	})
	require.NoError(t, err)
	require.NotNil(t, outpost.Outpost)

	assets, err := client.ListAssets(t.Context(), &outpostssdk.ListAssetsInput{
		OutpostIdentifier: outpost.Outpost.OutpostId,
	})
	require.NoError(t, err)
	require.Len(t, assets.Assets, 1)

	task, err := client.StartCapacityTask(t.Context(), &outpostssdk.StartCapacityTaskInput{
		OutpostIdentifier: outpost.Outpost.OutpostId,
		AssetId:           assets.Assets[0].AssetId,
		InstancePools: []types.InstanceTypeCapacity{
			{InstanceType: aws.String(instanceType), Count: count},
		},
	})
	require.NoError(t, err)
	require.Equal(t, types.CapacityTaskStatusRequested, task.CapacityTaskStatus)

	require.Eventually(t, func() bool {
		got, getErr := client.GetCapacityTask(t.Context(), &outpostssdk.GetCapacityTaskInput{
			OutpostIdentifier: outpost.Outpost.OutpostId,
			CapacityTaskId:    task.CapacityTaskId,
		})

		return getErr == nil && got.CapacityTaskStatus == types.CapacityTaskStatusCompleted
	}, time.Second, 10*time.Millisecond)

	return aws.ToString(outpost.Outpost.OutpostArn)
}

func TestCreateSubnetWithOutpost(t *testing.T) {
	t.Parallel()

	ec2Bk, client := newWiredBackends(t)

	vpc, err := ec2Bk.CreateVpc("10.1.0.0/16")
	require.NoError(t, err)

	validArn := setupOutpostWithCapacity(t, client, "m5.xlarge", 1)

	tests := []struct {
		wantErr    error
		name       string
		outpostArn string
	}{
		{name: "no outpost arn creates a normal subnet", outpostArn: ""},
		{name: "valid outpost arn is accepted", outpostArn: validArn},
		{
			name:       "unknown outpost arn is rejected",
			outpostArn: "arn:aws:outposts:us-east-1:123456789012:outpost/op-doesnotexist00",
			wantErr:    ec2.ErrOutpostArnNotFound,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cidr := fmt.Sprintf("10.1.%d.0/24", i+1)

			subnet, subnetErr := ec2Bk.CreateSubnetWithOutpost(vpc.ID, cidr, "us-east-1a", tt.outpostArn)

			if tt.wantErr != nil {
				require.ErrorIs(t, subnetErr, tt.wantErr)

				return
			}

			require.NoError(t, subnetErr)
			assert.Equal(t, tt.outpostArn, subnet.OutpostArn)
		})
	}
}

func TestCreateSubnetWithOutpost_UnwiredOutpostsIsNoop(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend(crossServiceTestAccountID, crossServiceTestRegion)

	vpc, err := b.CreateVpc("10.4.0.0/16")
	require.NoError(t, err)

	arn := "arn:aws:outposts:us-east-1:123456789012:outpost/op-anything00000"

	subnet, err := b.CreateSubnetWithOutpost(vpc.ID, "10.4.1.0/24", "us-east-1a", arn)
	require.NoError(t, err, "unwired Outposts backend must not block subnet creation")
	assert.Equal(t, arn, subnet.OutpostArn)
}

func TestRunInstances_OutpostCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr       error
		name          string
		configuredQty int32
		launchCount   int
	}{
		{name: "launches within available capacity", configuredQty: 3, launchCount: 2},
		{name: "launches exactly all available capacity", configuredQty: 2, launchCount: 2},
		{
			name: "rejects exceeding available capacity", configuredQty: 1, launchCount: 2,
			wantErr: ec2.ErrInsufficientInstanceCapacity,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ec2Bk, client := newWiredBackends(t)

			vpc, err := ec2Bk.CreateVpc("10.2.0.0/16")
			require.NoError(t, err)

			outpostArn := setupOutpostWithCapacity(t, client, "m5.xlarge", tt.configuredQty)
			subnet, err := ec2Bk.CreateSubnetWithOutpost(vpc.ID, "10.2.1.0/24", "us-east-1a", outpostArn)
			require.NoError(t, err)

			instances, err := ec2Bk.RunInstances("ami-123", "m5.xlarge", subnet.ID, tt.launchCount)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Empty(t, instances)

				return
			}

			require.NoError(t, err)
			require.Len(t, instances, tt.launchCount)

			for _, inst := range instances {
				assert.Equal(t, outpostArn, inst.OutpostArn)
			}
		})
	}
}

func TestRunInstances_UnwiredOutpostsSkipsCapacityCheck(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend(crossServiceTestAccountID, crossServiceTestRegion)

	vpc, err := b.CreateVpc("10.5.0.0/16")
	require.NoError(t, err)

	arn := "arn:aws:outposts:us-east-1:123456789012:outpost/op-anything00001"

	subnet, err := b.CreateSubnetWithOutpost(vpc.ID, "10.5.1.0/24", "us-east-1a", arn)
	require.NoError(t, err)

	instances, err := b.RunInstances("ami-123", "m5.xlarge", subnet.ID, 5)
	require.NoError(t, err, "unwired Outposts backend must not block RunInstances")
	assert.Len(t, instances, 5)
}

// TestRunInstancesThenTerminateInstances_ReleasesOutpostCapacity is a
// sequential lifecycle (launch -> deplete -> terminate -> capacity
// returns), not a permutation table.
func TestRunInstancesThenTerminateInstances_ReleasesOutpostCapacity(t *testing.T) {
	t.Parallel()

	ec2Bk, client := newWiredBackends(t)

	vpc, err := ec2Bk.CreateVpc("10.3.0.0/16")
	require.NoError(t, err)

	outpostArn := setupOutpostWithCapacity(t, client, "m5.xlarge", 1)
	subnet, err := ec2Bk.CreateSubnetWithOutpost(vpc.ID, "10.3.1.0/24", "us-east-1a", outpostArn)
	require.NoError(t, err)

	instances, err := ec2Bk.RunInstances("ami-123", "m5.xlarge", subnet.ID, 1)
	require.NoError(t, err)
	require.Len(t, instances, 1)
	assert.Equal(t, outpostArn, instances[0].OutpostArn)

	_, err = ec2Bk.RunInstances("ami-123", "m5.xlarge", subnet.ID, 1)
	require.ErrorIs(t, err, ec2.ErrInsufficientInstanceCapacity, "capacity is fully depleted by the first launch")

	_, err = ec2Bk.TerminateInstances([]string{instances[0].ID})
	require.NoError(t, err)

	instances2, err := ec2Bk.RunInstances("ami-123", "m5.xlarge", subnet.ID, 1)
	require.NoError(t, err, "terminating the first instance must return its capacity")
	require.Len(t, instances2, 1)

	_, err = ec2Bk.TerminateInstances([]string{instances[0].ID})
	require.NoError(t, err, "terminating an already-terminated instance is a no-op, not an error")

	_, err = ec2Bk.RunInstances("ami-123", "m5.xlarge", subnet.ID, 1)
	require.ErrorIs(t, err, ec2.ErrInsufficientInstanceCapacity, "double-terminate must not double-credit capacity")
}
