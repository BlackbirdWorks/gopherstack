package elasticache_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

// newTestStackWithBackend creates a test stack returning both the backend and the SDK client.
func newTestStackWithBackend(t *testing.T) (*elasticache.InMemoryBackend, *elasticachesdk.Client) {
	t.Helper()

	backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
	handler := elasticache.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	_ = registry.Register(handler)
	router := service.NewServiceRouter(registry)
	e.Use(router.RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := elasticachesdk.NewFromConfig(cfg, func(o *elasticachesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	return backend, client
}

// ----------------------------------------
// Log delivery configs in DescribeReplicationGroups response
// ----------------------------------------

func TestHandler_DescribeReplicationGroups_LogDeliveryConfigs_InResponse(t *testing.T) {
	t.Parallel()

	b, client := newTestStackWithBackend(t)

	_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID:          "ld-rg",
		Description: "log delivery test",
		LogDeliveryConfigurations: []elasticache.LogDeliveryConfig{
			{
				LogType:            "slow-log",
				DestinationType:    "cloudwatch-logs",
				DestinationDetails: "/aws/elasticache/ld-rg/slow-log",
				LogFormat:          "text",
				Status:             "enabled",
			},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("ld-rg"),
	})
	require.NoError(t, err)
	require.Len(t, out.ReplicationGroups, 1)
	rg := out.ReplicationGroups[0]
	assert.Len(t, rg.LogDeliveryConfigurations, 1)
	assert.Equal(t, elasticachetypes.LogTypeSlowLog, rg.LogDeliveryConfigurations[0].LogType)
	assert.Equal(t, elasticachetypes.DestinationTypeCloudWatchLogs, rg.LogDeliveryConfigurations[0].DestinationType)
}

// ----------------------------------------
// GlobalReplicationGroup NodeGroupCount in response
// ----------------------------------------

func TestHandler_CreateGlobalReplicationGroup_NodeGroupCountTracked(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateGlobalReplicationGroup(t.Context(), &elasticachesdk.CreateGlobalReplicationGroupInput{
		GlobalReplicationGroupIdSuffix: aws.String("nodecount-grg"),
		PrimaryReplicationGroupId:      aws.String(""),
	})
	require.NoError(t, err)

	out, err := client.DescribeGlobalReplicationGroups(
		t.Context(),
		&elasticachesdk.DescribeGlobalReplicationGroupsInput{
			GlobalReplicationGroupId: aws.String("ldgnf-nodecount-grg"),
		},
	)
	require.NoError(t, err)
	require.Len(t, out.GlobalReplicationGroups, 1)
	grg := out.GlobalReplicationGroups[0]
	assert.NotNil(t, grg.GlobalReplicationGroupId)
}

func TestHandler_IncreaseNodeGroupsInGRG_UpdatesCount(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateGlobalReplicationGroup(t.Context(), &elasticachesdk.CreateGlobalReplicationGroupInput{
		GlobalReplicationGroupIdSuffix: aws.String("inc-ng"),
		PrimaryReplicationGroupId:      aws.String(""),
	})
	require.NoError(t, err)

	out, err := client.IncreaseNodeGroupsInGlobalReplicationGroup(
		t.Context(),
		&elasticachesdk.IncreaseNodeGroupsInGlobalReplicationGroupInput{
			GlobalReplicationGroupId: aws.String("ldgnf-inc-ng"),
			ApplyImmediately:         aws.Bool(true),
			NodeGroupCount:           aws.Int32(4),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, out.GlobalReplicationGroup)
}

// ----------------------------------------
// DescribeServiceUpdates — real data
// ----------------------------------------

func TestHandler_DescribeServiceUpdates_ReturnsSeedData(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.DescribeServiceUpdates(t.Context(), &elasticachesdk.DescribeServiceUpdatesInput{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.ServiceUpdates), 1)
}

func TestHandler_DescribeServiceUpdates_FilterByName(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.DescribeServiceUpdates(t.Context(), &elasticachesdk.DescribeServiceUpdatesInput{
		ServiceUpdateName: aws.String("20240101-001-security-patch"),
	})
	require.NoError(t, err)
	require.Len(t, out.ServiceUpdates, 1)
	assert.Equal(t, "20240101-001-security-patch", aws.ToString(out.ServiceUpdates[0].ServiceUpdateName))
}

func TestHandler_DescribeServiceUpdates_FilterByStatus(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.DescribeServiceUpdates(t.Context(), &elasticachesdk.DescribeServiceUpdatesInput{
		ServiceUpdateStatus: []elasticachetypes.ServiceUpdateStatus{"available"},
	})
	require.NoError(t, err)

	for _, su := range out.ServiceUpdates {
		assert.Equal(t, elasticachetypes.ServiceUpdateStatusAvailable, su.ServiceUpdateStatus)
	}
}

// ----------------------------------------
// DescribeUpdateActions — after BatchApply
// ----------------------------------------

func TestHandler_DescribeUpdateActions_AfterBatchApply(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("ua-rg"),
		ReplicationGroupDescription: aws.String("update actions test"),
	})
	require.NoError(t, err)

	_, err = client.BatchApplyUpdateAction(t.Context(), &elasticachesdk.BatchApplyUpdateActionInput{
		ReplicationGroupIds: []string{"ua-rg"},
		ServiceUpdateName:   aws.String("20240101-001-security-patch"),
	})
	require.NoError(t, err)

	out, err := client.DescribeUpdateActions(t.Context(), &elasticachesdk.DescribeUpdateActionsInput{
		ServiceUpdateName: aws.String("20240101-001-security-patch"),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.UpdateActions), 1)
}

// ----------------------------------------
// DescribeEngineDefaultParameters — real defaults
// ----------------------------------------

func TestHandler_DescribeEngineDefaultParameters_ReturnsRealParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		family    string
		wantParam string
	}{
		{family: "redis7", wantParam: "maxmemory-policy"},
		{family: "memcached1.6", wantParam: "max_item_size"},
		{family: "valkey8", wantParam: "maxmemory-policy"},
	}

	for _, tt := range tests {
		t.Run(tt.family, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			out, err := client.DescribeEngineDefaultParameters(
				t.Context(),
				&elasticachesdk.DescribeEngineDefaultParametersInput{
					CacheParameterGroupFamily: aws.String(tt.family),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, out.EngineDefaults)
			assert.GreaterOrEqual(t, len(out.EngineDefaults.Parameters), 1)

			names := make(map[string]bool)
			for _, p := range out.EngineDefaults.Parameters {
				names[aws.ToString(p.ParameterName)] = true
			}

			assert.True(
				t,
				names[tt.wantParam],
				"expected parameter %q in defaults for family %q",
				tt.wantParam,
				tt.family,
			)
		})
	}
}

// ----------------------------------------
// Reserved cache node ARN in response
// ----------------------------------------

func TestHandler_PurchaseReservedCacheNode_ARNInResponse(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.PurchaseReservedCacheNodesOffering(
		t.Context(),
		&elasticachesdk.PurchaseReservedCacheNodesOfferingInput{
			ReservedCacheNodesOfferingId: aws.String("31153cd5-4ce6-45a9-b6ce-7f0b6789b8fa"),
			ReservedCacheNodeId:          aws.String("my-reserved-node"),
			CacheNodeCount:               aws.Int32(1),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, out.ReservedCacheNode)
	assert.Equal(t, "my-reserved-node", aws.ToString(out.ReservedCacheNode.ReservedCacheNodeId))
}

// ----------------------------------------
// CopySnapshot with KmsKeyId (route to copySnapshotFull handler)
// ----------------------------------------

func TestHandler_CopySnapshot_WithKmsKeyId(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("kms-copy-rg"),
		ReplicationGroupDescription: aws.String("for kms copy"),
	})
	require.NoError(t, err)

	_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
		SnapshotName:       aws.String("kms-src-snap"),
		ReplicationGroupId: aws.String("kms-copy-rg"),
	})
	require.NoError(t, err)

	out, err := client.CopySnapshot(t.Context(), &elasticachesdk.CopySnapshotInput{
		SourceSnapshotName: aws.String("kms-src-snap"),
		TargetSnapshotName: aws.String("kms-dst-snap"),
		KmsKeyId:           aws.String("arn:aws:kms:us-east-1:000000000000:key/kms-test"),
	})
	require.NoError(t, err)
	assert.Equal(t, "kms-dst-snap", aws.ToString(out.Snapshot.SnapshotName))
}

// ----------------------------------------
// ServerlessCache with UserGroupId in describe response
// ----------------------------------------

func TestHandler_DescribeServerlessCache_UserGroupId(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	opts := elasticache.ServerlessCreateOpts{
		Name:        "sc-ug",
		Engine:      "redis",
		UserGroupID: "grp-xyz",
	}
	_, err := b.CreateServerlessCacheFull(context.Background(), opts)
	require.NoError(t, err)

	p, err := b.DescribeServerlessCaches(context.Background(), "sc-ug", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "grp-xyz", p.Data[0].UserGroupID)
}
