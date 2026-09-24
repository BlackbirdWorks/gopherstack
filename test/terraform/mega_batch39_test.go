package terraform_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appsyncsvc39 "github.com/aws/aws-sdk-go-v2/service/appsync"
	appsynctypes39 "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	athenasvc39 "github.com/aws/aws-sdk-go-v2/service/athena"
	dssvc39 "github.com/aws/aws-sdk-go-v2/service/directoryservice"
	dstypes39 "github.com/aws/aws-sdk-go-v2/service/directoryservice/types"
	lfsvc39 "github.com/aws/aws-sdk-go-v2/service/lakeformation"
	lftypes39 "github.com/aws/aws-sdk-go-v2/service/lakeformation/types"
	neptunesvc39 "github.com/aws/aws-sdk-go-v2/service/neptune"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mega39ProviderBlock extends the shared providerBlock with the neptune
// endpoint, which mega-batch-39 needs alongside lakeformation, appsync,
// athena, and ds (all already present in providerBlock).
func mega39ProviderBlock(addr string) string {
	base := providerBlock(addr)

	return strings.Replace(base, "endpoints {\n", "endpoints {\n    neptune         = "+quoteHCL(addr)+"\n", 1)
}

func quoteHCL(s string) string {
	return `"` + s + `"`
}

// TestTerraform_MegaBatch39 provisions Lake Formation resource/tag/permission/
// opt-in/data-cells-filter, AppSync API cache/domain name/domain association/
// function/source API association/type, Neptune cluster endpoint/cluster and
// instance parameter groups/cluster snapshot/event subscription/global
// cluster, Athena capacity reservation/data catalog/database/named query/
// prepared statement, and Directory Service conditional forwarder/log
// subscription/RADIUS settings/shared directory via Terraform, verifying each
// through its own SDK client.
func TestTerraform_MegaBatch39(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:       "success",
			fixture:    "mega-batch-39",
			providerFn: mega39ProviderBlock,
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return vpcCIDRVars(t)
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyMegaBatch39LakeFormation(ctx, t)
				verifyMegaBatch39AppSync(ctx, t)
				verifyMegaBatch39Neptune(ctx, t)
				verifyMegaBatch39Athena(ctx, t)
				verifyMegaBatch39DirectoryService(ctx, t)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

func verifyMegaBatch39LakeFormation(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := lfsvc39.NewFromConfig(cfg, func(o *lfsvc39.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	tagOut, err := client.GetLFTag(ctx, &lfsvc39.GetLFTagInput{
		TagKey: aws.String("mega-batch-39-tag"),
	})
	require.NoError(t, err, "GetLFTag should succeed")
	assert.ElementsMatch(t, []string{"blue", "green"}, tagOut.TagValues)

	resTagsOut, err := client.GetResourceLFTags(ctx, &lfsvc39.GetResourceLFTagsInput{
		Resource: &lftypes39.Resource{
			Database: &lftypes39.DatabaseResource{Name: aws.String("mega_batch_39_db")},
		},
	})
	require.NoError(t, err, "GetResourceLFTags should succeed")
	require.NotEmpty(t, resTagsOut.LFTagOnDatabase)

	optInsOut, err := client.ListLakeFormationOptIns(ctx, &lfsvc39.ListLakeFormationOptInsInput{})
	require.NoError(t, err, "ListLakeFormationOptIns should succeed")
	assert.NotEmpty(t, optInsOut.LakeFormationOptInsInfoList)

	filterOut, err := client.GetDataCellsFilter(ctx, &lfsvc39.GetDataCellsFilterInput{
		DatabaseName:   aws.String("mega_batch_39_db"),
		Name:           aws.String("mega-batch-39-filter"),
		TableCatalogId: aws.String("000000000000"),
		TableName:      aws.String("mega_batch_39_table"),
	})
	require.NoError(t, err, "GetDataCellsFilter should succeed")
	require.NotNil(t, filterOut.DataCellsFilter)
}

func verifyMegaBatch39AppSync(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := appsyncsvc39.NewFromConfig(cfg, func(o *appsyncsvc39.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	associationOut, err := client.GetApiAssociation(ctx, &appsyncsvc39.GetApiAssociationInput{
		DomainName: aws.String("mega-batch-39.example.test"),
	})
	require.NoError(t, err, "GetApiAssociation should succeed")
	require.NotNil(t, associationOut.ApiAssociation)

	domainOut, err := client.GetDomainName(ctx, &appsyncsvc39.GetDomainNameInput{
		DomainName: aws.String("mega-batch-39.example.test"),
	})
	require.NoError(t, err, "GetDomainName should succeed")
	require.NotNil(t, domainOut.DomainNameConfig)

	apiID := aws.ToString(associationOut.ApiAssociation.ApiId)

	cacheOut, err := client.GetApiCache(ctx, &appsyncsvc39.GetApiCacheInput{ApiId: aws.String(apiID)})
	require.NoError(t, err, "GetApiCache should succeed")
	require.NotNil(t, cacheOut.ApiCache)
	assert.Equal(t, appsynctypes39.ApiCachingBehaviorFullRequestCaching, cacheOut.ApiCache.ApiCachingBehavior)

	funcsOut, err := client.ListFunctions(ctx, &appsyncsvc39.ListFunctionsInput{ApiId: aws.String(apiID)})
	require.NoError(t, err, "ListFunctions should succeed")

	var foundFunc bool

	for _, f := range funcsOut.Functions {
		if aws.ToString(f.Name) == "mega_batch_39_function" {
			foundFunc = true
		}
	}

	assert.True(t, foundFunc, "mega-batch-39 function should be listed")

	typeOut, err := client.GetType(ctx, &appsyncsvc39.GetTypeInput{
		ApiId:    aws.String(apiID),
		Format:   appsynctypes39.TypeDefinitionFormatSdl,
		TypeName: aws.String("MegaBatch39Widget"),
	})
	require.NoError(t, err, "GetType should succeed")
	require.NotNil(t, typeOut.Type)
}

func verifyMegaBatch39Neptune(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := neptunesvc39.NewFromConfig(cfg, func(o *neptunesvc39.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	endpointsOut, err := client.DescribeDBClusterEndpoints(ctx, &neptunesvc39.DescribeDBClusterEndpointsInput{
		DBClusterIdentifier: aws.String("mega-batch-39-neptune-cluster"),
	})
	require.NoError(t, err, "DescribeDBClusterEndpoints should succeed")
	require.NotEmpty(t, endpointsOut.DBClusterEndpoints)

	cpgOut, err := client.DescribeDBClusterParameterGroups(ctx, &neptunesvc39.DescribeDBClusterParameterGroupsInput{
		DBClusterParameterGroupName: aws.String("mega-batch-39-neptune-cpg"),
	})
	require.NoError(t, err, "DescribeDBClusterParameterGroups should succeed")
	require.Len(t, cpgOut.DBClusterParameterGroups, 1)

	pgOut, err := client.DescribeDBParameterGroups(ctx, &neptunesvc39.DescribeDBParameterGroupsInput{
		DBParameterGroupName: aws.String("mega-batch-39-neptune-pg"),
	})
	require.NoError(t, err, "DescribeDBParameterGroups should succeed")
	require.Len(t, pgOut.DBParameterGroups, 1)

	snapOut, err := client.DescribeDBClusterSnapshots(ctx, &neptunesvc39.DescribeDBClusterSnapshotsInput{
		DBClusterSnapshotIdentifier: aws.String("mega-batch-39-snapshot"),
	})
	require.NoError(t, err, "DescribeDBClusterSnapshots should succeed")
	require.Len(t, snapOut.DBClusterSnapshots, 1)

	subOut, err := client.DescribeEventSubscriptions(ctx, &neptunesvc39.DescribeEventSubscriptionsInput{
		SubscriptionName: aws.String("mega-batch-39-neptune-sub"),
	})
	require.NoError(t, err, "DescribeEventSubscriptions should succeed")
	require.Len(t, subOut.EventSubscriptionsList, 1)

	globalOut, err := client.DescribeGlobalClusters(ctx, &neptunesvc39.DescribeGlobalClustersInput{
		GlobalClusterIdentifier: aws.String("mega-batch-39-global"),
	})
	require.NoError(t, err, "DescribeGlobalClusters should succeed")
	require.Len(t, globalOut.GlobalClusters, 1)
}

func verifyMegaBatch39Athena(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := athenasvc39.NewFromConfig(cfg, func(o *athenasvc39.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	reservationOut, err := client.GetCapacityReservation(ctx, &athenasvc39.GetCapacityReservationInput{
		Name: aws.String("mega-batch-39-reservation"),
	})
	require.NoError(t, err, "GetCapacityReservation should succeed")
	assert.EqualValues(t, 24, aws.ToInt32(reservationOut.CapacityReservation.TargetDpus))

	catalogOut, err := client.GetDataCatalog(ctx, &athenasvc39.GetDataCatalogInput{
		Name: aws.String("mega-batch-39-catalog"),
	})
	require.NoError(t, err, "GetDataCatalog should succeed")
	require.NotNil(t, catalogOut.DataCatalog)

	dbOut, err := client.GetDatabase(ctx, &athenasvc39.GetDatabaseInput{
		CatalogName:  aws.String("AwsDataCatalog"),
		DatabaseName: aws.String("mega_batch_39_athena_db"),
	})
	require.NoError(t, err, "GetDatabase should succeed")
	require.NotNil(t, dbOut.Database)

	namedOut, err := client.ListNamedQueries(ctx, &athenasvc39.ListNamedQueriesInput{
		WorkGroup: aws.String("mega-batch-39-workgroup"),
	})
	require.NoError(t, err, "ListNamedQueries should succeed")
	require.NotEmpty(t, namedOut.NamedQueryIds)

	batchOut, err := client.BatchGetNamedQuery(ctx, &athenasvc39.BatchGetNamedQueryInput{
		NamedQueryIds: namedOut.NamedQueryIds,
	})
	require.NoError(t, err, "BatchGetNamedQuery should succeed")

	var foundQuery bool

	for _, q := range batchOut.NamedQueries {
		if aws.ToString(q.Name) == "mega-batch-39-named-query" {
			foundQuery = true
		}
	}

	assert.True(t, foundQuery, "mega-batch-39 named query should be listed")

	preparedOut, err := client.ListPreparedStatements(ctx, &athenasvc39.ListPreparedStatementsInput{
		WorkGroup: aws.String("mega-batch-39-workgroup"),
	})
	require.NoError(t, err, "ListPreparedStatements should succeed")

	var foundPrepared bool

	for _, p := range preparedOut.PreparedStatements {
		if aws.ToString(p.StatementName) == "mega_batch_39_prepared" {
			foundPrepared = true
		}
	}

	assert.True(t, foundPrepared, "mega-batch-39 prepared statement should be listed")
}

func verifyMegaBatch39DirectoryService(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := dssvc39.NewFromConfig(cfg, func(o *dssvc39.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	dirsOut, err := client.DescribeDirectories(ctx, &dssvc39.DescribeDirectoriesInput{})
	require.NoError(t, err, "DescribeDirectories should succeed")

	var directoryID string

	for _, d := range dirsOut.DirectoryDescriptions {
		if aws.ToString(d.Name) == "mega-batch-39.test" {
			directoryID = aws.ToString(d.DirectoryId)
		}
	}

	require.NotEmpty(t, directoryID, "mega-batch-39 directory should be listed")

	for _, d := range dirsOut.DirectoryDescriptions {
		if aws.ToString(d.DirectoryId) == directoryID {
			require.NotNil(t, d.RadiusSettings)
			assert.Equal(t, "mega-batch-39-secret", aws.ToString(d.RadiusSettings.SharedSecret))
		}
	}

	fwdOut, err := client.DescribeConditionalForwarders(ctx, &dssvc39.DescribeConditionalForwardersInput{
		DirectoryId: aws.String(directoryID),
	})
	require.NoError(t, err, "DescribeConditionalForwarders should succeed")
	require.Len(t, fwdOut.ConditionalForwarders, 1)
	assert.Equal(t, "mega-batch-39-remote.test", aws.ToString(fwdOut.ConditionalForwarders[0].RemoteDomainName))

	subsOut, err := client.ListLogSubscriptions(ctx, &dssvc39.ListLogSubscriptionsInput{
		DirectoryId: aws.String(directoryID),
	})
	require.NoError(t, err, "ListLogSubscriptions should succeed")
	require.Len(t, subsOut.LogSubscriptions, 1)
	assert.Equal(t, "/aws/directoryservice/mega-batch-39", aws.ToString(subsOut.LogSubscriptions[0].LogGroupName))

	var msadID string

	for _, d := range dirsOut.DirectoryDescriptions {
		if aws.ToString(d.Name) == "mega-batch-39-msad.test" {
			msadID = aws.ToString(d.DirectoryId)
		}
	}

	require.NotEmpty(t, msadID, "mega-batch-39 Microsoft AD directory should be listed")

	sharedOut, err := client.DescribeSharedDirectories(ctx, &dssvc39.DescribeSharedDirectoriesInput{
		OwnerDirectoryId: aws.String(msadID),
	})
	require.NoError(t, err, "DescribeSharedDirectories should succeed")
	require.Len(t, sharedOut.SharedDirectories, 1)
	assert.Equal(t, "999999999999", aws.ToString(sharedOut.SharedDirectories[0].SharedAccountId))
	// aws_directory_service_shared_directory defaults method to HANDSHAKE,
	// which starts PendingAcceptance until the consumer calls
	// AcceptSharedDirectory (not attempted here -- see PARITY.md).
	assert.Equal(t, dstypes39.ShareStatusPendingAcceptance, sharedOut.SharedDirectories[0].ShareStatus)
}
