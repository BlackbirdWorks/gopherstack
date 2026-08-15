package docdb_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	docdbsdk "github.com/aws/aws-sdk-go-v2/service/docdb"
	"github.com/aws/aws-sdk-go-v2/service/docdb/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/docdb"
)

// newTestDocDBClient stands up the real aws-sdk-go-v2 DocDB client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production. Round-tripping through the
// genuine SDK serializer/deserializer (rather than string-matching the raw XML
// body, as most other tests in this package do) is what actually proves a
// response is wire-compatible: a response can look plausible as a string yet
// still make the SDK's XML deserializer produce zero-valued fields (e.g. an
// extra nesting level inside a list member) or fail outright (e.g. a missing
// "*Result" element the deserializer unconditionally looks for).
func newTestDocDBClient(t *testing.T, h *docdb.Handler) *docdbsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(rtTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return docdbsdk.NewFromConfig(cfg, func(o *docdbsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

const rtTestRegion = "us-east-1"

// Test_SDKRoundTrip_CreateDBCluster_AvailabilityZones proves the real SDK
// client's AvailabilityZones round-trip end to end, exercising two
// independent bugs at once:
//  1. Request side: the real serializer
//     (awsAwsquery_serializeDocumentAvailabilityZones) encodes each AZ as
//     "AvailabilityZones.AvailabilityZone.N", not the generic
//     "AvailabilityZones.member.N" the handler used to parse -- so every real
//     client's AZs were silently dropped before even reaching the backend.
//  2. Response side: the handler used to wrap each AZ in an extra
//     <Name>...</Name> child element inside <AvailabilityZone>, but the real
//     deserializer (awsAwsquery_deserializeDocumentAvailabilityZones) reads
//     the <AvailabilityZone> element's own text value directly -- so even a
//     correctly-stored AZ would decode as an empty string.
func Test_SDKRoundTrip_CreateDBCluster_AvailabilityZones(t *testing.T) {
	t.Parallel()

	backend := docdb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := docdb.NewHandler(backend)
	client := newTestDocDBClient(t, h)

	out, err := client.CreateDBCluster(t.Context(), &docdbsdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("rt-az-cluster"),
		Engine:              aws.String("docdb"),
		AvailabilityZones:   []string{"us-east-1a", "us-east-1b"},
	})
	require.NoError(t, err)
	require.NotNil(t, out.DBCluster)
	require.ElementsMatch(t, []string{"us-east-1a", "us-east-1b"}, out.DBCluster.AvailabilityZones)
}

// Test_SDKRoundTrip_DeleteDBInstance_NotFoundIsTyped proves the real SDK
// client can type-assert a DeleteDBInstance "not found" error into
// *types.DBInstanceNotFoundFault. Before the fix, the handler put
// "DBInstanceNotFoundFault" on the wire as the error Code, but the real
// deserializer (awsAwsquery_deserializeOpErrorDeleteDBInstance) switches on
// the literal string "DBInstanceNotFound" (no "Fault" suffix) -- any other
// code value falls through to a generic *smithy.GenericAPIError, so callers
// doing errors.As against the typed fault silently stopped matching.
func Test_SDKRoundTrip_DeleteDBInstance_NotFoundIsTyped(t *testing.T) {
	t.Parallel()

	backend := docdb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := docdb.NewHandler(backend)
	client := newTestDocDBClient(t, h)

	_, err := client.DeleteDBInstance(t.Context(), &docdbsdk.DeleteDBInstanceInput{
		DBInstanceIdentifier: aws.String("does-not-exist"),
	})
	require.Error(t, err)

	var notFound *types.DBInstanceNotFoundFault
	require.ErrorAs(t, err, &notFound, "expected a typed DBInstanceNotFoundFault, got %v", err)
}

// Test_SDKRoundTrip_CreateDBClusterParameterGroup_AlreadyExistsIsTyped proves
// the real SDK client can type-assert a CreateDBClusterParameterGroup
// "already exists" error into *types.DBParameterGroupAlreadyExistsFault. AWS
// reuses the plain RDS DBParameterGroup fault codes ("DBParameterGroupNotFound"
// / "DBParameterGroupAlreadyExists") for DBClusterParameterGroup operations --
// there is no "DBClusterParameterGroupAlreadyExistsFault" wire code at all.
func Test_SDKRoundTrip_CreateDBClusterParameterGroup_AlreadyExistsIsTyped(t *testing.T) {
	t.Parallel()

	backend := docdb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := docdb.NewHandler(backend)
	client := newTestDocDBClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDBClusterParameterGroup(ctx, &docdbsdk.CreateDBClusterParameterGroupInput{
		DBClusterParameterGroupName: aws.String("rt-pg"),
		DBParameterGroupFamily:      aws.String("docdb4.0"),
		Description:                 aws.String("roundtrip test"),
	})
	require.NoError(t, err)

	_, err = client.CreateDBClusterParameterGroup(ctx, &docdbsdk.CreateDBClusterParameterGroupInput{
		DBClusterParameterGroupName: aws.String("rt-pg"),
		DBParameterGroupFamily:      aws.String("docdb4.0"),
		Description:                 aws.String("roundtrip test dup"),
	})
	require.Error(t, err)

	var alreadyExists *types.DBParameterGroupAlreadyExistsFault
	require.ErrorAs(t, err, &alreadyExists, "expected a typed DBParameterGroupAlreadyExistsFault, got %v", err)
}

// Test_SDKRoundTrip_CreateDBSubnetGroup_SubnetIds proves the real SDK client's
// SubnetIds actually reach the backend. The real serializer
// (awsAwsquery_serializeDocumentSubnetIdentifierList) encodes each element as
// "SubnetIds.SubnetIdentifier.N", not the generic "SubnetIds.member.N" the
// handler used to parse -- so every subnet ID a real client sent was silently
// dropped, and CreateDBSubnetGroup always persisted an empty subnet list.
func Test_SDKRoundTrip_CreateDBSubnetGroup_SubnetIds(t *testing.T) {
	t.Parallel()

	backend := docdb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := docdb.NewHandler(backend)
	client := newTestDocDBClient(t, h)

	out, err := client.CreateDBSubnetGroup(t.Context(), &docdbsdk.CreateDBSubnetGroupInput{
		DBSubnetGroupName:        aws.String("rt-subnet-group"),
		DBSubnetGroupDescription: aws.String("roundtrip test"),
		SubnetIds:                []string{"subnet-aaaa1111", "subnet-bbbb2222"},
	})
	require.NoError(t, err)
	require.NotNil(t, out.DBSubnetGroup)

	gotIDs := make([]string, 0, len(out.DBSubnetGroup.Subnets))
	for _, s := range out.DBSubnetGroup.Subnets {
		gotIDs = append(gotIDs, aws.ToString(s.SubnetIdentifier))
	}
	require.ElementsMatch(t, []string{"subnet-aaaa1111", "subnet-bbbb2222"}, gotIDs)
}

// Test_SDKRoundTrip_CreateDBCluster_VpcSecurityGroupIds proves the real SDK
// client's VpcSecurityGroupIds actually reach the backend. The real
// serializer (awsAwsquery_serializeDocumentVpcSecurityGroupIdList) encodes
// each element as "VpcSecurityGroupIds.VpcSecurityGroupId.N", not the generic
// "VpcSecurityGroupIds.member.N" the handler used to parse.
func Test_SDKRoundTrip_CreateDBCluster_VpcSecurityGroupIds(t *testing.T) {
	t.Parallel()

	backend := docdb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := docdb.NewHandler(backend)
	client := newTestDocDBClient(t, h)

	out, err := client.CreateDBCluster(t.Context(), &docdbsdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("rt-sg-cluster"),
		Engine:              aws.String("docdb"),
		VpcSecurityGroupIds: []string{"sg-11112222", "sg-33334444"},
	})
	require.NoError(t, err)
	require.NotNil(t, out.DBCluster)

	gotIDs := make([]string, 0, len(out.DBCluster.VpcSecurityGroups))
	for _, sg := range out.DBCluster.VpcSecurityGroups {
		gotIDs = append(gotIDs, aws.ToString(sg.VpcSecurityGroupId))
	}
	require.ElementsMatch(t, []string{"sg-11112222", "sg-33334444"}, gotIDs)
}

// Test_SDKRoundTrip_ModifyDBClusterParameterGroup_Parameters proves the real
// SDK client's Parameters actually apply. The real serializer
// (awsAwsquery_serializeDocumentParametersList) encodes each element as
// "Parameters.Parameter.N", not the generic "Parameters.member.N" the handler
// used to parse -- so ModifyDBClusterParameterGroup silently ignored every
// parameter a real client sent, a disguised no-op hidden entirely by the
// wrong form-field name.
func Test_SDKRoundTrip_ModifyDBClusterParameterGroup_Parameters(t *testing.T) {
	t.Parallel()

	backend := docdb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := docdb.NewHandler(backend)
	client := newTestDocDBClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDBClusterParameterGroup(ctx, &docdbsdk.CreateDBClusterParameterGroupInput{
		DBClusterParameterGroupName: aws.String("rt-mod-pg"),
		DBParameterGroupFamily:      aws.String("docdb4.0"),
		Description:                 aws.String("roundtrip test"),
	})
	require.NoError(t, err)

	_, err = client.ModifyDBClusterParameterGroup(ctx, &docdbsdk.ModifyDBClusterParameterGroupInput{
		DBClusterParameterGroupName: aws.String("rt-mod-pg"),
		Parameters: []types.Parameter{
			{ParameterName: aws.String("tls"), ParameterValue: aws.String("disabled")},
		},
	})
	require.NoError(t, err)

	descOut, err := client.DescribeDBClusterParameters(ctx, &docdbsdk.DescribeDBClusterParametersInput{
		DBClusterParameterGroupName: aws.String("rt-mod-pg"),
	})
	require.NoError(t, err)

	found := false
	for _, p := range descOut.Parameters {
		if aws.ToString(p.ParameterName) == "tls" {
			found = true
			require.Equal(t, "disabled", aws.ToString(p.ParameterValue))
		}
	}
	require.True(t, found, "expected the tls parameter in the describe response")
}

// Test_SDKRoundTrip_ResetDBClusterParameterGroup proves the real SDK
// client's ResetAllParameters actually clears a previously-set override.
// ResetDBClusterParameterGroup used to be a disguised no-op: it validated
// the group and returned an unchanged clone without ever touching the
// parameter overrides, so a real client's ResetAllParameters=true request
// silently did nothing.
func Test_SDKRoundTrip_ResetDBClusterParameterGroup(t *testing.T) {
	t.Parallel()

	backend := docdb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := docdb.NewHandler(backend)
	client := newTestDocDBClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDBClusterParameterGroup(ctx, &docdbsdk.CreateDBClusterParameterGroupInput{
		DBClusterParameterGroupName: aws.String("rt-reset-pg"),
		DBParameterGroupFamily:      aws.String("docdb4.0"),
		Description:                 aws.String("roundtrip reset test"),
	})
	require.NoError(t, err)

	_, err = client.ModifyDBClusterParameterGroup(ctx, &docdbsdk.ModifyDBClusterParameterGroupInput{
		DBClusterParameterGroupName: aws.String("rt-reset-pg"),
		Parameters: []types.Parameter{
			{ParameterName: aws.String("tls"), ParameterValue: aws.String("disabled")},
		},
	})
	require.NoError(t, err)

	_, err = client.ResetDBClusterParameterGroup(ctx, &docdbsdk.ResetDBClusterParameterGroupInput{
		DBClusterParameterGroupName: aws.String("rt-reset-pg"),
		ResetAllParameters:          aws.Bool(true),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeDBClusterParameters(ctx, &docdbsdk.DescribeDBClusterParametersInput{
		DBClusterParameterGroupName: aws.String("rt-reset-pg"),
	})
	require.NoError(t, err)

	for _, p := range descOut.Parameters {
		if aws.ToString(p.ParameterName) == "tls" {
			require.Equal(t, "enabled", aws.ToString(p.ParameterValue),
				"ResetAllParameters=true must have cleared the disabled override back to the engine default")
		}
	}
}

// Test_SDKRoundTrip_EventSubscription_FullFieldRoundTrip proves the real SDK
// client's EventCategories/Enabled round-trip end to end. xmlEventSubscription
// used to omit EventCategoriesList/EventSubscriptionArn/Enabled/CustomerAwsId/
// SubscriptionCreationTime entirely, so a real client reading the categories
// or ARN back always saw a zero value even though the backend tracked them
// correctly internally.
func Test_SDKRoundTrip_EventSubscription_FullFieldRoundTrip(t *testing.T) {
	t.Parallel()

	backend := docdb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := docdb.NewHandler(backend)
	client := newTestDocDBClient(t, h)
	ctx := t.Context()

	out, err := client.CreateEventSubscription(ctx, &docdbsdk.CreateEventSubscriptionInput{
		SubscriptionName: aws.String("rt-event-sub"),
		SnsTopicArn:      aws.String("arn:aws:sns:us-east-1:000000000000:rt-topic"),
		SourceType:       aws.String("db-cluster"),
		EventCategories:  []string{"failover", "maintenance"},
		SourceIds:        []string{"rt-source-cluster"},
		Enabled:          aws.Bool(false),
	})
	require.NoError(t, err)
	require.NotNil(t, out.EventSubscription)

	require.ElementsMatch(t, []string{"failover", "maintenance"}, out.EventSubscription.EventCategoriesList,
		"EventCategoriesList must round-trip, not silently drop to empty")
	require.ElementsMatch(t, []string{"rt-source-cluster"}, out.EventSubscription.SourceIdsList,
		"SourceIdsList must round-trip and must not be swapped with EventCategoriesList")
	require.False(t, aws.ToBool(out.EventSubscription.Enabled))
	require.NotEmpty(t, aws.ToString(out.EventSubscription.EventSubscriptionArn))
	require.Equal(t, "000000000000", aws.ToString(out.EventSubscription.CustomerAwsId))
}

// Test_SDKRoundTrip_GlobalCluster_MemberTracking proves the real SDK
// client's GlobalClusterMembers actually reflects real membership.
// GlobalCluster.GlobalClusterMembers previously had no backing field at
// all: CreateGlobalCluster never attached the source cluster, and
// DescribeGlobalClusters always answered an empty member list.
func Test_SDKRoundTrip_GlobalCluster_MemberTracking(t *testing.T) {
	t.Parallel()

	backend := docdb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := docdb.NewHandler(backend)
	client := newTestDocDBClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("rt-gc-source"),
		Engine:              aws.String("docdb"),
	})
	require.NoError(t, err)

	out, err := client.CreateGlobalCluster(ctx, &docdbsdk.CreateGlobalClusterInput{
		GlobalClusterIdentifier:   aws.String("rt-gc"),
		SourceDBClusterIdentifier: aws.String("rt-gc-source"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.GlobalCluster)
	require.Len(t, out.GlobalCluster.GlobalClusterMembers, 1)
	require.True(t, aws.ToBool(out.GlobalCluster.GlobalClusterMembers[0].IsWriter))
	require.Contains(t, aws.ToString(out.GlobalCluster.GlobalClusterMembers[0].DBClusterArn), "rt-gc-source")
}

// Test_SDKRoundTrip_DescribeEvents proves the real SDK client can decode a
// populated Events list end to end (Date/EventCategories/SourceIdentifier/
// SourceType/Message). Before this pass, DescribeEvents always answered an
// empty list -- no real event log existed at all -- so this response shape
// had never actually been exercised against the real deserializer with
// non-empty data.
func Test_SDKRoundTrip_DescribeEvents(t *testing.T) {
	t.Parallel()

	backend := docdb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := docdb.NewHandler(backend)
	client := newTestDocDBClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("rt-events-cluster"),
		Engine:              aws.String("docdb"),
	})
	require.NoError(t, err)

	out, err := client.DescribeEvents(ctx, &docdbsdk.DescribeEventsInput{
		SourceIdentifier: aws.String("rt-events-cluster"),
		SourceType:       types.SourceTypeDbCluster,
	})
	require.NoError(t, err)
	require.Len(t, out.Events, 1)
	event := out.Events[0]
	require.Equal(t, "rt-events-cluster", aws.ToString(event.SourceIdentifier))
	require.Equal(t, types.SourceTypeDbCluster, event.SourceType)
	require.NotNil(t, event.Date, "Date must decode, not be left nil by a wire-shape mismatch")
	require.NotEmpty(t, event.Message)
	require.Contains(t, event.EventCategories, "creation")
}

// Test_SDKRoundTrip_ApplyPendingMaintenanceAction proves the real SDK
// client's queued pending-maintenance-action fields round-trip. Before this
// pass there was no real queue at all, so ApplyPendingMaintenanceAction
// always answered an empty PendingMaintenanceActionDetails regardless of
// OptInType.
func Test_SDKRoundTrip_ApplyPendingMaintenanceAction(t *testing.T) {
	t.Parallel()

	backend := docdb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := docdb.NewHandler(backend)
	client := newTestDocDBClient(t, h)
	ctx := t.Context()

	const resourceARN = "arn:aws:rds:us-east-1:000000000000:cluster:rt-maint-cluster"
	backend.AddPendingMaintenanceActionInternal(resourceARN, "system-update", "roundtrip test")

	out, err := client.ApplyPendingMaintenanceAction(ctx, &docdbsdk.ApplyPendingMaintenanceActionInput{
		ResourceIdentifier: aws.String(resourceARN),
		ApplyAction:        aws.String("system-update"),
		OptInType:          aws.String("immediate"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.ResourcePendingMaintenanceActions)
	require.Equal(t, resourceARN, aws.ToString(out.ResourcePendingMaintenanceActions.ResourceIdentifier))
	require.Len(t, out.ResourcePendingMaintenanceActions.PendingMaintenanceActionDetails, 1)
	action := out.ResourcePendingMaintenanceActions.PendingMaintenanceActionDetails[0]
	require.Equal(t, "system-update", aws.ToString(action.Action))
	require.Equal(t, "immediate", aws.ToString(action.OptInStatus))
}

// Test_SDKRoundTrip_DescribeGlobalClusters_ListsClusters proves the real SDK
// client sees DescribeGlobalClusters' top-level list. The handler wrapped
// each entry in <GlobalCluster>, but the real deserializer
// (docdb@v1.51.4 deserializers.go:14551) matches <GlobalClusterMember> for
// this particular list -- unrecognized elements are skipped silently, so a
// real client always saw an empty slice regardless of what was stored.
func Test_SDKRoundTrip_DescribeGlobalClusters_ListsClusters(t *testing.T) {
	t.Parallel()

	backend := docdb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := docdb.NewHandler(backend)
	client := newTestDocDBClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("rt-gc-list-source"),
		Engine:              aws.String("docdb"),
	})
	require.NoError(t, err)

	_, err = client.CreateGlobalCluster(ctx, &docdbsdk.CreateGlobalClusterInput{
		GlobalClusterIdentifier:   aws.String("rt-gc-list"),
		SourceDBClusterIdentifier: aws.String("rt-gc-list-source"),
	})
	require.NoError(t, err)

	out, err := client.DescribeGlobalClusters(ctx, &docdbsdk.DescribeGlobalClustersInput{
		GlobalClusterIdentifier: aws.String("rt-gc-list"),
	})
	require.NoError(t, err)
	require.Len(t, out.GlobalClusters, 1)
	assert.Equal(t, "rt-gc-list", aws.ToString(out.GlobalClusters[0].GlobalClusterIdentifier))
}

// Test_SDKRoundTrip_DescribeEventCategories proves the real SDK client sees
// DescribeEventCategories' entries. The handler wrapped each entry in
// <EventCategoryMap>, but the real deserializer
// (docdb@v1.51.4 deserializers.go:13826) matches <EventCategoriesMap> --
// unrecognized elements are skipped silently, so a real client always saw
// an empty slice.
func Test_SDKRoundTrip_DescribeEventCategories(t *testing.T) {
	t.Parallel()

	backend := docdb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := docdb.NewHandler(backend)
	client := newTestDocDBClient(t, h)

	out, err := client.DescribeEventCategories(t.Context(), &docdbsdk.DescribeEventCategoriesInput{})
	require.NoError(t, err)
	require.NotEmpty(t, out.EventCategoriesMapList)
	assert.NotEmpty(t, out.EventCategoriesMapList[0].EventCategories)
}

// Test_SDKRoundTrip_CreateDBInstance_InstanceCreateTime proves the real SDK
// client's InstanceCreateTime is populated. types.DBInstance.InstanceCreateTime
// ("Provides the date and time that the instance was created") was declared
// on the real deserializer's field set (awsAwsquery_deserializeDocumentDBInstance)
// but the backend never tracked or emitted it at all -- unlike DBCluster's
// sibling ClusterCreateTime, which already did.
func Test_SDKRoundTrip_CreateDBInstance_InstanceCreateTime(t *testing.T) {
	t.Parallel()

	backend := docdb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := docdb.NewHandler(backend)
	client := newTestDocDBClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("rt-instance-create-time-cluster"),
		Engine:              aws.String("docdb"),
	})
	require.NoError(t, err)

	out, err := client.CreateDBInstance(ctx, &docdbsdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("rt-instance-create-time"),
		DBInstanceClass:      aws.String("db.t3.medium"),
		Engine:               aws.String("docdb"),
		DBClusterIdentifier:  aws.String("rt-instance-create-time-cluster"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.DBInstance)
	require.NotNil(t, out.DBInstance.InstanceCreateTime,
		"InstanceCreateTime must decode, not be left nil by a wire-shape gap")
}

// Test_SDKRoundTrip_CreateDBClusterSnapshot_DerivedFromSourceCluster proves
// the real SDK client's AvailabilityZones/KmsKeyId/MasterUsername/Port/
// ClusterCreateTime on a cluster snapshot are populated from the source
// cluster. All five are real types.DBClusterSnapshot members
// (awsAwsquery_deserializeDocumentDBClusterSnapshot) that the backend
// already tracked on the source DBCluster but never copied onto the
// snapshot record at all.
func Test_SDKRoundTrip_CreateDBClusterSnapshot_DerivedFromSourceCluster(t *testing.T) {
	t.Parallel()

	backend := docdb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := docdb.NewHandler(backend)
	client := newTestDocDBClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("rt-snap-source"),
		Engine:              aws.String("docdb"),
		MasterUsername:      aws.String("snapadmin"),
		Port:                aws.Int32(27018),
		AvailabilityZones:   []string{"us-east-1a", "us-east-1c"},
	})
	require.NoError(t, err)

	out, err := client.CreateDBClusterSnapshot(ctx, &docdbsdk.CreateDBClusterSnapshotInput{
		DBClusterSnapshotIdentifier: aws.String("rt-snap-derived"),
		DBClusterIdentifier:         aws.String("rt-snap-source"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.DBClusterSnapshot)

	snap := out.DBClusterSnapshot
	require.ElementsMatch(t, []string{"us-east-1a", "us-east-1c"}, snap.AvailabilityZones)
	assert.Equal(t, "snapadmin", aws.ToString(snap.MasterUsername))
	assert.Equal(t, int32(27018), aws.ToInt32(snap.Port))
	require.NotNil(t, snap.ClusterCreateTime, "ClusterCreateTime must decode, echoing the source cluster's own")
}

// Test_SDKRoundTrip_CopyDBClusterSnapshot_TagsAndSourceArn proves the real
// SDK client's CopyTags and SourceDBClusterSnapshotArn actually apply.
// CopyDBClusterSnapshotInput.CopyTags/Tags were parsed by neither the
// handler nor the backend at all -- a real client's "copy the source's
// tags to the target" request was a silent no-op -- and
// types.DBClusterSnapshot.SourceDBClusterSnapshotArn (a real response
// member) was never populated on a copy.
func Test_SDKRoundTrip_CopyDBClusterSnapshot_TagsAndSourceArn(t *testing.T) {
	t.Parallel()

	backend := docdb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := docdb.NewHandler(backend)
	client := newTestDocDBClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("rt-copy-source-cluster"),
		Engine:              aws.String("docdb"),
	})
	require.NoError(t, err)

	_, err = client.CreateDBClusterSnapshot(ctx, &docdbsdk.CreateDBClusterSnapshotInput{
		DBClusterSnapshotIdentifier: aws.String("rt-copy-source-snap"),
		DBClusterIdentifier:         aws.String("rt-copy-source-cluster"),
		Tags: []types.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
		},
	})
	require.NoError(t, err)

	out, err := client.CopyDBClusterSnapshot(ctx, &docdbsdk.CopyDBClusterSnapshotInput{
		SourceDBClusterSnapshotIdentifier: aws.String("rt-copy-source-snap"),
		TargetDBClusterSnapshotIdentifier: aws.String("rt-copy-target-snap"),
		CopyTags:                          aws.Bool(true),
	})
	require.NoError(t, err)
	require.NotNil(t, out.DBClusterSnapshot)
	assert.Contains(t, aws.ToString(out.DBClusterSnapshot.SourceDBClusterSnapshotArn), "rt-copy-source-snap")

	tagsOut, err := client.ListTagsForResource(ctx, &docdbsdk.ListTagsForResourceInput{
		ResourceName: out.DBClusterSnapshot.DBClusterSnapshotArn,
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.TagList, 1, "CopyTags=true must have copied the source snapshot's tags")
	assert.Equal(t, "env", aws.ToString(tagsOut.TagList[0].Key))
	assert.Equal(t, "prod", aws.ToString(tagsOut.TagList[0].Value))
}

// Test_SDKRoundTrip_RestoreDBClusterFromSnapshot proves the real SDK client's
// RestoreDBClusterFromSnapshotInput.SnapshotIdentifier reaches the backend.
// The real serializer (awsAwsquery_serializeOpDocumentRestoreDBClusterFromSnapshotInput,
// docdb@v1.51.4 serializers.go:5845) encodes the field as "SnapshotIdentifier";
// the handler used to read "DBClusterSnapshotIdentifier" instead (that key is
// valid for CreateDBClusterSnapshot/DescribeDBClusterSnapshots, not this op),
// so every real client's snapshot ID was silently dropped and the restore
// always failed with DBClusterSnapshotNotFoundFault.
func Test_SDKRoundTrip_RestoreDBClusterFromSnapshot(t *testing.T) {
	t.Parallel()

	backend := docdb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := docdb.NewHandler(backend)
	client := newTestDocDBClient(t, h)
	ctx := t.Context()

	_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String("rt-restore-source"),
		Engine:              aws.String("docdb"),
	})
	require.NoError(t, err)

	_, err = client.CreateDBClusterSnapshot(ctx, &docdbsdk.CreateDBClusterSnapshotInput{
		DBClusterSnapshotIdentifier: aws.String("rt-restore-snap"),
		DBClusterIdentifier:         aws.String("rt-restore-source"),
	})
	require.NoError(t, err)

	out, err := client.RestoreDBClusterFromSnapshot(ctx, &docdbsdk.RestoreDBClusterFromSnapshotInput{
		DBClusterIdentifier: aws.String("rt-restored"),
		SnapshotIdentifier:  aws.String("rt-restore-snap"),
		Engine:              aws.String("docdb"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.DBCluster)
	assert.Equal(t, "rt-restored", aws.ToString(out.DBCluster.DBClusterIdentifier))
}
