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
