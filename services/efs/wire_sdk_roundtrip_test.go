package efs_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	efssdk "github.com/aws/aws-sdk-go-v2/service/efs"
	efssdktypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

const (
	testAccountID = "123456789012"
	testRegion    = "us-east-1"
)

// newWireTestClient stands up a fresh in-memory efs backend and a real
// aws-sdk-go-v2 client against an httptest server running its Handler.
// Round-tripping through the genuine SDK serializer/deserializer proves
// wire-compatibility that a direct handler call or raw-body assertion alone
// would miss.
func newWireTestClient(t *testing.T) (*efssdk.Client, *efs.Handler) {
	t.Helper()

	bk := efs.NewInMemoryBackend(testAccountID, testRegion)
	h := efs.NewHandler(bk)

	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	client := efssdk.NewFromConfig(cfg, func(o *efssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	return client, h
}

// TestReplicationDestination_NoFabricatedFields proves that a replication
// destination entry never carries fileSystemArn, availabilityZoneName, or
// kmsKeyId on the wire. aws-sdk-go-v2/service/efs@v1.44.4's
// awsRestjson1_deserializeDocumentDestination (deserializers.go) declares
// exactly seven cases -- FileSystemId, LastReplicatedTimestamp, OwnerId,
// Region, RoleArn, Status, StatusMessage -- and types.Destination
// (types/types.go) has no Arn/AvailabilityZoneName/KmsKeyId field at all,
// unlike the request-side sibling type DestinationToCreate, which does carry
// AvailabilityZoneName/KmsKeyId as INPUT-only fields. Any client-observable
// value under those three keys is silently dropped by the real deserializer,
// so this is a raw-body absence assertion, not an SDK round-trip -- the typed
// client has no field to bind them to.
//
// RoleArn round-trips through the typed client separately below, proving the
// real field the fix adds actually reaches the wire.
func TestReplicationDestination_NoFabricatedFields(t *testing.T) {
	t.Parallel()

	client, h := newWireTestClient(t)

	fsOut, err := client.CreateFileSystem(t.Context(), &efssdk.CreateFileSystemInput{
		CreationToken: aws.String("rc-wire-token"),
	})
	require.NoError(t, err)

	rec := doREST(t, h, http.MethodPost,
		"/2015-02-01/file-systems/"+aws.ToString(fsOut.FileSystemId)+"/replication-configuration",
		map[string]any{
			"Destinations": []map[string]any{
				{"Region": "eu-west-1", "AvailabilityZoneName": "eu-west-1a", "KmsKeyId": "alias/test"},
			},
		})
	require.Equal(t, http.StatusOK, rec.Code, "CreateReplicationConfiguration failed: %s", rec.Body.String())

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	dests, ok := raw["Destinations"].([]any)
	require.True(t, ok)
	require.Len(t, dests, 1)

	dest, ok := dests[0].(map[string]any)
	require.True(t, ok)

	for _, fabricated := range []string{"FileSystemArn", "AvailabilityZoneName", "KmsKeyID", "KmsKeyId"} {
		_, present := dest[fabricated]
		assert.False(t, present, "%s must not appear on a Destination entry", fabricated)
	}
}

// TestReplicationDestination_RoleArnRoundTrips proves RoleArn -- a real field
// on both the request-side DestinationToCreate and the response-side
// Destination (types/types.go, deserializers.go
// awsRestjson1_deserializeDocumentDestination case "RoleArn") -- now actually
// reaches the wire through the typed SDK client. This field was missing
// entirely from gopherstack's ReplicationDestination struct before this pass.
func TestReplicationDestination_RoleArnRoundTrips(t *testing.T) {
	t.Parallel()

	client, _ := newWireTestClient(t)

	fsOut, err := client.CreateFileSystem(t.Context(), &efssdk.CreateFileSystemInput{
		CreationToken: aws.String("rc-rolearn-token"),
	})
	require.NoError(t, err)

	roleARN := "arn:aws:iam::123456789012:role/efs-replication"
	rcOut, err := client.CreateReplicationConfiguration(t.Context(), &efssdk.CreateReplicationConfigurationInput{
		SourceFileSystemId: fsOut.FileSystemId,
		Destinations: []efssdktypes.DestinationToCreate{
			{Region: aws.String(testRegion), RoleArn: aws.String(roleARN)},
		},
	})
	require.NoError(t, err)
	require.Len(t, rcOut.Destinations, 1)
	assert.Equal(t, roleARN, aws.ToString(rcOut.Destinations[0].RoleArn))

	descOut, err := client.DescribeReplicationConfigurations(
		t.Context(), &efssdk.DescribeReplicationConfigurationsInput{
			FileSystemId: fsOut.FileSystemId,
		},
	)
	require.NoError(t, err)
	require.Len(t, descOut.Replications, 1)
	require.Len(t, descOut.Replications[0].Destinations, 1)
	assert.Equal(t, roleARN, aws.ToString(descOut.Replications[0].Destinations[0].RoleArn))
}

// TestMountTargetDescription_NoFabricatedSecurityGroups proves that
// CreateMountTarget's and DescribeMountTargets' response bodies never carry a
// securityGroups member. aws-sdk-go-v2/service/efs@v1.44.4's
// awsRestjson1_deserializeDocumentMountTargetDescription (deserializers.go)
// declares no "SecurityGroups" case, and types.MountTargetDescription
// (types/types.go) has no such field -- security groups are exposed only via
// the separate DescribeMountTargetSecurityGroups operation, whose bare-list
// output is a different shape entirely. The typed client has no field to
// observe this on, so this is a raw-body absence assertion.
func TestMountTargetDescription_NoFabricatedSecurityGroups(t *testing.T) {
	t.Parallel()

	h := newTestEFSHandler()
	fsID := createFS(t, h, "mt-wire-sg-token")

	rec := doREST(t, h, http.MethodPost, "/2015-02-01/mount-targets", map[string]any{
		"FileSystemId":   fsID,
		"SubnetId":       "subnet-wire-sg",
		"SecurityGroups": []string{"sg-1", "sg-2"},
	})
	require.Equal(t, http.StatusOK, rec.Code, "CreateMountTarget failed: %s", rec.Body.String())

	created := parseResp(t, rec)
	_, present := created["SecurityGroups"]
	assert.False(t, present, "SecurityGroups must not appear on a CreateMountTarget response")

	mtID, ok := created["MountTargetId"].(string)
	require.True(t, ok)

	descRec := doREST(t, h, http.MethodGet, "/2015-02-01/mount-targets?MountTargetId="+mtID, nil)
	require.Equal(t, http.StatusOK, descRec.Code)

	descResp := parseResp(t, descRec)
	mts, ok := descResp["MountTargets"].([]any)
	require.True(t, ok)
	require.Len(t, mts, 1)

	mtEntry, ok := mts[0].(map[string]any)
	require.True(t, ok)
	_, present = mtEntry["SecurityGroups"]
	assert.False(t, present, "SecurityGroups must not appear on a DescribeMountTargets response entry")

	sgRec := doREST(t, h, http.MethodGet, "/2015-02-01/mount-targets/"+mtID+"/security-groups", nil)
	require.Equal(t, http.StatusOK, sgRec.Code)

	sgResp := parseResp(t, sgRec)
	sgs, ok := sgResp["SecurityGroups"].([]any)
	require.True(t, ok, "DescribeMountTargetSecurityGroups must still carry the bare SecurityGroups list")
	assert.ElementsMatch(t, []any{"sg-1", "sg-2"}, sgs)
}
