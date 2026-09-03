package ram_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ramsdk "github.com/aws/aws-sdk-go-v2/service/ram"
	ramtypes "github.com/aws/aws-sdk-go-v2/service/ram/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ram"
)

// newTestRAMClient stands up the real aws-sdk-go-v2 ram client against an httptest
// server running this package's Handler, wired through the same pkgs/service
// registry/router used in production -- round-tripping through the genuine SDK
// serializer/deserializer, not ad-hoc JSON structs.
func newTestRAMClient(t *testing.T, h *ram.Handler) *ramsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return ramsdk.NewFromConfig(cfg, func(o *ramsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// Test_SDKRoundTrip_CreatePermissionVersion_ReturnsPolicyDocument proves
// CreatePermissionVersionOutput.Permission decodes as a ResourceSharePermissionDetail
// (api_op_CreatePermissionVersion.go:100), which carries the "permission" policy-document
// field via deserializers.go:916's awsRestjson1_deserializeDocumentResourceSharePermissionDetail.
// Before the fix, gopherstack built the response from the narrower Summary shape, which has
// no "permission" case at all, so the SDK client's Permission.Permission always decoded nil.
func Test_SDKRoundTrip_CreatePermissionVersion_ReturnsPolicyDocument(t *testing.T) {
	t.Parallel()

	backend := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(backend)
	client := newTestRAMClient(t, h)

	created, err := backend.CreatePermission("shape-test-perm", "ec2:Subnet", `{"v":"1"}`, nil)
	require.NoError(t, err)

	const policy = `{"Effect":"Allow","Action":["ec2:DescribeSubnets"]}`

	out, err := client.CreatePermissionVersion(t.Context(), &ramsdk.CreatePermissionVersionInput{
		PermissionArn:  aws.String(created.ARN),
		PolicyTemplate: aws.String(policy),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Permission)
	require.NotNil(t, out.Permission.Permission)
	assert.JSONEq(t, policy, *out.Permission.Permission)
	require.NotNil(t, out.Permission.Version)
	assert.Equal(t, "2", *out.Permission.Version)
}

// Test_ListPermissionVersions_OmitsPolicyDocumentField proves gopherstack no longer leaks
// the policy-document text under a "permission" key in ListPermissionVersions items.
// ListPermissionVersionsOutput.Permissions is []types.ResourceSharePermissionSummary
// (api_op_ListPermissionVersions.go:75), whose deserializer (deserializers.go:3821's
// awsRestjson1_deserializeDocumentResourceSharePermissionSummary) has no "permission" case,
// so a typed SDK client can never observe this field either way -- a raw-body absence
// assertion is the correct instrument here, not a typed round trip.
func Test_ListPermissionVersions_OmitsPolicyDocumentField(t *testing.T) {
	t.Parallel()

	backend := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(backend)

	created, err := backend.CreatePermission("shape-list-perm", "ec2:Subnet", `{"v":"1"}`, nil)
	require.NoError(t, err)
	_, err = backend.CreatePermissionVersion(created.ARN, `{"v":"2-secret-policy-text"}`)
	require.NoError(t, err)

	rec := doRAMRequest(t, h, "/listpermissionversions", map[string]any{
		"permissionArn": created.ARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw struct {
		Permissions []map[string]any `json:"permissions"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	require.Len(t, raw.Permissions, 2)

	for _, item := range raw.Permissions {
		_, leaked := item["permission"]
		assert.Falsef(t, leaked, "ListPermissionVersions item leaked policy document: %+v", item)
	}
}

// Test_SDKRoundTrip_DeletePermissionVersion_PermissionStatus proves
// DeletePermissionVersionOutput.PermissionStatus decodes as a real
// types.PermissionStatus member. Real PermissionStatus only defines
// ATTACHABLE/UNATTACHABLE/DELETING/DELETED (ram@v1.39.4 types/enums.go:26);
// pre-fix, gopherstack emitted "UPDATING", not a member of that enum, for an
// operation that has nothing to do with updating -- deleting a permission
// version is an asynchronous delete, so DELETING is the correct in-progress
// status.
func Test_SDKRoundTrip_DeletePermissionVersion_PermissionStatus(t *testing.T) {
	t.Parallel()

	backend := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(backend)
	client := newTestRAMClient(t, h)

	created, err := backend.CreatePermission("delpv-shape-perm", "ec2:Subnet", `{"v":"1"}`, nil)
	require.NoError(t, err)
	_, err = backend.CreatePermissionVersion(created.ARN, `{"v":"2"}`)
	require.NoError(t, err)

	out, err := client.DeletePermissionVersion(t.Context(), &ramsdk.DeletePermissionVersionInput{
		PermissionArn:     aws.String(created.ARN),
		PermissionVersion: aws.Int32(2),
	})
	require.NoError(t, err)
	assert.Equal(t, ramtypes.PermissionStatusDeleting, out.PermissionStatus)
}
