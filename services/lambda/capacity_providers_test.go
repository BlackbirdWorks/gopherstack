package lambda_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/lambda"
)

const capacityProviderTestRegion = "us-east-1"

// newTestLambdaClient stands up the real aws-sdk-go-v2 Lambda client against
// an httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production. Round-tripping through
// the genuine SDK serializer/deserializer is what actually proves a request
// or response is wire-compatible -- a handler reading the wrong JSON field
// name (e.g. "Name" instead of "CapacityProviderName") passes a raw-JSON
// test that hand-builds the request body but fails here, because the real
// client only ever serializes "CapacityProviderName".
func newTestLambdaClient(t *testing.T, h *lambda.Handler) *lambdasdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(capacityProviderTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return lambdasdk.NewFromConfig(cfg, func(o *lambdasdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.HTTPClient = sdktest.PlainBodyHTTPClient()
	})
}

// newCapacityProviderTestBackend creates an InMemoryBackend suitable for unit
// testing capacity-provider function-version assignments. It uses nil allocators
// so no real HTTP servers are started, and closes the backend on cleanup.
func newCapacityProviderTestBackend(t *testing.T) *lambda.InMemoryBackend {
	t.Helper()

	bk := lambda.NewInMemoryBackend(
		nil,
		nil,
		lambda.DefaultSettings(),
		"000000000000",
		"us-east-1",
	)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		bk.Close(ctx)
	})

	return bk
}

// createCPTestProvider creates a capacity provider and returns its ARN.
func createCPTestProvider(t *testing.T, bk *lambda.InMemoryBackend, name string) string {
	t.Helper()

	cp, err := bk.CreateCapacityProvider(&lambda.CreateCapacityProviderInput{
		CapacityProviderName: name,
		PermissionsConfig: &lambda.CapacityProviderPermissionsConfig{
			CapacityProviderOperatorRoleArn: "arn:aws:iam::000000000000:role/cp-role",
		},
		VpcConfig: &lambda.CapacityProviderVpcConfig{
			SubnetIDs:        []string{"subnet-1"},
			SecurityGroupIDs: []string{"sg-1"},
		},
	})
	require.NoError(t, err)

	return cp.CapacityProviderArn
}

// createCPTestFunction creates a function bound to cpARN and publishes one version.
func createCPTestFunction(t *testing.T, h *lambda.Handler, fnName, cpARN string) {
	t.Helper()

	body := fmt.Sprintf(`{"FunctionName":%q,"PackageType":"Image","Code":{"ImageUri":"x"},`+
		`"Role":"arn:aws:iam::000000000000:role/r","CapacityProviderConfig":`+
		`{"LambdaManagedInstancesCapacityProviderConfig":{"CapacityProviderArn":%q}}}`, fnName, cpARN)
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions", body)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	rec = callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions/"+fnName+"/versions", `{}`)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
}

// TestListFunctionVersionsByCapacityProvider_Derived derives the listing from functions
// whose CapacityProviderConfig references the provider.
func TestListFunctionVersionsByCapacityProvider_Derived(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantFirst []string
		functions []string
		maxItems  int
		wantNext  bool
	}{
		{name: "none", functions: nil, wantFirst: nil},
		{
			name:      "latest_and_published",
			functions: []string{"fn"},
			wantFirst: []string{
				"arn:aws:lambda:us-east-1:000000000000:function:fn:$LATEST",
				"arn:aws:lambda:us-east-1:000000000000:function:fn:1",
			},
		},
		{
			name:      "paginated",
			functions: []string{"fn"},
			maxItems:  1,
			wantFirst: []string{"arn:aws:lambda:us-east-1:000000000000:function:fn:$LATEST"},
			wantNext:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newInMemoryHandler(t)
			cpARN := createCPTestProvider(t, bk, "cp")

			for _, fn := range tt.functions {
				createCPTestFunction(t, h, fn, cpARN)
			}

			p, gotARN, err := bk.ListFunctionVersionsByCapacityProvider("cp", "", tt.maxItems)
			require.NoError(t, err)
			assert.Equal(t, cpARN, gotARN)
			assert.Equal(t, tt.wantNext, p.Next != "")

			got := make([]string, 0, len(p.Data))
			for _, v := range p.Data {
				got = append(got, v.FunctionArn)
			}

			assert.ElementsMatch(t, tt.wantFirst, append([]string(nil), got...))
		})
	}
}

// TestListFunctionVersionsByCapacityProvider_NotFound verifies a missing
// provider yields ErrFunctionNotFound (ResourceNotFoundException on the wire).
func TestListFunctionVersionsByCapacityProvider_NotFound(t *testing.T) {
	t.Parallel()

	bk := newCapacityProviderTestBackend(t)

	_, _, err := bk.ListFunctionVersionsByCapacityProvider("missing", "", 0)
	require.ErrorIs(t, err, lambda.ErrFunctionNotFound)
}

// TestCapacityProvider_FunctionReferenceLifecycle covers function-level
// validation against provider existence and delete-while-in-use.
func TestCapacityProvider_FunctionReferenceLifecycle(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)

	const missingARN = "arn:aws:lambda:us-east-1:000000000000:capacity-provider:nope"

	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/functions",
		`{"FunctionName":"bad","PackageType":"Image","Code":{"ImageUri":"x"},`+
			`"Role":"arn:aws:iam::000000000000:role/r","CapacityProviderConfig":`+
			`{"LambdaManagedInstancesCapacityProviderConfig":{"CapacityProviderArn":"`+missingARN+`"}}}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	cpARN := createCPTestProvider(t, bk, "used-cp")
	createCPTestFunction(t, h, "user-fn", cpARN)

	rec = callInMemoryHandler(t, h, http.MethodDelete, "/2025-11-30/capacity-providers/used-cp", "")
	require.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "ResourceConflictException")

	rec = callInMemoryHandler(t, h, http.MethodDelete, "/2015-03-31/functions/user-fn", "")
	require.Less(t, rec.Code, 300)

	rec = callInMemoryHandler(t, h, http.MethodDelete, "/2025-11-30/capacity-providers/used-cp", "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "Deleting")
}

// TestCapacityProvider_StateTransitionAndTags covers Pending to Active and tag round trips.
func TestCapacityProvider_StateTransitionAndTags(t *testing.T) {
	t.Parallel()

	bk := newCapacityProviderTestBackend(t)
	h := lambda.NewHandler(bk)

	rec := callInMemoryHandler(t, h, http.MethodPost, "/2025-11-30/capacity-providers",
		`{"CapacityProviderName":"tagged","PermissionsConfig":{"CapacityProviderOperatorRoleArn":"r"},`+
			`"VpcConfig":{"SubnetIds":["s"],"SecurityGroupIds":["g"]},"Tags":{"k":"v"}}`)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"State":"Pending"`)

	var created struct {
		CapacityProvider struct {
			CapacityProviderArn string `json:"CapacityProviderArn"`
		} `json:"CapacityProvider"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))

	rec = callInMemoryHandler(t, h, http.MethodGet, "/2025-11-30/capacity-providers/tagged", "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"State":"Active"`)

	tagsPath := "/2017-03-31/tags/" + created.CapacityProvider.CapacityProviderArn
	rec = callInMemoryHandler(t, h, http.MethodGet, tagsPath, "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"k":"v"`)
}

// TestCapacityProvider_SnapshotRestore verifies providers survive Snapshot/Restore.
func TestCapacityProvider_SnapshotRestore(t *testing.T) {
	t.Parallel()

	bk := newCapacityProviderTestBackend(t)
	cpARN := createCPTestProvider(t, bk, "persisted")

	snap := bk.Snapshot(t.Context())
	require.NotNil(t, snap)

	restored := newCapacityProviderTestBackend(t)
	require.NoError(t, restored.Restore(t.Context(), snap))

	got, err := restored.GetCapacityProvider("persisted")
	require.NoError(t, err)
	assert.Equal(t, cpARN, got.CapacityProviderArn)
	assert.Equal(t, "persisted", got.Name)
}

// --- CapacityProvider tests ---

// Test_SDKRoundTrip_CreateCapacityProvider proves that CapacityProviderName,
// PermissionsConfig and VpcConfig -- all required members on the real wire
// (api_op_CreateCapacityProvider.go:28-45) -- are actually read by the
// handler and echoed back through Get/List, along with the optional
// CapacityProviderScalingConfig/InstanceRequirements/KmsKeyArn/PropagateTags.
// Before the fix, the handler read a nonexistent top-level "Name" field, so
// every real client request 400'd with "Name is required" and
// PermissionsConfig/VpcConfig were dropped entirely.
func Test_SDKRoundTrip_CreateCapacityProvider(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", capacityProviderTestRegion)
	h := lambda.NewHandler(backend)
	client := newTestLambdaClient(t, h)

	created, err := client.CreateCapacityProvider(t.Context(), &lambdasdk.CreateCapacityProviderInput{
		CapacityProviderName: aws.String("sdk-cp"),
		PermissionsConfig: &types.CapacityProviderPermissionsConfig{
			CapacityProviderOperatorRoleArn: aws.String("arn:aws:iam::000000000000:role/cp-role"),
		},
		VpcConfig: &types.CapacityProviderVpcConfig{
			SubnetIds:        []string{"subnet-1", "subnet-2"},
			SecurityGroupIds: []string{"sg-1"},
		},
		CapacityProviderScalingConfig: &types.CapacityProviderScalingConfig{
			MaxVCpuCount: aws.Int32(64),
		},
		InstanceRequirements: &types.InstanceRequirements{
			AllowedInstanceTypes: []string{"m5.large"},
		},
		KmsKeyArn: aws.String("arn:aws:kms:us-east-1:000000000000:key/test-key"),
	})
	require.NoError(t, err)
	require.NotNil(t, created.CapacityProvider)

	cp := created.CapacityProvider
	assert.Contains(t, *cp.CapacityProviderArn, "sdk-cp")
	require.NotNil(t, cp.PermissionsConfig)
	assert.Equal(t, "arn:aws:iam::000000000000:role/cp-role", *cp.PermissionsConfig.CapacityProviderOperatorRoleArn)
	require.NotNil(t, cp.VpcConfig)
	assert.Equal(t, []string{"subnet-1", "subnet-2"}, cp.VpcConfig.SubnetIds)
	assert.Equal(t, []string{"sg-1"}, cp.VpcConfig.SecurityGroupIds)
	require.NotNil(t, cp.CapacityProviderScalingConfig)
	assert.Equal(t, int32(64), *cp.CapacityProviderScalingConfig.MaxVCpuCount)
	require.NotNil(t, cp.InstanceRequirements)
	assert.Equal(t, []string{"m5.large"}, cp.InstanceRequirements.AllowedInstanceTypes)
	require.NotNil(t, cp.KmsKeyArn)
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/test-key", *cp.KmsKeyArn)
	assert.Equal(t, types.CapacityProviderStatePending, cp.State)

	got, err := client.GetCapacityProvider(t.Context(), &lambdasdk.GetCapacityProviderInput{
		CapacityProviderName: aws.String("sdk-cp"),
	})
	require.NoError(t, err)
	require.NotNil(t, got.CapacityProvider.PermissionsConfig)
	assert.Equal(t, "arn:aws:iam::000000000000:role/cp-role",
		*got.CapacityProvider.PermissionsConfig.CapacityProviderOperatorRoleArn)
	require.NotNil(t, got.CapacityProvider.VpcConfig)
	assert.Equal(t, []string{"subnet-1", "subnet-2"}, got.CapacityProvider.VpcConfig.SubnetIds)

	listed, err := client.ListCapacityProviders(t.Context(), &lambdasdk.ListCapacityProvidersInput{})
	require.NoError(t, err)
	require.Len(t, listed.CapacityProviders, 1)
	require.NotNil(t, listed.CapacityProviders[0].VpcConfig)
	assert.Equal(t, []string{"sg-1"}, listed.CapacityProviders[0].VpcConfig.SecurityGroupIds)
}

// Test_SDKRoundTrip_UpdateCapacityProvider proves CapacityProviderScalingConfig
// and PropagateTags round-trip through UpdateCapacityProvider, which the
// handler previously dropped in favor of a fabricated TargetOnDemandConcurrency
// field that does not exist anywhere on the real wire.
func Test_SDKRoundTrip_UpdateCapacityProvider(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", capacityProviderTestRegion)
	h := lambda.NewHandler(backend)
	client := newTestLambdaClient(t, h)

	_, err := client.CreateCapacityProvider(t.Context(), &lambdasdk.CreateCapacityProviderInput{
		CapacityProviderName: aws.String("update-cp"),
		PermissionsConfig: &types.CapacityProviderPermissionsConfig{
			CapacityProviderOperatorRoleArn: aws.String("arn:aws:iam::000000000000:role/cp-role"),
		},
		VpcConfig: &types.CapacityProviderVpcConfig{
			SubnetIds:        []string{"subnet-1"},
			SecurityGroupIds: []string{"sg-1"},
		},
	})
	require.NoError(t, err)

	updated, err := client.UpdateCapacityProvider(t.Context(), &lambdasdk.UpdateCapacityProviderInput{
		CapacityProviderName: aws.String("update-cp"),
		CapacityProviderScalingConfig: &types.CapacityProviderScalingConfig{
			MaxVCpuCount: aws.Int32(128),
		},
		PropagateTags: &types.PropagateTags{
			Mode: types.PropagateTagsModeExplicit,
			ExplicitTags: map[string]string{
				"team": "platform",
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.CapacityProvider.CapacityProviderScalingConfig)
	assert.Equal(t, int32(128), *updated.CapacityProvider.CapacityProviderScalingConfig.MaxVCpuCount)
	require.NotNil(t, updated.CapacityProvider.PropagateTags)
	assert.Equal(t, types.PropagateTagsModeExplicit, updated.CapacityProvider.PropagateTags.Mode)
	assert.Equal(t, "platform", updated.CapacityProvider.PropagateTags.ExplicitTags["team"])
}

// Test_SDKRoundTrip_DeleteCapacityProvider proves DeleteCapacityProvider
// echoes the deleted provider's state back on the wire.
// DeleteCapacityProviderOutput.CapacityProvider is required
// (api_op_DeleteCapacityProvider.go:44-46), and real AWS returns HTTP 200
// with that body -- not the empty 204 a delete op returns by default. The
// real SDK's deserializer treats an empty 204 body as valid JSON-decode-EOF
// and leaves CapacityProvider nil rather than erroring, so a handler that
// forgets this required member produces a "successful" client call carrying
// a zero value where AWS guarantees content.
func Test_SDKRoundTrip_DeleteCapacityProvider(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", capacityProviderTestRegion)
	h := lambda.NewHandler(backend)
	client := newTestLambdaClient(t, h)

	_, err := client.CreateCapacityProvider(t.Context(), &lambdasdk.CreateCapacityProviderInput{
		CapacityProviderName: aws.String("delete-cp"),
		PermissionsConfig: &types.CapacityProviderPermissionsConfig{
			CapacityProviderOperatorRoleArn: aws.String("arn:aws:iam::000000000000:role/cp-role"),
		},
		VpcConfig: &types.CapacityProviderVpcConfig{
			SubnetIds:        []string{"subnet-1"},
			SecurityGroupIds: []string{"sg-1"},
		},
	})
	require.NoError(t, err)

	deleted, err := client.DeleteCapacityProvider(t.Context(), &lambdasdk.DeleteCapacityProviderInput{
		CapacityProviderName: aws.String("delete-cp"),
	})
	require.NoError(t, err)
	require.NotNil(t, deleted.CapacityProvider)
	require.NotNil(t, deleted.CapacityProvider.CapacityProviderArn)
	assert.Contains(t, *deleted.CapacityProvider.CapacityProviderArn, "delete-cp")
	assert.Equal(t, types.CapacityProviderStateDeleting, deleted.CapacityProvider.State)

	_, err = client.GetCapacityProvider(t.Context(), &lambdasdk.GetCapacityProviderInput{
		CapacityProviderName: aws.String("delete-cp"),
	})
	require.Error(t, err)
}

// Test_SDKRoundTrip_ListFunctionVersionsByCapacityProvider proves the real
// wire shape of ListFunctionVersionsByCapacityProvider: FunctionVersions is a
// list of {FunctionArn, State} objects (api_op_ListFunctionVersionsByCapacityProvider.go,
// types.FunctionVersionsByCapacityProviderListItem), not bare ARN strings,
// and the response also carries a top-level CapacityProviderArn. Before the
// fix, the handler emitted a flat []string under FunctionVersions and never
// set CapacityProviderArn at all -- the real SDK's deserializer would fail to
// decode each string element as the required object shape.
func Test_SDKRoundTrip_ListFunctionVersionsByCapacityProvider(t *testing.T) {
	t.Parallel()

	backend := lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "000000000000", capacityProviderTestRegion)
	h := lambda.NewHandler(backend)
	client := newTestLambdaClient(t, h)

	created, err := client.CreateCapacityProvider(t.Context(), &lambdasdk.CreateCapacityProviderInput{
		CapacityProviderName: aws.String("list-versions-cp"),
		PermissionsConfig: &types.CapacityProviderPermissionsConfig{
			CapacityProviderOperatorRoleArn: aws.String("arn:aws:iam::000000000000:role/cp-role"),
		},
		VpcConfig: &types.CapacityProviderVpcConfig{
			SubnetIds:        []string{"subnet-1"},
			SecurityGroupIds: []string{"sg-1"},
		},
	})
	require.NoError(t, err)

	createCPTestFunction(t, h, "fn", *created.CapacityProvider.CapacityProviderArn)

	const versionArn = "arn:aws:lambda:us-east-1:000000000000:function:fn:1"

	listed, err := client.ListFunctionVersionsByCapacityProvider(
		t.Context(),
		&lambdasdk.ListFunctionVersionsByCapacityProviderInput{
			CapacityProviderName: aws.String("list-versions-cp"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, *created.CapacityProvider.CapacityProviderArn, *listed.CapacityProviderArn)
	require.Len(t, listed.FunctionVersions, 2)
	assert.Equal(t, versionArn, *listed.FunctionVersions[1].FunctionArn)
	assert.Equal(t, types.StateActive, listed.FunctionVersions[1].State)
}

// TestCapacityProvider_MissingRequiredFields verifies that
// CreateCapacityProvider rejects requests missing any of the three
// wire-required fields (CapacityProviderName, PermissionsConfig, VpcConfig).
// This is exercised at the raw-JSON layer because the real SDK's own
// client-side validation middleware refuses to even send a request that
// omits a required member, so a real client cannot reach the server in this
// state.
func TestCapacityProvider_MissingRequiredFields(t *testing.T) {
	t.Parallel()

	validPermissions := `"PermissionsConfig":{"CapacityProviderOperatorRoleArn":"arn:aws:iam::000000000000:role/r"}`
	validVpc := `"VpcConfig":{"SubnetIds":["subnet-1"],"SecurityGroupIds":["sg-1"]}`

	tests := []struct {
		name       string
		createBody string
		wantStatus int
	}{
		{
			name:       "missing_capacity_provider_name",
			createBody: `{` + validPermissions + `,` + validVpc + `}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_permissions_config",
			createBody: `{"CapacityProviderName":"cp",` + validVpc + `}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_vpc_config",
			createBody: `{"CapacityProviderName":"cp",` + validPermissions + `}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "all_required_fields_present",
			createBody: `{"CapacityProviderName":"cp",` + validPermissions + `,` + validVpc + `}`,
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)

			rec := callInMemoryHandler(t, h, http.MethodPost, "/2025-11-30/capacity-providers", tt.createBody)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestCapacityProvider_GetDeleteUpdateList(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	body := `{"CapacityProviderName":"test-cp",` +
		`"PermissionsConfig":{"CapacityProviderOperatorRoleArn":"arn:aws:iam::000000000000:role/r"},` +
		`"VpcConfig":{"SubnetIds":["subnet-1"],"SecurityGroupIds":["sg-1"]}}`

	// Create
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2025-11-30/capacity-providers", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Create duplicate → conflict
	dupRec := callInMemoryHandler(t, h, http.MethodPost, "/2025-11-30/capacity-providers", body)
	assert.Equal(t, http.StatusConflict, dupRec.Code)

	// Get
	getRec := callInMemoryHandler(t, h, http.MethodGet, "/2025-11-30/capacity-providers/test-cp", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	// Get not found
	getNotFound := callInMemoryHandler(t, h, http.MethodGet, "/2025-11-30/capacity-providers/nonexistent", "")
	assert.Equal(t, http.StatusNotFound, getNotFound.Code)

	// Update
	updateRec := callInMemoryHandler(t, h, http.MethodPut, "/2025-11-30/capacity-providers/test-cp",
		`{"CapacityProviderScalingConfig":{"MaxVCpuCount":200}}`)
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateOut lambda.UpdateCapacityProviderOutput
	require.NoError(t, json.NewDecoder(updateRec.Body).Decode(&updateOut))
	require.NotNil(t, updateOut.CapacityProvider)
	require.NotNil(t, updateOut.CapacityProvider.CapacityProviderScalingConfig)
	require.NotNil(t, updateOut.CapacityProvider.CapacityProviderScalingConfig.MaxVCpuCount)
	assert.Equal(t, int32(200), *updateOut.CapacityProvider.CapacityProviderScalingConfig.MaxVCpuCount)

	// List
	listRec := callInMemoryHandler(t, h, http.MethodGet, "/2025-11-30/capacity-providers", "")
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut lambda.ListCapacityProvidersOutput
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	assert.Len(t, listOut.CapacityProviders, 1)

	// Delete — real AWS returns 200 with the deleted provider's state, not 204
	// (DeleteCapacityProviderOutput.CapacityProvider is a required output member).
	delRec := callInMemoryHandler(t, h, http.MethodDelete, "/2025-11-30/capacity-providers/test-cp", "")
	require.Equal(t, http.StatusOK, delRec.Code)

	var deleteOut lambda.DeleteCapacityProviderOutput
	require.NoError(t, json.NewDecoder(delRec.Body).Decode(&deleteOut))
	require.NotNil(t, deleteOut.CapacityProvider)
	assert.True(t, strings.HasSuffix(deleteOut.CapacityProvider.CapacityProviderArn, "capacity-provider:test-cp"))

	// List after delete → empty
	listRec2 := callInMemoryHandler(t, h, http.MethodGet, "/2025-11-30/capacity-providers", "")
	require.Equal(t, http.StatusOK, listRec2.Code)

	var listOut2 lambda.ListCapacityProvidersOutput
	require.NoError(t, json.NewDecoder(listRec2.Body).Decode(&listOut2))
	assert.Empty(t, listOut2.CapacityProviders)
}

// --- ListFunctionVersionsByCapacityProvider tests ---

func TestListFunctionVersionsByCapacityProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cpName     string
		wantStatus int
		setup      bool // create the CP before the list call
		wantEmpty  bool
	}{
		{
			name:       "exists_returns_empty_list",
			cpName:     "my-cp",
			setup:      true,
			wantStatus: http.StatusOK,
			wantEmpty:  true,
		},
		{
			name:       "not_found_returns_404",
			cpName:     "nonexistent-cp",
			setup:      false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)

			if tt.setup {
				body := `{"CapacityProviderName":"` + tt.cpName + `",` +
					`"PermissionsConfig":{"CapacityProviderOperatorRoleArn":"arn:aws:iam::000000000000:role/r"},` +
					`"VpcConfig":{"SubnetIds":["subnet-1"],"SecurityGroupIds":["sg-1"]}}`
				rec := callInMemoryHandler(t, h, http.MethodPost, "/2025-11-30/capacity-providers", body)
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			rec := callInMemoryHandler(t, h, http.MethodGet,
				"/2025-11-30/capacity-providers/"+tt.cpName+"/function-versions", "")
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantEmpty {
				var out struct {
					FunctionVersions []any `json:"FunctionVersions"`
				}
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				assert.Empty(t, out.FunctionVersions)
			}
		})
	}
}

// TestCapacityProvider_UpdateNotFound tests updating a nonexistent capacity provider.
func TestCapacityProvider_UpdateNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(t, h, http.MethodPut,
		"/2025-11-30/capacity-providers/nonexistent", `{}`)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestCapacityProvider_TelemetryConfig locks in the PARITY.md deferred item:
// the SDK v1.94.1->v1.97.0 bump added TelemetryConfig to CapacityProvider.
// CreateCapacityProvider and UpdateCapacityProvider must accept-and-echo it
// verbatim (this emulator does not send real system logs to CloudWatch for
// capacity-provider-managed instances, so it is round-tripped only).
func TestCapacityProvider_TelemetryConfig(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	createBody := `{
		"CapacityProviderName":"telemetry-provider",
		"PermissionsConfig":{"CapacityProviderOperatorRoleArn":"arn:aws:iam::000000000000:role/r"},
		"VpcConfig":{"SubnetIds":["subnet-1"],"SecurityGroupIds":["sg-1"]},
		"TelemetryConfig":{"LoggingConfig":{
			"LogGroup":"/aws/lambda/capacity-provider/telemetry-provider",
			"SystemLogLevel":"WARN"
		}}
	}`
	createRec := callInMemoryHandler(t, h, http.MethodPost, "/2025-11-30/capacity-providers", createBody)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var createOut lambda.CreateCapacityProviderOutput
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
	require.NotNil(t, createOut.CapacityProvider.TelemetryConfig)
	require.NotNil(t, createOut.CapacityProvider.TelemetryConfig.LoggingConfig)
	assert.Equal(t, "WARN", createOut.CapacityProvider.TelemetryConfig.LoggingConfig.SystemLogLevel)

	updateBody := `{"TelemetryConfig":{"LoggingConfig":{"SystemLogLevel":"DEBUG"}}}`
	updateRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2025-11-30/capacity-providers/telemetry-provider", updateBody)
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateOut lambda.UpdateCapacityProviderOutput
	require.NoError(t, json.NewDecoder(updateRec.Body).Decode(&updateOut))
	require.NotNil(t, updateOut.CapacityProvider.TelemetryConfig)
	require.NotNil(t, updateOut.CapacityProvider.TelemetryConfig.LoggingConfig)
	assert.Equal(t, "DEBUG", updateOut.CapacityProvider.TelemetryConfig.LoggingConfig.SystemLogLevel)
}
