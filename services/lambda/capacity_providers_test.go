package lambda_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

// TestListFunctionVersionsByCapacityProvider_SeededAssignments verifies that
// function-version ARNs seeded onto a capacity provider via the internal seeding
// helper are returned by ListFunctionVersionsByCapacityProvider in sorted order.
//
// AWS exposes no public assignment API in this emulator's surface, so seeding is
// the only way to populate these assignments.
func TestListFunctionVersionsByCapacityProvider_SeededAssignments(t *testing.T) {
	t.Parallel()

	bk := newCapacityProviderTestBackend(t)

	_, err := bk.CreateCapacityProvider(&lambda.CreateCapacityProviderInput{
		CapacityProviderName: "my-cp",
		PermissionsConfig: &lambda.CapacityProviderPermissionsConfig{
			CapacityProviderOperatorRoleArn: "arn:aws:iam::000000000000:role/cp-role",
		},
		VpcConfig: &lambda.CapacityProviderVpcConfig{
			SubnetIDs:        []string{"subnet-1"},
			SecurityGroupIDs: []string{"sg-1"},
		},
	})
	require.NoError(t, err)

	const (
		v1 = "arn:aws:lambda:us-east-1:000000000000:function:fn:1"
		v2 = "arn:aws:lambda:us-east-1:000000000000:function:fn:2"
	)

	// Seed in reverse order to confirm deterministic sorted output.
	require.NoError(t, bk.SeedCapacityProviderFunctionVersions("my-cp", v2, v1))

	p, err := bk.ListFunctionVersionsByCapacityProvider("my-cp", "", 0)
	require.NoError(t, err)
	assert.Empty(t, p.Next)
	assert.Equal(t, []string{v1, v2}, p.Data)
}

// TestListFunctionVersionsByCapacityProvider_Pagination verifies that the
// MaxItems/Marker pagination is honoured for seeded assignments.
func TestListFunctionVersionsByCapacityProvider_Pagination(t *testing.T) {
	t.Parallel()

	bk := newCapacityProviderTestBackend(t)

	_, err := bk.CreateCapacityProvider(&lambda.CreateCapacityProviderInput{
		CapacityProviderName: "cp",
		PermissionsConfig: &lambda.CapacityProviderPermissionsConfig{
			CapacityProviderOperatorRoleArn: "arn:aws:iam::000000000000:role/cp-role",
		},
		VpcConfig: &lambda.CapacityProviderVpcConfig{
			SubnetIDs:        []string{"subnet-1"},
			SecurityGroupIDs: []string{"sg-1"},
		},
	})
	require.NoError(t, err)

	const (
		v1 = "arn:aws:lambda:us-east-1:000000000000:function:fn:1"
		v2 = "arn:aws:lambda:us-east-1:000000000000:function:fn:2"
		v3 = "arn:aws:lambda:us-east-1:000000000000:function:fn:3"
	)
	require.NoError(t, bk.SeedCapacityProviderFunctionVersions("cp", v1, v2, v3))

	first, err := bk.ListFunctionVersionsByCapacityProvider("cp", "", 2)
	require.NoError(t, err)
	assert.Equal(t, []string{v1, v2}, first.Data)
	require.NotEmpty(t, first.Next)

	second, err := bk.ListFunctionVersionsByCapacityProvider("cp", first.Next, 2)
	require.NoError(t, err)
	assert.Equal(t, []string{v3}, second.Data)
	assert.Empty(t, second.Next)
}

// TestListFunctionVersionsByCapacityProvider_NotFound verifies that listing or
// seeding versions for a missing capacity provider returns ErrFunctionNotFound,
// which the handler maps to ResourceNotFoundException.
func TestListFunctionVersionsByCapacityProvider_NotFound(t *testing.T) {
	t.Parallel()

	bk := newCapacityProviderTestBackend(t)

	_, err := bk.ListFunctionVersionsByCapacityProvider("missing", "", 0)
	require.ErrorIs(t, err, lambda.ErrFunctionNotFound)

	err = bk.SeedCapacityProviderFunctionVersions("missing", "arn:whatever")
	require.ErrorIs(t, err, lambda.ErrFunctionNotFound)
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
	assert.Equal(t, types.CapacityProviderStateActive, cp.State)

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

	const versionArn = "arn:aws:lambda:us-east-1:000000000000:function:fn:1"
	require.NoError(t, backend.SeedCapacityProviderFunctionVersions("list-versions-cp", versionArn))

	listed, err := client.ListFunctionVersionsByCapacityProvider(
		t.Context(),
		&lambdasdk.ListFunctionVersionsByCapacityProviderInput{
			CapacityProviderName: aws.String("list-versions-cp"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, *created.CapacityProvider.CapacityProviderArn, *listed.CapacityProviderArn)
	require.Len(t, listed.FunctionVersions, 1)
	assert.Equal(t, versionArn, *listed.FunctionVersions[0].FunctionArn)
	assert.Equal(t, types.StateActive, listed.FunctionVersions[0].State)
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

	// Delete
	delRec := callInMemoryHandler(t, h, http.MethodDelete, "/2025-11-30/capacity-providers/test-cp", "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)

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
