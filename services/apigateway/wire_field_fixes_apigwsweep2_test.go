package apigateway_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigwsdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwtypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// TestUpdateUsage_WireKeyValues_RealClient drives UpdateUsage/GetUsage
// through the real aws-sdk-go-v2 client (gopherstack-7185). The real
// UpdateUsageOutput/GetUsageOutput deserializer reads usage data under the
// wire key "values" into Items (apigateway@v1.42.4 deserializers.go:
// awsRestjson1_deserializeOpDocumentUpdateUsageOutput's "values" case, which
// feeds sv.Items) -- gopherstack's UsageData model tagged Items as "items"
// instead, so a real client's Items was ALWAYS empty regardless of what the
// backend computed. Confirmed by hand-reverting the json tag: this test then
// fails with "out.Items[keyID] must be non-empty -- real wire key for usage
// data is \"values\", not \"items\"" because out.Items comes back nil.
func TestUpdateUsage_WireKeyValues_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))

	plan, err := client.CreateUsagePlan(t.Context(), &apigwsdk.CreateUsagePlanInput{Name: aws.String("usage-plan")})
	require.NoError(t, err)

	key, err := client.CreateApiKey(t.Context(), &apigwsdk.CreateApiKeyInput{Name: aws.String("usage-key")})
	require.NoError(t, err)

	_, err = client.CreateUsagePlanKey(t.Context(), &apigwsdk.CreateUsagePlanKeyInput{
		UsagePlanId: plan.Id, KeyId: key.Id, KeyType: aws.String("API_KEY"),
	})
	require.NoError(t, err)

	updated, err := client.UpdateUsage(t.Context(), &apigwsdk.UpdateUsageInput{
		UsagePlanId: plan.Id, KeyId: key.Id,
		PatchOperations: []apigwtypes.PatchOperation{
			{Op: apigwtypes.OpReplace, Path: aws.String("/remaining"), Value: aws.String("42")},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, updated.Items[aws.ToString(key.Id)],
		"out.Items[keyID] must be non-empty -- real wire key for usage data is \"values\", not \"items\"")
	assert.Equal(t, int64(42), updated.Items[aws.ToString(key.Id)][0][1])

	got, err := client.GetUsage(t.Context(), &apigwsdk.GetUsageInput{
		UsagePlanId: plan.Id, StartDate: aws.String("2024-01-01"), EndDate: aws.String("2024-01-02"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, got.Items[aws.ToString(key.Id)])
	assert.Equal(t, int64(42), got.Items[aws.ToString(key.Id)][0][1])
}

// TestTestInvokeAuthorizer_Shape_RealClient drives TestInvokeAuthorizer
// through the real client. The real TestInvokeAuthorizerOutput.Authorization
// is a map[string][]string (apigateway@v1.42.4 deserializers.go:
// awsRestjson1_deserializeOpDocumentTestInvokeAuthorizerOutput's
// "authorization" case calls awsRestjson1_deserializeDocumentMapOfStringToList,
// which hard-errors with "unexpected JSON type" on anything but a JSON
// object) -- gopherstack emitted an int (an HTTP status code) under that same
// key, so a real client's call FAILED to deserialize entirely, not merely
// lost a field. Confirmed by hand-reverting: the call then fails with
// "unexpected JSON type %!v(float64=200)" surfaced as a smithy deserialization
// error, so this test's require.NoError below is what catches it.
func TestTestInvokeAuthorizer_Shape_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))

	api, err := client.CreateRestApi(t.Context(), &apigwsdk.CreateRestApiInput{Name: aws.String("authz-shape-api")})
	require.NoError(t, err)

	authz, err := client.CreateAuthorizer(t.Context(), &apigwsdk.CreateAuthorizerInput{
		RestApiId: api.Id, Name: aws.String("authz"), Type: apigwtypes.AuthorizerTypeToken,
	})
	require.NoError(t, err)

	out, err := client.TestInvokeAuthorizer(t.Context(), &apigwsdk.TestInvokeAuthorizerInput{
		RestApiId: api.Id, AuthorizerId: authz.Id,
	})
	require.NoError(t, err, "real client must be able to deserialize TestInvokeAuthorizerOutput")
	assert.Equal(t, "test-principal", aws.ToString(out.PrincipalId))
}

// TestTestInvokeMethod_MultiValueHeaders_RealClient drives TestInvokeMethod
// through the real client. The real TestInvokeMethodOutput carries a
// separate MultiValueHeaders member (apigateway@v1.42.4 deserializers.go:
// awsRestjson1_deserializeOpDocumentTestInvokeMethodOutput's
// "multiValueHeaders" case) alongside Headers -- gopherstack's model had no
// such field, so it was always empty/absent regardless of Headers. Confirmed
// by hand-reverting: this test then fails with "out.MultiValueHeaders must
// carry the same header gopherstack put in out.Headers" because
// MultiValueHeaders comes back nil.
func TestTestInvokeMethod_MultiValueHeaders_RealClient(t *testing.T) {
	t.Parallel()

	client, apiID, rootID := setupSDKMethod(t, nil)

	out, err := client.TestInvokeMethod(t.Context(), &apigwsdk.TestInvokeMethodInput{
		RestApiId: aws.String(apiID), ResourceId: aws.String(rootID), HttpMethod: aws.String("GET"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Headers)

	for k, v := range out.Headers {
		require.Contains(t, out.MultiValueHeaders, k,
			"out.MultiValueHeaders must carry the same header gopherstack put in out.Headers")
		assert.Equal(t, []string{v}, out.MultiValueHeaders[k])
	}
}

// TestUpdateRestApi_SecurityPolicyPatch_RealClient drives UpdateRestApi
// through the real client with a "/securityPolicy" PatchOperation
// (patch-operations.html documents this path for UpdateRestApi). Before the
// fix, UpdateRestAPIInput had no SecurityPolicy field at all, so the
// PATCH-flattened "securityPolicy" key was silently dropped by
// json.Unmarshal -- the call returned 200 with the UNMODIFIED RestApi, the
// same "PATCH silently does nothing" shape as an empty envelope. Confirmed by
// hand-reverting: this test then fails asserting "TLS_1_2" but getting "" for
// both the PATCH response and the follow-up GetRestApi.
func TestUpdateRestApi_SecurityPolicyPatch_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))

	api, err := client.CreateRestApi(t.Context(), &apigwsdk.CreateRestApiInput{Name: aws.String("secpol-api")})
	require.NoError(t, err)
	require.Empty(t, string(api.SecurityPolicy))

	updated, err := client.UpdateRestApi(t.Context(), &apigwsdk.UpdateRestApiInput{
		RestApiId: api.Id,
		PatchOperations: []apigwtypes.PatchOperation{
			{Op: apigwtypes.OpReplace, Path: aws.String("/securityPolicy"), Value: aws.String("TLS_1_2")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "TLS_1_2", string(updated.SecurityPolicy),
		"PATCH /securityPolicy must actually change SecurityPolicy, not silently no-op")

	got, err := client.GetRestApi(t.Context(), &apigwsdk.GetRestApiInput{RestApiId: api.Id})
	require.NoError(t, err)
	assert.Equal(t, "TLS_1_2", string(got.SecurityPolicy))
}

// TestUpdateAuthorizer_AuthTypePatch_RealClient drives UpdateAuthorizer
// through the real client with an "/authType" PatchOperation
// (patch-operations.html documents this path; types.Authorizer.AuthType in
// the SDK is a real, separate field from Authorizer's Type). Before the fix,
// gopherstack's Authorizer model had no AuthType field at all, so the
// PATCH-flattened "authType" key was silently dropped -- same "PATCH
// silently does nothing" shape as UpdateRestApi's SecurityPolicy above.
// Confirmed by hand-reverting: this test then fails asserting "COGNITO_USER_POOLS"
// but getting "" back from both the PATCH response and CreateAuthorizer's
// AuthType passthrough.
func TestUpdateAuthorizer_AuthTypePatch_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))

	api, err := client.CreateRestApi(t.Context(), &apigwsdk.CreateRestApiInput{Name: aws.String("authtype-api")})
	require.NoError(t, err)

	authz, err := client.CreateAuthorizer(t.Context(), &apigwsdk.CreateAuthorizerInput{
		RestApiId: api.Id, Name: aws.String("authz"), Type: apigwtypes.AuthorizerTypeToken,
		AuthType: aws.String("custom"),
	})
	require.NoError(t, err)
	assert.Equal(t, "custom", aws.ToString(authz.AuthType), "CreateAuthorizer must pass through AuthType")

	updated, err := client.UpdateAuthorizer(t.Context(), &apigwsdk.UpdateAuthorizerInput{
		RestApiId: api.Id, AuthorizerId: authz.Id,
		PatchOperations: []apigwtypes.PatchOperation{
			{Op: apigwtypes.OpReplace, Path: aws.String("/authType"), Value: aws.String("COGNITO_USER_POOLS")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "COGNITO_USER_POOLS", aws.ToString(updated.AuthType),
		"PATCH /authType must actually change AuthType, not silently no-op")
}

// TestCreateDeployment_APISummary_RealClient drives CreateDeployment through
// the real client. The real Deployment.ApiSummary
// (map[string]map[string]types.MethodSnapshot, apigateway@v1.42.4
// deserializers.go's "apiSummary" case) snapshots every resource path's
// methods at deployment time -- gopherstack's Deployment model had no
// ApiSummary field at all, so a real client's ApiSummary was always empty
// regardless of the API's configured methods. Confirmed by hand-reverting:
// this test then fails with "ApiSummary must contain the root resource's
// path" because out.ApiSummary comes back nil.
func TestCreateDeployment_APISummary_RealClient(t *testing.T) {
	t.Parallel()

	client, apiID, _ := setupSDKMethod(t, func(in *apigwsdk.PutMethodInput) {
		in.ApiKeyRequired = true
	})

	out, err := client.CreateDeployment(t.Context(), &apigwsdk.CreateDeploymentInput{RestApiId: aws.String(apiID)})
	require.NoError(t, err)
	require.Contains(t, out.ApiSummary, "/", "ApiSummary must contain the root resource's path")

	methods := out.ApiSummary["/"]
	require.Contains(t, methods, "GET")
	assert.Equal(t, "NONE", aws.ToString(methods["GET"].AuthorizationType))
	assert.True(t, methods["GET"].ApiKeyRequired)
}
