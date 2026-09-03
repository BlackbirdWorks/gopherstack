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

// TestGetApiKeys_CustomerIdAndNameQueryFilters_RealClient drives GetApiKeys
// through the real client. The real GetApiKeysInput.CustomerId/NameQuery
// filter results by wire keys "customerId"/"name"
// (apigateway@v1.42.4 serializers.go:4102,4114) -- gopherstack never read
// either, so a real client's filtered request always returned every API key
// regardless of customerId/nameQuery.
func TestGetApiKeys_CustomerIdAndNameQueryFilters_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))

	_, err := client.CreateApiKey(t.Context(), &apigwsdk.CreateApiKeyInput{
		Name: aws.String("prod-key"), CustomerId: aws.String("cust-1"),
	})
	require.NoError(t, err)
	_, err = client.CreateApiKey(t.Context(), &apigwsdk.CreateApiKeyInput{
		Name: aws.String("dev-key"), CustomerId: aws.String("cust-2"),
	})
	require.NoError(t, err)

	byCustomer, err := client.GetApiKeys(t.Context(), &apigwsdk.GetApiKeysInput{CustomerId: aws.String("cust-1")})
	require.NoError(t, err)
	require.Len(t, byCustomer.Items, 1, "customerId filter must exclude the key for a different customer")
	assert.Equal(t, "prod-key", aws.ToString(byCustomer.Items[0].Name))

	byName, err := client.GetApiKeys(t.Context(), &apigwsdk.GetApiKeysInput{NameQuery: aws.String("dev")})
	require.NoError(t, err)
	require.Len(t, byName.Items, 1, "name filter must exclude keys that don't match the query")
	assert.Equal(t, "dev-key", aws.ToString(byName.Items[0].Name))
}

// TestGetApiKeys_IncludeValues_RealClient drives GetApiKeys through the real
// client. The real GetApiKeysInput.IncludeValues field serializes to wire key
// "includeValues" (plural, apigateway@v1.42.4 serializers.go:4106) -- distinct
// from GetApiKeyInput.IncludeValue's singular "includeValue" (serializers.go:
// 4036) for the single-key op. gopherstack's list-op handler read the
// singular key, so a real client's includeValues=true never populated Value.
func TestGetApiKeys_IncludeValues_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))

	_, err := client.CreateApiKey(t.Context(), &apigwsdk.CreateApiKeyInput{Name: aws.String("k1")})
	require.NoError(t, err)

	out, err := client.GetApiKeys(t.Context(), &apigwsdk.GetApiKeysInput{IncludeValues: aws.Bool(true)})
	require.NoError(t, err)
	require.Len(t, out.Items, 1)
	assert.NotEmpty(t, aws.ToString(out.Items[0].Value),
		"includeValues=true must return the key value -- real wire key is \"includeValues\" (plural)")
}

// TestGetDocumentationParts_TypeFilter_RealClient drives GetDocumentationParts
// through the real client. The real GetDocumentationPartsInput.Type filters
// by wire key "type" (apigateway@v1.42.4 serializers.go:4925) --
// gopherstack never read it, so a real client's type=METHOD request always
// returned every documentation part regardless of location type.
func TestGetDocumentationParts_TypeFilter_RealClient(t *testing.T) {
	t.Parallel()

	client, apiID, _ := setupSDKMethod(t, nil)

	_, err := client.CreateDocumentationPart(t.Context(), &apigwsdk.CreateDocumentationPartInput{
		RestApiId: aws.String(apiID),
		Location: &apigwtypes.DocumentationPartLocation{
			Type: apigwtypes.DocumentationPartTypeMethod,
			Path: aws.String("/"),
		},
		Properties: aws.String(`{"description":"method doc"}`),
	})
	require.NoError(t, err)
	_, err = client.CreateDocumentationPart(t.Context(), &apigwsdk.CreateDocumentationPartInput{
		RestApiId: aws.String(apiID),
		Location: &apigwtypes.DocumentationPartLocation{
			Type: apigwtypes.DocumentationPartTypeResource,
			Path: aws.String("/"),
		},
		Properties: aws.String(`{"description":"resource doc"}`),
	})
	require.NoError(t, err)

	out, err := client.GetDocumentationParts(t.Context(), &apigwsdk.GetDocumentationPartsInput{
		RestApiId: aws.String(apiID),
		Type:      apigwtypes.DocumentationPartTypeMethod,
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 1, "type filter must exclude the RESOURCE-type documentation part")
	assert.Equal(t, apigwtypes.DocumentationPartTypeMethod, out.Items[0].Location.Type)
}

// TestGetStages_DeploymentIdFilter_RealClient drives GetStages through the
// real client. The real GetStagesInput.DeploymentId filters by wire key
// "deploymentId" (apigateway@v1.42.4 serializers.go:7042) -- gopherstack
// never read it, so a real client's deploymentId-scoped request always
// returned every stage on the REST API regardless of deployment.
func TestGetStages_DeploymentIdFilter_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))

	api, err := client.CreateRestApi(t.Context(), &apigwsdk.CreateRestApiInput{Name: aws.String("stages-api")})
	require.NoError(t, err)

	dep1, err := client.CreateDeployment(t.Context(), &apigwsdk.CreateDeploymentInput{RestApiId: api.Id})
	require.NoError(t, err)
	dep2, err := client.CreateDeployment(t.Context(), &apigwsdk.CreateDeploymentInput{RestApiId: api.Id})
	require.NoError(t, err)

	_, err = client.CreateStage(t.Context(), &apigwsdk.CreateStageInput{
		RestApiId: api.Id, StageName: aws.String("s1"), DeploymentId: dep1.Id,
	})
	require.NoError(t, err)
	_, err = client.CreateStage(t.Context(), &apigwsdk.CreateStageInput{
		RestApiId: api.Id, StageName: aws.String("s2"), DeploymentId: dep2.Id,
	})
	require.NoError(t, err)

	out, err := client.GetStages(t.Context(), &apigwsdk.GetStagesInput{RestApiId: api.Id, DeploymentId: dep1.Id})
	require.NoError(t, err)
	require.Len(t, out.Item, 1, "deploymentId filter must exclude the stage on a different deployment")
	assert.Equal(t, "s1", aws.ToString(out.Item[0].StageName))
}

// TestGetUsagePlans_KeyIdFilter_RealClient drives GetUsagePlans through the
// real client. The real GetUsagePlansInput.KeyId filters by wire key "keyId"
// (apigateway@v1.42.4 serializers.go:7521) -- gopherstack never read it, so a
// real client's keyId-scoped request always returned every usage plan
// regardless of key association.
func TestGetUsagePlans_KeyIdFilter_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))

	plan1, err := client.CreateUsagePlan(t.Context(), &apigwsdk.CreateUsagePlanInput{Name: aws.String("plan1")})
	require.NoError(t, err)
	_, err = client.CreateUsagePlan(t.Context(), &apigwsdk.CreateUsagePlanInput{Name: aws.String("plan2")})
	require.NoError(t, err)

	key, err := client.CreateApiKey(t.Context(), &apigwsdk.CreateApiKeyInput{Name: aws.String("k1")})
	require.NoError(t, err)
	_, err = client.CreateUsagePlanKey(t.Context(), &apigwsdk.CreateUsagePlanKeyInput{
		UsagePlanId: plan1.Id, KeyId: key.Id, KeyType: aws.String("API_KEY"),
	})
	require.NoError(t, err)

	out, err := client.GetUsagePlans(t.Context(), &apigwsdk.GetUsagePlansInput{KeyId: key.Id})
	require.NoError(t, err)
	require.Len(t, out.Items, 1, "keyId filter must exclude the plan the key isn't associated with")
	assert.Equal(t, "plan1", aws.ToString(out.Items[0].Name))
}

// TestGetUsagePlanKeys_NameFilter_RealClient drives GetUsagePlanKeys through
// the real client. The real GetUsagePlanKeysInput.NameQuery filters by wire
// key "name" (apigateway@v1.42.4 serializers.go:7442) -- gopherstack never
// read it, so a real client's name-scoped request always returned every key
// on the usage plan regardless of name.
func TestGetUsagePlanKeys_NameFilter_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))

	plan, err := client.CreateUsagePlan(t.Context(), &apigwsdk.CreateUsagePlanInput{Name: aws.String("plan")})
	require.NoError(t, err)

	alice, err := client.CreateApiKey(t.Context(), &apigwsdk.CreateApiKeyInput{Name: aws.String("alice")})
	require.NoError(t, err)
	bob, err := client.CreateApiKey(t.Context(), &apigwsdk.CreateApiKeyInput{Name: aws.String("bob")})
	require.NoError(t, err)

	_, err = client.CreateUsagePlanKey(t.Context(), &apigwsdk.CreateUsagePlanKeyInput{
		UsagePlanId: plan.Id, KeyId: alice.Id, KeyType: aws.String("API_KEY"),
	})
	require.NoError(t, err)
	_, err = client.CreateUsagePlanKey(t.Context(), &apigwsdk.CreateUsagePlanKeyInput{
		UsagePlanId: plan.Id, KeyId: bob.Id, KeyType: aws.String("API_KEY"),
	})
	require.NoError(t, err)

	out, err := client.GetUsagePlanKeys(t.Context(), &apigwsdk.GetUsagePlanKeysInput{
		UsagePlanId: plan.Id, NameQuery: aws.String("alice"),
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 1, "name filter must exclude the key that doesn't match the query")
	assert.Equal(t, "alice", aws.ToString(out.Items[0].Name))
}

// TestGetUsage_KeyIdFilter_RealClient drives GetUsage through the real
// client. The real GetUsageInput.KeyId filters by wire key "keyId"
// (apigateway@v1.42.4 serializers.go:7200) -- gopherstack's GetUsageInput had
// no KeyID field at all, so a real client's keyId-scoped request always
// returned every key's usage data on the plan.
func TestGetUsage_KeyIdFilter_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))

	plan, err := client.CreateUsagePlan(t.Context(), &apigwsdk.CreateUsagePlanInput{Name: aws.String("plan")})
	require.NoError(t, err)

	key1, err := client.CreateApiKey(t.Context(), &apigwsdk.CreateApiKeyInput{Name: aws.String("k1")})
	require.NoError(t, err)
	key2, err := client.CreateApiKey(t.Context(), &apigwsdk.CreateApiKeyInput{Name: aws.String("k2")})
	require.NoError(t, err)

	for _, k := range []*string{key1.Id, key2.Id} {
		_, err = client.CreateUsagePlanKey(t.Context(), &apigwsdk.CreateUsagePlanKeyInput{
			UsagePlanId: plan.Id, KeyId: k, KeyType: aws.String("API_KEY"),
		})
		require.NoError(t, err)
	}

	out, err := client.GetUsage(t.Context(), &apigwsdk.GetUsageInput{
		UsagePlanId: plan.Id, StartDate: aws.String("2024-01-01"), EndDate: aws.String("2024-01-02"),
		KeyId: key1.Id,
	})
	require.NoError(t, err)
	assert.Contains(t, out.Items, aws.ToString(key1.Id))
	assert.NotContains(t, out.Items, aws.ToString(key2.Id),
		"keyId filter must exclude usage data for a different key")
}

// TestGetDomainNames_ResourceOwnerFilter_RealClient drives GetDomainNames
// through the real client. The real GetDomainNamesInput.ResourceOwner
// filters by wire key "resourceOwner" (apigateway@v1.42.4 serializers.go:
// 5307) -- gopherstack never read it, so a real client's
// resourceOwner=OTHER_ACCOUNTS request always returned every domain name,
// including ones only ever created under the caller's own account.
func TestGetDomainNames_ResourceOwnerFilter_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))

	_, err := client.CreateDomainName(t.Context(), &apigwsdk.CreateDomainNameInput{
		DomainName: aws.String("api.example.com"),
	})
	require.NoError(t, err)

	out, err := client.GetDomainNames(t.Context(), &apigwsdk.GetDomainNamesInput{
		ResourceOwner: apigwtypes.ResourceOwnerOtherAccounts,
	})
	require.NoError(t, err)
	assert.Empty(t, out.Items,
		"resourceOwner=OTHER_ACCOUNTS must exclude self-owned domain names")

	self, err := client.GetDomainNames(t.Context(), &apigwsdk.GetDomainNamesInput{
		ResourceOwner: apigwtypes.ResourceOwnerSelf,
	})
	require.NoError(t, err)
	assert.Len(t, self.Items, 1)
}

// TestGetAuthorizers_Pagination_RealClient drives GetAuthorizers through the
// real client with Limit=1. The real GetAuthorizersInput.Limit/Position
// (apigateway@v1.42.4 serializers.go:4264,4268) bound the page size --
// gopherstack's handler never read either, so a real client's Limit=1 request
// always returned every authorizer on the REST API in one page.
func TestGetAuthorizers_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))

	api, err := client.CreateRestApi(t.Context(), &apigwsdk.CreateRestApiInput{Name: aws.String("authz-page-api")})
	require.NoError(t, err)

	for _, name := range []string{"a1", "a2", "a3"} {
		_, err = client.CreateAuthorizer(t.Context(), &apigwsdk.CreateAuthorizerInput{
			RestApiId: api.Id, Name: aws.String(name), Type: apigwtypes.AuthorizerTypeToken,
			AuthorizerUri:  aws.String("arn:aws:apigateway:us-east-1:lambda:path/fn"),
			IdentitySource: aws.String("method.request.header.Auth"),
		})
		require.NoError(t, err)
	}

	page, err := client.GetAuthorizers(
		t.Context(),
		&apigwsdk.GetAuthorizersInput{RestApiId: api.Id, Limit: aws.Int32(1)},
	)
	require.NoError(t, err)
	require.Len(t, page.Items, 1, "Limit=1 must return exactly one authorizer per page, not all three")
}

// TestGetClientCertificates_Pagination_RealClient drives
// GetClientCertificates through the real client with Limit=1. The real
// GetClientCertificatesInput.Limit/Position (apigateway@v1.42.4
// serializers.go:4581,4585) bound the page size -- gopherstack's handler
// never read either, so a real client's Limit=1 request always returned
// every client certificate in one page.
func TestGetClientCertificates_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))

	for range 3 {
		_, err := client.GenerateClientCertificate(t.Context(), &apigwsdk.GenerateClientCertificateInput{})
		require.NoError(t, err)
	}

	page, err := client.GetClientCertificates(t.Context(), &apigwsdk.GetClientCertificatesInput{Limit: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, page.Items, 1, "Limit=1 must return exactly one certificate per page, not all three")
}
