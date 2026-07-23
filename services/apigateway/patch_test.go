package apigateway_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// setupStage creates a REST API, a deployment, and a stage pointing at it,
// returning the API ID alongside the ready-to-patch stage. variables/canary
// may be nil.
func setupStage(
	t *testing.T, h *apigateway.Handler, variables map[string]string, canary *apigateway.CanarySettings,
) (string, string) {
	t.Helper()

	api, err := h.Backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "patch-test-api"})
	require.NoError(t, err)

	depl, err := h.Backend.CreateDeployment(api.ID, "", "v1")
	require.NoError(t, err)

	_, err = h.Backend.CreateStage(apigateway.CreateStageInput{
		RestAPIID:      api.ID,
		StageName:      "prod",
		DeploymentID:   depl.ID,
		Variables:      variables,
		CanarySettings: canary,
	})
	require.NoError(t, err)

	return api.ID, "prod"
}

// Test_ApplyStructuredPatch_StageVariables exercises per-variable "/variables/{name}"
// PATCH ops (add/replace/remove, including a JSON-Pointer-escaped name), which
// the old flatten silently dropped (it took the whole remaining path
// "variables/foo" as one bogus flat field, matching no Update*Input json tag).
func Test_ApplyStructuredPatch_StageVariables(t *testing.T) {
	t.Parallel()

	cases := []struct {
		want map[string]string
		name string
		ops  string
	}{
		{
			name: "add a new variable merges with existing ones",
			ops:  `[{"op":"add","path":"/variables/c","value":"3"}]`,
			want: map[string]string{"a": "1", "b": "2", "c": "3"},
		},
		{
			name: "replace an existing variable leaves siblings untouched",
			ops:  `[{"op":"replace","path":"/variables/a","value":"9"}]`,
			want: map[string]string{"a": "9", "b": "2"},
		},
		{
			name: "remove deletes only the named variable",
			ops:  `[{"op":"remove","path":"/variables/b"}]`,
			want: map[string]string{"a": "1"},
		},
		{
			name: "JSON-Pointer-escaped variable name is unescaped",
			ops:  `[{"op":"add","path":"/variables/a~1b","value":"x"}]`,
			want: map[string]string{"a": "1", "b": "2", "a/b": "x"},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAPIGWHandler()
			apiID, stageName := setupStage(t, h, map[string]string{"a": "1", "b": "2"}, nil)

			rec := restRequest(t, h, http.MethodPatch,
				fmt.Sprintf("/restapis/%s/stages/%s", apiID, stageName), tt.ops)
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

			stage, err := h.Backend.GetStage(apiID, stageName)
			require.NoError(t, err)
			assert.Equal(t, tt.want, stage.Variables)
		})
	}
}

// Test_ApplyStructuredPatch_StageCanaryPromotion exercises the AWS-documented
// canary-promotion pattern: a "copy" op from "/canarySettings/deploymentId" to
// "/deploymentId". The old flatten never implemented "copy" at all (only
// "add"/"replace" were honored), so promotion was a total no-op.
func Test_ApplyStructuredPatch_StageCanaryPromotion(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()
	apiID, stageName := setupStage(t, h, nil, &apigateway.CanarySettings{
		DeploymentID:   "canary-depl-id",
		PercentTraffic: 50,
	})

	before, err := h.Backend.GetStage(apiID, stageName)
	require.NoError(t, err)
	require.NotEqual(t, "canary-depl-id", before.DeploymentID, "precondition: main deployment differs from canary")

	body := `{"patchOperations":[{"op":"copy","from":"/canarySettings/deploymentId","path":"/deploymentId"}]}`
	rec := restRequest(t, h, http.MethodPatch, fmt.Sprintf("/restapis/%s/stages/%s", apiID, stageName), body)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	after, err := h.Backend.GetStage(apiID, stageName)
	require.NoError(t, err)
	assert.Equal(t, "canary-depl-id", after.DeploymentID,
		"promoted deployment id should replace the stage's deploymentId")
}

// Test_ApplyStructuredPatch_StageMethodSettings exercises per-route method
// settings, addressed by AWS with NO "methodSettings" path prefix
// ("/{resourcePath}/{httpMethod}/{category}/{property}", e.g. "/*/*/logging/loglevel"
// or "/~1pets/GET/caching/enabled"). The old flatten had no notion of this
// path shape at all.
func Test_ApplyStructuredPatch_StageMethodSettings(t *testing.T) {
	t.Parallel()

	cases := []struct {
		check    func(t *testing.T, ms apigateway.MethodSetting)
		name     string
		path     string
		value    string
		routeKey string
	}{
		{
			name:     "wildcard throttling burst limit (string value coerced to int)",
			path:     "/*/*/throttling/burstLimit",
			value:    "500",
			routeKey: "*/*",
			check: func(t *testing.T, ms apigateway.MethodSetting) {
				t.Helper()
				assert.Equal(t, 500, ms.ThrottlingBurstLimit)
			},
		},
		{
			name:     "wildcard throttling rate limit (string value coerced to float)",
			path:     "/*/*/throttling/rateLimit",
			value:    "12.5",
			routeKey: "*/*",
			check: func(t *testing.T, ms apigateway.MethodSetting) {
				t.Helper()
				assert.InDelta(t, 12.5, ms.ThrottlingRateLimit, 0.001)
			},
		},
		{
			name:     "wildcard logging level",
			path:     "/*/*/logging/loglevel",
			value:    "INFO",
			routeKey: "*/*",
			check: func(t *testing.T, ms apigateway.MethodSetting) {
				t.Helper()
				assert.Equal(t, "INFO", ms.LoggingLevel)
			},
		},
		{
			name:     "wildcard metrics enabled (string value coerced to bool)",
			path:     "/*/*/metrics/enabled",
			value:    "true",
			routeKey: "*/*",
			check: func(t *testing.T, ms apigateway.MethodSetting) {
				t.Helper()
				assert.True(t, ms.MetricsEnabled)
			},
		},
		{
			name:     "escaped resource path and specific method",
			path:     "/~1pets/GET/caching/enabled",
			value:    "true",
			routeKey: "/pets/GET",
			check: func(t *testing.T, ms apigateway.MethodSetting) {
				t.Helper()
				assert.True(t, ms.CachingEnabled)
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAPIGWHandler()
			apiID, stageName := setupStage(t, h, nil, nil)

			body := fmt.Sprintf(`[{"op":"replace","path":%q,"value":%q}]`, tt.path, tt.value)
			rec := restRequest(t, h, http.MethodPatch,
				fmt.Sprintf("/restapis/%s/stages/%s", apiID, stageName), body)
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

			stage, err := h.Backend.GetStage(apiID, stageName)
			require.NoError(t, err)
			require.Contains(t, stage.MethodSettings, tt.routeKey)
			tt.check(t, stage.MethodSettings[tt.routeKey])
		})
	}
}

// Test_ApplyStructuredPatch_StageMethodSettingsRemove verifies that "remove"
// resets a per-route property back to its zero value, merging with (not
// wiping) any other properties already set on that same route.
func Test_ApplyStructuredPatch_StageMethodSettingsRemove(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()
	apiID, stageName := setupStage(t, h, nil, nil)

	setBody := `[
		{"op":"replace","path":"/*/*/throttling/burstLimit","value":"500"},
		{"op":"replace","path":"/*/*/logging/loglevel","value":"INFO"}
	]`
	rec := restRequest(t, h, http.MethodPatch, fmt.Sprintf("/restapis/%s/stages/%s", apiID, stageName), setBody)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	removeBody := `[{"op":"remove","path":"/*/*/throttling/burstLimit"}]`
	rec = restRequest(t, h, http.MethodPatch, fmt.Sprintf("/restapis/%s/stages/%s", apiID, stageName), removeBody)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	stage, err := h.Backend.GetStage(apiID, stageName)
	require.NoError(t, err)
	require.Contains(t, stage.MethodSettings, "*/*")
	assert.Equal(t, 0, stage.MethodSettings["*/*"].ThrottlingBurstLimit, "removed property resets to zero value")
	assert.Equal(t, "INFO", stage.MethodSettings["*/*"].LoggingLevel, "sibling property on the same route is untouched")
}

// Test_ApplyStructuredPatch_StageTopLevelCoercion exercises top-level
// boolean/string fields whose PATCH value is transmitted as a JSON string on
// the wire (per aws-sdk-go-v2's PatchOperation serializer). Before the fix,
// copying that raw JSON string into a *bool field made json.Unmarshal fail
// with a type mismatch, so these patches errored instead of applying.
func Test_ApplyStructuredPatch_StageTopLevelCoercion(t *testing.T) {
	t.Parallel()

	cases := []struct {
		check func(t *testing.T, stage *apigateway.Stage)
		name  string
		ops   string
	}{
		{
			name: "tracingEnabled string value coerces to bool",
			ops:  `[{"op":"replace","path":"/tracingEnabled","value":"true"}]`,
			check: func(t *testing.T, stage *apigateway.Stage) {
				t.Helper()
				assert.True(t, stage.TracingEnabled)
			},
		},
		{
			name: "cacheClusterEnabled string value coerces to bool and derives status",
			ops:  `[{"op":"replace","path":"/cacheClusterEnabled","value":"true"}]`,
			check: func(t *testing.T, stage *apigateway.Stage) {
				t.Helper()
				assert.True(t, stage.CacheClusterEnabled)
				assert.Equal(t, "AVAILABLE", stage.CacheClusterStatus)
			},
		},
		{
			name: "cacheClusterSize is a plain string field",
			ops:  `[{"op":"replace","path":"/cacheClusterSize","value":"0.5"}]`,
			check: func(t *testing.T, stage *apigateway.Stage) {
				t.Helper()
				assert.Equal(t, "0.5", stage.CacheClusterSize)
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAPIGWHandler()
			apiID, stageName := setupStage(t, h, nil, nil)

			rec := restRequest(t, h, http.MethodPatch,
				fmt.Sprintf("/restapis/%s/stages/%s", apiID, stageName), tt.ops)
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

			stage, err := h.Backend.GetStage(apiID, stageName)
			require.NoError(t, err)
			tt.check(t, stage)
		})
	}
}

// Test_ApplyStructuredPatch_RestAPIBinaryMediaTypes exercises per-entry
// "/binaryMediaTypes/{escaped-media-type}" add/remove, which must merge with
// (not replace) the API's existing binary media types.
func Test_ApplyStructuredPatch_RestAPIBinaryMediaTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		ops  string
		want []string
	}{
		{
			name: "add merges with the existing type",
			ops:  `[{"op":"add","path":"/binaryMediaTypes/application~1octet-stream"}]`,
			want: []string{"image/png", "application/octet-stream"},
		},
		{
			name: "add is idempotent",
			ops:  `[{"op":"add","path":"/binaryMediaTypes/image~1png"}]`,
			want: []string{"image/png"},
		},
		{
			name: "remove drops only the named type",
			ops:  `[{"op":"remove","path":"/binaryMediaTypes/image~1png"}]`,
			want: []string{},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAPIGWHandler()
			api, err := h.Backend.CreateRestAPI(apigateway.CreateRestAPIInput{
				Name:             "media-test-api",
				BinaryMediaTypes: []string{"image/png"},
			})
			require.NoError(t, err)

			rec := restRequest(t, h, http.MethodPatch, fmt.Sprintf("/restapis/%s", api.ID), tt.ops)
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

			got, err := h.Backend.GetRestAPI(api.ID)
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.want, got.BinaryMediaTypes)
		})
	}
}

// Test_ApplyStructuredPatch_RestAPIMinimumCompressionSizeCoercion verifies the
// numeric top-level field /minimumCompressionSize (an *int) correctly parses
// its string-typed wire value instead of failing to unmarshal.
func Test_ApplyStructuredPatch_RestAPIMinimumCompressionSizeCoercion(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()
	api, err := h.Backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "compression-test-api"})
	require.NoError(t, err)

	ops := `[{"op":"replace","path":"/minimumCompressionSize","value":"1024"}]`
	rec := restRequest(t, h, http.MethodPatch, fmt.Sprintf("/restapis/%s", api.ID), ops)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	got, err := h.Backend.GetRestAPI(api.ID)
	require.NoError(t, err)
	assert.Equal(t, 1024, got.MinimumCompressionSize)
}

// Test_ApplyStructuredPatch_Account exercises UpdateAccount's nested
// "/throttle/{rateLimit,burstLimit}" edits (merged across separate PATCH
// calls, since each only touches one sub-field) and the top-level
// "/cloudwatchRoleArn" field, which UpdateAccountInput previously lacked
// entirely (so it could never be set via UpdateAccount at all).
func Test_ApplyStructuredPatch_Account(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()

	initial, err := h.Backend.GetAccount()
	require.NoError(t, err)
	require.NotNil(t, initial.ThrottleSettings, "the backend seeds default account throttle settings")
	initialBurstLimit := initial.ThrottleSettings.BurstLimit

	rec := restRequest(t, h, http.MethodPatch, "/account",
		`[{"op":"replace","path":"/cloudwatchRoleArn","value":"arn:aws:iam::123456789012:role/apigw"}]`)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	acct, err := h.Backend.GetAccount()
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::123456789012:role/apigw", acct.CloudwatchRoleARN)

	rec = restRequest(t, h, http.MethodPatch, "/account",
		`[{"op":"replace","path":"/throttle/rateLimit","value":"100.5"}]`)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	acct, err = h.Backend.GetAccount()
	require.NoError(t, err)
	require.NotNil(t, acct.ThrottleSettings)
	assert.InDelta(t, 100.5, acct.ThrottleSettings.RateLimit, 0.001)
	assert.Equal(t, initialBurstLimit, acct.ThrottleSettings.BurstLimit, "untouched sub-field keeps its prior value")

	rec = restRequest(t, h, http.MethodPatch, "/account",
		`[{"op":"replace","path":"/throttle/burstLimit","value":"50"}]`)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	acct, err = h.Backend.GetAccount()
	require.NoError(t, err)
	require.NotNil(t, acct.ThrottleSettings)
	assert.InDelta(t, 100.5, acct.ThrottleSettings.RateLimit, 0.001, "earlier rateLimit patch must not be clobbered")
	assert.Equal(t, 50, acct.ThrottleSettings.BurstLimit)
	assert.Equal(t, "arn:aws:iam::123456789012:role/apigw", acct.CloudwatchRoleARN, "unrelated field untouched")
}

// Test_ApplyStructuredPatch_UsagePlanAPIStages exercises the single-segment
// "/apiStages" add/remove membership edit, where AWS's Value is the string
// "{restApiId}:{stage}" (not a nested path). It also proves the companion fix
// to InMemoryBackend.UpdateUsagePlan, which previously only applied APIStages
// when non-empty (`len(...) > 0`), silently ignoring a patch that removes the
// last remaining API stage.
func Test_ApplyStructuredPatch_UsagePlanAPIStages(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		op   string
		want []apigateway.APIStageAssociation
	}{
		{
			name: "add merges with the existing stage",
			op:   `{"op":"add","path":"/apiStages","value":"api2:prod"}`,
			want: []apigateway.APIStageAssociation{
				{RestAPIID: "api1", Stage: "dev"},
				{RestAPIID: "api2", Stage: "prod"},
			},
		},
		{
			name: "add is idempotent",
			op:   `{"op":"add","path":"/apiStages","value":"api1:dev"}`,
			want: []apigateway.APIStageAssociation{{RestAPIID: "api1", Stage: "dev"}},
		},
		{
			name: "remove the only stage empties the list",
			op:   `{"op":"remove","path":"/apiStages","value":"api1:dev"}`,
			want: []apigateway.APIStageAssociation{},
		},
		{
			name: "remove a non-member stage is a no-op",
			op:   `{"op":"remove","path":"/apiStages","value":"api9:staging"}`,
			want: []apigateway.APIStageAssociation{{RestAPIID: "api1", Stage: "dev"}},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAPIGWHandler()
			plan, err := h.Backend.CreateUsagePlan(apigateway.CreateUsagePlanInput{
				Name:      "plan",
				APIStages: []apigateway.APIStageAssociation{{RestAPIID: "api1", Stage: "dev"}},
			})
			require.NoError(t, err)

			body := fmt.Sprintf(`{"patchOperations":[%s]}`, tt.op)
			rec := restRequest(t, h, http.MethodPatch, fmt.Sprintf("/usageplans/%s", plan.ID), body)
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

			got, err := h.Backend.GetUsagePlan(plan.ID)
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.want, got.APIStages)
		})
	}
}

// Test_ApplyStructuredPatch_GatewayResponseMerge proves UpdateGatewayResponse
// merges a per-key PATCH ("/responseParameters/{key}") with the response's
// existing StatusCode/ResponseTemplates/other ResponseParameters entries,
// instead of wholesale-replacing the resource (which previously reused
// PutGatewayResponse's full-replace semantics and silently reverted
// StatusCode to its default and wiped ResponseTemplates on every PATCH).
func Test_ApplyStructuredPatch_GatewayResponseMerge(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()
	api, err := h.Backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "gwresp-test-api"})
	require.NoError(t, err)

	putBody := `{
		"statusCode":"404",
		"responseParameters":{"gatewayresponse.header.A":"'a-value'"},
		"responseTemplates":{"application/json":"{\"message\":$context.error.messageString}"}
	}`
	rec := restRequest(t, h, http.MethodPut,
		fmt.Sprintf("/restapis/%s/gatewayresponses/ACCESS_DENIED", api.ID), putBody)
	require.Equal(t, http.StatusCreated, rec.Code, "put body: %s", rec.Body.String())

	patchBody := `[{"op":"add","path":"/responseParameters/gatewayresponse.header.B","value":"'b-value'"}]`
	rec = restRequest(t, h, http.MethodPatch,
		fmt.Sprintf("/restapis/%s/gatewayresponses/ACCESS_DENIED", api.ID), patchBody)
	require.Equal(t, http.StatusOK, rec.Code, "patch body: %s", rec.Body.String())

	gr, err := h.Backend.GetGatewayResponse(api.ID, "ACCESS_DENIED")
	require.NoError(t, err)
	assert.Equal(t, "404", gr.StatusCode, "unrelated statusCode must survive the patch")
	assert.Equal(t,
		map[string]string{"application/json": "{\"message\":$context.error.messageString}"},
		gr.ResponseTemplates,
		"unrelated responseTemplates must survive the patch",
	)
	assert.Equal(t, map[string]string{
		"gatewayresponse.header.A": "'a-value'",
		"gatewayresponse.header.B": "'b-value'",
	}, gr.ResponseParameters, "the pre-existing parameter must survive alongside the newly patched one")
}

// Test_ApplyStructuredPatch_TopLevelReplaceStillWorks is a regression check:
// the common case (a single plain top-level field replace) must keep working
// exactly as before, in both wire forms (bare array and the
// {"patchOperations":[...]} wrapper with sibling fields).
func Test_ApplyStructuredPatch_TopLevelReplaceStillWorks(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		body string
	}{
		{name: "bare patch array", body: `[{"op":"replace","path":"/description","value":"updated"}]`},
		{
			name: "patchOperations wrapper",
			body: `{"patchOperations":[{"op":"replace","path":"/description","value":"updated"}]}`,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAPIGWHandler()
			api, err := h.Backend.CreateRestAPI(apigateway.CreateRestAPIInput{
				Name:        "toplevel-test-api",
				Description: "original",
			})
			require.NoError(t, err)

			rec := restRequest(t, h, http.MethodPatch, fmt.Sprintf("/restapis/%s", api.ID), tt.body)
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

			got, err := h.Backend.GetRestAPI(api.ID)
			require.NoError(t, err)
			assert.Equal(t, "updated", got.Description)
			assert.Equal(t, "toplevel-test-api", got.Name, "unrelated field untouched")
		})
	}
}

// Test_ApplyStructuredPatch_RestAPIDescriptionRemove proves that PATCH
// "remove" on the top-level scalar "/description" actually clears the field,
// which AWS documents as supported (patch-operations.html: UpdateRestApi
// "/description" row lists op:remove). Before UpdateRestAPIInput.Description
// became a *string, "remove" was indistinguishable from "not provided in
// this PATCH at all" and silently no-op'd.
func Test_ApplyStructuredPatch_RestAPIDescriptionRemove(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()
	api, err := h.Backend.CreateRestAPI(apigateway.CreateRestAPIInput{
		Name:        "desc-remove-api",
		Description: "original description",
	})
	require.NoError(t, err)

	rec := restRequest(t, h, http.MethodPatch, fmt.Sprintf("/restapis/%s", api.ID),
		`[{"op":"remove","path":"/description"}]`)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	got, err := h.Backend.GetRestAPI(api.ID)
	require.NoError(t, err)
	assert.Empty(t, got.Description, "explicit remove must clear the field")
	assert.Equal(t, "desc-remove-api", got.Name, "unrelated field untouched")
}

// Test_ApplyStructuredPatch_RestAPIEndpointFields exercises the two RestApi
// fields absent from gopherstack's RestAPI struct until this sweep:
// "/disableExecuteApiEndpoint" (a bool needing wire-string coercion, like
// tracingEnabled) and "/endpointAccessMode" (a plain string enum). Both are
// documented as replace-only PATCH paths (patch-operations.html).
func Test_ApplyStructuredPatch_RestAPIEndpointFields(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()
	api, err := h.Backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "endpoint-fields-api"})
	require.NoError(t, err)
	require.False(t, api.DisableExecuteAPIEndpoint, "precondition: defaults to false")
	require.Equal(t, "AVAILABLE", api.APIStatus, "gopherstack creates RestApis synchronously")

	rec := restRequest(t, h, http.MethodPatch, fmt.Sprintf("/restapis/%s", api.ID),
		`[{"op":"replace","path":"/disableExecuteApiEndpoint","value":"true"},`+
			`{"op":"replace","path":"/endpointAccessMode","value":"STRICT"}]`)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	got, err := h.Backend.GetRestAPI(api.ID)
	require.NoError(t, err)
	assert.True(t, got.DisableExecuteAPIEndpoint, "string wire value must coerce to bool")
	assert.Equal(t, "STRICT", got.EndpointAccessMode)
}

// Test_ApplyStructuredPatch_AuthorizerIdentitySourceRemove is the second
// concrete instance (alongside RestApi's "/description") of a top-level
// scalar PATCH "remove" that AWS documents as supported
// (patch-operations.html: UpdateAuthorizer "/identitySource" row).
func Test_ApplyStructuredPatch_AuthorizerIdentitySourceRemove(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()
	api, err := h.Backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "authz-remove-api"})
	require.NoError(t, err)

	auth, err := h.Backend.CreateAuthorizer(api.ID, apigateway.CreateAuthorizerInput{
		Name:           "my-auth",
		Type:           "TOKEN",
		AuthorizerURI:  "arn:aws:apigateway:us-east-1:lambda:path/functions/f/invocations",
		IdentitySource: "method.request.header.Authorization",
	})
	require.NoError(t, err)
	require.NotEmpty(t, auth.IdentitySource, "precondition")

	rec := restRequest(t, h, http.MethodPatch, fmt.Sprintf("/restapis/%s/authorizers/%s", api.ID, auth.ID),
		`[{"op":"remove","path":"/identitySource"}]`)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	got, err := h.Backend.GetAuthorizer(api.ID, auth.ID)
	require.NoError(t, err)
	assert.Empty(t, got.IdentitySource, "explicit remove must clear the field")
	assert.Equal(t, "my-auth", got.Name, "unrelated field untouched")
}

// Test_ApplyStructuredPatch_StageDocumentationVersion exercises the
// top-level "/documentationVersion" field (types.Stage.DocumentationVersion
// in the SDK), absent from gopherstack's Stage struct until this sweep.
func Test_ApplyStructuredPatch_StageDocumentationVersion(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()
	apiID, stageName := setupStage(t, h, nil, nil)

	rec := restRequest(t, h, http.MethodPatch, fmt.Sprintf("/restapis/%s/stages/%s", apiID, stageName),
		`[{"op":"replace","path":"/documentationVersion","value":"1.0.0"}]`)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	stage, err := h.Backend.GetStage(apiID, stageName)
	require.NoError(t, err)
	assert.Equal(t, "1.0.0", stage.DocumentationVersion)
}

// Test_ApplyStructuredPatch_StageCanaryStageVariableOverrides exercises
// "/canarySettings/stageVariableOverrides", AWS's one documented
// canarySettings path with no per-key wildcard row (unlike "/variables",
// which supports "/variables/{name}") -- so, per the AWS "UpdateStage"
// patch-operations reference, its Value is a JSON string whose CONTENTS are
// themselves a JSON-encoded {name: value} object, replacing the whole map at
// once rather than merging one key.
func Test_ApplyStructuredPatch_StageCanaryStageVariableOverrides(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()
	apiID, stageName := setupStage(t, h, nil, &apigateway.CanarySettings{
		DeploymentID:   "canary-depl",
		PercentTraffic: 10,
	})

	// PatchOperation.Value is always a JSON string on the wire; here that
	// string's own contents are a JSON object, so the value is
	// double-encoded: `"{\"apiKey\":\"canary-value\"}"`.
	body := `[{"op":"replace","path":"/canarySettings/stageVariableOverrides",` +
		`"value":"{\"apiKey\":\"canary-value\",\"other\":\"x\"}"}]`
	rec := restRequest(t, h, http.MethodPatch, fmt.Sprintf("/restapis/%s/stages/%s", apiID, stageName), body)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	stage, err := h.Backend.GetStage(apiID, stageName)
	require.NoError(t, err)
	require.NotNil(t, stage.CanarySettings)
	wantOverrides := map[string]string{"apiKey": "canary-value", "other": "x"}
	assert.Equal(t, wantOverrides, stage.CanarySettings.StageVariableOverrides)
	assert.Equal(t, "canary-depl", stage.CanarySettings.DeploymentID, "sibling canary field untouched")
	assert.InDelta(t, 10.0, stage.CanarySettings.PercentTraffic, 0.001, "sibling canary field untouched")
}

// Test_ApplyStructuredPatch_StageMethodSettingsCachingFields exercises the
// two MethodSetting fields entirely absent from gopherstack's struct until
// this sweep: "caching/dataEncrypted" (bool) and
// "caching/unauthorizedCacheControlHeaderStrategy" (string enum), both
// addressed the same per-route way as every other method-settings property.
func Test_ApplyStructuredPatch_StageMethodSettingsCachingFields(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()
	apiID, stageName := setupStage(t, h, nil, nil)

	body := `[
		{"op":"replace","path":"/~1pets/GET/caching/dataEncrypted","value":"true"},
		{"op":"replace","path":"/~1pets/GET/caching/unauthorizedCacheControlHeaderStrategy","value":"FAIL_WITH_403"}
	]`
	rec := restRequest(t, h, http.MethodPatch, fmt.Sprintf("/restapis/%s/stages/%s", apiID, stageName), body)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	stage, err := h.Backend.GetStage(apiID, stageName)
	require.NoError(t, err)
	require.Contains(t, stage.MethodSettings, "/pets/GET")
	ms := stage.MethodSettings["/pets/GET"]
	assert.True(t, ms.CacheDataEncrypted)
	assert.Equal(t, "FAIL_WITH_403", ms.UnauthorizedCacheControlHeaderStrategy)
}

// Test_ApplyStructuredPatch_UsagePlanPerRouteThrottle exercises
// "/apiStages/{restApiId}:{stage}/throttle/{resourcePath}/{httpMethod}[/rateLimit|burstLimit]",
// the per-route throttle override path within one usage-plan API stage
// (distinct from the whole-apiStage "/apiStages" membership path already
// covered by Test_ApplyStructuredPatch_UsagePlanAPIStages).
func Test_ApplyStructuredPatch_UsagePlanPerRouteThrottle(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()
	plan, err := h.Backend.CreateUsagePlan(apigateway.CreateUsagePlanInput{
		Name:      "route-throttle-plan",
		APIStages: []apigateway.APIStageAssociation{{RestAPIID: "api1", Stage: "prod"}},
	})
	require.NoError(t, err)

	setBody := `[
		{"op":"replace","path":"/apiStages/api1:prod/throttle/~1items/GET/rateLimit","value":"10.5"},
		{"op":"replace","path":"/apiStages/api1:prod/throttle/~1items/GET/burstLimit","value":"20"}
	]`
	rec := restRequest(t, h, http.MethodPatch, fmt.Sprintf("/usageplans/%s", plan.ID), setBody)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	got, err := h.Backend.GetUsagePlan(plan.ID)
	require.NoError(t, err)
	require.Len(t, got.APIStages, 1)
	require.NotNil(t, got.APIStages[0].Throttle)
	require.Contains(t, got.APIStages[0].Throttle, "GET /items")
	assert.InDelta(t, 10.5, got.APIStages[0].Throttle["GET /items"].RateLimit, 0.001)
	assert.Equal(t, 20, got.APIStages[0].Throttle["GET /items"].BurstLimit)

	removeBody := `[{"op":"remove","path":"/apiStages/api1:prod/throttle/~1items/GET"}]`
	rec = restRequest(t, h, http.MethodPatch, fmt.Sprintf("/usageplans/%s", plan.ID), removeBody)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	got, err = h.Backend.GetUsagePlan(plan.ID)
	require.NoError(t, err)
	require.Len(t, got.APIStages, 1)
	assert.NotContains(t, got.APIStages[0].Throttle, "GET /items", "remove drops the whole per-route entry")
}

// Test_ApplyStructuredPatch_APIKeyStages exercises UpdateApiKey's "/stages"
// add/remove (AWS "DEPRECATED FOR USAGE PLANS" but still real and
// wire-modeled), where Value is "{restApiId}/{stageName}" -- distinct from
// UsagePlan's "{restApiId}:{stage}" apiStages membership value format (":"
// not "/").
func Test_ApplyStructuredPatch_APIKeyStages(t *testing.T) {
	t.Parallel()

	h := newAPIGWHandler()
	apiID, stageName := setupStage(t, h, nil, nil)
	key, err := h.Backend.CreateAPIKey(apigateway.CreateAPIKeyInput{Name: "stage-patch-key", Enabled: true})
	require.NoError(t, err)

	stageValue := apiID + "/" + stageName
	addBody := fmt.Sprintf(`[{"op":"add","path":"/stages","value":%q}]`, stageValue)
	rec := restRequest(t, h, http.MethodPatch, "/apikeys/"+key.ID, addBody)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	got, err := h.Backend.GetAPIKey(key.ID)
	require.NoError(t, err)
	assert.Equal(t, []string{stageValue}, got.StageKeys)

	removeBody := fmt.Sprintf(`[{"op":"remove","path":"/stages","value":%q}]`, stageValue)
	rec = restRequest(t, h, http.MethodPatch, "/apikeys/"+key.ID, removeBody)
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	got, err = h.Backend.GetAPIKey(key.ID)
	require.NoError(t, err)
	assert.Empty(t, got.StageKeys)
}
