package cloudfront_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestContinuousDeploymentPolicy_TrafficConfig verifies the full request shape
// (multiple staging DNS names plus a SingleWeight or SingleHeader traffic config) round-trips
// through Create/Get/Update, and that ARN/LastModifiedTime are populated.
func TestContinuousDeploymentPolicy_TrafficConfig(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")

	weightTraffic := cloudfront.ContinuousDeploymentTrafficConfig{
		Type: "SingleWeight",
		SingleWeightConfig: &cloudfront.ContinuousDeploymentSingleWeightConfig{
			Weight: 0.15,
			SessionStickinessConfig: &cloudfront.ContinuousDeploymentSessionStickinessConfig{
				IdleTTL:    300,
				MaximumTTL: 600,
			},
		},
	}

	policy, err := b.CreateContinuousDeploymentPolicyWithConfig(
		true, []string{"staging-a.example.com", "staging-b.example.com"}, weightTraffic,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, policy.ARN)
	assert.NotEmpty(t, policy.LastModifiedTime)
	assert.Equal(t, []string{"staging-a.example.com", "staging-b.example.com"}, policy.StagingDistributionDNSNames)
	require.NotNil(t, policy.TrafficConfig.SingleWeightConfig)
	assert.InDelta(t, 0.15, policy.TrafficConfig.SingleWeightConfig.Weight, 0.0001)
	require.NotNil(t, policy.TrafficConfig.SingleWeightConfig.SessionStickinessConfig)
	assert.Equal(t, int32(300), policy.TrafficConfig.SingleWeightConfig.SessionStickinessConfig.IdleTTL)

	got, err := b.GetContinuousDeploymentPolicy(policy.ID)
	require.NoError(t, err)
	assert.Equal(t, policy.ARN, got.ARN)
	assert.Equal(t, "SingleWeight", got.TrafficConfig.Type)

	headerTraffic := cloudfront.ContinuousDeploymentTrafficConfig{
		Type: "SingleHeader",
		SingleHeaderConfig: &cloudfront.ContinuousDeploymentSingleHeaderConfig{
			Header: "aws-cf-cd-staging",
			Value:  "true",
		},
	}
	updated, err := b.UpdateContinuousDeploymentPolicyWithConfig(
		policy.ID, false, []string{"staging-c.example.com"}, headerTraffic,
	)
	require.NoError(t, err)
	assert.False(t, updated.Enabled)
	assert.Equal(t, []string{"staging-c.example.com"}, updated.StagingDistributionDNSNames)
	require.NotNil(t, updated.TrafficConfig.SingleHeaderConfig)
	assert.Equal(t, "aws-cf-cd-staging", updated.TrafficConfig.SingleHeaderConfig.Header)
	assert.Nil(t, updated.TrafficConfig.SingleWeightConfig, "full replace should drop the old SingleWeightConfig")
}

// TestContinuousDeploymentPolicy_IfMatchEnforcement mirrors the trust store If-Match
// test for the continuous deployment policy update/delete handlers.
func TestContinuousDeploymentPolicy_IfMatchEnforcement(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	const prefix = "/2020-05-31/"

	createRec := doXML(t, h, http.MethodPost, prefix+"continuous-deployment-policy",
		[]byte(`<ContinuousDeploymentPolicyConfig><Enabled>true</Enabled></ContinuousDeploymentPolicyConfig>`))
	require.Equal(t, http.StatusCreated, createRec.Code)
	id := extractXMLID(t, createRec.Body.String())
	etag := createRec.Header().Get("ETag")
	require.NotEmpty(t, etag)

	badUpdate := doXMLWithHeaders(t, h, http.MethodPut, prefix+"continuous-deployment-policy/"+id,
		[]byte(`<ContinuousDeploymentPolicyConfig><Enabled>false</Enabled></ContinuousDeploymentPolicyConfig>`),
		map[string]string{"If-Match": "bogus-etag"})
	assert.Equal(t, http.StatusPreconditionFailed, badUpdate.Code)

	goodUpdate := doXMLWithHeaders(t, h, http.MethodPut, prefix+"continuous-deployment-policy/"+id,
		[]byte(`<ContinuousDeploymentPolicyConfig><Enabled>false</Enabled></ContinuousDeploymentPolicyConfig>`),
		map[string]string{"If-Match": etag})
	require.Equal(t, http.StatusOK, goodUpdate.Code)
	newETag := goodUpdate.Header().Get("ETag")
	assert.NotEmpty(t, newETag)
	assert.NotEqual(t, etag, newETag)

	badDelete := doXMLWithHeaders(t, h, http.MethodDelete, prefix+"continuous-deployment-policy/"+id, nil,
		map[string]string{"If-Match": "bogus-etag"})
	assert.Equal(t, http.StatusPreconditionFailed, badDelete.Code)

	goodDelete := doXMLWithHeaders(t, h, http.MethodDelete, prefix+"continuous-deployment-policy/"+id, nil,
		map[string]string{"If-Match": newETag})
	assert.Equal(t, http.StatusNoContent, goodDelete.Code)
}

// TestListContinuousDeploymentPolicies_RealClient drives ListContinuousDeploymentPolicies
// through the real aws-sdk-go-v2 CloudFront client. The real deserializer
// (awsRestxml_deserializeDocumentContinuousDeploymentPolicySummary, case
// "ContinuousDeploymentPolicy") wraps a single nested ContinuousDeploymentPolicy child in each
// summary; a summary with Id/Enabled flattened directly onto it (the pre-fix shape, confirmed
// by hand-reverting) decodes to a Summary.ContinuousDeploymentPolicy that is nil for every item
// -- the right item count, entirely blank content.
func TestListContinuousDeploymentPolicies_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	created, err := h.Backend.CreateContinuousDeploymentPolicy(true, "staging.example.com")
	require.NoError(t, err)

	client := newTestCloudFrontClient(t, h)

	listed, err := client.ListContinuousDeploymentPolicies(
		t.Context(), &cfsdk.ListContinuousDeploymentPoliciesInput{},
	)
	require.NoError(t, err)
	require.NotNil(t, listed.ContinuousDeploymentPolicyList)
	require.Len(t, listed.ContinuousDeploymentPolicyList.Items, 1)
	item := listed.ContinuousDeploymentPolicyList.Items[0]
	require.NotNil(t, item.ContinuousDeploymentPolicy)
	assert.Equal(t, created.ID, aws.ToString(item.ContinuousDeploymentPolicy.Id))
}

// ---------------------------------------------------------------------------
// MonitoringSubscription
// ---------------------------------------------------------------------------

func TestContinuousDeploymentPolicyXMLIncludesStagingDNS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stagingDNS string
		wantInXML  bool
	}{
		{name: "with_staging_dns_emits_element", stagingDNS: "abc.cloudfront.net", wantInXML: true},
		{name: "without_staging_dns_omits_element", stagingDNS: "", wantInXML: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			policy, err := b.CreateContinuousDeploymentPolicy(true, tc.stagingDNS)
			require.NoError(t, err)

			h := cloudfront.NewHandler(b)
			rec := doXML(t, h, http.MethodGet,
				"/2020-05-31/continuous-deployment-policy/"+policy.ID, nil)
			require.Equal(t, http.StatusOK, rec.Code, tc.name)

			// StagingDistributionDnsNames is a required element of ContinuousDeploymentPolicyConfig
			// in the real API (it always carries a Quantity, 0 or more DNS names), so the wrapper
			// element is always present; only its contents vary.
			assert.Contains(t, rec.Body.String(), "<StagingDistributionDnsNames>", tc.name)
			if tc.wantInXML {
				assert.Contains(t, rec.Body.String(), tc.stagingDNS, tc.name)
				assert.Contains(t, rec.Body.String(), "<Quantity>1</Quantity>", tc.name)
			} else {
				assert.Contains(t, rec.Body.String(), "<Quantity>0</Quantity>", tc.name)
			}
		})
	}
}

func extractXMLTag(t *testing.T, body string) string {
	t.Helper()

	const tag = "Id"
	open := "<" + tag + ">"
	closeTag := "</" + tag + ">"

	start := len(body)
	for i := range len(body) {
		if i+len(open) <= len(body) && body[i:i+len(open)] == open {
			start = i + len(open)

			break
		}
	}

	if start == len(body) {
		return ""
	}

	for i := start; i <= len(body)-len(closeTag); i++ {
		if body[i:i+len(closeTag)] == closeTag {
			return body[start:i]
		}
	}

	return ""
}

// TestContinuousDeploymentPolicyCRUD covers get, list, update, delete (create is already tested).
func TestContinuousDeploymentPolicyCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*testing.T, *cloudfront.Handler) string
		check       func(*testing.T, *httptest.ResponseRecorder, string)
		headersFunc func(*testing.T, *cloudfront.Handler, string) map[string]string
		name        string
		method      string
		path        string
		body        []byte
		wantStatus  int
	}{
		{
			name:   "list_continuous_deployment_policies",
			method: http.MethodGet,
			path:   "/2020-05-31/continuous-deployment-policy",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				_, err := h.Backend.CreateContinuousDeploymentPolicy(true, "staging.example.com")
				require.NoError(t, err)

				return ""
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<ContinuousDeploymentPolicyList")
				assert.Contains(t, rec.Body.String(), "<Quantity>1</Quantity>")
			},
		},
		{
			name:   "get_continuous_deployment_policy",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateContinuousDeploymentPolicy(true, "staging.example.com")
				require.NoError(t, err)

				return "/2020-05-31/continuous-deployment-policy/" + p.ID
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<ContinuousDeploymentPolicy")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "get_continuous_deployment_policy_config",
			method: http.MethodGet,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateContinuousDeploymentPolicy(false, "")
				require.NoError(t, err)

				return "/2020-05-31/continuous-deployment-policy/" + p.ID + "/config"
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<ContinuousDeploymentPolicy")
			},
		},
		{
			name:   "update_continuous_deployment_policy",
			method: http.MethodPut,
			path:   "",
			body: []byte(
				`<ContinuousDeploymentPolicyConfig><Enabled>false</Enabled></ContinuousDeploymentPolicyConfig>`,
			),
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateContinuousDeploymentPolicy(true, "staging.example.com")
				require.NoError(t, err)

				return "/2020-05-31/continuous-deployment-policy/" + p.ID
			},
			headersFunc: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				parts := strings.Split(strings.TrimRight(path, "/"), "/")
				id := parts[len(parts)-1]
				p, err := h.Backend.GetContinuousDeploymentPolicy(id)
				require.NoError(t, err)

				return map[string]string{"If-Match": p.ETag}
			},
			wantStatus: http.StatusOK,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<ContinuousDeploymentPolicy")
				assert.NotEmpty(t, rec.Header().Get("ETag"))
			},
		},
		{
			name:   "delete_continuous_deployment_policy",
			method: http.MethodDelete,
			path:   "",
			body:   nil,
			setup: func(t *testing.T, h *cloudfront.Handler) string {
				t.Helper()
				p, err := h.Backend.CreateContinuousDeploymentPolicy(true, "staging.example.com")
				require.NoError(t, err)

				return "/2020-05-31/continuous-deployment-policy/" + p.ID
			},
			headersFunc: func(t *testing.T, h *cloudfront.Handler, path string) map[string]string {
				t.Helper()
				parts := strings.Split(strings.TrimRight(path, "/"), "/")
				id := parts[len(parts)-1]
				p, err := h.Backend.GetContinuousDeploymentPolicy(id)
				require.NoError(t, err)

				return map[string]string{"If-Match": p.ETag}
			},
			wantStatus: http.StatusNoContent,
			check:      nil,
		},
		{
			name:   "get_continuous_deployment_policy_not_found",
			method: http.MethodGet,
			path:   "/2020-05-31/continuous-deployment-policy/doesnotexist",
			body:   nil,
			setup: func(t *testing.T, _ *cloudfront.Handler) string {
				t.Helper()

				return ""
			},
			wantStatus: http.StatusNotFound,
			check: func(t *testing.T, rec *httptest.ResponseRecorder, _ string) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<Error>")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			path := tt.path
			if tt.setup != nil {
				if p := tt.setup(t, h); p != "" {
					path = p
				}
			}

			var hdrs map[string]string
			if tt.headersFunc != nil {
				hdrs = tt.headersFunc(t, h, path)
			}
			rec := doXMLWithHeaders(t, h, tt.method, path, tt.body, hdrs)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.check != nil {
				tt.check(t, rec, path)
			}
		})
	}
}

// TestInMemoryBackend_ContinuousDeploymentPolicy tests CDP operations directly on the backend.
func TestInMemoryBackend_ContinuousDeploymentPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *cloudfront.InMemoryBackend)
		name string
	}{
		{
			name: "get_cdp",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				p, err := b.CreateContinuousDeploymentPolicy(true, "staging.example.com")
				require.NoError(t, err)

				got, err := b.GetContinuousDeploymentPolicy(p.ID)
				require.NoError(t, err)
				assert.True(t, got.Enabled)
				assert.Equal(t, "staging.example.com", got.StagingDistributionDNS)
			},
		},
		{
			name: "list_cdp",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateContinuousDeploymentPolicy(true, "")
				require.NoError(t, err)
				_, err = b.CreateContinuousDeploymentPolicy(false, "")
				require.NoError(t, err)

				list := b.ListContinuousDeploymentPolicies()
				assert.Len(t, list, 2)
			},
		},
		{
			name: "update_cdp",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				p, err := b.CreateContinuousDeploymentPolicy(true, "staging.example.com")
				require.NoError(t, err)

				updated, err := b.UpdateContinuousDeploymentPolicy(p.ID, false, "new.example.com")
				require.NoError(t, err)
				assert.False(t, updated.Enabled)
				assert.Equal(t, "new.example.com", updated.StagingDistributionDNS)
			},
		},
		{
			name: "delete_cdp",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				p, err := b.CreateContinuousDeploymentPolicy(true, "")
				require.NoError(t, err)
				require.NoError(t, b.DeleteContinuousDeploymentPolicy(p.ID))

				_, err = b.GetContinuousDeploymentPolicy(p.ID)
				require.Error(t, err)
			},
		},
		{
			name: "get_cdp_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.GetContinuousDeploymentPolicy("doesnotexist")
				require.Error(t, err)
			},
		},
		{
			name: "update_cdp_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				_, err := b.UpdateContinuousDeploymentPolicy("doesnotexist", false, "")
				require.Error(t, err)
			},
		},
		{
			name: "delete_cdp_not_found",
			run: func(t *testing.T, b *cloudfront.InMemoryBackend) {
				t.Helper()
				err := b.DeleteContinuousDeploymentPolicy("doesnotexist")
				require.Error(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
			tt.run(t, b)
		})
	}
}

// TestCreateContinuousDeploymentPolicy covers the CreateContinuousDeploymentPolicy operation.
func TestCreateContinuousDeploymentPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check      func(*testing.T, *httptest.ResponseRecorder)
		name       string
		body       []byte
		wantStatus int
	}{
		{
			name: "create_continuous_deployment_policy_enabled",
			body: []byte(
				`<ContinuousDeploymentPolicyConfig><Enabled>true</Enabled></ContinuousDeploymentPolicyConfig>`,
			),
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<ContinuousDeploymentPolicy")
				assert.Contains(t, rec.Body.String(), "<Enabled>true</Enabled>")
				assert.NotEmpty(t, rec.Header().Get("Location"))
			},
		},
		{
			name: "create_continuous_deployment_policy_disabled",
			body: []byte(
				`<ContinuousDeploymentPolicyConfig><Enabled>false</Enabled></ContinuousDeploymentPolicyConfig>`,
			),
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<ContinuousDeploymentPolicy")
				assert.Contains(t, rec.Body.String(), "<Enabled>false</Enabled>")
			},
		},
		{
			name:       "create_continuous_deployment_policy_no_body",
			body:       nil,
			wantStatus: http.StatusCreated,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "<ContinuousDeploymentPolicy")
			},
		},
		{
			name:       "create_continuous_deployment_policy_malformed_xml",
			body:       []byte(`<<<not xml`),
			wantStatus: http.StatusBadRequest,
			check: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Contains(t, rec.Body.String(), "MalformedXML")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXML(t, h, http.MethodPost, "/2020-05-31/continuous-deployment-policy", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			tt.check(t, rec)
		})
	}
}
