package accessanalyzer_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

// doRequest fires a request at the handler and returns the recorder.
func doRequest(t *testing.T, h *accessanalyzer.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error

		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))

	if len(bodyBytes) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// mustAnalyzer creates an analyzer and returns its ARN.
func mustAnalyzer(t *testing.T, b *accessanalyzer.InMemoryBackend, name string) string {
	t.Helper()

	a, err := b.CreateAnalyzer(name, accessanalyzer.AnalyzerTypeAccount, nil)
	require.NoError(t, err)

	return a.Arn
}

// mustFinding creates a finding for an analyzer and returns its ID.
func mustFinding(t *testing.T, b *accessanalyzer.InMemoryBackend, analyzerName string) string {
	t.Helper()

	f, err := b.AddFinding(analyzerName, "AWS::S3::Bucket", "arn:aws:s3:::bucket", nil, nil, nil)
	require.NoError(t, err)

	return f.ID
}

// TestApplyArchiveRule verifies ApplyArchiveRule archives active findings.
func TestApplyArchiveRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       map[string]any
		setupFn    func(b *accessanalyzer.InMemoryBackend) string // returns analyzerArn
		wantStatus int
	}{
		{
			name: "archives_active_findings",
			body: map[string]any{"ruleName": "my-rule"},
			setupFn: func(b *accessanalyzer.InMemoryBackend) string {
				arn := mustAnalyzer(t, b, "arch-analyzer")
				_, _ = b.CreateArchiveRule("arch-analyzer", "my-rule", nil)
				_, _ = b.AddFinding("arch-analyzer", "AWS::S3::Bucket", "arn:aws:s3:::b", nil, nil, nil)

				return arn
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found_returns_404",
			body: map[string]any{
				"analyzerArn": "arn:aws:access-analyzer:us-east-1:000000000000:analyzer/missing",
				"ruleName":    "r",
			},
			setupFn: func(_ *accessanalyzer.InMemoryBackend) string {
				return ""
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)
			arn := tt.setupFn(b)

			if arn != "" {
				tt.body["analyzerArn"] = arn
			}

			rec := doRequest(t, h, http.MethodPut, "/archive-rule", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestPolicyGenerationLifecycle verifies Start/Get/Cancel/List policy generation.
func TestPolicyGenerationLifecycle(t *testing.T) {
	t.Parallel()

	b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
	h := accessanalyzer.NewHandler(b)

	tests := []struct {
		name    string
		runFn   func() *httptest.ResponseRecorder
		checkFn func(t *testing.T, rec *httptest.ResponseRecorder)
	}{
		{
			name: "start_policy_generation",
			runFn: func() *httptest.ResponseRecorder {
				return doRequest(t, h, http.MethodPut, "/policy/generation", map[string]any{
					"policyGenerationDetails": map[string]any{"principalArn": "arn:aws:iam::000000000000:role/MyRole"},
				})
			},
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["jobId"])
			},
		},
		{
			name: "list_policy_generations",
			runFn: func() *httptest.ResponseRecorder {
				// start one first
				doRequest(t, h, http.MethodPut, "/policy/generation", map[string]any{
					"policyGenerationDetails": map[string]any{"principalArn": "arn:aws:iam::000000000000:role/R"},
				})

				return doRequest(t, h, http.MethodGet, "/policy/generation", nil)
			},
			checkFn: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["policyGenerations"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := tt.runFn()
			tt.checkFn(t, rec)
		})
	}
}

// TestGetGeneratedPolicy verifies getting a generated policy by jobId.
func TestGetGeneratedPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		missingJob bool
	}{
		{name: "existing_job", wantStatus: http.StatusOK},
		{name: "missing_job", wantStatus: http.StatusNotFound, missingJob: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)

			jobID := "missing-job-id"

			if !tt.missingJob {
				rec := doRequest(t, h, http.MethodPut, "/policy/generation", map[string]any{
					"policyGenerationDetails": map[string]any{"principalArn": "arn:aws:iam::000000000000:role/R"},
				})
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				jobID = resp["jobId"]
			}

			rec := doRequest(t, h, http.MethodGet, "/policy/generation/"+jobID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestCancelPolicyGeneration verifies PUT /policy/generation/{jobId} cancels job.
func TestCancelPolicyGeneration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		missingJob bool
	}{
		{name: "cancel_existing", wantStatus: http.StatusOK},
		{name: "cancel_missing", wantStatus: http.StatusNotFound, missingJob: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)

			jobID := "no-such-job"

			if !tt.missingJob {
				rec := doRequest(t, h, http.MethodPut, "/policy/generation", map[string]any{
					"policyGenerationDetails": map[string]any{"principalArn": "arn:aws:iam::000000000000:role/R"},
				})
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				jobID = resp["jobId"]
			}

			rec := doRequest(t, h, http.MethodPut, "/policy/generation/"+jobID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestCheckPolicyOps verifies Check* operations return PASS.
func TestCheckPolicyOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		body map[string]any
	}{
		{
			name: "check_access_not_granted",
			path: "/policy/check-access-not-granted",
			body: map[string]any{
				"access":         []any{},
				"policyDocument": `{"Version":"2012-10-17","Statement":[]}`,
				"policyType":     "IDENTITY_POLICY",
			},
		},
		{
			name: "check_no_new_access",
			path: "/policy/check-no-new-access",
			body: map[string]any{
				"existingPolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
				"newPolicyDocument":      `{"Version":"2012-10-17","Statement":[]}`,
				"policyType":             "IDENTITY_POLICY",
			},
		},
		{
			name: "check_no_public_access",
			path: "/policy/check-no-public-access",
			body: map[string]any{
				"policyDocument": `{"Version":"2012-10-17","Statement":[]}`,
				"resourceType":   "AWS::S3::Bucket",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, tt.path, tt.body)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "PASS", resp["result"])
		})
	}
}

// TestValidatePolicy verifies POST /policy/validation returns empty findings.
func TestValidatePolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/policy/validation", map[string]any{
		"policyDocument": `{"Version":"2012-10-17","Statement":[]}`,
		"policyType":     "IDENTITY_POLICY",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["findings"])
}

// TestAccessPreviewLifecycle verifies Create/Get/List/ListFindings for access previews.
func TestAccessPreviewLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler)
	}{
		{
			name: "create_and_get",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "preview-create")

				rec := doRequest(t, h, http.MethodPut, "/access-preview", map[string]any{
					"analyzerArn":    arn,
					"configurations": map[string]any{},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var created map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
				previewID := created["id"]
				require.NotEmpty(t, previewID)

				rec2 := doRequest(t, h, http.MethodGet, "/access-preview/"+previewID+"?analyzerArn="+arn, nil)
				assert.Equal(t, http.StatusOK, rec2.Code)

				var got map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &got))
				ap := got["accessPreview"].(map[string]any)
				assert.Equal(t, previewID, ap["id"])
				assert.Equal(t, "COMPLETED", ap["status"])
			},
		},
		{
			name: "list_access_previews",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "preview-list")

				doRequest(t, h, http.MethodPut, "/access-preview", map[string]any{
					"analyzerArn": arn, "configurations": map[string]any{},
				})

				rec := doRequest(t, h, http.MethodGet, "/access-preview?analyzerArn="+arn, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Len(t, resp["accessPreviews"], 1)
			},
		},
		{
			name: "list_access_preview_findings",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "preview-findings")
				mustFinding(t, b, "preview-findings")

				rec := doRequest(t, h, http.MethodPut, "/access-preview", map[string]any{
					"analyzerArn": arn, "configurations": map[string]any{},
				})
				var created map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
				previewID := created["id"]

				rec2 := doRequest(t, h, http.MethodPost, "/access-preview/"+previewID, map[string]any{
					"analyzerArn": arn,
				})
				assert.Equal(t, http.StatusOK, rec2.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
				findings := resp["findings"].([]any)
				assert.Len(t, findings, 1)
			},
		},
		{
			name: "get_missing_preview",
			fn: func(t *testing.T, _ *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				rec := doRequest(t, h, http.MethodGet, "/access-preview/no-such-id", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)
			tt.fn(t, b, h)
		})
	}
}

// TestServiceLinkedAnalyzerLifecycle verifies Create/Delete service-linked analyzer.
func TestServiceLinkedAnalyzerLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler)
	}{
		{
			name: "create_service_linked_analyzer",
			fn: func(t *testing.T, _ *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				rec := doRequest(t, h, http.MethodPut, "/service-linked-analyzer", map[string]any{
					"type": "ACCOUNT_UNUSED_ACCESS",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.True(t, strings.HasPrefix(resp["arn"], "arn:aws:access-analyzer:"))
			},
		},
		{
			name: "delete_service_linked_analyzer",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				mustAnalyzer(t, b, "sla-to-delete")

				rec := doRequest(t, h, http.MethodDelete, "/service-linked-analyzer/sla-to-delete", nil)
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "delete_missing_analyzer",
			fn: func(t *testing.T, _ *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				rec := doRequest(t, h, http.MethodDelete, "/service-linked-analyzer/no-such", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)
			tt.fn(t, b, h)
		})
	}
}

// TestFindingRecommendationLifecycle verifies Generate/Get finding recommendation.
func TestFindingRecommendationLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler)
	}{
		{
			name: "generate_and_get",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "rec-analyzer")
				findingID := mustFinding(t, b, "rec-analyzer")

				rec := doRequest(t, h, http.MethodPost, "/recommendation/"+findingID, map[string]any{
					"analyzerArn": arn,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec2 := doRequest(t, h, http.MethodGet, "/recommendation/"+findingID+"?analyzerArn="+arn, nil)
				assert.Equal(t, http.StatusOK, rec2.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["recommendationType"])
			},
		},
		{
			name: "get_missing_recommendation",
			fn: func(t *testing.T, _ *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				rec := doRequest(t, h, http.MethodGet, "/recommendation/no-such-id?analyzerArn=arn", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)
			tt.fn(t, b, h)
		})
	}
}

// TestAnalyzedResourceLifecycle verifies Get/List analyzed resources.
func TestAnalyzedResourceLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler)
	}{
		{
			name: "get_analyzed_resource",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "ar-get-analyzer")
				resourceArn := "arn:aws:s3:::my-bucket"

				_, err := b.AddAnalyzedResource(arn, resourceArn, "AWS::S3::Bucket", false)
				require.NoError(t, err)

				rec := doRequest(t, h, http.MethodGet,
					"/analyzed-resource?analyzerArn="+arn+"&resourceArn="+resourceArn, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				resource := resp["resource"].(map[string]any)
				assert.Equal(t, resourceArn, resource["resourceArn"])
			},
		},
		{
			name: "get_missing_resource",
			fn: func(t *testing.T, _ *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				rec := doRequest(
					t,
					h,
					http.MethodGet,
					"/analyzed-resource?analyzerArn=arn:aws:access-analyzer:us-east-1:000000000000:analyzer/a&resourceArn=arn:aws:s3:::no-bucket",
					nil,
				)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "list_analyzed_resources",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "ar-list-analyzer")

				_, _ = b.AddAnalyzedResource(arn, "arn:aws:s3:::bucket1", "AWS::S3::Bucket", false)
				_, _ = b.AddAnalyzedResource(arn, "arn:aws:s3:::bucket2", "AWS::S3::Bucket", true)

				rec := doRequest(t, h, http.MethodPost, "/analyzed-resource", map[string]any{
					"analyzerArn": arn,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				resources := resp["analyzedResources"].([]any)
				assert.Len(t, resources, 2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)
			tt.fn(t, b, h)
		})
	}
}

// TestFindingV2Lifecycle verifies GetFindingV2 and ListFindingsV2.
func TestFindingV2Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler)
	}{
		{
			name: "get_finding_v2",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "fv2-get-analyzer")
				findingID := mustFinding(t, b, "fv2-get-analyzer")

				rec := doRequest(t, h, http.MethodGet, "/findingv2/"+findingID+"?analyzerArn="+arn, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, findingID, resp["id"])
			},
		},
		{
			name: "list_findings_v2",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "fv2-list-analyzer")
				mustFinding(t, b, "fv2-list-analyzer")
				mustFinding(t, b, "fv2-list-analyzer")

				rec := doRequest(t, h, http.MethodPost, "/findingv2", map[string]any{
					"analyzerArn": arn,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				findings := resp["findings"].([]any)
				assert.Len(t, findings, 2)
			},
		},
		{
			name: "get_missing_finding_v2",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "fv2-miss-analyzer")

				rec := doRequest(t, h, http.MethodGet, "/findingv2/no-such-id?analyzerArn="+arn, nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)
			tt.fn(t, b, h)
		})
	}
}

// TestGetFindingsStatistics verifies POST /analyzer/findings/statistics.
func TestGetFindingsStatistics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupFn    func(b *accessanalyzer.InMemoryBackend) string
		wantStatus int
		wantActive int
	}{
		{
			name: "returns_counts",
			setupFn: func(b *accessanalyzer.InMemoryBackend) string {
				arn := mustAnalyzer(t, b, "stats-analyzer")
				mustFinding(t, b, "stats-analyzer")
				mustFinding(t, b, "stats-analyzer")

				return arn
			},
			wantStatus: http.StatusOK,
			wantActive: 2,
		},
		{
			name: "missing_analyzer",
			setupFn: func(_ *accessanalyzer.InMemoryBackend) string {
				return "arn:aws:access-analyzer:us-east-1:000000000000:analyzer/missing"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)
			arn := tt.setupFn(b)

			rec := doRequest(t, h, http.MethodPost, "/analyzer/findings/statistics", map[string]any{
				"analyzerArn": arn,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK && tt.wantActive > 0 {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				stats := resp["findingsStatistics"].([]any)
				require.Len(t, stats, 1)

				extStats := stats[0].(map[string]any)["externalAccessFindingsStatistics"].(map[string]any)
				active := extStats["activeFindings"].(map[string]any)
				assert.Equal(t, float64(tt.wantActive), active["total"])
			}
		})
	}
}

// TestUpdateAnalyzer verifies PUT /analyzer/{name} updates an analyzer.
func TestUpdateAnalyzer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupFn    func(b *accessanalyzer.InMemoryBackend) string
		wantStatus int
	}{
		{
			name: "update_existing",
			setupFn: func(b *accessanalyzer.InMemoryBackend) string {
				mustAnalyzer(t, b, "upd-analyzer")

				return "upd-analyzer"
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "update_missing",
			setupFn: func(_ *accessanalyzer.InMemoryBackend) string {
				return "no-such-analyzer"
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)
			name := tt.setupFn(b)

			rec := doRequest(t, h, http.MethodPut, "/analyzer/"+name, map[string]any{
				"configuration": map[string]any{},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
