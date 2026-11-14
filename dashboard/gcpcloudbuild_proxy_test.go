package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
)

const (
	cbTestLoc     = "/dashboard/api/v1/gcp/cloudbuild/v1/projects/p/locations/global"
	cbTestParent  = "projects/p/locations/global"
	cbTestCipher  = "c3VwZXItc2VjcmV0LWNpcGhlcnRleHQ="
	cbTestPlain   = "super-secret-ciphertext"
	cbTestSubPass = "hunter2-substitution"
)

func newCloudBuildDashboard(t *testing.T) (*dashboard.DashboardHandler, string) {
	t.Helper()

	h := gcpcloudbuild.NewHandler(gcpcloudbuild.NewInMemoryBackend())

	trig, err := h.API().CreateBuildTrigger(t.Context(), &cloudbuildpb.CreateBuildTriggerRequest{
		Parent: cbTestParent,
		Trigger: &cloudbuildpb.BuildTrigger{
			Name:          "gated",
			Substitutions: map[string]string{"_API_TOKEN": cbTestSubPass, "_REGION": "us"},
			BuildTemplate: &cloudbuildpb.BuildTrigger_Build{Build: &cloudbuildpb.Build{
				Steps: []*cloudbuildpb.BuildStep{
					{Name: "busybox", Args: []string{"true"}, Env: []string{"DB_PASSWORD=hunter2", "MODE=ci"}},
				},
				Approval: &cloudbuildpb.BuildApproval{
					Config: &cloudbuildpb.ApprovalConfig{ApprovalRequired: true},
				},
				Substitutions: map[string]string{"_API_TOKEN": cbTestSubPass},
				Secrets: []*cloudbuildpb.Secret{{
					KmsKeyName: "projects/p/locations/global/keyRings/r/cryptoKeys/k",
					SecretEnv:  map[string][]byte{"TOKEN": []byte(cbTestPlain)},
				}},
				AvailableSecrets: &cloudbuildpb.Secrets{Inline: []*cloudbuildpb.InlineSecret{{
					KmsKeyName: "projects/p/locations/global/keyRings/r/cryptoKeys/k",
					EnvMap:     map[string][]byte{"INLINE": []byte(cbTestPlain)},
				}}},
			}},
		},
	})
	require.NoError(t, err)

	return dashboard.NewHandler(dashboard.Config{GCPCloudBuildOps: h}), trig.GetId()
}

func serveGCP(t *testing.T, h *dashboard.DashboardHandler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	require.NoError(t, h.Handler()(echo.NewContext(req, rec)))

	return rec
}

func TestGCPCloudBuildProxyMasksSecrets(t *testing.T) {
	t.Parallel()

	h, trigID := newCloudBuildDashboard(t)

	trig := serveGCP(t, h, http.MethodGet, cbTestLoc+"/triggers/"+trigID, "")
	require.Equal(t, http.StatusOK, trig.Code, trig.Body.String())

	run := serveGCP(t, h, http.MethodPost, cbTestLoc+"/triggers/"+trigID+":run", "{}")
	require.Equal(t, http.StatusOK, run.Code, run.Body.String())

	builds := serveGCP(t, h, http.MethodGet, cbTestLoc+"/builds", "")
	require.Equal(t, http.StatusOK, builds.Code, builds.Body.String())

	bodies := map[string]string{"trigger": trig.Body.String(), "run": run.Body.String(), "builds": builds.Body.String()}

	for name, body := range bodies {
		assert.NotContains(t, body, cbTestCipher, name)
		assert.NotContains(t, body, cbTestPlain, name)
		assert.NotContains(t, body, cbTestSubPass, name)
		assert.NotContains(t, body, "hunter2", name)
	}

	assert.Contains(t, trig.Body.String(), `"_REGION":"us"`)
	assert.Contains(t, trig.Body.String(), `"_API_TOKEN":"***"`)
	assert.Contains(t, builds.Body.String(), `"TOKEN"`)
	assert.Contains(t, builds.Body.String(), "MODE=ci")
}

func pendingBuild(t *testing.T) (*dashboard.DashboardHandler, string) {
	t.Helper()

	h, trigID := newCloudBuildDashboard(t)
	run := serveGCP(t, h, http.MethodPost, cbTestLoc+"/triggers/"+trigID+":run",
		`{"source":{"branchName":"main","projectId":"evil","dir":"/etc"}}`)
	require.Equal(t, http.StatusOK, run.Code, run.Body.String())

	list := serveGCP(t, h, http.MethodGet, cbTestLoc+"/builds", "")
	require.Equal(t, http.StatusOK, list.Code, list.Body.String())
	require.Contains(t, list.Body.String(), `"PENDING"`)

	return h, cbTestLoc + "/builds/" + extractJSONString(t, list.Body.String(), `"id":"`)
}

func TestGCPCloudBuildProxyApproval(t *testing.T) {
	t.Parallel()

	const reject = `{"approvalResult":{"decision":"REJECTED","comment":"no","approverAccount":"x@y"}}`

	tests := []struct {
		name     string
		prior    string
		body     string
		wantBody string
		wantCode int
	}{
		{"missing decision", "", `{}`, "decision must be", http.StatusBadRequest},
		{
			"unspecified decision",
			"",
			`{"approvalResult":{"decision":"DECISION_UNSPECIFIED"}}`,
			"decision must be",
			http.StatusBadRequest,
		},
		{
			"long comment",
			"",
			`{"approvalResult":{"decision":"APPROVED","comment":"` + strings.Repeat("x", 1001) + `"}}`,
			"comment is too long",
			http.StatusBadRequest,
		},
		{"bad body", "", `{bad`, "invalid request body", http.StatusBadRequest},
		{"reject", "", reject, "operations", http.StatusOK},
		{"approve", "", `{"approvalResult":{"decision":"APPROVED"}}`, "operations", http.StatusOK},
		{
			"already decided",
			reject,
			`{"approvalResult":{"decision":"APPROVED"}}`,
			"not awaiting approval",
			http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, build := pendingBuild(t)

			if tt.prior != "" {
				require.Equal(t, http.StatusOK, serveGCP(t, h, http.MethodPost, build+":approve", tt.prior).Code)
			}

			rec := serveGCP(t, h, http.MethodPost, build+":approve", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tt.wantBody)
			assert.NotContains(t, rec.Body.String(), "x@y")
		})
	}
}

func extractJSONString(t *testing.T, body, key string) string {
	t.Helper()

	_, rest, ok := strings.Cut(body, key)
	require.True(t, ok, body)

	v, _, _ := strings.Cut(rest, `"`)

	return v
}

func TestGCPCloudBuildProxyRoutes(t *testing.T) {
	t.Parallel()

	h, trigID := newCloudBuildDashboard(t)
	trig := cbTestLoc + "/triggers/" + trigID

	tests := []struct {
		name     string
		method   string
		path     string
		body     string
		wantBody string
		wantCode int
	}{
		{"pools list", http.MethodGet, cbTestLoc + "/workerPools", "", "{", http.StatusOK},
		{"pool missing", http.MethodGet, cbTestLoc + "/workerPools/nope", "", "NOT_FOUND", http.StatusNotFound},
		{"pool create refused", http.MethodPost, cbTestLoc + "/workerPools", "{}", "unsupported", http.StatusNotFound},
		{
			"pool delete refused",
			http.MethodDelete,
			cbTestLoc + "/workerPools/p",
			"",
			"unsupported",
			http.StatusNotFound,
		},
		{"trigger list", http.MethodGet, cbTestLoc + "/triggers", "", `"gated"`, http.StatusOK},
		{"trigger create refused", http.MethodPost, cbTestLoc + "/triggers", "{}", "unsupported", http.StatusNotFound},
		{"trigger patch refused", http.MethodPatch, trig, "{}", "unsupported", http.StatusNotFound},
		{"build create refused", http.MethodPost, cbTestLoc + "/builds", "{}", "unsupported", http.StatusNotFound},
		{
			"bad substitution key",
			http.MethodPost,
			trig + ":run",
			`{"source":{"substitutions":{"bad key":"v"}}}`,
			"invalid substitution",
			http.StatusBadRequest,
		},
		{
			"bad substitution value",
			http.MethodPost,
			trig + ":run",
			`{"source":{"substitutions":{"_A":"` + strings.Repeat("v", 1025) + `"}}}`,
			"invalid substitution",
			http.StatusBadRequest,
		},
		{"unknown verb", http.MethodPost, trig + ":bogus", "", "unsupported", http.StatusNotFound},
		{"build bogus verb", http.MethodPost, cbTestLoc + "/builds/b1:bogus", "", "unsupported", http.StatusNotFound},
		{"build missing", http.MethodGet, cbTestLoc + "/builds/nope", "", "NOT_FOUND", http.StatusNotFound},
		{"cancel missing", http.MethodPost, cbTestLoc + "/builds/nope:cancel", "", "NOT_FOUND", http.StatusNotFound},
		{"other kind", http.MethodGet, cbTestLoc + "/other", "", "unsupported", http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := serveGCP(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestGCPCloudBuildProxyUnavailable(t *testing.T) {
	t.Parallel()

	rec := serveGCP(t, dashboard.NewHandler(dashboard.Config{}), http.MethodGet, cbTestLoc+"/builds", "")
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}
