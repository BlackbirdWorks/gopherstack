package dashboard_test

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/gcpartifactregistry"
)

type upstreamPassword = artifactregistrypb.RemoteRepositoryConfig_UpstreamCredentials_UsernamePasswordCredentials

const (
	arTestRoot   = "/dashboard/api/v1/gcp/artifactregistry"
	arTestParent = "projects/p/locations/us-central1"
	arTestRepo   = arTestRoot + "/v1/" + arTestParent + "/repositories/gen"
)

func newArtifactRegistryDashboard(t *testing.T) *dashboard.DashboardHandler {
	t.Helper()

	ar := gcpartifactregistry.NewHandler(gcpartifactregistry.NewInMemoryBackend())

	_, err := ar.Backend.CreateRepository(&artifactregistrypb.CreateRepositoryRequest{
		Parent: arTestParent, RepositoryId: "gen",
		Repository: &artifactregistrypb.Repository{
			Format:              artifactregistrypb.Repository_GENERIC,
			CleanupPolicyDryRun: true,
			CleanupPolicies: map[string]*artifactregistrypb.CleanupPolicy{"old": {
				Id: "old", Action: artifactregistrypb.CleanupPolicy_DELETE,
				ConditionType: &artifactregistrypb.CleanupPolicy_Condition{
					Condition: &artifactregistrypb.CleanupPolicyCondition{OlderThan: durationpb.New(time.Nanosecond)},
				},
			}},
		},
	})
	require.NoError(t, err)

	_, err = ar.Backend.CreateRepository(&artifactregistrypb.CreateRepositoryRequest{
		Parent: arTestParent, RepositoryId: "remote",
		Repository: &artifactregistrypb.Repository{
			Format: artifactregistrypb.Repository_DOCKER, Mode: artifactregistrypb.Repository_REMOTE_REPOSITORY,
			ModeConfig: &artifactregistrypb.Repository_RemoteRepositoryConfig{
				RemoteRepositoryConfig: &artifactregistrypb.RemoteRepositoryConfig{
					Description:               "hub",
					DisableUpstreamValidation: true,
					UpstreamCredentials: &artifactregistrypb.RemoteRepositoryConfig_UpstreamCredentials{
						Credentials: &artifactregistrypb.RemoteRepositoryConfig_UpstreamCredentials_UsernamePasswordCredentials_{
							UsernamePasswordCredentials: &upstreamPassword{
								Username:              "bot",
								PasswordSecretVersion: "projects/p/secrets/hub/versions/1",
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	var body bytes.Buffer

	mw := multipart.NewWriter(&body)
	meta, err := mw.CreatePart(map[string][]string{"Content-Type": {"application/json"}})
	require.NoError(t, err)
	_, err = meta.Write([]byte(`{"packageId":"pkg","versionId":"1.0.0","filename":"a.txt"}`))
	require.NoError(t, err)
	media, err := mw.CreatePart(map[string][]string{"Content-Type": {"application/octet-stream"}})
	require.NoError(t, err)
	_, err = media.Write([]byte("hello-artifact"))
	require.NoError(t, err)
	require.NoError(t, mw.Close())

	req := httptest.NewRequest(http.MethodPost,
		"/upload/v1/"+arTestParent+"/repositories/gen/genericArtifacts:create?uploadType=multipart", &body)
	req.Header.Set("Content-Type", "multipart/related; boundary="+mw.Boundary())

	rec := httptest.NewRecorder()
	ar.Backend.FileExtras().Handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	return dashboard.NewHandler(dashboard.Config{GCPArtifactRegistryOps: ar})
}

func TestGCPArtifactRegistryProxyRoutes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		path     string
		wantBody string
		wantCode int
	}{
		{"cleanup preview", http.MethodGet, arTestRepo + ":cleanupPreview", `"dryRun":true`, http.StatusOK},
		{
			"cleanup preview lists",
			http.MethodGet,
			arTestRepo + ":cleanupPreview",
			"packages/pkg/versions/1.0.0",
			http.StatusOK,
		},
		{
			"cleanup preview is read only",
			http.MethodPost,
			arTestRepo + ":cleanupPreview",
			"unsupported",
			http.StatusNotFound,
		},
		{
			"cleanup preview on a list",
			http.MethodGet,
			arTestRoot + "/v1/" + arTestParent + ":cleanupPreview",
			"unsupported",
			http.StatusNotFound,
		},
		{
			"remote detail names the secret",
			http.MethodGet,
			arTestRoot + "/v1/" + arTestParent + "/repositories/remote",
			"projects/p/secrets/hub/versions/1",
			http.StatusOK,
		},
		{
			"tag get in non docker repo",
			http.MethodGet,
			arTestRepo + "/packages/pkg/tags/t1",
			"FAILED_PRECONDITION",
			http.StatusBadRequest,
		},
		{
			"tag delete in non docker repo",
			http.MethodDelete,
			arTestRepo + "/packages/pkg/tags/t1",
			"FAILED_PRECONDITION",
			http.StatusBadRequest,
		},
		{"tag create refused", http.MethodPost, arTestRepo + "/packages/pkg/tags", "unsupported", http.StatusNotFound},
		{
			"tag patch refused",
			http.MethodPatch,
			arTestRepo + "/packages/pkg/tags/t1",
			"unsupported",
			http.StatusNotFound,
		},
		{
			"version delete",
			http.MethodDelete,
			arTestRepo + "/packages/pkg/versions/1.0.0",
			`"done":true`,
			http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := serveGCP(t, newArtifactRegistryDashboard(t), tt.method, tt.path, "")
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestGCPArtifactRegistryProxyRemoteNeverLeaksSecretValue(t *testing.T) {
	t.Parallel()

	rec := serveGCP(t, newArtifactRegistryDashboard(t), http.MethodGet,
		arTestRoot+"/v1/"+arTestParent+"/repositories/remote", "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.NotContains(t, rec.Body.String(), "hunter2")
	assert.Contains(t, rec.Body.String(), "passwordSecretVersion")
}

func TestGCPArtifactRegistryProxyDownload(t *testing.T) {
	t.Parallel()

	h := newArtifactRegistryDashboard(t)
	files := serveGCP(t, h, http.MethodGet, arTestRepo+"/files", "")
	require.Equal(t, http.StatusOK, files.Code, files.Body.String())

	name := extractJSONString(t, files.Body.String(), `"name":"`)
	require.Contains(t, name, "/files/")

	tests := []struct {
		name     string
		query    string
		wantBody string
		wantCode int
	}{
		{"file", name, "hello-artifact", http.StatusOK},
		{"empty", "", "invalid file name", http.StatusBadRequest},
		{"traversal", arTestParent + "/repositories/gen/files/..", "invalid file name", http.StatusBadRequest},
		{"wrong kind", arTestParent + "/repositories/gen/packages/pkg", "invalid file name", http.StatusBadRequest},
		{"too short", arTestParent + "/repositories/gen", "invalid file name", http.StatusBadRequest},
		{"missing file", arTestParent + "/repositories/gen/files/nope", "NOT_FOUND", http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := serveGCP(
				t,
				h,
				http.MethodGet,
				arTestRoot+"/download?name="+strings.ReplaceAll(tt.query, "%", "%25"),
				"",
			)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tt.wantBody)

			if tt.wantCode == http.StatusOK {
				assert.Equal(t, "application/octet-stream", rec.Header().Get("Content-Type"))
				assert.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
				assert.Contains(t, rec.Header().Get("Content-Disposition"), "attachment")
			}
		})
	}
}

func TestGCPArtifactRegistryProxyDownloadUnavailable(t *testing.T) {
	t.Parallel()

	rec := serveGCP(t, dashboard.NewHandler(dashboard.Config{}), http.MethodGet, arTestRoot+"/download?name=x", "")
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}
