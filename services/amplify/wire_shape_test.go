package amplify_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	amplifysdk "github.com/aws/aws-sdk-go-v2/service/amplify"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amplify"
)

// TestDeleteApp_RoundTrip verifies DeleteApp returns the deleted App under
// the real "app" wrapper key (amplify@v1.41.4 api_op_DeleteApp.go: App is a
// required DeleteAppOutput member). Before this fix the handler answered
// with a bare 204 No Content, so a real SDK client's out.App decoded as nil.
func TestDeleteApp_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "deleted app is returned"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := amplify.NewInMemoryBackend("000000000000", tagsRTRegion)
			client := newTestAmplifyClient(t, amplify.NewHandler(backend))

			created, err := client.CreateApp(t.Context(), &amplifysdk.CreateAppInput{
				Name: aws.String("delete-me"),
			})
			require.NoError(t, err)

			out, err := client.DeleteApp(t.Context(), &amplifysdk.DeleteAppInput{AppId: created.App.AppId})
			require.NoError(t, err)
			require.NotNil(t, out.App, "real DeleteAppOutput.App is a required member")
			assert.Equal(t, aws.ToString(created.App.AppId), aws.ToString(out.App.AppId))
			assert.Equal(t, "delete-me", aws.ToString(out.App.Name))
		})
	}
}

// TestDeleteBranch_RoundTrip verifies DeleteBranch returns the deleted
// Branch under the real "branch" wrapper key (amplify@v1.41.4
// api_op_DeleteBranch.go: Branch is a required DeleteBranchOutput member).
func TestDeleteBranch_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "deleted branch is returned"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := amplify.NewInMemoryBackend("000000000000", tagsRTRegion)
			client := newTestAmplifyClient(t, amplify.NewHandler(backend))

			app, err := client.CreateApp(t.Context(), &amplifysdk.CreateAppInput{Name: aws.String("branch-host")})
			require.NoError(t, err)

			_, err = client.CreateBranch(t.Context(), &amplifysdk.CreateBranchInput{
				AppId:      app.App.AppId,
				BranchName: aws.String("main"),
			})
			require.NoError(t, err)

			out, err := client.DeleteBranch(t.Context(), &amplifysdk.DeleteBranchInput{
				AppId:      app.App.AppId,
				BranchName: aws.String("main"),
			})
			require.NoError(t, err)
			require.NotNil(t, out.Branch, "real DeleteBranchOutput.Branch is a required member")
			assert.Equal(t, "main", aws.ToString(out.Branch.BranchName))
		})
	}
}

// TestGetArtifactUrl_RoundTrip verifies GetArtifactUrl echoes the real
// artifact ID under "artifactId" (amplify@v1.41.4 api_op_GetArtifactUrl.go:
// ArtifactId is a required, string-typed GetArtifactUrlOutput member). Before
// this fix, InMemoryBackend.GetArtifactURL's first return value was the
// artifact's *type* ("BUILD"), not its ID -- a real client reading
// out.ArtifactId got the wrong string back (same key, wrong value; no
// decode failure, since both are plain strings).
func TestGetArtifactUrl_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "artifact id round-trips, not the artifact type"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := amplify.NewInMemoryBackend("000000000000", tagsRTRegion)
			app := seedApp(t, backend, "ArtifactRTApp")
			branch := seedMainBranch(t, backend, app.AppID)

			job, err := backend.StartJob(app.AppID, branch.BranchName, "RELEASE", "", "", "", time.Time{})
			require.NoError(t, err)

			j := amplify.NewJanitor(backend, time.Second)
			j.SweepOnce(t.Context())

			artifacts, _, err := backend.ListArtifacts(app.AppID, branch.BranchName, job.JobID, "", 0)
			require.NoError(t, err)
			require.Len(t, artifacts, 1)

			client := newTestAmplifyClient(t, amplify.NewHandler(backend))

			out, err := client.GetArtifactUrl(t.Context(), &amplifysdk.GetArtifactUrlInput{
				ArtifactId: aws.String(artifacts[0].ArtifactID),
			})
			require.NoError(t, err)
			assert.Equal(t, artifacts[0].ArtifactID, aws.ToString(out.ArtifactId))
			assert.NotEqual(t, "BUILD", aws.ToString(out.ArtifactId))
			assert.NotEmpty(t, aws.ToString(out.ArtifactUrl))
		})
	}
}

// TestGetBackendEnvironment_NoFabricatedAppID verifies the
// backendEnvironment response body carries no "appId" key. Real
// types.BackendEnvironment (amplify@v1.41.4 types/types.go:230) has no
// AppId field at all -- gopherstack's wire view fabricated one with no case
// in the real deserializer (awsRestjson1_deserializeDocumentBackendEnvironment).
func TestGetBackendEnvironment_NoFabricatedAppID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "no appId key in backendEnvironment body"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			app := seedApp(t, b, "BackendEnvRTApp")

			createRec := doRequest(t, h, http.MethodPost, "/apps/"+app.AppID+"/backendenvironments",
				map[string]any{"environmentName": "dev", "stackName": "stack", "deploymentArtifacts": "artifacts"})
			require.Equal(t, http.StatusCreated, createRec.Code)

			rec := doRequest(t, h, http.MethodGet, "/apps/"+app.AppID+"/backendenvironments/dev", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			be, ok := resp["backendEnvironment"].(map[string]any)
			require.True(t, ok)

			_, hasAppID := be["appId"]
			assert.False(t, hasAppID, "backendEnvironment must not carry a fabricated appId key: %v", be)
		})
	}
}

// TestGetDomainAssociation_NoFabricatedAppID verifies the domainAssociation
// response body carries no "appId" key. Real types.DomainAssociation
// (amplify@v1.41.4 types/types.go:542) has no AppId field at all --
// gopherstack's wire view fabricated one with no case in the real
// deserializer (awsRestjson1_deserializeDocumentDomainAssociation). A real
// SDK client silently ignores unknown keys, so this can only be proven with
// a raw-body assertion, not a typed-client field check.
func TestGetDomainAssociation_NoFabricatedAppID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "no appId key in domainAssociation body"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			app := seedApp(t, b, "DomainRTApp")

			subDomains := []map[string]any{{"prefix": "www", "branchName": "main"}}
			createRec := doRequest(t, h, http.MethodPost, "/apps/"+app.AppID+"/domains",
				map[string]any{"domainName": "no-appid.example.com", "subDomainSettings": subDomains})
			require.Equal(t, http.StatusCreated, createRec.Code)

			rec := doRequest(t, h, http.MethodGet, "/apps/"+app.AppID+"/domains/no-appid.example.com", nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			da, ok := resp["domainAssociation"].(map[string]any)
			require.True(t, ok)

			_, hasAppID := da["appId"]
			assert.False(t, hasAppID, "domainAssociation must not carry a fabricated appId key: %v", da)
		})
	}
}
