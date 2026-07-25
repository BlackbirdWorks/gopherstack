package codedeploy_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codedeploy"
)

func s3Revision(bucket, key string) map[string]any {
	return map[string]any{
		"revisionType": "S3",
		"s3Location": map[string]any{
			"bucket":     bucket,
			"key":        key,
			"bundleType": "zip",
		},
	}
}

func TestHandler_RegisterApplicationRevision_GetApplicationRevision(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateApplication", map[string]any{"applicationName": "my-app", "computePlatform": "Server"})

	registerRec := doRequest(t, h, "RegisterApplicationRevision", map[string]any{
		"applicationName": "my-app",
		"description":     "v1",
		"revision":        s3Revision("my-bucket", "my-key"),
	})
	require.Equal(t, http.StatusOK, registerRec.Code)

	getRec := doRequest(t, h, "GetApplicationRevision", map[string]any{
		"applicationName": "my-app",
		"revision":        s3Revision("my-bucket", "my-key"),
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))
	assert.Equal(t, "my-app", resp["applicationName"])

	revisionInfo, ok := resp["revisionInfo"].(map[string]any)
	require.True(t, ok, "revisionInfo must be populated for a registered revision")
	assert.Equal(t, "v1", revisionInfo["description"])
	assert.NotZero(t, revisionInfo["registerTime"])
	// Never referenced by a deployment, so first/lastUsedTime stay unset.
	assert.Nil(t, revisionInfo["firstUsedTime"])
	assert.Nil(t, revisionInfo["lastUsedTime"])
}

func TestHandler_GetApplicationRevision_NotRegistered(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateApplication", map[string]any{"applicationName": "my-app", "computePlatform": "Server"})

	rec := doRequest(t, h, "GetApplicationRevision", map[string]any{
		"applicationName": "my-app",
		"revision":        s3Revision("never-registered", "key"),
	})
	require.Equal(t, http.StatusNotFound, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "RevisionDoesNotExistException", resp["__type"])
}

func TestHandler_GetApplicationRevision_AppNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetApplicationRevision", map[string]any{
		"applicationName": "nonexistent",
		"revision":        s3Revision("b", "k"),
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_RegisterApplicationRevision_Idempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateApplication", map[string]any{"applicationName": "my-app", "computePlatform": "Server"})

	doRequest(t, h, "RegisterApplicationRevision", map[string]any{
		"applicationName": "my-app",
		"description":     "v1",
		"revision":        s3Revision("bucket", "key"),
	})

	// Re-registering the same revision updates the description but does not
	// create a second entry: ListApplicationRevisions must still report one.
	doRequest(t, h, "RegisterApplicationRevision", map[string]any{
		"applicationName": "my-app",
		"description":     "v2",
		"revision":        s3Revision("bucket", "key"),
	})

	listRec := doRequest(t, h, "ListApplicationRevisions", map[string]any{"applicationName": "my-app"})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	revisions, ok := listResp["revisions"].([]any)
	require.True(t, ok)
	require.Len(t, revisions, 1)

	getRec := doRequest(t, h, "GetApplicationRevision", map[string]any{
		"applicationName": "my-app",
		"revision":        s3Revision("bucket", "key"),
	})
	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	revisionInfo, ok := getResp["revisionInfo"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "v2", revisionInfo["description"])
}

func TestHandler_ListApplicationRevisions_MultipleAndFilters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateApplication", map[string]any{"applicationName": "my-app", "computePlatform": "Server"})

	doRequest(t, h, "RegisterApplicationRevision", map[string]any{
		"applicationName": "my-app",
		"revision":        s3Revision("bucket-a", "key-a"),
	})
	doRequest(t, h, "RegisterApplicationRevision", map[string]any{
		"applicationName": "my-app",
		"revision":        s3Revision("bucket-b", "key-b"),
	})

	rec := doRequest(t, h, "ListApplicationRevisions", map[string]any{"applicationName": "my-app"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	revisions, ok := resp["revisions"].([]any)
	require.True(t, ok)
	require.Len(t, revisions, 2)

	filteredRec := doRequest(t, h, "ListApplicationRevisions", map[string]any{
		"applicationName": "my-app",
		"s3Bucket":        "bucket-a",
	})
	require.Equal(t, http.StatusOK, filteredRec.Code)

	var filteredResp map[string]any
	require.NoError(t, json.Unmarshal(filteredRec.Body.Bytes(), &filteredResp))
	filtered, ok := filteredResp["revisions"].([]any)
	require.True(t, ok)
	require.Len(t, filtered, 1)
}

// TestApplicationRevisions_CreateDeploymentTouchesRevision proves
// CreateDeployment auto-registers an unseen revision and stamps
// FirstUsedTime/LastUsedTime/DeploymentGroups on it, matching real
// CodeDeploy's auto-registration behavior for revisions supplied directly to
// CreateDeployment.
func TestApplicationRevisions_CreateDeploymentTouchesRevision(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend

	_, err := b.CreateApplication("my-app", "Server", nil)
	require.NoError(t, err)
	_, err = createDG(b, "my-app", "my-dg", "arn:aws:iam::000000000000:role/role", "", nil)
	require.NoError(t, err)

	revision := codedeploy.RevisionLocation{
		RevisionType: "S3",
		S3Location:   &codedeploy.RevisionS3Location{Bucket: "b", Key: "k", BundleType: "zip"},
	}

	_, err = b.CreateDeployment("my-app", "my-dg", codedeploy.DeploymentOptions{
		Creator:  "user",
		Revision: &revision,
	})
	require.NoError(t, err)

	rev, err := b.GetApplicationRevision("my-app", revision)
	require.NoError(t, err)
	assert.NotZero(t, rev.RegisterTime)
	require.NotNil(t, rev.FirstUsedTime)
	require.NotNil(t, rev.LastUsedTime)
	assert.Contains(t, rev.DeploymentGroups, "my-dg")
}

// TestApplicationRevisions_DeleteApplicationCascades proves deleting an
// application removes its registered revisions too, so no ghost rows survive
// under the deleted application's name.
func TestApplicationRevisions_DeleteApplicationCascades(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend

	_, err := b.CreateApplication("my-app", "Server", nil)
	require.NoError(t, err)

	revision := codedeploy.RevisionLocation{
		RevisionType: "S3",
		S3Location:   &codedeploy.RevisionS3Location{Bucket: "b", Key: "k"},
	}
	require.NoError(t, b.RegisterApplicationRevision("my-app", revision, "desc"))

	require.NoError(t, b.DeleteApplication("my-app"))

	_, err = b.CreateApplication("my-app", "Server", nil)
	require.NoError(t, err)

	_, err = b.GetApplicationRevision("my-app", revision)
	require.ErrorIs(t, err, codedeploy.ErrRevisionNotFound,
		"a revision registered before delete must not survive under the recreated application")
}

// TestApplicationRevisions_RenameApplicationMovesRevisions proves
// UpdateApplication's rename moves registered revisions to the new
// application name instead of orphaning them under the old one.
func TestApplicationRevisions_RenameApplicationMovesRevisions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend

	_, err := b.CreateApplication("old-name", "Server", nil)
	require.NoError(t, err)

	revision := codedeploy.RevisionLocation{
		RevisionType: "S3",
		S3Location:   &codedeploy.RevisionS3Location{Bucket: "b", Key: "k"},
	}
	require.NoError(t, b.RegisterApplicationRevision("old-name", revision, "desc"))

	require.NoError(t, b.UpdateApplication("old-name", "new-name"))

	rev, err := b.GetApplicationRevision("new-name", revision)
	require.NoError(t, err)
	assert.Equal(t, "new-name", rev.ApplicationName)
}

func TestHandler_BatchGetApplicationRevisions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codedeploy.Handler)
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *codedeploy.Handler) {
				_, _ = h.Backend.CreateApplication("my-app", "Server", nil)
			},
			input: map[string]any{
				"applicationName": "my-app",
				"revisions": []map[string]any{
					{"revisionType": "S3"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_application_name",
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "app_not_found",
			input: map[string]any{
				"applicationName": "nonexistent",
				"revisions":       []map[string]any{},
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "BatchGetApplicationRevisions", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "my-app", resp["applicationName"])
			}
		})
	}
}

// TestHandler_BatchGetApplicationRevisions_PopulatesRegistered proves the
// batch response's genericRevisionInfo is populated for revisions that are
// actually registered, and omitted for ones that are not -- rather than
// echoing the input unconditionally with no real lookup performed.
func TestHandler_BatchGetApplicationRevisions_PopulatesRegistered(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateApplication", map[string]any{"applicationName": "my-app", "computePlatform": "Server"})
	doRequest(t, h, "RegisterApplicationRevision", map[string]any{
		"applicationName": "my-app",
		"description":     "registered-one",
		"revision":        s3Revision("bucket", "registered-key"),
	})

	rec := doRequest(t, h, "BatchGetApplicationRevisions", map[string]any{
		"applicationName": "my-app",
		"revisions": []map[string]any{
			s3Revision("bucket", "registered-key"),
			s3Revision("bucket", "unregistered-key"),
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	revisions, ok := resp["revisions"].([]any)
	require.True(t, ok)
	require.Len(t, revisions, 2)

	registered, ok := revisions[0].(map[string]any)
	require.True(t, ok)
	info, ok := registered["genericRevisionInfo"].(map[string]any)
	require.True(t, ok, "registered revision must carry genericRevisionInfo")
	assert.Equal(t, "registered-one", info["description"])

	unregistered, ok := revisions[1].(map[string]any)
	require.True(t, ok)
	assert.Nil(t, unregistered["genericRevisionInfo"])
}

func TestApplicationRevisions_MaxLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, _ = h.Backend.CreateApplication("my-app", "Server", nil)

	// Build 26 revisions (> maxBatchRevisions=25)
	revisions := make([]map[string]string, 26)
	for i := range revisions {
		revisions[i] = map[string]string{"revisionType": "S3"}
	}

	rec := doRequest(t, h, "BatchGetApplicationRevisions", map[string]any{
		"applicationName": "my-app",
		"revisions":       revisions,
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
