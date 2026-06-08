package rekognition_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rekognition"
)

// =============================================================================
// Projects
// ============================================================================= //nolint:godot // existing issue.
func TestAppendixA_Projects(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		body     any
		setup    func(h *rekognition.Handler)
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "CreateProject returns ARN",
			action:   "CreateProject",
			body:     map[string]any{"ProjectName": "my-project"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["ProjectArn"], "arn:aws:rekognition:")
				assert.Contains(t, resp["ProjectArn"], "project/my-project")
			},
		},
		{
			name:     "CreateProject missing name returns error",
			action:   "CreateProject",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "DeleteProject returns DELETING",
			action: "DeleteProject",
			setup: func(h *rekognition.Handler) {
				rec := doRequest(t, h, "CreateProject", map[string]any{"ProjectName": "del-proj"})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["message"])
			},
		},
		{
			name:     "DeleteProject missing ARN returns error",
			action:   "DeleteProject",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "DescribeProjects returns projects",
			action: "DescribeProjects",
			setup: func(h *rekognition.Handler) {
				doRequest(t, h, "CreateProject", map[string]any{"ProjectName": "desc-proj"})
			},
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				descriptions, ok := resp["ProjectDescriptions"].([]any)
				require.True(t, ok)
				assert.GreaterOrEqual(t, len(descriptions), 1)
			},
		},
		{
			name:     "DescribeProjects empty list returns empty slice",
			action:   "DescribeProjects",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				descriptions, ok := resp["ProjectDescriptions"].([]any)
				require.True(t, ok)
				assert.Empty(t, descriptions)
			},
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}

			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code, tc.name)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		}()
	}
}
func TestAppendixA_Project_DeleteSuccess(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Create project
	rec := doRequest(t, h, "CreateProject", map[string]any{"ProjectName": "to-delete"})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	projectARN := createResp["ProjectArn"].(string)

	// Delete it
	rec = doRequest(t, h, "DeleteProject", map[string]any{"ProjectArn": projectARN})
	assert.Equal(t, http.StatusOK, rec.Code)

	var deleteResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &deleteResp))
	assert.Equal(t, "DELETING", deleteResp["Status"])

	// DescribeProjects should no longer return it
	rec = doRequest(t, h, "DescribeProjects", map[string]any{"ProjectArns": []string{projectARN}})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	descriptions := descResp["ProjectDescriptions"].([]any)
	assert.Empty(t, descriptions)
}

// =============================================================================
// Project Versions
// ============================================================================= //nolint:godot // existing issue.
func TestAppendixA_ProjectVersions(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		body     any
		setup    func(h *rekognition.Handler) string // returns projectARN
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateProjectVersion returns ARN",
			action: "CreateProjectVersion",
			setup: func(h *rekognition.Handler) string {
				rec := doRequest(t, h, "CreateProject", map[string]any{"ProjectName": "ver-proj"})
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["ProjectArn"].(string)
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["ProjectVersionArn"], "arn:aws:rekognition:")
			},
		},
		{
			name:     "CreateProjectVersion missing ProjectArn returns error",
			action:   "CreateProjectVersion",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			h := newTestHandler(t)
			var body any

			if tc.setup != nil {
				arn := tc.setup(h)
				body = map[string]any{
					"ProjectArn":  arn,
					"VersionName": "v1",
				}
			}

			rec := doRequest(t, h, tc.action, body)
			assert.Equal(t, tc.wantCode, rec.Code, tc.name)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		}()
	}
}
func TestAppendixA_ProjectVersion_Lifecycle(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Create project
	rec := doRequest(t, h, "CreateProject", map[string]any{"ProjectName": "life-proj"})
	require.Equal(t, http.StatusOK, rec.Code)

	var projResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &projResp))
	projectARN := projResp["ProjectArn"].(string)

	// Create version
	rec = doRequest(t, h, "CreateProjectVersion", map[string]any{
		"ProjectArn":  projectARN,
		"VersionName": "v1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var verResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &verResp))
	versionARN := verResp["ProjectVersionArn"].(string)

	// DescribeProjectVersions
	rec = doRequest(t, h, "DescribeProjectVersions", map[string]any{"ProjectArn": projectARN})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	versions := descResp["ProjectVersionDescriptions"].([]any)
	assert.Len(t, versions, 1)

	// StartProjectVersion
	rec = doRequest(t, h, "StartProjectVersion", map[string]any{
		"ProjectVersionArn": versionARN,
		"MinInferenceUnits": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	assert.Equal(t, "RUNNING", startResp["Status"])

	// StopProjectVersion
	rec = doRequest(t, h, "StopProjectVersion", map[string]any{
		"ProjectVersionArn": versionARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var stopResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &stopResp))
	assert.Equal(t, "STOPPED", stopResp["Status"])

	// DeleteProjectVersion
	rec = doRequest(t, h, "DeleteProjectVersion", map[string]any{
		"ProjectVersionArn": versionARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}
func TestAppendixA_CopyProjectVersion(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Create source project + version
	rec := doRequest(t, h, "CreateProject", map[string]any{"ProjectName": "src-proj"})
	require.Equal(t, http.StatusOK, rec.Code)

	var srcProjResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &srcProjResp))
	srcProjectARN := srcProjResp["ProjectArn"].(string)

	rec = doRequest(t, h, "CreateProjectVersion", map[string]any{
		"ProjectArn":  srcProjectARN,
		"VersionName": "v1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var srcVerResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &srcVerResp))
	sourceVersionARN := srcVerResp["ProjectVersionArn"].(string)

	// Create destination project
	rec = doRequest(t, h, "CreateProject", map[string]any{"ProjectName": "dst-proj"})
	require.Equal(t, http.StatusOK, rec.Code)

	var dstProjResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &dstProjResp))
	dstProjectARN := dstProjResp["ProjectArn"].(string)

	// Copy version
	rec = doRequest(t, h, "CopyProjectVersion", map[string]any{
		"SourceProjectVersionArn": sourceVersionARN,
		"DestinationProjectArn":   dstProjectARN,
		"VersionName":             "v1-copy",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var copyResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &copyResp))
	assert.Contains(t, copyResp["ProjectVersionArn"], "dst-proj")
}

// =============================================================================
// Project Policies
// ============================================================================= //nolint:godot // existing issue.
func TestAppendixA_ProjectPolicies(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Create project
	rec := doRequest(t, h, "CreateProject", map[string]any{"ProjectName": "policy-proj"})
	require.Equal(t, http.StatusOK, rec.Code)

	var projResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &projResp))
	projectARN := projResp["ProjectArn"].(string)

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "PutProjectPolicy creates policy",
			action: "PutProjectPolicy",
			body: map[string]any{
				"ProjectArn":     projectARN,
				"PolicyName":     "my-policy",
				"PolicyDocument": `{"Version":"2012-10-17"}`,
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["PolicyRevisionId"])
			},
		},
		{
			name:     "ListProjectPolicies returns policy",
			action:   "ListProjectPolicies",
			body:     map[string]any{"ProjectArn": projectARN},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				policies, ok := resp["ProjectPolicies"].([]any)
				require.True(t, ok)
				assert.Len(t, policies, 1)
			},
		},
		{
			name:   "DeleteProjectPolicy removes policy",
			action: "DeleteProjectPolicy",
			body: map[string]any{
				"ProjectArn": projectARN,
				"PolicyName": "my-policy",
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.action, tc.body) //nolint:govet // existing issue.
			assert.Equal(t, tc.wantCode, rec.Code, tc.name)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		}()
	}
}

// =============================================================================
// Datasets
// ============================================================================= //nolint:godot // existing issue.
func TestAppendixA_Datasets(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Create project first
	rec := doRequest(t, h, "CreateProject", map[string]any{"ProjectName": "ds-proj"})
	require.Equal(t, http.StatusOK, rec.Code)

	var projResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &projResp))
	projectARN := projResp["ProjectArn"].(string)

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte) string
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateDataset returns ARN",
			action: "CreateDataset",
			body: map[string]any{
				"ProjectArn":  projectARN,
				"DatasetType": "TRAIN",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) string {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				arn := resp["DatasetArn"].(string)
				assert.Contains(t, arn, "arn:aws:rekognition:")

				return arn
			},
		},
	}

	var datasetARN string

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.action, tc.body) //nolint:govet // existing issue.
			assert.Equal(t, tc.wantCode, rec.Code, tc.name)

			if tc.check != nil {
				datasetARN = tc.check(t, rec.Body.Bytes())
			}
		}()
	}

	// DescribeDataset
	t.Run("DescribeDataset returns details", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest( //nolint:govet // existing issue.
			t,
			h,
			"DescribeDataset",
			map[string]any{"DatasetArn": datasetARN},
		)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		desc := resp["DatasetDescription"].(map[string]any)
		assert.Equal(t, "CREATE_COMPLETE", desc["Status"])
		assert.Equal(t, "TRAIN", desc["DatasetType"])
	}()

	// UpdateDatasetEntries
	t.Run("UpdateDatasetEntries succeeds", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest(t, h, "UpdateDatasetEntries", map[string]any{ //nolint:govet // existing issue.
			"DatasetArn": datasetARN,
			"Changes":    []byte(`{"source-ref": "s3://bucket/img.jpg"}`),
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	}()

	// ListDatasetEntries
	t.Run("ListDatasetEntries returns entries", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest( //nolint:govet // existing issue.
			t,
			h,
			"ListDatasetEntries",
			map[string]any{"DatasetArn": datasetARN},
		)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.NotNil(t, resp["DatasetEntries"])
	}()

	// ListDatasetLabels
	t.Run("ListDatasetLabels returns empty list", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest( //nolint:govet // existing issue.
			t,
			h,
			"ListDatasetLabels",
			map[string]any{"DatasetArn": datasetARN},
		)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.NotNil(t, resp["DatasetLabelStats"])
	}()

	// DistributeDatasetEntries
	t.Run("DistributeDatasetEntries succeeds", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest(t, h, "DistributeDatasetEntries", map[string]any{ //nolint:govet // existing issue.
			"Datasets": []any{
				map[string]any{"DatasetArn": datasetARN},
			},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	}()

	// DeleteDataset
	t.Run("DeleteDataset removes dataset", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest( //nolint:govet // existing issue.
			t,
			h,
			"DeleteDataset",
			map[string]any{"DatasetArn": datasetARN},
		)
		assert.Equal(t, http.StatusOK, rec.Code)

		// DescribeDataset should now return not found
		rec = doRequest(t, h, "DescribeDataset", map[string]any{"DatasetArn": datasetARN})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	}()
}
func TestAppendixA_Dataset_MissingProjectArn(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body   any
		name   string
		action string
	}{
		{
			name:   "CreateDataset missing ProjectArn",
			action: "CreateDataset",
			body:   map[string]any{"DatasetType": "TRAIN"},
		},
		{
			name:   "CreateDataset missing DatasetType",
			action: "CreateDataset",
			body:   map[string]any{"ProjectArn": "arn:xxx"},
		},
		{
			name:   "DescribeDataset missing DatasetArn",
			action: "DescribeDataset",
			body:   map[string]any{},
		},
		{
			name:   "DeleteDataset missing DatasetArn",
			action: "DeleteDataset",
			body:   map[string]any{},
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code, tc.name)
		}()
	}
}

// =============================================================================
// Users
// ============================================================================= //nolint:godot // existing issue.
func TestAppendixA_Users(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Create collection
	rec := doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "user-coll"})
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "CreateUser succeeds",
			action:   "CreateUser",
			body:     map[string]any{"CollectionId": "user-coll", "UserId": "alice"},
			wantCode: http.StatusOK,
		},
		{
			name:     "CreateUser duplicate returns error",
			action:   "CreateUser",
			body:     map[string]any{"CollectionId": "user-coll", "UserId": "alice"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "ListUsers returns user",
			action:   "ListUsers",
			body:     map[string]any{"CollectionId": "user-coll"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				users, ok := resp["Users"].([]any)
				require.True(t, ok)
				assert.Len(t, users, 1)
				u := users[0].(map[string]any)
				assert.Equal(t, "alice", u["UserId"])
			},
		},
		{
			name:     "DeleteUser succeeds",
			action:   "DeleteUser",
			body:     map[string]any{"CollectionId": "user-coll", "UserId": "alice"},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteUser not found returns error",
			action:   "DeleteUser",
			body:     map[string]any{"CollectionId": "user-coll", "UserId": "alice"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.action, tc.body) //nolint:govet // existing issue.
			assert.Equal(t, tc.wantCode, rec.Code, tc.name)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		}()
	}
}
func TestAppendixA_AssociateFaces(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Create collection, index face, create user
	rec := doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "assoc-coll"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "IndexFaces", map[string]any{
		"CollectionId":    "assoc-coll",
		"ExternalImageId": "ext-img-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var indexResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &indexResp))
	faceRecords := indexResp["FaceRecords"].([]any)
	require.Len(t, faceRecords, 1)
	faceID := faceRecords[0].(map[string]any)["Face"].(map[string]any)["FaceId"].(string)

	rec = doRequest(t, h, "CreateUser", map[string]any{"CollectionId": "assoc-coll", "UserId": "bob"})
	require.Equal(t, http.StatusOK, rec.Code)

	// AssociateFaces
	t.Run("AssociateFaces succeeds", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest(t, h, "AssociateFaces", map[string]any{ //nolint:govet // existing issue.
			"CollectionId": "assoc-coll",
			"UserId":       "bob",
			"FaceIds":      []string{faceID},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		associated, ok := resp["AssociatedFaces"].([]any)
		require.True(t, ok)
		assert.Len(t, associated, 1)
	}()

	// AssociateFaces with unknown face
	t.Run("AssociateFaces unknown face is unsuccessful", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest(t, h, "AssociateFaces", map[string]any{ //nolint:govet // existing issue.
			"CollectionId": "assoc-coll",
			"UserId":       "bob",
			"FaceIds":      []string{"00000000-0000-0000-0000-000000000000"},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		unsuccessful, ok := resp["UnsuccessfulFaceAssociations"].([]any)
		require.True(t, ok)
		assert.Len(t, unsuccessful, 1)
	}()

	// DisassociateFaces
	t.Run("DisassociateFaces removes associated face", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest(t, h, "DisassociateFaces", map[string]any{ //nolint:govet // existing issue.
			"CollectionId": "assoc-coll",
			"UserId":       "bob",
			"FaceIds":      []string{faceID},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		disassociated, ok := resp["DisassociatedFaces"].([]any)
		require.True(t, ok)
		assert.Len(t, disassociated, 1)
	}()
}
func TestAppendixA_SearchUsers(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "search-coll"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateUser", map[string]any{"CollectionId": "search-coll", "UserId": "user1"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateUser", map[string]any{"CollectionId": "search-coll", "UserId": "user2"})
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "SearchUsers returns matches",
			action: "SearchUsers",
			body: map[string]any{
				"CollectionId": "search-coll",
				"UserId":       "user1",
				"MaxUsers":     10,
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, faceModelVersion, resp["FaceModelVersion"])
				matches, ok := resp["UserMatches"].([]any)
				require.True(t, ok)
				assert.Len(t, matches, 1)
			},
		},
		{
			name:   "SearchUsersByImage returns matches",
			action: "SearchUsersByImage",
			body: map[string]any{
				"CollectionId": "search-coll",
				"MaxUsers":     10,
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, faceModelVersion, resp["FaceModelVersion"])
				matches, ok := resp["UserMatches"].([]any)
				require.True(t, ok)
				assert.GreaterOrEqual(t, len(matches), 2)
			},
		},
		{
			name:     "SearchUsers missing CollectionId returns error",
			action:   "SearchUsers",
			body:     map[string]any{"UserId": "user1"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "SearchUsers missing UserId returns error",
			action:   "SearchUsers",
			body:     map[string]any{"CollectionId": "search-coll"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "SearchUsersByImage missing CollectionId returns error",
			action:   "SearchUsersByImage",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.action, tc.body) //nolint:govet // existing issue.
			assert.Equal(t, tc.wantCode, rec.Code, tc.name)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		}()
	}
}

// =============================================================================
// Face Liveness
// ============================================================================= //nolint:godot // existing issue.
func TestAppendixA_FaceLiveness(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "CreateFaceLivenessSession returns session ID",
			action:   "CreateFaceLivenessSession",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["SessionId"])
			},
		},
	}

	var sessionID string

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code, tc.name)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}

			if tc.action == "CreateFaceLivenessSession" && rec.Code == http.StatusOK {
				var resp map[string]any
				_ = json.Unmarshal(rec.Body.Bytes(), &resp)
				sessionID = resp["SessionId"].(string)
			}
		}()
	}

	t.Run("GetFaceLivenessSessionResults returns result", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest(t, h, "GetFaceLivenessSessionResults", map[string]any{"SessionId": sessionID})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "SUCCEEDED", resp["Status"])
		assert.Equal(t, sessionID, resp["SessionId"])
	}()

	t.Run( //nolint:paralleltest // existing issue.
		"GetFaceLivenessSessionResults unknown session returns error",
		func(t *testing.T) {
			rec := doRequest(t, h, "GetFaceLivenessSessionResults", map[string]any{
				"SessionId": "00000000-0000-0000-0000-000000000000",
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		},
	)

	t.Run( //nolint:paralleltest // existing issue.
		"GetFaceLivenessSessionResults missing ID returns error",
		func(t *testing.T) {
			rec := doRequest(t, h, "GetFaceLivenessSessionResults", map[string]any{})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		},
	)
}

// =============================================================================
// Image Analysis (stateless)
// ============================================================================= //nolint:godot // existing issue.
func TestAppendixA_ImageAnalysis(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "CompareFaces returns empty matches",
			action:   "CompareFaces",
			body:     map[string]any{"SourceImage": map[string]any{}, "TargetImage": map[string]any{}},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotNil(t, resp["FaceMatches"])
				assert.NotNil(t, resp["SourceImageFace"])
			},
		},
		{
			name:     "DetectFaces returns empty list",
			action:   "DetectFaces",
			body:     map[string]any{"Image": map[string]any{}},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotNil(t, resp["FaceDetails"])
			},
		},
		{
			name:     "DetectLabels returns empty list",
			action:   "DetectLabels",
			body:     map[string]any{"Image": map[string]any{}},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotNil(t, resp["Labels"])
			},
		},
		{
			name:     "DetectText returns empty list",
			action:   "DetectText",
			body:     map[string]any{"Image": map[string]any{}},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotNil(t, resp["TextDetections"])
			},
		},
		{
			name:     "DetectCustomLabels returns empty list",
			action:   "DetectCustomLabels",
			body:     map[string]any{"ProjectVersionArn": "arn:xxx", "Image": map[string]any{}},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotNil(t, resp["CustomLabels"])
			},
		},
		{
			name:     "DetectModerationLabels returns empty list",
			action:   "DetectModerationLabels",
			body:     map[string]any{"Image": map[string]any{}},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotNil(t, resp["ModerationLabels"])
			},
		},
		{
			name:     "DetectProtectiveEquipment returns empty list",
			action:   "DetectProtectiveEquipment",
			body:     map[string]any{"Image": map[string]any{}},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotNil(t, resp["Persons"])
			},
		},
		{
			name:     "RecognizeCelebrities returns empty list",
			action:   "RecognizeCelebrities",
			body:     map[string]any{"Image": map[string]any{}},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotNil(t, resp["CelebrityFaces"])
			},
		},
		{
			name:     "GetCelebrityInfo returns info",
			action:   "GetCelebrityInfo",
			body:     map[string]any{"Id": "celebrity-123"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotEmpty(t, resp["Name"])
				assert.NotNil(t, resp["KnownGender"])
			},
		},
		{
			name:     "GetCelebrityInfo missing ID returns error",
			action:   "GetCelebrityInfo",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code, tc.name)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		}()
	}
}

// =============================================================================
// Async Video Jobs
// ============================================================================= //nolint:godot // existing issue.
func TestAppendixA_AsyncVideoJobs(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	type jobFlow struct {
		startBody   any
		checkGet    func(t *testing.T, body []byte)
		startAction string
		getAction   string
	}

	flows := []jobFlow{
		{
			startAction: "StartCelebrityRecognition",
			startBody:   map[string]any{"Video": map[string]any{}},
			getAction:   "GetCelebrityRecognition",
			checkGet: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "SUCCEEDED", resp["JobStatus"])
				assert.NotNil(t, resp["Celebrities"])
			},
		},
		{
			startAction: "StartContentModeration",
			startBody:   map[string]any{"Video": map[string]any{}},
			getAction:   "GetContentModeration",
			checkGet: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "SUCCEEDED", resp["JobStatus"])
				assert.NotNil(t, resp["ModerationLabels"])
			},
		},
		{
			startAction: "StartFaceDetection",
			startBody:   map[string]any{"Video": map[string]any{}},
			getAction:   "GetFaceDetection",
			checkGet: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "SUCCEEDED", resp["JobStatus"])
				assert.NotNil(t, resp["Faces"])
			},
		},
		{
			startAction: "StartFaceSearch",
			startBody:   map[string]any{"Video": map[string]any{}, "CollectionId": ""},
			getAction:   "GetFaceSearch",
			checkGet: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "SUCCEEDED", resp["JobStatus"])
				assert.NotNil(t, resp["Persons"])
			},
		},
		{
			startAction: "StartLabelDetection",
			startBody:   map[string]any{"Video": map[string]any{}},
			getAction:   "GetLabelDetection",
			checkGet: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "SUCCEEDED", resp["JobStatus"])
				assert.NotNil(t, resp["Labels"])
			},
		},
		{
			startAction: "StartPersonTracking",
			startBody:   map[string]any{"Video": map[string]any{}},
			getAction:   "GetPersonTracking",
			checkGet: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "SUCCEEDED", resp["JobStatus"])
				assert.NotNil(t, resp["Persons"])
			},
		},
		{
			startAction: "StartSegmentDetection",
			startBody:   map[string]any{"Video": map[string]any{}, "SegmentTypes": []string{"SHOT"}},
			getAction:   "GetSegmentDetection",
			checkGet: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "SUCCEEDED", resp["JobStatus"])
				assert.NotNil(t, resp["Segments"])
			},
		},
		{
			startAction: "StartTextDetection",
			startBody:   map[string]any{"Video": map[string]any{}},
			getAction:   "GetTextDetection",
			checkGet: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "SUCCEEDED", resp["JobStatus"])
				assert.NotNil(t, resp["TextDetections"])
			},
		},
	}

	for _, flow := range flows { //nolint:paralleltest // existing issue.
		t.Run(flow.startAction+"/"+flow.getAction, func(t *testing.T) {
			// Start job
			rec := doRequest(t, h, flow.startAction, flow.startBody)
			require.Equal(t, http.StatusOK, rec.Code, flow.startAction)

			var startResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
			jobID, ok := startResp["JobId"].(string)
			require.True(t, ok)
			assert.NotEmpty(t, jobID)

			// Get results
			rec = doRequest(t, h, flow.getAction, map[string]any{"JobId": jobID})
			require.Equal(t, http.StatusOK, rec.Code, flow.getAction)

			if flow.checkGet != nil {
				flow.checkGet(t, rec.Body.Bytes())
			}
		}()
	}
}
func TestAppendixA_AsyncJob_NotFound(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	getActions := []string{
		"GetCelebrityRecognition",
		"GetContentModeration",
		"GetFaceDetection",
		"GetFaceSearch",
		"GetLabelDetection",
		"GetPersonTracking",
		"GetSegmentDetection",
		"GetTextDetection",
	}

	for _, action := range getActions { //nolint:paralleltest // existing issue.
		t.Run(action+"_unknown_job", func(t *testing.T) {
			rec := doRequest(t, h, action, map[string]any{"JobId": "00000000-0000-0000-0000-000000000000"})
			assert.Equal(t, http.StatusBadRequest, rec.Code, action)
		}()
	}
}
func TestAppendixA_AsyncJob_MissingJobId(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	getActions := []string{
		"GetCelebrityRecognition",
		"GetContentModeration",
		"GetFaceDetection",
		"GetFaceSearch",
		"GetLabelDetection",
		"GetPersonTracking",
		"GetSegmentDetection",
		"GetTextDetection",
	}

	for _, action := range getActions { //nolint:paralleltest // existing issue.
		t.Run(action+"_missing_job_id", func(t *testing.T) {
			rec := doRequest(t, h, action, map[string]any{})
			assert.Equal(t, http.StatusBadRequest, rec.Code, action)
		}()
	}
}

// =============================================================================
// MediaAnalysis Jobs
// ============================================================================= //nolint:godot // existing issue.
func TestAppendixA_MediaAnalysisJobs(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	// Start job
	rec := doRequest(t, h, "StartMediaAnalysisJob", map[string]any{"JobName": "my-analysis"})
	require.Equal(t, http.StatusOK, rec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
	jobID := startResp["JobId"].(string)
	assert.NotEmpty(t, jobID)

	// Get job
	t.Run("GetMediaAnalysisJob returns job", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest(t, h, "GetMediaAnalysisJob", map[string]any{"JobId": jobID}) //nolint:govet // existing issue.
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, jobID, resp["JobId"])
		assert.Equal(t, "my-analysis", resp["JobName"])
		assert.Equal(t, "SUCCEEDED", resp["Status"])
	}()

	// List jobs
	t.Run("ListMediaAnalysisJobs returns job", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest(t, h, "ListMediaAnalysisJobs", map[string]any{}) //nolint:govet // existing issue.
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		jobs, ok := resp["MediaAnalysisJobs"].([]any)
		require.True(t, ok)
		assert.Len(t, jobs, 1)
	}()

	// Not found
	t.Run("GetMediaAnalysisJob unknown returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest( //nolint:govet // existing issue.
			t,
			h,
			"GetMediaAnalysisJob",
			map[string]any{"JobId": "does-not-exist"},
		)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	}()

	// Missing ID
	t.Run("GetMediaAnalysisJob missing ID returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		rec := doRequest(t, h, "GetMediaAnalysisJob", map[string]any{}) //nolint:govet // existing issue.
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	}()
}

// faceModelVersion re-exported for use in tests.
const faceModelVersion = "7.0"
