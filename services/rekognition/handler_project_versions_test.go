package rekognition_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rekognition"
)

func TestProjectVersions(t *testing.T) { //nolint:paralleltest // existing issue.
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
		})
	}
}

func TestProjectVersion_Lifecycle(t *testing.T) { //nolint:paralleltest // existing issue.
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

// ---------------------------------------------------------------------------
// CreateProjectVersion persists and echoes OutputConfig/KmsKeyId/
// VersionDescription/Tags back through DescribeProjectVersions and
// ListTagsForResource. These were previously accepted by the request but
// silently dropped -- see PARITY.md gaps.
// ---------------------------------------------------------------------------

func TestCreateProjectVersion_FullFieldsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateProject", map[string]any{"ProjectName": "full-ver-proj"})
	require.Equal(t, http.StatusOK, rec.Code)

	var projResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &projResp))
	projectARN := projResp["ProjectArn"].(string)

	rec = doRequest(t, h, "CreateProjectVersion", map[string]any{
		"ProjectArn":  projectARN,
		"VersionName": "v-full",
		"OutputConfig": map[string]any{
			"S3Bucket":    "my-output-bucket",
			"S3KeyPrefix": "training-output/",
		},
		"KmsKeyId":           "arn:aws:kms:us-east-1:000000000000:key/abc",
		"VersionDescription": "a test model version",
		"Tags":               map[string]any{"team": "vision"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var verResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &verResp))
	versionARN := verResp["ProjectVersionArn"].(string)

	rec = doRequest(t, h, "DescribeProjectVersions", map[string]any{"ProjectArn": projectARN})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	versions := descResp["ProjectVersionDescriptions"].([]any)
	require.Len(t, versions, 1)

	desc := versions[0].(map[string]any)
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/abc", desc["KmsKeyId"])
	assert.Equal(t, "a test model version", desc["VersionDescription"])

	outputConfig, _ := desc["OutputConfig"].(map[string]any)
	require.NotNil(t, outputConfig)
	assert.Equal(t, "my-output-bucket", outputConfig["S3Bucket"])
	assert.Equal(t, "training-output/", outputConfig["S3KeyPrefix"])

	// ProjectVersion ARNs are taggable (see PARITY.md Notes #3) -- confirm
	// the initial Tags made it into the tag store.
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": versionARN})
	require.Equal(t, http.StatusOK, rec.Code)

	var tagsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))
	tags, _ := tagsResp["Tags"].(map[string]any)
	assert.Equal(t, "vision", tags["team"])
}

func TestCopyProjectVersion(t *testing.T) { //nolint:paralleltest // existing issue.
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
