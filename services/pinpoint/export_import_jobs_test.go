package pinpoint_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	pinpointsdk "github.com/aws/aws-sdk-go-v2/service/pinpoint"
	"github.com/aws/aws-sdk-go-v2/service/pinpoint/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImportJob_CreatesSegment(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "import-seg-app")

	rec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/jobs/import",
		map[string]any{
			"RoleArn":     "arn:aws:iam::123456789012:role/import-role",
			"S3Url":       "s3://my-bucket/contacts.csv",
			"Format":      "CSV",
			"SegmentName": "imported-contacts",
		})
	require.Equal(t, http.StatusCreated, rec.Code)

	// Verify segments list now contains the import segment.
	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	items, _ := listResp["Item"].([]any)
	require.Len(t, items, 1, "import job must materialise exactly one segment")

	seg := items[0].(map[string]any)
	assert.Equal(t, "IMPORT", seg["SegmentType"], "segment type must be IMPORT")
	assert.Equal(t, "imported-contacts", seg["Name"])
	assert.NotEmpty(t, seg["Id"])

	importDef, _ := seg["ImportDefinition"].(map[string]any)
	require.NotNil(t, importDef, "ImportDefinition must be present")
	assert.Equal(t, "CSV", importDef["Format"])
	assert.Equal(t, "s3://my-bucket/contacts.csv", importDef["S3Url"])
	assert.Equal(t, "arn:aws:iam::123456789012:role/import-role", importDef["RoleArn"])
}

func TestImportJob_DefaultSegmentName(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "import-default-app")

	// No SegmentName provided → backend generates one.
	rec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/jobs/import",
		map[string]any{
			"RoleArn": "arn:aws:iam::123:role/r",
			"S3Url":   "s3://bucket/file.json",
			"Format":  "JSON",
		})
	require.Equal(t, http.StatusCreated, rec.Code)

	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	items, _ := listResp["Item"].([]any)
	require.Len(t, items, 1)

	seg := items[0].(map[string]any)
	assert.Equal(t, "IMPORT", seg["SegmentType"])
	assert.NotEmpty(t, seg["Name"], "generated segment name must not be empty")
}

func TestImportJob_SegmentGetByID(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "import-get-app")

	doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/jobs/import",
		map[string]any{
			"RoleArn":     "arn:aws:iam::123:role/r",
			"S3Url":       "s3://bucket/data.csv",
			"Format":      "CSV",
			"SegmentName": "direct-import",
		})

	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments", nil)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	items := listResp["Item"].([]any)
	segID := items[0].(map[string]any)["Id"].(string)

	getRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments/"+segID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var s map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &s))
	assert.Equal(t, "IMPORT", s["SegmentType"])
	assert.Equal(t, "direct-import", s["Name"])
}

// ──────────────────────────────────────────────────
// Finding #22: VerifyOTPMessage validates the actual code
// ──────────────────────────────────────────────────

func TestCreateExportJobAppNotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/nonexistent/jobs/export",
		map[string]any{"RoleArn": "arn:aws:iam::123:role/r", "S3UrlPrefix": "s3://b/p"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestCreateImportJobAppNotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/nonexistent/jobs/import",
		map[string]any{"RoleArn": "arn:aws:iam::123:role/r", "S3Url": "s3://b/f.csv", "Format": "CSV"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ──────────────────────────────────────────────────
// Required-field validation for jobs
// ──────────────────────────────────────────────────

func TestCreateExportJobMissingRoleArn(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "export-validation-app")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/jobs/export",
		map[string]any{"S3UrlPrefix": "s3://bucket/prefix"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateExportJobMissingS3Prefix(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "export-validation-app2")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/jobs/export",
		map[string]any{"RoleArn": "arn:aws:iam::123:role/r"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateImportJobMissingRoleArn(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "import-validation-app")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/jobs/import",
		map[string]any{"S3Url": "s3://bucket/file.csv", "Format": "CSV"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateImportJobMissingFormat(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "import-validation-app2")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/jobs/import",
		map[string]any{"RoleArn": "arn:aws:iam::123:role/r", "S3Url": "s3://b/f.csv"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ──────────────────────────────────────────────────
// Job response field persistence
// ──────────────────────────────────────────────────

// TestExportJobFieldsPersisted_RealClient covers gopherstack-6flj: real
// ExportJobResponse (pinpoint@v1.42.4 types.ExportJobResponse) nests
// RoleArn/S3UrlPrefix under a Definition member (types.ExportJobResource);
// there is no top-level Arn member at all. A prior version emitted
// RoleArn/S3UrlPrefix flat at the top level and fabricated an Arn field --
// a real client's deserializer would silently drop both, since
// ExportJobResponse's own field switch has no top-level "RoleArn"/
// "S3UrlPrefix"/"Arn" cases (only "Definition", confirmed at
// deserializers.go's awsRestjson1_deserializeDocumentExportJobResponse).
func TestExportJobFieldsPersisted_RealClient(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	client := newTestPinpointClient(t, h)

	appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
		CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("export-fields-app")},
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.ApplicationResponse.Id)

	out, err := client.CreateExportJob(t.Context(), &pinpointsdk.CreateExportJobInput{
		ApplicationId: aws.String(appID),
		ExportJobRequest: &types.ExportJobRequest{
			RoleArn:     aws.String("arn:aws:iam::123:role/my-role"),
			S3UrlPrefix: aws.String("s3://my-bucket/prefix"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.ExportJobResponse.Definition)
	assert.Equal(t, "arn:aws:iam::123:role/my-role", aws.ToString(out.ExportJobResponse.Definition.RoleArn))
	assert.Equal(t, "s3://my-bucket/prefix", aws.ToString(out.ExportJobResponse.Definition.S3UrlPrefix))

	getOut, err := client.GetExportJob(t.Context(), &pinpointsdk.GetExportJobInput{
		ApplicationId: aws.String(appID),
		JobId:         out.ExportJobResponse.Id,
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.ExportJobResponse.Definition)
	assert.Equal(t, "arn:aws:iam::123:role/my-role", aws.ToString(getOut.ExportJobResponse.Definition.RoleArn))
	assert.Equal(t, "s3://my-bucket/prefix", aws.ToString(getOut.ExportJobResponse.Definition.S3UrlPrefix))
}

// TestImportJobFieldsPersisted_RealClient is the same nesting bug as
// TestExportJobFieldsPersisted_RealClient, for ImportJobResponse/
// types.ImportJobResource.
func TestImportJobFieldsPersisted_RealClient(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	client := newTestPinpointClient(t, h)

	appOut, err := client.CreateApp(t.Context(), &pinpointsdk.CreateAppInput{
		CreateApplicationRequest: &types.CreateApplicationRequest{Name: aws.String("import-fields-app")},
	})
	require.NoError(t, err)
	appID := aws.ToString(appOut.ApplicationResponse.Id)

	out, err := client.CreateImportJob(t.Context(), &pinpointsdk.CreateImportJobInput{
		ApplicationId: aws.String(appID),
		ImportJobRequest: &types.ImportJobRequest{
			RoleArn: aws.String("arn:aws:iam::123:role/my-import-role"),
			S3Url:   aws.String("s3://my-bucket/data.csv"),
			Format:  types.FormatCsv,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.ImportJobResponse.Definition)
	assert.Equal(t, "arn:aws:iam::123:role/my-import-role", aws.ToString(out.ImportJobResponse.Definition.RoleArn))
	assert.Equal(t, "s3://my-bucket/data.csv", aws.ToString(out.ImportJobResponse.Definition.S3Url))
	assert.Equal(t, types.FormatCsv, out.ImportJobResponse.Definition.Format)

	getOut, err := client.GetImportJob(t.Context(), &pinpointsdk.GetImportJobInput{
		ApplicationId: aws.String(appID),
		JobId:         out.ImportJobResponse.Id,
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.ImportJobResponse.Definition)
	assert.Equal(t, "arn:aws:iam::123:role/my-import-role", aws.ToString(getOut.ImportJobResponse.Definition.RoleArn))
	assert.Equal(t, "s3://my-bucket/data.csv", aws.ToString(getOut.ImportJobResponse.Definition.S3Url))
}

// ──────────────────────────────────────────────────
// Journey ARN is returned
// ──────────────────────────────────────────────────

func TestHandler_CreateExportJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantID     bool
	}{
		{
			name:       "creates_export_job",
			body:       map[string]any{"RoleArn": "arn:aws:iam::123:role/export", "S3UrlPrefix": "s3://bucket/prefix"},
			wantStatus: http.StatusCreated,
			wantID:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "export-test-app")

			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/jobs/export", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				assert.NotEmpty(t, resp["Id"])
				assert.Equal(t, appID, resp["ApplicationId"])
				assert.Equal(t, "CREATED", resp["JobStatus"])
				assert.Equal(t, "EXPORT", resp["Type"])
			}
		})
	}
}

func TestHandler_CreateImportJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantID     bool
	}{
		{
			name: "creates_import_job",
			body: map[string]any{
				"RoleArn": "arn:aws:iam::123:role/import",
				"Format":  "CSV",
				"S3Url":   "s3://bucket/data.csv",
			},
			wantStatus: http.StatusCreated,
			wantID:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			appID := createTestApp(t, h, "import-test-app")

			rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/jobs/import", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				assert.NotEmpty(t, resp["Id"])
				assert.Equal(t, appID, resp["ApplicationId"])
				assert.Equal(t, "CREATED", resp["JobStatus"])
				assert.Equal(t, "IMPORT", resp["Type"])
			}
		})
	}
}

func TestGetSegmentImportJobs_FiltersBySegment(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "seg-import-filter-app")

	// Create import job — backend creates a new IMPORT segment.
	importRec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/jobs/import",
		map[string]any{
			"RoleArn":     "arn:aws:iam::123456789012:role/ImportRole",
			"S3Url":       "s3://bucket/data.csv",
			"Format":      "CSV",
			"SegmentName": "my-import-segment",
		})
	require.Equal(t, http.StatusCreated, importRec.Code)

	// Find the segment that the import job created.
	segsRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments", nil)
	require.Equal(t, http.StatusOK, segsRec.Code)
	var segsResp map[string]any
	require.NoError(t, json.Unmarshal(segsRec.Body.Bytes(), &segsResp))

	items, _ := segsResp["Item"].([]any)
	require.Len(t, items, 1, "import job must materialise exactly one segment")
	importSegmentID := items[0].(map[string]any)["Id"].(string)

	// GetSegmentImportJobs for the import segment → must return 1 job.
	jobsRec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/segments/"+importSegmentID+"/jobs/import", nil)
	require.Equal(t, http.StatusOK, jobsRec.Code)

	var jobsResp map[string]any
	require.NoError(t, json.Unmarshal(jobsRec.Body.Bytes(), &jobsResp))

	jobs, _ := jobsResp["Item"].([]any)
	assert.Len(t, jobs, 1, "GetSegmentImportJobs must return the job that created the segment")
}

func TestGetSegmentImportJobs_OtherSegmentReturnsEmpty(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "seg-import-empty-app")

	// Create import job (creates its own IMPORT segment).
	doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/jobs/import",
		map[string]any{
			"RoleArn": "arn:aws:iam::123456789012:role/r",
			"S3Url":   "s3://b/f.csv",
			"Format":  "CSV",
		})

	// Create a second, unrelated DIMENSIONAL segment.
	segRec := doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/segments",
		map[string]any{"Name": "unrelated-segment"})
	require.Equal(t, http.StatusCreated, segRec.Code)
	var segResp map[string]any
	require.NoError(t, json.Unmarshal(segRec.Body.Bytes(), &segResp))
	unrelatedSegID := segResp["Id"].(string)

	// GetSegmentImportJobs for the unrelated segment → must return empty list.
	jobsRec := doPinpointRequest(t, h, http.MethodGet,
		"/v1/apps/"+appID+"/segments/"+unrelatedSegID+"/jobs/import", nil)
	require.Equal(t, http.StatusOK, jobsRec.Code)

	var jobsResp map[string]any
	require.NoError(t, json.Unmarshal(jobsRec.Body.Bytes(), &jobsResp))

	jobs, _ := jobsResp["Item"].([]any)
	assert.Empty(t, jobs, "GetSegmentImportJobs must return empty for segments not created by an import job")
}

func TestGetSegmentImportJobs_MultipleJobs_IsolatedBySegment(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "seg-import-multi-app")

	// Create two import jobs, each creating its own segment.
	doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/jobs/import",
		map[string]any{
			"RoleArn":     "arn:aws:iam::123:role/r",
			"S3Url":       "s3://b/a.csv",
			"Format":      "CSV",
			"SegmentName": "seg-a",
		})
	doPinpointRequest(t, h, http.MethodPost,
		"/v1/apps/"+appID+"/jobs/import",
		map[string]any{
			"RoleArn":     "arn:aws:iam::123:role/r",
			"S3Url":       "s3://b/b.csv",
			"Format":      "CSV",
			"SegmentName": "seg-b",
		})

	segsRec := doPinpointRequest(t, h, http.MethodGet, "/v1/apps/"+appID+"/segments", nil)
	require.Equal(t, http.StatusOK, segsRec.Code)
	var segsResp map[string]any
	require.NoError(t, json.Unmarshal(segsRec.Body.Bytes(), &segsResp))
	items, _ := segsResp["Item"].([]any)
	require.Len(t, items, 2)

	// Each segment's import job endpoint must return exactly 1 job (not 2).
	for _, item := range items {
		seg := item.(map[string]any)
		segID := seg["Id"].(string)

		jobsRec := doPinpointRequest(t, h, http.MethodGet,
			"/v1/apps/"+appID+"/segments/"+segID+"/jobs/import", nil)
		require.Equal(t, http.StatusOK, jobsRec.Code)

		var jobsResp map[string]any
		require.NoError(t, json.Unmarshal(jobsRec.Body.Bytes(), &jobsResp))
		jobs, _ := jobsResp["Item"].([]any)
		assert.Len(t, jobs, 1,
			"each segment's GetSegmentImportJobs must return only its own job, segment %s", segID)
	}
}
