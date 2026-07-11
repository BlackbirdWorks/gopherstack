package pinpoint_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

// ──────────────────────────────────────────────────
// Persistence (Snapshot / Restore)
// ──────────────────────────────────────────────────

func TestRefinement1_SnapshotRestore(t *testing.T) {
	t.Parallel()

	b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")

	app, err := b.CreateApp("us-east-1", "123456789012", "snap-app", map[string]string{"env": "test"})
	require.NoError(t, err)

	_, err = b.CreateCampaign("us-east-1", "123456789012", app.ID, pinpoint.ExportedCreateCampaignRequest{
		Name:      "snap-campaign",
		SegmentID: "seg-1",
	})
	require.NoError(t, err)

	data := b.Snapshot(t.Context())
	require.NotNil(t, data)

	b2 := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")
	require.NoError(t, b2.Restore(t.Context(), data))

	assert.Equal(t, 1, pinpoint.AppCount(b2))
	assert.Equal(t, 1, pinpoint.CampaignCount(b2))

	// ARN index should be rebuilt after restore.
	assert.Equal(t, pinpoint.ARNIndexSize(b2), pinpoint.ARNIndexSize(b))
}

func TestRefinement1_SnapshotRestoreEmpty(t *testing.T) {
	t.Parallel()

	b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")
	data := b.Snapshot(t.Context())
	require.NotNil(t, data)

	b2 := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")
	require.NoError(t, b2.Restore(t.Context(), data))

	assert.Equal(t, 0, pinpoint.AppCount(b2))
}

func TestRefinement1_RestoreInvalidJSON(t *testing.T) {
	t.Parallel()

	b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")
	err := b.Restore(t.Context(), []byte("not-json"))
	require.Error(t, err)
}

// ──────────────────────────────────────────────────
// Reset
// ──────────────────────────────────────────────────

func TestRefinement1_BackendReset(t *testing.T) {
	t.Parallel()

	b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")

	_, err := b.CreateApp("us-east-1", "123456789012", "reset-app", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, pinpoint.AppCount(b))

	b.Reset()
	assert.Equal(t, 0, pinpoint.AppCount(b))
	assert.Equal(t, 0, pinpoint.ARNIndexSize(b))
}

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps", map[string]any{"Name": "before-reset"})
	require.Equal(t, http.StatusCreated, rec.Code)

	h.Reset()

	rec2 := doPinpointRequest(t, h, http.MethodGet, "/v1/apps", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec2.Body).Decode(&resp))
	items, _ := resp["Item"].([]any)
	assert.Empty(t, items)
}

// ──────────────────────────────────────────────────
// Backend lifecycle methods
// ──────────────────────────────────────────────────

func TestRefinement1_BackendRegionAccountID(t *testing.T) {
	t.Parallel()

	b := pinpoint.NewInMemoryBackend("eu-west-1", "999888777666")
	assert.Equal(t, "eu-west-1", b.Region())
	assert.Equal(t, "999888777666", b.AccountID())
}

// ──────────────────────────────────────────────────
// ErrNilAppContext
// ──────────────────────────────────────────────────

func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &pinpoint.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, pinpoint.ErrNilAppContext)
}

// ──────────────────────────────────────────────────
// ARN index / tag operations on all resource types
// ──────────────────────────────────────────────────

func TestRefinement1_TagCampaignViaARN(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "tag-campaign-app")

	recC := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/campaigns",
		map[string]any{"Name": "taggable-campaign", "SegmentId": "seg-1"})
	require.Equal(t, http.StatusCreated, recC.Code)

	var cResp map[string]any
	require.NoError(t, json.NewDecoder(recC.Body).Decode(&cResp))
	campaignARN, _ := cResp["Arn"].(string)
	require.NotEmpty(t, campaignARN)

	// Tag the campaign via /v1/tags/{arn}
	tagRec := doPinpointRequest(t, h, http.MethodPost, "/v1/tags/"+campaignARN,
		map[string]any{"tags": map[string]string{"cost-center": "42"}})
	assert.Equal(t, http.StatusNoContent, tagRec.Code)

	// List tags
	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/tags/"+campaignARN, nil)
	assert.Equal(t, http.StatusOK, listRec.Code)

	var tagsBody map[string]any
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&tagsBody))
	tagsMap, _ := tagsBody["tags"].(map[string]any)
	assert.Equal(t, "42", tagsMap["cost-center"])
}

func TestRefinement1_TagJourneyViaARN(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "tag-journey-app")

	recJ := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/journeys",
		map[string]any{"Name": "taggable-journey"})
	require.Equal(t, http.StatusCreated, recJ.Code)

	var jResp map[string]any
	require.NoError(t, json.NewDecoder(recJ.Body).Decode(&jResp))
	journeyARN, _ := jResp["Arn"].(string)
	require.NotEmpty(t, journeyARN)

	tagRec := doPinpointRequest(t, h, http.MethodPost, "/v1/tags/"+journeyARN,
		map[string]any{"tags": map[string]string{"team": "growth"}})
	assert.Equal(t, http.StatusNoContent, tagRec.Code)

	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/tags/"+journeyARN, nil)
	assert.Equal(t, http.StatusOK, listRec.Code)
}

func TestRefinement1_TagSegmentViaARN(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "tag-segment-app")

	recS := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/segments",
		map[string]any{"Name": "taggable-segment"})
	require.Equal(t, http.StatusCreated, recS.Code)

	var sResp map[string]any
	require.NoError(t, json.NewDecoder(recS.Body).Decode(&sResp))
	segmentARN, _ := sResp["Arn"].(string)
	require.NotEmpty(t, segmentARN)

	tagRec := doPinpointRequest(t, h, http.MethodPost, "/v1/tags/"+segmentARN,
		map[string]any{"tags": map[string]string{"segment-owner": "analytics"}})
	assert.Equal(t, http.StatusNoContent, tagRec.Code)
}

// ──────────────────────────────────────────────────
// Template uniqueness (409 Conflict)
// ──────────────────────────────────────────────────

func TestRefinement1_DuplicateEmailTemplate(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec1 := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/dup-email/email",
		map[string]any{"Subject": "Hello"})
	assert.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/dup-email/email",
		map[string]any{"Subject": "Hello again"})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestRefinement1_DuplicatePushTemplate(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec1 := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/dup-push/push", map[string]any{})
	assert.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/dup-push/push", map[string]any{})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestRefinement1_DuplicateSmsTemplate(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec1 := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/dup-sms/sms",
		map[string]any{"Body": "Hi"})
	assert.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/dup-sms/sms",
		map[string]any{"Body": "Hi again"})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestRefinement1_DuplicateInAppTemplate(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec1 := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/dup-inapp/inapp", map[string]any{})
	assert.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/dup-inapp/inapp", map[string]any{})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

// ──────────────────────────────────────────────────
// App-existence validation for app-scoped ops
// ──────────────────────────────────────────────────

func TestRefinement1_CreateCampaignAppNotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/nonexistent/campaigns",
		map[string]any{"Name": "orphan-campaign"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRefinement1_CreateJourneyAppNotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/nonexistent/journeys",
		map[string]any{"Name": "orphan-journey"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRefinement1_CreateSegmentAppNotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/nonexistent/segments",
		map[string]any{"Name": "orphan-segment"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRefinement1_CreateExportJobAppNotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/nonexistent/jobs/export",
		map[string]any{"RoleArn": "arn:aws:iam::123:role/r", "S3UrlPrefix": "s3://b/p"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRefinement1_CreateImportJobAppNotFound(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/nonexistent/jobs/import",
		map[string]any{"RoleArn": "arn:aws:iam::123:role/r", "S3Url": "s3://b/f.csv", "Format": "CSV"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ──────────────────────────────────────────────────
// Required-field validation for jobs
// ──────────────────────────────────────────────────

func TestRefinement1_CreateExportJobMissingRoleArn(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "export-validation-app")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/jobs/export",
		map[string]any{"S3UrlPrefix": "s3://bucket/prefix"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_CreateExportJobMissingS3Prefix(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "export-validation-app2")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/jobs/export",
		map[string]any{"RoleArn": "arn:aws:iam::123:role/r"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_CreateImportJobMissingRoleArn(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "import-validation-app")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/jobs/import",
		map[string]any{"S3Url": "s3://bucket/file.csv", "Format": "CSV"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_CreateImportJobMissingFormat(t *testing.T) {
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

func TestRefinement1_ExportJobFieldsPersisted(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "export-fields-app")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/jobs/export",
		map[string]any{
			"RoleArn":     "arn:aws:iam::123:role/my-role",
			"S3UrlPrefix": "s3://my-bucket/prefix",
		})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "arn:aws:iam::123:role/my-role", resp["RoleArn"])
	assert.Equal(t, "s3://my-bucket/prefix", resp["S3UrlPrefix"])
	assert.NotEmpty(t, resp["Arn"])
}

func TestRefinement1_ImportJobFieldsPersisted(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "import-fields-app")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/jobs/import",
		map[string]any{
			"RoleArn": "arn:aws:iam::123:role/my-import-role",
			"S3Url":   "s3://my-bucket/data.csv",
			"Format":  "CSV",
		})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.Equal(t, "arn:aws:iam::123:role/my-import-role", resp["RoleArn"])
	assert.Equal(t, "s3://my-bucket/data.csv", resp["S3Url"])
	assert.Equal(t, "CSV", resp["Format"])
	assert.NotEmpty(t, resp["Arn"])
}

// ──────────────────────────────────────────────────
// Journey ARN is returned
// ──────────────────────────────────────────────────

func TestRefinement1_JourneyARNPresent(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "journey-arn-app")

	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/journeys",
		map[string]any{"Name": "arn-journey"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	assert.NotEmpty(t, resp["Arn"])
	assert.Contains(t, resp["Arn"].(string), appID)
}

// ──────────────────────────────────────────────────
// Seed helpers
// ──────────────────────────────────────────────────

func TestRefinement1_AddAppInternal(t *testing.T) {
	t.Parallel()

	b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")
	b.AddAppInternal(&pinpoint.App{
		ID:   "seeded-app-id",
		Name: "seeded-app",
		ARN:  "arn:aws:mobiletargeting:us-east-1:123456789012:apps/seeded-app-id",
		Tags: map[string]string{"seeded": "true"},
	})

	assert.Equal(t, 1, pinpoint.AppCount(b))
	assert.Equal(t, 1, pinpoint.ARNIndexSize(b))
}

func TestRefinement1_AddCampaignInternal(t *testing.T) {
	t.Parallel()

	b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")
	b.AddCampaignInternal(&pinpoint.Campaign{
		ID:            "seeded-campaign-id",
		ApplicationID: "app-1",
		ARN:           "arn:aws:mobiletargeting:us-east-1:123456789012:apps/app-1/campaigns/seeded-campaign-id",
		Name:          "seeded-campaign",
	})

	assert.Equal(t, 1, pinpoint.CampaignCount(b))
	assert.Equal(t, 1, pinpoint.ARNIndexSize(b))
}

func TestRefinement1_AddSegmentInternal(t *testing.T) {
	t.Parallel()

	b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")
	b.AddSegmentInternal(&pinpoint.Segment{
		ID:            "seeded-segment-id",
		ApplicationID: "app-1",
		ARN:           "arn:aws:mobiletargeting:us-east-1:123456789012:apps/app-1/segments/seeded-segment-id",
		Name:          "seeded-segment",
		SegmentType:   "DIMENSIONAL",
	})

	assert.Equal(t, 1, pinpoint.SegmentCount(b))
	assert.Equal(t, 1, pinpoint.ARNIndexSize(b))
}

func TestRefinement1_AddJourneyInternal(t *testing.T) {
	t.Parallel()

	b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")
	b.AddJourneyInternal(&pinpoint.Journey{
		ID:            "seeded-journey-id",
		ApplicationID: "app-1",
		ARN:           "arn:aws:mobiletargeting:us-east-1:123456789012:apps/app-1/journeys/seeded-journey-id",
		Name:          "seeded-journey",
		State:         "DRAFT",
	})

	assert.Equal(t, 1, pinpoint.JourneyCount(b))
	assert.Equal(t, 1, pinpoint.ARNIndexSize(b))
}

// ──────────────────────────────────────────────────
// HandlerOpsLen
// ──────────────────────────────────────────────────

func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	assert.Equal(t, 122, pinpoint.HandlerOpsLen(h))
}

// ──────────────────────────────────────────────────
// Method-not-allowed paths
// ──────────────────────────────────────────────────

func TestRefinement1_MethodNotAllowedOnApps(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodDelete, "/v1/apps", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestRefinement1_MethodNotAllowedOnRecommenders(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doPinpointRequest(t, h, http.MethodDelete, "/v1/recommenders", nil)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

// ──────────────────────────────────────────────────
// Helpers for raw requests (no auto-JSON body)
// ──────────────────────────────────────────────────

func doRawPinpointRequest(
	t *testing.T,
	h *pinpoint.Handler,
	method, path string,
	rawBody []byte,
) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()

	req := httptest.NewRequest(method, path, bytes.NewReader(rawBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/mobiletargeting/aws4_request",
	)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestRefinement1_CreateAppInvalidJSON(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	rec := doRawPinpointRequest(t, h, http.MethodPost, "/v1/apps", []byte("not-json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_CreateCampaignInvalidJSON(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	appID := createTestApp(t, h, "invalid-json-app")

	rec := doRawPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/campaigns", []byte("bad-body"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_DeleteAppRemovesFromARNIndex(t *testing.T) {
	t.Parallel()

	b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")

	app, err := b.CreateApp("us-east-1", "123456789012", "to-delete", nil)
	require.NoError(t, err)

	assert.Equal(t, 1, pinpoint.ARNIndexSize(b))

	_, err = b.DeleteApp(app.ID)
	require.NoError(t, err)

	assert.Equal(t, 0, pinpoint.ARNIndexSize(b))
}

// ──────────────────────────────────────────────────
// Count helpers coverage
// ──────────────────────────────────────────────────

func TestRefinement1_CountHelpers(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")

	// Email template count
	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/ct-email/email", map[string]any{"Subject": "hi"})
	require.Equal(t, http.StatusCreated, rec.Code)

	// InApp template count
	rec = doPinpointRequest(t, h, http.MethodPost, "/v1/templates/ct-inapp/inapp", map[string]any{})
	require.Equal(t, http.StatusCreated, rec.Code)

	// Push template count
	rec = doPinpointRequest(t, h, http.MethodPost, "/v1/templates/ct-push/push", map[string]any{})
	require.Equal(t, http.StatusCreated, rec.Code)

	// SMS template count
	rec = doPinpointRequest(t, h, http.MethodPost, "/v1/templates/ct-sms/sms", map[string]any{"Body": "test"})
	require.Equal(t, http.StatusCreated, rec.Code)

	// Export job count
	appID := createTestApp(t, h, "count-app")
	rec = doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/jobs/export",
		map[string]any{"RoleArn": "arn:aws:iam::123:role/r", "S3UrlPrefix": "s3://b/p"})
	require.Equal(t, http.StatusCreated, rec.Code)

	// Import job count
	rec = doPinpointRequest(t, h, http.MethodPost, "/v1/apps/"+appID+"/jobs/import",
		map[string]any{"RoleArn": "arn:aws:iam::123:role/r", "S3Url": "s3://b/f.csv", "Format": "CSV"})
	require.Equal(t, http.StatusCreated, rec.Code)

	// Recommender count
	rec = doPinpointRequest(t, h, http.MethodPost, "/v1/recommenders",
		map[string]any{
			"Name":                          "cnt-recommender",
			"RecommendationProviderRoleArn": "arn:aws:iam::123:role/r",
			"RecommendationProviderUri":     "arn:aws:personalize:us-east-1:123:campaign/c",
		})
	require.Equal(t, http.StatusCreated, rec.Code)

	// Verify empty counts via a new backend
	assert.Equal(t, 0, pinpoint.EmailTemplateCount(b))
	assert.Equal(t, 0, pinpoint.InAppTemplateCount(b))
	assert.Equal(t, 0, pinpoint.PushTemplateCount(b))
	assert.Equal(t, 0, pinpoint.SmsTemplateCount(b))
	assert.Equal(t, 0, pinpoint.ExportJobCount(b))
	assert.Equal(t, 0, pinpoint.ImportJobCount(b))
	assert.Equal(t, 0, pinpoint.RecommenderCount(b))
}

func TestRefinement1_CountHelpersViaBackend(t *testing.T) {
	t.Parallel()

	b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")

	app, err := b.CreateApp("us-east-1", "123456789012", "count-app", nil)
	require.NoError(t, err)

	_, err = b.CreateCampaign("us-east-1", "123456789012", app.ID, pinpoint.ExportedCreateCampaignRequest{Name: "c1"})
	require.NoError(t, err)

	_, err = b.CreateEmailTemplate(
		"us-east-1",
		"123456789012",
		"email-tpl",
		pinpoint.ExportedCreateEmailTemplateRequest{},
	)
	require.NoError(t, err)

	_, err = b.CreateInAppTemplate(
		"us-east-1",
		"123456789012",
		"inapp-tpl",
		pinpoint.ExportedCreateInAppTemplateRequest{},
	)
	require.NoError(t, err)

	_, err = b.CreatePushTemplate("us-east-1", "123456789012", "push-tpl", pinpoint.ExportedCreatePushTemplateRequest{})
	require.NoError(t, err)

	_, err = b.CreateSmsTemplate("us-east-1", "123456789012", "sms-tpl", pinpoint.ExportedCreateSmsTemplateRequest{})
	require.NoError(t, err)

	_, err = b.CreateExportJob("us-east-1", "123456789012", app.ID, pinpoint.ExportedCreateExportJobRequest{
		RoleArn: "arn:aws:iam::123:role/r", S3UrlPrefix: "s3://b/p",
	})
	require.NoError(t, err)

	_, err = b.CreateImportJob("us-east-1", "123456789012", app.ID, pinpoint.ExportedCreateImportJobRequest{
		RoleArn: "arn:aws:iam::123:role/r", S3Url: "s3://b/f.csv", Format: "CSV",
	})
	require.NoError(t, err)

	_, err = b.CreateJourney("us-east-1", "123456789012", app.ID, pinpoint.ExportedCreateJourneyRequest{Name: "j1"})
	require.NoError(t, err)

	_, err = b.CreateSegment("us-east-1", "123456789012", app.ID, pinpoint.ExportedCreateSegmentRequest{Name: "s1"})
	require.NoError(t, err)

	_, err = b.CreateRecommenderConfiguration(pinpoint.ExportedCreateRecommenderConfigRequest{Name: "r1"})
	require.NoError(t, err)

	assert.Equal(t, 1, pinpoint.AppCount(b))
	assert.Equal(t, 1, pinpoint.CampaignCount(b))
	assert.Equal(t, 1, pinpoint.EmailTemplateCount(b))
	assert.Equal(t, 1, pinpoint.InAppTemplateCount(b))
	assert.Equal(t, 1, pinpoint.PushTemplateCount(b))
	assert.Equal(t, 1, pinpoint.SmsTemplateCount(b))
	assert.Equal(t, 1, pinpoint.ExportJobCount(b))
	assert.Equal(t, 1, pinpoint.ImportJobCount(b))
	assert.Equal(t, 1, pinpoint.JourneyCount(b))
	// CreateImportJob materialises an IMPORT-type segment, so count is 2:
	// one from CreateSegment and one from CreateImportJob.
	assert.Equal(t, 2, pinpoint.SegmentCount(b))
	assert.Equal(t, 1, pinpoint.RecommenderCount(b))
}

func TestRefinement1_TagEmailTemplateViaARN(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)

	// Create email template
	rec := doPinpointRequest(t, h, http.MethodPost, "/v1/templates/tag-email/email",
		map[string]any{"Subject": "tag-me"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var tResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&tResp))
	templateARN, _ := tResp["Arn"].(string)
	require.NotEmpty(t, templateARN)

	// Tag it
	tagRec := doPinpointRequest(t, h, http.MethodPost, "/v1/tags/"+templateARN,
		map[string]any{"tags": map[string]string{"type": "email"}})
	assert.Equal(t, http.StatusNoContent, tagRec.Code)

	// List tags
	listRec := doPinpointRequest(t, h, http.MethodGet, "/v1/tags/"+templateARN, nil)
	assert.Equal(t, http.StatusOK, listRec.Code)

	var listBody map[string]any
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listBody))
	tagsMap, _ := listBody["tags"].(map[string]any)
	assert.Equal(t, "email", tagsMap["type"])

	// Untag
	untagRec := doPinpointRequest(t, h, http.MethodDelete, "/v1/tags/"+templateARN+"?tagKeys=type", nil)
	assert.Equal(t, http.StatusNoContent, untagRec.Code)
}

func TestRefinement1_SnapshotRestoreARNIndexIntegrity(t *testing.T) {
	t.Parallel()

	b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")

	app, err := b.CreateApp("us-east-1", "123456789012", "snap-int-app", nil)
	require.NoError(t, err)

	_, err = b.CreateCampaign("us-east-1", "123456789012", app.ID,
		pinpoint.ExportedCreateCampaignRequest{Name: "snap-int-campaign"})
	require.NoError(t, err)

	_, err = b.CreateJourney("us-east-1", "123456789012", app.ID,
		pinpoint.ExportedCreateJourneyRequest{Name: "snap-int-journey"})
	require.NoError(t, err)

	_, err = b.CreateSegment("us-east-1", "123456789012", app.ID,
		pinpoint.ExportedCreateSegmentRequest{Name: "snap-int-segment"})
	require.NoError(t, err)

	originalARNSize := pinpoint.ARNIndexSize(b)
	data := b.Snapshot(t.Context())
	require.NotNil(t, data)

	b2 := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")
	require.NoError(t, b2.Restore(t.Context(), data))

	// ARN index should be fully rebuilt.
	assert.Equal(t, originalARNSize, pinpoint.ARNIndexSize(b2))

	// Tags on restored app should work via ARN index.
	require.NoError(t, b2.TagResource(app.ARN, map[string]string{"restored": "true"}))
	tags, err := b2.ListTagsForResource(app.ARN)
	require.NoError(t, err)
	assert.Equal(t, "true", tags["restored"])
}

// TestSnapshotRestore_FullStateRoundTrip exercises a Snapshot->Restore round
// trip across every store.Table the Phase 3.3 datalayer conversion produced,
// covering both the persisted resource kinds (apps, campaigns, segments, the
// four templates, export/import jobs, journeys, recommenders) and the
// resource kinds that were never part of a persisted snapshot before the
// conversion (voice templates, endpoints, event streams, channels) to confirm
// the persisted/non-persisted split was preserved exactly.
func TestSnapshotRestore_FullStateRoundTrip(t *testing.T) {
	t.Parallel()

	region, accountID := "us-east-1", "123456789012"
	b := pinpoint.NewInMemoryBackend(region, accountID)

	app, err := b.CreateApp(region, accountID, "full-state-app", map[string]string{"env": "test"})
	require.NoError(t, err)

	_, err = b.CreateCampaign(region, accountID, app.ID, pinpoint.ExportedCreateCampaignRequest{Name: "c1"})
	require.NoError(t, err)

	_, err = b.CreateSegment(region, accountID, app.ID, pinpoint.ExportedCreateSegmentRequest{Name: "s1"})
	require.NoError(t, err)

	_, err = b.CreateJourney(region, accountID, app.ID, pinpoint.ExportedCreateJourneyRequest{Name: "j1"})
	require.NoError(t, err)

	_, err = b.CreateEmailTemplate(region, accountID, "email-t1",
		pinpoint.ExportedCreateEmailTemplateRequest{Subject: "hi"})
	require.NoError(t, err)

	_, err = b.CreateInAppTemplate(region, accountID, "inapp-t1", pinpoint.ExportedCreateInAppTemplateRequest{})
	require.NoError(t, err)

	_, err = b.CreatePushTemplate(region, accountID, "push-t1", pinpoint.ExportedCreatePushTemplateRequest{})
	require.NoError(t, err)

	_, err = b.CreateSmsTemplate(region, accountID, "sms-t1", pinpoint.ExportedCreateSmsTemplateRequest{})
	require.NoError(t, err)

	_, err = b.CreateExportJob(region, accountID, app.ID,
		pinpoint.ExportedCreateExportJobRequest{RoleArn: "role-1"})
	require.NoError(t, err)

	_, err = b.CreateImportJob(region, accountID, app.ID,
		pinpoint.ExportedCreateImportJobRequest{RoleArn: "role-1", S3Url: "s3://bucket/key"})
	require.NoError(t, err)

	_, err = b.CreateRecommenderConfiguration(pinpoint.ExportedCreateRecommenderConfigRequest{
		Name:                         "rec-1",
		RecommendationProviderIDType: "PINPOINT_ENDPOINT_ID",
	})
	require.NoError(t, err)

	// Resource kinds that have never been part of a persisted snapshot.
	require.NoError(t, pinpoint.CreateVoiceTemplateForTest(b, region, accountID, "voice-t1", "hello"))
	require.NoError(t, pinpoint.UpdateEndpointForTest(b, app.ID, "endpoint-1", "user@example.com"))
	streamARN := "arn:aws:kinesis:us-east-1:123456789012:stream/x"
	require.NoError(t, pinpoint.PutEventStreamForTest(b, app.ID, streamARN, "role-1"))
	b.UpsertChannel(app.ID, "EMAIL", true, nil)

	data := b.Snapshot(t.Context())
	require.NotNil(t, data)

	b2 := pinpoint.NewInMemoryBackend(region, accountID)
	require.NoError(t, b2.Restore(t.Context(), data))

	// Persisted resource kinds must all survive the round trip.
	assert.Equal(t, 1, pinpoint.AppCount(b2))
	assert.Equal(t, 1, pinpoint.CampaignCount(b2))
	// CreateImportJob materialises an additional IMPORT-type segment (AWS
	// behaviour), so the explicit segment created above plus that one gives 2.
	assert.Equal(t, 2, pinpoint.SegmentCount(b2))
	assert.Equal(t, 1, pinpoint.JourneyCount(b2))
	assert.Equal(t, 1, pinpoint.EmailTemplateCount(b2))
	assert.Equal(t, 1, pinpoint.InAppTemplateCount(b2))
	assert.Equal(t, 1, pinpoint.PushTemplateCount(b2))
	assert.Equal(t, 1, pinpoint.SmsTemplateCount(b2))
	assert.Equal(t, 1, pinpoint.ExportJobCount(b2))
	assert.Equal(t, 1, pinpoint.ImportJobCount(b2))
	assert.Equal(t, 1, pinpoint.RecommenderCount(b2))

	// Non-persisted resource kinds must NOT survive: this is the historical
	// behaviour, not a regression, and the round trip must preserve it.
	_, err = b2.GetVoiceTemplate("voice-t1")
	require.ErrorIs(t, err, pinpoint.ErrAppNotFound)

	_, err = b2.GetEndpoint(app.ID, "endpoint-1")
	require.ErrorIs(t, err, pinpoint.ErrAppNotFound)

	_, err = b2.GetEventStream(app.ID)
	require.ErrorIs(t, err, pinpoint.ErrAppNotFound)

	// GetChannel synthesises a disabled default when no channel is stored,
	// which is what a never-persisted channel must look like post-restore.
	ch := b2.GetChannel(app.ID, "EMAIL")
	assert.False(t, ch.Enabled)
}
