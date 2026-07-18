package pinpoint_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

// newTestBackend creates a bare InMemoryBackend for tests that exercise the
// backend directly without going through the HTTP handler.
func newTestBackend() *pinpoint.InMemoryBackend {
	return pinpoint.NewInMemoryBackend("us-east-1", "123456789012")
}

func TestBackendReset(t *testing.T) {
	t.Parallel()

	b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")

	_, err := b.CreateApp("us-east-1", "123456789012", "reset-app", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, pinpoint.AppCount(b))

	b.Reset()
	assert.Equal(t, 0, pinpoint.AppCount(b))
	assert.Equal(t, 0, pinpoint.ARNIndexSize(b))
}

func TestAddAppInternal(t *testing.T) {
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

func TestAddCampaignInternal(t *testing.T) {
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

func TestAddSegmentInternal(t *testing.T) {
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

func TestAddJourneyInternal(t *testing.T) {
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

func TestDeleteAppRemovesFromARNIndex(t *testing.T) {
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

func TestCountHelpers(t *testing.T) {
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

func TestCountHelpersViaBackend(t *testing.T) {
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
