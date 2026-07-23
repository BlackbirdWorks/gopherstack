package pinpoint_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

// Test_Handler_SnapshotRestore verifies Handler.Snapshot/Restore
// (persistence.go) delegate to the backend -- the shape persistence.Manager
// actually drives. cli.go's setupPersistence registers a service.Registerable
// (the *Handler returned by Provider.Init) in the persistence.Manager only if
// that Handler itself satisfies Snapshot(ctx)/Restore(ctx, []byte);
// InMemoryBackend implementing the same two methods (exercised directly by
// TestRefinement1_SnapshotRestore) is not enough on its own, since
// Handler.Backend is the StorageBackend interface and does not promote them.
// Mirrors services/securityhub's Test_Handler_SnapshotRestore.
func Test_Handler_SnapshotRestore(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")
	h := pinpoint.NewHandler(backend)

	// Compile-time proof Handler satisfies the persistence layer's contract.
	var _ persistence.Persistable = h

	app, err := backend.CreateApp("us-east-1", "123456789012", "handler-app", map[string]string{"env": "test"})
	require.NoError(t, err)

	data := h.Snapshot(ctx)
	require.NotEmpty(t, data)

	restoredBackend := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")
	restoredHandler := pinpoint.NewHandler(restoredBackend)
	require.NoError(t, restoredHandler.Restore(ctx, data))

	assert.Equal(t, 1, pinpoint.AppCount(restoredBackend))

	got, err := restoredBackend.GetApp(app.ID)
	require.NoError(t, err)
	assert.Equal(t, "handler-app", got.Name)
}

func TestSnapshotRestore(t *testing.T) {
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

func TestSnapshotRestoreEmpty(t *testing.T) {
	t.Parallel()

	b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")
	data := b.Snapshot(t.Context())
	require.NotNil(t, data)

	b2 := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")
	require.NoError(t, b2.Restore(t.Context(), data))

	assert.Equal(t, 0, pinpoint.AppCount(b2))
}

func TestRestoreInvalidJSON(t *testing.T) {
	t.Parallel()

	b := pinpoint.NewInMemoryBackend("us-east-1", "123456789012")
	err := b.Restore(t.Context(), []byte("not-json"))
	require.Error(t, err)
}

// ──────────────────────────────────────────────────
// Reset
// ──────────────────────────────────────────────────

func TestSnapshotRestoreARNIndexIntegrity(t *testing.T) {
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
// covering every persisted resource kind: apps, campaigns, segments, the
// five templates (including voice), export/import jobs, journeys,
// recommenders, endpoints, event streams, and channels. Voice
// templates/endpoints/event streams/channels were historically excluded from
// the snapshot (a real parity gap, since AWS Pinpoint has no such
// distinction) -- this test now locks that they DO survive a restart.
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

	// Resource kinds that were historically excluded from the snapshot.
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

	// Voice templates/endpoints/event streams/channels must now survive a
	// restart -- these are store.Table-backed the same as every other
	// resource kind, and excluding them from the snapshot was a parity gap,
	// not intentional AWS-accurate behaviour.
	voiceTmpl, err := b2.GetVoiceTemplate("voice-t1")
	require.NoError(t, err)
	assert.Equal(t, "hello", voiceTmpl.Body)

	endpoint, err := b2.GetEndpoint(app.ID, "endpoint-1")
	require.NoError(t, err)
	assert.Equal(t, "user@example.com", endpoint.Address)

	stream, err := b2.GetEventStream(app.ID)
	require.NoError(t, err)
	assert.Equal(t, streamARN, stream.DestinationStreamArn)

	ch := b2.GetChannel(app.ID, "EMAIL")
	assert.True(t, ch.Enabled)
}
