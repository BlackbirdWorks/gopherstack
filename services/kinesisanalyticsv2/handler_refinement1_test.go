package kinesisanalyticsv2_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

// ─── helpers ────────────────────────────────────────────────────────────────

func newRefinementBackend() *kinesisanalyticsv2.InMemoryBackend {
	return kinesisanalyticsv2.NewInMemoryBackend("000000000000", "us-east-1")
}

func newRefinementHandler(b *kinesisanalyticsv2.InMemoryBackend) *kinesisanalyticsv2.Handler {
	return kinesisanalyticsv2.NewHandler(b)
}

// ─── Reset ──────────────────────────────────────────────────────────────────

func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(*kinesisanalyticsv2.InMemoryBackend)
		name  string
	}{
		{
			name:  "empty backend",
			setup: func(_ *kinesisanalyticsv2.InMemoryBackend) {},
		},
		{
			name: "with applications",
			setup: func(b *kinesisanalyticsv2.InMemoryBackend) {
				ctx := context.Background()
				_, _ = b.CreateApplication(ctx, "app-1", "FLINK-1_18", "", "", "", nil)
				_, _ = b.CreateApplication(ctx, "app-2", "FLINK-1_18", "", "", "", nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newRefinementBackend()
			tt.setup(b)
			b.Reset()

			assert.Zero(t, kinesisanalyticsv2.ApplicationCount(b))

			_, err := b.CreateApplication(context.Background(), "post-reset", "FLINK-1_18", "", "", "", nil)
			require.NoError(t, err)
		})
	}
}

func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newRefinementBackend()

	for range 3 {
		_, _ = b.CreateApplication(ctx, "temp", "FLINK-1_18", "", "", "", nil)
		b.Reset()
		assert.Zero(t, kinesisanalyticsv2.ApplicationCount(b))
	}
}

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newRefinementBackend()
	h := newRefinementHandler(b)

	_, err := b.CreateApplication(ctx, "reset-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	h.Reset()

	assert.Zero(t, kinesisanalyticsv2.ApplicationCount(b))
}

// ─── Provider ───────────────────────────────────────────────────────────────

func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &kinesisanalyticsv2.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, kinesisanalyticsv2.ErrNilAppContext)
}

// ─── Dispatch map pre-built ─────────────────────────────────────────────────

func TestRefinement1_HandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	b := newRefinementBackend()
	h := newRefinementHandler(b)
	assert.Positive(t, kinesisanalyticsv2.HandlerOpsLen(h))
}

func TestRefinement1_GetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	b := newRefinementBackend()
	h := newRefinementHandler(b)
	ops := h.GetSupportedOperations()
	assert.Len(t, ops, kinesisanalyticsv2.HandlerOpsLen(h))
}

// ─── Seed helper ────────────────────────────────────────────────────────────

func TestRefinement1_SeedHelper(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newRefinementBackend()
	appARN := b.GenerateApplicationARN("seeded-app")

	b.AddApplicationInternal(ctx, &kinesisanalyticsv2.Application{
		ApplicationARN:       appARN,
		ApplicationName:      "seeded-app",
		ApplicationStatus:    "READY",
		RuntimeEnvironment:   "FLINK-1_18",
		ApplicationVersionID: 1,
	})

	assert.Equal(t, 1, kinesisanalyticsv2.ApplicationCount(b))

	app, err := b.DescribeApplication(ctx, "seeded-app")
	require.NoError(t, err)
	assert.Equal(t, "seeded-app", app.ApplicationName)
}

// ─── Export count helpers ────────────────────────────────────────────────────

func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newRefinementBackend()

	assert.Zero(t, kinesisanalyticsv2.ApplicationCount(b))
	assert.Zero(t, kinesisanalyticsv2.SnapshotCount(b))

	_, err := b.CreateApplication(ctx, "count-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	assert.Equal(t, 1, kinesisanalyticsv2.ApplicationCount(b))

	_, err = b.CreateApplicationSnapshot(ctx, "count-app", "snap-1")
	require.NoError(t, err)

	assert.Equal(t, 1, kinesisanalyticsv2.SnapshotCount(b))
}

// ─── Deep copy: DescribeApplication ─────────────────────────────────────────

func TestRefinement1_DescribeApplication_DeepCopy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newRefinementBackend()
	_, err := b.CreateApplication(
		ctx,
		"copy-app",
		"FLINK-1_18",
		"",
		"",
		"",
		[]kinesisanalyticsv2.Tag{{Key: "k", Value: "v"}},
	)
	require.NoError(t, err)

	app1, err := b.DescribeApplication(ctx, "copy-app")
	require.NoError(t, err)

	// Mutate returned copy
	app1.Tags[0].Value = "mutated"
	app1.ApplicationDescription = "mutated"

	app2, err := b.DescribeApplication(ctx, "copy-app")
	require.NoError(t, err)

	assert.Equal(t, "v", app2.Tags[0].Value, "mutation of returned copy must not affect stored state")
	assert.Empty(t, app2.ApplicationDescription)
}

// ─── UntagResource: no slice aliasing ───────────────────────────────────────

func TestRefinement1_UntagResource_NoSliceAliasing(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newRefinementBackend()
	app, err := b.CreateApplication(ctx, "untag-app", "FLINK-1_18", "", "", "", []kinesisanalyticsv2.Tag{
		{Key: "a", Value: "1"},
		{Key: "b", Value: "2"},
		{Key: "c", Value: "3"},
	})
	require.NoError(t, err)

	err = b.UntagResource(ctx, app.ApplicationARN, []string{"b"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(ctx, app.ApplicationARN)
	require.NoError(t, err)
	assert.Len(t, tags, 2)
	keys := []string{tags[0].Key, tags[1].Key}
	assert.NotContains(t, keys, "b")
}

// ─── ListTagsForResource: sorted output ─────────────────────────────────────

func TestRefinement1_ListTagsForResource_Sorted(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newRefinementBackend()
	app, err := b.CreateApplication(ctx, "sorted-tag-app", "FLINK-1_18", "", "", "", []kinesisanalyticsv2.Tag{
		{Key: "z", Value: "last"},
		{Key: "a", Value: "first"},
		{Key: "m", Value: "middle"},
	})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(ctx, app.ApplicationARN)
	require.NoError(t, err)
	require.Len(t, tags, 3)
	assert.Equal(t, "a", tags[0].Key)
	assert.Equal(t, "m", tags[1].Key)
	assert.Equal(t, "z", tags[2].Key)
}

// ─── ListTagsForResource: non-nil when empty ─────────────────────────────────

func TestRefinement1_ListTagsForResource_NonNilWhenEmpty(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newRefinementBackend()
	app, err := b.CreateApplication(ctx, "no-tag-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(ctx, app.ApplicationARN)
	require.NoError(t, err)
	assert.NotNil(t, tags)
	assert.Empty(t, tags)
}

// ─── CreateApplication: validation ───────────────────────────────────────────

func TestRefinement1_CreateApplication_RequiresName(t *testing.T) {
	t.Parallel()

	h := newRefinementHandler(newRefinementBackend())
	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"RuntimeEnvironment": "FLINK-1_18",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_CreateApplication_RequiresRuntimeEnvironment(t *testing.T) {
	t.Parallel()

	h := newRefinementHandler(newRefinementBackend())
	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName": "test-app",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ─── CreateApplicationPresignedUrl: URLType required ─────────────────────────

func TestRefinement1_CreateApplicationPresignedURL_RequiresURLType(t *testing.T) {
	t.Parallel()

	h := newRefinementHandler(newRefinementBackend())

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "url-app",
		"RuntimeEnvironment": "FLINK-1_18",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "CreateApplicationPresignedUrl", map[string]any{
		"ApplicationName": "url-app",
		// URLType omitted
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ─── DescribeApplicationSnapshot: direct lookup ──────────────────────────────

func TestRefinement1_DescribeApplicationSnapshot_DirectLookup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newRefinementBackend()

	_, err := b.CreateApplication(ctx, "snap-direct-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	_, err = b.CreateApplicationSnapshot(ctx, "snap-direct-app", "snap-direct")
	require.NoError(t, err)

	snap, err := b.DescribeApplicationSnapshot(ctx, "snap-direct-app", "snap-direct")
	require.NoError(t, err)
	assert.Equal(t, "snap-direct", snap.SnapshotName)

	_, err = b.DescribeApplicationSnapshot(ctx, "snap-direct-app", "missing-snap")
	require.ErrorIs(t, err, kinesisanalyticsv2.ErrNotFound)
}

// ─── VpcConfiguration: non-nil slices ────────────────────────────────────────

func TestRefinement1_VpcConfiguration_NonNilSlices(t *testing.T) {
	t.Parallel()

	h := newRefinementHandler(newRefinementBackend())

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "vpc-nil-app",
		"RuntimeEnvironment": "FLINK-1_18",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "AddApplicationVpcConfiguration", map[string]any{
		"ApplicationName":  "vpc-nil-app",
		"VpcConfiguration": map[string]any{
			// SubnetIds and SecurityGroupIds omitted (nil equivalent)
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	vpcDesc, ok := out["VpcConfigurationDescription"].(map[string]any)
	require.True(t, ok)

	subnets, ok := vpcDesc["SubnetIds"].([]any)
	require.True(t, ok)
	assert.NotNil(t, subnets)

	sgs, ok := vpcDesc["SecurityGroupIds"].([]any)
	require.True(t, ok)
	assert.NotNil(t, sgs)
}

// ─── ListApplicationSnapshots: sorted by creation time ───────────────────────

func TestRefinement1_ListApplicationSnapshots_SortedByCreationTime(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newRefinementBackend()

	_, err := b.CreateApplication(ctx, "sort-snap-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	for _, name := range []string{"snap-b", "snap-a", "snap-c"} {
		_, err = b.CreateApplicationSnapshot(ctx, "sort-snap-app", name)
		require.NoError(t, err)
	}

	snaps, _, err := b.ListApplicationSnapshots(ctx, "sort-snap-app", "")
	require.NoError(t, err)
	require.Len(t, snaps, 3)

	// Verify sorted by creation time (ascending)
	for i := 1; i < len(snaps); i++ {
		assert.False(t, snaps[i].SnapshotCreation.Before(snaps[i-1].SnapshotCreation),
			"snapshots should be sorted by creation time")
	}
}

// ─── ErrValidation sentinel ───────────────────────────────────────────────────

func TestRefinement1_ErrValidation_SentinelExists(t *testing.T) {
	t.Parallel()

	assert.Error(t, kinesisanalyticsv2.ErrValidation)
}

// ─── Persistence round-trip ───────────────────────────────────────────────────

func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newRefinementBackend()

	_, err := b.CreateApplication(
		ctx,
		"persist-app",
		"FLINK-1_18",
		"role-arn",
		"desc",
		"STREAMING",
		[]kinesisanalyticsv2.Tag{{Key: "env", Value: "test"}},
	)
	require.NoError(t, err)

	_, err = b.CreateApplicationSnapshot(ctx, "persist-app", "snap-1")
	require.NoError(t, err)

	h := newRefinementHandler(b)
	data := h.Snapshot(t.Context())
	require.NotNil(t, data)

	b2 := newRefinementBackend()
	h2 := newRefinementHandler(b2)
	require.NoError(t, h2.Restore(t.Context(), data))

	assert.Equal(t, 1, kinesisanalyticsv2.ApplicationCount(b2))
	assert.Equal(t, 1, kinesisanalyticsv2.SnapshotCount(b2))

	app, err := b2.DescribeApplication(ctx, "persist-app")
	require.NoError(t, err)
	assert.Equal(t, "persist-app", app.ApplicationName)
	assert.Equal(t, "FLINK-1_18", app.RuntimeEnvironment)
}

func TestRefinement1_PersistenceEmpty(t *testing.T) {
	t.Parallel()

	b := newRefinementBackend()
	h := newRefinementHandler(b)

	data := h.Snapshot(t.Context())
	require.NotNil(t, data)

	b2 := newRefinementBackend()
	h2 := newRefinementHandler(b2)
	require.NoError(t, h2.Restore(t.Context(), data))

	assert.Zero(t, kinesisanalyticsv2.ApplicationCount(b2))
	assert.Zero(t, kinesisanalyticsv2.SnapshotCount(b2))
}

// ─── Persistence: nextID preserved ───────────────────────────────────────────

func TestRefinement1_Persistence_NextIDPreserved(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newRefinementBackend()

	_, err := b.CreateApplication(ctx, "id-app", "SQL-1_0", "", "", "", nil)
	require.NoError(t, err)

	err = b.AddApplicationCloudWatchLoggingOption(ctx, "id-app", 0,
		"arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s", "")
	require.NoError(t, err)

	h := newRefinementHandler(b)
	data := h.Snapshot(t.Context())
	require.NotNil(t, data)

	b2 := newRefinementBackend()
	h2 := newRefinementHandler(b2)
	require.NoError(t, h2.Restore(t.Context(), data))

	// Adding another CWL option on b2 should generate a new distinct ID
	err = b2.AddApplicationCloudWatchLoggingOption(ctx, "id-app", 0,
		"arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s2", "")
	require.NoError(t, err)

	app, err := b2.DescribeApplication(ctx, "id-app")
	require.NoError(t, err)
	assert.Len(t, app.CloudWatchLoggingOptionDescs, 2)
	assert.NotEqual(
		t,
		app.CloudWatchLoggingOptionDescs[0].CloudWatchLoggingOptionID,
		app.CloudWatchLoggingOptionDescs[1].CloudWatchLoggingOptionID,
	)
}

// ─── Provider: Init with slog logger ─────────────────────────────────────────

func TestRefinement1_Provider_Init_WithLogger(t *testing.T) {
	t.Parallel()

	p := &kinesisanalyticsv2.Provider{}
	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

// ─── ConcurrentModification error ────────────────────────────────────────────

func TestRefinement1_ConcurrentModification(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newRefinementBackend()

	_, err := b.CreateApplication(ctx, "ver-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	// version check: wrong version should fail
	err = b.AddApplicationCloudWatchLoggingOption(ctx, "ver-app", 99,
		"arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s", "")
	require.ErrorIs(t, err, kinesisanalyticsv2.ErrConcurrentModification)
}

// ─── Handler: ConcurrentModification maps to 400 ─────────────────────────────

func TestRefinement1_ConcurrentModification_Returns400(t *testing.T) {
	t.Parallel()

	h := newRefinementHandler(newRefinementBackend())

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "ver-app-http",
		"RuntimeEnvironment": "FLINK-1_18",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "AddApplicationCloudWatchLoggingOption", map[string]any{
		"ApplicationName":             "ver-app-http",
		"CurrentApplicationVersionId": 99,
		"CloudWatchLoggingOption": map[string]any{
			"LogStreamARN": "arn:aws:logs:us-east-1:000000000000:log-group:g:log-stream:s",
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ─── AddApplicationInput: missing input returns 400 ──────────────────────────

func TestRefinement1_AddApplicationInput_MissingInput(t *testing.T) {
	t.Parallel()

	h := newRefinementHandler(newRefinementBackend())

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "input-missing-app",
		"RuntimeEnvironment": "SQL-1_0",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "AddApplicationInput", map[string]any{
		"ApplicationName": "input-missing-app",
		// Input field omitted
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ─── AddApplicationOutput: missing output returns 400 ────────────────────────

func TestRefinement1_AddApplicationOutput_MissingOutput(t *testing.T) {
	t.Parallel()

	h := newRefinementHandler(newRefinementBackend())

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "output-missing-app",
		"RuntimeEnvironment": "SQL-1_0",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "AddApplicationOutput", map[string]any{
		"ApplicationName": "output-missing-app",
		// Output field omitted
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ─── AddApplicationReferenceDataSource: missing source returns 400 ────────────

func TestRefinement1_AddApplicationRefDataSource_MissingSource(t *testing.T) {
	t.Parallel()

	h := newRefinementHandler(newRefinementBackend())

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "ref-missing-app",
		"RuntimeEnvironment": "SQL-1_0",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "AddApplicationReferenceDataSource", map[string]any{
		"ApplicationName": "ref-missing-app",
		// ReferenceDataSource field omitted
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ─── DeleteApplicationCloudWatchLoggingOption: not found returns 404 ─────────

func TestRefinement1_DeleteApplicationCWL_NotFound(t *testing.T) {
	t.Parallel()

	h := newRefinementHandler(newRefinementBackend())

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "del-cwl-app",
		"RuntimeEnvironment": "SQL-1_0",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "DeleteApplicationCloudWatchLoggingOption", map[string]any{
		"ApplicationName":           "del-cwl-app",
		"CloudWatchLoggingOptionId": "cwl-nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ─── DeleteApplicationOutput: not found returns 404 ──────────────────────────

func TestRefinement1_DeleteApplicationOutput_NotFound(t *testing.T) {
	t.Parallel()

	h := newRefinementHandler(newRefinementBackend())

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "del-out-app",
		"RuntimeEnvironment": "SQL-1_0",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "DeleteApplicationOutput", map[string]any{
		"ApplicationName": "del-out-app",
		"OutputId":        "output-nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ─── DeleteApplicationInputProcessingConfiguration: not found returns 404 ────

func TestRefinement1_DeleteApplicationInputProcessingConfig_NotFound(t *testing.T) {
	t.Parallel()

	h := newRefinementHandler(newRefinementBackend())

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "del-ipc-app",
		"RuntimeEnvironment": "SQL-1_0",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doKAV2Request(t, h, "DeleteApplicationInputProcessingConfiguration", map[string]any{
		"ApplicationName": "del-ipc-app",
		"InputId":         "input-nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ─── Non-nil slices in DescribeApplication response ──────────────────────────

func TestRefinement1_NonNilSlices_InDetailOutput(t *testing.T) {
	t.Parallel()

	h := newRefinementHandler(newRefinementBackend())

	rec := doKAV2Request(t, h, "CreateApplication", map[string]any{
		"ApplicationName":    "slice-app",
		"RuntimeEnvironment": "FLINK-1_18",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	detail := out["ApplicationDetail"].(map[string]any)
	// Tags should be present (may be empty but not nil)
	assert.Contains(t, detail, "ApplicationName")
	assert.Equal(t, "READY", detail["ApplicationStatus"])
}
