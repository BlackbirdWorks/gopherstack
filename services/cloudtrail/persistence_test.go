package cloudtrail_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

// newPersistenceTestBackend creates a backend with one populated entry in
// every store.Table (and, transitively, every secondary store.Index) plus one
// recorded event (the raw events field, []Event -- the one field left
// un-converted, see store_setup.go's file doc comment), so a Snapshot from it
// exercises the entire persisted table surface of the backend, including
// Event's MarshalJSON/UnmarshalJSON epoch-seconds round trip.
func newPersistenceTestBackend(t *testing.T) *cloudtrail.InMemoryBackend {
	t.Helper()

	b := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")

	// trails table + trailsByARN index.
	trail, err := b.CreateTrail(
		"trail1", "bucket1", "prefix", "", "", "", "",
		true, false, false,
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)

	// channels table + channelsByARN + channelsByName indexes.
	ch, err := b.CreateChannel("channel1", "aws", nil, map[string]string{"k": "v"})
	require.NoError(t, err)

	// dashboards table + dashboardsByARN + dashboardsByName indexes.
	dash, err := b.CreateDashboard("dashboard1", "CUSTOM", map[string]string{"k": "v"}, nil, nil, false)
	require.NoError(t, err)

	// eventDataStores table + edsByARN + edsByName indexes.
	eds, err := b.CreateEventDataStore(
		"eds1", true, false, false, 2557, nil, "", "",
		map[string]string{"k": "v"},
	)
	require.NoError(t, err)

	// queries table.
	_, err = b.StartQuery("SELECT * FROM events", eds.EventDataStoreARN, "", "")
	require.NoError(t, err)

	// resourcePolicies table.
	b.PutResourcePolicy(eds.EventDataStoreARN, `{"Version":"2012-10-17"}`)

	// imports table.
	_, err = b.StartImport([]string{eds.EventDataStoreARN}, &cloudtrail.ImportSource{
		S3: &cloudtrail.S3ImportSource{S3LocationURI: "s3://my-bucket/logs/"},
	})
	require.NoError(t, err)

	// raw events slice.
	b.RecordEvent(cloudtrail.Event{EventName: "CreateTrail", EventSource: "cloudtrail.amazonaws.com"})

	_ = trail
	_ = ch
	_ = dash

	return b
}

// TestInMemoryBackend_SnapshotRestore_FullState round-trips every store.Table
// (trails, channels, dashboards, eventDataStores, queries, resourcePolicies,
// imports), every secondary store.Index derived from them, and the one raw
// field left un-converted (events) through Snapshot -> Restore into a fresh
// backend.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := newPersistenceTestBackend(t)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// trails table, looked up by name.
	trail, err := fresh.GetTrail("trail1")
	require.NoError(t, err)
	assert.Equal(t, "bucket1", trail.S3BucketName)

	// trailsByARN index, looked up by ARN.
	trailByARN, err := fresh.GetTrail(trail.TrailARN)
	require.NoError(t, err)
	assert.Equal(t, "trail1", trailByARN.Name)

	// channels table + channelsByARN index.
	channels := fresh.ListChannels()
	require.Len(t, channels, 1)
	ch, err := fresh.GetChannel(channels[0].ChannelARN)
	require.NoError(t, err)
	assert.Equal(t, "channel1", ch.Name)

	// channelsByName uniqueness enforced post-restore (index survived).
	_, err = fresh.CreateChannel("channel1", "aws", nil, nil)
	require.ErrorIs(t, err, cloudtrail.ErrAlreadyExists)

	// dashboards table + dashboardsByARN index.
	dashboards := fresh.ListDashboards()
	require.Len(t, dashboards, 1)
	dash, err := fresh.GetDashboard(dashboards[0].DashboardARN)
	require.NoError(t, err)
	assert.Equal(t, "dashboard1", dash.Name)

	// dashboardsByName uniqueness enforced post-restore (index survived).
	_, err = fresh.CreateDashboard("dashboard1", "CUSTOM", nil, nil, nil, false)
	require.ErrorIs(t, err, cloudtrail.ErrAlreadyExists)

	// eventDataStores table + edsByARN index.
	edsList := fresh.ListEventDataStores()
	require.Len(t, edsList, 1)
	eds, err := fresh.GetEventDataStore(edsList[0].EventDataStoreARN)
	require.NoError(t, err)
	assert.Equal(t, "eds1", eds.Name)

	// edsByName uniqueness enforced post-restore (index survived).
	_, err = fresh.CreateEventDataStore("eds1", true, false, false, 0, nil, "", "", nil)
	require.ErrorIs(t, err, cloudtrail.ErrAlreadyExists)

	// queries table.
	queries := fresh.ListQueries()
	require.Len(t, queries, 1)
	assert.Equal(t, "SELECT * FROM events", queries[0].QueryString)

	// resourcePolicies table.
	policy, err := fresh.GetResourcePolicy(eds.EventDataStoreARN)
	require.NoError(t, err)
	assert.JSONEq(t, `{"Version":"2012-10-17"}`, policy.ResourcePolicy)

	// imports table.
	imports := fresh.ListImports()
	require.Len(t, imports, 1)
	require.NotNil(t, imports[0].ImportSource)
	require.NotNil(t, imports[0].ImportSource.S3)
	assert.Equal(t, "s3://my-bucket/logs/", imports[0].ImportSource.S3.S3LocationURI)

	// events raw slice: round-trips through Event's MarshalJSON/UnmarshalJSON
	// epoch-seconds pair (see models.go).
	out := fresh.LookupEvents(cloudtrail.LookupEventsInput{})
	require.Len(t, out.Events, 1)
	assert.Equal(t, "CreateTrail", out.Events[0].EventName)

	// Sanity: account/region carried through too.
	assert.Equal(t, "us-east-1", fresh.Region())
}

// TestInMemoryBackend_SnapshotRestore_EventsRoundTrip verifies that the raw
// events field ([]Event, the one field left un-converted -- see
// store_setup.go's file doc comment) survives Snapshot -> Restore, including
// EventTime (via Event's MarshalJSON/UnmarshalJSON epoch-seconds pair) and
// the EventCategory added for LookupEvents' EventCategory filter. A prior
// version of this backend had Event.MarshalJSON with no matching
// UnmarshalJSON, so any snapshot containing an event failed Restore entirely;
// this test locks in that the round trip now succeeds and is lossless.
func TestInMemoryBackend_SnapshotRestore_EventsRoundTrip(t *testing.T) {
	t.Parallel()

	b := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	eventTime := time.Date(2024, 3, 15, 9, 30, 0, 0, time.UTC)
	b.RecordEvent(cloudtrail.Event{
		EventName:   "CreateTrail",
		EventSource: "cloudtrail.amazonaws.com",
		EventTime:   eventTime,
	})

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	out := fresh.LookupEvents(cloudtrail.LookupEventsInput{})
	require.Len(t, out.Events, 1)
	assert.Equal(t, "CreateTrail", out.Events[0].EventName)
	assert.Equal(t, "Management", out.Events[0].EventCategory)
	assert.True(t, eventTime.Equal(out.Events[0].EventTime), "EventTime must round-trip exactly")
}

// TestInMemoryBackend_UpdateEventDataStore_RenamePreservesIndex verifies the
// store.Table gotcha called out for Phase 3.3: renaming eds.Name (an indexed
// field, via edsByName) must delete the old index entry before mutating and
// re-Put after, or the byName index goes stale. This also proves the rename
// survives a Snapshot -> Restore round trip.
func TestInMemoryBackend_UpdateEventDataStore_RenamePreservesIndex(t *testing.T) {
	t.Parallel()

	b := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")

	eds, err := b.CreateEventDataStore("original-name", false, false, false, 0, nil, "", "", nil)
	require.NoError(t, err)

	_, err = b.UpdateEventDataStore(eds.EventDataStoreID, "renamed", nil, nil, nil, nil, nil, "", "")
	require.NoError(t, err)

	// The old name must no longer resolve to anything.
	_, err = b.CreateEventDataStore("original-name", false, false, false, 0, nil, "", "", nil)
	require.NoError(t, err, "original-name index entry must have been removed on rename")

	// The new name must be findable and unique.
	_, err = b.CreateEventDataStore("renamed", false, false, false, 0, nil, "", "", nil)
	require.ErrorIs(t, err, cloudtrail.ErrAlreadyExists, "renamed index entry must be present")

	snap := b.Snapshot(t.Context())
	fresh := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	got, err := fresh.GetEventDataStore(eds.EventDataStoreID)
	require.NoError(t, err)
	assert.Equal(t, "renamed", got.Name)
}

// TestInMemoryBackend_UpdateDashboard_WidgetsAndScheduleSurviveRestore
// verifies that UpdateDashboard's real fields (Widgets, RefreshSchedule,
// TerminationProtectionEnabled -- real UpdateDashboardInput has no Name
// field, dashboards cannot be renamed; see UpdateDashboard's doc comment in
// dashboards.go) persist and survive a Snapshot -> Restore round trip,
// alongside the dashboardsByName index (looked up by the original name,
// which never changes).
func TestInMemoryBackend_UpdateDashboard_WidgetsAndScheduleSurviveRestore(t *testing.T) {
	t.Parallel()

	b := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")

	dash, err := b.CreateDashboard("my-dashboard", "CUSTOM", nil, nil, nil, false)
	require.NoError(t, err)

	widgets := []cloudtrail.Widget{
		{QueryStatement: "SELECT * FROM eds1", ViewProperties: map[string]string{"title": "w1"}},
	}
	schedule := &cloudtrail.RefreshSchedule{
		Status:    "ENABLED",
		Frequency: &cloudtrail.RefreshScheduleFrequency{Unit: "HOURS", Value: 6},
	}
	protect := true

	updated, err := b.UpdateDashboard(dash.DashboardID, widgets, schedule, &protect)
	require.NoError(t, err)
	assert.Equal(t, "UPDATED", updated.Status)
	assert.True(t, updated.TerminationProtectionEnabled)
	require.Len(t, updated.Widgets, 1)
	assert.Equal(t, "SELECT * FROM eds1", updated.Widgets[0].QueryStatement)

	// dashboardsByName index still resolves by the (unchanged) original name.
	_, err = b.CreateDashboard("my-dashboard", "CUSTOM", nil, nil, nil, false)
	require.ErrorIs(t, err, cloudtrail.ErrAlreadyExists)

	snap := b.Snapshot(t.Context())
	fresh := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	got, err := fresh.GetDashboard(dash.DashboardID)
	require.NoError(t, err)
	assert.True(t, got.TerminationProtectionEnabled)
	require.Len(t, got.Widgets, 1)
	assert.Equal(t, "SELECT * FROM eds1", got.Widgets[0].QueryStatement)
	require.NotNil(t, got.RefreshSchedule)
	assert.Equal(t, "ENABLED", got.RefreshSchedule.Status)
}

// TestInMemoryBackend_SnapshotRestore_EventConfiguration verifies that
// per-resource event configuration (aggregation configs, context key
// selectors, max event size) set via PutEventConfiguration survives a
// Snapshot -> Restore round trip. EventConfiguration is not a store.Table
// (it is keyed by an arbitrary caller-supplied resource ARN, not a resource
// identity field), so it is persisted through its own backendSnapshot field
// rather than through the registry -- this test guards that wiring.
func TestInMemoryBackend_SnapshotRestore_EventConfiguration(t *testing.T) {
	t.Parallel()

	b := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")

	trail, err := b.CreateTrail(
		"evtcfg-trail", "bucket1", "", "", "", "", "",
		false, false, false, nil,
	)
	require.NoError(t, err)

	b.PutEventConfiguration(
		trail.TrailARN,
		[]map[string]any{{"EventCategory": "Management"}},
		[]map[string]any{{"Type": "RequestContext", "Equals": []string{"authparams"}}},
		"Large",
	)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	cfg := fresh.GetEventConfiguration(trail.TrailARN)
	assert.Equal(t, "Large", cfg.MaxEventSize)
	require.Len(t, cfg.ContextKeySelectors, 1)
	assert.Equal(t, "RequestContext", cfg.ContextKeySelectors[0]["Type"])
	require.Len(t, cfg.AggregationConfigurations, 1)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including the pre-Phase-3.3
// format, which decodes with Version == 0) is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := newPersistenceTestBackend(t)

	// A syntactically valid but version-less/mismatched snapshot.
	err := b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Empty(t, b.ListTrails())
	assert.Empty(t, b.ListChannels())
	assert.Empty(t, b.ListDashboards())
	assert.Empty(t, b.ListEventDataStores())
	assert.Empty(t, b.ListQueries())
	assert.Empty(t, b.ListImports())

	out := b.LookupEvents(cloudtrail.LookupEventsInput{})
	assert.Empty(t, out.Events)

	_, err = b.GetResourcePolicy("arn:aws:cloudtrail:us-east-1:000000000000:eventdatastore/eds-000001")
	require.ErrorIs(t, err, cloudtrail.ErrNotFound)
}

// TestInMemoryBackend_RestoreInvalidData verifies malformed JSON surfaces as
// an error rather than being silently discarded (that path is reserved for a
// syntactically valid but version-mismatched snapshot; see
// TestInMemoryBackend_RestoreVersionMismatch).
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestHandler_SnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to the backend (the wiring cli.go's generic setupPersistence
// relies on).
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	h := cloudtrail.NewHandler(newPersistenceTestBackend(t))

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := cloudtrail.NewHandler(cloudtrail.NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, h2.Restore(t.Context(), snap))

	trails := h2.Backend.ListTrails()
	require.Len(t, trails, 1)
	assert.Equal(t, "trail1", trails[0].Name)
}

// TestCloudTrailPersistence verifies Snapshot and Restore round-trip.
func TestPersistenceTrailRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	doCloudTrailOp(t, h, "CreateTrail", map[string]any{
		"Name":         "trail-persist",
		"S3BucketName": "bucket-persist",
	})

	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	h2 := newTestCloudTrailHandler()
	require.NoError(t, h2.Restore(t.Context(), snap))

	rec := doCloudTrailOp(t, h2, "GetTrail", map[string]any{
		"Name": "trail-persist",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseCloudTrailResp(t, rec)
	trail, ok := resp["Trail"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "trail-persist", trail["Name"])
}

// TestPersistenceRoundTripAllResourceTypes verifies Snapshot/Restore persists all resource types.
func TestPersistenceRoundTripAllResourceTypes(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	// Create one of each resource type
	doCloudTrailOp(t, h, "CreateTrail", map[string]any{
		"Name": "persist-trail", "S3BucketName": "bucket",
	})
	doCloudTrailOp(t, h, "CreateChannel", map[string]any{
		"Name": "persist-chan", "Source": "src",
	})
	doCloudTrailOp(t, h, "CreateDashboard", map[string]any{
		"Name": "persist-dash",
	})
	doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
		"Name": "persist-eds",
	})
	q, err := h.Backend.StartQuery("SELECT eventName FROM events LIMIT 1", "", "", "")
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	h2 := newTestCloudTrailHandler()
	require.NoError(t, h2.Restore(t.Context(), snap))

	// Verify trail restored
	rec := doCloudTrailOp(t, h2, "GetTrail", map[string]any{"Name": "persist-trail"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify channel name uniqueness restored (creating dup should fail)
	dupChanRec := doCloudTrailOp(t, h2, "CreateChannel", map[string]any{
		"Name": "persist-chan", "Source": "src",
	})
	assert.Equal(t, http.StatusConflict, dupChanRec.Code)

	// Verify dashboard name uniqueness restored
	dupDashRec := doCloudTrailOp(t, h2, "CreateDashboard", map[string]any{
		"Name": "persist-dash",
	})
	assert.Equal(t, http.StatusConflict, dupDashRec.Code)

	// Verify EDS name uniqueness restored
	dupEDSRec := doCloudTrailOp(t, h2, "CreateEventDataStore", map[string]any{
		"Name": "persist-eds",
	})
	assert.Equal(t, http.StatusConflict, dupEDSRec.Code)

	// Verify query restored
	descRec := doCloudTrailOp(t, h2, "DescribeQuery", map[string]any{
		"QueryId": q.QueryID,
	})
	assert.Equal(t, http.StatusOK, descRec.Code)
}

// TestPersistenceWithNewFields verifies that Snapshot/Restore correctly persists
// all new fields introduced in the AWS-accuracy audit.
func TestPersistenceWithNewFields(t *testing.T) {
	t.Parallel()

	h := newTestCloudTrailHandler()

	// Create a trail with advanced event selectors and insight selectors.
	doCloudTrailOp(t, h, "CreateTrail", map[string]any{
		"Name":         "persist-adv-trail",
		"S3BucketName": "bucket",
	})
	doCloudTrailOp(t, h, "StartLogging", map[string]any{"Name": "persist-adv-trail"})
	doCloudTrailOp(t, h, "PutEventSelectors", map[string]any{
		"TrailName": "persist-adv-trail",
		"AdvancedEventSelectors": []map[string]any{
			{
				"Name": "Log all",
				"FieldSelectors": []map[string]any{
					{"Field": "eventCategory", "Equals": []string{"Management"}},
				},
			},
		},
	})
	doCloudTrailOp(t, h, "PutInsightSelectors", map[string]any{
		"TrailName": "persist-adv-trail",
		"InsightSelectors": []map[string]any{
			{"InsightType": "ApiCallRateInsight"},
		},
	})

	// Create an EDS with federation enabled.
	createRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
		"Name":        "persist-fed-eds",
		"BillingMode": "FIXED_RETENTION_PRICING",
	})
	createResp := parseCloudTrailResp(t, createRec)
	edsARN := createResp["EventDataStoreArn"].(string)

	doCloudTrailOp(t, h, "EnableFederation", map[string]any{
		"EventDataStore":    edsARN,
		"FederationRoleArn": "arn:aws:iam::123456789012:role/FedRole",
	})

	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	h2 := newTestCloudTrailHandler()
	require.NoError(t, h2.Restore(t.Context(), snap))

	// Verify trail with advanced event selectors is restored.
	getRec := doCloudTrailOp(t, h2, "GetEventSelectors", map[string]any{
		"TrailName": "persist-adv-trail",
	})
	assert.Equal(t, http.StatusOK, getRec.Code)
	getResp := parseCloudTrailResp(t, getRec)
	advSels, ok := getResp["AdvancedEventSelectors"].([]any)
	require.True(t, ok)
	assert.Len(t, advSels, 1)

	// Verify insight selectors are restored.
	insightRec := doCloudTrailOp(t, h2, "GetInsightSelectors", map[string]any{
		"TrailName": "persist-adv-trail",
	})
	assert.Equal(t, http.StatusOK, insightRec.Code)
	insightResp := parseCloudTrailResp(t, insightRec)
	sels, ok := insightResp["InsightSelectors"].([]any)
	require.True(t, ok)
	assert.Len(t, sels, 1)

	// Verify EDS federation status is restored.
	getEDSRec := doCloudTrailOp(t, h2, "GetEventDataStore", map[string]any{
		"EventDataStore": edsARN,
	})
	assert.Equal(t, http.StatusOK, getEDSRec.Code)
	getEDSResp := parseCloudTrailResp(t, getEDSRec)
	assert.Equal(t, "ENABLED", getEDSResp["FederationStatus"])
	assert.Equal(t, "FIXED_RETENTION_PRICING", getEDSResp["BillingMode"])

	// Verify trail status (StartLoggingTime) is also restored.
	statusRec := doCloudTrailOp(t, h2, "GetTrailStatus", map[string]any{
		"Name": "persist-adv-trail",
	})
	assert.Equal(t, http.StatusOK, statusRec.Code)
	statusResp := parseCloudTrailResp(t, statusRec)
	assert.Equal(t, true, statusResp["IsLogging"])
	assert.NotNil(t, statusResp["StartLoggingTime"])
}
