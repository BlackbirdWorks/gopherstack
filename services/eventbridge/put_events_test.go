package eventbridge_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// Compile-time proof that InMemoryBackend satisfies the services/ssm
// package's ParameterPolicyNotifier interface (see ssm_integration.go) --
// this is the adapter cli.go injects via
// ssmBackend.SetParameterPolicyNotifier(ebBackend) to wire real
// EventBridge publishing for SSM's NoChangeNotification/
// ExpirationNotification parameter policies.
var _ ssm.ParameterPolicyNotifier = (*eventbridge.InMemoryBackend)(nil)

func TestPutEvents_BatchSizeLimit(t *testing.T) {
	t.Parallel()
	b := newBackend()

	// Build one massive entry > 256KiB in detail.
	bigDetail := strings.Repeat("x", 260*1024)
	results, err := b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "s", DetailType: "T", Detail: bigDetail},
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "EventSizeLimitExceeded", results[0].ErrorCode)
}

func TestPutEvents_MixedBatch(t *testing.T) {
	t.Parallel()
	b := newBackend()

	// First entry is small (accepted), second is huge (rejected), third is small (accepted).
	bigDetail := strings.Repeat("y", 260*1024)
	results, err := b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "s", DetailType: "T", Detail: `{}`},
		{Source: "s", DetailType: "T", Detail: bigDetail},
		{Source: "s", DetailType: "T", Detail: `{}`},
	})
	require.NoError(t, err)
	require.Len(t, results, 3)
	assert.Empty(t, results[0].ErrorCode)
	assert.Equal(t, "EventSizeLimitExceeded", results[1].ErrorCode)
	assert.Empty(t, results[2].ErrorCode)
}

// TestPutEvents_EntryCountLimit proves AWS's 1-10 entries-per-request
// constraint on PutEvents (PutEventsRequestEntryList: Array Members Minimum
// 1, Maximum 10 item(s)): 0 or 11 entries fail the whole request, while 1
// and 10 succeed.
func TestPutEvents_EntryCountLimit(t *testing.T) {
	t.Parallel()

	makeEntries := func(n int) []eventbridge.EventEntry {
		entries := make([]eventbridge.EventEntry, n)
		for i := range entries {
			entries[i] = eventbridge.EventEntry{Source: "s", DetailType: "T", Detail: `{}`}
		}

		return entries
	}

	tests := []struct {
		name    string
		count   int
		wantErr bool
	}{
		{"zero entries rejected", 0, true},
		{"one entry accepted", 1, false},
		{"ten entries accepted", 10, false},
		{"eleven entries rejected", 11, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			results, err := b.PutEvents(context.Background(), makeEntries(tt.count))
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
				assert.Nil(t, results)

				return
			}
			require.NoError(t, err)
			require.Len(t, results, tt.count)
			for _, r := range results {
				assert.Empty(t, r.ErrorCode)
			}
		})
	}
}

// TestPutEvents_RequiredFields proves that Source, DetailType, and
// Detail are required on every PutEvents entry (per aws-sdk-go-v2's
// PutEventsRequestEntry doc: "Detail, DetailType, and Source are required
// for EventBridge to successfully send an event... If you include event
// entries in a request that do not include each of those properties,
// EventBridge fails that entry. If you submit a request in which none of the
// entries have each of these properties, EventBridge fails the entire
// request.").
func TestPutEvents_RequiredFields(t *testing.T) {
	t.Parallel()

	t.Run("entry missing Source fails individually", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		results, err := b.PutEvents(context.Background(), []eventbridge.EventEntry{
			{DetailType: "T", Detail: `{}`},
			{Source: "s", DetailType: "T", Detail: `{}`},
		})
		require.NoError(t, err)
		require.Len(t, results, 2)
		assert.Equal(t, "InvalidArgument", results[0].ErrorCode)
		assert.NotEmpty(t, results[0].ErrorMessage)
		assert.Empty(t, results[1].ErrorCode)
		assert.NotEmpty(t, results[1].EventID)
	})

	t.Run("entry missing DetailType fails individually", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		results, err := b.PutEvents(context.Background(), []eventbridge.EventEntry{
			{Source: "s", Detail: `{}`},
			{Source: "s", DetailType: "T", Detail: `{}`},
		})
		require.NoError(t, err)
		require.Len(t, results, 2)
		assert.Equal(t, "InvalidArgument", results[0].ErrorCode)
		assert.Empty(t, results[1].ErrorCode)
	})

	t.Run("entry missing Detail fails individually", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		results, err := b.PutEvents(context.Background(), []eventbridge.EventEntry{
			{Source: "s", DetailType: "T"},
			{Source: "s", DetailType: "T", Detail: `{}`},
		})
		require.NoError(t, err)
		require.Len(t, results, 2)
		assert.Equal(t, "InvalidArgument", results[0].ErrorCode)
		assert.Empty(t, results[1].ErrorCode)
	})

	t.Run("no entry complete fails the whole request", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		results, err := b.PutEvents(context.Background(), []eventbridge.EventEntry{
			{DetailType: "T", Detail: `{}`},
			{Source: "s", Detail: `{}`},
		})
		require.Error(t, err)
		require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
		assert.Nil(t, results)
	})
}

func TestPutEvents_DefaultBus(t *testing.T) {
	t.Parallel()
	b := newBackend()

	results, err := b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "default.test", DetailType: "Ping", Detail: `{}`},
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Empty(t, results[0].ErrorCode)
	assert.NotEmpty(t, results[0].EventID)
}

func TestPutEvents_NamedBus(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateEventBus(context.Background(), eventbridge.CreateEventBusParams{Name: "named-bus"})
	require.NoError(t, err)

	results, err := b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "s", DetailType: "T", Detail: `{}`, EventBusName: "named-bus"},
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Empty(t, results[0].ErrorCode)
}

// putEventsResult mirrors the PutEvents/PutPartnerEvents response shape.
type putEventsResult struct {
	Entries []struct {
		EventID      string `json:"EventId"`
		ErrorCode    string `json:"ErrorCode"`
		ErrorMessage string `json:"ErrorMessage"`
	} `json:"Entries"`
	FailedEntryCount int `json:"FailedEntryCount"`
}

// bigDetail returns a JSON detail string large enough to exceed the 256 KiB
// PutEvents batch limit on its own.
func bigDetail() string {
	return `{"blob":"` + strings.Repeat("x", 300*1024) + `"}`
}

// TestPutEvents_FailedEntryCount asserts PutEvents and PutPartnerEvents report a
// real FailedEntryCount matching the number of entries carrying an ErrorCode.
func TestPutEvents_FailedEntryCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		action          string
		entries         []eventbridge.EventEntry
		wantFailedCount int
	}{
		{
			name:   "put_events_one_oversized_one_ok",
			action: "PutEvents",
			entries: []eventbridge.EventEntry{
				{Source: "ok.svc", DetailType: "Small", Detail: `{"a":1}`},
				{Source: "big.svc", DetailType: "Big", Detail: bigDetail()},
			},
			wantFailedCount: 1,
		},
		{
			name:   "put_events_all_ok",
			action: "PutEvents",
			entries: []eventbridge.EventEntry{
				{Source: "a", DetailType: "T", Detail: `{}`},
				{Source: "b", DetailType: "T", Detail: `{}`},
			},
			wantFailedCount: 0,
		},
		{
			name:   "put_partner_events_one_oversized",
			action: "PutPartnerEvents",
			entries: []eventbridge.EventEntry{
				{Source: "partner/x", DetailType: "Big", Detail: bigDetail()},
				{Source: "partner/y", DetailType: "Small", Detail: `{}`},
			},
			wantFailedCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body, err := json.Marshal(map[string]any{"Entries": tt.entries})
			require.NoError(t, err)

			rec := makeRequest(t, tt.action, string(body))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var got putEventsResult
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))

			assert.Equal(t, tt.wantFailedCount, got.FailedEntryCount)

			// Verify the count equals the number of entries with an ErrorCode.
			realFailed := 0
			for _, e := range got.Entries {
				if e.ErrorCode != "" {
					realFailed++
					assert.Equal(t, "EventSizeLimitExceeded", e.ErrorCode)
				}
			}
			assert.Equal(t, tt.wantFailedCount, realFailed)
		})
	}
}

// TestNotifyParameterPolicyAction verifies the ssm.ParameterPolicyNotifier
// adapter (ssm_integration.go) translates a policy-action notification into
// a real PutEvents call using AWS's documented wire shape (source "aws.ssm",
// detail-type "Parameter Store Policy Action", detail
// {"parameter-name":..., "policy-type":...}) rather than silently dropping
// it or using an invented shape. An archive on the default bus with an
// EventPattern matching those exact source/detail-type values is used as an
// independent observer: if the archive's EventCount increments, the event
// really was published with that wire shape (not just "PutEvents returned no
// error").
func TestNotifyParameterPolicyAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		parameterName  string
		policyType     string
		archivePattern string
	}{
		{
			name:          "no_change_notification_matches_pattern",
			parameterName: "/app/prod/id",
			policyType:    "NoChangeNotification",
			archivePattern: `{"source":["aws.ssm"],"detail-type":["Parameter Store Policy Action"],` +
				`"detail":{"policy-type":["NoChangeNotification"]}}`,
		},
		{
			name:          "expiration_notification_matches_pattern",
			parameterName: "/app/prod/secret",
			policyType:    "ExpirationNotification",
			archivePattern: `{"source":["aws.ssm"],"detail-type":["Parameter Store Policy Action"],` +
				`"detail":{"policy-type":["ExpirationNotification"]}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			ctx := context.Background()

			_, err := b.CreateArchive(ctx, eventbridge.CreateArchiveInput{
				ArchiveName:    "capture-" + tt.name,
				EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/default",
				EventPattern:   tt.archivePattern,
			})
			require.NoError(t, err)

			err = b.NotifyParameterPolicyAction(ctx, tt.parameterName, tt.policyType)
			require.NoError(t, err)

			archive, err := b.DescribeArchive(ctx, "capture-"+tt.name)
			require.NoError(t, err)
			assert.EqualValues(t, 1, archive.EventCount,
				"the published event must match the exact source/detail-type/policy-type wire shape")
		})
	}
}
