package eventbridge_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

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

// TestListRules_Limit asserts the Limit request parameter is threaded through to
// backend pagination for ListRules.
func TestListRules_Limit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		limit    int
		numRules int
		wantPage int
		wantMore bool
	}{
		{name: "limit_2_of_5", limit: 2, numRules: 5, wantPage: 2, wantMore: true},
		{name: "limit_3_of_3", limit: 3, numRules: 3, wantPage: 3, wantMore: false},
		{name: "zero_limit_uses_default", limit: 0, numRules: 5, wantPage: 5, wantMore: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			for i := range tt.numRules {
				_, err := b.PutRule(t.Context(), eventbridge.PutRuleInput{
					Name:         fmt.Sprintf("rule-%02d", i),
					EventPattern: `{"source":["x"]}`,
					State:        "ENABLED",
				})
				require.NoError(t, err)
			}

			rules, next, err := b.ListRules(t.Context(), "default", "", "", tt.limit)
			require.NoError(t, err)
			assert.Len(t, rules, tt.wantPage)
			if tt.wantMore {
				assert.NotEmpty(t, next)
			} else {
				assert.Empty(t, next)
			}
		})
	}
}

// TestListTargetsByRule_Limit asserts the Limit request parameter is threaded
// through to backend pagination for ListTargetsByRule.
func TestListTargetsByRule_Limit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		limit      int
		numTargets int
		wantPage   int
		wantMore   bool
	}{
		{name: "limit_2_of_5", limit: 2, numTargets: 5, wantPage: 2, wantMore: true},
		{name: "zero_limit_uses_default", limit: 0, numTargets: 4, wantPage: 4, wantMore: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := b.PutRule(t.Context(), eventbridge.PutRuleInput{
				Name:         "target-limit-rule",
				EventPattern: `{"source":["x"]}`,
				State:        "ENABLED",
			})
			require.NoError(t, err)

			targets := make([]eventbridge.Target, 0, tt.numTargets)
			for i := range tt.numTargets {
				targets = append(targets, eventbridge.Target{
					ID:  fmt.Sprintf("t-%02d", i),
					Arn: fmt.Sprintf("arn:aws:sqs:us-east-1:123456789012:q-%02d", i),
				})
			}
			_, err = b.PutTargets(t.Context(), "target-limit-rule", "default", targets)
			require.NoError(t, err)

			got, next, err := b.ListTargetsByRule(t.Context(), "target-limit-rule", "default", "", tt.limit)
			require.NoError(t, err)
			assert.Len(t, got, tt.wantPage)
			if tt.wantMore {
				assert.NotEmpty(t, next)
			} else {
				assert.Empty(t, next)
			}
		})
	}
}

// TestListRules_LimitPaginationCoversAll walks all pages using the Limit and
// confirms every rule is returned exactly once across pages.
func TestListRules_LimitPaginationCoversAll(t *testing.T) {
	t.Parallel()

	b := newBackend()
	const total = 7
	for i := range total {
		_, err := b.PutRule(t.Context(), eventbridge.PutRuleInput{
			Name:         fmt.Sprintf("pg-%02d", i),
			EventPattern: `{"source":["x"]}`,
			State:        "ENABLED",
		})
		require.NoError(t, err)
	}

	seen := map[string]bool{}
	token := ""
	pages := 0
	for {
		rules, next, err := b.ListRules(t.Context(), "default", "", token, 3)
		require.NoError(t, err)
		pages++
		require.LessOrEqual(t, len(rules), 3)
		for _, r := range rules {
			assert.False(t, seen[r.Name], "duplicate rule %s", r.Name)
			seen[r.Name] = true
		}
		if next == "" {
			break
		}
		token = next
		require.LessOrEqual(t, pages, total, "pagination did not terminate")
	}

	assert.Len(t, seen, total)
	assert.Equal(t, 3, pages) // 3 + 3 + 1
}

// TestCaptureEventInArchives_UsesPatternCache confirms archive pattern matching
// reuses the compiled-pattern cache: firing many events against an archive
// compiles its pattern at most once, and events are still captured.
func TestCaptureEventInArchives_UsesPatternCache(t *testing.T) {
	t.Parallel()

	b := newBackend()

	bus, err := b.DescribeEventBus(t.Context(), "default")
	require.NoError(t, err)

	_, err = b.CreateArchive(t.Context(), eventbridge.CreateArchiveInput{
		ArchiveName:    "cache-archive",
		EventSourceArn: bus.Arn,
		EventPattern:   `{"source":["cache.svc"]}`,
	})
	require.NoError(t, err)

	const events = 25
	for range events {
		b.PutEvents(t.Context(), []eventbridge.EventEntry{
			{Source: "cache.svc", DetailType: "Evt", Detail: `{}`},
		})
	}

	assert.Equal(t, events, b.ArchivedEventCount("cache-archive"))
	// Only the single archive pattern should have been compiled and cached,
	// regardless of how many events were processed.
	assert.LessOrEqual(t, b.PatternCacheSize(), 1,
		"archive pattern should be compiled at most once via the cache")
}
