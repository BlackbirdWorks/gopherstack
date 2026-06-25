package eventbridge_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// TestParity_ListEventBuses_RespectsLimit verifies that the Limit field in
// ListEventBuses requests controls the page size. Real AWS honors this field.
func TestParity_ListEventBuses_RespectsLimit(t *testing.T) {
	t.Parallel()

	// Note: NewInMemoryBackendWithConfig pre-creates the default bus, so total = busCount+1.
	tests := []struct {
		name      string
		busCount  int
		limit     int
		wantCount int
		wantToken bool
	}{
		{
			name:      "limit_smaller_than_total",
			busCount:  5,
			limit:     3,
			wantCount: 3, // 6 total (5 custom + default), page size 3
			wantToken: true,
		},
		{
			name:      "limit_equals_total",
			busCount:  5,
			limit:     6,
			wantCount: 6, // exactly 6 total — no next token
			wantToken: false,
		},
		{
			name:      "limit_zero_uses_default",
			busCount:  5,
			limit:     0,
			wantCount: 6, // default limit 100 covers all 6
			wantToken: false,
		},
		{
			name:      "limit_larger_than_total",
			busCount:  2,
			limit:     10,
			wantCount: 3, // 3 total (2 custom + default) — all fit
			wantToken: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			for i := range tt.busCount {
				_, err := b.CreateEventBus(context.Background(), strings.Repeat("a", i+1)+"-bus", "")
				require.NoError(t, err)
			}

			page, token, err := b.ListEventBuses(context.Background(), "", "", tt.limit)
			require.NoError(t, err)
			assert.Len(t, page, tt.wantCount)
			if tt.wantToken {
				assert.NotEmpty(t, token, "expected a next-page token")
			} else {
				assert.Empty(t, token, "expected no next-page token")
			}
		})
	}
}

// TestParity_ListEventBuses_TokenIsOpaque verifies that next-page tokens
// are base64-encoded (opaque). Real AWS tokens are not plain integers.
func TestParity_ListEventBuses_TokenIsOpaque(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	for i := range 5 {
		_, err := b.CreateEventBus(context.Background(), strings.Repeat("z", i+1)+"-bus", "")
		require.NoError(t, err)
	}

	_, token, err := b.ListEventBuses(context.Background(), "", "", 3)
	require.NoError(t, err)
	require.NotEmpty(t, token, "expected a pagination token")

	// Token must be valid base64.
	decoded, decodeErr := base64.StdEncoding.DecodeString(token)
	require.NoError(t, decodeErr, "next-page token must be base64-encoded")

	// The decoded value should be a non-negative integer (offset).
	assert.NotEmpty(t, string(decoded))
}

// TestParity_ListEventBuses_PaginationFollowsToken verifies that providing the
// token from page 1 returns the correct page 2.
func TestParity_ListEventBuses_PaginationFollowsToken(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	// Create 5 buses; default bus makes 6 total.
	for i := range 5 {
		_, err := b.CreateEventBus(context.Background(), strings.Repeat("b", i+1)+"-bus", "")
		require.NoError(t, err)
	}

	page1, token, err := b.ListEventBuses(context.Background(), "", "", 4)
	require.NoError(t, err)
	require.NotEmpty(t, token)
	assert.Len(t, page1, 4)

	page2, token2, err := b.ListEventBuses(context.Background(), "", token, 4)
	require.NoError(t, err)
	assert.Empty(t, token2)
	assert.Len(t, page2, 2) // 6 total - 4 on page 1 = 2 remaining

	// Pages must not overlap.
	page1Names := make(map[string]bool, len(page1))
	for _, bus := range page1 {
		page1Names[bus.Name] = true
	}
	for _, bus := range page2 {
		assert.False(t, page1Names[bus.Name], "page 2 item %q appeared on page 1", bus.Name)
	}
}

// TestParity_CreateEventBus_QuotaIsPerAccount verifies that the 200-bus limit
// applies across all regions for the same account. Real AWS enforces per-account limits.
func TestParity_CreateEventBus_QuotaIsPerAccount(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	// Fill up to the limit (200) using buses spread across two regions.
	eastCtx := regionCtx("us-east-1")
	westCtx := regionCtx("us-west-2")

	for i := range 100 {
		_, err := b.CreateEventBus(eastCtx, strings.Repeat("e", i+1)+"-east", "")
		require.NoError(t, err)
	}
	for i := range 100 {
		_, err := b.CreateEventBus(westCtx, strings.Repeat("w", i+1)+"-west", "")
		require.NoError(t, err)
	}

	// 201st bus in any region must fail — quota is per-account.
	_, err := b.CreateEventBus(eastCtx, "one-too-many", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, eventbridge.ErrResourceLimitExceeded)
}

// TestParity_DeleteEventBus_CleansUpTags verifies that deleting an event bus
// removes its tag entry from the handler so the tags map doesn't grow unbounded.
func TestParity_DeleteEventBus_CleansUpTags(t *testing.T) {
	t.Parallel()

	backend := eventbridge.NewInMemoryBackend()
	handler := eventbridge.NewHandler(backend)
	e := echo.New()

	const busARN = "arn:aws:events:us-east-1:000000000000:event-bus/temp-bus"

	// Create and tag a bus.
	rec := makeRequestWithHandler(t, handler, e, "CreateEventBus", `{"Name":"temp-bus"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	tagBody := `{"ResourceARN":"` + busARN + `","Tags":[{"Key":"owner","Value":"test"}]}`
	rec = makeRequestWithHandler(t, handler, e, "TagResource", tagBody)
	require.Equal(t, http.StatusOK, rec.Code)

	// Tags should be present.
	rec = makeRequestWithHandler(t, handler, e, "ListTagsForResource", `{"ResourceARN":"`+busARN+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "owner")

	// Delete the bus.
	rec = makeRequestWithHandler(t, handler, e, "DeleteEventBus", `{"Name":"temp-bus"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Tags should now return empty (map entry cleaned up).
	rec = makeRequestWithHandler(t, handler, e, "ListTagsForResource", `{"ResourceARN":"`+busARN+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "owner")
}

// TestParity_DeleteRule_CleansUpTags verifies that deleting a rule removes its
// tag entry from the handler so the tags map doesn't grow unbounded.
func TestParity_DeleteRule_CleansUpTags(t *testing.T) {
	t.Parallel()

	backend := eventbridge.NewInMemoryBackend()
	handler := eventbridge.NewHandler(backend)
	e := echo.New()

	// Create a rule (Tags is map[string]string in this backend's JSON encoding).
	rec := makeRequestWithHandler(t, handler, e, "PutRule",
		`{"Name":"temp-rule","EventPattern":"{\"source\":[\"test\"]}","Tags":{"team":"backend"}}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var putOut map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putOut))
	ruleARN := putOut["RuleArn"]
	require.NotEmpty(t, ruleARN)

	// Confirm tag is set.
	rec = makeRequestWithHandler(t, handler, e, "ListTagsForResource",
		`{"ResourceARN":"`+ruleARN+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "team")

	// Delete the rule.
	rec = makeRequestWithHandler(t, handler, e, "DeleteRule",
		`{"Name":"temp-rule","EventBusName":"default"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Tag entry should be gone.
	rec = makeRequestWithHandler(t, handler, e, "ListTagsForResource",
		`{"ResourceARN":"`+ruleARN+`"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "team")
}
