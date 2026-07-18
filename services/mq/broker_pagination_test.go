package mq_test

// ListBrokers pagination behavior: opaque index-based tokens,
// continuation across pages, maxResults limits, and default listing.

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

func TestListBrokers_PaginationOpaqueToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		brokerNames   []string
		maxResults    int
		wantCount     int
		wantNextToken bool
	}{
		{
			name:          "single_page_no_token",
			brokerNames:   []string{"alpha", "beta"},
			maxResults:    10,
			wantCount:     2,
			wantNextToken: false,
		},
		{
			name:          "two_pages_token_present",
			brokerNames:   []string{"broker-1", "broker-2", "broker-3"},
			maxResults:    2,
			wantCount:     2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for _, name := range tt.brokerNames {
				createTestBroker(t, h, name, "ACTIVEMQ")
			}

			path := "/v1/brokers?maxResults=" + strconv.Itoa(tt.maxResults)
			rec := doRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			var brokers []any
			require.NoError(t, json.Unmarshal(out["brokerSummaries"], &brokers))
			assert.Len(t, brokers, tt.wantCount)

			rawToken, hasToken := out["nextToken"]
			if tt.wantNextToken {
				require.True(t, hasToken, "nextToken must be present when more pages exist")

				var tok string
				require.NoError(t, json.Unmarshal(rawToken, &tok))
				assert.NotEmpty(t, tok)

				// Token must be opaque base64 integer, not a broker name.
				offset := decodeOpaqueToken(t, tok)
				assert.Equal(t, tt.wantCount, offset, "token encodes offset = items returned so far")
			} else if hasToken {
				var tok string
				require.NoError(t, json.Unmarshal(rawToken, &tok))
				assert.Empty(t, tok, "nextToken must be empty string when no more pages")
			}
		})
	}
}

func TestListBrokers_PaginationContinuation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, name := range []string{"alpha", "beta", "gamma"} {
		createTestBroker(t, h, name, "ACTIVEMQ")
	}

	rec1 := doRequest(t, h, http.MethodGet, "/v1/brokers?maxResults=2", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var page1 map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &page1))

	var tok string
	require.NoError(t, json.Unmarshal(page1["nextToken"], &tok))
	require.NotEmpty(t, tok)

	rec2 := doRequest(t, h, http.MethodGet, "/v1/brokers?maxResults=2&nextToken="+tok, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var page2 map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &page2))

	var brokers2 []any
	require.NoError(t, json.Unmarshal(page2["brokerSummaries"], &brokers2))
	assert.Len(t, brokers2, 1, "page 2 should contain the remaining broker")

	// No further pages.
	if rawTok2, ok := page2["nextToken"]; ok {
		var tok2 string
		require.NoError(t, json.Unmarshal(rawTok2, &tok2))
		assert.Empty(t, tok2, "no nextToken on final page")
	}
}

func TestListBrokers_DefaultReturnsAll(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 5 {
		createTestBroker(t, h, "broker-list-"+string(rune('a'+i)), mq.EngineTypeActiveMQ)
	}

	rec := doRequest(t, h, http.MethodGet, "/v1/brokers", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseResponse(t, rec)
	summaries := out["brokerSummaries"].([]any)
	assert.Len(t, summaries, 5)

	_, hasToken := out["nextToken"]
	assert.False(t, hasToken, "nextToken must not appear when all results fit in one page")
}

func TestListBrokers_MaxResultsLimitsResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 5 {
		createTestBroker(t, h, "broker-page-"+string(rune('a'+i)), mq.EngineTypeActiveMQ)
	}

	rec := doRequest(t, h, http.MethodGet, "/v1/brokers?maxResults=3", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseResponse(t, rec)
	summaries := out["brokerSummaries"].([]any)
	assert.Len(t, summaries, 3, "maxResults=3 must return only 3 brokers")
	assert.Contains(t, out, "nextToken", "nextToken must appear when page is truncated")
}

func TestListBrokers_NextTokenPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	names := []string{
		"broker-pag-a", "broker-pag-b", "broker-pag-c", "broker-pag-d", "broker-pag-e",
	}
	for _, name := range names {
		createTestBroker(t, h, name, mq.EngineTypeActiveMQ)
	}

	// First page: 2 results.
	rec1 := doRequest(t, h, http.MethodGet, "/v1/brokers?maxResults=2", nil)
	require.Equal(t, http.StatusOK, rec1.Code)
	page1 := parseResponse(t, rec1)

	summaries1 := page1["brokerSummaries"].([]any)
	require.Len(t, summaries1, 2)

	nextToken, ok := page1["nextToken"].(string)
	require.True(t, ok && nextToken != "", "page 1 must return a nextToken")

	// Second page using the token.
	rec2 := doRequest(t, h, http.MethodGet, "/v1/brokers?maxResults=2&nextToken="+nextToken, nil)
	require.Equal(t, http.StatusOK, rec2.Code)
	page2 := parseResponse(t, rec2)

	summaries2 := page2["brokerSummaries"].([]any)
	require.Len(t, summaries2, 2)

	// Third page: 1 remaining.
	nextToken2, ok := page2["nextToken"].(string)
	require.True(t, ok && nextToken2 != "", "page 2 must return a nextToken")

	rec3 := doRequest(t, h, http.MethodGet, "/v1/brokers?maxResults=2&nextToken="+nextToken2, nil)
	require.Equal(t, http.StatusOK, rec3.Code)
	page3 := parseResponse(t, rec3)

	summaries3 := page3["brokerSummaries"].([]any)
	require.Len(t, summaries3, 1)

	_, hasMore := page3["nextToken"]
	assert.False(t, hasMore, "last page must not have a nextToken")

	allNames := make([]string, 0, 5)
	for _, page := range [][]any{summaries1, summaries2, summaries3} {
		for _, s := range page {
			allNames = append(allNames, s.(map[string]any)["brokerName"].(string))
		}
	}
	assert.Len(t, allNames, 5, "all 5 brokers must appear across pages")
}

func TestListBrokers_MaxResultsExceedingTotal(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestBroker(t, h, "only-broker", mq.EngineTypeActiveMQ)

	rec := doRequest(t, h, http.MethodGet, "/v1/brokers?maxResults=100", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseResponse(t, rec)
	summaries := out["brokerSummaries"].([]any)
	assert.Len(t, summaries, 1)
	_, hasToken := out["nextToken"]
	assert.False(t, hasToken)
}

func TestListBrokers_SummaryContainsRequiredFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestBroker(t, h, "summary-fields-broker", mq.EngineTypeActiveMQ)

	rec := doRequest(t, h, http.MethodGet, "/v1/brokers", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	summaries := parseResponse(t, rec)["brokerSummaries"].([]any)
	require.Len(t, summaries, 1)

	summary := summaries[0].(map[string]any)
	requiredFields := []string{
		"brokerArn", "brokerId", "brokerName", "brokerState",
		"deploymentMode", "engineType", "hostInstanceType", "created",
	}

	for _, f := range requiredFields {
		assert.Contains(t, summary, f, "broker summary must contain %q", f)
	}
}

func TestListBrokers_SortedByName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"broker-z", "broker-a", "broker-m"} {
		createTestBroker(t, h, name, mq.EngineTypeActiveMQ)
	}

	rec := doRequest(t, h, http.MethodGet, "/v1/brokers", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	summaries := parseResponse(t, rec)["brokerSummaries"].([]any)
	require.Len(t, summaries, 3)

	names := make([]string, len(summaries))
	for i, s := range summaries {
		names[i] = s.(map[string]any)["brokerName"].(string)
	}
	assert.Equal(t, []string{"broker-a", "broker-m", "broker-z"}, names,
		"ListBrokers must return brokers sorted by name")
}

func TestListBrokers_EmptyReturnsBrokerSummariesKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/v1/brokers", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	_, hasKey := parseResponse(t, rec)["brokerSummaries"]
	assert.True(t, hasKey, "brokerSummaries key must always be present")
}

func TestListBrokers_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/v1/brokers", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)

	summaries, hasSummaries := resp["brokerSummaries"]
	assert.True(t, hasSummaries, "ListBrokers must include 'brokerSummaries' key")
	assert.IsType(t, []any{}, summaries, "'brokerSummaries' must be an array")
	assert.Empty(t, summaries, "'brokerSummaries' must be [] when no brokers exist")

	_, hasNext := resp["nextToken"]
	assert.False(t, hasNext, "nextToken must be absent when no results")
}

func TestListBrokers_NextTokenAbsentWhenNotTruncated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestBroker(t, h, "broker-a", mq.EngineTypeActiveMQ)
	createTestBroker(t, h, "broker-b", mq.EngineTypeActiveMQ)

	rec := doRequest(t, h, http.MethodGet, "/v1/brokers", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	_, hasNext := parseResponse(t, rec)["nextToken"]
	assert.False(t, hasNext, "nextToken must be absent when all results fit on one page")
}
