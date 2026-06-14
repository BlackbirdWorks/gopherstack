package mq_test

// Tests for parity §B: MQ pagination uses opaque index-based tokens (go-nace).

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// decodeOpaqueToken decodes a base64 page token back to its integer index.
func decodeOpaqueToken(t *testing.T, token string) int {
	t.Helper()

	b, err := base64.StdEncoding.DecodeString(token)
	require.NoError(t, err, "next token must be valid base64")

	n, err := strconv.Atoi(string(b))
	require.NoError(t, err, "decoded token must be an integer offset")

	return n
}

// TestParityB_ListBrokers_PaginationOpaqueToken verifies that ListBrokers returns
// an opaque index-based token (not a broker name) when more pages exist.
func TestParityB_ListBrokers_PaginationOpaqueToken(t *testing.T) {
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

			h := newBatch1Handler(t)
			for _, name := range tt.brokerNames {
				createBatch1Broker(t, h, name, "ACTIVEMQ")
			}

			path := "/v1/brokers?maxResults=" + strconv.Itoa(tt.maxResults)
			rec := doAccuracyMQ(t, h, http.MethodGet, path, nil)
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
			} else {
				if hasToken {
					var tok string
					require.NoError(t, json.Unmarshal(rawToken, &tok))
					assert.Empty(t, tok, "nextToken must be empty string when no more pages")
				}
			}
		})
	}
}

// TestParityB_ListBrokers_PaginationContinuation verifies that a nextToken from
// page 1 correctly retrieves page 2.
func TestParityB_ListBrokers_PaginationContinuation(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler(t)
	for _, name := range []string{"alpha", "beta", "gamma"} {
		createBatch1Broker(t, h, name, "ACTIVEMQ")
	}

	rec1 := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers?maxResults=2", nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var page1 map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &page1))

	var tok string
	require.NoError(t, json.Unmarshal(page1["nextToken"], &tok))
	require.NotEmpty(t, tok)

	rec2 := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers?maxResults=2&nextToken="+tok, nil)
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

// TestParityB_ListConfigurations_PaginationOpaqueToken verifies that
// ListConfigurations uses the same opaque index-based pagination.
func TestParityB_ListConfigurations_PaginationOpaqueToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		configNames   []string
		maxResults    int
		wantCount     int
		wantNextToken bool
	}{
		{
			name:          "single_page_no_token",
			configNames:   []string{"cfg-a"},
			maxResults:    10,
			wantCount:     1,
			wantNextToken: false,
		},
		{
			name:          "two_pages_opaque_token",
			configNames:   []string{"cfg-1", "cfg-2", "cfg-3"},
			maxResults:    1,
			wantCount:     1,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newBatch1Handler(t)
			for _, name := range tt.configNames {
				createBatch1Config(t, h, name, "ACTIVEMQ")
			}

			path := "/v1/configurations?maxResults=" + strconv.Itoa(tt.maxResults)
			rec := doAccuracyMQ(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			var cfgs []any
			require.NoError(t, json.Unmarshal(out["configurations"], &cfgs))
			assert.Len(t, cfgs, tt.wantCount)

			rawToken, hasToken := out["nextToken"]
			if tt.wantNextToken {
				require.True(t, hasToken)

				var tok string
				require.NoError(t, json.Unmarshal(rawToken, &tok))

				offset := decodeOpaqueToken(t, tok)
				assert.Equal(t, tt.wantCount, offset)
			} else {
				if hasToken {
					var tok string
					require.NoError(t, json.Unmarshal(rawToken, &tok))
					assert.Empty(t, tok)
				}
			}
		})
	}
}
