package elasticsearch_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_DescribeDomainsUnprocessed verifies that DescribeElasticsearchDomains
// returns unknown domain names in UnprocessedDomains with structured error details,
// matching the AWS API contract. The previous implementation silently dropped
// unresolvable names, causing clients to miss partial-failure information.
func TestParity_DescribeDomainsUnprocessed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		create          []string
		query           []string
		wantFound       int
		wantUnprocessed int
	}{
		{
			name:            "all_found_empty_unprocessed",
			create:          []string{"dom-alpha", "dom-beta"},
			query:           []string{"dom-alpha", "dom-beta"},
			wantFound:       2,
			wantUnprocessed: 0,
		},
		{
			name:            "one_missing_one_unprocessed",
			create:          []string{"dom-exists"},
			query:           []string{"dom-exists", "dom-missing"},
			wantFound:       1,
			wantUnprocessed: 1,
		},
		{
			name:            "all_missing",
			create:          []string{},
			query:           []string{"never-created", "also-missing"},
			wantFound:       0,
			wantUnprocessed: 2,
		},
		{
			name:            "empty_query_no_unprocessed",
			create:          []string{},
			query:           []string{},
			wantFound:       0,
			wantUnprocessed: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			for _, name := range tt.create {
				resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
					"DomainName": name,
				})
				resp.Body.Close()
				require.Equal(t, http.StatusOK, resp.StatusCode)
			}

			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain-info", map[string]any{
				"DomainNames": tt.query,
			})
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out struct {
				DomainStatusList   []any `json:"DomainStatusList"`
				UnprocessedDomains []struct {
					DomainName   string `json:"DomainName"`
					ErrorDetails struct {
						ErrorType    string `json:"ErrorType"`
						ErrorMessage string `json:"ErrorMessage"`
					} `json:"ErrorDetails"`
				} `json:"UnprocessedDomains"`
			}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

			assert.Len(t, out.DomainStatusList, tt.wantFound,
				"DomainStatusList count")
			assert.Len(t, out.UnprocessedDomains, tt.wantUnprocessed,
				"UnprocessedDomains count")

			// UnprocessedDomains must always be present (not null) in AWS responses.
			assert.NotNil(t, out.UnprocessedDomains, "UnprocessedDomains must not be null")

			// Verify error structure on the missing entries.
			for _, up := range out.UnprocessedDomains {
				assert.NotEmpty(t, up.DomainName)
				assert.Equal(t, "ResourceNotFoundException", up.ErrorDetails.ErrorType)
				assert.NotEmpty(t, up.ErrorDetails.ErrorMessage)
			}
		})
	}
}

// TestParity_AddTags_DuplicateKeyRejected verifies that AddTags rejects a tag
// list containing duplicate keys with ValidationException, matching AWS behaviour.
// The previous implementation silently deduplicated by building a map.
func TestParity_AddTags_DuplicateKeyRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		domainName string
		name       string
		tags       []map[string]string
		wantCode   int
	}{
		{
			name:       "no_duplicates_accepted",
			domainName: "tag-dup-nodup",
			tags:       []map[string]string{{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "ops"}},
			wantCode:   http.StatusOK,
		},
		{
			name:       "duplicate_key_rejected",
			domainName: "tag-dup-dupkey",
			tags:       []map[string]string{{"Key": "env", "Value": "prod"}, {"Key": "env", "Value": "dev"}},
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "three_tags_one_duplicate_rejected",
			domainName: "tag-dup-three",
			tags: []map[string]string{
				{"Key": "a", "Value": "1"},
				{"Key": "b", "Value": "2"},
				{"Key": "a", "Value": "3"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:       "single_tag_accepted",
			domainName: "tag-dup-solo",
			tags:       []map[string]string{{"Key": "solo", "Value": "v"}},
			wantCode:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			arn := createDomainAndGetARN(t, h, tt.domainName)

			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/tags", map[string]any{
				"ARN":     arn,
				"TagList": tt.tags,
			})
			defer resp.Body.Close()

			assert.Equal(t, tt.wantCode, resp.StatusCode)
			if tt.wantCode == http.StatusBadRequest {
				var out map[string]any
				require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
				assert.Contains(t, out["message"], "Duplicate tag key")
			}
		})
	}
}
