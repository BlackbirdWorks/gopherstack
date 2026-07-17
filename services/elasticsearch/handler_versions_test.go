package elasticsearch_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestElasticsearchHandler_ListElasticsearchVersions verifies all 22 valid
// versions are returned.
func TestElasticsearchHandler_ListElasticsearchVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		expectedVersion string
	}{
		{name: "7.17", expectedVersion: "7.17"},
		{name: "7.10", expectedVersion: "7.10"},
		{name: "6.8", expectedVersion: "6.8"},
		{name: "5.1", expectedVersion: "5.1"},
		{name: "1.5", expectedVersion: "1.5"},
	}

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodGet, "/2015-01-01/es/versions", nil)
	t.Cleanup(func() { resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out struct {
		ElasticsearchVersions []string `json:"ElasticsearchVersions"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.ElasticsearchVersions, 22, "expected all 22 versions")

	versionSet := make(map[string]bool)
	for _, v := range out.ElasticsearchVersions {
		versionSet[v] = true
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.True(t, versionSet[tc.expectedVersion], "version %s should be in the list", tc.expectedVersion)
		})
	}
}

// TestElasticsearchHandler_CompatibleVersionsDomain verifies
// GetCompatibleVersions respects the domainName param.
func TestElasticsearchHandler_CompatibleVersionsDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		domainVersion     string
		wantSourceVersion string
	}{
		{
			name:              "6.8_domain",
			domainVersion:     "6.8",
			wantSourceVersion: "6.8",
		},
		{
			name:              "7.10_domain",
			domainVersion:     "7.10",
			wantSourceVersion: "7.10",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			resp := doRequest(t, h, http.MethodPost, "/2015-01-01/es/domain", map[string]any{
				"DomainName":           "compat-domain",
				"ElasticsearchVersion": tc.domainVersion,
			})
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			compatResp := doRequest(
				t,
				h,
				http.MethodGet,
				"/2015-01-01/es/compatibleVersions?domainName=compat-domain",
				nil,
			)
			defer compatResp.Body.Close()
			require.Equal(t, http.StatusOK, compatResp.StatusCode)

			var out struct {
				CompatibleElasticsearchVersions []struct {
					SourceVersion  string   `json:"SourceVersion"`
					TargetVersions []string `json:"TargetVersions"`
				} `json:"CompatibleElasticsearchVersions"`
			}
			require.NoError(t, json.NewDecoder(compatResp.Body).Decode(&out))
			require.Len(t, out.CompatibleElasticsearchVersions, 1)
			assert.Equal(t, tc.wantSourceVersion, out.CompatibleElasticsearchVersions[0].SourceVersion)
		})
	}
}

// TestElasticsearchHandler_VersionAndInstanceMetadata verifies the
// compatible-versions, instance-types, and instance-type-limits endpoints
// return non-empty responses.
func TestElasticsearchHandler_VersionAndInstanceMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	resp := doRequest(t, h, http.MethodGet, "/2015-01-01/es/compatibleVersions", nil)
	assert.NotEmpty(t, readJSONBody(t, resp)["CompatibleElasticsearchVersions"])

	resp = doRequest(t, h, http.MethodGet, "/2015-01-01/es/instanceTypes/7.10", nil)
	assert.NotEmpty(t, readJSONBody(t, resp)["ElasticsearchInstanceTypes"])

	resp = doRequest(t, h, http.MethodGet, "/2015-01-01/es/instanceTypeLimits/t3.small.elasticsearch/7.10", nil)
	assert.NotEmpty(t, readJSONBody(t, resp)["LimitsByRole"])
}
