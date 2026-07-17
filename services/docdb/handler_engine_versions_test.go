package docdb_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDescribeDBEngineVersions_Filter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		engine        string
		engineVersion string
		wantContains  string
		wantStatus    int
	}{
		{
			name:         "no_filter_returns_all",
			wantStatus:   http.StatusOK,
			wantContains: "DescribeDBEngineVersionsResponse",
		},
		{
			name:         "filter_by_engine",
			engine:       "docdb",
			wantStatus:   http.StatusOK,
			wantContains: "docdb",
		},
		{
			name:          "filter_by_version",
			engineVersion: "4.0.0",
			wantStatus:    http.StatusOK,
			wantContains:  "4.0.0",
		},
		{
			name:          "unknown_version_returns_empty_list",
			engineVersion: "99.0.0",
			wantStatus:    http.StatusOK,
			wantContains:  "DescribeDBEngineVersionsResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":  {"DescribeDBEngineVersions"},
				"Version": {"2014-10-31"},
			}
			if tt.engine != "" {
				vals.Set("Engine", tt.engine)
			}
			if tt.engineVersion != "" {
				vals.Set("EngineVersion", tt.engineVersion)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// TestParity_EngineVersions_36 verifies 3.6.0 appears in DescribeDBEngineVersions.
func TestEngineVersions_36(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeDBEngineVersions"},
		"Version": {"2014-10-31"},
		"Engine":  {"docdb"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "3.6.0", "DescribeDBEngineVersions must include version 3.6.0")
}
