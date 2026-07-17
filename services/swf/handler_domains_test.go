package swf_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_DescribeDomain_ReturnsArn verifies DescribeDomain returns an ARN in domainInfo.
func TestHandler_DescribeDomain_ReturnsArn(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)
	doSWFRequest(t, h, "RegisterDomain", map[string]any{
		"name":                                   "my-domain",
		"workflowExecutionRetentionPeriodInDays": "7",
	})
	rec := doSWFRequest(t, h, "DescribeDomain", map[string]any{"name": "my-domain"})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseSWFResp(t, rec)

	info, ok := resp["domainInfo"].(map[string]any)
	require.True(t, ok, "domainInfo missing")
	assert.Equal(t, "my-domain", info["name"])
	assert.Equal(t, "REGISTERED", info["status"])
	assert.NotEmpty(t, info["arn"], "domainInfo.arn must be present")
	_, hasRetention := info["workflowExecutionRetentionPeriodInDays"]
	assert.False(t, hasRetention, "domainInfo must not contain workflowExecutionRetentionPeriodInDays")

	cfg, ok := resp["configuration"].(map[string]any)
	require.True(t, ok, "configuration missing")
	assert.Equal(t, "7", cfg["workflowExecutionRetentionPeriodInDays"])
}

// TestHandler_ListDomains_NoDomainInfoRetention verifies ListDomains omits retention from domainInfos.
func TestHandler_ListDomains_NoDomainInfoRetention(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		domains    []string
		wantCount  int
	}{
		{
			name:       "registered_domains",
			domains:    []string{"d1", "d2"},
			wantCount:  2,
			wantStatus: "REGISTERED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)
			for _, d := range tt.domains {
				doSWFRequest(t, h, "RegisterDomain", map[string]any{
					"name": d, "workflowExecutionRetentionPeriodInDays": "30",
				})
			}
			rec := doSWFRequest(t, h, "ListDomains", map[string]any{"registrationStatus": "REGISTERED"})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseSWFResp(t, rec)

			infos, ok := resp["domainInfos"].([]any)
			require.True(t, ok)
			assert.Len(t, infos, tt.wantCount)

			for _, raw := range infos {
				info := raw.(map[string]any)
				_, hasRetention := info["workflowExecutionRetentionPeriodInDays"]
				assert.False(t, hasRetention, "domainInfos must not contain retention period")
				assert.NotEmpty(t, info["arn"])
			}
		})
	}
}

func TestHandler_ListDomains_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)

	for _, name := range []string{"domain-a", "domain-b", "domain-c"} {
		rec := doSWFRequest(t, h, "RegisterDomain", map[string]any{"name": name, "description": "test"})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		body          map[string]any
		name          string
		wantMinCount  int
		wantNextToken bool
	}{
		{
			name:         "all domains no limit",
			body:         map[string]any{"registrationStatus": "REGISTERED"},
			wantMinCount: 3,
		},
		{
			name:          "paginated maximumPageSize=1",
			body:          map[string]any{"registrationStatus": "REGISTERED", "maximumPageSize": 1},
			wantMinCount:  1,
			wantNextToken: true,
		},
		{
			name:         "paginated maximumPageSize=2",
			body:         map[string]any{"registrationStatus": "REGISTERED", "maximumPageSize": 2},
			wantMinCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSWFRequest(t, h, "ListDomains", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseSWFResp(t, rec)

			infos, ok := resp["domainInfos"].([]any)
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(infos), tt.wantMinCount)

			if tt.wantNextToken {
				assert.NotEmpty(t, resp["nextPageToken"])
			}
		})
	}
}

func TestHandler_ListDomains_TokenChaining(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)

	domains := []string{"domain-x", "domain-y", "domain-z"}
	for _, name := range domains {
		rec := doSWFRequest(t, h, "RegisterDomain", map[string]any{"name": name, "description": "test"})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// First page: maximumPageSize=2.
	rec := doSWFRequest(t, h, "ListDomains", map[string]any{
		"registrationStatus": "REGISTERED",
		"maximumPageSize":    2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	page1 := parseSWFResp(t, rec)

	infos1 := page1["domainInfos"].([]any)
	assert.Len(t, infos1, 2)

	nextToken, ok := page1["nextPageToken"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, nextToken)

	// Second page using the token.
	rec2 := doSWFRequest(t, h, "ListDomains", map[string]any{
		"registrationStatus": "REGISTERED",
		"maximumPageSize":    2,
		"nextPageToken":      nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	page2 := parseSWFResp(t, rec2)

	infos2 := page2["domainInfos"].([]any)
	assert.GreaterOrEqual(t, len(infos2), 1)

	// No duplicates.
	names1 := make(map[string]bool)
	for _, d := range infos1 {
		dm := d.(map[string]any)
		names1[dm["name"].(string)] = true
	}

	for _, d := range infos2 {
		dm := d.(map[string]any)
		assert.False(t, names1[dm["name"].(string)], "duplicate domain in page 2")
	}
}

func TestHandler_UndeprecateDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		setup    []setupAction
		wantCode int
	}{
		{
			name: "success",
			setup: []setupAction{
				{action: "RegisterDomain", body: map[string]any{"name": "d1"}},
				{action: "DeprecateDomain", body: map[string]any{"name": "d1"}},
			},
			body:     map[string]any{"name": "d1"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     map[string]any{"name": "missing"},
			wantCode: http.StatusNotFound,
		},
		{
			name: "already_registered",
			setup: []setupAction{
				{action: "RegisterDomain", body: map[string]any{"name": "d2"}},
			},
			body:     map[string]any{"name": "d2"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)
			for _, s := range tt.setup {
				doSWFRequest(t, h, s.action, s.body)
			}

			rec := doSWFRequest(t, h, "UndeprecateDomain", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
