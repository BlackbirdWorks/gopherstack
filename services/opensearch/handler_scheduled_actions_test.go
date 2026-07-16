package opensearch_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScheduledActions_ListWithDomainFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		domain     string
		queryParam string
		wantLen    int
	}{
		{
			name:       "filter_returns_domain_actions",
			domain:     "sched-domain",
			queryParam: "sched-domain",
			wantLen:    1,
		},
		{
			name:       "filter_different_domain_returns_empty",
			domain:     "sched-domain",
			queryParam: "other-domain",
			wantLen:    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			// Schedule an action for the domain.
			sr := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/scheduledActions/update",
				map[string]any{
					"DomainName": tt.domain,
					"ScheduledAction": map[string]any{
						"Id":            "action-1",
						"Type":          "JVM_HEAP_SIZE_TUNING",
						"ScheduledTime": "2026-07-01T00:00:00Z",
						"ScheduleAt":    "TIMESTAMP",
					},
				})
			sr.Body.Close()

			resp := doRequest(t, h, http.MethodGet,
				"/2021-01-01/opensearch/scheduledActions?DomainName="+tt.queryParam, nil)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
			actions, ok := out["ScheduledActions"].([]any)
			require.True(t, ok)
			assert.Len(t, actions, tt.wantLen)
		})
	}
}

func TestScheduledActions_UpdateReturnsAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPut, "/2021-01-01/opensearch/scheduledActions/update",
		map[string]any{
			"DomainName": "upd-sched-domain",
			"ScheduledAction": map[string]any{
				"Id":            "action-upd",
				"Type":          "SERVICE_SOFTWARE_UPDATE",
				"ScheduledTime": "2026-08-01T00:00:00Z",
				"ScheduleAt":    "TIMESTAMP",
			},
		})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	action, ok := out["ScheduledAction"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "action-upd", action["Id"])
}
