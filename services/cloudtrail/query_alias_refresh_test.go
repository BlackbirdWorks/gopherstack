package cloudtrail_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryAliasAndDashboardRefresh(t *testing.T) {
	t.Parallel()

	const stmt = "SELECT eventName FROM eds-1"

	tests := []struct {
		name      string
		alias     string
		refresh   bool
		wantStart int
		wantDescr int
	}{
		{name: "alias_resolves_widget", alias: "w1", wantStart: http.StatusOK},
		{name: "unknown_alias_rejected", alias: "nope", wantStart: http.StatusBadRequest},
		{
			name: "refresh_id_selects_query", alias: "w1", refresh: true,
			wantStart: http.StatusOK, wantDescr: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestCloudTrailHandler()

			rec := doCloudTrailOp(t, h, "CreateDashboard", map[string]any{
				"Name": "d1",
				"Widgets": []any{map[string]any{
					"QueryAlias": "w1", "QueryStatement": stmt, "ViewProperties": map[string]any{"title": "t"},
				}},
			})
			require.Equal(t, http.StatusOK, rec.Code)
			dash, _ := parseCloudTrailResp(t, rec)["DashboardArn"].(string)

			start := doCloudTrailOp(t, h, "StartQuery", map[string]any{
				"QueryAlias": tt.alias, "QueryParameters": []string{"$Period$"},
			})
			require.Equal(t, tt.wantStart, start.Code)

			if tt.wantStart != http.StatusOK {
				return
			}

			qid, _ := parseCloudTrailResp(t, start)["QueryId"].(string)
			desc := doCloudTrailOp(t, h, "DescribeQuery", map[string]any{"QueryId": qid})
			require.Equal(t, http.StatusOK, desc.Code)
			assert.Equal(t, stmt, parseCloudTrailResp(t, desc)["QueryString"])

			if !tt.refresh {
				return
			}

			ref := doCloudTrailOp(t, h, "StartDashboardRefresh", map[string]any{"DashboardId": dash})
			require.Equal(t, http.StatusOK, ref.Code)
			rid, _ := parseCloudTrailResp(t, ref)["RefreshId"].(string)

			byRefresh := doCloudTrailOp(t, h, "DescribeQuery", map[string]any{"QueryAlias": "w1", "RefreshId": rid})
			require.Equal(t, tt.wantDescr, byRefresh.Code)
			assert.NotEqual(t, qid, parseCloudTrailResp(t, byRefresh)["QueryId"])

			missing := doCloudTrailOp(
				t,
				h,
				"DescribeQuery",
				map[string]any{"QueryAlias": "w1", "RefreshId": "refresh-x"},
			)
			assert.Equal(t, http.StatusNotFound, missing.Code)
		})
	}
}
