package redshiftdata_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeWorkgroups map[string]string

func (f fakeWorkgroups) WorkgroupStatus(_, name string) (string, bool) {
	s, ok := f[name]

	return s, ok
}

func TestHandler_WorkgroupResolution(t *testing.T) {
	t.Parallel()

	resolver := fakeWorkgroups{"wg-up": "AVAILABLE", "wg-new": "CREATING"}

	exec := func(wg string) map[string]any {
		return map[string]any{"WorkgroupName": wg, "Sql": "select 1", "Database": "dev"}
	}
	batch := map[string]any{"WorkgroupName": "wg-gone", "Sqls": []string{"select 1"}, "Database": "dev"}
	list := map[string]any{"WorkgroupName": "wg-gone", "Database": "dev"}
	cluster := map[string]any{"ClusterIdentifier": "c1", "Sql": "select 1", "Database": "dev"}

	const (
		notFound = "ResourceNotFoundException"
		invalid  = "ValidationException"
	)

	tests := []struct {
		body     map[string]any
		name     string
		op       string
		wantType string
		wantCode int
		wire     bool
	}{
		{exec("wg-up"), "execute known", "ExecuteStatement", "", http.StatusOK, true},
		{exec("wg-gone"), "execute unknown", "ExecuteStatement", notFound, http.StatusBadRequest, true},
		{exec("wg-new"), "execute creating", "ExecuteStatement", invalid, http.StatusBadRequest, true},
		{exec("wg-gone"), "execute unwired", "ExecuteStatement", "", http.StatusOK, false},
		{batch, "batch unknown", "BatchExecuteStatement", notFound, http.StatusBadRequest, true},
		{list, "list databases unknown", "ListDatabases", notFound, http.StatusBadRequest, true},
		{list, "list schemas unknown", "ListSchemas", notFound, http.StatusBadRequest, true},
		{list, "list tables unknown", "ListTables", notFound, http.StatusBadRequest, true},
		{list, "describe table unknown", "DescribeTable", notFound, http.StatusBadRequest, true},
		{cluster, "cluster untouched", "ExecuteStatement", "", http.StatusOK, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.wire {
				h.SetWorkgroupResolver(resolver)
			}

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

			if tt.wantType != "" {
				require.Contains(t, rec.Body.String(), tt.wantType)
			}
		})
	}
}
