package gcpsql

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseRoute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		method string
		path   string
		want   route
	}{
		{"GET", "/v1/flags", route{op: opListFlags}},
		{"POST", "/v1/projects/p/instances", route{op: opInsertInstance, project: "p"}},
		{"GET", "/sql/v1beta4/projects/p/instances/i", route{op: opGetInstance, project: "p", instance: "i"}},
		{"PUT", "/sql/v1/projects/p/instances/i", route{op: opUpdateInstance, project: "p", instance: "i"}},
		{"POST", "/v1/projects/p/instances/i/restart", route{op: opRestartInstance, project: "p", instance: "i"}},
		{
			"POST",
			"/v1/projects/p/instances/i:generateEphemeralCert",
			route{op: opGenerateEphemeral, project: "p", instance: "i"},
		},
		{
			"GET",
			"/v1/projects/p/instances/i/connectSettings",
			route{op: opGetConnectSettings, project: "p", instance: "i"},
		},
		{
			"DELETE",
			"/v1/projects/p/instances/i/databases/d",
			route{op: opDeleteDatabase, project: "p", instance: "i", name: "d"},
		},
		{"DELETE", "/v1/projects/p/instances/i/users", route{op: opDeleteUser, project: "p", instance: "i"}},
		{"GET", "/v1/projects/p/instances/i/users/u", route{op: opGetUser, project: "p", instance: "i", name: "u"}},
		{
			"GET",
			"/v1/projects/p/instances/i/backupRuns/9",
			route{op: opGetBackupRun, project: "p", instance: "i", name: "9"},
		},
		{"POST", "/v1/projects/p/operations/o/cancel", route{op: opCancelOperation, project: "p", name: "o"}},
		{"GET", "/v1/projects/p/operations", route{op: opListOperations, project: "p"}},
		{"GET", "/v1/projects/p/tiers", route{op: opListTiers, project: "p"}},
		{"PATCH", "/v1/projects/p/instances/i/users", route{op: unknownOperation, project: "p", instance: "i"}},
	}

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequestWithContext(t.Context(), tt.method, tt.path, nil)
			assert.Equal(t, tt.want, parseRoute(r))
		})
	}

	_ = http.MethodGet
}
