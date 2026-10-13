package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunTaskCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		count int
		want  int
	}{
		{name: "one", count: 1, want: 1},
		{name: "ten", count: 10, want: 10},
		{name: "above_hint", count: 20, want: 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "cnt"})
			doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
				"family":               "cnt-task",
				"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
			})

			resp := doECSRequest(t, h, "RunTask", map[string]any{
				"cluster": "cnt", "taskDefinition": "cnt-task", "count": tt.count,
			})

			require.Equal(t, http.StatusOK, resp.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
			assert.Len(t, out["tasks"], tt.want)
		})
	}
}
