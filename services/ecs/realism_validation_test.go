package ecs_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestECSWire_InputValidation(t *testing.T) {
	t.Parallel()

	td := map[string]any{
		"family":               "t1",
		"containerDefinitions": []map[string]any{{"name": "c", "image": "nginx", "memory": 128}},
	}

	tests := []struct {
		body     map[string]any
		name     string
		action   string
		wantType string
		wantCode int
	}{
		{
			name: "cluster bad name", action: "CreateCluster", body: map[string]any{"clusterName": "bad name"},
			wantCode: http.StatusBadRequest, wantType: "InvalidParameterException",
		},
		{
			name:     "cluster ok",
			action:   "CreateCluster",
			body:     map[string]any{"clusterName": "ok_name-1"},
			wantCode: http.StatusOK,
		},
		{
			name: "family bad name", action: "RegisterTaskDefinition",
			body: map[string]any{
				"family":               "bad family!",
				"containerDefinitions": td["containerDefinitions"],
			},
			wantCode: http.StatusBadRequest, wantType: "ClientException",
		},
		{
			name: "service cluster missing", action: "CreateService",
			body: map[string]any{
				"cluster": "nope", "serviceName": "s", "taskDefinition": "t1", "desiredCount": 1,
			},
			wantCode: http.StatusBadRequest, wantType: "ClusterNotFoundException",
		},
		{
			name: "service negative count", action: "CreateService",
			body: map[string]any{
				"cluster": "c1", "serviceName": "s", "taskDefinition": "t1", "desiredCount": -1,
			},
			wantCode: http.StatusBadRequest, wantType: "InvalidParameterException",
		},
		{
			name: "list bad token", action: "ListTasks",
			body:     map[string]any{"cluster": "c1", "nextToken": "zz"},
			wantCode: http.StatusBadRequest, wantType: "InvalidParameterException",
		},
		{
			name: "service ok", action: "CreateService",
			body: map[string]any{
				"cluster": "c1", "serviceName": "s", "taskDefinition": "t1", "desiredCount": 1,
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			require.Equal(
				t,
				http.StatusOK,
				doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "c1"}).Code,
			)
			require.Equal(t, http.StatusOK, doECSRequest(t, h, "RegisterTaskDefinition", td).Code)

			rec := doECSRequest(t, h, tt.action, tt.body)
			require.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

			if tt.wantType == "" {
				return
			}

			var out map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantType, out["__type"])
			assert.False(t, strings.HasPrefix(out["message"], tt.wantType), out["message"])
		})
	}
}
