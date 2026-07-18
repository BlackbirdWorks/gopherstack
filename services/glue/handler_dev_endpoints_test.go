package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestDevEndpoint_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		check       func(t *testing.T, body string)
		name        string
		op          string
		wantCode    int
		skipPreSeed bool
	}{
		{
			name:        "create",
			op:          "CreateDevEndpoint",
			body:        map[string]any{"EndpointName": "dep1"},
			wantCode:    http.StatusOK,
			skipPreSeed: true,
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "dep1")
				assert.Contains(t, body, "Status")
			},
		},
		{
			name:     "create-duplicate",
			op:       "CreateDevEndpoint",
			body:     map[string]any{"EndpointName": "dep1"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get",
			op:       "GetDevEndpoint",
			body:     map[string]any{"EndpointName": "dep1"},
			wantCode: http.StatusOK,
		},
		{
			name:     "get-missing",
			op:       "GetDevEndpoint",
			body:     map[string]any{"EndpointName": "no-dep"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get-dev-endpoints",
			op:       "GetDevEndpoints",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name:     "list-dev-endpoints",
			op:       "ListDevEndpoints",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name: "batch-get-dev-endpoints",
			op:   "BatchGetDevEndpoints",
			body: map[string]any{
				"DevEndpointNames": []string{"dep1", "no-dep"},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "update",
			op:   "UpdateDevEndpoint",
			body: map[string]any{
				"EndpointName": "dep1",
				"AddArguments": map[string]any{"--enable-glue-datacatalog": ""},
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete",
			op:       "DeleteDevEndpoint",
			body:     map[string]any{"EndpointName": "dep1"},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete-missing",
			op:       "DeleteDevEndpoint",
			body:     map[string]any{"EndpointName": "no-dep"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if !tt.skipPreSeed {
				doGlueRequest(t, h, "CreateDevEndpoint", map[string]any{"EndpointName": "dep1"})
			}

			rec := doGlueRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.check != nil {
				tt.check(t, rec.Body.String())
			}
		})
	}
}

func TestDevEndpoint_UpdateAndGet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateDevEndpoint", map[string]any{"EndpointName": "dep-upd"})

	doGlueRequest(t, h, "UpdateDevEndpoint", map[string]any{
		"EndpointName": "dep-upd",
		"AddArguments": map[string]any{"--conf": "spark.executor.memory=4g"},
	})

	getRec := doGlueRequest(t, h, "GetDevEndpoint", map[string]any{"EndpointName": "dep-upd"})
	require.Equal(t, http.StatusOK, getRec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
	dep := out["DevEndpoint"].(map[string]any)
	args, ok := dep["Arguments"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "spark.executor.memory=4g", args["--conf"])
}

// TestUpdateDevEndpoint tests UpdateDevEndpoint.
func TestUpdateDevEndpoint(t *testing.T) {
	t.Parallel()
	b := glue.NewInMemoryBackend("123456789012", "us-east-1")
	h := glue.NewHandler(b)

	// Create a dev endpoint via internal helper.
	ep := &glue.DevEndpoint{EndpointName: "my-endpoint", Status: "READY"}
	b.AddDevEndpointInternal(ep)

	// UpdateDevEndpoint
	dispatchNewOp(t, h, "UpdateDevEndpoint", map[string]any{
		"EndpointName": "my-endpoint",
		"AddArguments": map[string]any{"GLUE_PYTHON_VERSION": "3"},
	})
}
