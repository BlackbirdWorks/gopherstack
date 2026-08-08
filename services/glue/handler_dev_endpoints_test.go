package glue_test

import (
	"encoding/json"
	"net/http"
	"strconv"
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
			name: "create",
			op:   "CreateDevEndpoint",
			body: map[string]any{
				"EndpointName": "dep1",
				"RoleArn":      "arn:aws:iam::123456789012:role/dep-role",
			},
			wantCode:    http.StatusOK,
			skipPreSeed: true,
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "dep1")
				assert.Contains(t, body, "Status")
			},
		},
		{
			name: "create-duplicate",
			op:   "CreateDevEndpoint",
			body: map[string]any{
				"EndpointName": "dep1",
				"RoleArn":      "arn:aws:iam::123456789012:role/dep-role",
			},
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
				doGlueRequest(t, h, "CreateDevEndpoint", map[string]any{
					"EndpointName": "dep1",
					"RoleArn":      "arn:aws:iam::123456789012:role/dep-role",
				})
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
	doGlueRequest(t, h, "CreateDevEndpoint", map[string]any{
		"EndpointName": "dep-upd",
		"RoleArn":      "arn:aws:iam::123456789012:role/dep-role",
	})

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

// TestCreateDevEndpoint_RequiresRoleArn confirms CreateDevEndpoint rejects
// requests missing the (AWS-required) RoleArn field.
func TestCreateDevEndpoint_RequiresRoleArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doGlueRequest(t, h, "CreateDevEndpoint", map[string]any{"EndpointName": "no-role"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidInputException")
}

// TestUpdateDevEndpoint_PublicKeys covers AddPublicKeys/DeletePublicKeys
// semantics, mirroring UpdateDevEndpointInput.
func TestUpdateDevEndpoint_PublicKeys(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateDevEndpoint", map[string]any{
		"EndpointName": "keyed",
		"RoleArn":      "arn:aws:iam::123456789012:role/dep-role",
		"PublicKeys":   []string{"key-a", "key-b"},
	})

	doGlueRequest(t, h, "UpdateDevEndpoint", map[string]any{
		"EndpointName":     "keyed",
		"AddPublicKeys":    []string{"key-c"},
		"DeletePublicKeys": []string{"key-a"},
	})

	getRec := doGlueRequest(t, h, "GetDevEndpoint", map[string]any{"EndpointName": "keyed"})
	require.Equal(t, http.StatusOK, getRec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
	dep := out["DevEndpoint"].(map[string]any)
	keysRaw, ok := dep["PublicKeys"].([]any)
	require.True(t, ok)

	keys := make([]string, 0, len(keysRaw))
	for _, k := range keysRaw {
		keys = append(keys, k.(string))
	}

	assert.ElementsMatch(t, []string{"key-b", "key-c"}, keys)
}

// TestDevEndpoint_ResourceNumberLimitExceeded covers gopherstack-dol3's
// quota-exception gap: AWS's real, published default quota is "Max
// development endpoint per account: 25" (docs.aws.amazon.com/general/latest/gr/glue.html),
// and CreateDevEndpoint's real error catalog documents
// ResourceNumberLimitExceededException (confirmed in
// aws-sdk-go-v2/service/glue/deserializers.go's
// awsAwsjson11_deserializeOpErrorCreateDevEndpoint switch).
func TestDevEndpoint_ResourceNumberLimitExceeded(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 25 {
		rec := doGlueRequest(t, h, "CreateDevEndpoint", map[string]any{
			"EndpointName": "dep-" + strconv.Itoa(i),
			"RoleArn":      "arn:aws:iam::123456789012:role/dep-role",
		})
		require.Equal(t, http.StatusOK, rec.Code, "endpoint %d should succeed under the limit", i)
	}

	rec := doGlueRequest(t, h, "CreateDevEndpoint", map[string]any{
		"EndpointName": "dep-over-limit",
		"RoleArn":      "arn:aws:iam::123456789012:role/dep-role",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "ResourceNumberLimitExceededException", out["__type"])
}
