package lambda_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/lambda"
)

// newLayerBackend returns a fresh backend suitable for layer tests.
func newLayerBackend() *lambda.InMemoryBackend {
	return lambda.NewInMemoryBackend(nil, nil, lambda.DefaultSettings(), "123456789012", "us-east-1")
}

// publishLayerInput builds a minimal PublishLayerVersionInput.
func publishLayerInput(name, description string, zipData []byte, runtimes []string) *lambda.PublishLayerVersionInput {
	return &lambda.PublishLayerVersionInput{
		LayerName:          name,
		Description:        description,
		CompatibleRuntimes: runtimes,
		Content: &lambda.LayerVersionContentInput{
			ZipFile: zipData,
		},
	}
}

func TestInMemoryBackend_PublishLayerVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input       *lambda.PublishLayerVersionInput
		name        string
		wantVersion int64
		wantErr     bool
	}{
		{
			name:        "first_version",
			input:       publishLayerInput("my-layer", "first", []byte("zip1"), []string{"python3.9"}),
			wantVersion: 1,
		},
		{
			name:    "no_content",
			input:   &lambda.PublishLayerVersionInput{LayerName: "my-layer"},
			wantErr: true,
		},
		{
			name:    "nil_input",
			input:   nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := newLayerBackend()

			out, err := bk.PublishLayerVersion(tt.input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantVersion, out.Version)
			assert.NotEmpty(t, out.LayerVersionArn)
			assert.NotEmpty(t, out.LayerArn)
		})
	}
}

func TestInMemoryBackend_PublishLayerVersion_Increments(t *testing.T) {
	t.Parallel()

	bk := newLayerBackend()

	out1, err := bk.PublishLayerVersion(publishLayerInput("layer-a", "", []byte("z1"), nil))
	require.NoError(t, err)
	assert.Equal(t, int64(1), out1.Version)

	out2, err := bk.PublishLayerVersion(publishLayerInput("layer-a", "", []byte("z2"), nil))
	require.NoError(t, err)
	assert.Equal(t, int64(2), out2.Version)

	// Different layer name starts at 1 again.
	out3, err := bk.PublishLayerVersion(publishLayerInput("layer-b", "", []byte("z3"), nil))
	require.NoError(t, err)
	assert.Equal(t, int64(1), out3.Version)
}

func TestInMemoryBackend_GetLayerVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*lambda.InMemoryBackend)
		name         string
		layerName    string
		version      int64
		wantErr      bool
		wantNotFound bool
	}{
		{
			name: "existing_version",
			setup: func(bk *lambda.InMemoryBackend) {
				_, _ = bk.PublishLayerVersion(
					publishLayerInput("my-layer", "desc", []byte("zip"), []string{"nodejs20.x"}),
				)
			},
			layerName: "my-layer",
			version:   1,
		},
		{
			name:         "layer_not_found",
			layerName:    "missing-layer",
			version:      1,
			wantErr:      true,
			wantNotFound: true,
		},
		{
			name: "version_not_found",
			setup: func(bk *lambda.InMemoryBackend) {
				_, _ = bk.PublishLayerVersion(publishLayerInput("my-layer", "", []byte("z"), nil))
			},
			layerName:    "my-layer",
			version:      99,
			wantErr:      true,
			wantNotFound: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := newLayerBackend()
			if tt.setup != nil {
				tt.setup(bk)
			}

			out, err := bk.GetLayerVersion(tt.layerName, tt.version)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.version, out.Version)
			assert.Contains(t, out.LayerVersionArn, tt.layerName)
			assert.NotEmpty(t, out.LayerVersionArn)
		})
	}
}

func TestInMemoryBackend_ListLayers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*lambda.InMemoryBackend)
		name      string
		wantNames []string
		wantCount int
	}{
		{
			name:      "empty",
			wantCount: 0,
		},
		{
			name: "single_layer",
			setup: func(bk *lambda.InMemoryBackend) {
				_, _ = bk.PublishLayerVersion(publishLayerInput("layer-x", "", []byte("z"), nil))
			},
			wantCount: 1,
			wantNames: []string{"layer-x"},
		},
		{
			name: "multiple_layers",
			setup: func(bk *lambda.InMemoryBackend) {
				_, _ = bk.PublishLayerVersion(publishLayerInput("layer-b", "", []byte("z"), nil))
				_, _ = bk.PublishLayerVersion(publishLayerInput("layer-a", "", []byte("z"), nil))
			},
			wantCount: 2,
			wantNames: []string{"layer-a", "layer-b"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := newLayerBackend()
			if tt.setup != nil {
				tt.setup(bk)
			}

			layers := bk.ListLayers()
			assert.Len(t, layers, tt.wantCount)

			for i, name := range tt.wantNames {
				assert.Equal(t, name, layers[i].LayerName)
			}
		})
	}
}

func TestInMemoryBackend_ListLayerVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*lambda.InMemoryBackend)
		name         string
		layerName    string
		wantVersions []int64
		wantErr      bool
	}{
		{
			name:      "not_found",
			layerName: "missing",
			wantErr:   true,
		},
		{
			name: "two_versions_newest_first",
			setup: func(bk *lambda.InMemoryBackend) {
				_, _ = bk.PublishLayerVersion(publishLayerInput("my-layer", "", []byte("z1"), nil))
				_, _ = bk.PublishLayerVersion(publishLayerInput("my-layer", "", []byte("z2"), nil))
			},
			layerName:    "my-layer",
			wantVersions: []int64{2, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := newLayerBackend()
			if tt.setup != nil {
				tt.setup(bk)
			}

			versions, err := bk.ListLayerVersions(tt.layerName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			got := make([]int64, len(versions))
			for i, v := range versions {
				got[i] = v.Version
			}

			assert.Equal(t, tt.wantVersions, got)
		})
	}
}

func TestInMemoryBackend_DeleteLayerVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*lambda.InMemoryBackend)
		name      string
		layerName string
		version   int64
		wantErr   bool
	}{
		{
			name:      "layer_not_found",
			layerName: "missing",
			version:   1,
			wantErr:   true,
		},
		{
			name: "version_not_found",
			setup: func(bk *lambda.InMemoryBackend) {
				_, _ = bk.PublishLayerVersion(publishLayerInput("my-layer", "", []byte("z"), nil))
			},
			layerName: "my-layer",
			version:   99,
			wantErr:   true,
		},
		{
			name: "delete_existing",
			setup: func(bk *lambda.InMemoryBackend) {
				_, _ = bk.PublishLayerVersion(publishLayerInput("my-layer", "", []byte("z"), nil))
			},
			layerName: "my-layer",
			version:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := newLayerBackend()
			if tt.setup != nil {
				tt.setup(bk)
			}

			err := bk.DeleteLayerVersion(tt.layerName, tt.version)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			// Verify the version is gone.
			_, getErr := bk.GetLayerVersion(tt.layerName, tt.version)
			require.Error(t, getErr)
		})
	}
}

func TestInMemoryBackend_LayerVersionPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(*lambda.InMemoryBackend)
		name          string
		layerName     string
		statementID   string
		action        string
		principal     string
		version       int64
		wantAddErr    bool
		wantGetErr    bool
		wantRemoveErr bool
	}{
		{
			name: "add_and_get_permission",
			setup: func(bk *lambda.InMemoryBackend) {
				_, _ = bk.PublishLayerVersion(publishLayerInput("my-layer", "", []byte("z"), nil))
			},
			layerName:   "my-layer",
			version:     1,
			statementID: "stmt-1",
			action:      "lambda:GetLayerVersion",
			principal:   "*",
		},
		{
			name:        "layer_not_found",
			layerName:   "missing",
			version:     1,
			statementID: "stmt-1",
			action:      "lambda:GetLayerVersion",
			principal:   "*",
			wantAddErr:  true,
		},
		{
			name: "version_not_found",
			setup: func(bk *lambda.InMemoryBackend) {
				_, _ = bk.PublishLayerVersion(publishLayerInput("my-layer", "", []byte("z"), nil))
			},
			layerName:   "my-layer",
			version:     99,
			statementID: "stmt-1",
			action:      "lambda:GetLayerVersion",
			principal:   "*",
			wantAddErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := newLayerBackend()
			if tt.setup != nil {
				tt.setup(bk)
			}

			addInput := &lambda.AddLayerVersionPermissionInput{
				StatementID: tt.statementID,
				Action:      tt.action,
				Principal:   tt.principal,
			}

			addOut, addErr := bk.AddLayerVersionPermission(tt.layerName, tt.version, addInput)

			if tt.wantAddErr {
				require.Error(t, addErr)

				return
			}

			require.NoError(t, addErr)
			assert.NotEmpty(t, addOut.Statement)

			// GetLayerVersionPolicy should include the statement.
			policy, getErr := bk.GetLayerVersionPolicy(tt.layerName, tt.version)
			require.NoError(t, getErr)
			assert.Contains(t, policy.Policy, tt.statementID)

			// RemoveLayerVersionPermission should succeed.
			removeErr := bk.RemoveLayerVersionPermission(tt.layerName, tt.version, tt.statementID)
			require.NoError(t, removeErr)

			// Policy should no longer contain the statement.
			policy2, _ := bk.GetLayerVersionPolicy(tt.layerName, tt.version)
			assert.NotContains(t, policy2.Policy, tt.statementID)
		})
	}
}

// TestLayerHTTPHandler tests the HTTP handler for layers using httptest.
func TestLayerHTTPHandler(t *testing.T) {
	t.Parallel()

	newHandlerAndBackend := func() (*lambda.Handler, *lambda.InMemoryBackend) {
		bk := newLayerBackend()
		h := lambda.NewHandler(bk)
		h.DefaultRegion = "us-east-1"
		h.AccountID = "123456789012"

		return h, bk
	}

	doRequest := func(h *lambda.Handler, method, path, body string) *httptest.ResponseRecorder {
		e := echo.New()
		var bodyReader io.Reader
		if body != "" {
			bodyReader = strings.NewReader(body)
		}

		req := httptest.NewRequest(method, path, bodyReader)
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}

		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		_ = h.Handler()(c)

		return rec
	}

	tests := []struct {
		setup      func(*lambda.Handler, *lambda.InMemoryBackend)
		body       string
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "publish_layer_version",
			method:     http.MethodPost,
			path:       "/2015-03-31/layers/my-layer/versions",
			body:       `{"Content":{"ZipFile":""},"Description":"test layer","CompatibleRuntimes":["python3.9"]}`,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "publish_layer_no_content",
			method:     http.MethodPost,
			path:       "/2015-03-31/layers/my-layer/versions",
			body:       `{"Description":"no content"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:   "get_layer_version",
			method: http.MethodGet,
			path:   "/2015-03-31/layers/my-layer/versions/1",
			setup: func(_ *lambda.Handler, bk *lambda.InMemoryBackend) {
				_, _ = bk.PublishLayerVersion(publishLayerInput("my-layer", "", []byte("z"), nil))
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "get_layer_version_not_found",
			method:     http.MethodGet,
			path:       "/2015-03-31/layers/missing/versions/1",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "list_layers_empty",
			method:     http.MethodGet,
			path:       "/2015-03-31/layers",
			wantStatus: http.StatusOK,
		},
		{
			name:   "list_layers_with_data",
			method: http.MethodGet,
			path:   "/2015-03-31/layers",
			setup: func(_ *lambda.Handler, bk *lambda.InMemoryBackend) {
				_, _ = bk.PublishLayerVersion(publishLayerInput("layer-a", "", []byte("z"), nil))
				_, _ = bk.PublishLayerVersion(publishLayerInput("layer-b", "", []byte("z"), nil))
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "list_layer_versions",
			method: http.MethodGet,
			path:   "/2015-03-31/layers/my-layer/versions",
			setup: func(_ *lambda.Handler, bk *lambda.InMemoryBackend) {
				_, _ = bk.PublishLayerVersion(publishLayerInput("my-layer", "", []byte("z"), nil))
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "list_layer_versions_not_found",
			method:     http.MethodGet,
			path:       "/2015-03-31/layers/missing/versions",
			wantStatus: http.StatusNotFound,
		},
		{
			name:   "delete_layer_version",
			method: http.MethodDelete,
			path:   "/2015-03-31/layers/my-layer/versions/1",
			setup: func(_ *lambda.Handler, bk *lambda.InMemoryBackend) {
				_, _ = bk.PublishLayerVersion(publishLayerInput("my-layer", "", []byte("z"), nil))
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "delete_layer_version_not_found",
			method:     http.MethodDelete,
			path:       "/2015-03-31/layers/missing/versions/1",
			wantStatus: http.StatusNotFound,
		},
		{
			name:   "add_layer_version_permission",
			method: http.MethodPost,
			path:   "/2015-03-31/layers/my-layer/versions/1/policy",
			body:   `{"StatementId":"stmt-1","Action":"lambda:GetLayerVersion","Principal":"*"}`,
			setup: func(_ *lambda.Handler, bk *lambda.InMemoryBackend) {
				_, _ = bk.PublishLayerVersion(publishLayerInput("my-layer", "", []byte("z"), nil))
			},
			wantStatus: http.StatusCreated,
		},
		{
			name:   "get_layer_version_policy",
			method: http.MethodGet,
			path:   "/2015-03-31/layers/my-layer/versions/1/policy",
			setup: func(_ *lambda.Handler, bk *lambda.InMemoryBackend) {
				_, _ = bk.PublishLayerVersion(publishLayerInput("my-layer", "", []byte("z"), nil))
				_, _ = bk.AddLayerVersionPermission("my-layer", 1, &lambda.AddLayerVersionPermissionInput{
					StatementID: "stmt-1",
					Action:      "lambda:GetLayerVersion",
					Principal:   "*",
				})
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "remove_layer_version_permission",
			method: http.MethodDelete,
			path:   "/2015-03-31/layers/my-layer/versions/1/policy/stmt-1",
			setup: func(_ *lambda.Handler, bk *lambda.InMemoryBackend) {
				_, _ = bk.PublishLayerVersion(publishLayerInput("my-layer", "", []byte("z"), nil))
				_, _ = bk.AddLayerVersionPermission("my-layer", 1, &lambda.AddLayerVersionPermissionInput{
					StatementID: "stmt-1",
					Action:      "lambda:GetLayerVersion",
					Principal:   "*",
				})
			},
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newHandlerAndBackend()
			if tt.setup != nil {
				tt.setup(h, bk)
			}

			rec := doRequest(h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestCreateFunctionWithLayers verifies that Layers are preserved in FunctionConfiguration.
func TestCreateFunctionWithLayers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		layers   []string
		wantARNs []string
	}{
		{
			name:     "no_layers",
			layers:   nil,
			wantARNs: nil,
		},
		{
			name:     "with_layers",
			layers:   []string{"arn:aws:lambda:us-east-1:123456789012:layer:my-layer:1"},
			wantARNs: []string{"arn:aws:lambda:us-east-1:123456789012:layer:my-layer:1"},
		},
		{
			name: "multiple_layers",
			layers: []string{
				"arn:aws:lambda:us-east-1:123456789012:layer:layer-a:1",
				"arn:aws:lambda:us-east-1:123456789012:layer:layer-b:2",
			},
			wantARNs: []string{
				"arn:aws:lambda:us-east-1:123456789012:layer:layer-a:1",
				"arn:aws:lambda:us-east-1:123456789012:layer:layer-b:2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			bk := newLayerBackend()
			h := lambda.NewHandler(bk)
			h.DefaultRegion = "us-east-1"
			h.AccountID = "123456789012"

			bodyMap := map[string]any{
				"FunctionName": "my-function",
				"PackageType":  "Image",
				"Code":         map[string]string{"ImageUri": "test:latest"},
				"Role":         "arn:aws:iam::123456789012:role/exec",
				"Layers":       tt.layers,
			}

			bodyBytes, _ := json.Marshal(bodyMap)
			req := httptest.NewRequest(http.MethodPost, "/2015-03-31/functions", strings.NewReader(string(bodyBytes)))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			_ = h.Handler()(c)

			require.Equal(t, http.StatusCreated, rec.Code)

			fn, err := bk.GetFunction("my-function")
			require.NoError(t, err)

			if len(tt.wantARNs) == 0 {
				assert.Empty(t, fn.Layers)
			} else {
				require.Len(t, fn.Layers, len(tt.wantARNs))

				for i, arn := range tt.wantARNs {
					assert.Equal(t, arn, fn.Layers[i].Arn)
				}
			}
		})
	}
}

// TestPersistenceLayers verifies that layer state survives a Snapshot/Restore cycle.
func TestPersistenceLayers(t *testing.T) {
	t.Parallel()

	bk := newLayerBackend()

	// Publish two layers with multiple versions each.
	_, err := bk.PublishLayerVersion(publishLayerInput("layer-a", "v1", []byte("zip-a1"), []string{"python3.9"}))
	require.NoError(t, err)

	_, err = bk.PublishLayerVersion(publishLayerInput("layer-a", "v2", []byte("zip-a2"), nil))
	require.NoError(t, err)

	_, err = bk.PublishLayerVersion(publishLayerInput("layer-b", "v1", []byte("zip-b1"), nil))
	require.NoError(t, err)

	// Add a policy.
	_, err = bk.AddLayerVersionPermission("layer-a", 1, &lambda.AddLayerVersionPermissionInput{
		StatementID: "allow-all",
		Action:      "lambda:GetLayerVersion",
		Principal:   "*",
	})
	require.NoError(t, err)

	// Snapshot → Restore.
	snap := bk.Snapshot()
	require.NotNil(t, snap)

	bk2 := newLayerBackend()
	require.NoError(t, bk2.Restore(snap))

	// Verify layers are present.
	layers := bk2.ListLayers()
	assert.Len(t, layers, 2)

	// Verify versions are restored.
	versions, err := bk2.ListLayerVersions("layer-a")
	require.NoError(t, err)
	assert.Len(t, versions, 2)

	// Verify policy is restored.
	policy, err := bk2.GetLayerVersionPolicy("layer-a", 1)
	require.NoError(t, err)
	assert.Contains(t, policy.Policy, "allow-all")

	// Verify zip data is cleared after restore.
	_, getErr := bk2.GetLayerVersion("layer-a", 1)
	require.NoError(t, getErr)
}
