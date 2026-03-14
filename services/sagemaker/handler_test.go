package sagemaker_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

func newTestHandler(t *testing.T) *sagemaker.Handler {
	t.Helper()

	return sagemaker.NewHandler(sagemaker.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doSageMakerRequest(t *testing.T, h *sagemaker.Handler, target string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "SageMaker."+target)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/sagemaker/aws4_request")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "SageMaker", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateModel")
	assert.Contains(t, ops, "DescribeModel")
	assert.Contains(t, ops, "ListModels")
	assert.Contains(t, ops, "DeleteModel")
	assert.Contains(t, ops, "CreateEndpointConfig")
	assert.Contains(t, ops, "DescribeEndpointConfig")
	assert.Contains(t, ops, "ListEndpointConfigs")
	assert.Contains(t, ops, "DeleteEndpointConfig")
	assert.Contains(t, ops, "AddTags")
	assert.Contains(t, ops, "ListTags")
	assert.Contains(t, ops, "DeleteTags")
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 100, h.MatchPriority())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "sagemaker", h.ChaosServiceName())
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	regions := h.ChaosRegions()
	require.Len(t, regions, 1)
	assert.Equal(t, "us-east-1", regions[0])
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "matches SageMaker.CreateModel",
			target: "SageMaker.CreateModel",
			want:   true,
		},
		{
			name:   "matches SageMaker.DescribeModel",
			target: "SageMaker.DescribeModel",
			want:   true,
		},
		{
			name:   "does not match DynamoDB.PutItem",
			target: "DynamoDB.PutItem",
			want:   false,
		},
		{
			name:   "does not match empty target",
			target: "",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestHandler_ChaosInterface(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, "sagemaker", h.ChaosServiceName())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
}

func TestHandler_CreateModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantARN  bool
	}{
		{
			name: "success",
			body: map[string]any{
				"ModelName":        "my-model",
				"ExecutionRoleArn": "arn:aws:iam::000000000000:role/test",
				"PrimaryContainer": map[string]any{
					"Image": "123456789.dkr.ecr.us-east-1.amazonaws.com/my-image:latest",
				},
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name:     "missing model name",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
			wantARN:  false,
		},
		{
			name:     "invalid json",
			body:     nil,
			wantCode: http.StatusBadRequest,
			wantARN:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var bodyBytes []byte

			if tt.body != nil {
				var err error
				bodyBytes, err = json.Marshal(tt.body)
				require.NoError(t, err)
			} else {
				bodyBytes = []byte("not-json")
			}

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/x-amz-json-1.1")
			req.Header.Set("X-Amz-Target", "SageMaker.CreateModel")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantARN {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["ModelArn"], "arn:aws:sagemaker")
				assert.Contains(t, resp["ModelArn"], "model/my-model")
			}
		})
	}
}

func TestHandler_DescribeModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *sagemaker.Handler)
		body     map[string]any
		name     string
		wantName string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *sagemaker.Handler) {
				t.Helper()

				_, err := h.Backend.CreateModel("my-model", "arn:aws:iam::000000000000:role/test",
					&sagemaker.ContainerDefinition{Image: "my-image"}, nil, nil)
				require.NoError(t, err)
			},
			body:     map[string]any{"ModelName": "my-model"},
			wantCode: http.StatusOK,
			wantName: "my-model",
		},
		{
			name:     "not found",
			body:     map[string]any{"ModelName": "nonexistent"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing name",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doSageMakerRequest(t, h, "DescribeModel", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantName != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantName, resp["ModelName"])
			}
		})
	}
}

func TestHandler_ListModels(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, err := h.Backend.CreateModel("model-a", "arn:aws:iam::000000000000:role/test", nil, nil, nil)
	require.NoError(t, err)

	_, err = h.Backend.CreateModel("model-b", "arn:aws:iam::000000000000:role/test", nil, nil, nil)
	require.NoError(t, err)

	rec := doSageMakerRequest(t, h, "ListModels", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	models, ok := resp["Models"].([]any)
	require.True(t, ok)
	assert.Len(t, models, 2)
}

func TestHandler_DeleteModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *sagemaker.Handler)
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *sagemaker.Handler) {
				t.Helper()

				_, err := h.Backend.CreateModel("to-delete", "arn:aws:iam::000000000000:role/test", nil, nil, nil)
				require.NoError(t, err)
			},
			body:     map[string]any{"ModelName": "to-delete"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not found",
			body:     map[string]any{"ModelName": "nonexistent"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing name",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doSageMakerRequest(t, h, "DeleteModel", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_CreateEndpointConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantARN  bool
	}{
		{
			name: "success",
			body: map[string]any{
				"EndpointConfigName": "my-config",
				"ProductionVariants": []map[string]any{
					{
						"VariantName":          "AllTraffic",
						"ModelName":            "my-model",
						"InstanceType":         "ml.t2.medium",
						"InitialInstanceCount": 1,
					},
				},
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name:     "missing config name",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, "CreateEndpointConfig", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantARN {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["EndpointConfigArn"], "arn:aws:sagemaker")
			}
		})
	}
}

func TestHandler_DescribeEndpointConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *sagemaker.Handler)
		body     map[string]any
		name     string
		wantName string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *sagemaker.Handler) {
				t.Helper()

				_, err := h.Backend.CreateEndpointConfig("my-config", nil, nil)
				require.NoError(t, err)
			},
			body:     map[string]any{"EndpointConfigName": "my-config"},
			wantCode: http.StatusOK,
			wantName: "my-config",
		},
		{
			name:     "not found",
			body:     map[string]any{"EndpointConfigName": "nonexistent"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doSageMakerRequest(t, h, "DescribeEndpointConfig", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantName != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantName, resp["EndpointConfigName"])
			}
		})
	}
}

func TestHandler_DeleteEndpointConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *sagemaker.Handler)
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *sagemaker.Handler) {
				t.Helper()

				_, err := h.Backend.CreateEndpointConfig("to-delete", nil, nil)
				require.NoError(t, err)
			},
			body:     map[string]any{"EndpointConfigName": "to-delete"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not found",
			body:     map[string]any{"EndpointConfigName": "nonexistent"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doSageMakerRequest(t, h, "DeleteEndpointConfig", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_Tags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	m, err := h.Backend.CreateModel("tagged-model", "arn:aws:iam::000000000000:role/test", nil, nil, nil)
	require.NoError(t, err)

	// Add tags.
	rec := doSageMakerRequest(t, h, "AddTags", map[string]any{
		"ResourceArn": m.ModelARN,
		"Tags":        []map[string]string{{"Key": "Env", "Value": "test"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// List tags.
	rec = doSageMakerRequest(t, h, "ListTags", map[string]any{
		"ResourceArn": m.ModelARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tags, ok := resp["Tags"].([]any)
	require.True(t, ok)
	require.Len(t, tags, 1)

	// Delete tags.
	rec = doSageMakerRequest(t, h, "DeleteTags", map[string]any{
		"ResourceArn": m.ModelARN,
		"TagKeys":     []string{"Env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify deleted.
	rec = doSageMakerRequest(t, h, "ListTags", map[string]any{
		"ResourceArn": m.ModelARN,
	})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tags, ok = resp["Tags"].([]any)
	require.True(t, ok)
	assert.Empty(t, tags)
}

func TestHandler_Tags_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		target string
	}{
		{
			name:   "add tags to nonexistent resource",
			target: "AddTags",
			body: map[string]any{
				"ResourceArn": "arn:aws:sagemaker:us-east-1:000000000000:model/nonexistent",
				"Tags":        []map[string]string{{"Key": "k", "Value": "v"}},
			},
		},
		{
			name:   "list tags for nonexistent resource",
			target: "ListTags",
			body:   map[string]any{"ResourceArn": "arn:aws:sagemaker:us-east-1:000000000000:model/nonexistent"},
		},
		{
			name:   "delete tags from nonexistent resource",
			target: "DeleteTags",
			body: map[string]any{
				"ResourceArn": "arn:aws:sagemaker:us-east-1:000000000000:model/nonexistent",
				"TagKeys":     []string{"k"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, tt.target, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doSageMakerRequest(t, h, "UnknownOp", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &sagemaker.Provider{}

	assert.Equal(t, "SageMaker", p.Name())

	backend := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
	h := sagemaker.NewHandler(backend)

	assert.NotNil(t, h)
	assert.Equal(t, "SageMaker", h.Name())
	assert.Equal(t, "us-east-1", backend.Region())
}

func TestHandler_ListEndpointConfigs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *sagemaker.Handler)
		name       string
		wantCode   int
		wantLength int
	}{
		{
			name:       "empty list",
			wantCode:   http.StatusOK,
			wantLength: 0,
		},
		{
			name: "returns all configs",
			setup: func(t *testing.T, h *sagemaker.Handler) {
				t.Helper()

				_, err := h.Backend.CreateEndpointConfig("config-a", nil, nil)
				require.NoError(t, err)

				_, err = h.Backend.CreateEndpointConfig("config-b", nil, nil)
				require.NoError(t, err)
			},
			wantCode:   http.StatusOK,
			wantLength: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doSageMakerRequest(t, h, "ListEndpointConfigs", map[string]any{})
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			configs, ok := resp["EndpointConfigs"].([]any)
			require.True(t, ok)
			assert.Len(t, configs, tt.wantLength)
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{
			name:   "extract CreateModel operation",
			target: "SageMaker.CreateModel",
			want:   "CreateModel",
		},
		{
			name:   "extract DescribeEndpointConfig operation",
			target: "SageMaker.DescribeEndpointConfig",
			want:   "DescribeEndpointConfig",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())

			resource := h.ExtractResource(c)
			assert.Equal(t, tt.want, resource)
		})
	}
}

func TestHandler_Tags_EndpointConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	ec, err := h.Backend.CreateEndpointConfig("tagged-config", nil, nil)
	require.NoError(t, err)

	// Add tags to endpoint config.
	rec := doSageMakerRequest(t, h, "AddTags", map[string]any{
		"ResourceArn": ec.EndpointConfigARN,
		"Tags":        []map[string]string{{"Key": "Env", "Value": "test"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// List tags for endpoint config.
	rec = doSageMakerRequest(t, h, "ListTags", map[string]any{
		"ResourceArn": ec.EndpointConfigARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tags, ok := resp["Tags"].([]any)
	require.True(t, ok)
	require.Len(t, tags, 1)

	// Delete tags from endpoint config.
	rec = doSageMakerRequest(t, h, "DeleteTags", map[string]any{
		"ResourceArn": ec.EndpointConfigARN,
		"TagKeys":     []string{"Env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestProvider_InitFull(t *testing.T) {
	t.Parallel()

	ctx := &service.AppContext{}
	p := &sagemaker.Provider{}
	reg, err := p.Init(ctx)

	require.NoError(t, err)
	require.NotNil(t, reg)
	assert.Equal(t, "SageMaker", reg.Name())
}

func TestHandler_ListModelsPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		count         int
		wantNextToken bool
	}{
		{
			name:          "single_page",
			count:         5,
			wantNextToken: false,
		},
		{
			name:          "multi_page",
			count:         105, // exceeds sagemakerDefaultPageSize=100
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range tt.count {
				_, err := h.Backend.CreateModel(
					fmt.Sprintf("model-%04d", i),
					"arn:aws:iam::000000000000:role/test",
					nil, nil, nil,
				)
				require.NoError(t, err)
			}

			// First page.
			rec := doSageMakerRequest(t, h, "ListModels", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			models, modelsOK := resp["Models"].([]any)
			require.True(t, modelsOK)

			if tt.wantNextToken {
				assert.Len(t, models, 100)
				nextToken, tokenOK := resp["NextToken"].(string)
				require.True(t, tokenOK, "NextToken should be present")
				assert.NotEmpty(t, nextToken)

				// Second page using the token.
				rec2 := doSageMakerRequest(t, h, "ListModels", map[string]any{"NextToken": nextToken})
				assert.Equal(t, http.StatusOK, rec2.Code)

				var resp2 map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

				models2, models2OK := resp2["Models"].([]any)
				require.True(t, models2OK)
				assert.Len(t, models2, tt.count-100)
				assert.Empty(t, resp2["NextToken"])
			} else {
				assert.Len(t, models, tt.count)
				assert.Empty(t, resp["NextToken"])
			}
		})
	}
}

func TestHandler_ListEndpointConfigsPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		count         int
		wantNextToken bool
	}{
		{
			name:          "single_page",
			count:         3,
			wantNextToken: false,
		},
		{
			name:          "multi_page",
			count:         105, // exceeds sagemakerDefaultPageSize=100
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range tt.count {
				_, err := h.Backend.CreateEndpointConfig(
					fmt.Sprintf("cfg-%04d", i),
					nil,
					nil,
				)
				require.NoError(t, err)
			}

			rec := doSageMakerRequest(t, h, "ListEndpointConfigs", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			configs, configsOK := resp["EndpointConfigs"].([]any)
			require.True(t, configsOK)

			if tt.wantNextToken {
				assert.Len(t, configs, 100)
				nextToken, tokenOK := resp["NextToken"].(string)
				require.True(t, tokenOK, "NextToken should be present")
				assert.NotEmpty(t, nextToken)

				// Second page.
				rec2 := doSageMakerRequest(t, h, "ListEndpointConfigs", map[string]any{"NextToken": nextToken})
				assert.Equal(t, http.StatusOK, rec2.Code)

				var resp2 map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

				configs2, configs2OK := resp2["EndpointConfigs"].([]any)
				require.True(t, configs2OK)
				assert.Len(t, configs2, tt.count-100)
				assert.Empty(t, resp2["NextToken"])
			} else {
				assert.Len(t, configs, tt.count)
				assert.Empty(t, resp["NextToken"])
			}
		})
	}
}
