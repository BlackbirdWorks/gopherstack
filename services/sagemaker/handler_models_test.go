package sagemaker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

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

				_, err := h.Backend.CreateModel(context.Background(), "my-model", "arn:aws:iam::000000000000:role/test",
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

	_, err := h.Backend.CreateModel(
		context.Background(),
		"model-a",
		"arn:aws:iam::000000000000:role/test",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	_, err = h.Backend.CreateModel(
		context.Background(),
		"model-b",
		"arn:aws:iam::000000000000:role/test",
		nil,
		nil,
		nil,
	)
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

				_, err := h.Backend.CreateModel(context.Background(),
					"to-delete",
					"arn:aws:iam::000000000000:role/test",
					nil,
					nil,
					nil,
				)
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
				_, err := h.Backend.CreateModel(context.Background(),
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
				rec2 := doSageMakerRequest(
					t,
					h,
					"ListModels",
					map[string]any{"NextToken": nextToken},
				)
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

// TestCreateModel_ExecutionRoleArnOptional verifies that CreateModel accepts a
// request with no ExecutionRoleArn. CreateModelInput
// (api_op_CreateModel.go:50-90, sagemaker@v1.263.2) marks only ModelName "This
// member is required" — ExecutionRoleArn carries no such tag. A prior version
// of this test asserted the opposite (rejecting a missing/empty
// ExecutionRoleArn as a 400), which was itself wrong: it rejected valid real
// requests this backend should accept. ModelName absent is still rejected,
// since that member is genuinely required.
func TestCreateModel_ExecutionRoleArnOptional(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "absent_role_arn_accepted",
			body: map[string]any{
				"ModelName": "my-model",
				"PrimaryContainer": map[string]any{
					"Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest",
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "empty_role_arn_accepted",
			body: map[string]any{
				"ModelName":        "my-model",
				"ExecutionRoleArn": "",
				"PrimaryContainer": map[string]any{
					"Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest",
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "valid_role_arn_accepted",
			body: map[string]any{
				"ModelName":        "my-model",
				"ExecutionRoleArn": "arn:aws:iam::123456789012:role/SageMakerRole",
				"PrimaryContainer": map[string]any{
					"Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest",
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "absent_model_name_rejected",
			body: map[string]any{
				"ExecutionRoleArn": "arn:aws:iam::123456789012:role/SageMakerRole",
				"PrimaryContainer": map[string]any{
					"Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest",
				},
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, "CreateModel", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"CreateModel status for case %q", tt.name)
		})
	}
}

// TestCreateModel_PrimaryContainerAndContainersAreMutuallyExclusive verifies that
// providing both PrimaryContainer and Containers returns a 400. Real AWS rejects this
// combination with a ValidationException.
func TestCreateModel_PrimaryContainerAndContainersAreMutuallyExclusive(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doSageMakerRequest(t, h, "CreateModel", map[string]any{
		"ModelName":        "dual-container-model",
		"ExecutionRoleArn": "arn:aws:iam::123456789012:role/SageMakerRole",
		"PrimaryContainer": map[string]any{
			"Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:v1",
		},
		"Containers": []map[string]any{
			{"Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:v2"},
		},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"CreateModel with both PrimaryContainer and Containers must return 400; body: %s",
		rec.Body.String())
}

// TestHandler_ListModels_FilterSort verifies ListModelsInput's
// NameContains/SortBy/SortOrder (api_op_ListModels.go, previously entirely
// dropped except NextToken) are honored.
func TestHandler_ListModels_FilterSort(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"alpha-model", "beta-model", "other"} {
		doSageMakerRequest(t, h, "CreateModel", map[string]any{
			"ModelName": name,
			"PrimaryContainer": map[string]any{
				"Image": "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest",
			},
		})
	}

	rec := doSageMakerRequest(t, h, "ListModels", map[string]any{
		"NameContains": "-model",
		"SortBy":       "Name",
		"SortOrder":    "Ascending",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	list, ok := resp["Models"].([]any)
	require.True(t, ok)
	require.Len(t, list, 2)

	first, _ := list[0].(map[string]any)
	second, _ := list[1].(map[string]any)
	assert.Equal(t, "alpha-model", first["ModelName"])
	assert.Equal(t, "beta-model", second["ModelName"])
}

func TestDeleteModel_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "delete non-existent model returns 400",
			body:     map[string]any{"ModelName": "does-not-exist"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, "DeleteModel", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
