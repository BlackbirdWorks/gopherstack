package sagemaker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

// ---------------------------------------------------------------------------
// Algorithm — DescribeAlgorithm / DeleteAlgorithm / ListAlgorithms
// ---------------------------------------------------------------------------

func TestHandler_DescribeAlgorithm(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreateAlgorithm", map[string]any{
		"AlgorithmName":        "my-algo",
		"AlgorithmDescription": "a description",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	rec := doSageMakerRequest(t, h, "DescribeAlgorithm", map[string]any{"AlgorithmName": "my-algo"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "my-algo", resp["AlgorithmName"])
	assert.Equal(t, "a description", resp["AlgorithmDescription"])
	assert.Equal(t, "Completed", resp["AlgorithmStatus"])
	assert.Contains(t, resp["AlgorithmArn"], "algorithm/my-algo")
	assert.NotNil(t, resp["AlgorithmStatusDetails"])
	assert.NotNil(t, resp["CreationTime"])
}

func TestHandler_DescribeAlgorithm_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeAlgorithm", map[string]any{"AlgorithmName": "missing"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DescribeAlgorithm_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeAlgorithm", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DeleteAlgorithm(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAlgorithm", map[string]any{"AlgorithmName": "algo-del"})

	rec := doSageMakerRequest(t, h, "DeleteAlgorithm", map[string]any{"AlgorithmName": "algo-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeAlgorithm", map[string]any{"AlgorithmName": "algo-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DeleteAlgorithm_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DeleteAlgorithm", map[string]any{"AlgorithmName": "missing"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListAlgorithms(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListAlgorithms", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["AlgorithmSummaryList"])

	doSageMakerRequest(t, h, "CreateAlgorithm", map[string]any{"AlgorithmName": "algo-a"})
	doSageMakerRequest(t, h, "CreateAlgorithm", map[string]any{"AlgorithmName": "algo-b"})

	rec = doSageMakerRequest(t, h, "ListAlgorithms", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	list, ok := resp["AlgorithmSummaryList"].([]any)
	require.True(t, ok)
	assert.Len(t, list, 2)
}

func TestAddAlgorithmInternal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		algoName  string
		wantCount int
	}{
		{
			name:      "creates algorithm with correct fields",
			algoName:  "my-algo",
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
			al := b.AddAlgorithmInternal(context.Background(), tt.algoName)

			require.NotNil(t, al)
			assert.Equal(t, tt.algoName, al.AlgorithmName)
			assert.Contains(t, al.AlgorithmArn, "arn:aws:sagemaker")
			assert.Equal(t, tt.wantCount, sagemaker.AlgorithmCount(b))
		})
	}
}

func TestCreateAlgorithm_TagsPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantTags bool
	}{
		{
			name: "create algorithm with tags",
			body: map[string]any{
				"AlgorithmName": "my-algo",
				"Tags":          []map[string]string{{"Key": "env", "Value": "dev"}},
			},
			wantCode: http.StatusOK,
			wantTags: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, "CreateAlgorithm", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantTags {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				arnStr := resp["AlgorithmArn"]
				require.NotEmpty(t, arnStr)

				rec2 := doSageMakerRequest(t, h, "ListTags", map[string]any{"ResourceArn": arnStr})
				require.Equal(t, http.StatusOK, rec2.Code)
			}
		})
	}
}

func TestHandler_CreateAlgorithm(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantARN  bool
	}{
		{
			name: "success with description and tags",
			body: map[string]any{
				"AlgorithmName":        "my-algorithm",
				"AlgorithmDescription": "a great algorithm",
				"Tags":                 []map[string]string{{"Key": "owner", "Value": "team-a"}},
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name: "success minimal",
			body: map[string]any{
				"AlgorithmName": "minimal-algo",
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name:     "missing AlgorithmName",
			body:     map[string]any{"AlgorithmDescription": "desc"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid json",
			body:     nil,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var body map[string]any
			if tt.body != nil {
				body = tt.body
			}

			rec := doSageMakerRequest(t, h, "CreateAlgorithm", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantARN {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["AlgorithmArn"], "arn:aws:sagemaker")
				assert.Contains(t, resp["AlgorithmArn"], "algorithm/")
			}
		})
	}
}

func TestHandler_CreateAlgorithm_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"AlgorithmName": "dup-algo",
	}

	rec := doSageMakerRequest(t, h, "CreateAlgorithm", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSageMakerRequest(t, h, "CreateAlgorithm", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}
