package mediastore_test

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

	"github.com/blackbirdworks/gopherstack/services/mediastore"
)

func newTestHandler(t *testing.T) *mediastore.Handler {
	t.Helper()

	b := mediastore.NewInMemoryBackend()
	h := mediastore.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h
}

func doRequest(t *testing.T, h *mediastore.Handler, op string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()

	var req *http.Request

	if len(bodyBytes) > 0 {
		req = httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	} else {
		req = httptest.NewRequest(http.MethodPost, "/", http.NoBody)
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	}

	req.Header.Set("X-Amz-Target", "MediaStore_20170901."+op)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_CreateContainer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantField  string
		wantStatus int
	}{
		{
			name:       "creates container",
			body:       map[string]any{"ContainerName": "my-container"},
			wantStatus: http.StatusOK,
			wantField:  "Container",
		},
		{
			name:       "missing container name",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "duplicate returns conflict",
			body:       map[string]any{"ContainerName": "dup-container"},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantStatus == http.StatusConflict {
				rec := doRequest(t, h, "CreateContainer", tt.body)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "CreateContainer", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantField != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, tt.wantField)
			}
		})
	}
}

func TestHandler_DeleteContainer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		container  string
		wantStatus int
	}{
		{
			name:       "deletes existing container",
			container:  "to-delete",
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing container returns not found",
			container:  "missing",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "missing container name returns bad request",
			container:  "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantStatus == http.StatusOK {
				rec := doRequest(t, h, "CreateContainer", map[string]any{"ContainerName": tt.container})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "DeleteContainer", map[string]any{"ContainerName": tt.container})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DescribeContainer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		container  string
		wantStatus int
	}{
		{
			name:       "describes existing container",
			container:  "describe-me",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			container:  "missing",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantStatus == http.StatusOK {
				rec := doRequest(t, h, "CreateContainer", map[string]any{"ContainerName": tt.container})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "DescribeContainer", map[string]any{"ContainerName": tt.container})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "Container")
			}
		})
	}
}

func TestHandler_ListContainers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		createN   int
		wantCount int
	}{
		{
			name:      "empty list",
			createN:   0,
			wantCount: 0,
		},
		{
			name:      "lists created containers",
			createN:   2,
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range tt.createN {
				rec := doRequest(t, h, "CreateContainer",
					map[string]any{"ContainerName": fmt.Sprintf("container-%d", i)})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "ListContainers", map[string]any{})

			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			containers, _ := resp["Containers"].([]any)
			assert.Len(t, containers, tt.wantCount)
		})
	}
}

func TestHandler_ContainerPolicy(t *testing.T) {
	t.Parallel()

	const policy = `{"Version":"2012-10-17","Statement":[]}`

	tests := []struct {
		name       string
		op         string
		wantStatus int
		withPolicy bool
		deleted    bool
	}{
		{
			name:       "put container policy",
			op:         "PutContainerPolicy",
			wantStatus: http.StatusOK,
		},
		{
			name:       "get container policy",
			op:         "GetContainerPolicy",
			withPolicy: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete container policy",
			op:         "DeleteContainerPolicy",
			withPolicy: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "get policy after delete returns not found",
			op:         "GetContainerPolicy",
			withPolicy: true,
			deleted:    true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupRec := doRequest(t, h, "CreateContainer", map[string]any{"ContainerName": "policy-test"})
			require.Equal(t, http.StatusOK, setupRec.Code)

			if tt.withPolicy {
				putRec := doRequest(t, h, "PutContainerPolicy",
					map[string]any{"ContainerName": "policy-test", "Policy": policy})
				require.Equal(t, http.StatusOK, putRec.Code)
			}

			if tt.deleted {
				delRec := doRequest(t, h, "DeleteContainerPolicy",
					map[string]any{"ContainerName": "policy-test"})
				require.Equal(t, http.StatusOK, delRec.Code)
			}

			body := map[string]any{"ContainerName": "policy-test"}
			if tt.op == "PutContainerPolicy" {
				body["Policy"] = policy
			}

			result := doRequest(t, h, tt.op, body)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

func TestHandler_CorsPolicy(t *testing.T) {
	t.Parallel()

	corsBody := map[string]any{
		"ContainerName": "cors-test",
		"CorsPolicy": []any{
			map[string]any{
				"AllowedOrigins": []any{"https://example.com"},
				"AllowedHeaders": []any{"*"},
			},
		},
	}

	tests := []struct {
		name       string
		op         string
		wantStatus int
		withPolicy bool
		deleted    bool
	}{
		{
			name:       "put cors policy",
			op:         "PutCorsPolicy",
			wantStatus: http.StatusOK,
		},
		{
			name:       "get cors policy",
			op:         "GetCorsPolicy",
			withPolicy: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete cors policy",
			op:         "DeleteCorsPolicy",
			withPolicy: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "get cors policy after delete returns not found",
			op:         "GetCorsPolicy",
			withPolicy: true,
			deleted:    true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupRec := doRequest(t, h, "CreateContainer", map[string]any{"ContainerName": "cors-test"})
			require.Equal(t, http.StatusOK, setupRec.Code)

			if tt.withPolicy {
				putRec := doRequest(t, h, "PutCorsPolicy", corsBody)
				require.Equal(t, http.StatusOK, putRec.Code)
			}

			if tt.deleted {
				delRec := doRequest(t, h, "DeleteCorsPolicy", map[string]any{"ContainerName": "cors-test"})
				require.Equal(t, http.StatusOK, delRec.Code)
			}

			var body map[string]any
			if tt.op == "PutCorsPolicy" {
				body = corsBody
			} else {
				body = map[string]any{"ContainerName": "cors-test"}
			}

			result := doRequest(t, h, tt.op, body)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

func TestHandler_LifecyclePolicy(t *testing.T) {
	t.Parallel()

	const lcPolicy = `{"rules":[]}`

	tests := []struct {
		name       string
		op         string
		wantStatus int
		withPolicy bool
	}{
		{
			name:       "put lifecycle policy",
			op:         "PutLifecyclePolicy",
			wantStatus: http.StatusOK,
		},
		{
			name:       "get lifecycle policy",
			op:         "GetLifecyclePolicy",
			withPolicy: true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete lifecycle policy",
			op:         "DeleteLifecyclePolicy",
			withPolicy: true,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupRec := doRequest(t, h, "CreateContainer", map[string]any{"ContainerName": "lifecycle-test"})
			require.Equal(t, http.StatusOK, setupRec.Code)

			if tt.withPolicy {
				putRec := doRequest(t, h, "PutLifecyclePolicy",
					map[string]any{"ContainerName": "lifecycle-test", "LifecyclePolicy": lcPolicy})
				require.Equal(t, http.StatusOK, putRec.Code)
			}

			body := map[string]any{"ContainerName": "lifecycle-test"}
			if tt.op == "PutLifecyclePolicy" {
				body["LifecyclePolicy"] = lcPolicy
			}

			result := doRequest(t, h, tt.op, body)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

func TestHandler_AccessLogging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "start access logging",
			op:         "StartAccessLogging",
			wantStatus: http.StatusOK,
		},
		{
			name:       "stop access logging",
			op:         "StopAccessLogging",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupRec := doRequest(t, h, "CreateContainer", map[string]any{"ContainerName": "logging-test"})
			require.Equal(t, http.StatusOK, setupRec.Code)

			result := doRequest(t, h, tt.op, map[string]any{"ContainerName": "logging-test"})
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

func TestHandler_Tags(t *testing.T) {
	t.Parallel()

	type tagOp struct {
		name       string
		op         string
		wantStatus int
		withTag    bool
	}

	tests := []tagOp{
		{
			name:       "tag resource",
			op:         "TagResource",
			wantStatus: http.StatusOK,
		},
		{
			name:       "list tags for resource",
			op:         "ListTagsForResource",
			withTag:    true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "untag resource",
			op:         "UntagResource",
			withTag:    true,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupRec := doRequest(t, h, "CreateContainer", map[string]any{"ContainerName": "tags-test"})
			require.Equal(t, http.StatusOK, setupRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(setupRec.Body.Bytes(), &createResp))
			containerMap, _ := createResp["Container"].(map[string]any)
			containerARN, _ := containerMap["ARN"].(string)

			if tt.withTag {
				tagRec := doRequest(t, h, "TagResource", map[string]any{
					"Resource": containerARN,
					"Tags":     []any{map[string]any{"Key": "env", "Value": "test"}},
				})
				require.Equal(t, http.StatusOK, tagRec.Code)
			}

			var body map[string]any
			switch tt.op {
			case "TagResource":
				body = map[string]any{
					"Resource": containerARN,
					"Tags":     []any{map[string]any{"Key": "env", "Value": "test"}},
				}
			case "UntagResource":
				body = map[string]any{"Resource": containerARN, "TagKeys": []any{"env"}}
			default:
				body = map[string]any{"Resource": containerARN}
			}

			result := doRequest(t, h, tt.op, body)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

func TestHandler_MissingTarget(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	req.Header.Set("X-Amz-Target", "MediaStore_20170901.BogusOp")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
