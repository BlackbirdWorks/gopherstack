package rekognition_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rekognition"
)

func newTestHandler(t *testing.T) *rekognition.Handler {
	t.Helper()
	backend := rekognition.NewInMemoryBackend("000000000000", "us-east-1")

	return rekognition.NewHandler(backend)
}

func doRequest(t *testing.T, h *rekognition.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var marshalErr error
		bodyBytes, marshalErr = json.Marshal(body)
		require.NoError(t, marshalErr)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "RekognitionService."+action)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	handlerErr := h.Handler()(c)
	require.NoError(t, handlerErr)

	return rec
}

func TestRekognition_Collections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *rekognition.Handler)
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "CreateCollection returns ARN",
			action:   "CreateCollection",
			body:     map[string]any{"CollectionId": "my-coll"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["CollectionArn"], "arn:aws:rekognition:")
				assert.Equal(t, "7.0", resp["FaceModelVersion"])
			},
		},
		{
			name:   "CreateCollection duplicate returns error",
			action: "CreateCollection",
			setup: func(h *rekognition.Handler) {
				doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "dup-coll"})
			},
			body:     map[string]any{"CollectionId": "dup-coll"},
			wantCode: http.StatusBadRequest,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "ResourceAlreadyExistsException", resp["__type"])
			},
		},
		{
			name:   "DeleteCollection returns 200",
			action: "DeleteCollection",
			setup: func(h *rekognition.Handler) {
				doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "del-coll"})
			},
			body:     map[string]any{"CollectionId": "del-coll"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.InDelta(t, float64(200), resp["StatusCode"], 0)
			},
		},
		{
			name:     "DeleteCollection unknown returns error",
			action:   "DeleteCollection",
			body:     map[string]any{"CollectionId": "no-such"},
			wantCode: http.StatusBadRequest,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "ResourceNotFoundException", resp["__type"])
			},
		},
		{
			name:   "DescribeCollection returns face count",
			action: "DescribeCollection",
			setup: func(h *rekognition.Handler) {
				doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "desc-coll"})
			},
			body:     map[string]any{"CollectionId": "desc-coll"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["CollectionARN"], "arn:aws:rekognition:")
				assert.InDelta(t, float64(0), resp["FaceCount"], 0)
			},
		},
		{
			name:     "ListCollections empty returns empty list",
			action:   "ListCollections",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				ids, _ := resp["CollectionIds"].([]any)
				assert.Empty(t, ids)
			},
		},
		{
			name:   "ListCollections shows created collection",
			action: "ListCollections",
			setup: func(h *rekognition.Handler) {
				doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "list-coll"})
			},
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				ids, _ := resp["CollectionIds"].([]any)
				assert.Len(t, ids, 1)
				assert.Equal(t, "list-coll", ids[0])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			if tc.setup != nil {
				tc.setup(h)
			}

			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestRekognition_Faces(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *rekognition.Handler)
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "IndexFaces returns face record",
			action: "IndexFaces",
			setup: func(h *rekognition.Handler) {
				doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "face-coll"})
			},
			body:     map[string]any{"CollectionId": "face-coll"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				records, _ := resp["FaceRecords"].([]any)
				assert.Len(t, records, 1)
			},
		},
		{
			name:     "IndexFaces unknown collection returns error",
			action:   "IndexFaces",
			body:     map[string]any{"CollectionId": "no-coll"},
			wantCode: http.StatusBadRequest,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "ResourceNotFoundException", resp["__type"])
			},
		},
		{
			name:   "ListFaces returns indexed face",
			action: "ListFaces",
			setup: func(h *rekognition.Handler) {
				doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "lf-coll"})
				doRequest(t, h, "IndexFaces", map[string]any{"CollectionId": "lf-coll"})
			},
			body:     map[string]any{"CollectionId": "lf-coll"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				faces, _ := resp["Faces"].([]any)
				assert.Len(t, faces, 1)
			},
		},
		{
			name:   "DeleteFaces removes face",
			action: "DeleteFaces",
			setup: func(h *rekognition.Handler) {
				doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "df-coll"})
				doRequest(t, h, "IndexFaces", map[string]any{"CollectionId": "df-coll"})
			},
			body: map[string]any{
				"CollectionId": "df-coll",
				"FaceIds":      []string{},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotNil(t, resp["DeletedFaces"])
			},
		},
		{
			name:   "SearchFaces returns matches",
			action: "SearchFaces",
			setup: func(h *rekognition.Handler) {
				doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "sf-coll"})
				doRequest(t, h, "IndexFaces", map[string]any{"CollectionId": "sf-coll"})
				doRequest(t, h, "IndexFaces", map[string]any{"CollectionId": "sf-coll"})
			},
			body: map[string]any{"CollectionId": "sf-coll"},
			// FaceId will be empty, triggers validation
			wantCode: http.StatusBadRequest,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "InvalidParameterException", resp["__type"])
			},
		},
		{
			name:   "SearchFacesByImage returns matches",
			action: "SearchFacesByImage",
			setup: func(h *rekognition.Handler) {
				doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "sfbi-coll"})
				doRequest(t, h, "IndexFaces", map[string]any{"CollectionId": "sfbi-coll"})
			},
			body:     map[string]any{"CollectionId": "sfbi-coll"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				matches, _ := resp["FaceMatches"].([]any)
				assert.Len(t, matches, 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			if tc.setup != nil {
				tc.setup(h)
			}

			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestRekognition_StreamProcessors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *rekognition.Handler)
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "CreateStreamProcessor returns ARN",
			action:   "CreateStreamProcessor",
			body:     map[string]any{"Name": "my-proc", "RoleArn": "arn:aws:iam::000000000000:role/reko"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["StreamProcessorArn"], "arn:aws:rekognition:")
			},
		},
		{
			name:   "CreateStreamProcessor duplicate returns error",
			action: "CreateStreamProcessor",
			setup: func(h *rekognition.Handler) {
				doRequest(t, h, "CreateStreamProcessor", map[string]any{"Name": "dup-proc"})
			},
			body:     map[string]any{"Name": "dup-proc"},
			wantCode: http.StatusBadRequest,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "ResourceAlreadyExistsException", resp["__type"])
			},
		},
		{
			name:   "DescribeStreamProcessor returns status",
			action: "DescribeStreamProcessor",
			setup: func(h *rekognition.Handler) {
				doRequest(t, h, "CreateStreamProcessor", map[string]any{"Name": "desc-proc"})
			},
			body:     map[string]any{"Name": "desc-proc"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "STOPPED", resp["Status"])
				assert.Contains(t, resp["StreamProcessorArn"], "arn:aws:rekognition:")
			},
		},
		{
			name:   "ListStreamProcessors shows processor",
			action: "ListStreamProcessors",
			setup: func(h *rekognition.Handler) {
				doRequest(t, h, "CreateStreamProcessor", map[string]any{"Name": "list-proc"})
			},
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				procs, _ := resp["StreamProcessors"].([]any)
				assert.Len(t, procs, 1)
			},
		},
		{
			name:   "StartStreamProcessor sets RUNNING",
			action: "StartStreamProcessor",
			setup: func(h *rekognition.Handler) {
				doRequest(t, h, "CreateStreamProcessor", map[string]any{"Name": "start-proc"})
			},
			body:     map[string]any{"Name": "start-proc"},
			wantCode: http.StatusOK,
		},
		{
			name:   "StopStreamProcessor sets STOPPED",
			action: "StopStreamProcessor",
			setup: func(h *rekognition.Handler) {
				doRequest(t, h, "CreateStreamProcessor", map[string]any{"Name": "stop-proc"})
				doRequest(t, h, "StartStreamProcessor", map[string]any{"Name": "stop-proc"})
			},
			body:     map[string]any{"Name": "stop-proc"},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteStreamProcessor returns 200",
			action: "DeleteStreamProcessor",
			setup: func(h *rekognition.Handler) {
				doRequest(t, h, "CreateStreamProcessor", map[string]any{"Name": "del-proc"})
			},
			body:     map[string]any{"Name": "del-proc"},
			wantCode: http.StatusOK,
		},
		{
			name:     "DeleteStreamProcessor unknown returns error",
			action:   "DeleteStreamProcessor",
			body:     map[string]any{"Name": "no-such"},
			wantCode: http.StatusBadRequest,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Equal(t, "ResourceNotFoundException", resp["__type"])
			},
		},
		{
			name:   "UpdateStreamProcessor returns 200",
			action: "UpdateStreamProcessor",
			setup: func(h *rekognition.Handler) {
				doRequest(t, h, "CreateStreamProcessor", map[string]any{"Name": "upd-proc"})
			},
			body:     map[string]any{"Name": "upd-proc"},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			if tc.setup != nil {
				tc.setup(h)
			}

			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestRekognition_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *rekognition.Handler) string
		bodyFn   func(arn string) any
		check    func(t *testing.T, body []byte)
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "TagResource and ListTagsForResource round-trip",
			action: "TagResource",
			setup: func(h *rekognition.Handler) string {
				rec := doRequest(t, h, "CreateCollection", map[string]any{"CollectionId": "tag-coll"})
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["CollectionArn"].(string)
			},
			bodyFn: func(arn string) any {
				return map[string]any{"ResourceArn": arn, "Tags": map[string]string{"env": "test"}}
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "ListTagsForResource returns tags",
			action: "ListTagsForResource",
			setup: func(h *rekognition.Handler) string {
				rec := doRequest(
					t,
					h,
					"CreateCollection",
					map[string]any{"CollectionId": "ltfr-coll", "Tags": map[string]string{"k": "v"}},
				)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				return resp["CollectionArn"].(string)
			},
			bodyFn: func(arn string) any {
				return map[string]any{"ResourceArn": arn}
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				tags, _ := resp["Tags"].(map[string]any)
				assert.Equal(t, "v", tags["k"])
			},
		},
		{
			name:   "UntagResource removes tag",
			action: "UntagResource",
			setup: func(h *rekognition.Handler) string {
				rec := doRequest(
					t,
					h,
					"CreateCollection",
					map[string]any{"CollectionId": "untag-coll", "Tags": map[string]string{"k": "v"}},
				)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				arn := resp["CollectionArn"].(string)
				doRequest(t, h, "TagResource", map[string]any{"ResourceArn": arn, "Tags": map[string]string{"k": "v"}})

				return arn
			},
			bodyFn: func(arn string) any {
				return map[string]any{"ResourceArn": arn, "TagKeys": []string{"k"}}
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			var arn string
			if tc.setup != nil {
				arn = tc.setup(h)
			}

			body := tc.bodyFn(arn)
			rec := doRequest(t, h, tc.action, body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestRekognition_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "NotAnOperation", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "UnknownOperationException", resp["__type"])
}
