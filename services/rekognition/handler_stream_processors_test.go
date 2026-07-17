package rekognition_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rekognition"
)

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
				// AWS reports a duplicate stream processor name as
				// ResourceInUseException, not ResourceAlreadyExistsException
				// (verified against aws-sdk-go-v2/service/rekognition's
				// CreateStreamProcessor error deserializer switch).
				assert.Equal(t, "ResourceInUseException", resp["__type"])
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

// ---------------------------------------------------------------------------
// Pagination: ListStreamProcessors
// ---------------------------------------------------------------------------

func TestListStreamProcessors_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create 3 stream processors: proc-a, proc-b, proc-c.
	for _, name := range []string{"proc-a", "proc-b", "proc-c"} {
		rec := doRequest(t, h, "CreateStreamProcessor", map[string]any{"Name": name})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Page 1: MaxResults=2.
	rec := doRequest(t, h, "ListStreamProcessors", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))

	procs1, _ := page1["StreamProcessors"].([]any)
	require.Len(t, procs1, 2)

	nextToken1, _ := page1["NextToken"].(string)
	require.NotEmpty(t, nextToken1)

	// Page 2: remaining processor.
	rec = doRequest(t, h, "ListStreamProcessors", map[string]any{"NextToken": nextToken1})
	require.Equal(t, http.StatusOK, rec.Code)

	var page2 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))

	procs2, _ := page2["StreamProcessors"].([]any)
	require.Len(t, procs2, 1)
	assert.Empty(t, page2["NextToken"])
}

// ---------------------------------------------------------------------------
// ListStreamProcessors empty returns non-null StreamProcessors slice
// ---------------------------------------------------------------------------

func TestListStreamProcessors_EmptyReturnsNonNullSlice(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListStreamProcessors", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	_, hasProcs := raw["StreamProcessors"]
	assert.True(t, hasProcs, "StreamProcessors field must be present")

	var procs []any
	require.NoError(t, json.Unmarshal(raw["StreamProcessors"], &procs))
	assert.NotNil(t, procs, "StreamProcessors must not be null")
}

// ---------------------------------------------------------------------------
// StopStreamProcessor on already-stopped is idempotent
// ---------------------------------------------------------------------------

func TestStopStreamProcessor_AlreadyStopped_IsIdempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateStreamProcessor", map[string]any{"Name": "idem-proc"})

	// Stop a processor that was never started — should succeed without error.
	rec := doRequest(t, h, "StopStreamProcessor", map[string]any{"Name": "idem-proc"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Status should remain STOPPED.
	rec = doRequest(t, h, "DescribeStreamProcessor", map[string]any{"Name": "idem-proc"})
	require.Equal(t, http.StatusOK, rec.Code)

	var desc map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &desc))
	assert.Equal(t, "STOPPED", desc["Status"])
}

// ---------------------------------------------------------------------------
// StartStreamProcessor on unknown returns ResourceNotFoundException
// ---------------------------------------------------------------------------

func TestStartStreamProcessor_Unknown_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "StartStreamProcessor", map[string]any{"Name": "no-such-proc"})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}

// ---------------------------------------------------------------------------
// UpdateStreamProcessor on unknown returns ResourceNotFoundException
// ---------------------------------------------------------------------------

func TestUpdateStreamProcessor_Unknown_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UpdateStreamProcessor", map[string]any{"Name": "no-such-proc"})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}

// ---------------------------------------------------------------------------
// DescribeStreamProcessor reflects start/stop status transitions
// ---------------------------------------------------------------------------

func TestStreamProcessor_StatusTransitions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateStreamProcessor", map[string]any{"Name": "status-proc"})

	getStatus := func() string {
		rec := doRequest(t, h, "DescribeStreamProcessor", map[string]any{"Name": "status-proc"})
		require.Equal(t, http.StatusOK, rec.Code)
		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		return resp["Status"].(string)
	}

	assert.Equal(t, "STOPPED", getStatus())

	doRequest(t, h, "StartStreamProcessor", map[string]any{"Name": "status-proc"})
	assert.Equal(t, "RUNNING", getStatus())

	doRequest(t, h, "StopStreamProcessor", map[string]any{"Name": "status-proc"})
	assert.Equal(t, "STOPPED", getStatus())
}

// ---------------------------------------------------------------------------
// StreamProcessor Name length validation
// ---------------------------------------------------------------------------

func TestCreateStreamProcessor_NameLengthValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		procName string
		wantCode int
	}{
		{
			name:     "empty name rejected",
			procName: "",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "name at limit (128) accepted",
			procName: strings.Repeat("p", 128),
			wantCode: http.StatusOK,
		},
		{
			name:     "name over limit (129) rejected",
			procName: strings.Repeat("p", 129),
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateStreamProcessor", map[string]any{"Name": tc.procName})
			assert.Equal(t, tc.wantCode, rec.Code, tc.name)
		})
	}
}

// ---------------------------------------------------------------------------
// Tag validation on CreateStreamProcessor initial tags
// ---------------------------------------------------------------------------

func TestCreateStreamProcessor_InvalidInitialTags_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "empty tag key rejected",
			tags:     map[string]string{"": "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "tag key too long rejected",
			tags:     map[string]string{strings.Repeat("k", 129): "v"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateStreamProcessor", map[string]any{
				"Name": "proc-tags-test",
				"Tags": tc.tags,
			})
			assert.Equal(t, tc.wantCode, rec.Code, tc.name)
		})
	}
}
