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

// ---------------------------------------------------------------------------
// CreateStreamProcessor persists and echoes Input/Output/Settings/
// RegionsOfInterest/NotificationChannel/KmsKeyId/DataSharingPreference back
// through DescribeStreamProcessor. These were previously accepted by the
// request but silently dropped (see PARITY.md gaps) -- this locks the fix.
// ---------------------------------------------------------------------------

func TestCreateStreamProcessor_FullFieldsRoundTripThroughDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createBody := map[string]any{
		"Name":     "full-proc",
		"RoleArn":  "arn:aws:iam::000000000000:role/reko",
		"KmsKeyId": "arn:aws:kms:us-east-1:000000000000:key/abc",
		"Input": map[string]any{
			"KinesisVideoStream": map[string]any{"Arn": "arn:aws:kinesisvideo:us-east-1:000000000000:stream/in/1"},
		},
		"Output": map[string]any{
			"KinesisDataStream": map[string]any{"Arn": "arn:aws:kinesis:us-east-1:000000000000:stream/out"},
		},
		"Settings": map[string]any{
			"FaceSearch": map[string]any{
				"CollectionId":       "coll1",
				"FaceMatchThreshold": 90.0,
			},
		},
		"NotificationChannel": map[string]any{
			"SNSTopicArn": "arn:aws:sns:us-east-1:000000000000:topic",
		},
		"DataSharingPreference": map[string]any{"OptIn": true},
		"RegionsOfInterest": []map[string]any{
			{"BoundingBox": map[string]any{"Height": 0.5, "Left": 0.1, "Top": 0.1, "Width": 0.5}},
		},
	}

	rec := doRequest(t, h, "CreateStreamProcessor", createBody)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "DescribeStreamProcessor", map[string]any{"Name": "full-proc"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/abc", resp["KmsKeyId"])

	input, _ := resp["Input"].(map[string]any)
	require.NotNil(t, input)
	kvs, _ := input["KinesisVideoStream"].(map[string]any)
	require.NotNil(t, kvs)
	assert.Equal(t, "arn:aws:kinesisvideo:us-east-1:000000000000:stream/in/1", kvs["Arn"])

	output, _ := resp["Output"].(map[string]any)
	require.NotNil(t, output)
	kds, _ := output["KinesisDataStream"].(map[string]any)
	require.NotNil(t, kds)
	assert.Equal(t, "arn:aws:kinesis:us-east-1:000000000000:stream/out", kds["Arn"])

	settings, _ := resp["Settings"].(map[string]any)
	require.NotNil(t, settings)
	faceSearch, _ := settings["FaceSearch"].(map[string]any)
	require.NotNil(t, faceSearch)
	assert.Equal(t, "coll1", faceSearch["CollectionId"])
	assert.InDelta(t, 90.0, faceSearch["FaceMatchThreshold"], 0.001)

	notif, _ := resp["NotificationChannel"].(map[string]any)
	require.NotNil(t, notif)
	assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:topic", notif["SNSTopicArn"])

	dsp, _ := resp["DataSharingPreference"].(map[string]any)
	require.NotNil(t, dsp)
	assert.Equal(t, true, dsp["OptIn"])

	rois, _ := resp["RegionsOfInterest"].([]any)
	require.Len(t, rois, 1)
	roi, _ := rois[0].(map[string]any)
	bbox, _ := roi["BoundingBox"].(map[string]any)
	require.NotNil(t, bbox)
	assert.InDelta(t, 0.5, bbox["Height"], 0.001)
}

// ---------------------------------------------------------------------------
// UpdateStreamProcessor actually applies SettingsForUpdate/
// RegionsOfInterestForUpdate/DataSharingPreferenceForUpdate/
// ParametersToDelete. Previously UpdateStreamProcessor was a pure
// existence-check no-op (see PARITY.md gaps) -- this locks the fix.
// ---------------------------------------------------------------------------

func TestUpdateStreamProcessor_AppliesFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateStreamProcessor", map[string]any{
		"Name": "upd-full-proc",
		"Settings": map[string]any{
			"ConnectedHome": map[string]any{"Labels": []string{"PERSON"}, "MinConfidence": 50.0},
		},
		"RegionsOfInterest": []map[string]any{
			{"BoundingBox": map[string]any{"Height": 0.1, "Left": 0.1, "Top": 0.1, "Width": 0.1}},
		},
	})

	describe := func() map[string]any {
		rec := doRequest(t, h, "DescribeStreamProcessor", map[string]any{"Name": "upd-full-proc"})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		return resp
	}

	// Sanity: initial state has the region of interest set above.
	initial := describe()
	assert.Len(t, initial["RegionsOfInterest"], 1)

	// Update Labels/MinConfidence and DataSharingPreference.
	rec := doRequest(t, h, "UpdateStreamProcessor", map[string]any{
		"Name": "upd-full-proc",
		"SettingsForUpdate": map[string]any{
			"ConnectedHomeForUpdate": map[string]any{"Labels": []string{"PET", "PACKAGE"}, "MinConfidence": 80.0},
		},
		"DataSharingPreferenceForUpdate": map[string]any{"OptIn": true},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	updated := describe()
	settings, _ := updated["Settings"].(map[string]any)
	require.NotNil(t, settings)
	connectedHome, _ := settings["ConnectedHome"].(map[string]any)
	require.NotNil(t, connectedHome)
	labels, _ := connectedHome["Labels"].([]any)
	assert.ElementsMatch(t, []any{"PET", "PACKAGE"}, labels)
	assert.InDelta(t, 80.0, connectedHome["MinConfidence"], 0.001)

	dsp, _ := updated["DataSharingPreference"].(map[string]any)
	require.NotNil(t, dsp)
	assert.Equal(t, true, dsp["OptIn"])

	// Delete RegionsOfInterest and ConnectedHomeMinConfidence via ParametersToDelete.
	rec = doRequest(t, h, "UpdateStreamProcessor", map[string]any{
		"Name":               "upd-full-proc",
		"ParametersToDelete": []string{"RegionsOfInterest", "ConnectedHomeMinConfidence"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	final := describe()
	assert.Empty(t, final["RegionsOfInterest"])
	settings, _ = final["Settings"].(map[string]any)
	require.NotNil(t, settings)
	connectedHome, _ = settings["ConnectedHome"].(map[string]any)
	require.NotNil(t, connectedHome)
	_, hasMinConfidence := connectedHome["MinConfidence"]
	assert.False(t, hasMinConfidence, "MinConfidence should be cleared")
}

// ---------------------------------------------------------------------------
// UpdateStreamProcessor RegionsOfInterestForUpdate replaces the stored list
// wholesale (not merge/append).
// ---------------------------------------------------------------------------

func TestUpdateStreamProcessor_RegionsOfInterestForUpdate_Replaces(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateStreamProcessor", map[string]any{
		"Name": "roi-proc",
		"RegionsOfInterest": []map[string]any{
			{"BoundingBox": map[string]any{"Height": 0.1, "Left": 0.1, "Top": 0.1, "Width": 0.1}},
		},
	})

	rec := doRequest(t, h, "UpdateStreamProcessor", map[string]any{
		"Name": "roi-proc",
		"RegionsOfInterestForUpdate": []map[string]any{
			{"BoundingBox": map[string]any{"Height": 0.2, "Left": 0.2, "Top": 0.2, "Width": 0.2}},
			{"BoundingBox": map[string]any{"Height": 0.3, "Left": 0.3, "Top": 0.3, "Width": 0.3}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "DescribeStreamProcessor", map[string]any{"Name": "roi-proc"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	rois, _ := resp["RegionsOfInterest"].([]any)
	assert.Len(t, rois, 2)
}
