package sagemakerruntime_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemakerruntime"
)

// TestParity_AsyncInvocationOutputLocation verifies that a caller-supplied
// X-Amzn-Sagemaker-Outputlocation request header is used verbatim in the
// response header and in the stored async invocation record.
func TestParity_AsyncInvocationOutputLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		headers            map[string]string
		wantOutputLocation string
		containsInferID    bool
	}{
		{
			name:            "generated_location_when_not_supplied",
			headers:         nil,
			containsInferID: true,
		},
		{
			name: "caller_supplied_location_used_verbatim",
			headers: map[string]string{
				"X-Amzn-Sagemaker-Outputlocation": "s3://my-bucket/results/",
			},
			wantOutputLocation: "s3://my-bucket/results/",
		},
		{
			name: "caller_supplied_location_with_explicit_inference_id",
			headers: map[string]string{
				"X-Amzn-Sagemaker-Outputlocation": "s3://my-bucket/out/",
				"X-Amzn-Sagemaker-Inference-Id":   "my-infer-42",
			},
			wantOutputLocation: "s3://my-bucket/out/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequestWithHeaders(
				t, h, http.MethodPost,
				"/endpoints/my-endpoint/async-invocations",
				map[string]any{"data": "payload"},
				tt.headers,
			)

			require.Equal(t, http.StatusAccepted, rec.Code)

			respLoc := rec.Header().Get("X-Amzn-Sagemaker-Outputlocation")
			require.NotEmpty(t, respLoc)

			async := h.Backend.ListAsyncInvocations()
			require.Len(t, async, 1)

			if tt.wantOutputLocation != "" {
				assert.Equal(t, tt.wantOutputLocation, respLoc,
					"response header must echo caller-supplied output location")
				assert.Equal(t, tt.wantOutputLocation, async[0].OutputLocation,
					"stored output location must equal caller-supplied value")
			}

			if tt.containsInferID {
				assert.Contains(t, respLoc, async[0].InferenceID,
					"generated location must embed the inference ID")
				assert.Equal(t, respLoc, async[0].OutputLocation,
					"response header and stored location must agree")
			}
		})
	}
}

// TestParity_SessionLifecycle verifies NEW_SESSION creation and subsequent
// session-touch behaviour match AWS semantics.
func TestParity_SessionLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment: readability over micro-optimization
		name         string
		headers      map[string]string
		wantSessions int
		wantNewSessH bool
	}{
		{
			name:         "no_session_header_creates_nothing",
			headers:      nil,
			wantSessions: 0,
			wantNewSessH: false,
		},
		{
			name: "new_session_header_creates_session",
			headers: map[string]string{
				"X-Amzn-Sagemaker-Session-Id": "NEW_SESSION",
			},
			wantSessions: 1,
			wantNewSessH: true,
		},
		{
			name: "existing_session_id_touches_without_creating",
			headers: map[string]string{
				"X-Amzn-Sagemaker-Session-Id": "some-pre-existing-id",
			},
			wantSessions: 0,
			wantNewSessH: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequestWithHeaders(
				t, h, http.MethodPost,
				"/endpoints/ep/invocations",
				nil,
				tt.headers,
			)

			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, tt.wantNewSessH, rec.Header().Get("X-Amzn-Sagemaker-New-Session-Id") != "")
			assert.Len(t, h.Backend.ListSessions(), tt.wantSessions)
		})
	}
}

// TestParity_NewSessionHeaderFormat checks that the new-session response header
// contains both the session ID and an Expires attribute.
func TestParity_NewSessionHeaderFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequestWithHeaders(
		t, h, http.MethodPost,
		"/endpoints/ep/invocations",
		nil,
		map[string]string{"X-Amzn-Sagemaker-Session-Id": "NEW_SESSION"},
	)

	require.Equal(t, http.StatusOK, rec.Code)

	hdr := rec.Header().Get("X-Amzn-Sagemaker-New-Session-Id")
	require.NotEmpty(t, hdr)
	assert.Contains(t, hdr, "Expires=", "new session header must contain Expires attribute")
}

// TestParity_CustomAttributesForwarding verifies that X-Amzn-Sagemaker-Custom-Attributes
// is reflected back on all three operation types.
func TestParity_CustomAttributesForwarding(t *testing.T) {
	t.Parallel()

	const attrValue = "trace=abc;env=test"

	tests := []struct {
		name string
		path string
	}{
		{
			name: "invoke_endpoint",
			path: "/endpoints/ep/invocations",
		},
		{
			name: "invoke_endpoint_with_response_stream",
			path: "/endpoints/ep/invocations-response-stream",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequestWithHeaders(
				t, h, http.MethodPost, tt.path, nil,
				map[string]string{
					"X-Amzn-Sagemaker-Custom-Attributes": attrValue,
				},
			)

			assert.Equal(t, attrValue, rec.Header().Get("X-Amzn-Sagemaker-Custom-Attributes"))
		})
	}
}

// TestParity_TargetVariantForwarding verifies that the X-Amzn-Invoked-Production-Variant
// response header reflects the X-Amzn-Sagemaker-Target-Variant request header,
// and defaults to "AllTraffic" when absent.
func TestParity_TargetVariantForwarding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		headers     map[string]string
		wantVariant string
	}{
		{
			name:        "default_all_traffic",
			headers:     nil,
			wantVariant: "AllTraffic",
		},
		{
			name: "explicit_variant",
			headers: map[string]string{
				"X-Amzn-Sagemaker-Target-Variant": "blue",
			},
			wantVariant: "blue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequestWithHeaders(
				t, h, http.MethodPost, "/endpoints/ep/invocations", nil, tt.headers,
			)

			assert.Equal(t, tt.wantVariant, rec.Header().Get("X-Amzn-Invoked-Production-Variant"))
		})
	}
}

// TestParity_AsyncInvocationInferenceIDPreserved verifies that a caller-supplied
// X-Amzn-Sagemaker-Inference-Id is used verbatim as the stored inference ID.
func TestParity_AsyncInvocationInferenceIDPreserved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		headers         map[string]string
		wantInferenceID string
		wantGenerated   bool
	}{
		{
			name:          "generated_when_not_supplied",
			headers:       nil,
			wantGenerated: true,
		},
		{
			name: "supplied_id_preserved",
			headers: map[string]string{
				"X-Amzn-Sagemaker-Inference-Id": "caller-id-99",
			},
			wantInferenceID: "caller-id-99",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequestWithHeaders(
				t, h, http.MethodPost, "/endpoints/ep/async-invocations", nil, tt.headers,
			)

			require.Equal(t, http.StatusAccepted, rec.Code)

			async := h.Backend.ListAsyncInvocations()
			require.Len(t, async, 1)

			if tt.wantInferenceID != "" {
				assert.Equal(t, tt.wantInferenceID, async[0].InferenceID)
			}

			if tt.wantGenerated {
				assert.NotEmpty(t, async[0].InferenceID)
				assert.True(t, strings.HasPrefix(async[0].InferenceID, "gopherstack-"),
					"generated ID should have gopherstack- prefix")
			}
		})
	}
}

// TestParity_ResponseStreamContentType verifies that InvokeEndpointWithResponseStream
// uses the event-stream content type and echoes X-Amzn-Sagemaker-Content-Type from
// the Accept header.
func TestParity_ResponseStreamContentType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		headers         map[string]string
		wantContentType string
	}{
		{
			name:            "default_octet_stream",
			headers:         nil,
			wantContentType: "application/octet-stream",
		},
		{
			name: "accept_json_reflected",
			headers: map[string]string{
				"X-Amzn-Sagemaker-Accept": "application/json",
			},
			wantContentType: "application/json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequestWithHeaders(
				t, h, http.MethodPost,
				"/endpoints/ep/invocations-response-stream",
				nil,
				tt.headers,
			)

			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))
			assert.Equal(t, tt.wantContentType, rec.Header().Get("X-Amzn-Sagemaker-Content-Type"))
		})
	}
}

// TestParity_Shutdown verifies that Handler implements service.Shutdowner and
// that calling Shutdown does not panic.
func TestParity_Shutdown(t *testing.T) {
	t.Parallel()

	h := sagemakerruntime.NewHandler(sagemakerruntime.NewInMemoryBackend("000000000000", "us-east-1"))
	assert.NotPanics(t, func() { h.Shutdown(t.Context()) })
}

// TestParity_BackendRecordAsyncWithSuppliedLocation verifies that an explicit
// outputLocation is stored instead of the generated fake S3 path.
func TestParity_BackendRecordAsyncWithSuppliedLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		outputLocation     string
		wantContainsS3Mock bool
	}{
		{
			name:               "no_location_generates_fake",
			outputLocation:     "",
			wantContainsS3Mock: true,
		},
		{
			name:           "supplied_location_stored_verbatim",
			outputLocation: "s3://real-bucket/path/output/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemakerruntime.NewInMemoryBackend("000000000000", "us-east-1")
			inv := b.RecordAsyncInvocation("ep", "infer-1", "payload", tt.outputLocation)

			if tt.wantContainsS3Mock {
				assert.Contains(t, inv.OutputLocation, "sagemaker-runtime-mock")
			} else {
				assert.Equal(t, tt.outputLocation, inv.OutputLocation)
			}
		})
	}
}
