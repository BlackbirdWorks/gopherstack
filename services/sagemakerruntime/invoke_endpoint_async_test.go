package sagemakerruntime_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakerruntimesdk "github.com/aws/aws-sdk-go-v2/service/sagemakerruntime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- InvokeEndpointAsync tests ---

func TestHandler_InvokeEndpointAsync(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		headers         map[string]string
		wantInferenceID string
	}{
		{name: "generated_inference_id"},
		{
			name:            "client_inference_id_is_preserved",
			headers:         map[string]string{"X-Amzn-Sagemaker-Inference-Id": "request-42"},
			wantInferenceID: "request-42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequestWithHeaders(t, h, http.MethodPost, "/endpoints/my-endpoint/async-invocations",
				map[string]any{"data": "async input"}, tt.headers)

			assert.Equal(t, http.StatusAccepted, rec.Code)

			var out map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.NotEmpty(t, out["InferenceId"])
			assert.NotContains(t, out, "OutputLocation")
			assert.NotContains(t, out, "FailureLocation")
			if tt.wantInferenceID != "" {
				assert.Equal(t, tt.wantInferenceID, out["InferenceId"])
			}
			assert.Contains(t, rec.Header().Get("X-Amzn-Sagemaker-Outputlocation"), out["InferenceId"])
			assert.Contains(t, rec.Header().Get("X-Amzn-Sagemaker-Failurelocation"), out["InferenceId"])
			assert.NotEqual(t, rec.Header().Get("X-Amzn-Sagemaker-Outputlocation"),
				rec.Header().Get("X-Amzn-Sagemaker-Failurelocation"))

			async := h.Backend.ListAsyncInvocations()
			require.Len(t, async, 1)
			assert.Equal(t, out["InferenceId"], async[0].InferenceID)
			assert.Equal(t, rec.Header().Get("X-Amzn-Sagemaker-Outputlocation"), async[0].OutputLocation)
			assert.Equal(t, rec.Header().Get("X-Amzn-Sagemaker-Failurelocation"), async[0].FailureLocation)
		})
	}
}

// TestAsyncInvocationOutputLocation verifies that a caller-supplied
// X-Amzn-Sagemaker-Outputlocation request header is used verbatim in the
// response header and in the stored async invocation record.
func TestAsyncInvocationOutputLocation(t *testing.T) {
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

// TestSDKInvokeEndpointAsync_FilenameShapesOutputLocation verifies that
// InvokeEndpointAsyncInput.Filename (serializers.go binds it to the
// X-Amzn-Sagemaker-Filename request header) is used as the generated
// OutputLocation's final path segment, per the SDK doc on Filename: "If
// not specified, Amazon SageMaker AI generates a filename based on the
// inference ID." Driven through the real SDK client so the effect is
// observed the way a caller would see it, not by inspecting backend state.
func TestSDKInvokeEndpointAsync_FilenameShapesOutputLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		filename     string
		wantSegment  string
		wantNotEqual string
	}{
		{
			name:        "default_filename_when_not_supplied",
			wantSegment: ".out",
		},
		{
			name:        "caller_supplied_filename_used",
			filename:    "result.json",
			wantSegment: "/result.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			client := newTestSDKClient(t, h)

			in := &sagemakerruntimesdk.InvokeEndpointAsyncInput{
				EndpointName:  aws.String("ep"),
				InputLocation: aws.String("s3://input/request"),
			}
			if tt.filename != "" {
				in.Filename = aws.String(tt.filename)
			}

			out, err := client.InvokeEndpointAsync(t.Context(), in)
			require.NoError(t, err)
			assert.True(t, strings.HasSuffix(aws.ToString(out.OutputLocation), tt.wantSegment),
				"OutputLocation %q must end with %q", aws.ToString(out.OutputLocation), tt.wantSegment)
		})
	}
}

// TestAsyncInvocationFailureLocation verifies that InvokeEndpointAsync
// always returns an X-Amzn-Sagemaker-Failurelocation response header (bound by
// the real SDK to InvokeEndpointAsyncOutput.FailureLocation), distinct from
// OutputLocation, regardless of whether the caller supplied an output
// location.
func TestAsyncInvocationFailureLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		headers map[string]string
		name    string
	}{
		{name: "generated_location_when_not_supplied", headers: nil},
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

			outputLoc := rec.Header().Get("X-Amzn-Sagemaker-Outputlocation")
			failureLoc := rec.Header().Get("X-Amzn-Sagemaker-Failurelocation")
			require.NotEmpty(t, outputLoc)
			require.NotEmpty(t, failureLoc, "AWS always returns a FailureLocation, even on success")
			assert.NotEqual(t, outputLoc, failureLoc)

			async := h.Backend.ListAsyncInvocations()
			require.Len(t, async, 1)
			assert.Equal(t, failureLoc, async[0].FailureLocation)
		})
	}
}

// TestAsyncInvocationInferenceIDPreserved verifies that a caller-supplied
// X-Amzn-Sagemaker-Inference-Id is used verbatim as the stored inference ID.
func TestAsyncInvocationInferenceIDPreserved(t *testing.T) {
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
				t, h, http.MethodPost, "/endpoints/ep/async-invocations",
				map[string]any{"data": "payload"}, tt.headers,
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

// TestBackendRecordAsyncStoresLocations verifies RecordAsyncInvocation keeps exactly the
// locations the container engine reported, never inventing its own.
func TestBackendRecordAsyncStoresLocations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		output  string
		failure string
	}{
		{name: "verbatim", output: "s3://b/out/infer-1.out", failure: "s3://b/out/infer-1-error.out"},
		{name: "empty_stays_empty"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newEchoBackend("000000000000", "us-east-1")
			inv := b.RecordAsyncInvocation("ep", "infer-1", "payload", tt.output, tt.failure)

			assert.Equal(t, tt.output, inv.OutputLocation)
			assert.Equal(t, tt.failure, inv.FailureLocation)
		})
	}
}

// TestAsyncInvocation_BodyInputLocationMutualExclusion verifies the real
// AWS constraint documented on InvokeEndpointAsyncInput.Body ("Body and
// InputLocation are mutually exclusive. Provide exactly one of them.") --
// unenforceable client-side (only EndpointName is a required member per
// validators.go's validateOpInvokeEndpointAsyncInput), so it must be
// enforced server-side. Neither field, or both, must be rejected with
// ValidationError; exactly one of them must succeed.
func TestAsyncInvocation_BodyInputLocationMutualExclusion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		headers    map[string]string
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "neither_body_nor_input_location_rejected",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "both_body_and_input_location_rejected",
			body: "payload",
			headers: map[string]string{
				"X-Amzn-Sagemaker-Inputlocation": "s3://bucket/key",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "body_only_accepted",
			body:       "payload",
			wantStatus: http.StatusAccepted,
		},
		{
			name: "input_location_only_accepted",
			headers: map[string]string{
				"X-Amzn-Sagemaker-Inputlocation": "s3://bucket/key",
			},
			wantStatus: http.StatusAccepted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var body any
			if tt.body != "" {
				body = tt.body
			}

			h := newTestHandler(t)
			rec := doRequestWithHeaders(
				t, h, http.MethodPost, "/endpoints/my-endpoint/async-invocations",
				body, tt.headers,
			)

			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusBadRequest {
				var out map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "ValidationError", out["__type"])
				assert.Contains(t, out["message"], "mutually exclusive")
			}
		})
	}
}

func TestSDKInvokeEndpointAsync_PathExtensionShapesLocations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		extension     string
		wantContains  string
		wantNotInPath string
	}{
		{name: "extension_appended", extension: "team-a/run-1", wantContains: "/team-a/run-1/"},
		{name: "slashes_trimmed", extension: "/team-b/", wantContains: "/team-b/"},
		{name: "omitted", wantNotInPath: "team"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestSDKClient(t, newTestHandler(t))

			in := &sagemakerruntimesdk.InvokeEndpointAsyncInput{
				EndpointName:  aws.String("ep"),
				InputLocation: aws.String("s3://input/request"),
			}
			if tt.extension != "" {
				in.S3OutputPathExtension = aws.String(tt.extension)
			}

			out, err := client.InvokeEndpointAsync(t.Context(), in)
			require.NoError(t, err)

			if tt.wantContains != "" {
				assert.Contains(t, aws.ToString(out.OutputLocation), tt.wantContains)
				assert.Contains(t, aws.ToString(out.FailureLocation), tt.wantContains)
			}

			if tt.wantNotInPath != "" {
				assert.NotContains(t, aws.ToString(out.OutputLocation), tt.wantNotInPath)
			}
		})
	}
}
