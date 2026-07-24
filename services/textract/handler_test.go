package textract_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/textract"
)

func newTestHandler(t *testing.T) *textract.Handler {
	t.Helper()

	return textract.NewHandler(textract.NewInMemoryBackendSync("123456789012", "us-east-1"))
}

func doTextractRequest(
	t *testing.T,
	h *textract.Handler,
	target string,
	body any,
) *httptest.ResponseRecorder {
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
	req.Header.Set("X-Amz-Target", "Textract."+target)

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
	assert.Equal(t, "Textract", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	assert.Contains(t, ops, "AnalyzeDocument")
	assert.Contains(t, ops, "DetectDocumentText")
	assert.Contains(t, ops, "StartDocumentAnalysis")
	assert.Contains(t, ops, "GetDocumentAnalysis")
	assert.Contains(t, ops, "StartDocumentTextDetection")
	assert.Contains(t, ops, "GetDocumentTextDetection")
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{name: "matching target", target: "Textract.AnalyzeDocument", want: true},
		{name: "non-matching target", target: "SageMaker.ListModels", want: false},
		{name: "empty target", target: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "UnknownOperation", map[string]string{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_UnknownAction_ErrorEnvelope verifies that dispatching an unknown
// action produces a ValidationException error envelope, not just a 400 status.
func TestHandler_UnknownAction_ErrorEnvelope(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "UndefinedOperation", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	var errResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ValidationException", errResp["__type"])
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{name: "valid target", target: "Textract.AnalyzeDocument", want: "AnalyzeDocument"},
		{name: "empty target", target: "", want: "Unknown"},
		{name: "no prefix", target: "SomethingElse.Action", want: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		body map[string]any
		name string
		want string
	}{
		{
			name: "valid s3 location",
			body: map[string]any{
				"DocumentLocation": map[string]any{
					"S3Object": map[string]any{
						"Bucket": "my-bucket",
						"Name":   "my-document.pdf",
					},
				},
			},
			want: "s3://my-bucket/my-document.pdf",
		},
		{
			name: "missing bucket",
			body: map[string]any{
				"DocumentLocation": map[string]any{
					"S3Object": map[string]any{
						"Bucket": "",
						"Name":   "my-document.pdf",
					},
				},
			},
			want: "",
		},
		{
			name: "missing key",
			body: map[string]any{
				"DocumentLocation": map[string]any{
					"S3Object": map[string]any{
						"Bucket": "my-bucket",
						"Name":   "",
					},
				},
			},
			want: "",
		},
		{
			name: "empty body",
			body: map[string]any{},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "textract", h.ChaosServiceName())
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.ChaosOperations()
	assert.Equal(t, h.GetSupportedOperations(), ops)
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	regions := h.ChaosRegions()
	assert.NotEmpty(t, regions)
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Positive(t, h.MatchPriority())
}

func TestHandler_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	opsLen := textract.HandlerOpsLen(h)
	assert.Equal(t, len(h.GetSupportedOperations()), opsLen)
}

// TestHandler_Snapshot_Restore proves that Handler.Snapshot/Restore round-trips
// a mix of DocumentAnalysis and TextDetection jobs.
func TestHandler_Snapshot_Restore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		jobCount int
	}{
		{name: "empty handler", jobCount: 0},
		{name: "handler with jobs", jobCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range tt.jobCount {
				if i%2 == 0 {
					doTextractRequest(t, h, "StartDocumentAnalysis", map[string]any{
						"DocumentLocation": map[string]any{
							"S3Object": map[string]any{
								"Bucket": "bucket",
								"Name":   "doc.pdf",
							},
						},
						"FeatureTypes": []string{"TABLES"},
					})
				} else {
					doTextractRequest(t, h, "StartDocumentTextDetection", map[string]any{
						"DocumentLocation": map[string]any{
							"S3Object": map[string]any{
								"Bucket": "bucket",
								"Name":   "doc.png",
							},
						},
					})
				}
			}

			snap := h.Snapshot(t.Context())
			require.NotNil(t, snap)

			h2 := newTestHandler(t)
			require.NoError(t, h2.Restore(t.Context(), snap))

			jobs := h2.Backend.ListJobs(t.Context())
			assert.Len(t, jobs, tt.jobCount)
		})
	}
}

func TestProvider_Name(t *testing.T) {
	t.Parallel()

	p := &textract.Provider{}
	assert.Equal(t, "Textract", p.Name())
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &textract.Provider{}
	ctx := &service.AppContext{}
	reg, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, reg)
	assert.Equal(t, "Textract", reg.Name())
}

// TestHandler_ProviderNilAppContext verifies Init returns ErrNilAppContext
// when passed a nil context.
func TestHandler_ProviderNilAppContext(t *testing.T) {
	t.Parallel()

	p := &textract.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, textract.ErrNilAppContext)
}

// TestHandler_Reset verifies Handler.Reset delegates to the backend's Reset.
func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doTextractRequest(t, h, "StartDocumentAnalysis", map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "k"},
		},
		"FeatureTypes": []string{"TABLES"},
	})

	h.Reset()

	b := h.Backend.(*textract.InMemoryBackend)
	assert.Equal(t, 0, textract.JobCount(b))
}

// TestHandler_HandleError_AdapterNotFound ensures adapter not-found returns
// ResourceNotFoundException (not InvalidJobIdException).
func TestHandler_HandleError_AdapterNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "GetAdapter", map[string]any{
		"AdapterId": "no-such-adapter",
	})

	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceNotFoundException", errResp["__type"])
}

// TestHandler_HandleError_JobNotFound ensures job not-found returns
// InvalidJobIdException (not InvalidParameterException).
func TestHandler_HandleError_JobNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "GetDocumentAnalysis", map[string]any{
		"JobId": "no-such-job",
	})

	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidJobIdException", errResp["__type"])
}

// TestHandler_ErrValidation_WrapsInvalidParameter verifies ErrValidation wraps
// awserr.ErrInvalidParameter.
func TestHandler_ErrValidation_WrapsInvalidParameter(t *testing.T) {
	t.Parallel()

	assert.ErrorIs(t, textract.ErrValidation, awserr.ErrInvalidParameter)
}

// TestHandler_ContentType_Success verifies that successful responses carry the
// application/x-amz-json-1.1 content type.
func TestHandler_ContentType_Success(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "DetectDocumentText", map[string]any{
		"Document": map[string]any{
			"S3Object": map[string]any{"Bucket": "b", "Name": "doc.pdf"},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Header().Get("Content-Type"), "application/x-amz-json-1.1",
		"success response must have application/x-amz-json-1.1 content type")
}

// TestHandler_ContentType_Error verifies that error responses across
// different error causes carry the application/x-amz-json-1.1 content type.
func TestHandler_ContentType_Error(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "invalid_job_id",
			action: "GetDocumentAnalysis",
			body:   map[string]any{"JobId": "no-such-job"},
		},
		{
			name:   "validation_error",
			action: "AnalyzeID",
			body:   map[string]any{"DocumentPages": []any{}},
		},
		{
			name:   "adapter_not_found",
			action: "GetAdapter",
			body:   map[string]any{"AdapterId": "no-such-adapter"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTextractRequest(t, h, tt.action, tt.body)
			assert.GreaterOrEqual(t, rec.Code, http.StatusBadRequest)
			assert.Contains(t, rec.Header().Get("Content-Type"), "application/x-amz-json-1.1",
				"error response must have application/x-amz-json-1.1 content type")
		})
	}
}

// TestHandler_ErrorEnvelope_TypeAndMessage verifies that error responses carry
// the expected __type and a non-empty message.
func TestHandler_ErrorEnvelope_TypeAndMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		body     map[string]any
		wantType string
	}{
		{
			name:     "job_not_found_gives_InvalidJobIdException",
			action:   "GetDocumentAnalysis",
			body:     map[string]any{"JobId": "no-such-job"},
			wantType: "InvalidJobIdException",
		},
		{
			name:     "adapter_not_found_gives_ResourceNotFoundException",
			action:   "GetAdapter",
			body:     map[string]any{"AdapterId": "no-such-adapter"},
			wantType: "ResourceNotFoundException",
		},
		{
			// StartDocumentAnalysis has no ValidationException case in the
			// real SDK's deserializeOpErrorStartDocumentAnalysis switch --
			// only InvalidParameterException -- verified against
			// aws-sdk-go-v2/service/textract deserializers.go.
			name:   "validation_error_gives_InvalidParameterException",
			action: "StartDocumentAnalysis",
			body: map[string]any{
				"DocumentLocation": map[string]any{
					"S3Object": map[string]any{"Bucket": "", "Name": ""},
				},
			},
			wantType: "InvalidParameterException",
		},
		{
			name:     "unknown_action_gives_ValidationException",
			action:   "NoSuchAction",
			body:     map[string]any{},
			wantType: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTextractRequest(t, h, tt.action, tt.body)
			assert.GreaterOrEqual(t, rec.Code, http.StatusBadRequest)

			var m map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))
			assert.Equal(t, tt.wantType, m["__type"], "__type must match expected error code")
			assert.NotEmpty(t, m["message"], "message must be present in error envelope")
		})
	}
}
