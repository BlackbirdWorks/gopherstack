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

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/textract"
)

func newTestHandler(t *testing.T) *textract.Handler {
	t.Helper()

	return textract.NewHandler(textract.NewInMemoryBackend())
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

func TestHandler_AnalyzeDocument(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantBlocks bool
	}{
		{
			name: "success with S3 document",
			body: map[string]any{
				"Document": map[string]any{
					"S3Object": map[string]any{
						"Bucket": "my-bucket",
						"Name":   "invoice.pdf",
					},
				},
				"FeatureTypes": []string{"TABLES", "FORMS"},
			},
			wantStatus: http.StatusOK,
			wantBlocks: true,
		},
		{
			name:       "empty body still returns blocks",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantBlocks: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTextractRequest(t, h, "AnalyzeDocument", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBlocks {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				blocks, ok := resp["Blocks"].([]any)
				assert.True(t, ok)
				assert.NotEmpty(t, blocks)
			}
		})
	}
}

func TestHandler_DetectDocumentText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"Document": map[string]any{
					"S3Object": map[string]any{
						"Bucket": "my-bucket",
						"Name":   "page.png",
					},
				},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTextractRequest(t, h, "DetectDocumentText", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_StartAndGetDocumentAnalysis(t *testing.T) {
	t.Parallel()

	tests := []struct {
		startBody  any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			startBody: map[string]any{
				"DocumentLocation": map[string]any{
					"S3Object": map[string]any{
						"Bucket": "my-bucket",
						"Name":   "document.pdf",
					},
				},
				"FeatureTypes": []string{"TABLES"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Start the job
			rec := doTextractRequest(t, h, "StartDocumentAnalysis", tt.startBody)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var startResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
			jobID := startResp["JobId"]
			assert.NotEmpty(t, jobID)

			// Get results
			getBody := map[string]string{"JobId": jobID}
			getRec := doTextractRequest(t, h, "GetDocumentAnalysis", getBody)
			assert.Equal(t, http.StatusOK, getRec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
			assert.Equal(t, "SUCCEEDED", getResp["JobStatus"])
			blocks, ok := getResp["Blocks"].([]any)
			assert.True(t, ok)
			assert.NotEmpty(t, blocks)
		})
	}
}

func TestHandler_StartDocumentAnalysis_MissingBucket(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{
				"Bucket": "",
				"Name":   "",
			},
		},
	}

	rec := doTextractRequest(t, h, "StartDocumentAnalysis", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_StartAndGetDocumentTextDetection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		startBody  any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			startBody: map[string]any{
				"DocumentLocation": map[string]any{
					"S3Object": map[string]any{
						"Bucket": "my-bucket",
						"Name":   "page.jpg",
					},
				},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doTextractRequest(t, h, "StartDocumentTextDetection", tt.startBody)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var startResp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
			jobID := startResp["JobId"]
			assert.NotEmpty(t, jobID)

			getBody := map[string]string{"JobId": jobID}
			getRec := doTextractRequest(t, h, "GetDocumentTextDetection", getBody)
			assert.Equal(t, http.StatusOK, getRec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
			assert.Equal(t, "SUCCEEDED", getResp["JobStatus"])
		})
	}
}

func TestHandler_GetDocumentAnalysis_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{"JobId": "nonexistent-job"}

	rec := doTextractRequest(t, h, "GetDocumentAnalysis", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetDocumentAnalysis_MissingJobId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{}

	rec := doTextractRequest(t, h, "GetDocumentAnalysis", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetDocumentTextDetection_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{"JobId": "nonexistent-job"}

	rec := doTextractRequest(t, h, "GetDocumentTextDetection", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTextractRequest(t, h, "UnknownOperation", map[string]string{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
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

func TestHandler_StartDocumentTextDetection_MissingBucket(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]any{
		"DocumentLocation": map[string]any{
			"S3Object": map[string]any{
				"Bucket": "",
				"Name":   "",
			},
		},
	}

	rec := doTextractRequest(t, h, "StartDocumentTextDetection", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetDocumentTextDetection_MissingJobId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{}

	rec := doTextractRequest(t, h, "GetDocumentTextDetection", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

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

			snap := h.Snapshot()
			require.NotNil(t, snap)

			h2 := newTestHandler(t)
			require.NoError(t, h2.Restore(snap))

			jobs := h2.Backend.ListJobs()
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
