package textract_test

import (
	"bytes"
	"context"
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
			name:       "empty body rejects with 400 (FeatureTypes required)",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
			wantBlocks: false,
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

			snap := h.Snapshot()
			require.NotNil(t, snap)

			h2 := newTestHandler(t)
			require.NoError(t, h2.Restore(snap))

			jobs := h2.Backend.ListJobs(context.Background())
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

func TestHandler_AnalyzeExpense(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantDocs   bool
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
			},
			wantStatus: http.StatusOK,
			wantDocs:   true,
		},
		{
			name:       "empty body still returns expense docs",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantDocs:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTextractRequest(t, h, "AnalyzeExpense", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantDocs {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				docs, ok := resp["ExpenseDocuments"].([]any)
				assert.True(t, ok)
				assert.NotEmpty(t, docs)
			}
		})
	}
}

func TestHandler_AnalyzeID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantStatus int
		wantErr    bool
	}{
		{
			name: "success with document pages",
			body: map[string]any{
				"DocumentPages": []any{
					map[string]any{
						"S3Object": map[string]any{
							"Bucket": "my-bucket",
							"Name":   "id-front.jpg",
						},
					},
					map[string]any{
						"S3Object": map[string]any{
							"Bucket": "my-bucket",
							"Name":   "id-back.jpg",
						},
					},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty document pages returns error",
			body:       map[string]any{"DocumentPages": []any{}},
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTextractRequest(t, h, "AnalyzeID", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if !tt.wantErr {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				docs, ok := resp["IdentityDocuments"].([]any)
				assert.True(t, ok)
				assert.Len(t, docs, 2)
			}
		})
	}
}

func TestHandler_AdapterLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		adapterName string
		description string
		wantStatus  int
	}{
		{
			name:        "full adapter lifecycle",
			adapterName: "my-adapter",
			description: "test adapter",
			wantStatus:  http.StatusOK,
		},
		{
			name:        "missing adapter name returns error",
			adapterName: "",
			description: "",
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// CreateAdapter
			createBody := map[string]any{
				"AdapterName":  tt.adapterName,
				"Description":  tt.description,
				"FeatureTypes": []string{"QUERIES"},
			}
			createRec := doTextractRequest(t, h, "CreateAdapter", createBody)
			assert.Equal(t, tt.wantStatus, createRec.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			var createResp map[string]string
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			adapterID := createResp["AdapterId"]
			assert.NotEmpty(t, adapterID)

			// GetAdapter
			getBody := map[string]string{"AdapterId": adapterID}
			getRec := doTextractRequest(t, h, "GetAdapter", getBody)
			assert.Equal(t, http.StatusOK, getRec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
			assert.Equal(t, tt.adapterName, getResp["AdapterName"])
			assert.Equal(t, tt.description, getResp["Description"])

			// DeleteAdapter
			deleteBody := map[string]string{"AdapterId": adapterID}
			deleteRec := doTextractRequest(t, h, "DeleteAdapter", deleteBody)
			assert.Equal(t, http.StatusOK, deleteRec.Code)

			// GetAdapter after delete returns error
			getRec2 := doTextractRequest(t, h, "GetAdapter", getBody)
			assert.Equal(t, http.StatusBadRequest, getRec2.Code)
		})
	}
}

func TestHandler_GetAdapter_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{"AdapterId": "nonexistent-adapter"}
	rec := doTextractRequest(t, h, "GetAdapter", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetAdapter_MissingID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{}
	rec := doTextractRequest(t, h, "GetAdapter", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DeleteAdapter_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{"AdapterId": "nonexistent-adapter"}
	rec := doTextractRequest(t, h, "DeleteAdapter", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_AdapterVersionLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "full adapter version lifecycle", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// First create an adapter
			createAdapterBody := map[string]any{
				"AdapterName":  "version-test-adapter",
				"FeatureTypes": []string{"QUERIES"},
			}
			createAdapterRec := doTextractRequest(t, h, "CreateAdapter", createAdapterBody)
			require.Equal(t, tt.wantStatus, createAdapterRec.Code)

			var createAdapterResp map[string]string
			require.NoError(t, json.Unmarshal(createAdapterRec.Body.Bytes(), &createAdapterResp))
			adapterID := createAdapterResp["AdapterId"]

			// CreateAdapterVersion
			createVersionBody := map[string]any{
				"AdapterId": adapterID,
				"Tags":      map[string]string{"env": "test"},
			}
			createVersionRec := doTextractRequest(t, h, "CreateAdapterVersion", createVersionBody)
			assert.Equal(t, http.StatusOK, createVersionRec.Code)

			var createVersionResp map[string]string
			require.NoError(t, json.Unmarshal(createVersionRec.Body.Bytes(), &createVersionResp))
			adapterVersion := createVersionResp["AdapterVersion"]
			assert.NotEmpty(t, adapterVersion)
			assert.Equal(t, adapterID, createVersionResp["AdapterId"])

			// GetAdapterVersion
			getVersionBody := map[string]string{
				"AdapterId":      adapterID,
				"AdapterVersion": adapterVersion,
			}
			getVersionRec := doTextractRequest(t, h, "GetAdapterVersion", getVersionBody)
			assert.Equal(t, http.StatusOK, getVersionRec.Code)

			var getVersionResp map[string]any
			require.NoError(t, json.Unmarshal(getVersionRec.Body.Bytes(), &getVersionResp))
			assert.Equal(t, adapterID, getVersionResp["AdapterId"])
			assert.Equal(t, adapterVersion, getVersionResp["AdapterVersion"])
			assert.Equal(t, "ACTIVE", getVersionResp["Status"])

			// DeleteAdapterVersion
			deleteVersionRec := doTextractRequest(t, h, "DeleteAdapterVersion", getVersionBody)
			assert.Equal(t, http.StatusOK, deleteVersionRec.Code)

			// GetAdapterVersion after delete returns error
			getVersionRec2 := doTextractRequest(t, h, "GetAdapterVersion", getVersionBody)
			assert.Equal(t, http.StatusBadRequest, getVersionRec2.Code)
		})
	}
}

func TestHandler_CreateAdapterVersion_MissingAdapterId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]any{"AdapterId": ""}
	rec := doTextractRequest(t, h, "CreateAdapterVersion", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateAdapterVersion_AdapterNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]any{"AdapterId": "nonexistent-adapter"}
	rec := doTextractRequest(t, h, "CreateAdapterVersion", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetAdapterVersion_MissingFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]string
		name string
	}{
		{
			name: "missing adapter id",
			body: map[string]string{"AdapterId": "", "AdapterVersion": "v1"},
		},
		{
			name: "missing adapter version",
			body: map[string]string{"AdapterId": "adapter-id", "AdapterVersion": ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTextractRequest(t, h, "GetAdapterVersion", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_DeleteAdapter_CascadesVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create adapter
	createResp := doTextractRequest(t, h, "CreateAdapter", map[string]any{
		"AdapterName":  "cascade-test",
		"FeatureTypes": []string{"QUERIES"},
	})
	require.Equal(t, http.StatusOK, createResp.Code)

	var cr map[string]string
	require.NoError(t, json.Unmarshal(createResp.Body.Bytes(), &cr))
	adapterID := cr["AdapterId"]

	// Create a version
	versionResp := doTextractRequest(t, h, "CreateAdapterVersion", map[string]any{
		"AdapterId": adapterID,
	})
	require.Equal(t, http.StatusOK, versionResp.Code)

	var vr map[string]string
	require.NoError(t, json.Unmarshal(versionResp.Body.Bytes(), &vr))
	adapterVersion := vr["AdapterVersion"]

	// Delete adapter should cascade to versions
	deleteResp := doTextractRequest(t, h, "DeleteAdapter", map[string]string{
		"AdapterId": adapterID,
	})
	require.Equal(t, http.StatusOK, deleteResp.Code)

	// Version should now be gone
	getVersionResp := doTextractRequest(t, h, "GetAdapterVersion", map[string]string{
		"AdapterId":      adapterID,
		"AdapterVersion": adapterVersion,
	})
	assert.Equal(t, http.StatusBadRequest, getVersionResp.Code)
}

func TestHandler_GetExpenseAnalysis_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{"JobId": "nonexistent-job"}
	rec := doTextractRequest(t, h, "GetExpenseAnalysis", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetExpenseAnalysis_MissingJobId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{}
	rec := doTextractRequest(t, h, "GetExpenseAnalysis", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetLendingAnalysis_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{"JobId": "nonexistent-job"}
	rec := doTextractRequest(t, h, "GetLendingAnalysis", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetLendingAnalysis_MissingJobId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body := map[string]string{}
	rec := doTextractRequest(t, h, "GetLendingAnalysis", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	opsLen := textract.HandlerOpsLen(h)
	assert.Equal(t, len(h.GetSupportedOperations()), opsLen)
}

func TestHandler_Snapshot_Restore_WithAdapters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		adapterCount int
	}{
		{name: "empty backend", adapterCount: 0},
		{name: "backend with adapters", adapterCount: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for range tt.adapterCount {
				doTextractRequest(t, h, "CreateAdapter", map[string]any{
					"AdapterName":  "test-adapter",
					"FeatureTypes": []string{"QUERIES"},
				})
			}

			snap := h.Snapshot()
			require.NotNil(t, snap)

			h2 := newTestHandler(t)
			require.NoError(t, h2.Restore(snap))

			assert.Equal(t, tt.adapterCount, textract.AdapterCount(h2.Backend.(*textract.InMemoryBackend)))
		})
	}
}
