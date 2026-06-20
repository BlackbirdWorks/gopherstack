package textract_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestParity_AnalyzeDocumentRequiresNonEmptyFeatureTypes verifies that
// AnalyzeDocument and StartDocumentAnalysis reject requests with empty or
// absent FeatureTypes. Real AWS requires at least one valid feature type;
// the emulator previously accepted empty lists, returning empty blocks.
func TestParity_AnalyzeDocumentRequiresNonEmptyFeatureTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		op           string
		featureTypes []string
		wantCode     int
	}{
		{
			name:         "AnalyzeDocument_nil_feature_types_rejected",
			op:           "AnalyzeDocument",
			featureTypes: nil,
			wantCode:     http.StatusBadRequest,
		},
		{
			name:         "AnalyzeDocument_empty_feature_types_rejected",
			op:           "AnalyzeDocument",
			featureTypes: []string{},
			wantCode:     http.StatusBadRequest,
		},
		{
			name:         "StartDocumentAnalysis_nil_feature_types_rejected",
			op:           "StartDocumentAnalysis",
			featureTypes: nil,
			wantCode:     http.StatusBadRequest,
		},
		{
			name:         "StartDocumentAnalysis_empty_feature_types_rejected",
			op:           "StartDocumentAnalysis",
			featureTypes: []string{},
			wantCode:     http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var body map[string]any

			switch tt.op {
			case "AnalyzeDocument":
				body = map[string]any{
					"Document": map[string]any{
						"S3Object": map[string]any{
							"Bucket": "test-bucket",
							"Name":   "doc.pdf",
						},
					},
				}
			default:
				body = map[string]any{
					"DocumentLocation": map[string]any{
						"S3Object": map[string]any{
							"Bucket": "test-bucket",
							"Name":   "doc.pdf",
						},
					},
				}
			}

			if tt.featureTypes != nil {
				body["FeatureTypes"] = tt.featureTypes
			}

			rec := doTextractRequest(t, h, tt.op, body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"%s with empty FeatureTypes must return 400", tt.op)
		})
	}
}
