package textract_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_AnalyzeDocumentFeatureTypesValidation verifies that AnalyzeDocument
// rejects unknown FeatureType strings with InvalidParameterException (HTTP 400).
// The previous implementation accepted any string silently, returning an empty
// feature-specific block set rather than the expected error.
func TestParity_AnalyzeDocumentFeatureTypesValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		featureTypes []string
		wantCode     int
	}{
		{
			name:         "valid_tables_accepted",
			featureTypes: []string{"TABLES"},
			wantCode:     http.StatusOK,
		},
		{
			name:         "valid_forms_accepted",
			featureTypes: []string{"FORMS"},
			wantCode:     http.StatusOK,
		},
		{
			name:         "valid_queries_accepted",
			featureTypes: []string{"QUERIES"},
			wantCode:     http.StatusOK,
		},
		{
			name:         "valid_signatures_accepted",
			featureTypes: []string{"SIGNATURES"},
			wantCode:     http.StatusOK,
		},
		{
			name:         "valid_layout_accepted",
			featureTypes: []string{"LAYOUT"},
			wantCode:     http.StatusOK,
		},
		{
			name:         "multiple_valid_accepted",
			featureTypes: []string{"TABLES", "FORMS"},
			wantCode:     http.StatusOK,
		},
		{
			name:         "unknown_feature_type_rejected",
			featureTypes: []string{"UNKNOWN_FEATURE"},
			wantCode:     http.StatusBadRequest,
		},
		{
			name:         "mixed_valid_invalid_rejected",
			featureTypes: []string{"TABLES", "INVALID"},
			wantCode:     http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTextractRequest(t, h, "AnalyzeDocument", map[string]any{
				"Document": map[string]any{
					"S3Object": map[string]any{
						"Bucket": "test-bucket",
						"Name":   "test-doc.pdf",
					},
				},
				"FeatureTypes": tt.featureTypes,
				// Always include QueriesConfig so QUERIES cases are not rejected
				// for the missing-config reason rather than the feature-type reason.
				"QueriesConfig": map[string]any{
					"Queries": []any{map[string]any{"Text": "What is the total?"}},
				},
			})

			assert.Equal(t, tt.wantCode, rec.Code, "FeatureTypes=%v", tt.featureTypes)
		})
	}
}

// TestParity_StartDocumentAnalysisFeatureTypesValidation verifies the same
// unknown-FeatureType validation applies to the async StartDocumentAnalysis path.
func TestParity_StartDocumentAnalysisFeatureTypesValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		featureTypes []string
		wantCode     int
	}{
		{
			name:         "valid_tables_starts_job",
			featureTypes: []string{"TABLES"},
			wantCode:     http.StatusOK,
		},
		{
			name:         "unknown_feature_type_rejected",
			featureTypes: []string{"BANANA"},
			wantCode:     http.StatusBadRequest,
		},
		{
			name:         "mixed_valid_invalid_rejected",
			featureTypes: []string{"FORMS", "NOT_A_TYPE"},
			wantCode:     http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTextractRequest(t, h, "StartDocumentAnalysis", map[string]any{
				"DocumentLocation": map[string]any{
					"S3Object": map[string]any{
						"Bucket": "test-bucket",
						"Name":   "test-doc.pdf",
					},
				},
				"FeatureTypes": tt.featureTypes,
			})

			assert.Equal(t, tt.wantCode, rec.Code, "FeatureTypes=%v", tt.featureTypes)

			if tt.wantCode == http.StatusOK {
				require.Contains(t, rec.Body.String(), "JobId")
			}
		})
	}
}
