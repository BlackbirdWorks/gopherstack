package forecast_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_CreateResource_NameFormatValidation verifies that Forecast Create*
// operations enforce AWS name format rules: only alphanumeric characters,
// underscores, and hyphens are allowed; max 256 characters.
// Real AWS returns InvalidInputException for names that violate these rules;
// the emulator previously accepted any non-empty string.
func TestParity_CreateResource_NameFormatValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resName  string
		wantCode int
	}{
		{
			name:     "space_in_name_rejected",
			resName:  "my dataset",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "dot_in_name_rejected",
			resName:  "my.dataset",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "slash_in_name_rejected",
			resName:  "my/dataset",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "at_sign_rejected",
			resName:  "my@dataset",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "simple_name_accepted",
			resName:  "mydataset",
			wantCode: http.StatusOK,
		},
		{
			name:     "underscores_accepted",
			resName:  "my_dataset_v2",
			wantCode: http.StatusOK,
		},
		{
			name:     "hyphens_accepted",
			resName:  "my-dataset-v2",
			wantCode: http.StatusOK,
		},
		{
			name:     "mixed_case_accepted",
			resName:  "MyDataset123",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			code, resp := request(t, h, "CreateDatasetGroup", map[string]any{
				"DatasetGroupName": tt.resName,
				"Domain":           "RETAIL",
			})

			assert.Equal(t, tt.wantCode, code, "DatasetGroupName=%q", tt.resName)

			if tt.wantCode == http.StatusBadRequest {
				assert.Equal(t, "InvalidInputException", resp["__type"],
					"expected InvalidInputException for DatasetGroupName=%q", tt.resName)
			}
		})
	}
}

// TestParity_CreateResource_NameMaxLength verifies that a 256-character name is
// accepted and a 257-character name is rejected with InvalidInputException.
// Real AWS Forecast enforces a 256-character maximum across all resource types.
func TestParity_CreateResource_NameMaxLength(t *testing.T) {
	t.Parallel()

	h := newHandler()

	validName := strings.Repeat("a", 256)
	code, _ := request(t, h, "CreateDatasetGroup", map[string]any{
		"DatasetGroupName": validName,
		"Domain":           "RETAIL",
	})
	require.Equal(t, http.StatusOK, code, "256-char name should be accepted")

	h2 := newHandler()
	tooLongName := strings.Repeat("a", 257)
	code2, resp2 := request(t, h2, "CreateDatasetGroup", map[string]any{
		"DatasetGroupName": tooLongName,
		"Domain":           "RETAIL",
	})
	assert.Equal(t, http.StatusBadRequest, code2, "257-char name should be rejected")
	assert.Equal(t, "InvalidInputException", resp2["__type"])
}
