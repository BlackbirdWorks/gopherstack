package awsconfig_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

// TestConformancePackARN verifies PutConformancePack generates an ARN and ID.
func TestConformancePackARN(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	rec := doAWSConfigRequest(t, h, "PutConformancePack", map[string]any{
		"ConformancePackName": "test-pack",
		"DeliveryS3Bucket":    "my-delivery-bucket",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doAWSConfigRequest(t, h, "DescribeConformancePacks", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ConformancePackDetails []struct {
			ConformancePackArn string `json:"ConformancePackArn"`
			ConformancePackID  string `json:"ConformancePackId"`
			DeliveryS3Bucket   string `json:"DeliveryS3Bucket"`
		} `json:"ConformancePackDetails"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.ConformancePackDetails, 1)
	assert.Contains(t, out.ConformancePackDetails[0].ConformancePackArn, "arn:aws:config:")
	assert.NotEmpty(t, out.ConformancePackDetails[0].ConformancePackID)
	assert.Equal(t, "my-delivery-bucket", out.ConformancePackDetails[0].DeliveryS3Bucket)
}

func TestAWSConfigHandler_DeleteConformancePack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *awsconfig.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *awsconfig.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.PutConformancePack("my-pack", "", ""))
			},
			body:     map[string]any{"ConformancePackName": "my-pack"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     map[string]any{"ConformancePackName": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doAWSConfigRequest(t, h, "DeleteConformancePack", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
