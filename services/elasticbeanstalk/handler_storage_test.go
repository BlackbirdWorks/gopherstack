package elasticbeanstalk_test

import (
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateStorageLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantXML    string
		wantStatus int
	}{
		{
			name:       "success returns bucket name",
			wantStatus: http.StatusOK,
			wantXML:    "CreateStorageLocationResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := postEBForm(t, h, "Version=2010-12-01&Action=CreateStorageLocation")
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantXML != "" {
				assert.Contains(t, rec.Body.String(), tt.wantXML)

				var out struct {
					CreateStorageLocationResult struct {
						S3Bucket string `xml:"S3Bucket"`
					} `xml:"CreateStorageLocationResult"`
				}

				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.CreateStorageLocationResult.S3Bucket)
				assert.Contains(t, out.CreateStorageLocationResult.S3Bucket, "elasticbeanstalk")
			}
		})
	}
}

// TestHandler_CreateStorageLocation_Idempotent verifies CreateStorageLocation
// returns the same bucket name on repeated calls.
func TestHandler_CreateStorageLocation_Idempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec1 := postEBForm(t, h, "Version=2010-12-01&Action=CreateStorageLocation")
	rec2 := postEBForm(t, h, "Version=2010-12-01&Action=CreateStorageLocation")

	require.Equal(t, http.StatusOK, rec1.Code)
	require.Equal(t, http.StatusOK, rec2.Code)
	assert.Equal(t, rec1.Body.String(), rec2.Body.String())
	assert.Contains(t, rec1.Body.String(), "elasticbeanstalk-us-east-1-123456789012")
}
