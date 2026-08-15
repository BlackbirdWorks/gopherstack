package elasticbeanstalk_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	platformAction = "Version=2010-12-01&Action=CreatePlatformVersion"
	// bundleParams is the PlatformDefinitionBundle.S3Bucket/S3Key form suffix
	// CreatePlatformVersion requires (S3Location: This member is required).
	bundleParams = "&PlatformDefinitionBundle.S3Bucket=my-bucket&PlatformDefinitionBundle.S3Key=my-key.zip"
)

func TestHandler_CreatePlatformVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantXML    string
		wantStatus int
	}{
		{
			name:       "success",
			body:       platformAction + "&PlatformName=MyPlatform&PlatformVersion=1.0.0" + bundleParams,
			wantStatus: http.StatusOK,
			wantXML:    "CreatePlatformVersionResponse",
		},
		{
			name:       "missing platform name",
			body:       platformAction + "&PlatformVersion=1.0.0" + bundleParams,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing platform version",
			body:       platformAction + "&PlatformName=MyPlatform" + bundleParams,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing platform definition bundle",
			body:       platformAction + "&PlatformName=MyPlatform&PlatformVersion=1.0.0",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := postEBForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantXML != "" {
				assert.Contains(t, rec.Body.String(), tt.wantXML)
			}
		})
	}
}

// TestHandler_CreatePlatformVersion_DuplicateRejected verifies duplicate platform
// versions (same name and version) are rejected.
func TestHandler_CreatePlatformVersion_DuplicateRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	body := platformAction + "&PlatformName=MyPlatform&PlatformVersion=1.0" + bundleParams

	rec1 := postEBForm(t, h, body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := postEBForm(t, h, body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}
