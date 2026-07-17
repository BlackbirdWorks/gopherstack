package elasticbeanstalk_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			body:       "Version=2010-12-01&Action=CreatePlatformVersion&PlatformName=MyPlatform&PlatformVersion=1.0.0",
			wantStatus: http.StatusOK,
			wantXML:    "CreatePlatformVersionResponse",
		},
		{
			name:       "missing platform name",
			body:       "Version=2010-12-01&Action=CreatePlatformVersion&PlatformVersion=1.0.0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing platform version",
			body:       "Version=2010-12-01&Action=CreatePlatformVersion&PlatformName=MyPlatform",
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

	rec1 := postEBForm(t, h,
		"Version=2010-12-01&Action=CreatePlatformVersion&PlatformName=MyPlatform&PlatformVersion=1.0")
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := postEBForm(t, h,
		"Version=2010-12-01&Action=CreatePlatformVersion&PlatformName=MyPlatform&PlatformVersion=1.0")
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}
