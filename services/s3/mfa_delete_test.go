package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMFADeleteRequiresMFAHeader(t *testing.T) {
	t.Parallel()

	const enable = `<VersioningConfiguration><Status>Enabled</Status>` +
		`<MfaDelete>Enabled</MfaDelete></VersioningConfiguration>`

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		mfa        string
		wantStatus int
	}{
		{"enable_without_mfa", http.MethodPut, "/mfa-b?versioning", enable, "", http.StatusForbidden},
		{"enable_with_mfa", http.MethodPut, "/mfa-b?versioning", enable, "arn:aws:iam::1:mfa/x 123456", http.StatusOK},
		{"delete_version_without_mfa", http.MethodDelete, "/mfa-b/k?versionId=v1", "", "", http.StatusForbidden},
		{"delete_version_with_mfa", http.MethodDelete, "/mfa-b/k?versionId=v1", "", "arn 123456", http.StatusNoContent},
		{"delete_marker_without_mfa", http.MethodDelete, "/mfa-b/k", "", "", http.StatusNoContent},
		{
			"delete_objects_versioned_without_mfa", http.MethodPost, "/mfa-b?delete",
			`<Delete><Object><Key>k</Key><VersionId>v1</VersionId></Object></Delete>`, "", http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "mfa-b")

			setup := httptest.NewRequest(http.MethodPut, "/mfa-b?versioning", strings.NewReader(enable))
			setup.Header.Set("X-Amz-Mfa", "arn 000000")
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, setup)
			require.Equal(t, http.StatusOK, rec.Code)

			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			if tt.mfa != "" {
				req.Header.Set("X-Amz-Mfa", tt.mfa)
			}

			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			if tt.wantStatus == http.StatusForbidden {
				assert.Equal(t, http.StatusForbidden, rec.Code)
				assert.Contains(t, rec.Body.String(), "AccessDenied")

				return
			}

			assert.NotEqual(t, http.StatusForbidden, rec.Code)
		})
	}
}
