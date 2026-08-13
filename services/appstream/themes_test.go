package appstream_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

// validThemeBody is a CreateThemeForStack request body populating all five
// required members (StackName, FaviconS3Location, OrganizationLogoS3Location,
// ThemeStyling, TitleText) plus an optional FooterLinks entry.
func validThemeBody(stackName string) map[string]any {
	return map[string]any{
		"StackName": stackName,
		"FaviconS3Location": map[string]any{
			"S3Bucket": "theme-assets",
			"S3Key":    "favicon.ico",
		},
		"OrganizationLogoS3Location": map[string]any{
			"S3Bucket": "theme-assets",
			"S3Key":    "logo.png",
		},
		"ThemeStyling": "BLUE",
		"TitleText":    "My Streaming App",
		"FooterLinks": []map[string]any{
			{"DisplayName": "Support", "FooterLinkURL": "https://support.example.com"},
		},
	}
}

// TestAppStream_Themes covers Theme CRUD.
func TestAppStream_Themes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *appstream.Handler)
		check    func(t *testing.T, body []byte)
		body     any
		name     string
		action   string
		wantCode int
	}{
		{
			name:   "CreateThemeForStack returns theme",
			action: "CreateThemeForStack",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "theme-stk")
			},
			body:     validThemeBody("theme-stk"),
			wantCode: http.StatusOK,
			check: func(t *testing.T, respBody []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(respBody, &resp))
				th := resp["Theme"].(map[string]any)
				assert.Equal(t, "theme-stk", th["StackName"])
				assert.Equal(t, "ENABLED", th["State"])
				assert.Equal(t, "BLUE", th["ThemeStyling"],
					"ThemeStyling must be recorded, not dropped")
				assert.Equal(t, "My Streaming App", th["ThemeTitleText"])
				assert.Equal(t, "https://s3.amazonaws.com/theme-assets/favicon.ico", th["ThemeFaviconURL"],
					"FaviconS3Location must be reflected in the response")
				assert.Equal(t, "https://s3.amazonaws.com/theme-assets/logo.png", th["ThemeOrganizationLogoURL"],
					"OrganizationLogoS3Location must be reflected in the response")
				links, ok := th["ThemeFooterLinks"].([]any)
				require.True(t, ok)
				require.Len(t, links, 1)
				link := links[0].(map[string]any)
				assert.Equal(t, "Support", link["DisplayName"])
				assert.Equal(t, "https://support.example.com", link["FooterLinkURL"])
			},
		},
		{
			name:   "CreateThemeForStack missing FaviconS3Location rejected",
			action: "CreateThemeForStack",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "theme-nofavicon-stk")
			},
			body: map[string]any{
				"StackName": "theme-nofavicon-stk",
				"OrganizationLogoS3Location": map[string]any{
					"S3Bucket": "theme-assets",
					"S3Key":    "logo.png",
				},
				"ThemeStyling": "BLUE",
				"TitleText":    "My Streaming App",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "DescribeThemeForStack returns theme",
			action: "DescribeThemeForStack",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "desc-theme-stk")
				rec := doRequest(t, h, "CreateThemeForStack", validThemeBody("desc-theme-stk"))
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"StackName": "desc-theme-stk"},
			wantCode: http.StatusOK,
		},
		{
			name:   "UpdateThemeForStack returns theme",
			action: "UpdateThemeForStack",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "upd-theme-stk")
				rec := doRequest(t, h, "CreateThemeForStack", validThemeBody("upd-theme-stk"))
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"StackName": "upd-theme-stk"},
			wantCode: http.StatusOK,
		},
		{
			name:   "DeleteThemeForStack removes theme",
			action: "DeleteThemeForStack",
			setup: func(h *appstream.Handler) {
				createStack(t, h, "del-theme-stk")
				rec := doRequest(t, h, "CreateThemeForStack", validThemeBody("del-theme-stk"))
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"StackName": "del-theme-stk"},
			wantCode: http.StatusOK,
		},
		{
			name:     "DescribeThemeForStack unknown returns error",
			action:   "DescribeThemeForStack",
			body:     map[string]any{"StackName": "no-such"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.action, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}
