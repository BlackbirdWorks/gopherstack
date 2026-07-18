package sns_test

import (
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSNSHandler_CreatePlatformApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup            func(b *sns.InMemoryBackend)
		form             url.Values
		name             string
		wantBodyContains []string
		wantStatus       int
	}{
		{
			name: "success",
			form: url.Values{
				"Action":                   {"CreatePlatformApplication"},
				"Name":                     {"MyApp"},
				"Platform":                 {"GCM"},
				"Attributes.entry.1.key":   {"PlatformCredential"},
				"Attributes.entry.1.value": {"my-api-key"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"CreatePlatformApplicationResponse", "app/GCM/MyApp"},
		},
		{
			name: "duplicate_returns_error",
			setup: func(b *sns.InMemoryBackend) {
				b.CreatePlatformApplication("MyApp", "GCM", nil)
			},
			form: url.Values{
				"Action":   {"CreatePlatformApplication"},
				"Name":     {"MyApp"},
				"Platform": {"GCM"},
			},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: []string{"PlatformApplicationAlreadyExists"},
		},
		{
			name: "invalid_name_with_slash",
			form: url.Values{
				"Action":   {"CreatePlatformApplication"},
				"Name":     {"My/App"},
				"Platform": {"GCM"},
			},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: []string{"InvalidParameter"},
		},
		{
			name: "missing_name",
			form: url.Values{
				"Action":   {"CreatePlatformApplication"},
				"Platform": {"GCM"},
			},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: []string{"InvalidParameter"},
		},
		{
			name: "missing_platform",
			form: url.Values{
				"Action": {"CreatePlatformApplication"},
				"Name":   {"MyApp"},
			},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: []string{"InvalidParameter"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, b := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := snsPost(t, h, tt.form)
			assert.Equal(t, tt.wantStatus, rec.Code)
			for _, want := range tt.wantBodyContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestSNSHandler_GetPlatformApplicationAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup            func(b *sns.InMemoryBackend) string
		formOverrides    url.Values
		name             string
		wantBodyContains []string
		wantStatus       int
	}{
		{
			name: "success",
			setup: func(b *sns.InMemoryBackend) string {
				app, _ := b.CreatePlatformApplication("MyApp", "GCM", map[string]string{"PlatformCredential": "key123"})

				return app.PlatformApplicationArn
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"GetPlatformApplicationAttributesResponse", "PlatformCredential", "key123"},
		},
		{
			name:          "missing_arn",
			formOverrides: url.Values{"PlatformApplicationArn": {""}},
			wantStatus:    http.StatusBadRequest,
		},
		{
			name: "not_found",
			formOverrides: url.Values{
				"PlatformApplicationArn": {"arn:aws:sns:us-east-1:000000000000:app/GCM/nonexistent"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, b := newTestHandler(t)
			form := url.Values{"Action": {"GetPlatformApplicationAttributes"}}

			if tt.setup != nil {
				form.Set("PlatformApplicationArn", tt.setup(b))
			}

			maps.Copy(form, tt.formOverrides)

			rec := snsPost(t, h, form)
			assert.Equal(t, tt.wantStatus, rec.Code)
			for _, want := range tt.wantBodyContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestSNSHandler_SetPlatformApplicationAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup            func(b *sns.InMemoryBackend) string
		formOverrides    url.Values
		name             string
		wantBodyContains []string
		wantStatus       int
	}{
		{
			name: "success",
			setup: func(b *sns.InMemoryBackend) string {
				app, _ := b.CreatePlatformApplication("MyApp", "GCM", nil)

				return app.PlatformApplicationArn
			},
			formOverrides: url.Values{
				"Attributes.entry.1.key":   {"PlatformCredential"},
				"Attributes.entry.1.value": {"new-key"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"SetPlatformApplicationAttributesResponse"},
		},
		{
			name:          "missing_arn",
			formOverrides: url.Values{"PlatformApplicationArn": {""}},
			wantStatus:    http.StatusBadRequest,
		},
		{
			name: "not_found",
			formOverrides: url.Values{
				"PlatformApplicationArn": {"arn:aws:sns:us-east-1:000000000000:app/GCM/none"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, b := newTestHandler(t)
			form := url.Values{"Action": {"SetPlatformApplicationAttributes"}}

			if tt.setup != nil {
				form.Set("PlatformApplicationArn", tt.setup(b))
			}

			maps.Copy(form, tt.formOverrides)

			rec := snsPost(t, h, form)
			assert.Equal(t, tt.wantStatus, rec.Code)
			for _, want := range tt.wantBodyContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestSNSHandler_ListPlatformApplications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup            func(b *sns.InMemoryBackend)
		name             string
		wantBodyContains []string
		wantStatus       int
	}{
		{
			name:             "empty",
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"ListPlatformApplicationsResponse"},
		},
		{
			name: "with_applications",
			setup: func(b *sns.InMemoryBackend) {
				b.CreatePlatformApplication("App1", "GCM", nil)
				b.CreatePlatformApplication("App2", "APNS", nil)
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"ListPlatformApplicationsResponse", "app/GCM/App1", "app/APNS/App2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, b := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := snsPost(t, h, url.Values{"Action": {"ListPlatformApplications"}})
			assert.Equal(t, tt.wantStatus, rec.Code)
			for _, want := range tt.wantBodyContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestSNSHandler_DeletePlatformApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(b *sns.InMemoryBackend) string
		formOverrides url.Values
		name          string
		wantStatus    int
	}{
		{
			name: "success",
			setup: func(b *sns.InMemoryBackend) string {
				app, _ := b.CreatePlatformApplication("MyApp", "GCM", nil)

				return app.PlatformApplicationArn
			},
			wantStatus: http.StatusOK,
		},
		{
			name:          "missing_arn",
			formOverrides: url.Values{"PlatformApplicationArn": {""}},
			wantStatus:    http.StatusBadRequest,
		},
		{
			name: "not_found",
			formOverrides: url.Values{
				"PlatformApplicationArn": {"arn:aws:sns:us-east-1:000000000000:app/GCM/none"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, b := newTestHandler(t)
			form := url.Values{"Action": {"DeletePlatformApplication"}}

			if tt.setup != nil {
				form.Set("PlatformApplicationArn", tt.setup(b))
			}

			maps.Copy(form, tt.formOverrides)

			rec := snsPost(t, h, form)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestInMemoryBackend_PlatformApplicationLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *sns.InMemoryBackend)
		name string
	}{
		{
			name: "create_and_get",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				app, err := b.CreatePlatformApplication("App1", "GCM", map[string]string{"PlatformCredential": "key1"})
				require.NoError(t, err)
				assert.Contains(t, app.PlatformApplicationArn, "app/GCM/App1")

				attrs, err := b.GetPlatformApplicationAttributes(app.PlatformApplicationArn)
				require.NoError(t, err)
				assert.Equal(t, "key1", attrs["PlatformCredential"])
			},
		},
		{
			name: "get_not_found",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				_, err := b.GetPlatformApplicationAttributes("arn:aws:sns:us-east-1:000000000000:app/GCM/nonexistent")
				require.ErrorIs(t, err, sns.ErrPlatformApplicationNotFound)
			},
		},
		{
			name: "set_attributes",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				app, err := b.CreatePlatformApplication("App2", "APNS", nil)
				require.NoError(t, err)

				err = b.SetPlatformApplicationAttributes(
					app.PlatformApplicationArn,
					map[string]string{"EventEndpointCreated": "arn:aws:sns:us-east-1:000000000000:notify"},
				)
				require.NoError(t, err)

				attrs, err := b.GetPlatformApplicationAttributes(app.PlatformApplicationArn)
				require.NoError(t, err)
				assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:notify", attrs["EventEndpointCreated"])
			},
		},
		{
			name: "delete",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				app, err := b.CreatePlatformApplication("App3", "GCM", nil)
				require.NoError(t, err)

				err = b.DeletePlatformApplication(app.PlatformApplicationArn)
				require.NoError(t, err)

				_, err = b.GetPlatformApplicationAttributes(app.PlatformApplicationArn)
				require.ErrorIs(t, err, sns.ErrPlatformApplicationNotFound)
			},
		},
		{
			name: "delete_also_removes_endpoints",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				app, err := b.CreatePlatformApplication("App4", "GCM", nil)
				require.NoError(t, err)

				ep, err := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "tok", nil)
				require.NoError(t, err)

				err = b.DeletePlatformApplication(app.PlatformApplicationArn)
				require.NoError(t, err)

				_, err = b.GetEndpointAttributes(ep.EndpointArn)
				require.ErrorIs(t, err, sns.ErrEndpointNotFound)
			},
		},
		{
			name: "duplicate_returns_error",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				_, err := b.CreatePlatformApplication("DupApp", "GCM", nil)
				require.NoError(t, err)

				_, err = b.CreatePlatformApplication("DupApp", "GCM", nil)
				require.ErrorIs(t, err, sns.ErrPlatformApplicationAlreadyExists)
			},
		},
		{
			name: "slash_in_name_returns_error",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				_, err := b.CreatePlatformApplication("My/App", "GCM", nil)
				require.ErrorIs(t, err, sns.ErrInvalidParameter)
			},
		},
		{
			name: "slash_in_platform_returns_error",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				_, err := b.CreatePlatformApplication("MyApp", "GCM/v2", nil)
				require.ErrorIs(t, err, sns.ErrInvalidParameter)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := sns.NewInMemoryBackend()
			tt.run(t, b)
		})
	}
}

// TestSNS_PlatformApplicationRegion verifies that platform application ARNs embed
// the request region rather than the backend's default region.
func TestSNS_PlatformApplicationRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
	}{
		{name: "us-east-1", region: "us-east-1"},
		{name: "eu-west-1", region: "eu-west-1"},
		{name: "ap-southeast-1", region: "ap-southeast-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			app, err := b.CreatePlatformApplicationInRegion("myapp", "GCM", tt.region, nil)
			require.NoError(t, err)

			assert.Contains(t, app.PlatformApplicationArn, ":"+tt.region+":")
		})
	}
}

// TestListAllPlatformApplications verifies list works.
func TestListAllPlatformApplications(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	apps := b.ListAllPlatformApplications()
	assert.Empty(t, apps)

	_, err := b.CreatePlatformApplication("my-app", "GCM", map[string]string{
		"PlatformCredential": "fake-cred",
	})
	require.NoError(t, err)

	apps = b.ListAllPlatformApplications()
	assert.Len(t, apps, 1)
}

// TestFCMPlatformCreated verifies FCM is a valid platform name.
func TestFCMPlatformCreated(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	app, err := b.CreatePlatformApplication("my-fcm-app", "FCM", map[string]string{
		"PlatformCredential": "fake-api-key",
	})
	require.NoError(t, err)
	assert.Contains(t, app.PlatformApplicationArn, "FCM")
}

// TestAPNSVoIPPlatformCreated verifies APNS_VOIP is a valid platform name.
func TestAPNSVoIPPlatformCreated(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	app, err := b.CreatePlatformApplication("my-voip-app", "APNS_VOIP", map[string]string{
		"PlatformCredential": "fake-cred",
		"PlatformPrincipal":  "fake-cert",
	})
	require.NoError(t, err)
	assert.Contains(t, app.PlatformApplicationArn, "APNS_VOIP")
}

// TestAPNSVoIPSandboxPlatformCreated verifies APNS_VOIP_SANDBOX is valid.
func TestAPNSVoIPSandboxPlatformCreated(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	app, err := b.CreatePlatformApplication("my-voip-sandbox", "APNS_VOIP_SANDBOX", map[string]string{
		"PlatformCredential": "fake-cred",
	})
	require.NoError(t, err)
	assert.Contains(t, app.PlatformApplicationArn, "APNS_VOIP_SANDBOX")
}

// TestAllKnownPlatformsAccepted verifies every documented AWS SNS push platform.
func TestAllKnownPlatformsAccepted(t *testing.T) {
	t.Parallel()

	platforms := []string{
		"GCM", "FCM", "APNS", "APNS_SANDBOX",
		"APNS_VOIP", "APNS_VOIP_SANDBOX",
		"ADM", "BAIDU", "WNS", "MPNS",
	}

	b := newTestBackend(t)

	for i, platform := range platforms {
		name := fmt.Sprintf("app-%d", i)
		_, err := b.CreatePlatformApplication(name, platform, map[string]string{
			"PlatformCredential": "cred",
		})
		require.NoErrorf(t, err, "platform %q should be accepted", platform)
	}
}

// TestInvalidPlatformRejected verifies that unknown platforms are rejected.
func TestInvalidPlatformRejected(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	_, err := b.CreatePlatformApplication("bad-app", "BOGUS_PLATFORM", nil)
	require.ErrorIs(t, err, sns.ErrInvalidParameter)
}

// TestFCMPlatformCreatedViaHandler verifies FCM platform creation through the HTTP handler.
func TestFCMPlatformCreatedViaHandler(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandlerPair(t)

	rec := doTestRequest(t, h, url.Values{
		"Action":                   {"CreatePlatformApplication"},
		"Name":                     {"fcm-handler-app"},
		"Platform":                 {"FCM"},
		"Attributes.entry.1.key":   {"PlatformCredential"},
		"Attributes.entry.1.value": {"server-key"},
		"Version":                  {"2010-03-31"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "PlatformApplicationArn")
	assert.Contains(t, rec.Body.String(), "FCM")
}
