package sns_test

import (
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSNSHandler_CreatePlatformEndpoint(t *testing.T) {
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
				"Token": {"device-token-123"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"CreatePlatformEndpointResponse", "endpoint/GCM/MyApp"},
		},
		{
			name: "with_custom_user_data",
			setup: func(b *sns.InMemoryBackend) string {
				app, _ := b.CreatePlatformApplication("MyApp", "GCM", nil)

				return app.PlatformApplicationArn
			},
			formOverrides: url.Values{
				"Token":          {"device-token-456"},
				"CustomUserData": {"my-custom-data"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"CreatePlatformEndpointResponse"},
		},
		{
			name:          "missing_arn",
			formOverrides: url.Values{"Token": {"tok"}, "PlatformApplicationArn": {""}},
			wantStatus:    http.StatusBadRequest,
		},
		{
			name: "missing_token",
			formOverrides: url.Values{
				"PlatformApplicationArn": {"arn:aws:sns:us-east-1:000000000000:app/GCM/App"},
				"Token":                  {""},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "app_not_found",
			formOverrides: url.Values{
				"PlatformApplicationArn": {"arn:aws:sns:us-east-1:000000000000:app/GCM/none"},
				"Token":                  {"tok"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, b := newTestHandler(t)
			form := url.Values{"Action": {"CreatePlatformEndpoint"}}

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

func TestSNSHandler_GetEndpointAttributes(t *testing.T) {
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
				ep, _ := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "my-token", nil)

				return ep.EndpointArn
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"GetEndpointAttributesResponse", "my-token"},
		},
		{
			name:          "missing_arn",
			formOverrides: url.Values{"EndpointArn": {""}},
			wantStatus:    http.StatusBadRequest,
		},
		{
			name: "not_found",
			formOverrides: url.Values{
				"EndpointArn": {"arn:aws:sns:us-east-1:000000000000:endpoint/GCM/App/nonexistent"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, b := newTestHandler(t)
			form := url.Values{"Action": {"GetEndpointAttributes"}}

			if tt.setup != nil {
				form.Set("EndpointArn", tt.setup(b))
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

func TestSNSHandler_SetEndpointAttributes(t *testing.T) {
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
				ep, _ := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "tok", nil)

				return ep.EndpointArn
			},
			formOverrides: url.Values{
				"Attributes.entry.1.key":   {"Enabled"},
				"Attributes.entry.1.value": {"false"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"SetEndpointAttributesResponse"},
		},
		{
			name:          "missing_arn",
			formOverrides: url.Values{"EndpointArn": {""}},
			wantStatus:    http.StatusBadRequest,
		},
		{
			name: "not_found",
			formOverrides: url.Values{
				"EndpointArn": {"arn:aws:sns:us-east-1:000000000000:endpoint/GCM/App/none"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, b := newTestHandler(t)
			form := url.Values{"Action": {"SetEndpointAttributes"}}

			if tt.setup != nil {
				form.Set("EndpointArn", tt.setup(b))
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

func TestSNSHandler_ListEndpointsByPlatformApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup            func(b *sns.InMemoryBackend) string
		formOverrides    url.Values
		name             string
		wantBodyContains []string
		wantStatus       int
	}{
		{
			name: "success_empty",
			setup: func(b *sns.InMemoryBackend) string {
				app, _ := b.CreatePlatformApplication("MyApp", "GCM", nil)

				return app.PlatformApplicationArn
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"ListEndpointsByPlatformApplicationResponse"},
		},
		{
			name: "with_endpoints",
			setup: func(b *sns.InMemoryBackend) string {
				app, _ := b.CreatePlatformApplication("MyApp", "GCM", nil)
				b.CreatePlatformEndpoint(app.PlatformApplicationArn, "tok1", nil)
				b.CreatePlatformEndpoint(app.PlatformApplicationArn, "tok2", nil)

				return app.PlatformApplicationArn
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"ListEndpointsByPlatformApplicationResponse", "tok1", "tok2"},
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
			form := url.Values{"Action": {"ListEndpointsByPlatformApplication"}}

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

func TestSNSHandler_DeleteEndpoint(t *testing.T) {
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
				ep, _ := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "tok", nil)

				return ep.EndpointArn
			},
			wantStatus: http.StatusOK,
		},
		{
			name:          "missing_arn",
			formOverrides: url.Values{"EndpointArn": {""}},
			wantStatus:    http.StatusBadRequest,
		},
		{
			name: "not_found",
			formOverrides: url.Values{
				"EndpointArn": {"arn:aws:sns:us-east-1:000000000000:endpoint/GCM/App/none"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, b := newTestHandler(t)
			form := url.Values{"Action": {"DeleteEndpoint"}}

			if tt.setup != nil {
				form.Set("EndpointArn", tt.setup(b))
			}

			maps.Copy(form, tt.formOverrides)

			rec := snsPost(t, h, form)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestInMemoryBackend_PlatformEndpointLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *sns.InMemoryBackend)
		name string
	}{
		{
			name: "create_and_get",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				app, _ := b.CreatePlatformApplication("App", "GCM", nil)
				ep, err := b.CreatePlatformEndpoint(
					app.PlatformApplicationArn,
					"dev-token",
					map[string]string{"CustomData": "some-data"},
				)
				require.NoError(t, err)
				assert.Contains(t, ep.EndpointArn, "endpoint/GCM/App")

				attrs, err := b.GetEndpointAttributes(ep.EndpointArn)
				require.NoError(t, err)
				assert.Equal(t, "dev-token", attrs["Token"])
				assert.Equal(t, "true", attrs["Enabled"])
				assert.Equal(t, "some-data", attrs["CustomData"])
			},
		},
		{
			name: "create_app_not_found",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				_, err := b.CreatePlatformEndpoint("arn:aws:sns:us-east-1:000000000000:app/GCM/none", "tok", nil)
				require.ErrorIs(t, err, sns.ErrPlatformApplicationNotFound)
			},
		},
		{
			name: "set_attributes",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				app, _ := b.CreatePlatformApplication("App", "GCM", nil)
				ep, _ := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "tok", nil)

				err := b.SetEndpointAttributes(ep.EndpointArn, map[string]string{"Enabled": "false"})
				require.NoError(t, err)

				attrs, err := b.GetEndpointAttributes(ep.EndpointArn)
				require.NoError(t, err)
				assert.Equal(t, "false", attrs["Enabled"])
			},
		},
		{
			name: "list_by_app",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				app, _ := b.CreatePlatformApplication("App", "GCM", nil)
				app2, _ := b.CreatePlatformApplication("App2", "GCM", nil)
				b.CreatePlatformEndpoint(app.PlatformApplicationArn, "tok1", nil)
				b.CreatePlatformEndpoint(app.PlatformApplicationArn, "tok2", nil)
				b.CreatePlatformEndpoint(app2.PlatformApplicationArn, "tok3", nil)

				eps, _, err := b.ListEndpointsByPlatformApplication(app.PlatformApplicationArn, "")
				require.NoError(t, err)
				require.Len(t, eps, 2)
			},
		},
		{
			name: "delete_endpoint",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				app, _ := b.CreatePlatformApplication("App", "GCM", nil)
				ep, _ := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "tok", nil)

				err := b.DeleteEndpoint(ep.EndpointArn)
				require.NoError(t, err)

				_, err = b.GetEndpointAttributes(ep.EndpointArn)
				require.ErrorIs(t, err, sns.ErrEndpointNotFound)
			},
		},
		{
			name: "delete_endpoint_not_found",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				err := b.DeleteEndpoint("arn:aws:sns:us-east-1:000000000000:endpoint/GCM/App/none")
				require.ErrorIs(t, err, sns.ErrEndpointNotFound)
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

// TestSNS_PlatformEndpointInheritsAppRegion verifies that platform endpoint ARNs
// inherit the region from their parent platform application, not the backend default.
func TestSNS_PlatformEndpointInheritsAppRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		appRegion string
	}{
		{name: "app in eu-west-1", appRegion: "eu-west-1"},
		{name: "app in ap-northeast-1", appRegion: "ap-northeast-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			app, err := b.CreatePlatformApplicationInRegion("myapp", "GCM", tt.appRegion, nil)
			require.NoError(t, err)

			ep, err := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "device-token-xyz", nil)
			require.NoError(t, err)

			assert.Contains(t, ep.EndpointArn, ":"+tt.appRegion+":")
		})
	}
}

func auditCreateAppEndpoint(
	t *testing.T,
	b *sns.InMemoryBackend,
	appName, token string,
	attrs map[string]string,
) string {
	t.Helper()

	app, err := b.CreatePlatformApplication(appName, "GCM", nil)
	if err != nil {
		t.Fatalf("CreatePlatformApplication: %v", err)
	}

	ep, epErr := b.CreatePlatformEndpoint(app.PlatformApplicationArn, token, attrs)
	if epErr != nil {
		t.Fatalf("CreatePlatformEndpoint: %v", epErr)
	}

	return ep.EndpointArn
}

// TestAudit_ApplicationTopicDelivery_EnabledEndpoint confirms that an application
// subscription to an enabled endpoint records an ApplicationDelivery on publish.
func TestApplicationTopicDeliveryEnabledEndpoint(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	epArn := auditCreateAppEndpoint(t, b, "AuditApp", "tok-enabled", nil)
	topicArn := auditCreateTopic(t, b, "app-topic")
	auditSubscribe(t, b, topicArn, "application", epArn)
	auditPublish(t, b, topicArn, "push message")

	deliveries := b.DrainApplicationDeliveries()
	if len(deliveries) != 1 {
		t.Fatalf("application delivery count = %d, want 1", len(deliveries))
	}

	d := deliveries[0]
	if d.EndpointARN != epArn {
		t.Errorf("endpointARN = %q, want %q", d.EndpointARN, epArn)
	}
	if d.Message != "push message" {
		t.Errorf("message = %q, want %q", d.Message, "push message")
	}
	if d.MessageID == "" {
		t.Error("expected non-empty MessageID")
	}

	// drain is destructive
	if again := b.DrainApplicationDeliveries(); len(again) != 0 {
		t.Fatalf("second drain = %d, want 0", len(again))
	}
}

// TestAudit_ApplicationTopicDelivery_DisabledEndpoint confirms that publishing to
// a topic with a disabled endpoint subscription silently skips delivery (no panic,
// no publisher error, no delivery recorded).
func TestApplicationTopicDeliveryDisabledEndpoint(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	// CreatePlatformEndpoint always starts Enabled=true; disable after creation.
	epArn := auditCreateAppEndpoint(t, b, "DisabledApp", "tok-disabled", nil)
	if err := b.SetEndpointAttributes(epArn, map[string]string{"Enabled": "false"}); err != nil {
		t.Fatalf("SetEndpointAttributes: %v", err)
	}
	topicArn := auditCreateTopic(t, b, "disabled-app-topic")
	auditSubscribe(t, b, topicArn, "application", epArn)
	auditPublish(t, b, topicArn, "push to disabled")

	if deliveries := b.DrainApplicationDeliveries(); len(deliveries) != 0 {
		t.Fatalf("expected 0 deliveries for disabled endpoint, got %d", len(deliveries))
	}
}

// TestAudit_ApplicationTopicDelivery_MultipleEndpoints confirms all enabled endpoints
// receive the publish.
func TestApplicationTopicDeliveryMultipleEndpoints(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	app, err := b.CreatePlatformApplication("MultiApp", "GCM", nil)
	if err != nil {
		t.Fatalf("CreatePlatformApplication: %v", err)
	}

	epArns := make([]string, 3)
	for i, token := range []string{"token-a", "token-b", "token-c"} {
		ep, epErr := b.CreatePlatformEndpoint(app.PlatformApplicationArn, token, nil)
		if epErr != nil {
			t.Fatalf("CreatePlatformEndpoint(%q): %v", token, epErr)
		}
		epArns[i] = ep.EndpointArn
	}

	topicArn := auditCreateTopic(t, b, "multi-app-topic")
	for _, epArn := range epArns {
		auditSubscribe(t, b, topicArn, "application", epArn)
	}
	auditPublish(t, b, topicArn, "fan-out")

	deliveries := b.DrainApplicationDeliveries()
	if len(deliveries) != 3 {
		t.Fatalf("application delivery count = %d, want 3", len(deliveries))
	}

	seen := make(map[string]bool, 3)
	for _, d := range deliveries {
		seen[d.EndpointARN] = true
		if d.Message != "fan-out" {
			t.Errorf("endpoint %s: message = %q, want fan-out", d.EndpointARN, d.Message)
		}
	}
	for _, want := range epArns {
		if !seen[want] {
			t.Errorf("endpoint %s not in deliveries", want)
		}
	}
}

// TestAudit_ApplicationTopicDelivery_DrainEmptyByDefault confirms DrainApplicationDeliveries
// returns nil on a fresh backend.
func TestApplicationTopicDeliveryDrainEmptyByDefault(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	if got := b.DrainApplicationDeliveries(); got != nil {
		t.Fatalf("expected nil drain on fresh backend, got %v", got)
	}
}

// TestPublishToDisabledEndpointFails verifies that publishing to an
// endpoint with Enabled=false returns ErrEndpointDisabled.
func TestPublishToDisabledEndpointFails(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	app, err := b.CreatePlatformApplication("push-app", "GCM", map[string]string{
		"PlatformCredential": "server-key",
	})
	require.NoError(t, err)

	ep, err := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "device-token-abc", nil)
	require.NoError(t, err)

	// Disable the endpoint.
	require.NoError(t, b.SetEndpointAttributes(ep.EndpointArn, map[string]string{
		"Enabled": "false",
	}))

	_, err = b.PublishToTargetArn(ep.EndpointArn, "hello", "", nil)
	require.ErrorIs(t, err, sns.ErrEndpointDisabled)
}

// TestPublishToEnabledEndpointSucceeds verifies that a re-enabled
// endpoint (Enabled=true) accepts messages.
func TestPublishToEnabledEndpointSucceeds(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	app, err := b.CreatePlatformApplication("push-app-en", "APNS", map[string]string{
		"PlatformCredential": "cert-pem",
		"PlatformPrincipal":  "key-pem",
	})
	require.NoError(t, err)

	ep, err := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "device-apns-token", nil)
	require.NoError(t, err)

	// Disable then re-enable.
	require.NoError(t, b.SetEndpointAttributes(ep.EndpointArn, map[string]string{"Enabled": "false"}))
	require.NoError(t, b.SetEndpointAttributes(ep.EndpointArn, map[string]string{"Enabled": "true"}))

	msgID, err := b.PublishToTargetArn(ep.EndpointArn, "payload", "", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, msgID)
}

// TestEndpointDisabledViaHandler verifies that the HTTP handler
// returns an EndpointDisabled error code.
func TestEndpointDisabledViaHandler(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerPair(t)
	app, err := b.CreatePlatformApplication("handler-push-app", "FCM", map[string]string{
		"PlatformCredential": "key",
	})
	require.NoError(t, err)

	ep, err := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "token-xyz", nil)
	require.NoError(t, err)

	require.NoError(t, b.SetEndpointAttributes(ep.EndpointArn, map[string]string{"Enabled": "false"}))

	rec := doTestRequest(t, h, url.Values{
		"Action":    {"Publish"},
		"TargetArn": {ep.EndpointArn},
		"Message":   {"push payload"},
		"Version":   {"2010-03-31"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "EndpointDisabled")
}

// TestEndpointCreatedEventFired verifies that creating an endpoint
// fires EventEndpointCreated to the configured topic.
func TestEndpointCreatedEventFired(t *testing.T) {
	t.Parallel()

	received := make(chan string, 5)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newTestBackend(t)

	// Create the event notification topic.
	eventTopic, err := b.CreateTopic("endpoint-events", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(eventTopic.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)

	// Create a platform application that fires to eventTopic on endpoint creation.
	app, err := b.CreatePlatformApplication("event-app", "GCM", map[string]string{
		"PlatformCredential":   "server-key",
		"EventEndpointCreated": eventTopic.TopicArn,
	})
	require.NoError(t, err)

	// Create an endpoint — should trigger EventEndpointCreated.
	_, err = b.CreatePlatformEndpoint(app.PlatformApplicationArn, "device-token-event", nil)
	require.NoError(t, err)

	select {
	case raw := <-received:
		env := parseNotificationEnvelope(t, raw)
		assert.Contains(t, env.Message, "EndpointCreated")
		assert.Contains(t, env.Message, "device-token-event")
	case <-time.After(2 * time.Second):
		t.Fatal("EventEndpointCreated notification was not fired")
	}
}

// TestEndpointDeletedEventFired verifies that deleting an endpoint
// fires EventEndpointDeleted to the configured topic.
func TestEndpointDeletedEventFired(t *testing.T) {
	t.Parallel()

	received := make(chan string, 5)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newTestBackend(t)

	eventTopic, err := b.CreateTopic("delete-events", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(eventTopic.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)

	app, err := b.CreatePlatformApplication("delete-event-app", "APNS", map[string]string{
		"PlatformCredential":   "cert",
		"PlatformPrincipal":    "key",
		"EventEndpointDeleted": eventTopic.TopicArn,
	})
	require.NoError(t, err)

	ep, err := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "del-token", nil)
	require.NoError(t, err)

	// Drain the creation event if EventEndpointCreated is not configured.
	// Then delete.
	require.NoError(t, b.DeleteEndpoint(ep.EndpointArn))

	select {
	case raw := <-received:
		env := parseNotificationEnvelope(t, raw)
		assert.Contains(t, env.Message, "EndpointDeleted")
	case <-time.After(2 * time.Second):
		t.Fatal("EventEndpointDeleted notification was not fired")
	}
}

// TestEndpointUpdatedEventFired verifies that updating endpoint
// attributes fires EventEndpointUpdated.
func TestEndpointUpdatedEventFired(t *testing.T) {
	t.Parallel()

	received := make(chan string, 5)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newTestBackend(t)

	eventTopic, err := b.CreateTopic("update-events", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(eventTopic.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)

	app, err := b.CreatePlatformApplication("update-event-app", "FCM", map[string]string{
		"PlatformCredential":   "server-key",
		"EventEndpointUpdated": eventTopic.TopicArn,
	})
	require.NoError(t, err)

	ep, err := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "upd-token", nil)
	require.NoError(t, err)

	// Update endpoint attributes — should fire EventEndpointUpdated.
	require.NoError(t, b.SetEndpointAttributes(ep.EndpointArn, map[string]string{
		"CustomUserData": "user-123",
	}))

	select {
	case raw := <-received:
		env := parseNotificationEnvelope(t, raw)
		assert.Contains(t, env.Message, "EndpointUpdated")
	case <-time.After(2 * time.Second):
		t.Fatal("EventEndpointUpdated notification was not fired")
	}
}

// TestEventFiredToNonexistentTopicSilent verifies that a missing event
// topic does not cause endpoint operations to fail.
func TestEventFiredToNonexistentTopicSilent(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	app, err := b.CreatePlatformApplication("no-event-topic-app", "GCM", map[string]string{
		"PlatformCredential":   "key",
		"EventEndpointCreated": "arn:aws:sns:us-east-1:000000000000:nonexistent-topic",
	})
	require.NoError(t, err)

	// This must succeed even though the event topic doesn't exist.
	ep, err := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "silent-token", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, ep.EndpointArn)
}

// TestNoEventConfiguredNoFire verifies that endpoint operations complete
// normally when no event topics are configured on the platform application.
func TestNoEventConfiguredNoFire(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	app, err := b.CreatePlatformApplication("no-event-app", "ADM", map[string]string{
		"PlatformCredential": "client-id",
	})
	require.NoError(t, err)

	ep, err := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "no-event-token", nil)
	require.NoError(t, err)

	require.NoError(t, b.SetEndpointAttributes(ep.EndpointArn, map[string]string{
		"Enabled": "true",
	}))
	require.NoError(t, b.DeleteEndpoint(ep.EndpointArn))
}

// TestEndpointActiveCounts verifies that GetPlatformApplicationAttributes
// returns accurate EndpointActive and EndpointDisabled counts.
func TestEndpointActiveCounts(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	app, err := b.CreatePlatformApplication("count-app", "GCM", map[string]string{
		"PlatformCredential": "key",
	})
	require.NoError(t, err)

	// Create 3 endpoints.
	ep1, err := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "tok1", nil)
	require.NoError(t, err)
	ep2, err := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "tok2", nil)
	require.NoError(t, err)
	ep3, err := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "tok3", nil)
	require.NoError(t, err)

	// Initially all active.
	attrs, err := b.GetPlatformApplicationAttributes(app.PlatformApplicationArn)
	require.NoError(t, err)
	assert.Equal(t, "3", attrs["EndpointActive"])
	assert.Equal(t, "0", attrs["EndpointDisabled"])

	// Disable two.
	require.NoError(t, b.SetEndpointAttributes(ep1.EndpointArn, map[string]string{"Enabled": "false"}))
	require.NoError(t, b.SetEndpointAttributes(ep2.EndpointArn, map[string]string{"Enabled": "false"}))
	_ = ep3

	attrs, err = b.GetPlatformApplicationAttributes(app.PlatformApplicationArn)
	require.NoError(t, err)
	assert.Equal(t, "1", attrs["EndpointActive"])
	assert.Equal(t, "2", attrs["EndpointDisabled"])
}

// TestEndpointCountsViaHandler verifies the counts are returned by the handler.
func TestEndpointCountsViaHandler(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerPair(t)
	app, err := b.CreatePlatformApplication("count-handler-app", "FCM", map[string]string{
		"PlatformCredential": "key",
	})
	require.NoError(t, err)

	ep, err := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "count-tok", nil)
	require.NoError(t, err)
	require.NoError(t, b.SetEndpointAttributes(ep.EndpointArn, map[string]string{"Enabled": "false"}))

	rec := doTestRequest(t, h, url.Values{
		"Action":                 {"GetPlatformApplicationAttributes"},
		"PlatformApplicationArn": {app.PlatformApplicationArn},
		"Version":                {"2010-03-31"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "EndpointActive")
	assert.Contains(t, rec.Body.String(), "EndpointDisabled")
}

// TestEventAttributesStoredOnCreate verifies that event topic ARNs
// set at CreatePlatformApplication time are returned via GetPlatformApplicationAttributes.
func TestEventAttributesStoredOnCreate(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	eventTopicArn := "arn:aws:sns:us-east-1:000000000000:app-events"

	app, err := b.CreatePlatformApplication("attrs-app", "GCM", map[string]string{
		"PlatformCredential":        "server-key",
		"EventEndpointCreated":      eventTopicArn,
		"EventEndpointDeleted":      eventTopicArn,
		"EventEndpointUpdated":      eventTopicArn,
		"EventEndpointFailure":      eventTopicArn,
		"EventDeliveryFailure":      eventTopicArn,
		"SuccessFeedbackRoleArn":    "arn:aws:iam::000000000000:role/success-role",
		"FailureFeedbackRoleArn":    "arn:aws:iam::000000000000:role/failure-role",
		"SuccessFeedbackSampleRate": "50",
	})
	require.NoError(t, err)

	attrs, err := b.GetPlatformApplicationAttributes(app.PlatformApplicationArn)
	require.NoError(t, err)

	assert.Equal(t, eventTopicArn, attrs["EventEndpointCreated"])
	assert.Equal(t, eventTopicArn, attrs["EventEndpointDeleted"])
	assert.Equal(t, eventTopicArn, attrs["EventEndpointUpdated"])
	assert.Equal(t, eventTopicArn, attrs["EventEndpointFailure"])
	assert.Equal(t, eventTopicArn, attrs["EventDeliveryFailure"])
	assert.Equal(t, "arn:aws:iam::000000000000:role/success-role", attrs["SuccessFeedbackRoleArn"])
	assert.Equal(t, "50", attrs["SuccessFeedbackSampleRate"])
}

// TestEventAttributesUpdatableViaSet verifies that event attributes can
// be updated via SetPlatformApplicationAttributes.
func TestEventAttributesUpdatableViaSet(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	app, err := b.CreatePlatformApplication("set-event-app", "FCM", map[string]string{
		"PlatformCredential": "key",
	})
	require.NoError(t, err)

	newEventArn := "arn:aws:sns:us-east-1:000000000000:new-events"
	require.NoError(t, b.SetPlatformApplicationAttributes(app.PlatformApplicationArn, map[string]string{
		"EventEndpointCreated": newEventArn,
	}))

	attrs, err := b.GetPlatformApplicationAttributes(app.PlatformApplicationArn)
	require.NoError(t, err)
	assert.Equal(t, newEventArn, attrs["EventEndpointCreated"])
}
