package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/ocinotifications"
)

const (
	ociNotifComp = "ocid1.compartment.oc1..aaaa"
	ociNotifBase = "/dashboard/api/v1/oci/notifications"
)

func newOCINotificationsDashboard(t *testing.T) (*dashboard.DashboardHandler, string, string) {
	t.Helper()

	b := ocinotifications.NewInMemoryBackend()
	b.SetTransition(0)

	topic, err := b.CreateTopic(
		ocinotifications.CreateTopicIn{Name: "alerts", Compartment: ociNotifComp},
		"http://localhost:8157",
	)
	require.NoError(t, err)

	sub, err := b.CreateSubscription(ocinotifications.CreateSubscriptionIn{
		TopicID: topic.TopicID, Compartment: ociNotifComp, Protocol: "EMAIL", Endpoint: "ops@example.com",
	}, "http://localhost:8157")
	require.NoError(t, err)

	return dashboard.NewHandler(
		dashboard.Config{OCINotificationsOps: ocinotifications.NewHandler(b)},
	), topic.TopicID, sub.ID
}

func TestOCINotificationsProxy(t *testing.T) {
	t.Parallel()

	api := ociNotifBase + "/api"
	q := "?compartmentId=" + ociNotifComp

	tests := []struct {
		steps func(topic, sub string) []gcpStep
		name  string
	}{
		{name: "read", steps: func(topic, sub string) []gcpStep {
			return []gcpStep{
				{http.MethodGet, ociNotifBase + "/compartments", "", ociNotifComp, 200},
				{http.MethodGet, api + "/topics" + q, "", `"name":"alerts"`, 200},
				{http.MethodGet, api + "/topics/" + topic, "", `"name":"alerts"`, 200},
				{http.MethodGet, api + "/subscriptions" + q, "", `"endpoint":"ops@example.com"`, 200},
				{http.MethodGet, api + "/subscriptions/" + sub, "", `"lifecycleState":"PENDING"`, 200},
			}
		}},
		{name: "deliveries", steps: func(topic, sub string) []gcpStep {
			return []gcpStep{
				{http.MethodGet, api + "/deliveries", "", `"kind":"CONFIRMATION"`, 200},
				{
					http.MethodGet,
					api + "/deliveries",
					"",
					`"confirmationUrl":"http://localhost:8157/20181201/subscriptions/` + sub,
					200,
				},
				{http.MethodGet, api + "/deliveries?topicId=" + topic, "", `"topicName":"alerts"`, 200},
				{http.MethodGet, api + "/deliveries?topicId=ocid1.onstopic.oc1..other", "", "null", 200},
			}
		}},
		{name: "resend", steps: func(_, sub string) []gcpStep {
			return []gcpStep{
				{http.MethodPost, api + "/subscriptions/" + sub + "/resendConfirmation", "", `"id":"` + sub, 200},
				{http.MethodGet, api + "/deliveries", "", `"kind":"CONFIRMATION"`, 200},
			}
		}},
		{name: "rejects", steps: func(topic, sub string) []gcpStep {
			return []gcpStep{
				{http.MethodDelete, api + "/topics/" + topic, "", "", http.StatusMethodNotAllowed},
				{http.MethodPost, api + "/topics/" + topic + "/messages", `{"body":"x"}`, "unsupported", 400},
				{http.MethodPost, api + "/topics", `{}`, "unsupported", 400},
				{http.MethodPost, api + "/subscriptions", `{}`, "unsupported", 400},
				{http.MethodGet, api + "/subscriptions/" + sub + "/unsubscription", "", "unsupported", 400},
				{http.MethodGet, api + "/admin", "", "unsupported", 400},
				{http.MethodGet, api + "/topics/%2e%2e", "", "unsupported", 400},
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, topic, sub := newOCINotificationsDashboard(t)
			runGCPSteps(t, h, tt.steps(topic, sub))
		})
	}

	t.Run("confirm link", func(t *testing.T) {
		t.Parallel()

		h, _, sub := newOCINotificationsDashboard(t)
		rec := httptest.NewRecorder()
		require.NoError(
			t,
			h.Handler()(echo.NewContext(httptest.NewRequest(http.MethodGet, api+"/deliveries", nil), rec)),
		)

		_, rest, ok := strings.Cut(rec.Body.String(), `"confirmationUrl":"`)
		require.True(t, ok)

		link, _, _ := strings.Cut(rest, `"`)
		_, query, _ := strings.Cut(strings.ReplaceAll(link, `\u0026`, "&"), "?")

		runGCPSteps(t, h, []gcpStep{
			{http.MethodGet, api + "/subscriptions/" + sub + "/confirmation?" + query, "", sub, 200},
			{http.MethodGet, api + "/subscriptions/" + sub, "", `"lifecycleState":"ACTIVE"`, 200},
		})
	})

	t.Run("unavailable", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, api+"/topics"+q, nil)
		require.NoError(t, dashboard.NewHandler(dashboard.Config{}).Handler()(echo.NewContext(req, rec)))
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})
}
