package ocinotifications_test

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	oci "github.com/blackbirdworks/gopherstack/services/ocinotifications"
)

func TestRecordedDeliveryLogsWouldSend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		protocol string
		endpoint string
		wantTo   bool
	}{
		{name: "email", protocol: "EMAIL", endpoint: "ops@example.com", wantTo: true},
		{name: "sms", protocol: "SMS", endpoint: "+15555550100", wantTo: true},
		{name: "slack", protocol: "SLACK", endpoint: "https://hooks.slack.com/services/T0/B0/secretvalue"},
		{
			name:     "pagerduty",
			protocol: "PAGERDUTY",
			endpoint: "https://events.pagerduty.com/integration/secretkey/enqueue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{register: true})
			topic := e.mkTopic(t, "alerts")

			var buf bytes.Buffer

			ctx := logger.Save(t.Context(), slog.New(slog.NewTextHandler(&buf, nil)))

			_, err := e.backend.CreateSubscription(ctx, oci.CreateSubscriptionIn{
				TopicID: *topic.TopicId, Compartment: testCompartment, Protocol: tt.protocol, Endpoint: tt.endpoint,
			}, e.base)
			require.NoError(t, err)

			out := buf.String()
			if tt.protocol == "PAGERDUTY" {
				assert.NotContains(t, out, "would send")

				_, err = e.backend.PublishMessage(ctx, *topic.TopicId, "", oci.PublishIn{Body: new("hi")})
				require.NoError(t, err)

				out = buf.String()
			}

			assert.Contains(t, out, "would send notification")
			assert.Contains(t, out, "protocol="+tt.protocol)
			assert.NotContains(t, out, "secret")

			if tt.wantTo {
				assert.Contains(t, out, tt.endpoint)
			}
		})
	}
}
