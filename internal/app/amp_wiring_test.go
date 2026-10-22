package app

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ampbackend "github.com/blackbirdworks/gopherstack/services/amp"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
)

func TestAMPAlertManagerDeliversToSNS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		snsConfig   string
		wantSubject string
		wantMessage string
	}{
		{
			name:        "templates",
			snsConfig:   "subject: 'page {{ .CommonLabels.alertname }}'\n        message: 'status={{ .Status }}'",
			wantSubject: "page DiskFull",
			wantMessage: "status=firing",
		},
		{
			name:        "default templates",
			snsConfig:   "send_resolved: true",
			wantSubject: "[FIRING:1] DiskFull (a)",
			wantMessage: `[firing] alertname="DiskFull" host="a"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ampH := ampbackend.NewHandler(ampbackend.NewInMemoryBackend("123456789012", "us-east-1"))
			snsB := snsbackend.NewInMemoryBackend()
			wireAMPScraperSources(map[string]service.Registerable{"AMP": ampH, "SNS": snsbackend.NewHandler(snsB)})

			topic, err := snsB.CreateTopic("alerts", nil)
			require.NoError(t, err)

			sub, err := snsB.Subscribe(topic.TopicArn, "email", "oncall@example.com", "")
			require.NoError(t, err)
			_, err = snsB.ConfirmSubscription(topic.TopicArn, sub.SubscriptionArn)
			require.NoError(t, err)

			b := ampH.Backend
			b.SetLifecycleDelay(time.Nanosecond)

			ws, err := b.CreateWorkspace(t.Context(), ampbackend.CreateWorkspaceInput{Alias: "w"})
			require.NoError(t, err)

			def := "alertmanager_config: |\n  route:\n    receiver: oncall\n    group_by: [alertname]\n    group_wait: 1ms\n" +
				"  receivers:\n    - name: oncall\n      sns_configs:\n      - topic_arn: " + topic.TopicArn +
				"\n        " + tt.snsConfig + "\n"
			_, err = b.CreateAlertManagerDefinition(t.Context(), ws.WorkspaceID, "", []byte(def))
			require.NoError(t, err)

			b.Tick(t.Context())

			now := time.Now()
			require.NoError(t, b.ReceiveAlerts(t.Context(), ws.WorkspaceID, []ampbackend.Alert{{
				Labels:   map[string]string{"alertname": "DiskFull", "host": "a"},
				StartsAt: now, EndsAt: now.Add(time.Hour),
			}}))

			var got []snsbackend.EmailDelivery

			require.Eventually(t, func() bool {
				b.Tick(t.Context())

				got = append(got, snsB.DrainEmailDeliveries()...)

				return len(got) > 0
			}, 5*time.Second, 5*time.Millisecond)

			require.Len(t, got, 1)
			assert.Equal(t, "oncall@example.com", got[0].EndpointEmail)
			assert.Equal(t, tt.wantSubject, got[0].Subject)
			assert.Contains(t, got[0].Message, tt.wantMessage)
		})
	}
}
