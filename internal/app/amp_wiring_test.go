package app

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ampbackend "github.com/blackbirdworks/gopherstack/services/amp"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
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

func TestAMPKeysAndLogsWiring(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	kmsBk := kmsbackend.NewInMemoryBackend()
	logsBk := cwlogsbackend.NewInMemoryBackend()

	live, err := kmsBk.CreateKey(ctx, &kmsbackend.CreateKeyInput{})
	require.NoError(t, err)

	off, err := kmsBk.CreateKey(ctx, &kmsbackend.CreateKeyInput{})
	require.NoError(t, err)
	require.NoError(t, kmsBk.DisableKey(ctx, &kmsbackend.DisableKeyInput{KeyID: off.KeyMetadata.KeyID}))

	_, err = logsBk.CreateLogGroup(ctx, "/amp", "", "")
	require.NoError(t, err)

	keys := ampKeys{kms: kmsBk}
	logs := ampLogs{logs: logsBk}

	tests := []struct {
		name        string
		key         string
		wantExists  bool
		wantEnabled bool
	}{
		{name: "enabled", key: live.KeyMetadata.Arn, wantExists: true, wantEnabled: true},
		{name: "disabled", key: off.KeyMetadata.Arn, wantExists: true},
		{name: "unknown", key: "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-0000-0000-000000000000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			exists, enabled := keys.KMSKeyState(t.Context(), tt.key)
			assert.Equal(t, tt.wantExists, exists)
			assert.Equal(t, tt.wantEnabled, enabled)
		})
	}

	assert.True(t, logs.LogGroupExists(ctx, "us-east-1", "/amp"))
	assert.False(t, logs.LogGroupExists(ctx, "us-east-1", "/other"))

	require.NoError(t, logs.PutLogLine(ctx, "us-east-1", "/amp", "ruler", "one", time.Now()))
	require.NoError(t, logs.PutLogLine(ctx, "us-east-1", "/amp", "ruler", "two", time.Now()))
}

func TestAMPPolicyEvaluator(t *testing.T) {
	t.Parallel()

	const (
		arn   = "arn:aws:aps:us-east-1:123456789012:workspace/ws-1"
		allow = `{"Version":"2012-10-17","Statement":[` +
			`{"Effect":"Allow","Principal":"*","Action":"aps:RemoteWrite","Resource":"*"}]}`
		deny = `{"Version":"2012-10-17","Statement":[` +
			`{"Effect":"Deny","Principal":"*","Action":"aps:*","Resource":"*"}]}`
	)

	tests := []struct {
		name        string
		doc, action string
		allowed     bool
		denied      bool
	}{
		{name: "allow", doc: allow, action: "aps:RemoteWrite", allowed: true},
		{name: "other action", doc: allow, action: "aps:QueryMetrics"},
		{name: "deny", doc: deny, action: "aps:QueryMetrics", denied: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			allowed, denied := ampPolicyEvaluator(tt.doc, tt.action, arn, "123456789012")
			assert.Equal(t, tt.allowed, allowed)
			assert.Equal(t, tt.denied, denied)
		})
	}
}
