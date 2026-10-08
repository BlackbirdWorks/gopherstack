package sns_test

import (
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/services/sns"
)

type snsTally struct {
	counts map[string]float64
	mu     sync.Mutex
}

func (s *snsTally) emit(p cwmetric.Point) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.counts[p.Name] += p.Value

	return nil
}

func (s *snsTally) get(name string) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.counts[name]
}

func TestPublish_SMSAndApplicationOutcomeMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, b *sns.InMemoryBackend, topicArn string)
		name       string
		wantFailed float64
		wantOK     float64
	}{
		{
			name: "sms_delivered",
			setup: func(t *testing.T, b *sns.InMemoryBackend, topicArn string) {
				t.Helper()
				_, err := b.Subscribe(topicArn, "sms", "+15005550001", "")
				require.NoError(t, err)
			},
			wantOK: 1,
		},
		{
			name: "sms_opted_out",
			setup: func(t *testing.T, b *sns.InMemoryBackend, topicArn string) {
				t.Helper()
				sns.AddOptedOutPhoneNumberForTest(b, "+15005550006")
				_, err := b.Subscribe(topicArn, "sms", "+15005550006", "")
				require.NoError(t, err)
			},
			wantFailed: 1,
		},
		{
			name: "application_enabled",
			setup: func(t *testing.T, b *sns.InMemoryBackend, topicArn string) {
				t.Helper()
				ep := newAppEndpoint(t, b, nil)
				_, err := b.Subscribe(topicArn, "application", ep, "")
				require.NoError(t, err)
			},
			wantOK: 1,
		},
		{
			name: "application_disabled",
			setup: func(t *testing.T, b *sns.InMemoryBackend, topicArn string) {
				t.Helper()
				ep := newAppEndpoint(t, b, map[string]string{"Enabled": "false"})
				_, err := b.Subscribe(topicArn, "application", ep, "")
				require.NoError(t, err)
			},
			wantFailed: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tally := &snsTally{counts: map[string]float64{}}
			b := sns.NewInMemoryBackend()
			b.SetMetricEmitter(cwmetric.EmitterFunc(tally.emit))

			topic, err := b.CreateTopic("t", nil)
			require.NoError(t, err)

			tt.setup(t, b, topic.TopicArn)

			_, err = b.Publish(topic.TopicArn, "hello", "", "", nil)
			require.NoError(t, err)

			assert.InDelta(t, tt.wantOK, tally.get("NumberOfNotificationsDelivered"), 0)
			assert.InDelta(t, tt.wantFailed, tally.get("NumberOfNotificationsFailed"), 0)
		})
	}
}

func newAppEndpoint(t *testing.T, b *sns.InMemoryBackend, attrs map[string]string) string {
	t.Helper()

	app, err := b.CreatePlatformApplication("app", "GCM", map[string]string{"PlatformCredential": "k"})
	require.NoError(t, err)

	ep, err := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "device-token", nil)
	require.NoError(t, err)

	if len(attrs) > 0 {
		require.NoError(t, b.SetEndpointAttributes(ep.EndpointArn, attrs))
	}

	return ep.EndpointArn
}

func TestUnsubscribe_AuthenticateOnUnsubscribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		flag    bool
		wantErr bool
	}{
		{name: "flag_blocks_unsigned_unsubscribe", flag: true, wantErr: true},
		{name: "no_flag_allows_unsubscribe"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			client := newTestSNSClient(t, h)

			topic, err := client.CreateTopic(t.Context(), &snssdk.CreateTopicInput{Name: aws.String("t")})
			require.NoError(t, err)

			sub, err := client.Subscribe(t.Context(), &snssdk.SubscribeInput{
				TopicArn: topic.TopicArn, Protocol: aws.String("http"), Endpoint: aws.String("http://127.0.0.1:1/x"),
				ReturnSubscriptionArn: true,
			})
			require.NoError(t, err)

			_, err = client.ConfirmSubscription(t.Context(), &snssdk.ConfirmSubscriptionInput{
				TopicArn: topic.TopicArn, Token: aws.String("tok"),
				AuthenticateOnUnsubscribe: aws.String(map[bool]string{true: "true", false: "false"}[tt.flag]),
			})
			require.NoError(t, err)

			_, err = client.Unsubscribe(t.Context(), &snssdk.UnsubscribeInput{SubscriptionArn: sub.SubscriptionArn})

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "AuthorizationError")
				require.Error(t, b.UnsubscribeAs(aws.ToString(sub.SubscriptionArn), ""), "unsigned stays rejected")
				require.Error(
					t,
					b.UnsubscribeAs(aws.ToString(sub.SubscriptionArn), "999999999999"),
					"a stranger's account is rejected",
				)
				require.NoError(
					t,
					b.UnsubscribeAs(aws.ToString(sub.SubscriptionArn), config.DefaultAccountID),
					"the owner may unsubscribe",
				)

				return
			}

			assert.NoError(t, err)
		})
	}
}
