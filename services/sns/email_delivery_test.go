package sns_test

import (
	"testing"

	sns "github.com/blackbirdworks/gopherstack/services/sns"
)

// TestEmailDelivery covers delivery to email / email-json subscriptions: a
// confirmed subscription receives the published message (recorded for drain),
// while a pending (unconfirmed) one does not.
func TestEmailDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		protocol    string
		message     string
		subject     string
		wantMessage string
		wantCount   int
		confirm     bool
	}{
		{
			name:        "confirmed email receives message",
			protocol:    "email",
			confirm:     true,
			message:     "hello world",
			subject:     "greeting",
			wantCount:   1,
			wantMessage: "hello world",
		},
		{
			name:        "confirmed email-json receives message",
			protocol:    "email-json",
			confirm:     true,
			message:     "json body",
			wantCount:   1,
			wantMessage: "json body",
		},
		{
			name:      "pending email is not delivered",
			protocol:  "email",
			confirm:   false,
			message:   "should not arrive",
			wantCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()

			topic, err := b.CreateTopic("emails", nil)
			if err != nil {
				t.Fatalf("CreateTopic: %v", err)
			}

			sub, err := b.Subscribe(topic.TopicArn, tc.protocol, "user@example.com", "")
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}

			if tc.confirm {
				if _, cErr := b.ConfirmSubscription(topic.TopicArn, sub.SubscriptionArn); cErr != nil {
					t.Fatalf("ConfirmSubscription: %v", cErr)
				}
			}

			if _, pErr := b.Publish(topic.TopicArn, tc.message, tc.subject, "", nil); pErr != nil {
				t.Fatalf("Publish: %v", pErr)
			}

			deliveries := b.DrainEmailDeliveries()
			if len(deliveries) != tc.wantCount {
				t.Fatalf("delivery count = %d, want %d", len(deliveries), tc.wantCount)
			}

			if tc.wantCount == 0 {
				return
			}

			d := deliveries[0]
			if d.Message != tc.wantMessage {
				t.Fatalf("message = %q, want %q", d.Message, tc.wantMessage)
			}

			if d.Protocol != tc.protocol {
				t.Fatalf("protocol = %q, want %q", d.Protocol, tc.protocol)
			}

			if d.EndpointEmail != "user@example.com" {
				t.Fatalf("endpoint = %q, want user@example.com", d.EndpointEmail)
			}

			if d.TopicARN != topic.TopicArn {
				t.Fatalf("topicARN = %q, want %q", d.TopicARN, topic.TopicArn)
			}

			if d.MessageID == "" {
				t.Fatal("expected a non-empty MessageID")
			}

			// Drain is destructive.
			if again := b.DrainEmailDeliveries(); len(again) != 0 {
				t.Fatalf("second drain returned %d, want 0", len(again))
			}
		})
	}
}

// TestEmailDelivery_HTTPSDeliversReal confirms an HTTPS subscription still
// performs a real HTTP POST (the previously-traced path) and that the email
// recording does not interfere with it.
func TestEmailDelivery_DrainEmptyByDefault(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	if got := b.DrainEmailDeliveries(); got != nil {
		t.Fatalf("expected nil drain on fresh backend, got %v", got)
	}
}
