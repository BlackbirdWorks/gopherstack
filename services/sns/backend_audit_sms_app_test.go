package sns_test

import (
	"testing"

	sns "github.com/blackbirdworks/gopherstack/services/sns"
)

// helpers scoped to sms/application delivery tests.

func auditCreateTopic(t *testing.T, b *sns.InMemoryBackend, name string) string {
	t.Helper()

	topic, err := b.CreateTopic(name, nil)
	if err != nil {
		t.Fatalf("CreateTopic(%q): %v", name, err)
	}

	return topic.TopicArn
}

// auditSubscribe subscribes to a topic. SMS, application, SQS, Lambda, and Firehose
// subscriptions are auto-confirmed by the backend so no ConfirmSubscription call is needed.
func auditSubscribe(t *testing.T, b *sns.InMemoryBackend, topicArn, protocol, endpoint string) string {
	t.Helper()

	sub, err := b.Subscribe(topicArn, protocol, endpoint, "")
	if err != nil {
		t.Fatalf("Subscribe(%q, %q, %q): %v", topicArn, protocol, endpoint, err)
	}

	return sub.SubscriptionArn
}

// auditSubscribeEmail subscribes and explicitly confirms an email endpoint (email
// requires out-of-band confirmation unlike SMS/application).
func auditSubscribeEmail(t *testing.T, b *sns.InMemoryBackend, topicArn, endpoint string) {
	t.Helper()
	sub, err := b.Subscribe(topicArn, "email", endpoint, "")
	if err != nil {
		t.Fatalf("Subscribe email %q: %v", endpoint, err)
	}
	if _, err = b.ConfirmSubscription(topicArn, sub.SubscriptionArn); err != nil {
		t.Fatalf("ConfirmSubscription email: %v", err)
	}
}

func auditPublish(t *testing.T, b *sns.InMemoryBackend, topicArn, message string) {
	t.Helper()
	if _, err := b.Publish(topicArn, message, "", "", nil); err != nil {
		t.Fatalf("Publish: %v", err)
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

// --- SMS topic subscription delivery ---

// TestAudit_SMSTopicDelivery_Delivered confirms an SMS subscription (auto-confirmed)
// receives the published message and the delivery is observable via DrainSMSDeliveries.
func TestAudit_SMSTopicDelivery_Delivered(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	topicArn := auditCreateTopic(t, b, "sms-topic")
	auditSubscribe(t, b, topicArn, "sms", "+15005550006")
	auditPublish(t, b, topicArn, "hello sms")

	deliveries := b.DrainSMSDeliveries()
	if len(deliveries) != 1 {
		t.Fatalf("SMS delivery count = %d, want 1", len(deliveries))
	}

	d := deliveries[0]
	if d.PhoneNumber != "+15005550006" {
		t.Errorf("phone = %q, want +15005550006", d.PhoneNumber)
	}
	if d.Message != "hello sms" {
		t.Errorf("message = %q, want %q", d.Message, "hello sms")
	}
	if d.MessageID == "" {
		t.Error("expected non-empty MessageID")
	}

	// drain is destructive
	if again := b.DrainSMSDeliveries(); len(again) != 0 {
		t.Fatalf("second drain = %d, want 0", len(again))
	}
}

// TestAudit_SMSTopicDelivery_MultipleSubscribers confirms all SMS subscribers
// on a topic receive the message independently.
func TestAudit_SMSTopicDelivery_MultipleSubscribers(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	topicArn := auditCreateTopic(t, b, "multi-sms")
	auditSubscribe(t, b, topicArn, "sms", "+15005550001")
	auditSubscribe(t, b, topicArn, "sms", "+15005550002")
	auditSubscribe(t, b, topicArn, "sms", "+15005550003")
	auditPublish(t, b, topicArn, "broadcast")

	deliveries := b.DrainSMSDeliveries()
	if len(deliveries) != 3 {
		t.Fatalf("SMS delivery count = %d, want 3", len(deliveries))
	}

	phones := make(map[string]bool, 3)
	for _, d := range deliveries {
		phones[d.PhoneNumber] = true
		if d.Message != "broadcast" {
			t.Errorf("phone %s: message = %q, want %q", d.PhoneNumber, d.Message, "broadcast")
		}
	}

	for _, want := range []string{"+15005550001", "+15005550002", "+15005550003"} {
		if !phones[want] {
			t.Errorf("phone %s not in deliveries", want)
		}
	}
}

// TestAudit_SMSTopicDelivery_OptedOut confirms an opted-out number does not
// receive topic publishes (silently dropped, not an error to the publisher).
func TestAudit_SMSTopicDelivery_OptedOut(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	sns.AddOptedOutPhoneNumberForTest(b, "+15005550006")

	topicArn := auditCreateTopic(t, b, "opted-out-sms")
	auditSubscribe(t, b, topicArn, "sms", "+15005550006")
	auditPublish(t, b, topicArn, "should be dropped")

	if deliveries := b.DrainSMSDeliveries(); len(deliveries) != 0 {
		t.Fatalf("expected 0 deliveries for opted-out number, got %d", len(deliveries))
	}
}

// TestAudit_SMSTopicDelivery_SandboxUnverified confirms an unverified sandbox
// number does not receive topic publishes (silently dropped).
func TestAudit_SMSTopicDelivery_SandboxUnverified(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	// Register a phone in sandbox (not yet verified).
	if err := b.CreateSMSSandboxPhoneNumber("+15005550007", "en-US"); err != nil {
		t.Fatalf("CreateSMSSandboxPhoneNumber: %v", err)
	}

	topicArn := auditCreateTopic(t, b, "sandbox-sms")
	auditSubscribe(t, b, topicArn, "sms", "+15005550007")
	auditPublish(t, b, topicArn, "should be dropped")

	if deliveries := b.DrainSMSDeliveries(); len(deliveries) != 0 {
		t.Fatalf("expected 0 deliveries for unverified sandbox number, got %d", len(deliveries))
	}
}

// TestAudit_SMSTopicDelivery_FilterPolicyBlocks confirms that a FilterPolicy on the
// subscription prevents delivery when message attributes do not match.
func TestAudit_SMSTopicDelivery_FilterPolicyBlocks(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	topicArn := auditCreateTopic(t, b, "filter-sms")

	// SMS subscriptions are auto-confirmed; just set the FilterPolicy afterwards.
	subArn := auditSubscribe(t, b, topicArn, "sms", "+15005550009")
	if err := b.SetSubscriptionAttributes(subArn, "FilterPolicy", `{"env":["prod"]}`); err != nil {
		t.Fatalf("SetSubscriptionAttributes: %v", err)
	}

	// Publish with env=staging — should be filtered out.
	attrs := map[string]sns.MessageAttribute{"env": {DataType: "String", StringValue: "staging"}}
	if _, err := b.Publish(topicArn, "filtered message", "", "", attrs); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	if deliveries := b.DrainSMSDeliveries(); len(deliveries) != 0 {
		t.Fatalf("expected 0 deliveries (filtered), got %d", len(deliveries))
	}
}

// --- Application topic subscription delivery ---

// TestAudit_ApplicationTopicDelivery_EnabledEndpoint confirms that an application
// subscription to an enabled endpoint records an ApplicationDelivery on publish.
func TestAudit_ApplicationTopicDelivery_EnabledEndpoint(t *testing.T) {
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
func TestAudit_ApplicationTopicDelivery_DisabledEndpoint(t *testing.T) {
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
func TestAudit_ApplicationTopicDelivery_MultipleEndpoints(t *testing.T) {
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
func TestAudit_ApplicationTopicDelivery_DrainEmptyByDefault(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	if got := b.DrainApplicationDeliveries(); got != nil {
		t.Fatalf("expected nil drain on fresh backend, got %v", got)
	}
}

// TestAudit_MixedProtocols confirms that a topic with mixed-protocol subscriptions
// (SMS + application + email) delivers independently to each protocol.
func TestAudit_MixedProtocols(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	topicArn := auditCreateTopic(t, b, "mixed-proto")

	// SMS subscriber (auto-confirmed)
	auditSubscribe(t, b, topicArn, "sms", "+15005550010")

	// Application subscriber (auto-confirmed)
	epArn := auditCreateAppEndpoint(t, b, "MixedApp", "tok-mixed", nil)
	auditSubscribe(t, b, topicArn, "application", epArn)

	// Email subscriber (requires explicit confirmation)
	auditSubscribeEmail(t, b, topicArn, "user@example.com")

	auditPublish(t, b, topicArn, "mixed message")

	smsDels := b.DrainSMSDeliveries()
	if len(smsDels) != 1 {
		t.Errorf("SMS delivery count = %d, want 1", len(smsDels))
	}

	appDels := b.DrainApplicationDeliveries()
	if len(appDels) != 1 {
		t.Errorf("application delivery count = %d, want 1", len(appDels))
	}

	emailDels := b.DrainEmailDeliveries()
	if len(emailDels) != 1 {
		t.Errorf("email delivery count = %d, want 1", len(emailDels))
	}
}
