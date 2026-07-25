package sns_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIssue8_RedrivePolicyValidated verifies that a malformed ARN is rejected.
func TestRedrivePolicyValidated(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("rdp-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "http",
		"http://example.com", "")
	require.NoError(t, err)

	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "RedrivePolicy",
		`{"deadLetterTargetArn":"not-a-valid-arn"}`)
	require.Error(t, err)
}

// TestIssue8_RedrivePolicyRequiresDeadLetterTargetArn verifies the required field.
func TestRedrivePolicyRequiresDeadLetterTargetArn(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("rdp-req-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "http", "http://example.com", "")
	require.NoError(t, err)

	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "RedrivePolicy",
		`{"noDeadLetterKey":"arn:aws:sqs:us-east-1:000000000000:dlq"}`)
	require.Error(t, err)
}

// TestIssue8_RedrivePolicyValidSQSARN verifies that a valid SQS ARN is accepted.
func TestRedrivePolicyValidSQSARN(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("rdp-valid-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "http", "http://example.com", "")
	require.NoError(t, err)

	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "RedrivePolicy",
		`{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:my-dlq"}`)
	require.NoError(t, err)
}

// TestIssue8_RedrivePolicyRoundTrips verifies the policy is persisted and
// returned via GetSubscriptionAttributes.
func TestRedrivePolicyRoundTrips(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("rdp-rt-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "http", "http://example.com", "")
	require.NoError(t, err)

	policy := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq"}`
	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "RedrivePolicy", policy)
	require.NoError(t, err)

	attrs, err := b.GetSubscriptionAttributes(sub.SubscriptionArn)
	require.NoError(t, err)
	assert.Equal(t, policy, attrs["RedrivePolicy"])
}

// TestSubscriptionDeliveryPolicyRoundTrips verifies that a subscription-level
// DeliveryPolicy can be set and retrieved via Get/SetSubscriptionAttributes.
func TestSubscriptionDeliveryPolicyRoundTrips(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("delivery-policy-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:dp-q", "")
	require.NoError(t, err)

	policy := `{"healthyRetryPolicy":{"numRetries":5,"minDelayTarget":10,"maxDelayTarget":30}}`
	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "DeliveryPolicy", policy)
	require.NoError(t, err)

	attrs, err := b.GetSubscriptionAttributes(sub.SubscriptionArn)
	require.NoError(t, err)
	assert.Equal(t, policy, attrs["DeliveryPolicy"])
}

// TestSubscriptionDeliveryPolicyClear verifies clearing DeliveryPolicy by
// setting it to empty string removes it from the attribute map.
func TestSubscriptionDeliveryPolicyClear(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("dp-clear-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:dp-clear-q", "")
	require.NoError(t, err)

	policy := `{"healthyRetryPolicy":{"numRetries":3}}`
	require.NoError(t, b.SetSubscriptionAttributes(sub.SubscriptionArn, "DeliveryPolicy", policy))
	require.NoError(t, b.SetSubscriptionAttributes(sub.SubscriptionArn, "DeliveryPolicy", ""))

	attrs, err := b.GetSubscriptionAttributes(sub.SubscriptionArn)
	require.NoError(t, err)
	assert.Empty(t, attrs["DeliveryPolicy"])
}

// TestSubscriptionReplayPolicyRoundTrips verifies that a subscription-level
// ReplayPolicy can be set and retrieved.
func TestSubscriptionReplayPolicyRoundTrips(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("replay-policy-topic.fifo", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:rp-q", "")
	require.NoError(t, err)

	policy := `{"replayFromTimestamp":"2024-01-01T00:00:00Z"}`
	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "ReplayPolicy", policy)
	require.NoError(t, err)

	attrs, err := b.GetSubscriptionAttributes(sub.SubscriptionArn)
	require.NoError(t, err)
	assert.Equal(t, policy, attrs["ReplayPolicy"])
}

// TestSubscriptionReplayPolicyClear verifies clearing ReplayPolicy removes it.
func TestSubscriptionReplayPolicyClear(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("rp-clear-topic.fifo", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:rp-clear-q", "")
	require.NoError(t, err)

	policy := `{"replayFromTimestamp":"2024-06-01T00:00:00Z"}`
	require.NoError(t, b.SetSubscriptionAttributes(sub.SubscriptionArn, "ReplayPolicy", policy))
	require.NoError(t, b.SetSubscriptionAttributes(sub.SubscriptionArn, "ReplayPolicy", ""))

	attrs, err := b.GetSubscriptionAttributes(sub.SubscriptionArn)
	require.NoError(t, err)
	assert.Empty(t, attrs["ReplayPolicy"])
}

// TestSubscriptionDeliveryAndReplayPolicyCoexist verifies both attributes can
// be set independently and retrieved together.
func TestSubscriptionDeliveryAndReplayPolicyCoexist(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("coexist-topic.fifo", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:coexist-q", "")
	require.NoError(t, err)

	dp := `{"healthyRetryPolicy":{"numRetries":2}}`
	rp := `{"replayFromTimestamp":"2025-01-01T00:00:00Z"}`

	require.NoError(t, b.SetSubscriptionAttributes(sub.SubscriptionArn, "DeliveryPolicy", dp))
	require.NoError(t, b.SetSubscriptionAttributes(sub.SubscriptionArn, "ReplayPolicy", rp))

	attrs, err := b.GetSubscriptionAttributes(sub.SubscriptionArn)
	require.NoError(t, err)
	assert.Equal(t, dp, attrs["DeliveryPolicy"])
	assert.Equal(t, rp, attrs["ReplayPolicy"])
}

// TestSubscriptionDeliveryPolicyViaHandler verifies DeliveryPolicy flows
// through the HTTP handler (SetSubscriptionAttributes / GetSubscriptionAttributes).
func TestSubscriptionDeliveryPolicyViaHandler(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("handler-dp-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:handler-dp-q", "")
	require.NoError(t, err)

	policy := `{"healthyRetryPolicy":{"numRetries":5,"minDelayTarget":5,"maxDelayTarget":60}}`
	rec := doSNSRequest(t, h, url.Values{
		"Action":          {"SetSubscriptionAttributes"},
		"SubscriptionArn": {sub.SubscriptionArn},
		"AttributeName":   {"DeliveryPolicy"},
		"AttributeValue":  {policy},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSNSRequest(t, h, url.Values{
		"Action":          {"GetSubscriptionAttributes"},
		"SubscriptionArn": {sub.SubscriptionArn},
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "DeliveryPolicy")
	assert.Contains(t, rec2.Body.String(), "numRetries")
}

// TestSubscriptionReplayPolicyViaHandler verifies ReplayPolicy flows
// through the HTTP handler.
func TestSubscriptionReplayPolicyViaHandler(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("handler-rp-topic.fifo", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:handler-rp-q", "")
	require.NoError(t, err)

	policy := `{"replayFromTimestamp":"2025-03-01T00:00:00Z"}`
	rec := doSNSRequest(t, h, url.Values{
		"Action":          {"SetSubscriptionAttributes"},
		"SubscriptionArn": {sub.SubscriptionArn},
		"AttributeName":   {"ReplayPolicy"},
		"AttributeValue":  {policy},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSNSRequest(t, h, url.Values{
		"Action":          {"GetSubscriptionAttributes"},
		"SubscriptionArn": {sub.SubscriptionArn},
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "ReplayPolicy")
	assert.Contains(t, rec2.Body.String(), "replayFromTimestamp")
}

type stubSQSChecker struct {
	existingARNs map[string]bool
}

func (s *stubSQSChecker) QueueExists(_ context.Context, queueARN string) (bool, error) {
	return s.existingARNs[queueARN], nil
}

func TestSetSubscriptionAttributesDLQExistenceCheck(t *testing.T) {
	t.Parallel()

	existingARN := "arn:aws:sqs:us-east-1:123456789012:existing-dlq"
	missingARN := "arn:aws:sqs:us-east-1:123456789012:missing-dlq"

	tests := []struct {
		name       string
		dlqARN     string
		errContain string
		wantErr    bool
	}{
		{
			name:    "existing_queue_accepted",
			dlqARN:  existingARN,
			wantErr: false,
		},
		{
			name:       "missing_queue_rejected",
			dlqARN:     missingARN,
			wantErr:    true,
			errContain: "does not exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			b.SetSQSChecker(&stubSQSChecker{
				existingARNs: map[string]bool{existingARN: true},
			})

			topic, err := b.CreateTopic("check-topic", nil)
			require.NoError(t, err)

			sub, err := b.Subscribe(topic.TopicArn, "http", "http://example.com/notify", "")
			require.NoError(t, err)

			redrivePolicy, _ := json.Marshal(map[string]string{
				"deadLetterTargetArn": tt.dlqARN,
			})
			err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "RedrivePolicy", string(redrivePolicy))

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContain)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSetSubscriptionAttributesDLQNoCheckerSkips(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	// No SQSChecker wired — existence check must be skipped, not panic or error.

	topic, err := b.CreateTopic("no-checker-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(topic.TopicArn, "http", "http://example.com/notify", "")
	require.NoError(t, err)

	redrivePolicy, _ := json.Marshal(map[string]string{
		"deadLetterTargetArn": "arn:aws:sqs:us-east-1:123456789012:any-dlq",
	})
	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "RedrivePolicy", string(redrivePolicy))
	require.NoError(t, err, "no checker means existence check is skipped")
}

func TestSNSHandler_GetSubscriptionAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup            func(b *sns.InMemoryBackend)
		form             url.Values
		name             string
		wantBodyContains []string
		wantStatus       int
	}{
		{
			name: "missing_arn",
			form: url.Values{
				"Action": {"GetSubscriptionAttributes"},
			},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: []string{"InvalidParameter"},
		},
		{
			name: "not_found",
			form: url.Values{
				"Action":          {"GetSubscriptionAttributes"},
				"SubscriptionArn": {"arn:aws:sns:us-east-1:000000000000:t:nonexistent"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "success",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("sub-topic", nil)
				b.Subscribe(
					"arn:aws:sns:us-east-1:000000000000:sub-topic",
					"sqs",
					"arn:aws:sqs:us-east-1:000000000000:q",
					"",
				)
			},
			form: url.Values{
				"Action": {"GetSubscriptionAttributes"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"GetSubscriptionAttributesResult", "SubscriptionArn"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := sns.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}
			h := sns.NewHandler(b)

			form := tt.form
			// For the success case, look up the subscription ARN dynamically.
			if tt.name == "success" {
				subs, _, _ := b.ListSubscriptions("")
				require.NotEmpty(t, subs)
				form.Set("SubscriptionArn", subs[0].SubscriptionArn)
			}

			rec := snsPost(t, h, form)
			assert.Equal(t, tt.wantStatus, rec.Code)
			for _, want := range tt.wantBodyContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestSNSHandler_SetSubscriptionAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup            func(b *sns.InMemoryBackend) string
		form             url.Values
		name             string
		wantBodyContains []string
		wantStatus       int
	}{
		{
			name: "missing_subscription_arn",
			form: url.Values{
				"Action":        {"SetSubscriptionAttributes"},
				"AttributeName": {"RawMessageDelivery"},
			},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: []string{"InvalidParameter"},
		},
		{
			name: "missing_attribute_name",
			form: url.Values{
				"Action":          {"SetSubscriptionAttributes"},
				"SubscriptionArn": {"arn:aws:sns:us-east-1:000000000000:t:x"},
			},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: []string{"InvalidParameter"},
		},
		{
			name: "not_found",
			form: url.Values{
				"Action":          {"SetSubscriptionAttributes"},
				"SubscriptionArn": {"arn:aws:sns:us-east-1:000000000000:t:nonexistent"},
				"AttributeName":   {"RawMessageDelivery"},
				"AttributeValue":  {"true"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "set_raw_message_delivery",
			setup: func(b *sns.InMemoryBackend) string {
				b.CreateTopic("attr-topic", nil)
				sub, _ := b.Subscribe(
					"arn:aws:sns:us-east-1:000000000000:attr-topic",
					"sqs",
					"arn:aws:sqs:us-east-1:000000000000:q",
					"",
				)

				return sub.SubscriptionArn
			},
			form: url.Values{
				"Action":         {"SetSubscriptionAttributes"},
				"AttributeName":  {"RawMessageDelivery"},
				"AttributeValue": {"true"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"SetSubscriptionAttributesResponse"},
		},
		{
			name: "set_redrive_policy",
			setup: func(b *sns.InMemoryBackend) string {
				b.CreateTopic("rdq-topic", nil)
				sub, _ := b.Subscribe(
					"arn:aws:sns:us-east-1:000000000000:rdq-topic",
					"sqs",
					"arn:aws:sqs:us-east-1:000000000000:q",
					"",
				)

				return sub.SubscriptionArn
			},
			form: url.Values{
				"Action":         {"SetSubscriptionAttributes"},
				"AttributeName":  {"RedrivePolicy"},
				"AttributeValue": {`{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq"}`},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"SetSubscriptionAttributesResponse"},
		},
		{
			name: "unknown_attribute_returns_error",
			setup: func(b *sns.InMemoryBackend) string {
				b.CreateTopic("unk-topic", nil)
				sub, _ := b.Subscribe(
					"arn:aws:sns:us-east-1:000000000000:unk-topic",
					"sqs",
					"arn:aws:sqs:us-east-1:000000000000:q",
					"",
				)

				return sub.SubscriptionArn
			},
			form: url.Values{
				"Action":         {"SetSubscriptionAttributes"},
				"AttributeName":  {"UnknownAttribute"},
				"AttributeValue": {"some-value"},
			},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: []string{"InvalidParameter"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := sns.NewInMemoryBackend()
			h := sns.NewHandler(b)

			form := tt.form
			if tt.setup != nil {
				subArn := tt.setup(b)
				form.Set("SubscriptionArn", subArn)
			}

			rec := snsPost(t, h, form)
			assert.Equal(t, tt.wantStatus, rec.Code)
			for _, want := range tt.wantBodyContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

// TestSNS_SetSubscriptionAttributesExtended validates SubscriptionRoleArn and FilterPolicyScope.
func TestSNS_SetSubscriptionAttributesExtended(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		attrName  string
		attrValue string
		wantErr   bool
	}{
		{
			name:      "set_subscription_role_arn",
			attrName:  "SubscriptionRoleArn",
			attrValue: "arn:aws:iam::123456789012:role/MyRole",
		},
		{
			name:      "set_filter_policy_scope_message_body",
			attrName:  "FilterPolicyScope",
			attrValue: "MessageBody",
		},
		{
			name:      "set_filter_policy_scope_message_attributes",
			attrName:  "FilterPolicyScope",
			attrValue: "MessageAttributes",
		},
		{
			name:      "invalid_filter_policy_scope",
			attrName:  "FilterPolicyScope",
			attrValue: "InvalidValue",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			topic, err := b.CreateTopic("ext-attrs-topic", nil)
			require.NoError(t, err)
			sub, err := b.Subscribe(topic.TopicArn, "sqs", "arn:aws:sqs:us-east-1:123:q", "")
			require.NoError(t, err)

			err = b.SetSubscriptionAttributes(sub.SubscriptionArn, tt.attrName, tt.attrValue)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)

			attrs, err := b.GetSubscriptionAttributes(sub.SubscriptionArn)
			require.NoError(t, err)
			assert.Equal(t, tt.attrValue, attrs[tt.attrName])
		})
	}
}

func TestSNS_SetSubscriptionAttributesValidatesRedrivePolicy(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	tp, err := b.CreateTopic("redrive-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "http", "http://example.invalid", "")
	require.NoError(t, err)

	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "RedrivePolicy", `{"deadLetterTargetArn":"not-an-arn"}`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SQS queue ARN")

	err = b.SetSubscriptionAttributes(
		sub.SubscriptionArn,
		"RedrivePolicy",
		`{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq"}`,
	)
	require.NoError(t, err)
}
