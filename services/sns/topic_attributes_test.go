package sns_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testDataProtectionPolicy is a well-formed data protection policy JSON used across
// GetDataProtectionPolicy and PutDataProtectionPolicy tests.
const testDataProtectionPolicy = `{"Version":"2021-06-01","Statement":[]}`

// TestCreateTopicKMSMasterKeyIDValidation verifies that CreateTopic accepts
// well-formed KMS alias/ARN/key-ID values for KmsMasterKeyId and rejects garbage.
func TestCreateTopicKMSMasterKeyIDValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		keyID   string
		wantErr bool
	}{
		{
			name:  "alias_accepted",
			keyID: "alias/aws/sns",
		},
		{
			name:  "arn_accepted",
			keyID: "arn:aws:kms:us-east-1:000000000000:key/abcdef01-1234-5678-9abc-def012345678",
		},
		{
			name:    "garbage_rejected",
			keyID:   "not-a-valid-key-id",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newA1679Backend(t)
			_, err := b.CreateTopic("kms-topic-"+tt.name, map[string]string{
				"KmsMasterKeyId": tt.keyID,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestIssue3_KMSSetTopicAttributeValidation verifies KMS validation via
// SetTopicAttributes.
func TestKMSSetTopicAttributeValidation(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("kms-set-attr-topic", nil)
	require.NoError(t, err)

	// Valid alias.
	err = b.SetTopicAttributes(tp.TopicArn, "KmsMasterKeyId", "alias/my-key")
	require.NoError(t, err)

	// Clearing (empty value) is allowed.
	err = b.SetTopicAttributes(tp.TopicArn, "KmsMasterKeyId", "")
	require.NoError(t, err)

	// Invalid value.
	err = b.SetTopicAttributes(tp.TopicArn, "KmsMasterKeyId", "::bad::")
	require.Error(t, err)
}

// TestTopicAttributeRoundTrips verifies that setting and getting topic
// attributes preserves values for all known configurable attributes.
func TestTopicAttributeRoundTrips(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)

	cases := []struct {
		name  string
		attr  string
		value string
	}{
		{"DisplayName", "DisplayName", "My Test Topic"},
		{"HTTPSuccessFeedbackSampleRate", "HTTPSuccessFeedbackSampleRate", "100"},
		{
			"SQSSuccessFeedbackRoleArn", "SQSSuccessFeedbackRoleArn",
			"arn:aws:iam::000000000000:role/sqs-feedback",
		},
		{
			"LambdaFailureFeedbackRoleArn", "LambdaFailureFeedbackRoleArn",
			"arn:aws:iam::000000000000:role/lambda-feedback",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tp, err := b.CreateTopic("attr-rt-"+strings.ToLower(tc.name), nil)
			require.NoError(t, err)

			err = b.SetTopicAttributes(tp.TopicArn, tc.attr, tc.value)
			require.NoError(t, err)

			attrs, err := b.GetTopicAttributes(tp.TopicArn)
			require.NoError(t, err)
			assert.Equal(t, tc.value, attrs[tc.attr])
		})
	}
}

func TestInMemoryBackend_GetTopicAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		setup    func(b *sns.InMemoryBackend)
		name     string
		topicArn string
	}{
		{
			name: "success",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("attr-topic", nil)
			},
			topicArn: "arn:aws:sns:us-east-1:000000000000:attr-topic",
		},
		{
			name:     "not found",
			topicArn: "arn:aws:sns:us-east-1:000000000000:missing",
			wantErr:  sns.ErrTopicNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := sns.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}
			attrs, err := b.GetTopicAttributes(tt.topicArn)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.topicArn, attrs["TopicArn"])
			// Verify computed attributes are always present.
			assert.NotEmpty(t, attrs["EffectiveDeliveryPolicy"], "EffectiveDeliveryPolicy should be set")
			assert.Equal(t, "0", attrs["SubscriptionsDeleted"], "SubscriptionsDeleted should default to 0")
			assert.Equal(t, "0", attrs["SubscriptionsConfirmed"])
			assert.Equal(t, "0", attrs["SubscriptionsPending"])
		})
	}
}

func TestInMemoryBackend_SetTopicAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(b *sns.InMemoryBackend)
		name      string
		topicArn  string
		attrName  string
		attrValue string
	}{
		{
			name: "success",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("set-topic", nil)
			},
			topicArn:  "arn:aws:sns:us-east-1:000000000000:set-topic",
			attrName:  "DisplayName",
			attrValue: "Hello",
		},
		{
			name: "sqs_success_feedback_sample_rate",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("feedback-topic", nil)
			},
			topicArn:  "arn:aws:sns:us-east-1:000000000000:feedback-topic",
			attrName:  "SQSSuccessFeedbackSampleRate",
			attrValue: "50",
		},
		{
			name: "http_success_feedback_sample_rate",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("http-feedback-topic", nil)
			},
			topicArn:  "arn:aws:sns:us-east-1:000000000000:http-feedback-topic",
			attrName:  "HTTPSuccessFeedbackSampleRate",
			attrValue: "100",
		},
		{
			name: "lambda_failure_feedback_role_arn",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("lambda-feedback-topic", nil)
			},
			topicArn:  "arn:aws:sns:us-east-1:000000000000:lambda-feedback-topic",
			attrName:  "LambdaFailureFeedbackRoleArn",
			attrValue: "arn:aws:iam::000000000000:role/sns-feedback",
		},
		{
			name: "unknown_attribute_rejected",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("unknown-attr-topic", nil)
			},
			topicArn:  "arn:aws:sns:us-east-1:000000000000:unknown-attr-topic",
			attrName:  "NotARealAttribute",
			attrValue: "value",
			wantErr:   sns.ErrInvalidParameter,
		},
		{
			name:      "not found",
			topicArn:  "arn:aws:sns:us-east-1:000000000000:missing",
			attrName:  "X",
			attrValue: "Y",
			wantErr:   sns.ErrTopicNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := sns.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}
			err := b.SetTopicAttributes(tt.topicArn, tt.attrName, tt.attrValue)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			attrs, err := b.GetTopicAttributes(tt.topicArn)
			require.NoError(t, err)
			assert.Equal(t, tt.attrValue, attrs[tt.attrName])
		})
	}
}

func TestSNSHandler_GetTopicAttributes(t *testing.T) {
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
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("attr-topic", nil)
			},
			form: url.Values{
				"Action":   {"GetTopicAttributes"},
				"Version":  {"2010-03-31"},
				"TopicArn": {"arn:aws:sns:us-east-1:000000000000:attr-topic"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"TopicArn"},
		},
		{
			name: "not found",
			form: url.Values{
				"Action":   {"GetTopicAttributes"},
				"Version":  {"2010-03-31"},
				"TopicArn": {"arn:aws:sns:us-east-1:000000000000:missing"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing ARN",
			form: url.Values{
				"Action":  {"GetTopicAttributes"},
				"Version": {"2010-03-31"},
			},
			wantStatus: http.StatusBadRequest,
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
			rec := snsPost(t, h, tt.form)
			assert.Equal(t, tt.wantStatus, rec.Code)
			for _, want := range tt.wantBodyContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestSNSHandler_SetTopicAttributes(t *testing.T) {
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
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("set-topic", nil)
			},
			form: url.Values{
				"Action":         {"SetTopicAttributes"},
				"Version":        {"2010-03-31"},
				"TopicArn":       {"arn:aws:sns:us-east-1:000000000000:set-topic"},
				"AttributeName":  {"DisplayName"},
				"AttributeValue": {"My Topic"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found",
			form: url.Values{
				"Action":        {"SetTopicAttributes"},
				"Version":       {"2010-03-31"},
				"TopicArn":      {"arn:aws:sns:us-east-1:000000000000:missing"},
				"AttributeName": {"X"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing params",
			form: url.Values{
				"Action":  {"SetTopicAttributes"},
				"Version": {"2010-03-31"},
			},
			wantStatus: http.StatusBadRequest,
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
			rec := snsPost(t, h, tt.form)
			assert.Equal(t, tt.wantStatus, rec.Code)
			for _, want := range tt.wantBodyContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

// TestSNS_GetDataProtectionPolicy validates the GetDataProtectionPolicy operation.
func TestSNS_GetDataProtectionPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(b *sns.InMemoryBackend) string
		name       string
		wantPolicy string
		wantStatus int
	}{
		{
			name: "empty_policy",
			setup: func(b *sns.InMemoryBackend) string {
				tp, _ := b.CreateTopic("policy-topic", nil)

				return tp.TopicArn
			},
			wantStatus: http.StatusOK,
			wantPolicy: "",
		},
		{
			name: "with_policy",
			setup: func(b *sns.InMemoryBackend) string {
				tp, _ := b.CreateTopic("policy-topic2", nil)
				require.NoError(t, b.SetTopicAttributes(tp.TopicArn, "DataProtectionPolicy", testDataProtectionPolicy))

				return tp.TopicArn
			},
			wantStatus: http.StatusOK,
			wantPolicy: testDataProtectionPolicy,
		},
		{
			name: "topic_not_found",
			setup: func(_ *sns.InMemoryBackend) string {
				return "arn:aws:sns:us-east-1:000000000000:missing"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			topicArn := tt.setup(b)

			form := url.Values{
				"Action":      {"GetDataProtectionPolicy"},
				"Version":     {"2010-03-31"},
				"ResourceArn": {topicArn},
			}

			rec := snsPost(t, h, form)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantPolicy != "" {
				// Decode the XML response and check the DataProtectionPolicy field directly.
				var resp sns.GetDataProtectionPolicyResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantPolicy, resp.GetDataProtectionPolicyResult.DataProtectionPolicy)
			}
		})
	}
}

// TestSNS_PutDataProtectionPolicy validates the PutDataProtectionPolicy operation end-to-end.
func TestSNS_PutDataProtectionPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		topicName  string
		policy     string
		wantStatus int
	}{
		{
			name:       "set_and_get",
			topicName:  "pp-topic",
			policy:     testDataProtectionPolicy,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_resource_arn",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)

			var topicArn string

			if tt.topicName != "" {
				tp, err := b.CreateTopic(tt.topicName, nil)
				require.NoError(t, err)
				topicArn = tp.TopicArn
			}

			rec := snsPost(t, h, url.Values{
				"Action":               {"PutDataProtectionPolicy"},
				"Version":              {"2010-03-31"},
				"ResourceArn":          {topicArn},
				"DataProtectionPolicy": {tt.policy},
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				got, err := b.GetDataProtectionPolicy(topicArn)
				require.NoError(t, err)
				assert.Equal(t, tt.policy, got)
			}
		})
	}
}

// TestSNS_PutDataProtectionPolicyJSONValidation validates that PutDataProtectionPolicy
// rejects non-JSON policy strings.
func TestSNS_PutDataProtectionPolicyJSONValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		policy  string
		wantErr bool
	}{
		{
			name:   "valid_json",
			policy: `{"Version":"2021-06-01","Statement":[]}`,
		},
		{
			name:   "empty_policy_allowed",
			policy: "",
		},
		{
			name:    "invalid_json_rejected",
			policy:  `not json`,
			wantErr: true,
		},
		{
			name:    "partial_json_rejected",
			policy:  `{"Version":`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			tp, err := b.CreateTopic("json-policy-topic", nil)
			require.NoError(t, err)

			err = b.PutDataProtectionPolicy(tp.TopicArn, tt.policy)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)

			got, err := b.GetDataProtectionPolicy(tp.TopicArn)
			require.NoError(t, err)
			assert.Equal(t, tt.policy, got)
		})
	}
}

// TestSNS_GetTopicAttributesComputed validates that GetTopicAttributes returns computed attributes.
func TestSNS_GetTopicAttributesComputed(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	topic, err := b.CreateTopic("computed-attrs-topic", nil)
	require.NoError(t, err)

	// Before any subscriptions: counts should be zero.
	attrs, err := b.GetTopicAttributes(topic.TopicArn)
	require.NoError(t, err)
	assert.Equal(t, "0", attrs["SubscriptionsConfirmed"])
	assert.Equal(t, "0", attrs["SubscriptionsPending"])
	assert.NotEmpty(t, attrs["Owner"])

	// Add an http subscription (PendingConfirmation=true) and an sqs subscription (confirmed).
	_, err = b.Subscribe(topic.TopicArn, "http", "https://example.com", "")
	require.NoError(t, err)
	_, err = b.Subscribe(topic.TopicArn, "sqs", "arn:aws:sqs:us-east-1:123:queue", "")
	require.NoError(t, err)

	attrs, err = b.GetTopicAttributes(topic.TopicArn)
	require.NoError(t, err)
	assert.Equal(t, "1", attrs["SubscriptionsConfirmed"])
	assert.Equal(t, "1", attrs["SubscriptionsPending"])
}

func TestSNS_CreateTopicRejectsInvalidKmsMasterKeyId(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	_, err := b.CreateTopic("kms-topic", map[string]string{"KmsMasterKeyId": "??not-valid??"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KmsMasterKeyId")

	// Valid alias is accepted.
	_, err = b.CreateTopic("kms-topic", map[string]string{"KmsMasterKeyId": "alias/aws/sns"})
	require.NoError(t, err)
}
