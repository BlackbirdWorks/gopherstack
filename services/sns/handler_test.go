package sns_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/blackbirdworks/gopherstack/services/sns"
)

// testDataProtectionPolicy is a well-formed data protection policy JSON used across
// GetDataProtectionPolicy and PutDataProtectionPolicy tests.
const testDataProtectionPolicy = `{"Version":"2021-06-01","Statement":[]}`

// snsPost is a helper that sends a form-encoded SNS request to the handler.
func snsPost(t *testing.T, h *sns.Handler, form url.Values) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	body := form.Encode()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// mustCreateTopic is a helper that creates a topic and returns its ARN.
func mustCreateTopic(t *testing.T, b *sns.InMemoryBackend, name string) string {
	t.Helper()

	topic, err := b.CreateTopic(name, nil)
	require.NoError(t, err)

	return topic.TopicArn
}

// mustSubscribe is a helper that creates a subscription and returns its ARN.
func mustSubscribe(t *testing.T, b *sns.InMemoryBackend, topicArn, protocol, endpoint string) string {
	t.Helper()

	sub, err := b.Subscribe(topicArn, protocol, endpoint, "")
	require.NoError(t, err)

	return sub.SubscriptionArn
}

// newTestHandler creates a handler and backend pair for testing.
func newTestHandler(t *testing.T) (*sns.Handler, *sns.InMemoryBackend) {
	t.Helper()
	b := sns.NewInMemoryBackend()

	return sns.NewHandler(b), b
}

// extractSNSHTTPMessage extracts the Message field from a standard SNS HTTP notification
// JSON envelope. If the body is not a JSON object with a "Message" field, the raw body is
// returned as-is. This allows tests to work correctly regardless of RawMessageDelivery setting.
func extractSNSHTTPMessage(body string) string {
	var env struct {
		Message string `json:"Message"`
		Type    string `json:"Type"`
	}
	if err := json.Unmarshal([]byte(body), &env); err == nil && env.Type == "Notification" {
		return env.Message
	}

	return body
}

// ---------------------------------------------------------------------------
// Backend tests
// ---------------------------------------------------------------------------

func TestInMemoryBackend_CreateTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(b *sns.InMemoryBackend)
		attrs     map[string]string
		wantAttr  map[string]string
		name      string
		topicName string
		wantArn   string
	}{
		{
			name:      "success",
			topicName: "my-topic",
			attrs:     map[string]string{"DisplayName": "My Topic"},
			wantArn:   "arn:aws:sns:us-east-1:000000000000:my-topic",
			wantAttr:  map[string]string{"DisplayName": "My Topic"},
		},
		{
			name: "duplicate",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("dup-topic", nil)
			},
			topicName: "dup-topic",
			// AWS CreateTopic is idempotent: no error, returns existing ARN.
			wantArn: "arn:aws:sns:us-east-1:000000000000:dup-topic",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := sns.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}
			topic, err := b.CreateTopic(tt.topicName, tt.attrs)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantArn, topic.TopicArn)
			for k, v := range tt.wantAttr {
				assert.Equal(t, v, topic.Attributes[k])
			}
		})
	}
}

func TestInMemoryBackend_DeleteTopic(t *testing.T) {
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
				b.CreateTopic("del-topic", nil)
			},
			topicArn: "arn:aws:sns:us-east-1:000000000000:del-topic",
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
			err := b.DeleteTopic(tt.topicArn)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Empty(t, b.ListAllTopics())
		})
	}
}

func TestInMemoryBackend_ListTopics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(b *sns.InMemoryBackend)
		name      string
		token     string
		wantCount int
		wantNext  bool
	}{
		{
			name:      "empty",
			token:     "",
			wantCount: 0,
			wantNext:  false,
		},
		{
			name: "with items",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("topic-a", nil)
				b.CreateTopic("topic-b", nil)
			},
			token:     "",
			wantCount: 2,
			wantNext:  false,
		},
		{
			name:    "invalid token",
			token:   "not-base64!!!",
			wantErr: sns.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := sns.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}
			topics, next, err := b.ListTopics(tt.token)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Len(t, topics, tt.wantCount)
			if tt.wantNext {
				assert.NotEmpty(t, next)
			} else {
				assert.Empty(t, next)
			}
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

func TestInMemoryBackend_Subscribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		setup    func(b *sns.InMemoryBackend)
		name     string
		topicArn string
		protocol string
		endpoint string
	}{
		{
			name: "success",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("sub-topic", nil)
			},
			topicArn: "arn:aws:sns:us-east-1:000000000000:sub-topic",
			protocol: "sqs",
			endpoint: "arn:aws:sqs:us-east-1:000000000000:q",
		},
		{
			name:     "topic not found",
			topicArn: "arn:aws:sns:us-east-1:000000000000:missing",
			protocol: "sqs",
			endpoint: "x",
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
			sub, err := b.Subscribe(tt.topicArn, tt.protocol, tt.endpoint, "")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Contains(t, sub.SubscriptionArn, "sub-topic")
			assert.Equal(t, tt.topicArn, sub.TopicArn)
		})
	}
}

func TestInMemoryBackend_Unsubscribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(b *sns.InMemoryBackend) string
		name    string
		subArn  string
	}{
		{
			name: "success",
			setup: func(b *sns.InMemoryBackend) string {
				tp, _ := b.CreateTopic("unsub-topic", nil)
				sub, _ := b.Subscribe(tp.TopicArn, "sqs", "x", "")

				return sub.SubscriptionArn
			},
		},
		{
			name:    "not found",
			subArn:  "arn:aws:sns:us-east-1:000000000000:x:missing",
			wantErr: sns.ErrSubscriptionNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := sns.NewInMemoryBackend()
			arn := tt.subArn
			if tt.setup != nil {
				arn = tt.setup(b)
			}
			err := b.Unsubscribe(arn)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Empty(t, b.ListAllSubscriptions())
		})
	}
}

func TestInMemoryBackend_ListSubscriptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(b *sns.InMemoryBackend)
		name      string
		wantCount int
	}{
		{
			name:      "empty",
			wantCount: 0,
		},
		{
			name: "with items",
			setup: func(b *sns.InMemoryBackend) {
				tp, _ := b.CreateTopic("ls-topic", nil)
				b.Subscribe(tp.TopicArn, "sqs", "x", "")
				b.Subscribe(tp.TopicArn, "https", "https://example.com", "")
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := sns.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}
			subs, next, err := b.ListSubscriptions("")
			require.NoError(t, err)
			assert.Len(t, subs, tt.wantCount)
			assert.Empty(t, next)
		})
	}
}

func TestInMemoryBackend_ListSubscriptionsByTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(b *sns.InMemoryBackend)
		name      string
		topicArn  string
		wantCount int
	}{
		{
			name: "success",
			setup: func(b *sns.InMemoryBackend) {
				tp1, _ := b.CreateTopic("lstt-1", nil)
				tp2, _ := b.CreateTopic("lstt-2", nil)
				b.Subscribe(tp1.TopicArn, "sqs", "x", "")
				b.Subscribe(tp2.TopicArn, "sqs", "y", "")
			},
			topicArn:  "arn:aws:sns:us-east-1:000000000000:lstt-1",
			wantCount: 1,
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
			subs, _, err := b.ListSubscriptionsByTopic(tt.topicArn, "")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Len(t, subs, tt.wantCount)
			assert.Equal(t, tt.topicArn, subs[0].TopicArn)
		})
	}
}

func TestInMemoryBackend_TopicSubscriptionCounts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup              func(t *testing.T, b *sns.InMemoryBackend) string
		name               string
		wantConfirmedCount string
		wantPendingCount   string
	}{
		{
			name: "counts include confirmed and pending subscriptions",
			setup: func(t *testing.T, b *sns.InMemoryBackend) string {
				t.Helper()
				topicArn := mustCreateTopic(t, b, "count-topic-1")
				mustSubscribe(t, b, topicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:count-topic-1")
				mustSubscribe(t, b, topicArn, "http", "http://localhost:12345")

				return topicArn
			},
			wantConfirmedCount: "1",
			wantPendingCount:   "1",
		},
		{
			name: "unsubscribed subscription is excluded from counts",
			setup: func(t *testing.T, b *sns.InMemoryBackend) string {
				t.Helper()
				topicArn := mustCreateTopic(t, b, "count-topic-2")
				subArn := mustSubscribe(t, b, topicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:count-topic-2")
				err := b.Unsubscribe(subArn)
				require.NoError(t, err)

				return topicArn
			},
			wantConfirmedCount: "0",
			wantPendingCount:   "0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			topicArn := tt.setup(t, b)

			attrs, err := b.GetTopicAttributes(topicArn)
			require.NoError(t, err)
			assert.Equal(t, tt.wantConfirmedCount, attrs["SubscriptionsConfirmed"])
			assert.Equal(t, tt.wantPendingCount, attrs["SubscriptionsPending"])
		})
	}
}

func TestInMemoryBackend_Publish(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		setup    func(b *sns.InMemoryBackend)
		name     string
		topicArn string
		message  string
		subject  string
	}{
		{
			name: "success",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("pub-topic", nil)
			},
			topicArn: "arn:aws:sns:us-east-1:000000000000:pub-topic",
			message:  "hello",
			subject:  "subject",
		},
		{
			name:     "topic not found",
			topicArn: "arn:aws:sns:us-east-1:000000000000:missing",
			message:  "x",
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
			msgID, err := b.Publish(tt.topicArn, tt.message, tt.subject, "", nil)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.NotEmpty(t, msgID)
		})
	}
}

func TestInMemoryBackend_ListAll(t *testing.T) {
	t.Parallel()

	t.Run("topics", func(t *testing.T) {
		t.Parallel()
		b := sns.NewInMemoryBackend()
		mustCreateTopic(t, b, "z-topic")
		mustCreateTopic(t, b, "a-topic")
		all := b.ListAllTopics()
		require.Len(t, all, 2)
		assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:a-topic", all[0].TopicArn)
		assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:z-topic", all[1].TopicArn)
	})

	t.Run("subscriptions", func(t *testing.T) {
		t.Parallel()
		b := sns.NewInMemoryBackend()
		arn := mustCreateTopic(t, b, "la-topic")
		mustSubscribe(t, b, arn, "sqs", "q")
		all := b.ListAllSubscriptions()
		require.Len(t, all, 1)
		assert.Equal(t, arn, all[0].TopicArn)
	})
}

// ---------------------------------------------------------------------------
// Handler tests
// ---------------------------------------------------------------------------

func TestSNSHandler_CreateTopic(t *testing.T) {
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
			form: url.Values{
				"Action":  {"CreateTopic"},
				"Version": {"2010-03-31"},
				"Name":    {"test-topic"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"test-topic"},
		},
		{
			name: "missing name",
			form: url.Values{
				"Action":  {"CreateTopic"},
				"Version": {"2010-03-31"},
			},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: []string{"InvalidParameter"},
		},
		{
			name: "duplicate",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("dup", nil)
			},
			form: url.Values{
				"Action":  {"CreateTopic"},
				"Version": {"2010-03-31"},
				"Name":    {"dup"},
			},
			// AWS CreateTopic is idempotent: duplicate name returns existing ARN (200 OK).
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"dup"},
		},
		{
			name: "with attributes",
			form: url.Values{
				"Action":                   {"CreateTopic"},
				"Version":                  {"2010-03-31"},
				"Name":                     {"attrs-topic"},
				"Attributes.entry.1.key":   {"DisplayName"},
				"Attributes.entry.1.value": {"My Display Name"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"attrs-topic"},
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

func TestSNSHandler_DeleteTopic(t *testing.T) {
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
				b.CreateTopic("del-topic", nil)
			},
			form: url.Values{
				"Action":   {"DeleteTopic"},
				"Version":  {"2010-03-31"},
				"TopicArn": {"arn:aws:sns:us-east-1:000000000000:del-topic"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found",
			form: url.Values{
				"Action":   {"DeleteTopic"},
				"Version":  {"2010-03-31"},
				"TopicArn": {"arn:aws:sns:us-east-1:000000000000:missing"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing ARN",
			form: url.Values{
				"Action":  {"DeleteTopic"},
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

func TestSNSHandler_ListTopics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup            func(b *sns.InMemoryBackend)
		form             url.Values
		name             string
		wantBodyContains []string
		wantStatus       int
	}{
		{
			name: "empty",
			form: url.Values{
				"Action":  {"ListTopics"},
				"Version": {"2010-03-31"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "with items",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("t1", nil)
			},
			form: url.Values{
				"Action":  {"ListTopics"},
				"Version": {"2010-03-31"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"t1"},
		},
		{
			name: "invalid token",
			form: url.Values{
				"Action":    {"ListTopics"},
				"Version":   {"2010-03-31"},
				"NextToken": {"!!!not-base64"},
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

func TestSNSHandler_Subscribe(t *testing.T) {
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
				b.CreateTopic("sub-topic", nil)
			},
			form: url.Values{
				"Action":   {"Subscribe"},
				"Version":  {"2010-03-31"},
				"TopicArn": {"arn:aws:sns:us-east-1:000000000000:sub-topic"},
				"Protocol": {"sqs"},
				"Endpoint": {"arn:aws:sqs:us-east-1:000000000000:q"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"SubscriptionArn"},
		},
		{
			name: "with filter policy",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("filter-topic", nil)
			},
			form: url.Values{
				"Action":                   {"Subscribe"},
				"Version":                  {"2010-03-31"},
				"TopicArn":                 {"arn:aws:sns:us-east-1:000000000000:filter-topic"},
				"Protocol":                 {"sqs"},
				"Endpoint":                 {"arn:aws:sqs:us-east-1:000000000000:q"},
				"Attributes.entry.1.key":   {"FilterPolicy"},
				"Attributes.entry.1.value": {`{"store":["example"]}`},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found",
			form: url.Values{
				"Action":   {"Subscribe"},
				"Version":  {"2010-03-31"},
				"TopicArn": {"arn:aws:sns:us-east-1:000000000000:missing"},
				"Protocol": {"sqs"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing params",
			form: url.Values{
				"Action":  {"Subscribe"},
				"Version": {"2010-03-31"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid protocol",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("proto-topic", nil)
			},
			form: url.Values{
				"Action":   {"Subscribe"},
				"Version":  {"2010-03-31"},
				"TopicArn": {"arn:aws:sns:us-east-1:000000000000:proto-topic"},
				"Protocol": {"ftp"},
				"Endpoint": {"ftp://example.com"},
			},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: []string{"InvalidParameter"},
		},
		{
			name: "pending confirmation http",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("http-topic", nil)
			},
			form: url.Values{
				"Action":   {"Subscribe"},
				"Version":  {"2010-03-31"},
				"TopicArn": {"arn:aws:sns:us-east-1:000000000000:http-topic"},
				"Protocol": {"http"},
				"Endpoint": {"http://example.com/notify"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"pending confirmation"},
		},
		{
			name: "pending confirmation https",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("https-topic", nil)
			},
			form: url.Values{
				"Action":   {"Subscribe"},
				"Version":  {"2010-03-31"},
				"TopicArn": {"arn:aws:sns:us-east-1:000000000000:https-topic"},
				"Protocol": {"https"},
				"Endpoint": {"https://example.com/notify"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"pending confirmation"},
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

func TestSNSHandler_Unsubscribe(t *testing.T) {
	t.Parallel()

	// Success case requires dynamic subscription ARN, so it's an individual subtest.
	t.Run("success", func(t *testing.T) {
		t.Parallel()
		h, b := newTestHandler(t)
		arn := mustCreateTopic(t, b, "unsub-topic")
		subArn := mustSubscribe(t, b, arn, "sqs", "q")
		rec := snsPost(t, h, url.Values{
			"Action":          {"Unsubscribe"},
			"Version":         {"2010-03-31"},
			"SubscriptionArn": {subArn},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	tests := []struct {
		form             url.Values
		name             string
		wantBodyContains []string
		wantStatus       int
	}{
		{
			name: "not found",
			form: url.Values{
				"Action":          {"Unsubscribe"},
				"Version":         {"2010-03-31"},
				"SubscriptionArn": {"arn:aws:sns:us-east-1:000000000000:x:missing"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing ARN",
			form: url.Values{
				"Action":  {"Unsubscribe"},
				"Version": {"2010-03-31"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newTestHandler(t)
			rec := snsPost(t, h, tt.form)
			assert.Equal(t, tt.wantStatus, rec.Code)
			for _, want := range tt.wantBodyContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestSNSHandler_ListSubscriptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup            func(b *sns.InMemoryBackend)
		form             url.Values
		name             string
		wantBodyContains []string
		wantStatus       int
	}{
		{
			name: "empty",
			form: url.Values{
				"Action":  {"ListSubscriptions"},
				"Version": {"2010-03-31"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "with items",
			setup: func(b *sns.InMemoryBackend) {
				tp, _ := b.CreateTopic("ls-topic", nil)
				b.Subscribe(tp.TopicArn, "sqs", "q", "")
			},
			form: url.Values{
				"Action":  {"ListSubscriptions"},
				"Version": {"2010-03-31"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"sqs"},
		},
		{
			name: "invalid token",
			form: url.Values{
				"Action":    {"ListSubscriptions"},
				"Version":   {"2010-03-31"},
				"NextToken": {"!!!not-base64"},
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

func TestSNSHandler_ListSubscriptionsByTopic(t *testing.T) {
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
				tp, _ := b.CreateTopic("lstt", nil)
				b.Subscribe(tp.TopicArn, "sqs", "q", "")
			},
			form: url.Values{
				"Action":   {"ListSubscriptionsByTopic"},
				"Version":  {"2010-03-31"},
				"TopicArn": {"arn:aws:sns:us-east-1:000000000000:lstt"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"lstt"},
		},
		{
			name: "not found",
			form: url.Values{
				"Action":   {"ListSubscriptionsByTopic"},
				"Version":  {"2010-03-31"},
				"TopicArn": {"arn:aws:sns:us-east-1:000000000000:missing"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing ARN",
			form: url.Values{
				"Action":  {"ListSubscriptionsByTopic"},
				"Version": {"2010-03-31"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid token",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("tkn-topic", nil)
			},
			form: url.Values{
				"Action":    {"ListSubscriptionsByTopic"},
				"Version":   {"2010-03-31"},
				"TopicArn":  {"arn:aws:sns:us-east-1:000000000000:tkn-topic"},
				"NextToken": {"!!!not-base64"},
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

func TestSNSHandler_Publish(t *testing.T) {
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
				b.CreateTopic("pub-topic", nil)
			},
			form: url.Values{
				"Action":   {"Publish"},
				"Version":  {"2010-03-31"},
				"TopicArn": {"arn:aws:sns:us-east-1:000000000000:pub-topic"},
				"Message":  {"hello world"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"MessageId"},
		},
		{
			name: "with attributes",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("pub-attr-topic", nil)
			},
			form: url.Values{
				"Action":                         {"Publish"},
				"Version":                        {"2010-03-31"},
				"TopicArn":                       {"arn:aws:sns:us-east-1:000000000000:pub-attr-topic"},
				"Message":                        {"hello"},
				"Subject":                        {"test"},
				"MessageAttributes.entry.1.Name": {"attr1"},
				"MessageAttributes.entry.1.Value.DataType":    {"String"},
				"MessageAttributes.entry.1.Value.StringValue": {"val1"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found",
			form: url.Values{
				"Action":   {"Publish"},
				"Version":  {"2010-03-31"},
				"TopicArn": {"arn:aws:sns:us-east-1:000000000000:missing"},
				"Message":  {"x"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing params",
			form: url.Values{
				"Action":  {"Publish"},
				"Version": {"2010-03-31"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "with message structure JSON",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("ms-handler-topic", nil)
			},
			form: url.Values{
				"Action":           {"Publish"},
				"Version":          {"2010-03-31"},
				"TopicArn":         {"arn:aws:sns:us-east-1:000000000000:ms-handler-topic"},
				"Message":          {`{"default":"hello","sqs":"sqs-specific"}`},
				"MessageStructure": {"json"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"MessageId"},
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

func TestSNSHandler_PublishBatch(t *testing.T) {
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
				b.CreateTopic("batch-topic", nil)
			},
			form: url.Values{
				"Action":                                 {"PublishBatch"},
				"Version":                                {"2010-03-31"},
				"TopicArn":                               {"arn:aws:sns:us-east-1:000000000000:batch-topic"},
				"PublishBatchRequestEntries.member.1.Id": {"msg1"},
				"PublishBatchRequestEntries.member.1.Message": {"hello"},
				"PublishBatchRequestEntries.member.2.Id":      {"msg2"},
				"PublishBatchRequestEntries.member.2.Message": {"world"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"msg1", "msg2"},
		},
		{
			name: "missing topic ARN",
			form: url.Values{
				"Action":  {"PublishBatch"},
				"Version": {"2010-03-31"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing entries",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("be-topic", nil)
			},
			form: url.Values{
				"Action":   {"PublishBatch"},
				"Version":  {"2010-03-31"},
				"TopicArn": {"arn:aws:sns:us-east-1:000000000000:be-topic"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "partial failure",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("pf-topic", nil)
			},
			form: url.Values{
				"Action":                                 {"PublishBatch"},
				"Version":                                {"2010-03-31"},
				"TopicArn":                               {"arn:aws:sns:us-east-1:000000000000:pf-topic"},
				"PublishBatchRequestEntries.member.1.Id": {"ok"},
				"PublishBatchRequestEntries.member.1.Message": {"hello"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"Successful"},
		},
		{
			name: "topic not found",
			form: url.Values{
				"Action":                                 {"PublishBatch"},
				"Version":                                {"2010-03-31"},
				"TopicArn":                               {"arn:aws:sns:us-east-1:000000000000:nonexistent-topic"},
				"PublishBatchRequestEntries.member.1.Id": {"fail1"},
				"PublishBatchRequestEntries.member.1.Message": {"msg"},
			},
			// AWS SNS returns a top-level NotFoundException for non-existent topics,
			// not per-entry failures.
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: []string{"NotFound"},
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

func TestSNSHandler_Routing(t *testing.T) {
	t.Parallel()

	t.Run("unknown action", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		rec := snsPost(t, h, url.Values{
			"Action":  {"FakeAction"},
			"Version": {"2010-03-31"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "InvalidAction")
	})

	t.Run("parse form error", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		e := echo.New()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestSNSHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		body        string
		want        bool
	}{
		{
			name:        "SNS request",
			contentType: "application/x-www-form-urlencoded",
			body:        "Action=CreateTopic&Version=2010-03-31&Name=test",
			want:        true,
		},
		{
			name:        "non-SNS request",
			contentType: "application/json",
			body:        `{"Action":"test"}`,
			want:        false,
		},
		{
			name:        "wrong version",
			contentType: "application/x-www-form-urlencoded",
			body:        "Action=CreateTopic&Version=2012-11-05&Name=test",
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", tt.contentType)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestSNSHandler_Introspection(t *testing.T) {
	t.Parallel()

	t.Run("Name", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		assert.Equal(t, "SNS", h.Name())
	})

	t.Run("MatchPriority", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		assert.Equal(t, 80, h.MatchPriority())
	})

	t.Run("GetSupportedOperations", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		ops := h.GetSupportedOperations()
		assert.Contains(t, ops, "CreateTopic")
		assert.Contains(t, ops, "Publish")
		assert.Contains(t, ops, "Subscribe")
	})

	t.Run("ExtractOperation", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		e := echo.New()
		body := "Action=CreateTopic&Version=2010-03-31&Name=test"
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		assert.Equal(t, "CreateTopic", h.ExtractOperation(c))
	})

	t.Run("ExtractOperation_EmptyBody", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		e := echo.New()
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		assert.Equal(t, "Unknown", h.ExtractOperation(c))
	})

	t.Run("ExtractResource_ByArn", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		e := echo.New()
		body := "Action=DeleteTopic&Version=2010-03-31&TopicArn=arn:aws:sns:us-east-1:000000000000:my-topic"
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:my-topic", h.ExtractResource(c))
	})

	t.Run("ExtractResource_ByName", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		e := echo.New()
		body := "Action=CreateTopic&Version=2010-03-31&Name=my-topic"
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		assert.Equal(t, "my-topic", h.ExtractResource(c))
	})

	t.Run("ExtractResource_Empty", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		e := echo.New()
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=ListTopics"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		assert.Empty(t, h.ExtractResource(c))
	})
}

// ---------------------------------------------------------------------------
// Standalone tests (kept as-is)
// ---------------------------------------------------------------------------

// TestCreateTopicXMLResponse verifies the XML structure of CreateTopic response.
func TestCreateTopicXMLResponse(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	h := sns.NewHandler(b)

	rec := snsPost(t, h, url.Values{
		"Action":  {"CreateTopic"},
		"Version": {"2010-03-31"},
		"Name":    {"xml-test"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)

	var resp sns.CreateTopicResponse
	require.NoError(t, xml.Unmarshal(body, &resp))
	assert.Contains(t, resp.CreateTopicResult.TopicArn, "xml-test")
	assert.NotEmpty(t, resp.ResponseMetadata.RequestID)
}

// TestProviderInit verifies the SNS provider initializes correctly.
func TestProviderInit(t *testing.T) {
	t.Parallel()

	p := &sns.Provider{}
	assert.Equal(t, "SNS", p.Name())

	log := logger.NewLogger(slog.LevelDebug)
	ctx := &service.AppContext{Logger: log}
	reg, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, reg)
	assert.Equal(t, "SNS", reg.Name())
}

// TestSNSPagination verifies pagination via tokens for ListTopics.
func TestSNSPagination(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	// Create 110 topics (>100 page size) to trigger pagination
	for i := range 110 {
		_, err := b.CreateTopic(fmt.Sprintf("topic-%03d", i), nil)
		require.NoError(t, err)
	}

	// First page
	page1, token1, err := b.ListTopics("")
	require.NoError(t, err)
	assert.Len(t, page1, 100)
	assert.NotEmpty(t, token1)

	// Second page using token from first
	page2, token2, err := b.ListTopics(token1)
	require.NoError(t, err)
	assert.Len(t, page2, 10)
	assert.Empty(t, token2)

	// Verify no overlap
	arns1 := make(map[string]bool)
	for _, tp := range page1 {
		arns1[tp.TopicArn] = true
	}
	for _, tp := range page2 {
		assert.False(t, arns1[tp.TopicArn], "page2 should not contain items from page1")
	}
}

// TestSNSSubscriptionPagination verifies pagination for ListSubscriptions.
func TestSNSSubscriptionPagination(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	topic, err := b.CreateTopic("big-topic", nil)
	require.NoError(t, err)

	// Create 105 subscriptions (>100 page size) to trigger pagination
	for i := range 105 {
		_, subErr := b.Subscribe(topic.TopicArn, "sqs", fmt.Sprintf("arn:aws:sqs:us-east-1:000000000000:q%d", i), "")
		require.NoError(t, subErr)
	}

	// First page
	subs1, token, err := b.ListSubscriptions("")
	require.NoError(t, err)
	assert.Len(t, subs1, 100)
	assert.NotEmpty(t, token)

	// Second page
	subs2, tok2, err := b.ListSubscriptions(token)
	require.NoError(t, err)
	assert.Len(t, subs2, 5)
	assert.Empty(t, tok2)

	// ListSubscriptions with invalid token
	_, _, err = b.ListSubscriptions("not-base64!!!")
	require.ErrorIs(t, err, sns.ErrInvalidParameter)

	// ListSubscriptionsByTopic with invalid token
	_, _, err = b.ListSubscriptionsByTopic(topic.TopicArn, "not-base64!!!")
	require.ErrorIs(t, err, sns.ErrInvalidParameter)
}

// TestSNSHTTPDelivery verifies Publish attempts HTTP delivery to http/https subscriptions.
func TestSNSHTTPDelivery(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := sns.NewInMemoryBackend()
	tp, err := b.CreateTopic("http-topic", nil)
	require.NoError(t, err)
	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)

	_, err = b.Publish(tp.TopicArn, "test-message", "", "", nil)
	require.NoError(t, err)

	// Verify message was delivered
	select {
	case msg := <-received:
		assert.Equal(t, "test-message", extractSNSHTTPMessage(msg))
	case <-time.After(500 * time.Millisecond):
		require.FailNow(t, "HTTP delivery did not arrive in time")
	}
}

// TestSNSHTTPDeliveryBadEndpoint verifies that bad HTTP endpoints don't panic.
func TestSNSHTTPDeliveryBadEndpoint(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	tp, err := b.CreateTopic("http-err-topic", nil)
	require.NoError(t, err)
	_, err = b.Subscribe(tp.TopicArn, "http", "http://localhost:1", "")
	require.NoError(t, err)

	// Should not panic or return error; delivery is best-effort
	_, err = b.Publish(tp.TopicArn, "test", "", "", nil)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Error helpers and error-path tests (kept as-is)
// ---------------------------------------------------------------------------

// errReadErr is the sentinel error returned by errReader.
var errRead = errors.New("read error")

// errBackendErr is the sentinel error returned by errBackend.
var errBackend2 = errors.New("unexpected internal backend failure")

// errReader is a helper that always fails on read.
type errReader struct{}

func (errReader) Read(_ []byte) (int, error) { return 0, errRead }
func (errReader) Close() error               { return nil }

// errBackend wraps InMemoryBackend and overrides CreateTopic/CreateTopicInRegion to return a custom error.
type errBackend struct {
	*sns.InMemoryBackend
}

func (b *errBackend) CreateTopic(_ string, _ map[string]string) (*sns.Topic, error) {
	return nil, errBackend2
}

func (b *errBackend) CreateTopicInRegion(_ string, _ string, _ map[string]string) (*sns.Topic, error) {
	return nil, errBackend2
}

// newErrContext builds an echo context whose request body always fails to read.
func newErrContext() *echo.Context {
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", errReader{})
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	return c
}

// TestSNSBodyReadErrors covers error paths triggered when the request body cannot be read.
func TestSNSBodyReadErrors(t *testing.T) {
	t.Parallel()

	newSNSHandler := func() *sns.Handler {
		return sns.NewHandler(sns.NewInMemoryBackend())
	}

	t.Run("RouteMatcher_BodyReadError", func(t *testing.T) {
		t.Parallel()
		assert.False(t, newSNSHandler().RouteMatcher()(newErrContext()))
	})

	t.Run("ExtractOperation_BodyReadError", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "Unknown", newSNSHandler().ExtractOperation(newErrContext()))
	})

	t.Run("ExtractResource_BodyReadError", func(t *testing.T) {
		t.Parallel()
		assert.Empty(t, newSNSHandler().ExtractResource(newErrContext()))
	})
}

// TestSNSHandlerInternalError covers the handleBackendError default case using a mock backend.
func TestSNSHandlerInternalError(t *testing.T) {
	t.Parallel()

	b := &errBackend{sns.NewInMemoryBackend()}
	h := sns.NewHandler(b)

	// CreateTopic will call handleBackendError with an unexpected (non-sentinel) error,
	// exercising the default branch in handleBackendError and errorCode.
	rec := snsPost(t, h, url.Values{
		"Action":  {"CreateTopic"},
		"Version": {"2010-03-31"},
		"Name":    {"test"},
	})
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "InternalError")
}

// TestFilterPolicy covers matchesFilterPolicy edge cases via the Publish method.
func TestFilterPolicy(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	tp, err := b.CreateTopic("fp-topic", nil)
	require.NoError(t, err)

	delivered := make(chan string, 10)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		delivered <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	// Subscriber with exact-match filter on attribute "color"=["red"]
	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, `{"color":["red"]}`)
	require.NoError(t, err)

	attrs := map[string]sns.MessageAttribute{
		"color": {DataType: "String", StringValue: "red"},
	}
	_, err = b.Publish(tp.TopicArn, "match", "", "", attrs)
	require.NoError(t, err)
	select {
	case msg := <-delivered:
		assert.Equal(t, "match", extractSNSHTTPMessage(msg))
	case <-time.After(500 * time.Millisecond):
		require.FailNow(t, "expected delivery for matching attribute")
	}

	// Publish with non-matching attribute - subscriber should NOT receive it.
	attrsBlue := map[string]sns.MessageAttribute{
		"color": {DataType: "String", StringValue: "blue"},
	}
	_, err = b.Publish(tp.TopicArn, "no-match", "", "", attrsBlue)
	require.NoError(t, err)
	select {
	case <-delivered:
		require.FailNow(t, "expected no delivery for non-matching attribute")
	case <-time.After(100 * time.Millisecond):
		// OK: nothing delivered
	}
}

func TestInMemoryBackend_SetSubscriptionAttributes_FilterPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		attrs           map[string]sns.MessageAttribute
		name            string
		filterPolicy    string
		wantDelivery    bool
		wantSetAttrsErr bool
	}{
		{
			name:         "matching filter policy delivers message",
			filterPolicy: `{"color":["red"]}`,
			attrs: map[string]sns.MessageAttribute{
				"color": {DataType: "String", StringValue: "red"},
			},
			wantDelivery: true,
		},
		{
			name:         "non matching filter policy blocks delivery",
			filterPolicy: `{"color":["red"]}`,
			attrs: map[string]sns.MessageAttribute{
				"color": {DataType: "String", StringValue: "blue"},
			},
			wantDelivery: false,
		},
		{
			name:            "invalid filter policy is rejected",
			filterPolicy:    `{"color":[}`,
			attrs:           map[string]sns.MessageAttribute{},
			wantSetAttrsErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			topicArn := mustCreateTopic(t, b, "set-filter-policy-topic-"+strings.ReplaceAll(tt.name, " ", "-"))
			received := make(chan string, 1)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				received <- string(body)
				w.WriteHeader(http.StatusOK)
			}))
			defer ts.Close()

			sub, err := b.Subscribe(topicArn, "http", ts.URL, "")
			require.NoError(t, err)

			err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "FilterPolicy", tt.filterPolicy)
			if tt.wantSetAttrsErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			_, err = b.Publish(topicArn, "hello", "", "", tt.attrs)
			require.NoError(t, err)

			if tt.wantDelivery {
				select {
				case msg := <-received:
					assert.Equal(t, "hello", extractSNSHTTPMessage(msg))
				case <-time.After(500 * time.Millisecond):
					require.FailNow(t, "expected delivery")
				}

				return
			}

			select {
			case <-received:
				require.FailNow(t, "expected no delivery")
			case <-time.After(120 * time.Millisecond):
				// no-op
			}
		})
	}
}

// TestFilterPolicyPrefixAndAnythingBut covers prefix and anything-but conditions.
func TestFilterPolicyPrefixAndAnythingBut(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	delivered := make(chan string, 10)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		delivered <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	tp, err := b.CreateTopic("fp2-topic", nil)
	require.NoError(t, err)

	// prefix match subscriber
	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, `{"event":[{"prefix":"order"}]}`)
	require.NoError(t, err)

	attrs := map[string]sns.MessageAttribute{"event": {DataType: "String", StringValue: "order.placed"}}
	_, err = b.Publish(tp.TopicArn, "prefix-match", "", "", attrs)
	require.NoError(t, err)
	select {
	case msg := <-delivered:
		assert.Equal(t, "prefix-match", extractSNSHTTPMessage(msg))
	case <-time.After(500 * time.Millisecond):
		require.FailNow(t, "expected delivery for prefix match")
	}

	tp2, err := b.CreateTopic("fp3-topic", nil)
	require.NoError(t, err)

	// anything-but subscriber
	_, err = b.Subscribe(tp2.TopicArn, "http", ts.URL, `{"status":[{"anything-but":"deleted"}]}`)
	require.NoError(t, err)

	attrsOK := map[string]sns.MessageAttribute{"status": {DataType: "String", StringValue: "active"}}
	_, err = b.Publish(tp2.TopicArn, "anything-but-match", "", "", attrsOK)
	require.NoError(t, err)
	select {
	case msg := <-delivered:
		assert.Equal(t, "anything-but-match", extractSNSHTTPMessage(msg))
	case <-time.After(500 * time.Millisecond):
		require.FailNow(t, "expected delivery for anything-but match")
	}

	// "deleted" should NOT be delivered
	attrsNo := map[string]sns.MessageAttribute{"status": {DataType: "String", StringValue: "deleted"}}
	_, err = b.Publish(tp2.TopicArn, "should-not-deliver", "", "", attrsNo)
	require.NoError(t, err)
	select {
	case <-delivered:
		require.FailNow(t, "expected no delivery for anything-but excluded value")
	case <-time.After(100 * time.Millisecond):
		// OK
	}
}

// TestFilterPolicySuffixAndEqualsIgnoreCase verifies suffix and equals-ignore-case
// SNS filter policy operators added for AWS parity.
func TestFilterPolicySuffixAndEqualsIgnoreCase(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	delivered := make(chan string, 10)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		delivered <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	// suffix subscriber
	suffixTopic, err := b.CreateTopic("fp-suffix", nil)
	require.NoError(t, err)
	_, err = b.Subscribe(suffixTopic.TopicArn, "http", ts.URL, `{"file":[{"suffix":".jpg"}]}`)
	require.NoError(t, err)

	_, err = b.Publish(suffixTopic.TopicArn, "suffix-match", "", "",
		map[string]sns.MessageAttribute{"file": {DataType: "String", StringValue: "photo.jpg"}})
	require.NoError(t, err)
	select {
	case msg := <-delivered:
		assert.Equal(t, "suffix-match", extractSNSHTTPMessage(msg))
	case <-time.After(500 * time.Millisecond):
		require.FailNow(t, "expected delivery for suffix match")
	}

	_, err = b.Publish(suffixTopic.TopicArn, "no-match", "", "",
		map[string]sns.MessageAttribute{"file": {DataType: "String", StringValue: "doc.pdf"}})
	require.NoError(t, err)
	select {
	case <-delivered:
		require.FailNow(t, "expected no delivery for non-matching suffix")
	case <-time.After(100 * time.Millisecond):
	}

	// equals-ignore-case subscriber
	eqTopic, err := b.CreateTopic("fp-eqic", nil)
	require.NoError(t, err)
	_, err = b.Subscribe(eqTopic.TopicArn, "http", ts.URL, `{"region":[{"equals-ignore-case":"us-east-1"}]}`)
	require.NoError(t, err)

	_, err = b.Publish(eqTopic.TopicArn, "case-match", "", "",
		map[string]sns.MessageAttribute{"region": {DataType: "String", StringValue: "US-EAST-1"}})
	require.NoError(t, err)
	select {
	case msg := <-delivered:
		assert.Equal(t, "case-match", extractSNSHTTPMessage(msg))
	case <-time.After(500 * time.Millisecond):
		require.FailNow(t, "expected delivery for equals-ignore-case match")
	}

	_, err = b.Publish(eqTopic.TopicArn, "case-no-match", "", "",
		map[string]sns.MessageAttribute{"region": {DataType: "String", StringValue: "eu-west-1"}})
	require.NoError(t, err)
	select {
	case <-delivered:
		require.FailNow(t, "expected no delivery for differing region")
	case <-time.After(100 * time.Millisecond):
	}
}

// TestMessageStructureJSON verifies per-protocol message extraction when MessageStructure=json.
func TestMessageStructureJSON(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	tp, err := b.CreateTopic("ms-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)

	jsonMsg := `{"http":"http specific msg","default":"default msg"}`
	_, err = b.Publish(tp.TopicArn, jsonMsg, "", "json", nil)
	require.NoError(t, err)

	select {
	case msg := <-received:
		assert.Equal(t, "http specific msg", extractSNSHTTPMessage(msg))
	case <-time.After(500 * time.Millisecond):
		require.FailNow(t, "expected HTTP delivery with per-protocol message")
	}
}

// TestMessageStructureJSONDefaultFallback verifies default key is used when protocol key is absent.
func TestMessageStructureJSONDefaultFallback(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	tp, err := b.CreateTopic("ms-fallback-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)

	jsonMsg := `{"default":"fallback msg"}`
	_, err = b.Publish(tp.TopicArn, jsonMsg, "", "json", nil)
	require.NoError(t, err)

	select {
	case msg := <-received:
		assert.Equal(t, "fallback msg", extractSNSHTTPMessage(msg))
	case <-time.After(500 * time.Millisecond):
		require.FailNow(t, "expected delivery with default fallback")
	}
}

// TestCreateTopic_RegionExtraction verifies that the handler uses the region from the SigV4
// Authorization header when creating a topic, and falls back to the default region otherwise.
func TestCreateTopic_RegionExtraction(t *testing.T) {
	t.Parallel()

	bk := sns.NewInMemoryBackend()
	h := sns.NewHandler(bk)
	h.DefaultRegion = "us-east-1"

	t.Run("default region when no Authorization header", func(t *testing.T) {
		t.Parallel()

		rec := snsPost(t, h, url.Values{
			"Action":  {"CreateTopic"},
			"Version": {"2010-03-31"},
			"Name":    {"default-topic"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "arn:aws:sns:us-east-1:000000000000:default-topic")
	})

	t.Run("region extracted from SigV4 Authorization header", func(t *testing.T) {
		t.Parallel()

		bk2 := sns.NewInMemoryBackend()
		h2 := sns.NewHandler(bk2)
		h2.DefaultRegion = "us-east-1"

		e := echo.New()
		form := url.Values{
			"Action":  {"CreateTopic"},
			"Version": {"2010-03-31"},
			"Name":    {"eu-topic"},
		}
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		// Minimal SigV4 Authorization header with eu-west-1 region in credential scope
		req.Header.Set("Authorization",
			"AWS4-HMAC-SHA256 Credential=AKID/20240101/eu-west-1/sns/aws4_request, SignedHeaders=host, Signature=abc")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		require.NoError(t, h2.Handler()(c))

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "arn:aws:sns:eu-west-1:000000000000:eu-topic")
	})
}

func TestSNSHandler_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		form             url.Values
		name             string
		wantBodyContains []string
		wantStatus       int
	}{
		{
			name: "tag_resource",
			form: url.Values{
				"Action":              {"TagResource"},
				"ResourceArn":         {"arn:aws:sns:us-east-1:000000000000:tag-topic"},
				"Tags.member.1.Key":   {"env"},
				"Tags.member.1.Value": {"prod"},
				"Tags.member.2.Key":   {"team"},
				"Tags.member.2.Value": {"infra"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"TagResourceResponse"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := sns.NewInMemoryBackend()
			b.CreateTopic("tag-topic", nil)
			h := sns.NewHandler(b)
			rec := snsPost(t, h, tt.form)
			assert.Equal(t, tt.wantStatus, rec.Code)
			for _, want := range tt.wantBodyContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestSNSHandler_UntagResource(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	b.CreateTopic("untag-topic", nil)
	topicArn := "arn:aws:sns:us-east-1:000000000000:untag-topic"
	b.SetTopicTags(topicArn, svcTags.FromMap("test.sns.untag", map[string]string{"env": "prod", "team": "infra"}))

	h := sns.NewHandler(b)

	rec := snsPost(t, h, url.Values{
		"Action":           {"UntagResource"},
		"ResourceArn":      {topicArn},
		"TagKeys.member.1": {"team"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "UntagResourceResponse")

	// Verify only "env" remains.
	remaining := b.GetTopicTags(topicArn)
	assert.Len(t, remaining, 1)
	assert.Equal(t, "prod", remaining["env"])
}

func TestSNSHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	b.CreateTopic("listtag-topic", nil)
	topicArn := "arn:aws:sns:us-east-1:000000000000:listtag-topic"
	b.SetTopicTags(topicArn, svcTags.FromMap("test.sns.list", map[string]string{"env": "staging"}))

	h := sns.NewHandler(b)

	rec := snsPost(t, h, url.Values{
		"Action":      {"ListTagsForResource"},
		"ResourceArn": {topicArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListTagsForResourceResponse")
	assert.Contains(t, rec.Body.String(), "env")
	assert.Contains(t, rec.Body.String(), "staging")
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

// TestCreateTopicInRegion_Backend tests the CreateTopicInRegion backend method directly.
func TestCreateTopicInRegion_Backend(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	topic, err := b.CreateTopicInRegion("my-topic", "eu-west-1", nil)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:sns:eu-west-1:000000000000:my-topic", topic.TopicArn)

	// Duplicate returns existing topic (idempotent), no error.
	tp2, err := b.CreateTopicInRegion("my-topic", "eu-west-1", nil)
	require.NoError(t, err)
	assert.Equal(t, topic.TopicArn, tp2.TopicArn, "idempotent CreateTopicInRegion must return same ARN")

	// Empty region falls back to backend default.
	topic2, err := b.CreateTopicInRegion("default-topic", "", nil)
	require.NoError(t, err)
	assert.Contains(t, topic2.TopicArn, "arn:aws:sns:us-east-1:000000000000:default-topic")
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

// ---------------------------------------------------------------------------
// Platform Application tests
// ---------------------------------------------------------------------------

func TestSNSHandler_CreatePlatformApplication(t *testing.T) {
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
			form: url.Values{
				"Action":                   {"CreatePlatformApplication"},
				"Name":                     {"MyApp"},
				"Platform":                 {"GCM"},
				"Attributes.entry.1.key":   {"PlatformCredential"},
				"Attributes.entry.1.value": {"my-api-key"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"CreatePlatformApplicationResponse", "app/GCM/MyApp"},
		},
		{
			name: "duplicate_returns_error",
			setup: func(b *sns.InMemoryBackend) {
				b.CreatePlatformApplication("MyApp", "GCM", nil)
			},
			form: url.Values{
				"Action":   {"CreatePlatformApplication"},
				"Name":     {"MyApp"},
				"Platform": {"GCM"},
			},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: []string{"PlatformApplicationAlreadyExists"},
		},
		{
			name: "invalid_name_with_slash",
			form: url.Values{
				"Action":   {"CreatePlatformApplication"},
				"Name":     {"My/App"},
				"Platform": {"GCM"},
			},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: []string{"InvalidParameter"},
		},
		{
			name: "missing_name",
			form: url.Values{
				"Action":   {"CreatePlatformApplication"},
				"Platform": {"GCM"},
			},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: []string{"InvalidParameter"},
		},
		{
			name: "missing_platform",
			form: url.Values{
				"Action": {"CreatePlatformApplication"},
				"Name":   {"MyApp"},
			},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: []string{"InvalidParameter"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, b := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := snsPost(t, h, tt.form)
			assert.Equal(t, tt.wantStatus, rec.Code)
			for _, want := range tt.wantBodyContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestSNSHandler_GetPlatformApplicationAttributes(t *testing.T) {
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
				app, _ := b.CreatePlatformApplication("MyApp", "GCM", map[string]string{"PlatformCredential": "key123"})

				return app.PlatformApplicationArn
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"GetPlatformApplicationAttributesResponse", "PlatformCredential", "key123"},
		},
		{
			name:          "missing_arn",
			formOverrides: url.Values{"PlatformApplicationArn": {""}},
			wantStatus:    http.StatusBadRequest,
		},
		{
			name: "not_found",
			formOverrides: url.Values{
				"PlatformApplicationArn": {"arn:aws:sns:us-east-1:000000000000:app/GCM/nonexistent"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, b := newTestHandler(t)
			form := url.Values{"Action": {"GetPlatformApplicationAttributes"}}

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

func TestSNSHandler_SetPlatformApplicationAttributes(t *testing.T) {
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
				"Attributes.entry.1.key":   {"PlatformCredential"},
				"Attributes.entry.1.value": {"new-key"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"SetPlatformApplicationAttributesResponse"},
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
			form := url.Values{"Action": {"SetPlatformApplicationAttributes"}}

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

func TestSNSHandler_ListPlatformApplications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup            func(b *sns.InMemoryBackend)
		name             string
		wantBodyContains []string
		wantStatus       int
	}{
		{
			name:             "empty",
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"ListPlatformApplicationsResponse"},
		},
		{
			name: "with_applications",
			setup: func(b *sns.InMemoryBackend) {
				b.CreatePlatformApplication("App1", "GCM", nil)
				b.CreatePlatformApplication("App2", "APNS", nil)
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"ListPlatformApplicationsResponse", "app/GCM/App1", "app/APNS/App2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, b := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := snsPost(t, h, url.Values{"Action": {"ListPlatformApplications"}})
			assert.Equal(t, tt.wantStatus, rec.Code)
			for _, want := range tt.wantBodyContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestSNSHandler_DeletePlatformApplication(t *testing.T) {
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

				return app.PlatformApplicationArn
			},
			wantStatus: http.StatusOK,
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
			form := url.Values{"Action": {"DeletePlatformApplication"}}

			if tt.setup != nil {
				form.Set("PlatformApplicationArn", tt.setup(b))
			}

			maps.Copy(form, tt.formOverrides)

			rec := snsPost(t, h, form)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// Platform Endpoint tests
// ---------------------------------------------------------------------------

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

func TestInMemoryBackend_PlatformApplicationLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *sns.InMemoryBackend)
		name string
	}{
		{
			name: "create_and_get",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				app, err := b.CreatePlatformApplication("App1", "GCM", map[string]string{"PlatformCredential": "key1"})
				require.NoError(t, err)
				assert.Contains(t, app.PlatformApplicationArn, "app/GCM/App1")

				attrs, err := b.GetPlatformApplicationAttributes(app.PlatformApplicationArn)
				require.NoError(t, err)
				assert.Equal(t, "key1", attrs["PlatformCredential"])
			},
		},
		{
			name: "get_not_found",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				_, err := b.GetPlatformApplicationAttributes("arn:aws:sns:us-east-1:000000000000:app/GCM/nonexistent")
				require.ErrorIs(t, err, sns.ErrPlatformApplicationNotFound)
			},
		},
		{
			name: "set_attributes",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				app, err := b.CreatePlatformApplication("App2", "APNS", nil)
				require.NoError(t, err)

				err = b.SetPlatformApplicationAttributes(
					app.PlatformApplicationArn,
					map[string]string{"EventEndpointCreated": "arn:aws:sns:us-east-1:000000000000:notify"},
				)
				require.NoError(t, err)

				attrs, err := b.GetPlatformApplicationAttributes(app.PlatformApplicationArn)
				require.NoError(t, err)
				assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:notify", attrs["EventEndpointCreated"])
			},
		},
		{
			name: "delete",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				app, err := b.CreatePlatformApplication("App3", "GCM", nil)
				require.NoError(t, err)

				err = b.DeletePlatformApplication(app.PlatformApplicationArn)
				require.NoError(t, err)

				_, err = b.GetPlatformApplicationAttributes(app.PlatformApplicationArn)
				require.ErrorIs(t, err, sns.ErrPlatformApplicationNotFound)
			},
		},
		{
			name: "delete_also_removes_endpoints",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				app, err := b.CreatePlatformApplication("App4", "GCM", nil)
				require.NoError(t, err)

				ep, err := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "tok", nil)
				require.NoError(t, err)

				err = b.DeletePlatformApplication(app.PlatformApplicationArn)
				require.NoError(t, err)

				_, err = b.GetEndpointAttributes(ep.EndpointArn)
				require.ErrorIs(t, err, sns.ErrEndpointNotFound)
			},
		},
		{
			name: "duplicate_returns_error",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				_, err := b.CreatePlatformApplication("DupApp", "GCM", nil)
				require.NoError(t, err)

				_, err = b.CreatePlatformApplication("DupApp", "GCM", nil)
				require.ErrorIs(t, err, sns.ErrPlatformApplicationAlreadyExists)
			},
		},
		{
			name: "slash_in_name_returns_error",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				_, err := b.CreatePlatformApplication("My/App", "GCM", nil)
				require.ErrorIs(t, err, sns.ErrInvalidParameter)
			},
		},
		{
			name: "slash_in_platform_returns_error",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				_, err := b.CreatePlatformApplication("MyApp", "GCM/v2", nil)
				require.ErrorIs(t, err, sns.ErrInvalidParameter)
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

// TestSNSHTTPDelivery_Robustness is a table-driven test that validates delivery
// behaviour under various adverse conditions (large server response, all
// concurrency slots occupied by slow handlers).
func TestSNSHTTPDelivery_Robustness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		serverFn    func() (http.Handler, <-chan string)
		name        string
		wantBody    string
		numSubs     int
		wantFastPub bool
	}{
		{
			name:    "large_response_bounded",
			numSubs: 1,
			serverFn: func() (http.Handler, <-chan string) {
				received := make(chan string, 1)
				large := bytes.Repeat([]byte("x"), 2*1024*1024)
				h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, _ := io.ReadAll(r.Body)
					received <- string(body)
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write(large)
				})

				return h, received
			},
			wantBody: "ping",
		},
		{
			name:    "non_blocking_under_load",
			numSubs: 12, // more than snsMaxConcurrentDeliveries (8)
			serverFn: func() (http.Handler, <-chan string) {
				// Sleep briefly so that goroutines are still in-flight when we
				// assert on elapsed time, but ensure the server completes well
				// before any test timeout. This avoids the deadlock that occurs
				// when httptest.Server.Close() (called via defer) waits for
				// handlers that are blocked indefinitely.
				h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					time.Sleep(200 * time.Millisecond)
					w.WriteHeader(http.StatusOK)
				})

				return h, nil
			},
			wantFastPub: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, received := tt.serverFn()
			ts := httptest.NewServer(h)
			defer ts.Close()

			b := sns.NewInMemoryBackend()
			b.SetHTTPDeliveryClient(&http.Client{Timeout: 5 * time.Second})
			tp, err := b.CreateTopic("robustness-"+tt.name, nil)
			require.NoError(t, err)

			for range tt.numSubs {
				_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, "")
				require.NoError(t, err)
			}

			// Ensure goroutines are drained before the test exits to avoid leaks.
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				done := make(chan struct{})
				go func() { b.WaitDeliveries(); close(done) }()
				select {
				case <-done:
				case <-ctx.Done():
					t.Errorf("WaitDeliveries timed out; goroutines may have leaked")
				}
			})

			start := time.Now()
			_, err = b.Publish(tp.TopicArn, "ping", "", "", nil)
			require.NoError(t, err)
			elapsed := time.Since(start)

			if tt.wantFastPub {
				assert.Less(t, elapsed, 500*time.Millisecond,
					"Publish must not block waiting for slow HTTP endpoints")
			}

			if tt.wantBody != "" {
				select {
				case got := <-received:
					assert.Equal(t, tt.wantBody, extractSNSHTTPMessage(got))
				case <-time.After(3 * time.Second):
					require.FailNow(t, "timed out waiting for HTTP delivery")
				}
			}
		})
	}
}

// TestSNSHandler_Shutdown validates Handler.Shutdown behaviour: it must drain
// in-flight delivery goroutines when the context permits, and return promptly
// when the context is already cancelled.
func TestSNSHandler_Shutdown(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		blockServer    bool // server blocks until unblocked by cleanup
		cancelCtxFirst bool // cancel the context before calling Shutdown
		wantFast       bool // Shutdown should return in <500 ms
		wantDelivered  bool // delivery must complete before Shutdown returns
	}{
		{
			name:          "drains_in_flight_delivery",
			blockServer:   false,
			wantDelivered: true,
		},
		{
			name:           "returns_promptly_on_cancelled_context",
			blockServer:    true,
			cancelCtxFirst: true,
			wantFast:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			delivered := make(chan struct{}, 1)
			unblock := make(chan struct{})

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tt.blockServer {
					<-unblock
				}
				delivered <- struct{}{}
				w.WriteHeader(http.StatusOK)
			}))
			defer func() {
				select {
				case <-unblock: // already closed
				default:
					close(unblock)
				}
				ts.Close()
			}()

			b := sns.NewInMemoryBackend()
			b.SetHTTPDeliveryClient(&http.Client{Timeout: 10 * time.Second})
			h := sns.NewHandler(b)

			tp, err := b.CreateTopic("shutdown-"+tt.name, nil)
			require.NoError(t, err)
			_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, "")
			require.NoError(t, err)

			_, err = b.Publish(tp.TopicArn, "msg", "", "", nil)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()

			if tt.cancelCtxFirst {
				cancel()
			}

			start := time.Now()
			h.Shutdown(ctx)
			elapsed := time.Since(start)

			if tt.wantFast {
				assert.Less(t, elapsed, 500*time.Millisecond,
					"Shutdown with cancelled context must return immediately")
			}

			if tt.wantDelivered {
				select {
				case <-delivered:
					// delivery completed before Shutdown returned
				default:
					require.FailNow(t, "delivery goroutine had not completed when Shutdown returned")
				}
			}
		})
	}
}

// TestMatchesFilterPolicy_OversizedPolicy verifies that a FilterPolicy exceeding
// the size limit is rejected at SetSubscriptionAttributes time rather than silently
// accepted (which would let an attacker poison the in-memory subscription).
func TestMatchesFilterPolicy_OversizedPolicy(t *testing.T) {
	t.Parallel()

	// Build a FilterPolicy that exceeds maxFilterPolicySizeBytes (256 KiB).
	bigPolicy := `{"key": ["` + strings.Repeat("a", 300*1024) + `"]}`

	b := sns.NewInMemoryBackend()
	tp, err := b.CreateTopic("big-policy-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "http", "http://example.invalid", "")
	require.NoError(t, err)

	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "FilterPolicy", bigPolicy)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "FilterPolicy")
}

// TestSNS_SMSSandboxFlow validates the SMS sandbox lifecycle: create, list, check opt-out,
// list opted-out, get sandbox status, and delete.
func TestSNS_SMSSandboxFlow(t *testing.T) {
	t.Parallel()

	type operation string

	const (
		opCreate      operation = "create"
		opDelete      operation = "delete"
		opCheckOptOut operation = "check_opt_out"
	)

	tests := []struct {
		setup        func(b *sns.InMemoryBackend)
		wantErr      error
		name         string
		phone        string
		op           operation
		wantCount    int
		wantOptedOut bool
	}{
		{
			name:      "create_and_list",
			op:        opCreate,
			phone:     "+12125551234",
			wantCount: 1,
		},
		{
			name:    "invalid_e164_rejected",
			op:      opCreate,
			phone:   "12125551234",
			wantErr: sns.ErrInvalidParameter,
		},
		{
			name: "duplicate_rejected",
			op:   opCreate,
			setup: func(b *sns.InMemoryBackend) {
				require.NoError(t, b.CreateSMSSandboxPhoneNumber("+12125559999", ""))
			},
			phone:   "+12125559999",
			wantErr: sns.ErrSandboxPhoneAlreadyExists,
		},
		{
			name:    "delete_not_found",
			op:      opDelete,
			phone:   "+19999999999",
			wantErr: sns.ErrPhoneNumberNotFound,
		},
		{
			name:         "opted_out_false_by_default",
			op:           opCheckOptOut,
			phone:        "+12125550001",
			wantOptedOut: false,
		},
		{
			name:    "check_opted_out_invalid_e164",
			op:      opCheckOptOut,
			phone:   "badnumber",
			wantErr: sns.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			switch tt.op {
			case opDelete:
				err := b.DeleteSMSSandboxPhoneNumber(tt.phone)
				if tt.wantErr != nil {
					require.ErrorIs(t, err, tt.wantErr)

					return
				}
				require.NoError(t, err)

			case opCheckOptOut:
				opted, err := b.CheckIfPhoneNumberIsOptedOut(tt.phone)
				if tt.wantErr != nil {
					require.ErrorIs(t, err, tt.wantErr)

					return
				}
				require.NoError(t, err)
				assert.Equal(t, tt.wantOptedOut, opted)

			default: // opCreate
				err := b.CreateSMSSandboxPhoneNumber(tt.phone, "en-US")
				if tt.wantErr != nil {
					require.ErrorIs(t, err, tt.wantErr)

					return
				}
				require.NoError(t, err)

				nums, _, listErr := b.ListSMSSandboxPhoneNumbers("", 0)
				require.NoError(t, listErr)
				assert.Len(t, nums, tt.wantCount)

				if tt.wantCount > 0 {
					// Newly created sandbox numbers always start as Pending.
					assert.Equal(t, "Pending", nums[0].Status)
					assert.Equal(t, "en-US", nums[0].LanguageCode)
				}
			}
		})
	}
}

// TestSNS_SMSSandboxHandlerFlow validates SMS sandbox HTTP handler operations end-to-end.
func TestSNS_SMSSandboxHandlerFlow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		form       url.Values
		name       string
		action     string
		wantBody   string
		wantStatus int
	}{
		{
			name:   "create_sandbox_number",
			action: "CreateSMSSandboxPhoneNumber",
			form: url.Values{
				"Action":      {"CreateSMSSandboxPhoneNumber"},
				"Version":     {"2010-03-31"},
				"PhoneNumber": {"+12125551111"},
			},
			wantStatus: http.StatusOK,
			wantBody:   "CreateSMSSandboxPhoneNumberResponse",
		},
		{
			name:   "create_sandbox_number_invalid_e164",
			action: "CreateSMSSandboxPhoneNumber",
			form: url.Values{
				"Action":      {"CreateSMSSandboxPhoneNumber"},
				"Version":     {"2010-03-31"},
				"PhoneNumber": {"12125551111"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "list_sandbox_numbers",
			action:     "ListSMSSandboxPhoneNumbers",
			form:       url.Values{"Action": {"ListSMSSandboxPhoneNumbers"}, "Version": {"2010-03-31"}},
			wantStatus: http.StatusOK,
			wantBody:   "ListSMSSandboxPhoneNumbersResponse",
		},
		{
			name:       "get_sandbox_status",
			action:     "GetSMSSandboxAccountStatus",
			form:       url.Values{"Action": {"GetSMSSandboxAccountStatus"}, "Version": {"2010-03-31"}},
			wantStatus: http.StatusOK,
			wantBody:   "GetSMSSandboxAccountStatusResponse",
		},
		{
			name:   "check_opted_out_false",
			action: "CheckIfPhoneNumberIsOptedOut",
			form: url.Values{
				"Action":      {"CheckIfPhoneNumberIsOptedOut"},
				"Version":     {"2010-03-31"},
				"phoneNumber": {"+12125551234"},
			},
			wantStatus: http.StatusOK,
			wantBody:   "CheckIfPhoneNumberIsOptedOutResponse",
		},
		{
			name:       "list_opted_out_empty",
			action:     "ListPhoneNumbersOptedOut",
			form:       url.Values{"Action": {"ListPhoneNumbersOptedOut"}, "Version": {"2010-03-31"}},
			wantStatus: http.StatusOK,
			wantBody:   "ListPhoneNumbersOptedOutResponse",
		},
		{
			name:       "get_sms_attributes_empty",
			action:     "GetSMSAttributes",
			form:       url.Values{"Action": {"GetSMSAttributes"}, "Version": {"2010-03-31"}},
			wantStatus: http.StatusOK,
			wantBody:   "GetSMSAttributesResponse",
		},
		{
			name:       "list_origination_numbers_empty",
			action:     "ListOriginationNumbers",
			form:       url.Values{"Action": {"ListOriginationNumbers"}, "Version": {"2010-03-31"}},
			wantStatus: http.StatusOK,
			wantBody:   "ListOriginationNumbersResponse",
		},
		{
			name:   "delete_sandbox_number_not_found",
			action: "DeleteSMSSandboxPhoneNumber",
			form: url.Values{
				"Action":      {"DeleteSMSSandboxPhoneNumber"},
				"Version":     {"2010-03-31"},
				"PhoneNumber": {"+19999999999"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "delete_sandbox_number_missing_param",
			action:     "DeleteSMSSandboxPhoneNumber",
			form:       url.Values{"Action": {"DeleteSMSSandboxPhoneNumber"}, "Version": {"2010-03-31"}},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := snsPost(t, h, tt.form)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestSNS_TopicPermissions validates the AddPermission operation lifecycle.
func TestSNS_TopicPermissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		setup    func(b *sns.InMemoryBackend) string
		name     string
		label    string
		accounts []string
		actions  []string
	}{
		{
			name: "add_permission_success",
			setup: func(b *sns.InMemoryBackend) string {
				t, _ := b.CreateTopic("perm-topic", nil)

				return t.TopicArn
			},
			label:    "allow-publish",
			accounts: []string{"123456789012"},
			actions:  []string{"Publish"},
		},
		{
			name: "add_permission_duplicate_label",
			setup: func(b *sns.InMemoryBackend) string {
				tp, _ := b.CreateTopic("perm-topic-dup", nil)

				err := b.AddPermission(tp.TopicArn, "my-label", []string{"123"}, []string{"Publish"})
				require.NoError(t, err)

				return tp.TopicArn
			},
			label:    "my-label",
			accounts: []string{"999"},
			actions:  []string{"Subscribe"},
			wantErr:  sns.ErrPermissionLabelExists,
		},
		{
			name:     "add_permission_topic_not_found",
			setup:    func(_ *sns.InMemoryBackend) string { return "arn:aws:sns:us-east-1:000000000000:missing" },
			label:    "some-label",
			accounts: []string{"123"},
			actions:  []string{"Publish"},
			wantErr:  sns.ErrTopicNotFound,
		},
		{
			name: "add_permission_empty_label",
			setup: func(b *sns.InMemoryBackend) string {
				tp, _ := b.CreateTopic("t", nil)

				return tp.TopicArn
			},
			label:    "",
			accounts: []string{"123"},
			actions:  []string{"Publish"},
			wantErr:  sns.ErrInvalidParameter,
		},
		{
			name: "add_permission_label_too_long",
			setup: func(b *sns.InMemoryBackend) string {
				tp, _ := b.CreateTopic("t2", nil)

				return tp.TopicArn
			},
			label:    strings.Repeat("a", 81),
			accounts: []string{"123"},
			actions:  []string{"Publish"},
			wantErr:  sns.ErrInvalidParameter,
		},
		{
			name: "add_permission_label_invalid_chars",
			setup: func(b *sns.InMemoryBackend) string {
				tp, _ := b.CreateTopic("t3", nil)

				return tp.TopicArn
			},
			label:    "invalid label!",
			accounts: []string{"123"},
			actions:  []string{"Publish"},
			wantErr:  sns.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			topicArn := tt.setup(b)

			err := b.AddPermission(topicArn, tt.label, tt.accounts, tt.actions)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
		})
	}
}

// TestSNS_TopicPermissionsHandler validates AddPermission HTTP handler.
func TestSNS_TopicPermissionsHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		form       url.Values
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "add_permission_success",
			form: url.Values{
				"Action":                {"AddPermission"},
				"Version":               {"2010-03-31"},
				"TopicArn":              {"arn:aws:sns:us-east-1:000000000000:perm-test"},
				"Label":                 {"allow-sub"},
				"AWSAccountId.member.1": {"123456789012"},
				"ActionName.member.1":   {"Publish"},
			},
			wantStatus: http.StatusOK,
			wantBody:   "AddPermissionResponse",
		},
		{
			name: "add_permission_missing_topic_arn",
			form: url.Values{
				"Action":  {"AddPermission"},
				"Version": {"2010-03-31"},
				"Label":   {"x"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "add_permission_missing_label",
			form: url.Values{
				"Action":   {"AddPermission"},
				"Version":  {"2010-03-31"},
				"TopicArn": {"arn:aws:sns:us-east-1:000000000000:perm-test"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			_, err := b.CreateTopic("perm-test", nil)
			require.NoError(t, err)

			rec := snsPost(t, h, tt.form)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
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

// TestSNS_RemovePermission validates the RemovePermission operation.
func TestSNS_RemovePermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(b *sns.InMemoryBackend) string
		name    string
		label   string
	}{
		{
			name: "remove_existing_label",
			setup: func(b *sns.InMemoryBackend) string {
				tp, _ := b.CreateTopic("rm-perm-topic", nil)

				err := b.AddPermission(tp.TopicArn, "my-label", []string{"123"}, []string{"Publish"})
				require.NoError(t, err)

				return tp.TopicArn
			},
			label: "my-label",
		},
		{
			name: "remove_missing_label",
			setup: func(b *sns.InMemoryBackend) string {
				tp, _ := b.CreateTopic("rm-perm-topic2", nil)

				return tp.TopicArn
			},
			label:   "nonexistent",
			wantErr: sns.ErrPermissionLabelNotFound,
		},
		{
			name:    "remove_topic_not_found",
			setup:   func(_ *sns.InMemoryBackend) string { return "arn:aws:sns:us-east-1:000000000000:missing" },
			label:   "x",
			wantErr: sns.ErrTopicNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			topicArn := tt.setup(b)

			err := b.RemovePermission(topicArn, tt.label)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
		})
	}
}

// TestSNS_RemovePermissionHandler validates the RemovePermission HTTP handler.
func TestSNS_RemovePermissionHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		form       url.Values
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "success",
			form: url.Values{
				"Action":   {"RemovePermission"},
				"Version":  {"2010-03-31"},
				"TopicArn": {"arn:aws:sns:us-east-1:000000000000:perm-test"},
				"Label":    {"my-label"},
			},
			wantStatus: http.StatusOK,
			wantBody:   "RemovePermissionResponse",
		},
		{
			name: "missing_label",
			form: url.Values{
				"Action":   {"RemovePermission"},
				"Version":  {"2010-03-31"},
				"TopicArn": {"arn:aws:sns:us-east-1:000000000000:perm-test"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_topic_arn",
			form: url.Values{
				"Action":  {"RemovePermission"},
				"Version": {"2010-03-31"},
				"Label":   {"my-label"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			tp, err := b.CreateTopic("perm-test", nil)
			require.NoError(t, err)

			err = b.AddPermission(tp.TopicArn, "my-label", []string{"123"}, []string{"Publish"})
			require.NoError(t, err)

			rec := snsPost(t, h, tt.form)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestSNS_VerifySMSSandboxPhoneNumber validates VerifySMSSandboxPhoneNumber.
func TestSNS_VerifySMSSandboxPhoneNumber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(b *sns.InMemoryBackend)
		name    string
		phone   string
		otp     string
	}{
		{
			name: "verify_success",
			setup: func(b *sns.InMemoryBackend) {
				require.NoError(t, b.CreateSMSSandboxPhoneNumber("+12125551234", ""))
			},
			phone: "+12125551234",
			otp:   "123456",
		},
		{
			name:    "missing_otp",
			phone:   "+12125551234",
			otp:     "",
			wantErr: sns.ErrInvalidParameter,
		},
		{
			name:    "phone_not_found",
			phone:   "+12125559999",
			otp:     "123456",
			wantErr: sns.ErrPhoneNumberNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.VerifySMSSandboxPhoneNumber(tt.phone, tt.otp)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)

			// Verify that the phone number status has been set to Verified.
			nums, _, listErr := b.ListSMSSandboxPhoneNumbers("", 0)
			require.NoError(t, listErr)
			require.Len(t, nums, 1)
			assert.Equal(t, "Verified", nums[0].Status)
		})
	}
}

// TestSNS_VerifySMSSandboxPhoneNumberHandler validates the VerifySMSSandboxPhoneNumber HTTP handler.
func TestSNS_VerifySMSSandboxPhoneNumberHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		form       url.Values
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "success",
			form: url.Values{
				"Action":          {"VerifySMSSandboxPhoneNumber"},
				"Version":         {"2010-03-31"},
				"PhoneNumber":     {"+12125551234"},
				"OneTimePassword": {"123456"},
			},
			wantStatus: http.StatusOK,
			wantBody:   "VerifySMSSandboxPhoneNumberResponse",
		},
		{
			name: "missing_phone",
			form: url.Values{
				"Action":          {"VerifySMSSandboxPhoneNumber"},
				"Version":         {"2010-03-31"},
				"OneTimePassword": {"123456"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			require.NoError(t, b.CreateSMSSandboxPhoneNumber("+12125551234", ""))

			rec := snsPost(t, h, tt.form)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestSNS_OptInPhoneNumber validates OptInPhoneNumber operation.
func TestSNS_OptInPhoneNumber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		phone   string
	}{
		{
			name:  "opt_in_valid",
			phone: "+12125550001",
		},
		{
			name:    "invalid_e164",
			phone:   "12125550001",
			wantErr: sns.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()

			err := b.OptInPhoneNumber(tt.phone)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)

			// Verify the phone is no longer opted out.
			optedOut, err := b.CheckIfPhoneNumberIsOptedOut(tt.phone)
			require.NoError(t, err)
			assert.False(t, optedOut)
		})
	}
}

// TestSNS_OptInPhoneNumberHandler validates the OptInPhoneNumber HTTP handler.
func TestSNS_OptInPhoneNumberHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		form       url.Values
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "success",
			form: url.Values{
				"Action":      {"OptInPhoneNumber"},
				"Version":     {"2010-03-31"},
				"phoneNumber": {"+12125550001"},
			},
			wantStatus: http.StatusOK,
			wantBody:   "OptInPhoneNumberResponse",
		},
		{
			name: "missing_phone",
			form: url.Values{
				"Action":  {"OptInPhoneNumber"},
				"Version": {"2010-03-31"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid_e164",
			form: url.Values{
				"Action":      {"OptInPhoneNumber"},
				"Version":     {"2010-03-31"},
				"phoneNumber": {"12125550001"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := snsPost(t, h, tt.form)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestSNS_SetSMSAttributes validates SetSMSAttributes operation and its interaction with GetSMSAttributes.
func TestSNS_SetSMSAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		attributes map[string]string
		wantAttrs  map[string]string
		name       string
		wantErr    bool
	}{
		{
			name:       "set_and_get_all",
			attributes: map[string]string{"DefaultSMSType": "Transactional", "MonthlySpendLimit": "50"},
			wantAttrs:  map[string]string{"DefaultSMSType": "Transactional", "MonthlySpendLimit": "50"},
		},
		{
			name:       "empty_attributes",
			attributes: map[string]string{},
			wantAttrs:  map[string]string{},
		},
		{
			name:       "unknown_attribute_rejected",
			attributes: map[string]string{"UnknownKey": "value"},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()

			err := b.SetSMSAttributes(tt.attributes)
			if tt.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)

			got, err := b.GetSMSAttributes(nil)
			require.NoError(t, err)

			for k, v := range tt.wantAttrs {
				assert.Equal(t, v, got[k])
			}
		})
	}
}

// TestSNS_SetSMSAttributesHandler validates the SetSMSAttributes HTTP handler.
func TestSNS_SetSMSAttributesHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		form       url.Values
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "success",
			form: url.Values{
				"Action":                   {"SetSMSAttributes"},
				"Version":                  {"2010-03-31"},
				"attributes.entry.1.key":   {"DefaultSMSType"},
				"attributes.entry.1.value": {"Transactional"},
			},
			wantStatus: http.StatusOK,
			wantBody:   "SetSMSAttributesResponse",
		},
		{
			name: "empty_attributes",
			form: url.Values{
				"Action":  {"SetSMSAttributes"},
				"Version": {"2010-03-31"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := snsPost(t, h, tt.form)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestSNS_DeleteSMSSandboxPhoneNumberE164Validation validates that DeleteSMSSandboxPhoneNumber
// rejects numbers that are not in E.164 format.
func TestSNS_DeleteSMSSandboxPhoneNumberE164Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		phone   string
	}{
		{
			name:    "invalid_e164_no_plus",
			phone:   "12125551234",
			wantErr: sns.ErrInvalidParameter,
		},
		{
			name:    "invalid_e164_letters",
			phone:   "+1212555ABCD",
			wantErr: sns.ErrInvalidParameter,
		},
		{
			name:    "not_found_but_valid_e164",
			phone:   "+12125559999",
			wantErr: sns.ErrPhoneNumberNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()

			err := b.DeleteSMSSandboxPhoneNumber(tt.phone)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// TestSNS_PersistenceWithNewFields validates that Snapshot/Restore correctly round-trips
// SMS sandbox state, opted-out phone numbers, SMS attributes, topic permissions, and
// platform applications/endpoints.
func TestSNS_PersistenceWithNewFields(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	// SMS sandbox numbers.
	require.NoError(t, b.CreateSMSSandboxPhoneNumber("+12125550001", "en-US"))
	require.NoError(t, b.CreateSMSSandboxPhoneNumber("+12125550002", "de-DE"))

	// SMS attributes.
	require.NoError(t, b.SetSMSAttributes(map[string]string{
		"DefaultSMSType":    "Transactional",
		"MonthlySpendLimit": "100",
	}))

	// Topic with permission.
	tp, err := b.CreateTopic("persist-test", nil)
	require.NoError(t, err)
	require.NoError(t, b.AddPermission(tp.TopicArn, "allow-all", []string{"*"}, []string{"Publish"}))

	// Platform application with endpoint.
	app, err := b.CreatePlatformApplication("my-app", "GCM", map[string]string{"PlatformCredential": "fake-key"})
	require.NoError(t, err)
	ep, err := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "device-token-123", nil)
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotEmpty(t, snap)

	b2 := sns.NewInMemoryBackend()
	require.NoError(t, b2.Restore(snap))

	// Verify SMS sandbox numbers are preserved.
	nums, _, err := b2.ListSMSSandboxPhoneNumbers("", 0)
	require.NoError(t, err)
	require.Len(t, nums, 2)
	assert.Equal(t, "Pending", nums[0].Status)
	assert.Equal(t, "en-US", nums[0].LanguageCode)

	// Verify SMS attributes are preserved.
	attrs, err := b2.GetSMSAttributes(nil)
	require.NoError(t, err)
	assert.Equal(t, "Transactional", attrs["DefaultSMSType"])
	assert.Equal(t, "100", attrs["MonthlySpendLimit"])

	// Verify permissions survive the round-trip via GetTopicAttributes.
	topicAttrs, err := b2.GetTopicAttributes(tp.TopicArn)
	require.NoError(t, err)
	assert.NotEmpty(t, topicAttrs)

	// Verify platform applications are preserved.
	apps, _, err := b2.ListPlatformApplications("")
	require.NoError(t, err)
	require.Len(t, apps, 1)
	assert.Equal(t, app.PlatformApplicationArn, apps[0].PlatformApplicationArn)

	// Verify platform endpoints are preserved.
	eps, _, err := b2.ListEndpointsByPlatformApplication(app.PlatformApplicationArn, "")
	require.NoError(t, err)
	require.Len(t, eps, 1)
	assert.Equal(t, ep.EndpointArn, eps[0].EndpointArn)
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

// TestSNS_MaxResultsListSandbox validates that ListSMSSandboxPhoneNumbers and
// ListPhoneNumbersOptedOut respect the MaxResults parameter via the HTTP handler.
func TestSNS_MaxResultsListSandbox(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults string
		wantCount  int
	}{
		{
			name:       "default_returns_all",
			maxResults: "",
			wantCount:  3,
		},
		{
			name:       "max_results_1",
			maxResults: "1",
			wantCount:  1,
		},
		{
			name:       "max_results_2",
			maxResults: "2",
			wantCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)

			// Create 3 sandbox phone numbers.
			for _, phone := range []string{"+12125550001", "+12125550002", "+12125550003"} {
				require.NoError(t, b.CreateSMSSandboxPhoneNumber(phone, ""))
			}

			form := url.Values{
				"Action":  {"ListSMSSandboxPhoneNumbers"},
				"Version": {"2010-03-31"},
			}
			if tt.maxResults != "" {
				form["MaxResults"] = []string{tt.maxResults}
			}

			rec := snsPost(t, h, form)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp sns.ListSMSSandboxPhoneNumbersResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.ListSMSSandboxPhoneNumbersResult.PhoneNumbers, tt.wantCount)
		})
	}
}

// TestSNS_FIFOTopic validates that FIFO topic names are accepted and attributes auto-set.
func TestSNS_FIFOTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		topicName string
		wantFifo  bool
		wantErr   bool
	}{
		{
			name:      "standard_topic",
			topicName: "my-topic",
			wantFifo:  false,
		},
		{
			name:      "fifo_topic",
			topicName: "my-topic.fifo",
			wantFifo:  true,
		},
		{
			name:      "invalid_name_too_long",
			topicName: strings.Repeat("a", sns.ExportedMaxTopicNameLen+1),
			wantErr:   true,
		},
		{
			name:      "invalid_name_special_chars",
			topicName: "my topic!",
			wantErr:   true,
		},
		{
			name:      "empty_name",
			topicName: "",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			topic, err := b.CreateTopic(tt.topicName, nil)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			attrs, err := b.GetTopicAttributes(topic.TopicArn)
			require.NoError(t, err)

			if tt.wantFifo {
				assert.Equal(t, "true", attrs["FifoTopic"])
				assert.Equal(t, "false", attrs["ContentBasedDeduplication"])
			} else {
				assert.Empty(t, attrs["FifoTopic"])
			}
		})
	}
}

// TestSNS_SubscribeProtocols validates that all supported protocols are accepted and unsupported ones rejected.
func TestSNS_SubscribeProtocols(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		protocol   string
		endpoint   string
		wantStatus int
	}{
		{name: "http", protocol: "http", endpoint: "http://example.com/sns", wantStatus: http.StatusOK},
		{name: "https", protocol: "https", endpoint: "https://example.com/sns", wantStatus: http.StatusOK},
		{name: "email", protocol: "email", endpoint: "user@example.com", wantStatus: http.StatusOK},
		{name: "email_json", protocol: "email-json", endpoint: "user@example.com", wantStatus: http.StatusOK},
		{
			name: "sqs", protocol: "sqs",
			endpoint:   "arn:aws:sqs:us-east-1:123456789012:my-queue",
			wantStatus: http.StatusOK,
		},
		{
			name: "lambda", protocol: "lambda",
			endpoint:   "arn:aws:lambda:us-east-1:123456789012:function:my-fn",
			wantStatus: http.StatusOK,
		},
		{name: "sms", protocol: "sms", endpoint: "+12125551234", wantStatus: http.StatusOK},
		{
			name: "application", protocol: "application",
			endpoint:   "arn:aws:sns:us-east-1:123456789012:endpoint/GCM/app/uuid",
			wantStatus: http.StatusOK,
		},
		{
			name: "firehose", protocol: "firehose",
			endpoint: "arn:aws:firehose:us-east-1:123456789012:deliverystream/my-stream",
			// firehose requires SubscriptionRoleArn — tested without it here to verify 400.
			wantStatus: http.StatusBadRequest,
		},
		{name: "invalid", protocol: "ftp", endpoint: "ftp://example.com", wantStatus: http.StatusBadRequest},
		{name: "empty", protocol: "", endpoint: "", wantStatus: http.StatusBadRequest},
		{
			name: "email_invalid_endpoint", protocol: "email",
			endpoint:   "not-an-email",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "email_json_invalid_endpoint", protocol: "email-json",
			endpoint:   "also-not-an-email",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			topicArn := mustCreateTopic(t, b, "proto-test")

			form := url.Values{
				"Action":   {"Subscribe"},
				"Version":  {"2010-03-31"},
				"TopicArn": {topicArn},
				"Protocol": {tt.protocol},
				"Endpoint": {tt.endpoint},
			}

			rec := snsPost(t, h, form)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestSNS_SubscribeSMSEndpointValidation validates that SMS subscriptions require E.164 endpoint.
func TestSNS_SubscribeSMSEndpointValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		endpoint   string
		wantStatus int
	}{
		{
			name:       "valid_e164",
			endpoint:   "+12125551234",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_no_plus",
			endpoint:   "12125551234",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_letters",
			endpoint:   "+1212ABCD",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			topicArn := mustCreateTopic(t, b, "sms-val-topic")

			rec := snsPost(t, h, url.Values{
				"Action":   {"Subscribe"},
				"Version":  {"2010-03-31"},
				"TopicArn": {topicArn},
				"Protocol": {"sms"},
				"Endpoint": {tt.endpoint},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestSNS_ReturnSubscriptionArn validates that ReturnSubscriptionArn=true returns the real ARN.
func TestSNS_ReturnSubscriptionArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		protocol       string
		returnSubArn   string
		wantPendingStr bool
	}{
		{
			name:           "http_without_flag_returns_pending",
			protocol:       "http",
			returnSubArn:   "false",
			wantPendingStr: true,
		},
		{
			name:           "http_with_flag_returns_real_arn",
			protocol:       "http",
			returnSubArn:   "true",
			wantPendingStr: false,
		},
		{
			name:           "sqs_always_returns_real_arn",
			protocol:       "sqs",
			returnSubArn:   "false",
			wantPendingStr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			topicArn := mustCreateTopic(t, b, "ret-sub-arn-topic")

			form := url.Values{
				"Action":                {"Subscribe"},
				"Version":               {"2010-03-31"},
				"TopicArn":              {topicArn},
				"Protocol":              {tt.protocol},
				"Endpoint":              {"https://example.com/endpoint"},
				"ReturnSubscriptionArn": {tt.returnSubArn},
			}
			if tt.protocol == "sqs" {
				form["Endpoint"] = []string{"arn:aws:sqs:us-east-1:123:queue"}
			}

			rec := snsPost(t, h, form)
			require.Equal(t, http.StatusOK, rec.Code)

			if tt.wantPendingStr {
				assert.Contains(t, rec.Body.String(), "pending confirmation")
			} else {
				assert.NotContains(t, rec.Body.String(), "pending confirmation")
				assert.Contains(t, rec.Body.String(), "arn:aws:sns:")
			}
		})
	}
}

// TestSNS_PublishBatchMaxEntries validates the 10-entry limit for PublishBatch.
func TestSNS_PublishBatchMaxEntries(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)
	topicArn := mustCreateTopic(t, b, "batch-limit-topic")

	// Build a form with 11 entries (exceeding the 10-entry limit).
	form := url.Values{
		"Action":   {"PublishBatch"},
		"Version":  {"2010-03-31"},
		"TopicArn": {topicArn},
	}

	for i := 1; i <= sns.ExportedMaxPublishBatchEntries+1; i++ {
		form[fmt.Sprintf("PublishBatchRequestEntries.member.%d.Id", i)] = []string{fmt.Sprintf("id%d", i)}
		form[fmt.Sprintf("PublishBatchRequestEntries.member.%d.Message", i)] = []string{"msg"}
	}

	rec := snsPost(t, h, form)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "TooManyEntriesInBatchRequest")
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

// TestSNS_TopicNameValidation validates CreateTopic name format rules.
func TestSNS_TopicNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		topicName string
		wantValid bool
	}{
		{name: "valid_alphanumeric", topicName: "MyTopic123", wantValid: true},
		{name: "valid_with_hyphen", topicName: "my-topic", wantValid: true},
		{name: "valid_with_underscore", topicName: "my_topic", wantValid: true},
		{name: "valid_fifo", topicName: "my-topic.fifo", wantValid: true},
		{name: "max_length", topicName: strings.Repeat("a", sns.ExportedMaxTopicNameLen), wantValid: true},
		{name: "too_long", topicName: strings.Repeat("a", sns.ExportedMaxTopicNameLen+1), wantValid: false},
		{name: "empty", topicName: "", wantValid: false},
		{name: "space", topicName: "my topic", wantValid: false},
		{name: "dot_only_fifo", topicName: ".fifo", wantValid: false},
		{name: "special_chars", topicName: "my!topic", wantValid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.wantValid, sns.IsValidTopicNameForTest(tt.topicName))
		})
	}
}

func TestSNS_PublishRejectsInvalidMessageAttribute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		attrs   map[string]sns.MessageAttribute
		name    string
		wantErr string
	}{
		{
			name: "unsupported_data_type",
			attrs: map[string]sns.MessageAttribute{
				"k": {DataType: "Bogus", StringValue: "v"},
			},
			wantErr: "unsupported DataType",
		},
		{
			name: "empty_attribute_name",
			attrs: map[string]sns.MessageAttribute{
				"": {DataType: "String", StringValue: "v"},
			},
			wantErr: "attribute name must be",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			tp, err := b.CreateTopic("attr-validation-topic", nil)
			require.NoError(t, err)

			_, err = b.Publish(tp.TopicArn, "hello", "", "", tt.attrs)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
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

func TestSNS_FilterPolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		filterPolicy string
		wantErr      string
	}{
		{
			name:         "rejects_unknown_numeric_operator",
			filterPolicy: `{"price":[{"numeric":["!=", 10]}]}`,
			wantErr:      "is not supported",
		},
		{
			name:         "rejects_odd_numeric_operands",
			filterPolicy: `{"price":[{"numeric":[">"]}]}`,
			wantErr:      "operator/number pairs",
		},
		{
			name:         "rejects_non_numeric_threshold",
			filterPolicy: `{"price":[{"numeric":[">", "ten"]}]}`,
			wantErr:      "must be a number",
		},
		{
			name:         "accepts_valid_numeric_condition",
			filterPolicy: `{"price":[{"numeric":[">", 10, "<=", 100]}]}`,
		},
		{
			name:         "rejects_unknown_operator_name",
			filterPolicy: `{"event":[{"contains":"foo"}]}`,
			wantErr:      "unsupported operator",
		},
		{
			name:         "accepts_suffix_operator",
			filterPolicy: `{"file":[{"suffix":".jpg"}]}`,
		},
		{
			name:         "accepts_equals_ignore_case_operator",
			filterPolicy: `{"region":[{"equals-ignore-case":"us-east-1"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			tp, err := b.CreateTopic("filter-policy-topic", nil)
			require.NoError(t, err)

			sub, err := b.Subscribe(tp.TopicArn, "http", "http://example.invalid", "")
			require.NoError(t, err)

			err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "FilterPolicy", tt.filterPolicy)
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// TestSNS_PublishSizeIncludesAttributes verifies that AWS-style SNS sizing
// (body + attribute name + type + value) is enforced against the 256 KiB cap.
func TestSNS_PublishSizeIncludesAttributes(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	tp, err := b.CreateTopic("size-topic", nil)
	require.NoError(t, err)

	const limit = 256 * 1024

	// Body alone is under the limit; large attribute pushes the total over.
	body := strings.Repeat("a", limit-100)
	attrs := map[string]sns.MessageAttribute{
		strings.Repeat("k", 50): {
			DataType:    "String",
			StringValue: strings.Repeat("v", 200),
		},
	}

	_, err = b.Publish(tp.TopicArn, body, "", "", attrs)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds SNS limit")

	// Body alone with no attributes succeeds at the same body size.
	_, err = b.Publish(tp.TopicArn, body, "", "", nil)
	require.NoError(t, err)
}
