package sns_test

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
			endpoint: "arn:aws:sqs:us-east-1:000000000000:missing-q",
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
				sub, _ := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:unsub-q", "")

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
				b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:ls-q", "")
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
				b.Subscribe(tp1.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:lstt-q1", "")
				b.Subscribe(tp2.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:lstt-q2", "")
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
		subArn := mustSubscribe(t, b, arn, "sqs", "arn:aws:sqs:us-east-1:000000000000:unsub-h-q")
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
				b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:ls-h-q", "")
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
				b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:lstt-h-q", "")
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

// TestListSubscriptionsByTopicPagination verifies that ListSubscriptionsByTopic
// correctly paginates when a topic has more than 100 subscriptions.
func TestListSubscriptionsByTopicPagination(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("paginate-by-topic", nil)
	require.NoError(t, err)

	for i := range 115 {
		_, subErr := b.Subscribe(tp.TopicArn, "sqs",
			fmt.Sprintf("arn:aws:sqs:us-east-1:000000000000:pbt-q%d", i), "")
		require.NoError(t, subErr)
	}

	page1, tok1, err := b.ListSubscriptionsByTopic(tp.TopicArn, "")
	require.NoError(t, err)
	assert.Len(t, page1, 100)
	assert.NotEmpty(t, tok1)

	page2, tok2, err := b.ListSubscriptionsByTopic(tp.TopicArn, tok1)
	require.NoError(t, err)
	assert.Len(t, page2, 15)
	assert.Empty(t, tok2)

	seen := make(map[string]bool, 115)
	for _, s := range append(page1, page2...) {
		assert.False(t, seen[s.SubscriptionArn], "duplicate subscription in pages")
		seen[s.SubscriptionArn] = true
	}
	assert.Len(t, seen, 115)
}

// TestListSubscriptionsByTopicInvalidToken verifies that ListSubscriptionsByTopic
// returns ErrInvalidParameter for a malformed token.
func TestListSubscriptionsByTopicInvalidToken(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("invalid-token-topic", nil)
	require.NoError(t, err)

	_, _, err = b.ListSubscriptionsByTopic(tp.TopicArn, "!!!not-base64!!!")
	require.ErrorIs(t, err, sns.ErrInvalidParameter)
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

// TestSNS_SubscribeARNUsesTopicRegion verifies that the subscription ARN embeds
// the topic's region, not the backend's default construction-time region.
func TestSNS_SubscribeARNUsesTopicRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		topicRegion string
		protocol    string
		endpoint    string
	}{
		{
			name:        "topic in us-east-1",
			topicRegion: "us-east-1",
			protocol:    "sqs",
			endpoint:    "arn:aws:sqs:us-east-1:000000000000:q1",
		},
		{
			name:        "topic in eu-west-1",
			topicRegion: "eu-west-1",
			protocol:    "sqs",
			endpoint:    "arn:aws:sqs:eu-west-1:000000000000:q2",
		},
		{
			name:        "topic in ap-southeast-1",
			topicRegion: "ap-southeast-1",
			protocol:    "sqs",
			endpoint:    "arn:aws:sqs:ap-southeast-1:000000000000:q3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			topic, err := b.CreateTopicInRegion("my-topic", tt.topicRegion, nil)
			require.NoError(t, err)

			sub, err := b.Subscribe(topic.TopicArn, tt.protocol, tt.endpoint, "")
			require.NoError(t, err)

			// The subscription ARN must embed the topic's region, not the backend default.
			assert.Contains(t, sub.SubscriptionArn, ":"+tt.topicRegion+":")
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
				"Endpoint":              {"http://example.com/endpoint"},
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
