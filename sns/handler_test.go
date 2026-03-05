package sns_test

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
	"github.com/blackbirdworks/gopherstack/sns"
)

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
			wantErr:   sns.ErrTopicAlreadyExists,
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
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: []string{"TopicAlreadyExists"},
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
			wantBodyContains: []string{"PendingConfirmation"},
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
			wantBodyContains: []string{"PendingConfirmation"},
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
			name: "partial failure topic not found",
			setup: func(b *sns.InMemoryBackend) {
				tp, _ := b.CreateTopic("pfail-topic", nil)
				b.DeleteTopic(tp.TopicArn)
			},
			form: url.Values{
				"Action":                                 {"PublishBatch"},
				"Version":                                {"2010-03-31"},
				"TopicArn":                               {"arn:aws:sns:us-east-1:000000000000:pfail-topic"},
				"PublishBatchRequestEntries.member.1.Id": {"fail1"},
				"PublishBatchRequestEntries.member.1.Message": {"msg"},
			},
			wantStatus:       http.StatusOK,
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

	// Create 30 topics (>25 page size) to trigger pagination
	for i := range 30 {
		_, err := b.CreateTopic(fmt.Sprintf("topic-%02d", i), nil)
		require.NoError(t, err)
	}

	// First page
	page1, token1, err := b.ListTopics("")
	require.NoError(t, err)
	assert.Len(t, page1, 25)
	assert.NotEmpty(t, token1)

	// Second page using token from first
	page2, token2, err := b.ListTopics(token1)
	require.NoError(t, err)
	assert.Len(t, page2, 5)
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

	// Create 28 subscriptions
	for i := range 28 {
		_, subErr := b.Subscribe(topic.TopicArn, "sqs", fmt.Sprintf("arn:aws:sqs:us-east-1:000000000000:q%d", i), "")
		require.NoError(t, subErr)
	}

	// First page
	subs1, token, err := b.ListSubscriptions("")
	require.NoError(t, err)
	assert.Len(t, subs1, 25)
	assert.NotEmpty(t, token)

	// Second page
	subs2, tok2, err := b.ListSubscriptions(token)
	require.NoError(t, err)
	assert.Len(t, subs2, 3)
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
		assert.Equal(t, "test-message", msg)
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
		assert.Equal(t, "match", msg)
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
		assert.Equal(t, "prefix-match", msg)
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
		assert.Equal(t, "anything-but-match", msg)
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
		assert.Equal(t, "http specific msg", msg)
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
		assert.Equal(t, "fallback msg", msg)
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

	// Duplicate should fail.
	_, err = b.CreateTopicInRegion("my-topic", "eu-west-1", nil)
	require.ErrorIs(t, err, sns.ErrTopicAlreadyExists)

	// Empty region falls back to backend default.
	topic2, err := b.CreateTopicInRegion("default-topic", "", nil)
	require.NoError(t, err)
	assert.Contains(t, topic2.TopicArn, "arn:aws:sns:us-east-1:000000000000:default-topic")
}
