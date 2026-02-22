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

// TestInMemoryBackend exercises all backend operations.
func TestInMemoryBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *sns.InMemoryBackend)
		name string
	}{
		{
			name: "CreateTopic_Success",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				topic, err := b.CreateTopic("my-topic", map[string]string{"DisplayName": "My Topic"})
				require.NoError(t, err)
				assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:my-topic", topic.TopicArn)
				assert.Equal(t, "My Topic", topic.Attributes["DisplayName"])
			},
		},
		{
			name: "CreateTopic_Duplicate",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateTopic("dup-topic", nil)
				require.NoError(t, err)
				_, err = b.CreateTopic("dup-topic", nil)
				require.ErrorIs(t, err, sns.ErrTopicAlreadyExists)
			},
		},
		{
			name: "DeleteTopic_Success",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				arn := mustCreateTopic(t, b, "del-topic")
				require.NoError(t, b.DeleteTopic(arn))
				assert.Empty(t, b.ListAllTopics())
			},
		},
		{
			name: "DeleteTopic_NotFound",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				err := b.DeleteTopic("arn:aws:sns:us-east-1:000000000000:missing")
				require.ErrorIs(t, err, sns.ErrTopicNotFound)
			},
		},
		{
			name: "ListTopics_Empty",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				topics, next, err := b.ListTopics("")
				require.NoError(t, err)
				assert.Empty(t, topics)
				assert.Empty(t, next)
			},
		},
		{
			name: "ListTopics_WithItems",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				mustCreateTopic(t, b, "topic-a")
				mustCreateTopic(t, b, "topic-b")
				topics, next, err := b.ListTopics("")
				require.NoError(t, err)
				assert.Len(t, topics, 2)
				assert.Empty(t, next)
			},
		},
		{
			name: "ListTopics_InvalidToken",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				_, _, err := b.ListTopics("not-base64!!!")
				require.ErrorIs(t, err, sns.ErrInvalidParameter)
			},
		},
		{
			name: "GetTopicAttributes_Success",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				arn := mustCreateTopic(t, b, "attr-topic")
				attrs, err := b.GetTopicAttributes(arn)
				require.NoError(t, err)
				assert.Equal(t, arn, attrs["TopicArn"])
			},
		},
		{
			name: "GetTopicAttributes_NotFound",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				_, err := b.GetTopicAttributes("arn:aws:sns:us-east-1:000000000000:missing")
				require.ErrorIs(t, err, sns.ErrTopicNotFound)
			},
		},
		{
			name: "SetTopicAttributes_Success",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				arn := mustCreateTopic(t, b, "set-topic")
				require.NoError(t, b.SetTopicAttributes(arn, "DisplayName", "Hello"))
				attrs, err := b.GetTopicAttributes(arn)
				require.NoError(t, err)
				assert.Equal(t, "Hello", attrs["DisplayName"])
			},
		},
		{
			name: "SetTopicAttributes_NotFound",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				err := b.SetTopicAttributes("arn:aws:sns:us-east-1:000000000000:missing", "X", "Y")
				require.ErrorIs(t, err, sns.ErrTopicNotFound)
			},
		},
		{
			name: "Subscribe_Success",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				arn := mustCreateTopic(t, b, "sub-topic")
				sub, err := b.Subscribe(arn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q", "")
				require.NoError(t, err)
				assert.Contains(t, sub.SubscriptionArn, "sub-topic")
				assert.Equal(t, arn, sub.TopicArn)
			},
		},
		{
			name: "Subscribe_TopicNotFound",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				_, err := b.Subscribe("arn:aws:sns:us-east-1:000000000000:missing", "sqs", "x", "")
				require.ErrorIs(t, err, sns.ErrTopicNotFound)
			},
		},
		{
			name: "Unsubscribe_Success",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				arn := mustCreateTopic(t, b, "unsub-topic")
				subArn := mustSubscribe(t, b, arn, "sqs", "x")
				require.NoError(t, b.Unsubscribe(subArn))
				assert.Empty(t, b.ListAllSubscriptions())
			},
		},
		{
			name: "Unsubscribe_NotFound",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				err := b.Unsubscribe("arn:aws:sns:us-east-1:000000000000:x:missing")
				require.ErrorIs(t, err, sns.ErrSubscriptionNotFound)
			},
		},
		{
			name: "ListSubscriptions_Empty",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				subs, next, err := b.ListSubscriptions("")
				require.NoError(t, err)
				assert.Empty(t, subs)
				assert.Empty(t, next)
			},
		},
		{
			name: "ListSubscriptions_WithItems",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				arn := mustCreateTopic(t, b, "ls-topic")
				mustSubscribe(t, b, arn, "sqs", "x")
				mustSubscribe(t, b, arn, "https", "https://example.com")
				subs, next, err := b.ListSubscriptions("")
				require.NoError(t, err)
				assert.Len(t, subs, 2)
				assert.Empty(t, next)
			},
		},
		{
			name: "ListSubscriptionsByTopic_Success",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				arn1 := mustCreateTopic(t, b, "lstt-1")
				arn2 := mustCreateTopic(t, b, "lstt-2")
				mustSubscribe(t, b, arn1, "sqs", "x")
				mustSubscribe(t, b, arn2, "sqs", "y")
				subs, _, err := b.ListSubscriptionsByTopic(arn1, "")
				require.NoError(t, err)
				assert.Len(t, subs, 1)
				assert.Equal(t, arn1, subs[0].TopicArn)
			},
		},
		{
			name: "ListSubscriptionsByTopic_NotFound",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				_, _, err := b.ListSubscriptionsByTopic("arn:aws:sns:us-east-1:000000000000:missing", "")
				require.ErrorIs(t, err, sns.ErrTopicNotFound)
			},
		},
		{
			name: "Publish_Success",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				arn := mustCreateTopic(t, b, "pub-topic")
				msgID, err := b.Publish(arn, "hello", "subject", nil)
				require.NoError(t, err)
				assert.NotEmpty(t, msgID)
			},
		},
		{
			name: "Publish_TopicNotFound",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				_, err := b.Publish("arn:aws:sns:us-east-1:000000000000:missing", "x", "", nil)
				require.ErrorIs(t, err, sns.ErrTopicNotFound)
			},
		},
		{
			name: "ListAllTopics",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				mustCreateTopic(t, b, "z-topic")
				mustCreateTopic(t, b, "a-topic")
				all := b.ListAllTopics()
				require.Len(t, all, 2)
				assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:a-topic", all[0].TopicArn)
				assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:z-topic", all[1].TopicArn)
			},
		},
		{
			name: "ListAllSubscriptions",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				t.Helper()
				arn := mustCreateTopic(t, b, "la-topic")
				mustSubscribe(t, b, arn, "sqs", "q")
				all := b.ListAllSubscriptions()
				require.Len(t, all, 1)
				assert.Equal(t, arn, all[0].TopicArn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, sns.NewInMemoryBackend())
		})
	}
}

// TestSNSHandler exercises the HTTP handler for all SNS actions.
func TestSNSHandler(t *testing.T) {
	t.Parallel()

	newHandler := func() (*sns.Handler, *sns.InMemoryBackend) {
		b := sns.NewInMemoryBackend()
		log := logger.NewLogger(slog.LevelDebug)

		return sns.NewHandler(b, log), b
	}

	t.Run("CreateTopic_Success", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":  {"CreateTopic"},
			"Version": {"2010-03-31"},
			"Name":    {"test-topic"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "test-topic")
	})

	t.Run("CreateTopic_MissingName", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":  {"CreateTopic"},
			"Version": {"2010-03-31"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "InvalidParameter")
	})

	t.Run("CreateTopic_Duplicate", func(t *testing.T) {
		t.Parallel()
		h, b := newHandler()
		mustCreateTopic(t, b, "dup")
		rec := snsPost(t, h, url.Values{
			"Action":  {"CreateTopic"},
			"Version": {"2010-03-31"},
			"Name":    {"dup"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "TopicAlreadyExists")
	})

	t.Run("DeleteTopic_Success", func(t *testing.T) {
		t.Parallel()
		h, b := newHandler()
		arn := mustCreateTopic(t, b, "del-topic")
		rec := snsPost(t, h, url.Values{
			"Action":   {"DeleteTopic"},
			"Version":  {"2010-03-31"},
			"TopicArn": {arn},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("DeleteTopic_NotFound", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":   {"DeleteTopic"},
			"Version":  {"2010-03-31"},
			"TopicArn": {"arn:aws:sns:us-east-1:000000000000:missing"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("DeleteTopic_MissingArn", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":  {"DeleteTopic"},
			"Version": {"2010-03-31"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("ListTopics_Empty", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":  {"ListTopics"},
			"Version": {"2010-03-31"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("ListTopics_WithItems", func(t *testing.T) {
		t.Parallel()
		h, b := newHandler()
		mustCreateTopic(t, b, "t1")
		rec := snsPost(t, h, url.Values{
			"Action":  {"ListTopics"},
			"Version": {"2010-03-31"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "t1")
	})

	t.Run("GetTopicAttributes_Success", func(t *testing.T) {
		t.Parallel()
		h, b := newHandler()
		arn := mustCreateTopic(t, b, "attr-topic")
		rec := snsPost(t, h, url.Values{
			"Action":   {"GetTopicAttributes"},
			"Version":  {"2010-03-31"},
			"TopicArn": {arn},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "TopicArn")
	})

	t.Run("GetTopicAttributes_NotFound", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":   {"GetTopicAttributes"},
			"Version":  {"2010-03-31"},
			"TopicArn": {"arn:aws:sns:us-east-1:000000000000:missing"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("GetTopicAttributes_MissingArn", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":  {"GetTopicAttributes"},
			"Version": {"2010-03-31"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("SetTopicAttributes_Success", func(t *testing.T) {
		t.Parallel()
		h, b := newHandler()
		arn := mustCreateTopic(t, b, "set-topic")
		rec := snsPost(t, h, url.Values{
			"Action":         {"SetTopicAttributes"},
			"Version":        {"2010-03-31"},
			"TopicArn":       {arn},
			"AttributeName":  {"DisplayName"},
			"AttributeValue": {"My Topic"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("SetTopicAttributes_NotFound", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":        {"SetTopicAttributes"},
			"Version":       {"2010-03-31"},
			"TopicArn":      {"arn:aws:sns:us-east-1:000000000000:missing"},
			"AttributeName": {"X"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("SetTopicAttributes_MissingParams", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":  {"SetTopicAttributes"},
			"Version": {"2010-03-31"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("Subscribe_Success", func(t *testing.T) {
		t.Parallel()
		h, b := newHandler()
		arn := mustCreateTopic(t, b, "sub-topic")
		rec := snsPost(t, h, url.Values{
			"Action":   {"Subscribe"},
			"Version":  {"2010-03-31"},
			"TopicArn": {arn},
			"Protocol": {"sqs"},
			"Endpoint": {"arn:aws:sqs:us-east-1:000000000000:q"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "SubscriptionArn")
	})

	t.Run("Subscribe_WithFilterPolicy", func(t *testing.T) {
		t.Parallel()
		h, b := newHandler()
		arn := mustCreateTopic(t, b, "filter-topic")
		rec := snsPost(t, h, url.Values{
			"Action":                   {"Subscribe"},
			"Version":                  {"2010-03-31"},
			"TopicArn":                 {arn},
			"Protocol":                 {"sqs"},
			"Endpoint":                 {"arn:aws:sqs:us-east-1:000000000000:q"},
			"Attributes.entry.1.key":   {"FilterPolicy"},
			"Attributes.entry.1.value": {`{"store":["example"]}`},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("Subscribe_NotFound", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":   {"Subscribe"},
			"Version":  {"2010-03-31"},
			"TopicArn": {"arn:aws:sns:us-east-1:000000000000:missing"},
			"Protocol": {"sqs"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("Subscribe_MissingParams", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":  {"Subscribe"},
			"Version": {"2010-03-31"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("Unsubscribe_Success", func(t *testing.T) {
		t.Parallel()
		h, b := newHandler()
		arn := mustCreateTopic(t, b, "unsub-topic")
		subArn := mustSubscribe(t, b, arn, "sqs", "q")
		rec := snsPost(t, h, url.Values{
			"Action":          {"Unsubscribe"},
			"Version":         {"2010-03-31"},
			"SubscriptionArn": {subArn},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("Unsubscribe_NotFound", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":          {"Unsubscribe"},
			"Version":         {"2010-03-31"},
			"SubscriptionArn": {"arn:aws:sns:us-east-1:000000000000:x:missing"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("Unsubscribe_MissingArn", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":  {"Unsubscribe"},
			"Version": {"2010-03-31"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("ListSubscriptions_Empty", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":  {"ListSubscriptions"},
			"Version": {"2010-03-31"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("ListSubscriptions_WithItems", func(t *testing.T) {
		t.Parallel()
		h, b := newHandler()
		arn := mustCreateTopic(t, b, "ls-topic")
		mustSubscribe(t, b, arn, "sqs", "q")
		rec := snsPost(t, h, url.Values{
			"Action":  {"ListSubscriptions"},
			"Version": {"2010-03-31"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "sqs")
	})

	t.Run("ListSubscriptionsByTopic_Success", func(t *testing.T) {
		t.Parallel()
		h, b := newHandler()
		arn := mustCreateTopic(t, b, "lstt")
		mustSubscribe(t, b, arn, "sqs", "q")
		rec := snsPost(t, h, url.Values{
			"Action":   {"ListSubscriptionsByTopic"},
			"Version":  {"2010-03-31"},
			"TopicArn": {arn},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "lstt")
	})

	t.Run("ListSubscriptionsByTopic_NotFound", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":   {"ListSubscriptionsByTopic"},
			"Version":  {"2010-03-31"},
			"TopicArn": {"arn:aws:sns:us-east-1:000000000000:missing"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("ListSubscriptionsByTopic_MissingArn", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":  {"ListSubscriptionsByTopic"},
			"Version": {"2010-03-31"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("Publish_Success", func(t *testing.T) {
		t.Parallel()
		h, b := newHandler()
		arn := mustCreateTopic(t, b, "pub-topic")
		rec := snsPost(t, h, url.Values{
			"Action":   {"Publish"},
			"Version":  {"2010-03-31"},
			"TopicArn": {arn},
			"Message":  {"hello world"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "MessageId")
	})

	t.Run("Publish_WithAttributes", func(t *testing.T) {
		t.Parallel()
		h, b := newHandler()
		arn := mustCreateTopic(t, b, "pub-attr-topic")
		rec := snsPost(t, h, url.Values{
			"Action":                         {"Publish"},
			"Version":                        {"2010-03-31"},
			"TopicArn":                       {arn},
			"Message":                        {"hello"},
			"Subject":                        {"test"},
			"MessageAttributes.entry.1.Name": {"attr1"},
			"MessageAttributes.entry.1.Value.DataType":    {"String"},
			"MessageAttributes.entry.1.Value.StringValue": {"val1"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("Publish_NotFound", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":   {"Publish"},
			"Version":  {"2010-03-31"},
			"TopicArn": {"arn:aws:sns:us-east-1:000000000000:missing"},
			"Message":  {"x"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("Publish_MissingParams", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":  {"Publish"},
			"Version": {"2010-03-31"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("PublishBatch_Success", func(t *testing.T) {
		t.Parallel()
		h, b := newHandler()
		arn := mustCreateTopic(t, b, "batch-topic")
		rec := snsPost(t, h, url.Values{
			"Action":                                 {"PublishBatch"},
			"Version":                                {"2010-03-31"},
			"TopicArn":                               {arn},
			"PublishBatchRequestEntries.member.1.Id": {"msg1"},
			"PublishBatchRequestEntries.member.1.Message": {"hello"},
			"PublishBatchRequestEntries.member.2.Id":      {"msg2"},
			"PublishBatchRequestEntries.member.2.Message": {"world"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "msg1")
		assert.Contains(t, rec.Body.String(), "msg2")
	})

	t.Run("PublishBatch_MissingTopicArn", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":  {"PublishBatch"},
			"Version": {"2010-03-31"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("PublishBatch_MissingEntries", func(t *testing.T) {
		t.Parallel()
		h, b := newHandler()
		arn := mustCreateTopic(t, b, "be-topic")
		rec := snsPost(t, h, url.Values{
			"Action":   {"PublishBatch"},
			"Version":  {"2010-03-31"},
			"TopicArn": {arn},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("PublishBatch_PartialFailure", func(t *testing.T) {
		t.Parallel()
		h, b := newHandler()
		arn := mustCreateTopic(t, b, "pf-topic")
		rec := snsPost(t, h, url.Values{
			"Action":  {"PublishBatch"},
			"Version": {"2010-03-31"},
			// TopicArn in entries but a bad one for each publish call inside batch
			"TopicArn":                                    {arn},
			"PublishBatchRequestEntries.member.1.Id":      {"ok"},
			"PublishBatchRequestEntries.member.1.Message": {"hello"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "Successful")
	})

	t.Run("UnknownAction", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":  {"FakeAction"},
			"Version": {"2010-03-31"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "InvalidAction")
	})

	t.Run("RouteMatcher_SNSRequest", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		e := echo.New()
		body := "Action=CreateTopic&Version=2010-03-31&Name=test"
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		assert.True(t, h.RouteMatcher()(c))
	})

	t.Run("RouteMatcher_NonSNSRequest", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		e := echo.New()
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"Action":"test"}`))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		assert.False(t, h.RouteMatcher()(c))
	})

	t.Run("RouteMatcher_WrongVersion", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		e := echo.New()
		body := "Action=CreateTopic&Version=2012-11-05&Name=test"
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		assert.False(t, h.RouteMatcher()(c))
	})

	t.Run("GetSupportedOperations", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		ops := h.GetSupportedOperations()
		assert.Contains(t, ops, "CreateTopic")
		assert.Contains(t, ops, "Publish")
		assert.Contains(t, ops, "Subscribe")
	})

	t.Run("ExtractOperation", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		e := echo.New()
		body := "Action=CreateTopic&Version=2010-03-31&Name=test"
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		assert.Equal(t, "CreateTopic", h.ExtractOperation(c))
	})

	t.Run("ExtractResource_ByArn", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
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
		h, _ := newHandler()
		e := echo.New()
		body := "Action=CreateTopic&Version=2010-03-31&Name=my-topic"
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		assert.Equal(t, "my-topic", h.ExtractResource(c))
	})

	t.Run("Name", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		assert.Equal(t, "SNS", h.Name())
	})

	t.Run("MatchPriority", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		assert.Equal(t, 80, h.MatchPriority())
	})
}

// TestCreateTopicXMLResponse verifies the XML structure of CreateTopic response.
func TestCreateTopicXMLResponse(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	h := sns.NewHandler(b, logger.NewLogger(slog.LevelDebug))

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

	_, err = b.Publish(tp.TopicArn, "test-message", "", nil)
	require.NoError(t, err)

	// Verify message was delivered
	select {
	case msg := <-received:
		assert.Equal(t, "test-message", msg)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("HTTP delivery did not arrive in time")
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
	_, err = b.Publish(tp.TopicArn, "test", "", nil)
	require.NoError(t, err)
}

// TestSNSHandlerAdditional covers edge cases not covered by TestSNSHandler.
func TestSNSHandlerAdditional(t *testing.T) {
	t.Parallel()

	newHandler := func() (*sns.Handler, *sns.InMemoryBackend) {
		b := sns.NewInMemoryBackend()
		log := logger.NewLogger(slog.LevelDebug)

		return sns.NewHandler(b, log), b
	}

	t.Run("Handler_ParseFormError", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		e := echo.New()
		// Use a broken content-type to cause ParseForm issues, but easiest is to use
		// a plain GET to the handler (no body, action = "", which leads to dispatch unknown)
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("ListTopics_InvalidToken", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":    {"ListTopics"},
			"Version":   {"2010-03-31"},
			"NextToken": {"!!!not-base64"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("ListSubscriptions_InvalidToken", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":    {"ListSubscriptions"},
			"Version":   {"2010-03-31"},
			"NextToken": {"!!!not-base64"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("ListSubscriptionsByTopic_InvalidToken", func(t *testing.T) {
		t.Parallel()
		h, b := newHandler()
		arn := mustCreateTopic(t, b, "tkn-topic")
		rec := snsPost(t, h, url.Values{
			"Action":    {"ListSubscriptionsByTopic"},
			"Version":   {"2010-03-31"},
			"TopicArn":  {arn},
			"NextToken": {"!!!not-base64"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("ExtractOperation_EmptyBody", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		e := echo.New()
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		assert.Equal(t, "Unknown", h.ExtractOperation(c))
	})

	t.Run("ExtractResource_Empty", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		e := echo.New()
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=ListTopics"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		assert.Empty(t, h.ExtractResource(c))
	})

	t.Run("CreateTopic_WithAttributes", func(t *testing.T) {
		t.Parallel()
		h, _ := newHandler()
		rec := snsPost(t, h, url.Values{
			"Action":                   {"CreateTopic"},
			"Version":                  {"2010-03-31"},
			"Name":                     {"attrs-topic"},
			"Attributes.entry.1.key":   {"DisplayName"},
			"Attributes.entry.1.value": {"My Display Name"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "attrs-topic")
	})

	t.Run("PublishBatch_PartialFailure_TopicNotFound", func(t *testing.T) {
		t.Parallel()
		h, b := newHandler()
		arn := mustCreateTopic(t, b, "pfail-topic")
		// Delete the topic so the publish fails for all entries
		require.NoError(t, b.DeleteTopic(arn))
		rec := snsPost(t, h, url.Values{
			"Action":                                 {"PublishBatch"},
			"Version":                                {"2010-03-31"},
			"TopicArn":                               {arn},
			"PublishBatchRequestEntries.member.1.Id": {"fail1"},
			"PublishBatchRequestEntries.member.1.Message": {"msg"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "NotFound")
	})
}

// errReadErr is the sentinel error returned by errReader.
var errRead = errors.New("read error")

// errBackendErr is the sentinel error returned by errBackend.
var errBackend2 = errors.New("unexpected internal backend failure")

// errReader is a helper that always fails on read.
type errReader struct{}

func (errReader) Read(_ []byte) (int, error) { return 0, errRead }
func (errReader) Close() error               { return nil }

// errBackend wraps InMemoryBackend and overrides CreateTopic to return a custom error.
type errBackend struct {
	*sns.InMemoryBackend
}

func (b *errBackend) CreateTopic(_ string, _ map[string]string) (*sns.Topic, error) {
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
		return sns.NewHandler(sns.NewInMemoryBackend(), logger.NewLogger(slog.LevelDebug))
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
	h := sns.NewHandler(b, logger.NewLogger(slog.LevelDebug))

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
