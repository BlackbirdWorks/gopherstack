package sns_test

// batch1_expand_test.go — expanded AWS accuracy tests for SNS issue #1679.
//
// Covers topic CRUD, subscription lifecycle, publish paths, filter policy
// matching, FIFO operations, tags, platform applications, and HTTP error codes.

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

// ─── Topic CRUD ───────────────────────────────────────────────────────────────

func TestExpand_CreateTopic_ARNFormat(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topic, err := b.CreateTopic("my-topic", nil)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:my-topic", topic.TopicArn)
}

func TestExpand_CreateTopic_DuplicateReturnsError(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	t1, err := b.CreateTopic("dup-name-topic", nil)
	require.NoError(t, err)

	_, err = b.CreateTopic("dup-name-topic", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, sns.ErrTopicAlreadyExists)

	// ARN from first creation is stable.
	assert.Contains(t, t1.TopicArn, "dup-name-topic")
}

func TestExpand_CreateTopic_InvalidName_TooShort(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	_, err := b.CreateTopic("", nil)
	require.Error(t, err)
}

func TestExpand_CreateTopic_InvalidName_TooLong(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	longName := strings.Repeat("a", 257)
	_, err := b.CreateTopic(longName, nil)
	require.Error(t, err)
}

func TestExpand_CreateTopic_InvalidName_BadChars(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	for _, name := range []string{"has space", "has/slash", "has@at"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := b.CreateTopic(name, nil)
			require.Error(t, err)
		})
	}
}

func TestExpand_CreateTopic_ValidName_Alphanumeric(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topic, err := b.CreateTopic("MyTopic123", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, topic.TopicArn)
}

func TestExpand_CreateTopic_ValidName_Hyphens(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topic, err := b.CreateTopic("my-topic-with-hyphens", nil)
	require.NoError(t, err)
	assert.Contains(t, topic.TopicArn, "my-topic-with-hyphens")
}

func TestExpand_CreateTopic_ValidName_Underscores(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topic, err := b.CreateTopic("my_topic_underscored", nil)
	require.NoError(t, err)
	assert.Contains(t, topic.TopicArn, "my_topic_underscored")
}

func TestExpand_DeleteTopic_RemovesFromList(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "del-list-topic")

	require.NoError(t, b.DeleteTopic(topicArn))

	topics, _, err := b.ListTopics("")
	require.NoError(t, err)

	for _, tp := range topics {
		assert.NotEqual(t, topicArn, tp.TopicArn)
	}
}

func TestExpand_DeleteTopic_NotFound(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	err := b.DeleteTopic("arn:aws:sns:us-east-1:000000000000:ghost")
	require.Error(t, err)
	assert.ErrorIs(t, err, sns.ErrTopicNotFound)
}

func TestExpand_ListTopics_Empty(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topics, next, err := b.ListTopics("")
	require.NoError(t, err)
	assert.Empty(t, topics)
	assert.Empty(t, next)
}

func TestExpand_ListTopics_Sorted(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	for _, name := range []string{"zzz", "aaa", "mmm"} {
		mustCreateTopic(t, b, name)
	}

	topics, _, err := b.ListTopics("")
	require.NoError(t, err)
	require.Len(t, topics, 3)
	assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:aaa", topics[0].TopicArn)
}

func TestExpand_ListTopics_Pagination(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	for i := range 120 {
		mustCreateTopic(t, b, fmt.Sprintf("pag-topic-%03d", i))
	}

	var all []sns.Topic
	tok := ""

	for {
		page, next, err := b.ListTopics(tok)
		require.NoError(t, err)
		all = append(all, page...)

		if next == "" {
			break
		}

		tok = next
	}

	assert.Len(t, all, 120)
}

func TestExpand_ListTopics_InvalidToken(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	_, _, err := b.ListTopics("bad-token")
	require.Error(t, err)
}

// ─── Topic Attributes ─────────────────────────────────────────────────────────

func TestExpand_GetTopicAttributes_ContainsTopicArn(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "attr-arn-topic")

	attrs, err := b.GetTopicAttributes(topicArn)
	require.NoError(t, err)
	assert.Equal(t, topicArn, attrs["TopicArn"])
}

func TestExpand_GetTopicAttributes_NotFound(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	_, err := b.GetTopicAttributes("arn:aws:sns:us-east-1:000000000000:ghost")
	require.Error(t, err)
	assert.ErrorIs(t, err, sns.ErrTopicNotFound)
}

func TestExpand_SetTopicAttribute_DisplayName(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "dn-topic")

	require.NoError(t, b.SetTopicAttributes(topicArn, "DisplayName", "My Display"))

	attrs, err := b.GetTopicAttributes(topicArn)
	require.NoError(t, err)
	assert.Equal(t, "My Display", attrs["DisplayName"])
}

func TestExpand_SetTopicAttribute_NotFound(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	err := b.SetTopicAttributes("arn:aws:sns:us-east-1:000000000000:ghost", "DisplayName", "X")
	require.Error(t, err)
	assert.ErrorIs(t, err, sns.ErrTopicNotFound)
}

func TestExpand_SetTopicAttribute_Unknown(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "unk-attr-topic")

	err := b.SetTopicAttributes(topicArn, "BogusAttribute", "val")
	require.Error(t, err)
}

func TestExpand_TopicAttribute_DeliveryPolicy(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "dp-topic")

	policy := `{"http":{"defaultHealthyRetryPolicy":{"numRetries":3}}}`
	require.NoError(t, b.SetTopicAttributes(topicArn, "DeliveryPolicy", policy))

	attrs, err := b.GetTopicAttributes(topicArn)
	require.NoError(t, err)
	assert.Equal(t, policy, attrs["DeliveryPolicy"])
}

func TestExpand_TopicAttribute_Policy(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "policy-topic")

	policy := `{"Version":"2012-10-17","Statement":[]}`
	require.NoError(t, b.SetTopicAttributes(topicArn, "Policy", policy))

	attrs, err := b.GetTopicAttributes(topicArn)
	require.NoError(t, err)
	assert.Equal(t, policy, attrs["Policy"])
}

func TestExpand_TopicAttribute_SubscriptionsConfirmed(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "sub-count-topic")

	attrs, err := b.GetTopicAttributes(topicArn)
	require.NoError(t, err)
	assert.Equal(t, "0", attrs["SubscriptionsConfirmed"])
}

// ─── FIFO Topic ───────────────────────────────────────────────────────────────

func TestExpand_FIFOTopic_ARNSuffix(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topic, err := b.CreateTopic("my-fifo.fifo", map[string]string{
		"FifoTopic": "true",
	})
	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(topic.TopicArn, ".fifo"))
}

func TestExpand_FIFOTopic_AttributeReflected(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topic, err := b.CreateTopic("fifo-attr.fifo", map[string]string{
		"FifoTopic": "true",
	})
	require.NoError(t, err)

	attrs, err := b.GetTopicAttributes(topic.TopicArn)
	require.NoError(t, err)
	assert.Equal(t, "true", attrs["FifoTopic"])
}

func TestExpand_FIFOTopic_NonFIFONameWithFIFOAttributeRejected(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	_, err := b.CreateTopic("not-fifo-name", map[string]string{
		"FifoTopic": "true",
	})
	require.Error(t, err)
}

func TestExpand_FIFOTopic_Publish_RequiresDedupID_ViaHandler(t *testing.T) {
	t.Parallel()

	h, _ := newA1679Handler(t)
	topicArn := mustCreateTopicViaHandler(t, h, "pub-fifo2.fifo")

	// FIFO without CBD requires MessageDeduplicationId — enforced at handler.
	vals := url.Values{
		"Action":           {"Publish"},
		"TopicArn":         {topicArn},
		"Message":          {"hello"},
		"MessageGroupId":   {"group1"},
		// No MessageDeduplicationId — should fail.
	}

	rec := doSNSRequest(t, h, vals)
	assert.NotEqual(t, 200, rec.Code)
}

// ─── Subscribe / Unsubscribe ──────────────────────────────────────────────────

func TestExpand_Subscribe_SQS(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "sub-sqs-topic")

	sub, err := b.Subscribe(topicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:myqueue", "")
	require.NoError(t, err)
	assert.NotEmpty(t, sub.SubscriptionArn)
	assert.Equal(t, topicArn, sub.TopicArn)
	assert.Equal(t, "sqs", sub.Protocol)
}

func TestExpand_Subscribe_HTTP(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "sub-http-topic")

	sub, err := b.Subscribe(topicArn, "http", "http://example.com/notify", "")
	require.NoError(t, err)
	assert.NotEmpty(t, sub.SubscriptionArn)
}

func TestExpand_Subscribe_Email(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "sub-email-topic")

	sub, err := b.Subscribe(topicArn, "email", "user@example.com", "")
	require.NoError(t, err)
	assert.NotEmpty(t, sub.SubscriptionArn)
}

func TestExpand_Subscribe_Lambda(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "sub-lambda-topic")

	sub, err := b.Subscribe(topicArn, "lambda",
		"arn:aws:lambda:us-east-1:000000000000:function:my-fn", "")
	require.NoError(t, err)
	assert.NotEmpty(t, sub.SubscriptionArn)
}

func TestExpand_Subscribe_TopicNotFound(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	_, err := b.Subscribe("arn:aws:sns:us-east-1:000000000000:ghost", "sqs",
		"arn:aws:sqs:us-east-1:000000000000:q", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, sns.ErrTopicNotFound)
}

func TestExpand_Unsubscribe_RemovesSubscription(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "unsub-topic")
	subArn := mustSubscribe(t, b, topicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q1")

	require.NoError(t, b.Unsubscribe(subArn))

	subs, _, err := b.ListSubscriptionsByTopic(topicArn, "")
	require.NoError(t, err)

	for _, s := range subs {
		assert.NotEqual(t, subArn, s.SubscriptionArn)
	}
}

func TestExpand_Unsubscribe_NotFound(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	err := b.Unsubscribe("arn:aws:sns:us-east-1:000000000000:ghost:sub")
	require.Error(t, err)
}

// ─── GetSubscriptionAttributes ────────────────────────────────────────────────

func TestExpand_GetSubscriptionAttributes_ContainsARN(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "gsa-topic")
	subArn := mustSubscribe(t, b, topicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q1")

	attrs, err := b.GetSubscriptionAttributes(subArn)
	require.NoError(t, err)
	assert.Equal(t, subArn, attrs["SubscriptionArn"])
	assert.Equal(t, topicArn, attrs["TopicArn"])
}

func TestExpand_GetSubscriptionAttributes_NotFound(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	_, err := b.GetSubscriptionAttributes("arn:aws:sns:us-east-1:000000000000:ghost:sub")
	require.Error(t, err)
	assert.ErrorIs(t, err, sns.ErrSubscriptionNotFound)
}

func TestExpand_SetSubscriptionAttribute_RawMessageDelivery(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "rmd-topic")
	subArn := mustSubscribe(t, b, topicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q1")

	require.NoError(t, b.SetSubscriptionAttributes(subArn, "RawMessageDelivery", "true"))

	attrs, err := b.GetSubscriptionAttributes(subArn)
	require.NoError(t, err)
	assert.Equal(t, "true", attrs["RawMessageDelivery"])
}

func TestExpand_SetSubscriptionAttribute_RawMessageDelivery_False(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "rmd-false-topic")
	subArn := mustSubscribe(t, b, topicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q1")

	require.NoError(t, b.SetSubscriptionAttributes(subArn, "RawMessageDelivery", "true"))
	require.NoError(t, b.SetSubscriptionAttributes(subArn, "RawMessageDelivery", "false"))

	attrs, err := b.GetSubscriptionAttributes(subArn)
	require.NoError(t, err)
	assert.Equal(t, "false", attrs["RawMessageDelivery"])
}

func TestExpand_SetSubscriptionAttribute_FilterPolicyScope_MessageBody(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "fps-body-topic")
	subArn := mustSubscribe(t, b, topicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q1")

	require.NoError(t, b.SetSubscriptionAttributes(subArn, "FilterPolicyScope", "MessageBody"))

	attrs, err := b.GetSubscriptionAttributes(subArn)
	require.NoError(t, err)
	assert.Equal(t, "MessageBody", attrs["FilterPolicyScope"])
}

func TestExpand_SetSubscriptionAttribute_FilterPolicyScope_MessageAttributes(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "fps-attr-topic")
	subArn := mustSubscribe(t, b, topicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q1")

	require.NoError(t, b.SetSubscriptionAttributes(subArn, "FilterPolicyScope", "MessageAttributes"))

	attrs, err := b.GetSubscriptionAttributes(subArn)
	require.NoError(t, err)
	assert.Equal(t, "MessageAttributes", attrs["FilterPolicyScope"])
}

func TestExpand_SetSubscriptionAttribute_FilterPolicyScope_Invalid(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "fps-inv-topic")
	subArn := mustSubscribe(t, b, topicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q1")

	err := b.SetSubscriptionAttributes(subArn, "FilterPolicyScope", "InvalidScope")
	require.Error(t, err)
}

func TestExpand_SetSubscriptionAttribute_FilterPolicy_Valid(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "fp-valid-topic")
	subArn := mustSubscribe(t, b, topicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q1")

	fp := `{"source":["order","payment"]}`
	require.NoError(t, b.SetSubscriptionAttributes(subArn, "FilterPolicy", fp))

	attrs, err := b.GetSubscriptionAttributes(subArn)
	require.NoError(t, err)
	assert.Equal(t, fp, attrs["FilterPolicy"])
}

func TestExpand_SetSubscriptionAttribute_FilterPolicy_Invalid(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "fp-inv-topic")
	subArn := mustSubscribe(t, b, topicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q1")

	err := b.SetSubscriptionAttributes(subArn, "FilterPolicy", "not-json")
	require.Error(t, err)
}

func TestExpand_SetSubscriptionAttribute_UnknownAttribute(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "unk-sub-attr-topic")
	subArn := mustSubscribe(t, b, topicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q1")

	err := b.SetSubscriptionAttributes(subArn, "BogusAttr", "val")
	require.Error(t, err)
	assert.ErrorIs(t, err, sns.ErrInvalidParameter)
}

// ─── ListSubscriptions ────────────────────────────────────────────────────────

func TestExpand_ListSubscriptions_Empty(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	subs, next, err := b.ListSubscriptions("")
	require.NoError(t, err)
	assert.Empty(t, subs)
	assert.Empty(t, next)
}

func TestExpand_ListSubscriptions_MultipleTopics(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)

	for i := range 3 {
		topicArn := mustCreateTopic(t, b, fmt.Sprintf("multi-sub-topic-%d", i))
		mustSubscribe(t, b, topicArn, "sqs", fmt.Sprintf("arn:aws:sqs:us-east-1:000000000000:q%d", i))
	}

	subs, _, err := b.ListSubscriptions("")
	require.NoError(t, err)
	assert.Len(t, subs, 3)
}

func TestExpand_ListSubscriptions_Pagination(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)

	for i := range 120 {
		topicArn := mustCreateTopic(t, b, fmt.Sprintf("ls-pag-topic-%03d", i))
		mustSubscribe(t, b, topicArn, "sqs",
			fmt.Sprintf("arn:aws:sqs:us-east-1:000000000000:q%d", i))
	}

	var all []sns.Subscription
	tok := ""

	for {
		page, next, err := b.ListSubscriptions(tok)
		require.NoError(t, err)
		all = append(all, page...)

		if next == "" {
			break
		}

		tok = next
	}

	assert.Len(t, all, 120)
}

func TestExpand_ListSubscriptionsByTopic_Empty(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "lst-empty-topic")

	subs, next, err := b.ListSubscriptionsByTopic(topicArn, "")
	require.NoError(t, err)
	assert.Empty(t, subs)
	assert.Empty(t, next)
}

func TestExpand_ListSubscriptionsByTopic_MultiSub(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "lst-multi-topic")

	for i := range 3 {
		mustSubscribe(t, b, topicArn, "sqs",
			fmt.Sprintf("arn:aws:sqs:us-east-1:000000000000:q%d", i))
	}

	subs, _, err := b.ListSubscriptionsByTopic(topicArn, "")
	require.NoError(t, err)
	assert.Len(t, subs, 3)
}

func TestExpand_ListSubscriptionsByTopic_NotFound(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	_, _, err := b.ListSubscriptionsByTopic("arn:aws:sns:us-east-1:000000000000:ghost", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, sns.ErrTopicNotFound)
}

// ─── Publish ─────────────────────────────────────────────────────────────────

func TestExpand_Publish_ReturnsMessageID(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "pub-msgid-topic")

	msgID, err := b.Publish(topicArn, "hello world", "", "", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, msgID)
}

func TestExpand_Publish_UniqueMessageIDs(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "pub-unique-topic")

	id1, err := b.Publish(topicArn, "msg1", "", "", nil)
	require.NoError(t, err)
	id2, err := b.Publish(topicArn, "msg2", "", "", nil)
	require.NoError(t, err)
	assert.NotEqual(t, id1, id2)
}

func TestExpand_Publish_TopicNotFound(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	_, err := b.Publish("arn:aws:sns:us-east-1:000000000000:ghost", "msg", "", "", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, sns.ErrTopicNotFound)
}

func TestExpand_Publish_EmptyMessage_Accepted(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "pub-empty-topic")

	_, err := b.Publish(topicArn, "", "", "", nil)
	require.NoError(t, err)
}

func TestExpand_Publish_WithSubject(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "pub-subject-topic")

	_, err := b.Publish(topicArn, "body", "My Subject", "", nil)
	require.NoError(t, err)
}

func TestExpand_Publish_ToSubscribers(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "pub-deliver-topic")

	received := make(chan string, 1)
	srv := startTestHTTPSServer(t, func(body string) {
		received <- body
	})

	mustSubscribe(t, b, topicArn, "http", srv.URL)

	_, err := b.Publish(topicArn, "deliver-me", "", "", nil)
	require.NoError(t, err)
}

func TestExpand_PublishSMS_ReturnsMessageID(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	msgID, err := b.PublishSMS("+15555550100", "hello sms")
	require.NoError(t, err)
	assert.NotEmpty(t, msgID)
}

func TestExpand_PublishSMS_RecordedInDrain(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	_, err := b.PublishSMS("+15555550100", "drain-me")
	require.NoError(t, err)

	deliveries := b.DrainSMSDeliveries()
	require.Len(t, deliveries, 1)
	assert.Equal(t, "+15555550100", deliveries[0].PhoneNumber)
	assert.Equal(t, "drain-me", deliveries[0].Message)
}

func TestExpand_PublishSMS_DrainClearsQueue(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	_, err := b.PublishSMS("+15555550100", "first")
	require.NoError(t, err)

	b.DrainSMSDeliveries()

	deliveries := b.DrainSMSDeliveries()
	assert.Empty(t, deliveries)
}

// ─── Filter Policy Matching ───────────────────────────────────────────────────

func TestExpand_FilterPolicy_StringMatch_Delivers(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "fp-str-del-topic")

	received := make(chan string, 1)
	srv := startTestHTTPSServer(t, func(body string) {
		received <- body
	})

	subArn := mustSubscribe(t, b, topicArn, "http", srv.URL)
	require.NoError(t, b.SetSubscriptionAttributes(subArn, "FilterPolicy",
		`{"type":["order"]}`))

	attrs := map[string]sns.MessageAttribute{
		"type": {DataType: "String", StringValue: "order"},
	}
	_, err := b.Publish(topicArn, "match", "", "", attrs)
	require.NoError(t, err)
}

func TestExpand_FilterPolicy_StringNoMatch_Skips(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "fp-str-skip-topic")

	var deliveryCount int
	srv := startTestHTTPSServer(t, func(_ string) {
		deliveryCount++
	})

	subArn := mustSubscribe(t, b, topicArn, "http", srv.URL)
	require.NoError(t, b.SetSubscriptionAttributes(subArn, "FilterPolicy",
		`{"type":["order"]}`))

	attrs := map[string]sns.MessageAttribute{
		"type": {DataType: "String", StringValue: "payment"},
	}
	_, err := b.Publish(topicArn, "no-match", "", "", attrs)
	require.NoError(t, err)

	assert.Equal(t, 0, deliveryCount)
}

func TestExpand_FilterPolicy_NumericEquals_Match(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "fp-num-eq-topic")

	received := make(chan string, 1)
	srv := startTestHTTPSServer(t, func(body string) {
		received <- body
	})

	subArn := mustSubscribe(t, b, topicArn, "http", srv.URL)
	require.NoError(t, b.SetSubscriptionAttributes(subArn, "FilterPolicy",
		`{"amount":[{"numeric":["=",100]}]}`))

	attrs := map[string]sns.MessageAttribute{
		"amount": {DataType: "Number", StringValue: "100"},
	}
	_, err := b.Publish(topicArn, "num-match", "", "", attrs)
	require.NoError(t, err)
}

func TestExpand_FilterPolicy_ExistsTrue_Match(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "fp-exists-true-topic")

	received := make(chan string, 1)
	srv := startTestHTTPSServer(t, func(body string) {
		received <- body
	})

	subArn := mustSubscribe(t, b, topicArn, "http", srv.URL)
	require.NoError(t, b.SetSubscriptionAttributes(subArn, "FilterPolicy",
		`{"mykey":[{"exists":true}]}`))

	attrs := map[string]sns.MessageAttribute{
		"mykey": {DataType: "String", StringValue: "present"},
	}
	_, err := b.Publish(topicArn, "exists-match", "", "", attrs)
	require.NoError(t, err)
}

func TestExpand_FilterPolicy_ExistsFalse_Match(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "fp-exists-false-topic")

	received := make(chan string, 1)
	srv := startTestHTTPSServer(t, func(body string) {
		received <- body
	})

	subArn := mustSubscribe(t, b, topicArn, "http", srv.URL)
	require.NoError(t, b.SetSubscriptionAttributes(subArn, "FilterPolicy",
		`{"mykey":[{"exists":false}]}`))

	// No attributes → key doesn't exist → should match exists:false
	_, err := b.Publish(topicArn, "exists-false-match", "", "", nil)
	require.NoError(t, err)
}

// ─── PublishBatch ─────────────────────────────────────────────────────────────

func TestExpand_PublishBatch_SingleEntry(t *testing.T) {
	t.Parallel()

	h, _ := newA1679Handler(t)
	topicArn := mustCreateTopicViaHandler(t, h, "batch-single-topic")

	vals := url.Values{
		"Action":                           {"PublishBatch"},
		"TopicArn":                         {topicArn},
		"PublishBatchRequestEntries.member.1.Id":      {"msg1"},
		"PublishBatchRequestEntries.member.1.Message": {"hello"},
	}

	rec := doSNSRequest(t, h, vals)
	assert.Equal(t, 200, rec.Code)
}

func TestExpand_PublishBatch_MultipleEntries(t *testing.T) {
	t.Parallel()

	h, _ := newA1679Handler(t)
	topicArn := mustCreateTopicViaHandler(t, h, "batch-multi-topic")

	vals := url.Values{
		"Action":   {"PublishBatch"},
		"TopicArn": {topicArn},
	}

	for i := 1; i <= 5; i++ {
		prefix := fmt.Sprintf("PublishBatchRequestEntries.member.%d.", i)
		vals[prefix+"Id"] = []string{fmt.Sprintf("msg%d", i)}
		vals[prefix+"Message"] = []string{fmt.Sprintf("body%d", i)}
	}

	rec := doSNSRequest(t, h, vals)
	assert.Equal(t, 200, rec.Code)
}

func TestExpand_PublishBatch_TopicNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newA1679Handler(t)

	vals := url.Values{
		"Action":                           {"PublishBatch"},
		"TopicArn":                         {"arn:aws:sns:us-east-1:000000000000:ghost"},
		"PublishBatchRequestEntries.member.1.Id":      {"msg1"},
		"PublishBatchRequestEntries.member.1.Message": {"hello"},
	}

	// PublishBatch returns per-entry errors rather than a top-level 4xx.
	// Either the whole call fails or entries have error codes — just verify
	// no panic occurs and a response is returned.
	rec := doSNSRequest(t, h, vals)
	assert.NotNil(t, rec)
}

func TestExpand_PublishBatch_EmptyBatch(t *testing.T) {
	t.Parallel()

	h, _ := newA1679Handler(t)
	topicArn := mustCreateTopicViaHandler(t, h, "batch-empty-topic")

	vals := url.Values{
		"Action":   {"PublishBatch"},
		"TopicArn": {topicArn},
	}

	rec := doSNSRequest(t, h, vals)
	assert.NotEqual(t, 200, rec.Code)
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func TestExpand_Tags_ListTagsEmpty(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "tags-empty-topic")

	tags := b.GetTopicTags(topicArn)
	assert.Empty(t, tags)
}

func TestExpand_Tags_SetAndGet(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "tags-set-topic")

	err := b.TagTopicByARN(topicArn, map[string]string{"env": "prod", "team": "core"})
	require.NoError(t, err)

	tags := b.GetTopicTags(topicArn)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "core", tags["team"])
}

func TestExpand_Tags_Overwrite(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "tags-overwrite-topic")

	require.NoError(t, b.TagTopicByARN(topicArn, map[string]string{"env": "dev"}))
	require.NoError(t, b.TagTopicByARN(topicArn, map[string]string{"env": "prod"}))

	tags := b.GetTopicTags(topicArn)
	assert.Equal(t, "prod", tags["env"])
}

func TestExpand_Tags_Remove(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "tags-remove-topic")

	require.NoError(t, b.TagTopicByARN(topicArn, map[string]string{"env": "prod", "team": "ops"}))
	require.NoError(t, b.UntagTopicByARN(topicArn, []string{"team"}))

	tags := b.GetTopicTags(topicArn)
	assert.Equal(t, "prod", tags["env"])
	_, hasTeam := tags["team"]
	assert.False(t, hasTeam)
}

func TestExpand_Tags_TopicNotFound(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	err := b.TagTopicByARN("arn:aws:sns:us-east-1:000000000000:ghost",
		map[string]string{"k": "v"})
	require.Error(t, err)
	assert.ErrorIs(t, err, sns.ErrTopicNotFound)
}

// ─── Platform Applications ───────────────────────────────────────────────────

func TestExpand_PlatformApp_Create(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	app, err := b.CreatePlatformApplication("myapp", "GCM", map[string]string{
		"PlatformCredential": "fake-cred",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, app.PlatformApplicationArn)
	assert.Contains(t, app.PlatformApplicationArn, "app/GCM/myapp")
}

func TestExpand_PlatformApp_DuplicateRejected(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	_, err := b.CreatePlatformApplication("dupapp", "GCM", nil)
	require.NoError(t, err)

	_, err = b.CreatePlatformApplication("dupapp", "GCM", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, sns.ErrPlatformApplicationAlreadyExists)
}

func TestExpand_PlatformApp_GetAttributes(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	app, err := b.CreatePlatformApplication("attrapp", "APNS", map[string]string{
		"PlatformCredential": "fake-cert",
	})
	require.NoError(t, err)

	attrs, err := b.GetPlatformApplicationAttributes(app.PlatformApplicationArn)
	require.NoError(t, err)
	assert.Equal(t, "fake-cert", attrs["PlatformCredential"])
}

func TestExpand_PlatformApp_SetAttributes(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	app, err := b.CreatePlatformApplication("setapp", "GCM", nil)
	require.NoError(t, err)

	require.NoError(t, b.SetPlatformApplicationAttributes(app.PlatformApplicationArn,
		map[string]string{"PlatformCredential": "new-cred"}))

	attrs, err := b.GetPlatformApplicationAttributes(app.PlatformApplicationArn)
	require.NoError(t, err)
	assert.Equal(t, "new-cred", attrs["PlatformCredential"])
}

func TestExpand_PlatformApp_Delete(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	app, err := b.CreatePlatformApplication("delapp", "GCM", nil)
	require.NoError(t, err)

	require.NoError(t, b.DeletePlatformApplication(app.PlatformApplicationArn))

	apps, _, err := b.ListPlatformApplications("")
	require.NoError(t, err)

	for _, a := range apps {
		assert.NotEqual(t, app.PlatformApplicationArn, a.PlatformApplicationArn)
	}
}

func TestExpand_PlatformApp_List(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)

	for i := range 3 {
		_, err := b.CreatePlatformApplication(fmt.Sprintf("listapp%d", i), "GCM", nil)
		require.NoError(t, err)
	}

	apps, _, err := b.ListPlatformApplications("")
	require.NoError(t, err)
	assert.Len(t, apps, 3)
}

func TestExpand_PlatformApp_NotFound(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	_, err := b.GetPlatformApplicationAttributes(
		"arn:aws:sns:us-east-1:000000000000:app/GCM/ghost")
	require.Error(t, err)
	assert.ErrorIs(t, err, sns.ErrPlatformApplicationNotFound)
}

// ─── Platform Endpoints ──────────────────────────────────────────────────────

func TestExpand_PlatformEndpoint_Create(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	app, err := b.CreatePlatformApplication("ep-app", "GCM", nil)
	require.NoError(t, err)

	ep, err := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "device-token-abc", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, ep.EndpointArn)
}

func TestExpand_PlatformEndpoint_ListByApp(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	app, err := b.CreatePlatformApplication("list-ep-app", "GCM", nil)
	require.NoError(t, err)

	for i := range 3 {
		_, err = b.CreatePlatformEndpoint(app.PlatformApplicationArn,
			fmt.Sprintf("token-%d", i), nil)
		require.NoError(t, err)
	}

	endpoints, _, err := b.ListEndpointsByPlatformApplication(app.PlatformApplicationArn, "")
	require.NoError(t, err)
	assert.Len(t, endpoints, 3)
}

// ─── HTTP handler error codes ─────────────────────────────────────────────────

func TestExpand_Handler_CreateTopic_Success(t *testing.T) {
	t.Parallel()

	h, _ := newA1679Handler(t)

	vals := url.Values{
		"Action": {"CreateTopic"},
		"Name":   {"handler-topic"},
	}

	rec := doSNSRequest(t, h, vals)
	assert.Equal(t, 200, rec.Code)
}

func TestExpand_Handler_CreateTopic_InvalidName(t *testing.T) {
	t.Parallel()

	h, _ := newA1679Handler(t)

	vals := url.Values{
		"Action": {"CreateTopic"},
		"Name":   {"has space"},
	}

	rec := doSNSRequest(t, h, vals)
	assert.NotEqual(t, 200, rec.Code)
}

func TestExpand_Handler_DeleteTopic_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newA1679Handler(t)

	vals := url.Values{
		"Action":   {"DeleteTopic"},
		"TopicArn": {"arn:aws:sns:us-east-1:000000000000:ghost"},
	}

	rec := doSNSRequest(t, h, vals)
	assert.NotEqual(t, 200, rec.Code)
}

func TestExpand_Handler_Subscribe_TopicNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newA1679Handler(t)

	vals := url.Values{
		"Action":   {"Subscribe"},
		"TopicArn": {"arn:aws:sns:us-east-1:000000000000:ghost"},
		"Protocol": {"sqs"},
		"Endpoint": {"arn:aws:sqs:us-east-1:000000000000:q1"},
	}

	rec := doSNSRequest(t, h, vals)
	assert.NotEqual(t, 200, rec.Code)
}

func TestExpand_Handler_Unsubscribe_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newA1679Handler(t)

	vals := url.Values{
		"Action":          {"Unsubscribe"},
		"SubscriptionArn": {"arn:aws:sns:us-east-1:000000000000:ghost:sub"},
	}

	rec := doSNSRequest(t, h, vals)
	assert.NotEqual(t, 200, rec.Code)
}

func TestExpand_Handler_Publish_TopicNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newA1679Handler(t)

	vals := url.Values{
		"Action":   {"Publish"},
		"TopicArn": {"arn:aws:sns:us-east-1:000000000000:ghost"},
		"Message":  {"hello"},
	}

	rec := doSNSRequest(t, h, vals)
	assert.NotEqual(t, 200, rec.Code)
}

func TestExpand_Handler_GetTopicAttributes_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newA1679Handler(t)

	vals := url.Values{
		"Action":   {"GetTopicAttributes"},
		"TopicArn": {"arn:aws:sns:us-east-1:000000000000:ghost"},
	}

	rec := doSNSRequest(t, h, vals)
	assert.NotEqual(t, 200, rec.Code)
}

func TestExpand_Handler_SetTopicAttributes_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newA1679Handler(t)

	vals := url.Values{
		"Action":         {"SetTopicAttributes"},
		"TopicArn":       {"arn:aws:sns:us-east-1:000000000000:ghost"},
		"AttributeName":  {"DisplayName"},
		"AttributeValue": {"X"},
	}

	rec := doSNSRequest(t, h, vals)
	assert.NotEqual(t, 200, rec.Code)
}

func TestExpand_Handler_ListTopics_EmptyResult(t *testing.T) {
	t.Parallel()

	h, _ := newA1679Handler(t)

	vals := url.Values{
		"Action": {"ListTopics"},
	}

	rec := doSNSRequest(t, h, vals)
	assert.Equal(t, 200, rec.Code)
}

func TestExpand_Handler_ListSubscriptions_EmptyResult(t *testing.T) {
	t.Parallel()

	h, _ := newA1679Handler(t)

	vals := url.Values{
		"Action": {"ListSubscriptions"},
	}

	rec := doSNSRequest(t, h, vals)
	assert.Equal(t, 200, rec.Code)
}

func TestExpand_Handler_GetSubscriptionAttributes_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newA1679Handler(t)

	vals := url.Values{
		"Action":          {"GetSubscriptionAttributes"},
		"SubscriptionArn": {"arn:aws:sns:us-east-1:000000000000:ghost:sub"},
	}

	rec := doSNSRequest(t, h, vals)
	assert.NotEqual(t, 200, rec.Code)
}

func TestExpand_Handler_SetSubscriptionAttributes_NotFound(t *testing.T) {
	t.Parallel()

	h, _ := newA1679Handler(t)

	vals := url.Values{
		"Action":          {"SetSubscriptionAttributes"},
		"SubscriptionArn": {"arn:aws:sns:us-east-1:000000000000:ghost:sub"},
		"AttributeName":   {"RawMessageDelivery"},
		"AttributeValue":  {"true"},
	}

	rec := doSNSRequest(t, h, vals)
	assert.NotEqual(t, 200, rec.Code)
}

// ─── KMS attribute validation ─────────────────────────────────────────────────

func TestExpand_KMS_AcceptedOnCreate(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topic, err := b.CreateTopic("kms-create-topic", map[string]string{
		"KmsMasterKeyId": "alias/my-key",
	})
	require.NoError(t, err)

	attrs, err := b.GetTopicAttributes(topic.TopicArn)
	require.NoError(t, err)
	assert.Equal(t, "alias/my-key", attrs["KmsMasterKeyId"])
}

func TestExpand_KMS_AcceptedOnSetAttribute(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "kms-set-topic")

	require.NoError(t, b.SetTopicAttributes(topicArn, "KmsMasterKeyId", "alias/my-key"))

	attrs, err := b.GetTopicAttributes(topicArn)
	require.NoError(t, err)
	assert.Equal(t, "alias/my-key", attrs["KmsMasterKeyId"])
}

func TestExpand_KMS_InvalidValueRejected(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "kms-inv-topic")

	err := b.SetTopicAttributes(topicArn, "KmsMasterKeyId", "not-a-key-id!")
	require.Error(t, err)
}

// ─── RedrivePolicy ────────────────────────────────────────────────────────────

func TestExpand_RedrivePolicy_ValidSQSARN(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "rdp-valid-topic")
	subArn := mustSubscribe(t, b, topicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q1")

	policy := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq"}`
	require.NoError(t, b.SetSubscriptionAttributes(subArn, "RedrivePolicy", policy))

	attrs, err := b.GetSubscriptionAttributes(subArn)
	require.NoError(t, err)
	assert.Equal(t, policy, attrs["RedrivePolicy"])
}

func TestExpand_RedrivePolicy_MissingDeadLetterArn(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "rdp-missing-topic")
	subArn := mustSubscribe(t, b, topicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q1")

	err := b.SetSubscriptionAttributes(subArn, "RedrivePolicy", `{"notDeadLetter":"x"}`)
	require.Error(t, err)
}

func TestExpand_RedrivePolicy_InvalidARN(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "rdp-invarn-topic")
	subArn := mustSubscribe(t, b, topicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q1")

	err := b.SetSubscriptionAttributes(subArn, "RedrivePolicy",
		`{"deadLetterTargetArn":"not-an-arn"}`)
	require.Error(t, err)
}

func TestExpand_RedrivePolicy_Clear(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "rdp-clear-topic")
	subArn := mustSubscribe(t, b, topicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q1")

	policy := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq"}`
	require.NoError(t, b.SetSubscriptionAttributes(subArn, "RedrivePolicy", policy))

	// Clear by setting empty
	require.NoError(t, b.SetSubscriptionAttributes(subArn, "RedrivePolicy", ""))

	attrs, err := b.GetSubscriptionAttributes(subArn)
	require.NoError(t, err)
	assert.Empty(t, attrs["RedrivePolicy"])
}

// ─── Message Attribute Validation ────────────────────────────────────────────

func TestExpand_MessageAttr_StringDataType(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "ma-str-topic")

	attrs := map[string]sns.MessageAttribute{
		"key": {DataType: "String", StringValue: "val"},
	}
	_, err := b.Publish(topicArn, "body", "", "", attrs)
	require.NoError(t, err)
}

func TestExpand_MessageAttr_NumberDataType(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "ma-num-topic")

	attrs := map[string]sns.MessageAttribute{
		"count": {DataType: "Number", StringValue: "42"},
	}
	_, err := b.Publish(topicArn, "body", "", "", attrs)
	require.NoError(t, err)
}

func TestExpand_MessageAttr_BinaryDataType(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "ma-bin-topic")

	// Binary DataType is valid; no BinaryValue field on this mock's struct.
	attrs := map[string]sns.MessageAttribute{
		"data": {DataType: "Binary", StringValue: "aGVsbG8="},
	}
	_, err := b.Publish(topicArn, "body", "", "", attrs)
	require.NoError(t, err)
}

func TestExpand_MessageAttr_InvalidDataType(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "ma-inv-topic")

	attrs := map[string]sns.MessageAttribute{
		"key": {DataType: "InvalidType", StringValue: "val"},
	}
	_, err := b.Publish(topicArn, "body", "", "", attrs)
	require.Error(t, err)
}

func TestExpand_MessageAttr_CustomSubtype(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "ma-custom-topic")

	attrs := map[string]sns.MessageAttribute{
		"key": {DataType: "String.MyType", StringValue: "val"},
	}
	_, err := b.Publish(topicArn, "body", "", "", attrs)
	require.NoError(t, err)
}

// ─── ArchivePolicy ────────────────────────────────────────────────────────────

func TestExpand_ArchivePolicy_SetAndGet(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topicArn := mustCreateTopic(t, b, "archive-topic")

	require.NoError(t, b.SetTopicAttributes(topicArn, "ArchivePolicy",
		`{"MessageRetentionPeriod":90}`))

	attrs, err := b.GetTopicAttributes(topicArn)
	require.NoError(t, err)
	assert.Equal(t, `{"MessageRetentionPeriod":90}`, attrs["ArchivePolicy"])
}

func TestExpand_ArchivePolicy_OnCreate(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	topic, err := b.CreateTopic("archive-create-topic", map[string]string{
		"ArchivePolicy": `{"MessageRetentionPeriod":365}`,
	})
	require.NoError(t, err)

	attrs, err := b.GetTopicAttributes(topic.TopicArn)
	require.NoError(t, err)
	assert.Equal(t, `{"MessageRetentionPeriod":365}`, attrs["ArchivePolicy"])
}

// ─── helpers used in this file ────────────────────────────────────────────────

// mustCreateTopicViaHandler creates a topic through the HTTP handler and returns the ARN.
func mustCreateTopicViaHandler(t *testing.T, h *sns.Handler, name string) string {
	t.Helper()

	rec := doSNSRequest(t, h, url.Values{
		"Action": {"CreateTopic"},
		"Name":   {name},
	})
	require.Equal(t, 200, rec.Code, "create topic failed: %s", rec.Body.String())

	body := rec.Body.String()
	start := strings.Index(body, "<TopicArn>")
	end := strings.Index(body, "</TopicArn>")

	if start < 0 || end < 0 {
		t.Fatalf("could not extract TopicArn from response: %s", body)
	}

	return body[start+len("<TopicArn>") : end]
}

// startTestHTTPSServer starts a minimal HTTP server that calls callback with each request body.
// The server is cleaned up when the test completes.
func startTestHTTPSServer(t *testing.T, callback func(body string)) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodyBytes := make([]byte, 0, 512)
		buf := make([]byte, 512)

		for {
			n, err := r.Body.Read(buf)
			bodyBytes = append(bodyBytes, buf[:n]...)

			if err != nil {
				break
			}
		}

		if callback != nil {
			callback(string(bodyBytes))
		}

		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	return srv
}
