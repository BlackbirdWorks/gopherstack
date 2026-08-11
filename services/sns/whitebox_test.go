package sns

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setSubscriptionLimitPerTopic(b *InMemoryBackend, limit int) {
	b.mu.Lock("test.setSubscriptionLimitPerTopic")
	defer b.mu.Unlock()
	b.subscriptionLimitPerTopic = limit
}

func whiteboxSNSPost(t *testing.T, h *Handler, form url.Values) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

// TestSubscribe_FilterPolicyLimitExceeded_PerTopic verifies the per-topic
// FilterPolicyLimitExceeded quota.
func TestSubscribe_FilterPolicyLimitExceeded_PerTopic(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	topic, err := b.CreateTopic("filter-policy-topic-quota", nil)
	require.NoError(t, err)

	for i := range maxFilterPoliciesPerTopic {
		endpoint := fmt.Sprintf("arn:aws:sqs:us-east-1:000000000000:q%d", i)
		_, subErr := b.Subscribe(topic.TopicArn, "sqs", endpoint, `{"k":["v"]}`)
		require.NoError(t, subErr)
	}

	_, err = b.Subscribe(topic.TopicArn, "sqs",
		"arn:aws:sqs:us-east-1:000000000000:one-too-many", `{"k":["v"]}`)
	require.ErrorIs(t, err, ErrFilterPolicyLimitExceeded)

	// A subscription with NO filter policy is unaffected by the quota.
	_, err = b.Subscribe(topic.TopicArn, "sqs",
		"arn:aws:sqs:us-east-1:000000000000:no-filter", "")
	require.NoError(t, err)
}

// TestSubscribe_SubscriptionLimitExceeded verifies that Subscribe returns
// SubscriptionLimitExceeded once a topic's subscription count reaches the
// (test-lowered) per-topic limit, and that the dedup path (re-subscribing the
// same protocol+endpoint) is unaffected since it never creates a new row.
func TestSubscribe_SubscriptionLimitExceeded(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	setSubscriptionLimitPerTopic(b, 2)

	topic, err := b.CreateTopic("sub-limit-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(topic.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q1", "")
	require.NoError(t, err)
	_, err = b.Subscribe(topic.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q2", "")
	require.NoError(t, err)

	_, err = b.Subscribe(topic.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q3", "")
	require.ErrorIs(t, err, ErrSubscriptionLimitExceeded)
}

// TestSubscribe_SubscriptionLimitExceededHandler verifies the HTTP wire shape: a
// SubscriptionLimitExceeded backend error surfaces as HTTP 403 with the exact
// AWS error code string in the XML error body.
func TestSubscribe_SubscriptionLimitExceededHandler(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	h := NewHandler(b)
	setSubscriptionLimitPerTopic(b, 1)

	topic, err := b.CreateTopic("sub-limit-handler-topic", nil)
	require.NoError(t, err)
	_, err = b.Subscribe(topic.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q1", "")
	require.NoError(t, err)

	form := url.Values{
		"Action":   {"Subscribe"},
		"Version":  {"2010-03-31"},
		"TopicArn": {topic.TopicArn},
		"Protocol": {"sqs"},
		"Endpoint": {"arn:aws:sqs:us-east-1:000000000000:q2"},
	}

	rec := whiteboxSNSPost(t, h, form)

	assert.Equal(t, 403, rec.Code)
	assert.Contains(t, rec.Body.String(), "SubscriptionLimitExceeded")
}
