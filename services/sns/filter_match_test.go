package sns_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFilterPolicyAnythingButMatchDelivery verifies "anything-but" delivers
// to subscribers when the attribute does NOT match the exclusion list.
func TestFilterPolicyAnythingButMatchDelivery(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("fp-ab-topic", nil)
	require.NoError(t, err)

	fp := `{"color":[{"anything-but":["red","green"]}]}`
	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, fp)
	require.NoError(t, err)

	// "blue" is not in the exclusion list → should be delivered.
	attrs := map[string]sns.MessageAttribute{
		"color": {DataType: "String", StringValue: "blue"},
	}
	_, err = b.Publish(tp.TopicArn, "blue-msg", "", "", attrs)
	require.NoError(t, err)

	select {
	case raw := <-received:
		var env snsNotificationEnvelope
		if json.Unmarshal([]byte(raw), &env) == nil {
			assert.Equal(t, "blue-msg", env.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Message should have been delivered for non-excluded color")
	}
}

// TestFilterPolicyAnythingButNoDelivery verifies "anything-but" blocks
// messages whose attribute matches the exclusion list.
func TestFilterPolicyAnythingButNoDelivery(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("fp-ab-no-topic", nil)
	require.NoError(t, err)

	fp := `{"color":[{"anything-but":["red"]}]}`
	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, fp)
	require.NoError(t, err)

	// "red" matches exclusion → should NOT be delivered.
	attrs := map[string]sns.MessageAttribute{
		"color": {DataType: "String", StringValue: "red"},
	}
	_, err = b.Publish(tp.TopicArn, "red-msg", "", "", attrs)
	require.NoError(t, err)

	select {
	case <-received:
		t.Fatal("Message with excluded color should not have been delivered")
	case <-time.After(300 * time.Millisecond):
		// Expected: no delivery.
	}
}

// TestFilterPolicyPrefixMatch verifies prefix filtering.
func TestFilterPolicyPrefixMatch(t *testing.T) {
	t.Parallel()

	received := make(chan string, 2)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("fp-prefix-topic", nil)
	require.NoError(t, err)

	fp := `{"source":[{"prefix":"order-"}]}`
	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, fp)
	require.NoError(t, err)

	// Matching prefix.
	attrs1 := map[string]sns.MessageAttribute{
		"source": {DataType: "String", StringValue: "order-123"},
	}
	_, err = b.Publish(tp.TopicArn, "order-msg", "", "", attrs1)
	require.NoError(t, err)

	// Non-matching prefix.
	attrs2 := map[string]sns.MessageAttribute{
		"source": {DataType: "String", StringValue: "shipment-123"},
	}
	_, err = b.Publish(tp.TopicArn, "shipment-msg", "", "", attrs2)
	require.NoError(t, err)

	select {
	case raw := <-received:
		var env snsNotificationEnvelope
		if json.Unmarshal([]byte(raw), &env) == nil {
			assert.Equal(t, "order-msg", env.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Prefix-matching message was not delivered")
	}

	// Second message should not arrive.
	select {
	case raw := <-received:
		var env snsNotificationEnvelope
		if json.Unmarshal([]byte(raw), &env) == nil {
			t.Fatalf("non-matching message was delivered: %s", env.Message)
		}
	case <-time.After(300 * time.Millisecond):
		// Expected: no delivery for non-matching.
	}
}

// TestFilterPolicySuffixMatch verifies suffix filtering.
func TestFilterPolicySuffixMatch(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("fp-suffix-topic", nil)
	require.NoError(t, err)

	fp := `{"filename":[{"suffix":".log"}]}`
	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, fp)
	require.NoError(t, err)

	attrs := map[string]sns.MessageAttribute{
		"filename": {DataType: "String", StringValue: "app.log"},
	}
	_, err = b.Publish(tp.TopicArn, "log-msg", "", "", attrs)
	require.NoError(t, err)

	select {
	case raw := <-received:
		var env snsNotificationEnvelope
		if json.Unmarshal([]byte(raw), &env) == nil {
			assert.Equal(t, "log-msg", env.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Suffix-matching message was not delivered")
	}
}

// TestFilterPolicyEqualsIgnoreCase verifies case-insensitive equality.
func TestFilterPolicyEqualsIgnoreCase(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("fp-eic-topic", nil)
	require.NoError(t, err)

	fp := `{"env":[{"equals-ignore-case":"production"}]}`
	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, fp)
	require.NoError(t, err)

	attrs := map[string]sns.MessageAttribute{
		"env": {DataType: "String", StringValue: "PRODUCTION"},
	}
	_, err = b.Publish(tp.TopicArn, "prod-msg", "", "", attrs)
	require.NoError(t, err)

	select {
	case raw := <-received:
		var env snsNotificationEnvelope
		if json.Unmarshal([]byte(raw), &env) == nil {
			assert.Equal(t, "prod-msg", env.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Case-insensitive match was not delivered")
	}
}

// TestFilterPolicyMessageBodyMatch verifies that a message body JSON
// field matching the filter policy is delivered.
func TestFilterPolicyMessageBodyMatch(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newTestBackend(t)
	tp, err := b.CreateTopic("body-filter-topic", nil)
	require.NoError(t, err)

	// Subscribe with MessageBody scope: filter on {"status":["active"]}.
	sub, err := b.Subscribe(tp.TopicArn, "http", ts.URL, `{"status":["active"]}`)
	require.NoError(t, err)
	require.NoError(t, b.SetSubscriptionAttributes(sub.SubscriptionArn, "FilterPolicyScope", "MessageBody"))

	// Publish a message whose body matches.
	_, err = b.Publish(tp.TopicArn, `{"status":"active","user":"alice"}`, "", "", nil)
	require.NoError(t, err)

	select {
	case raw := <-received:
		env := parseNotificationEnvelope(t, raw)
		assert.Contains(t, env.Message, "active")
	case <-time.After(2 * time.Second):
		t.Fatal("MessageBody-filtered message was not delivered")
	}
}

// TestFilterPolicyMessageBodyNoMatch verifies that a message body not
// matching the filter policy is NOT delivered.
func TestFilterPolicyMessageBodyNoMatch(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newTestBackend(t)
	tp, err := b.CreateTopic("body-filter-no-match-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "http", ts.URL, `{"status":["active"]}`)
	require.NoError(t, err)
	require.NoError(t, b.SetSubscriptionAttributes(sub.SubscriptionArn, "FilterPolicyScope", "MessageBody"))

	// Publish a message that does NOT match.
	_, err = b.Publish(tp.TopicArn, `{"status":"inactive","user":"bob"}`, "", "", nil)
	require.NoError(t, err)

	select {
	case <-received:
		t.Fatal("non-matching message body should not have been delivered")
	case <-time.After(300 * time.Millisecond):
		// Expected: no delivery.
	}
}

// TestFilterPolicyMessageBodyNonJSONBlocked verifies that a non-JSON
// message body is not delivered to a MessageBody-scoped subscription.
func TestFilterPolicyMessageBodyNonJSONBlocked(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newTestBackend(t)
	tp, err := b.CreateTopic("body-filter-nonjson-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "http", ts.URL, `{"event":["click"]}`)
	require.NoError(t, err)
	require.NoError(t, b.SetSubscriptionAttributes(sub.SubscriptionArn, "FilterPolicyScope", "MessageBody"))

	// Non-JSON message body.
	_, err = b.Publish(tp.TopicArn, "plain text message", "", "", nil)
	require.NoError(t, err)

	select {
	case <-received:
		t.Fatal("non-JSON body should not be delivered to MessageBody-scoped subscription")
	case <-time.After(300 * time.Millisecond):
		// Expected: no delivery.
	}
}

// TestFilterPolicyMessageBodyNoFilter verifies that an empty filter
// policy delivers regardless of scope.
func TestFilterPolicyMessageBodyNoFilter(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newTestBackend(t)
	tp, err := b.CreateTopic("body-nopolicy-topic", nil)
	require.NoError(t, err)

	// No filter policy but MessageBody scope.
	sub, err := b.Subscribe(tp.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)
	require.NoError(t, b.SetSubscriptionAttributes(sub.SubscriptionArn, "FilterPolicyScope", "MessageBody"))

	_, err = b.Publish(tp.TopicArn, "any message", "", "", nil)
	require.NoError(t, err)

	select {
	case <-received:
		// Expected: delivered.
	case <-time.After(2 * time.Second):
		t.Fatal("message without filter policy should have been delivered")
	}
}

// TestFilterPolicyScopeMessageBodyVsAttributesSameSubscriber verifies
// that two subscribers on the same topic — one with MessageAttributes scope
// and one with MessageBody scope — each receive independently filtered messages.
func TestFilterPolicyScopeMessageBodyVsAttributesSameSubscriber(t *testing.T) {
	t.Parallel()

	attrReceived := make(chan string, 1)
	bodyReceived := make(chan string, 1)

	tsAttr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		attrReceived <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer tsAttr.Close()

	tsBody := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodyReceived <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer tsBody.Close()

	b := newTestBackend(t)
	tp, err := b.CreateTopic("scope-dual-topic", nil)
	require.NoError(t, err)

	// Subscriber 1: MessageAttributes scope, filter on attribute "type"="order".
	subAttr, err := b.Subscribe(tp.TopicArn, "http", tsAttr.URL, `{"type":["order"]}`)
	require.NoError(t, err)
	// Default scope = MessageAttributes, no explicit set needed.
	_ = subAttr

	// Subscriber 2: MessageBody scope, filter on body field "event"="checkout".
	subBody, err := b.Subscribe(tp.TopicArn, "http", tsBody.URL, `{"event":["checkout"]}`)
	require.NoError(t, err)
	require.NoError(t, b.SetSubscriptionAttributes(subBody.SubscriptionArn, "FilterPolicyScope", "MessageBody"))

	// Publish: message body matches body filter; attribute matches attr filter.
	attrs := map[string]sns.MessageAttribute{
		"type": {DataType: "String", StringValue: "order"},
	}
	_, err = b.Publish(tp.TopicArn, `{"event":"checkout"}`, "", "", attrs)
	require.NoError(t, err)

	for i := range 2 {
		select {
		case <-attrReceived:
		case <-bodyReceived:
		case <-time.After(2 * time.Second):
			t.Fatalf("subscriber %d did not receive message in time", i+1)
		}
	}
}

// TestFilterPolicyMessageBodyMatcherUnit tests the exported evaluator directly.
func TestFilterPolicyMessageBodyMatcherUnit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		policy  string
		message string
		want    bool
	}{
		{
			name:    "exact_string_match",
			policy:  `{"status":["active"]}`,
			message: `{"status":"active"}`,
			want:    true,
		},
		{
			name:    "exact_string_no_match",
			policy:  `{"status":["active"]}`,
			message: `{"status":"inactive"}`,
			want:    false,
		},
		{
			name:    "prefix_match",
			policy:  `{"id":[{"prefix":"order-"}]}`,
			message: `{"id":"order-123"}`,
			want:    true,
		},
		{
			name:    "prefix_no_match",
			policy:  `{"id":[{"prefix":"order-"}]}`,
			message: `{"id":"invoice-123"}`,
			want:    false,
		},
		{
			name:    "anything_but",
			policy:  `{"status":[{"anything-but":["deleted"]}]}`,
			message: `{"status":"active"}`,
			want:    true,
		},
		{
			name:    "anything_but_excluded",
			policy:  `{"status":[{"anything-but":["deleted"]}]}`,
			message: `{"status":"deleted"}`,
			want:    false,
		},
		{
			name:    "non_json_body",
			policy:  `{"key":["val"]}`,
			message: "not json",
			want:    false,
		},
		{
			name:    "missing_key",
			policy:  `{"key":[{"exists":false}]}`,
			message: `{"other":"value"}`,
			want:    true,
		},
		{
			name:    "exists_true_missing",
			policy:  `{"key":[{"exists":true}]}`,
			message: `{"other":"value"}`,
			want:    false,
		},
		{
			name:    "numeric_gt",
			policy:  `{"score":[{"numeric":[">",90]}]}`,
			message: `{"score":95}`,
			want:    true,
		},
		{
			name:    "numeric_gt_no_match",
			policy:  `{"score":[{"numeric":[">",90]}]}`,
			message: `{"score":80}`,
			want:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := sns.MatchesFilterPolicyMessageBodyForTest(tc.policy, tc.message)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestFilterPolicyScopeRoundTripViaHandler verifies FilterPolicyScope
// can be set and retrieved via the HTTP handler.
func TestFilterPolicyScopeRoundTripViaHandler(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerPair(t)
	tp, err := b.CreateTopic("scope-rt-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:scope-q", "")
	require.NoError(t, err)

	rec := doTestRequest(t, h, url.Values{
		"Action":          {"SetSubscriptionAttributes"},
		"SubscriptionArn": {sub.SubscriptionArn},
		"AttributeName":   {"FilterPolicyScope"},
		"AttributeValue":  {"MessageBody"},
		"Version":         {"2010-03-31"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doTestRequest(t, h, url.Values{
		"Action":          {"GetSubscriptionAttributes"},
		"SubscriptionArn": {sub.SubscriptionArn},
		"Version":         {"2010-03-31"},
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "FilterPolicyScope")
	assert.Contains(t, rec2.Body.String(), "MessageBody")
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
