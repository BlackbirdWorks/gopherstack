package sns_test

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

func newTestBackend(t *testing.T) *sns.InMemoryBackend {
	t.Helper()

	return sns.NewInMemoryBackendWithContext(t.Context(), "000000000000", "us-east-1")
}

func newTestHandlerPair(t *testing.T) (*sns.Handler, *sns.InMemoryBackend) {
	t.Helper()

	b := newTestBackend(t)

	return sns.NewHandler(b), b
}

func doTestRequest(t *testing.T, h *sns.Handler, vals url.Values) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	body := vals.Encode()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	_ = h.Handler()(c)

	return rec
}

// publishResponseSequenceNumber extracts the SequenceNumber from an XML PublishResponse body.
func publishResponseSequenceNumber(t *testing.T, body string) string {
	t.Helper()

	type result struct {
		XMLName        xml.Name `xml:"PublishResult"`
		SequenceNumber string   `xml:"SequenceNumber"`
	}
	type resp struct {
		XMLName xml.Name `xml:"PublishResponse"`
		Result  result   `xml:"PublishResult"`
	}

	var r resp
	require.NoError(t, xml.Unmarshal([]byte(body), &r), "parse PublishResponse XML")

	return r.Result.SequenceNumber
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

// Test_PublishBatchEntryMessageAttributesFilterPolicy verifies that a
// PublishBatch entry's MessageAttributes are read from the correct AWS
// query-protocol field name — PublishBatchRequestEntries.member.N.MessageAttributes.entry.M.*
// — and applied to subscription FilterPolicy matching. A prior version parsed
// PublishBatchRequestEntries.member.N.entry.M.* (missing the "MessageAttributes."
// segment), so every batch entry's message attributes were silently dropped and
// no batch publish could ever match an attribute-based FilterPolicy.
func TestPublishBatchEntryMessageAttributesFilterPolicy(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		colorValue  string
		wantMatched bool
	}{
		{name: "matching attribute delivers", colorValue: "red", wantMatched: true},
		{name: "non-matching attribute is filtered out", colorValue: "blue", wantMatched: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Each subtest builds its own isolated backend, topic, subscriber, and server.
			received := make(chan string, 1)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				received <- extractSNSHTTPMessage(string(body))
				w.WriteHeader(http.StatusOK)
			}))
			defer ts.Close()

			b := sns.NewInMemoryBackend()
			h := sns.NewHandler(b)
			topicArn := mustCreateTopic(t, b, "batch-attr-topic")

			sub, err := b.Subscribe(topicArn, "http", ts.URL, `{"color":["red"]}`)
			require.NoError(t, err)
			require.NotEmpty(t, sub.SubscriptionArn)

			rec := snsPost(t, h, url.Values{
				"Action":                                 {"PublishBatch"},
				"Version":                                {"2010-03-31"},
				"TopicArn":                               {topicArn},
				"PublishBatchRequestEntries.member.1.Id": {"entry-1"},
				"PublishBatchRequestEntries.member.1.Message":                                     {"batch-message"},
				"PublishBatchRequestEntries.member.1.MessageAttributes.entry.1.Name":              {"color"},
				"PublishBatchRequestEntries.member.1.MessageAttributes.entry.1.Value.DataType":    {"String"},
				"PublishBatchRequestEntries.member.1.MessageAttributes.entry.1.Value.StringValue": {tc.colorValue},
			})
			require.Equal(t, http.StatusOK, rec.Code)
			require.Contains(t, rec.Body.String(), "entry-1")

			sns.WaitDeliveriesForTest(b)

			select {
			case msg := <-received:
				if !tc.wantMatched {
					t.Fatalf("message delivered despite non-matching FilterPolicy: %q", msg)
				}
				assert.Equal(t, "batch-message", msg)
			case <-time.After(300 * time.Millisecond):
				if tc.wantMatched {
					t.Fatal("message was not delivered despite matching FilterPolicy")
				}
			}
		})
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

// TestAudit_MixedProtocols confirms that a topic with mixed-protocol subscriptions
// (SMS + application + email) delivers independently to each protocol.
func TestMixedProtocolDelivery(t *testing.T) {
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

// TestOps2_PublishSubjectTooLongRejected verifies that Publish rejects a
// Subject longer than 100 characters with InvalidParameter.
func TestPublishSubjectTooLongRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		subject string
		wantOK  bool
	}{
		{name: "exactly 100 chars", subject: strings.Repeat("A", 100), wantOK: true},
		{name: "101 chars rejected", subject: strings.Repeat("A", 101), wantOK: false},
		{name: "empty subject ok", subject: "", wantOK: true},
		{name: "printable ASCII ok", subject: "Order #1234 shipped!", wantOK: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			tp, err := b.CreateTopic("subj-len-topic", nil)
			require.NoError(t, err)

			_, err = b.Publish(tp.TopicArn, "hello", tc.subject, "", nil)

			if tc.wantOK {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "Subject must be no longer than 100 characters")
			}
		})
	}
}

// TestOps2_PublishSubjectNonASCIIRejected verifies that Publish rejects a
// Subject containing non-printable or non-ASCII characters.
func TestPublishSubjectNonASCIIRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		subject string
		wantOK  bool
	}{
		{name: "control char tab rejected", subject: "Hello\tWorld", wantOK: false},
		{name: "control char newline rejected", subject: "Hello\nWorld", wantOK: false},
		{name: "high-byte UTF-8 rejected", subject: "café", wantOK: false},
		{name: "null byte rejected", subject: "Hello\x00", wantOK: false},
		{name: "all printable ASCII ok", subject: "~!@#$%^&*()_+-=[]{}|;':\",./<>?", wantOK: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)
			tp, err := b.CreateTopic("subj-ascii-topic", nil)
			require.NoError(t, err)

			_, err = b.Publish(tp.TopicArn, "hello", tc.subject, "", nil)

			if tc.wantOK {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "InvalidParameter")
			}
		})
	}
}

// TestOps2_PublishSubjectTooLongViaHandler verifies the Subject validation is
// enforced at the HTTP handler level.
func TestPublishSubjectTooLongViaHandler(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandlerPair(t)

	createRec := doTestRequest(t, h, url.Values{"Action": {"CreateTopic"}, "Name": {"subj-handler-topic"}})
	require.Equal(t, http.StatusOK, createRec.Code)

	matches := regexp.MustCompile(`<TopicArn>(arn:[^<]+)</TopicArn>`).FindStringSubmatch(createRec.Body.String())
	require.Len(t, matches, 2)
	topicArn := matches[1]

	rec := doTestRequest(t, h, url.Values{
		"Action":   {"Publish"},
		"TopicArn": {topicArn},
		"Message":  {"hello"},
		"Subject":  {strings.Repeat("X", 101)},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameter")
}

// TestPublishBatchTopicNotFoundTopLevelError verifies that PublishBatch
// returns a top-level 400 NotFoundException when the topic does not exist,
// rather than per-entry failures.
func TestPublishBatchTopicNotFoundTopLevelError(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandlerPair(t)

	rec := doTestRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {"arn:aws:sns:us-east-1:000000000000:no-such-topic"},
		"PublishBatchRequestEntries.member.1.Id": {"e1"},
		"PublishBatchRequestEntries.member.1.Message": {"hello"},
		"Version": {"2010-03-31"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"PublishBatch to non-existent topic must return 400 top-level error")
	assert.Contains(t, rec.Body.String(), "NotFound")
}

// TestPublishBatchExistingTopicStillWorks verifies that after the
// topic existence pre-check, PublishBatch still works correctly for valid topics.
func TestPublishBatchExistingTopicStillWorks(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerPair(t)
	tp, err := b.CreateTopic("batch-precheck-topic", nil)
	require.NoError(t, err)

	rec := doTestRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {"e1"},
		"PublishBatchRequestEntries.member.1.Message": {"hello"},
		"Version": {"2010-03-31"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "e1")
}

// TestIssue6_BatchMessageStructureJSON verifies that each batch entry can
// carry MessageStructure=json and that the per-protocol payload is delivered.
func TestBatchMessageStructureJSON(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("batch-json-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(tp.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)
	_ = sub

	msgJSON := `{"http":"http-specific","default":"fallback"}`

	rec := doSNSRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {"e1"},
		"PublishBatchRequestEntries.member.1.Message":          {msgJSON},
		"PublishBatchRequestEntries.member.1.MessageStructure": {"json"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "<Code>")

	select {
	case raw := <-received:
		// The delivered body should be the http-protocol-specific value.
		var env snsNotificationEnvelope
		if json.Unmarshal([]byte(raw), &env) == nil {
			assert.Equal(t, "http-specific", env.Message)
		} else {
			assert.Equal(t, "http-specific", raw)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP delivery did not arrive")
	}
}

// TestIssue6_BatchMessageStructureDefault verifies that batch entries without
// explicit MessageStructure are delivered as plain text.
func TestBatchMessageStructureDefault(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("batch-plain-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)

	rec := doSNSRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {"e1"},
		"PublishBatchRequestEntries.member.1.Message": {"plain-text"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "<Code>")

	select {
	case raw := <-received:
		var env snsNotificationEnvelope
		if json.Unmarshal([]byte(raw), &env) == nil {
			assert.Equal(t, "plain-text", env.Message)
		} else {
			assert.Equal(t, "plain-text", raw)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP delivery did not arrive")
	}
}

// TestIssue10_BatchEntryIDTooLong verifies the InvalidBatchEntryId code.
func TestBatchEntryIDTooLong(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("batch-id-long-topic", nil)
	require.NoError(t, err)

	longID := strings.Repeat("x", 81)

	rec := doSNSRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {longID},
		"PublishBatchRequestEntries.member.1.Message": {"msg"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidBatchEntryId")
}

// TestIssue10_BatchDuplicateIDs verifies the BatchEntryIdsNotDistinct code.
func TestBatchDuplicateIDs(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("batch-dup-id-topic", nil)
	require.NoError(t, err)

	rec := doSNSRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {"same-id"},
		"PublishBatchRequestEntries.member.1.Message": {"msg1"},
		"PublishBatchRequestEntries.member.2.Id":      {"same-id"},
		"PublishBatchRequestEntries.member.2.Message": {"msg2"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "BatchEntryIdsNotDistinct")
}

// TestIssue10_BatchTooManyEntries verifies the TooManyEntriesInBatchRequest code.
func TestBatchTooManyEntries(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("batch-too-many-topic", nil)
	require.NoError(t, err)

	vals := url.Values{
		"Action":   {"PublishBatch"},
		"TopicArn": {tp.TopicArn},
	}
	for i := 1; i <= sns.ExportedMaxPublishBatchEntries+1; i++ {
		vals.Set(fmt.Sprintf("PublishBatchRequestEntries.member.%d.Id", i),
			fmt.Sprintf("e%d", i))
		vals.Set(fmt.Sprintf("PublishBatchRequestEntries.member.%d.Message", i),
			"msg")
	}

	rec := doSNSRequest(t, h, vals)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "TooManyEntriesInBatchRequest")
}

// TestIssue10_BatchBodyTooLarge verifies the BatchRequestTooLong error.
func TestBatchBodyTooLarge(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("batch-large-topic", nil)
	require.NoError(t, err)

	// 256 KiB + 1 byte triggers the limit.
	bigMsg := strings.Repeat("a", 256*1024+1)

	rec := doSNSRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {"e1"},
		"PublishBatchRequestEntries.member.1.Message": {bigMsg},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "BatchRequestTooLong")
}

// TestIssue11_MessageAttributeInvalidDataType verifies that unsupported
// DataTypes are rejected at publish time.
func TestMessageAttributeInvalidDataType(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("attr-dt-topic", nil)
	require.NoError(t, err)

	rec := doSNSRequest(t, h, url.Values{
		"Action":                         {"Publish"},
		"TopicArn":                       {tp.TopicArn},
		"Message":                        {"hello"},
		"MessageAttributes.entry.1.Name": {"myAttr"},
		"MessageAttributes.entry.1.Value.DataType":    {"InvalidType"},
		"MessageAttributes.entry.1.Value.StringValue": {"val"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameter")
}

// TestIssue11_MessageAttributeValidDataTypes verifies all supported types.
func TestMessageAttributeValidDataTypes(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("attr-valid-dt-topic", nil)
	require.NoError(t, err)

	for _, dt := range []string{"String", "Number", "Binary", "String.Array", "String.Custom"} {
		attrs := map[string]sns.MessageAttribute{
			"attr1": {DataType: dt, StringValue: "value"},
		}
		_, err = b.Publish(tp.TopicArn, "msg", "", "", attrs)
		require.NoErrorf(t, err, "DataType %q should be accepted", dt)
	}
}

// TestIssue11_MessageAttributeNameTooLong verifies max attribute name length.
func TestMessageAttributeNameTooLong(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("attr-long-name-topic", nil)
	require.NoError(t, err)

	longName := strings.Repeat("x", 257)
	attrs := map[string]sns.MessageAttribute{
		longName: {DataType: "String", StringValue: "value"},
	}
	_, err = b.Publish(tp.TopicArn, "msg", "", "", attrs)
	require.Error(t, err)
}

// TestConcurrentPublishSafety verifies that concurrent Publish calls do not
// race on internal backend state.
func TestConcurrentPublishSafety(t *testing.T) {
	t.Parallel()

	b := newA1679Backend(t)
	tp, err := b.CreateTopic("concurrent-topic", nil)
	require.NoError(t, err)

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := range goroutines {
		go func(i int) {
			defer wg.Done()
			_, _ = b.Publish(tp.TopicArn, fmt.Sprintf("msg-%d", i), "", "", nil)
		}(i)
	}

	wg.Wait()
}

// TestMessageStructureJSONSinglePublish verifies protocol-specific routing for
// the non-batch Publish action.
func TestMessageStructureJSONSinglePublish(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("json-single-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)

	msgJSON := `{"http":"http-only","default":"fallback"}`
	rec := doSNSRequest(t, h, url.Values{
		"Action":           {"Publish"},
		"TopicArn":         {tp.TopicArn},
		"Message":          {msgJSON},
		"MessageStructure": {"json"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	select {
	case raw := <-received:
		var env snsNotificationEnvelope
		if json.Unmarshal([]byte(raw), &env) == nil {
			assert.Equal(t, "http-only", env.Message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP delivery did not arrive")
	}
}
