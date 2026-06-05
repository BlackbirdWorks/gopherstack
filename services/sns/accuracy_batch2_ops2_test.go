package sns_test

// accuracy_batch2_ops2_test.go — SNS batch-2 AWS-accuracy ops2 tests.
//
// Covers three gaps found during the batch-2 ops audit:
//
//  1. GetTopicAttributes FifoTopic default: standard topics must return
//     FifoTopic="false"; previously the key was absent.
//
//  2. Empty NextToken in list responses: all list operations (ListTopics,
//     ListSubscriptions, ListSubscriptionsByTopic, ListPlatformApplications,
//     ListEndpointsByPlatformApplication, ListSMSSandboxPhoneNumbers,
//     ListPhoneNumbersOptedOut, ListOriginationNumbers) must omit the
//     NextToken element entirely when on the last page. Previously an empty
//     <NextToken></NextToken> was serialised, which the AWS SDK v2 interprets
//     as a valid cursor and causes infinite pagination loops.
//
//  3. Publish Subject validation: AWS rejects subjects longer than 100
//     characters or containing non-printable / non-ASCII characters.
//     Previously any Subject was accepted without validation.

import (
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Fix 1 — GetTopicAttributes FifoTopic default
// ---------------------------------------------------------------------------

// TestOps2_StandardTopicFifoTopicFalse verifies that GetTopicAttributes
// returns FifoTopic="false" for a standard (non-FIFO) topic.
// AWS always returns this attribute for all topics; omitting it for standard
// topics causes SDK clients to treat the topic as unknown type.
func TestOps2_StandardTopicFifoTopicFalse(t *testing.T) {
	t.Parallel()

	b := newB2Backend(t)
	tp, err := b.CreateTopic("standard-topic", nil)
	require.NoError(t, err)

	attrs, err := b.GetTopicAttributes(tp.TopicArn)
	require.NoError(t, err)

	assert.Equal(t, "false", attrs["FifoTopic"], "standard topic must report FifoTopic=false")
}

// TestOps2_FIFOTopicFifoTopicTrue verifies that the existing FifoTopic=true
// behaviour for FIFO topics is unaffected by the fix.
func TestOps2_FIFOTopicFifoTopicTrue(t *testing.T) {
	t.Parallel()

	b := newB2Backend(t)
	tp, err := b.CreateTopic("fifo-topic.fifo", map[string]string{"FifoTopic": "true"})
	require.NoError(t, err)

	attrs, err := b.GetTopicAttributes(tp.TopicArn)
	require.NoError(t, err)

	assert.Equal(t, "true", attrs["FifoTopic"])
}

// TestOps2_StandardTopicFifoTopicViaHandler verifies the FifoTopic=false
// attribute is visible in GetTopicAttributes HTTP responses.
func TestOps2_StandardTopicFifoTopicViaHandler(t *testing.T) {
	t.Parallel()

	h, _ := newB2Handler(t)

	createRec := doB2Request(t, h, url.Values{
		"Action": {"CreateTopic"},
		"Name":   {"standard-via-handler"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	arnRe := regexp.MustCompile(`<TopicArn>(arn:[^<]+)</TopicArn>`)
	matches := arnRe.FindStringSubmatch(createRec.Body.String())
	require.Len(t, matches, 2, "could not extract TopicArn")
	topicArn := matches[1]

	getAttrsRec := doB2Request(t, h, url.Values{
		"Action":   {"GetTopicAttributes"},
		"TopicArn": {topicArn},
	})
	require.Equal(t, http.StatusOK, getAttrsRec.Code)

	body := getAttrsRec.Body.String()
	assert.Contains(t, body, "<key>FifoTopic</key>")
	assert.Contains(t, body, "<value>false</value>")
}

// ---------------------------------------------------------------------------
// Fix 2 — Empty NextToken omitted from list responses
// ---------------------------------------------------------------------------

// TestOps2_ListTopicsNoNextToken verifies ListTopics omits NextToken when
// all topics fit on one page.
func TestOps2_ListTopicsNoNextToken(t *testing.T) {
	t.Parallel()

	h, _ := newB2Handler(t)

	rec := doB2Request(t, h, url.Values{"Action": {"ListTopics"}})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, "<NextToken>", "ListTopics must omit NextToken on last page")
}

// TestOps2_ListSubscriptionsNoNextToken verifies ListSubscriptions omits
// NextToken when all subscriptions fit on one page.
func TestOps2_ListSubscriptionsNoNextToken(t *testing.T) {
	t.Parallel()

	h, _ := newB2Handler(t)

	rec := doB2Request(t, h, url.Values{"Action": {"ListSubscriptions"}})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, "<NextToken>", "ListSubscriptions must omit NextToken on last page")
}

// TestOps2_ListSubscriptionsByTopicNoNextToken verifies ListSubscriptionsByTopic
// omits NextToken when all subscriptions fit on one page.
func TestOps2_ListSubscriptionsByTopicNoNextToken(t *testing.T) {
	t.Parallel()

	h, b := newB2Handler(t)

	tp, err := b.CreateTopic("listsubs-topic", nil)
	require.NoError(t, err)

	rec := doB2Request(t, h, url.Values{
		"Action":   {"ListSubscriptionsByTopic"},
		"TopicArn": {tp.TopicArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, "<NextToken>", "ListSubscriptionsByTopic must omit NextToken on last page")
}

// TestOps2_ListPlatformApplicationsNoNextToken verifies ListPlatformApplications
// omits NextToken on the last page.
func TestOps2_ListPlatformApplicationsNoNextToken(t *testing.T) {
	t.Parallel()

	h, _ := newB2Handler(t)

	rec := doB2Request(t, h, url.Values{"Action": {"ListPlatformApplications"}})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, "<NextToken>", "ListPlatformApplications must omit NextToken on last page")
}

// TestOps2_ListEndpointsByPlatformApplicationNoNextToken verifies
// ListEndpointsByPlatformApplication omits NextToken on the last page.
func TestOps2_ListEndpointsByPlatformApplicationNoNextToken(t *testing.T) {
	t.Parallel()

	h, b := newB2Handler(t)

	app, err := b.CreatePlatformApplication("no-next-app", "GCM", nil)
	require.NoError(t, err)

	rec := doB2Request(t, h, url.Values{
		"Action":                 {"ListEndpointsByPlatformApplication"},
		"PlatformApplicationArn": {app.PlatformApplicationArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, "<NextToken>", "ListEndpointsByPlatformApplication must omit NextToken on last page")
}

// TestOps2_ListSMSSandboxPhoneNumbersNoNextToken verifies
// ListSMSSandboxPhoneNumbers omits NextToken on the last page.
func TestOps2_ListSMSSandboxPhoneNumbersNoNextToken(t *testing.T) {
	t.Parallel()

	h, _ := newB2Handler(t)

	rec := doB2Request(t, h, url.Values{"Action": {"ListSMSSandboxPhoneNumbers"}})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, "<NextToken>", "ListSMSSandboxPhoneNumbers must omit NextToken on last page")
}

// TestOps2_ListPhoneNumbersOptedOutNoNextToken verifies ListPhoneNumbersOptedOut
// omits nextToken on the last page.
func TestOps2_ListPhoneNumbersOptedOutNoNextToken(t *testing.T) {
	t.Parallel()

	h, _ := newB2Handler(t)

	rec := doB2Request(t, h, url.Values{"Action": {"ListPhoneNumbersOptedOut"}})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, "<nextToken>", "ListPhoneNumbersOptedOut must omit nextToken on last page")
}

// ---------------------------------------------------------------------------
// Fix 3 — Publish Subject validation
// ---------------------------------------------------------------------------

// TestOps2_PublishSubjectTooLongRejected verifies that Publish rejects a
// Subject longer than 100 characters with InvalidParameter.
func TestOps2_PublishSubjectTooLongRejected(t *testing.T) {
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

			b := newB2Backend(t)
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
func TestOps2_PublishSubjectNonASCIIRejected(t *testing.T) {
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

			b := newB2Backend(t)
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
func TestOps2_PublishSubjectTooLongViaHandler(t *testing.T) {
	t.Parallel()

	h, _ := newB2Handler(t)

	createRec := doB2Request(t, h, url.Values{"Action": {"CreateTopic"}, "Name": {"subj-handler-topic"}})
	require.Equal(t, http.StatusOK, createRec.Code)

	matches := regexp.MustCompile(`<TopicArn>(arn:[^<]+)</TopicArn>`).FindStringSubmatch(createRec.Body.String())
	require.Len(t, matches, 2)
	topicArn := matches[1]

	rec := doB2Request(t, h, url.Values{
		"Action":   {"Publish"},
		"TopicArn": {topicArn},
		"Message":  {"hello"},
		"Subject":  {strings.Repeat("X", 101)},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameter")
}
