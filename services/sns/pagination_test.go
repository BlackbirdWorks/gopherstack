package sns_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

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

// TestOps2_ListTopicsNoNextToken verifies ListTopics omits NextToken when
// all topics fit on one page.
func TestListTopicsNoNextToken(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandlerPair(t)

	rec := doTestRequest(t, h, url.Values{"Action": {"ListTopics"}})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, "<NextToken>", "ListTopics must omit NextToken on last page")
}

// TestOps2_ListSubscriptionsNoNextToken verifies ListSubscriptions omits
// NextToken when all subscriptions fit on one page.
func TestListSubscriptionsNoNextToken(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandlerPair(t)

	rec := doTestRequest(t, h, url.Values{"Action": {"ListSubscriptions"}})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, "<NextToken>", "ListSubscriptions must omit NextToken on last page")
}

// TestOps2_ListSubscriptionsByTopicNoNextToken verifies ListSubscriptionsByTopic
// omits NextToken when all subscriptions fit on one page.
func TestListSubscriptionsByTopicNoNextToken(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerPair(t)

	tp, err := b.CreateTopic("listsubs-topic", nil)
	require.NoError(t, err)

	rec := doTestRequest(t, h, url.Values{
		"Action":   {"ListSubscriptionsByTopic"},
		"TopicArn": {tp.TopicArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, "<NextToken>", "ListSubscriptionsByTopic must omit NextToken on last page")
}

// TestOps2_ListPlatformApplicationsNoNextToken verifies ListPlatformApplications
// omits NextToken on the last page.
func TestListPlatformApplicationsNoNextToken(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandlerPair(t)

	rec := doTestRequest(t, h, url.Values{"Action": {"ListPlatformApplications"}})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, "<NextToken>", "ListPlatformApplications must omit NextToken on last page")
}

// TestOps2_ListEndpointsByPlatformApplicationNoNextToken verifies
// ListEndpointsByPlatformApplication omits NextToken on the last page.
func TestListEndpointsByPlatformApplicationNoNextToken(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerPair(t)

	app, err := b.CreatePlatformApplication("no-next-app", "GCM", nil)
	require.NoError(t, err)

	rec := doTestRequest(t, h, url.Values{
		"Action":                 {"ListEndpointsByPlatformApplication"},
		"PlatformApplicationArn": {app.PlatformApplicationArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, "<NextToken>", "ListEndpointsByPlatformApplication must omit NextToken on last page")
}

// TestOps2_ListSMSSandboxPhoneNumbersNoNextToken verifies
// ListSMSSandboxPhoneNumbers omits NextToken on the last page.
func TestListSMSSandboxPhoneNumbersNoNextToken(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandlerPair(t)

	rec := doTestRequest(t, h, url.Values{"Action": {"ListSMSSandboxPhoneNumbers"}})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, "<NextToken>", "ListSMSSandboxPhoneNumbers must omit NextToken on last page")
}

// TestOps2_ListPhoneNumbersOptedOutNoNextToken verifies ListPhoneNumbersOptedOut
// omits nextToken on the last page.
func TestListPhoneNumbersOptedOutNoNextToken(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandlerPair(t)

	rec := doTestRequest(t, h, url.Values{"Action": {"ListPhoneNumbersOptedOut"}})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, "<nextToken>", "ListPhoneNumbersOptedOut must omit nextToken on last page")
}

// TestSNSPagination_NegativeToken verifies that a continuation token decoding
// to a negative offset is rejected rather than reaching items[offset:end] and
// panicking with "slice bounds out of range". LTU= is base64 for "-5".
func TestSNSPagination_NegativeToken(t *testing.T) {
	t.Parallel()

	const negativeToken = "LTU="

	b := sns.NewInMemoryBackend()

	_, err := b.CreateTopic("topic-0", nil)
	require.NoError(t, err)

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("ListTopics panicked on negative-offset token: %v", r)
			}
		}()

		_, _, err = b.ListTopics(negativeToken)
	}()

	require.Error(t, err, "a negative-offset token must be rejected, not silently accepted")
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
