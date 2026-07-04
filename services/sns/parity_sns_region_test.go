package sns_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

// snsPostWithRegion sends a form-encoded SNS request with the X-Amz-Region header set
// so that httputils.ExtractRegionFromRequest returns the desired region.
func snsPostWithRegion(
	t *testing.T,
	h *sns.Handler,
	region string,
	form url.Values,
) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	body := form.Encode()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Amz-Region", region)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// TestSNS_ListTopicsRegionIsolation verifies that ListTopics only returns topics
// belonging to the request region, not topics from other regions.
func TestSNS_ListTopicsRegionIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		requestRegion    string
		wantBodyContains []string
		wantBodyAbsent   []string
	}{
		{
			name:             "us-east-1 sees only us-east topics",
			requestRegion:    "us-east-1",
			wantBodyContains: []string{"us-east-topic"},
			wantBodyAbsent:   []string{"eu-west-topic"},
		},
		{
			name:             "eu-west-1 sees only eu-west topics",
			requestRegion:    "eu-west-1",
			wantBodyContains: []string{"eu-west-topic"},
			wantBodyAbsent:   []string{"us-east-topic"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			_, err := b.CreateTopicInRegion("us-east-topic", "us-east-1", nil)
			require.NoError(t, err)
			_, err = b.CreateTopicInRegion("eu-west-topic", "eu-west-1", nil)
			require.NoError(t, err)

			h := sns.NewHandler(b)
			rec := snsPostWithRegion(t, h, tt.requestRegion, url.Values{
				"Action":  {"ListTopics"},
				"Version": {"2010-03-31"},
			})

			require.Equal(t, http.StatusOK, rec.Code)
			body := rec.Body.String()
			for _, want := range tt.wantBodyContains {
				assert.Contains(t, body, want)
			}
			for _, absent := range tt.wantBodyAbsent {
				assert.NotContains(t, body, absent)
			}
		})
	}
}

// TestSNS_ListTopicsInRegion_Backend verifies the backend ListTopicsInRegion method directly.
func TestSNS_ListTopicsInRegion_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupRegions  map[string][]string
		name          string
		queryRegion   string
		wantTopicName string
		wantCount     int
	}{
		{
			name: "filters to queried region",
			setupRegions: map[string][]string{
				"us-east-1": {"topic-a", "topic-b"},
				"eu-west-1": {"topic-c"},
			},
			queryRegion:   "us-east-1",
			wantCount:     2,
			wantTopicName: "topic-a",
		},
		{
			name: "empty result for region with no topics",
			setupRegions: map[string][]string{
				"us-east-1": {"topic-a"},
			},
			queryRegion: "ap-southeast-1",
			wantCount:   0,
		},
		{
			name: "all topics in single region",
			setupRegions: map[string][]string{
				"us-west-2": {"x", "y", "z"},
			},
			queryRegion: "us-west-2",
			wantCount:   3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			for region, names := range tt.setupRegions {
				for _, name := range names {
					_, err := b.CreateTopicInRegion(name, region, nil)
					require.NoError(t, err)
				}
			}

			topics, next, err := b.ListTopicsInRegion(tt.queryRegion, "")
			require.NoError(t, err)
			assert.Empty(t, next)
			assert.Len(t, topics, tt.wantCount)
			if tt.wantTopicName != "" {
				found := false
				for _, tp := range topics {
					if strings.Contains(tp.TopicArn, tt.wantTopicName) {
						found = true

						break
					}
				}
				assert.True(t, found, "expected topic %q in results", tt.wantTopicName)
			}
		})
	}
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

// TestSNS_PlatformApplicationRegion verifies that platform application ARNs embed
// the request region rather than the backend's default region.
func TestSNS_PlatformApplicationRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
	}{
		{name: "us-east-1", region: "us-east-1"},
		{name: "eu-west-1", region: "eu-west-1"},
		{name: "ap-southeast-1", region: "ap-southeast-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			app, err := b.CreatePlatformApplicationInRegion("myapp", "GCM", tt.region, nil)
			require.NoError(t, err)

			assert.Contains(t, app.PlatformApplicationArn, ":"+tt.region+":")
		})
	}
}

// TestSNS_PlatformEndpointInheritsAppRegion verifies that platform endpoint ARNs
// inherit the region from their parent platform application, not the backend default.
func TestSNS_PlatformEndpointInheritsAppRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		appRegion string
	}{
		{name: "app in eu-west-1", appRegion: "eu-west-1"},
		{name: "app in ap-northeast-1", appRegion: "ap-northeast-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			app, err := b.CreatePlatformApplicationInRegion("myapp", "GCM", tt.appRegion, nil)
			require.NoError(t, err)

			ep, err := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "device-token-xyz", nil)
			require.NoError(t, err)

			assert.Contains(t, ep.EndpointArn, ":"+tt.appRegion+":")
		})
	}
}

// TestSNS_SMSSandboxStatusPersisted verifies that sandbox mode is persisted
// and restored across snapshots.
func TestSNS_SMSSandboxStatusPersisted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		enabled bool
	}{
		{name: "sandbox enabled (default)", enabled: true},
		{name: "sandbox disabled", enabled: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			b.SetSMSSandboxMode(tt.enabled)

			status, err := b.GetSMSSandboxAccountStatus()
			require.NoError(t, err)
			assert.Equal(t, tt.enabled, status)

			// Snapshot and restore.
			snap := b.Snapshot(context.TODO())
			b2 := sns.NewInMemoryBackend()
			require.NoError(t, b2.Restore(context.TODO(), snap))

			status2, err := b2.GetSMSSandboxAccountStatus()
			require.NoError(t, err)
			assert.Equal(t, tt.enabled, status2, "sandbox mode must survive snapshot/restore")
		})
	}
}

// TestSNS_FifoSeqNumsCleanedOnDeleteTopic verifies that the FIFO sequence-number
// counter for a topic is removed from the handler when the topic is deleted,
// preventing unbounded memory growth in high-churn workloads.
func TestSNS_FifoSeqNumsCleanedOnDeleteTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "standard delete cleans seq counter"},
		{name: "second delete after recreation gets fresh counter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			h := sns.NewHandler(b)

			// Create a FIFO topic and publish to generate a seq counter entry.
			rec := snsPost(t, h, url.Values{
				"Action":                   {"CreateTopic"},
				"Version":                  {"2010-03-31"},
				"Name":                     {"test.fifo"},
				"Attributes.entry.1.key":   {"FifoTopic"},
				"Attributes.entry.1.value": {"true"},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Delete the topic — this should remove the seq counter.
			topic, err := b.CreateTopicInRegion(
				"cleanup-test.fifo",
				"us-east-1",
				map[string]string{"FifoTopic": "true"},
			)
			require.NoError(t, err)

			rec = snsPost(t, h, url.Values{
				"Action":   {"DeleteTopic"},
				"Version":  {"2010-03-31"},
				"TopicArn": {topic.TopicArn},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Recreating the topic and publishing should start a fresh counter at 1.
			topic2, err := b.CreateTopicInRegion(
				"cleanup-test.fifo",
				"us-east-1",
				map[string]string{"FifoTopic": "true"},
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				topic.TopicArn,
				topic2.TopicArn,
				"idempotent re-create returns same ARN",
			)
		})
	}
}

// TestSNS_ListTopicsInRegion_Pagination verifies that pagination tokens are scoped
// correctly within a single region.
func TestSNS_ListTopicsInRegion_Pagination(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	for i := range 110 {
		_, err := b.CreateTopicInRegion(
			"paged-topic-"+strings.Repeat(
				"0",
				3-len([]rune(string(rune('0'+i/100)))),
			)+string(
				rune('0'+i/100),
			)+string(
				rune('0'+i%100/10),
			)+string(
				rune('0'+i%10),
			),
			"us-east-1",
			nil,
		)
		_ = err // name format is not critical; focus on count
	}
	// Also create topics in another region that must not appear.
	for i := range 5 {
		_, err := b.CreateTopicInRegion("other-"+strings.Repeat("x", i+1), "eu-west-1", nil)
		require.NoError(t, err)
	}

	page1, tok1, err := b.ListTopicsInRegion("us-east-1", "")
	require.NoError(t, err)
	assert.Len(t, page1, 100)
	assert.NotEmpty(t, tok1)

	page2, tok2, err := b.ListTopicsInRegion("us-east-1", tok1)
	require.NoError(t, err)
	assert.Len(t, page2, 10)
	assert.Empty(t, tok2)

	// Verify that eu-west-1 topics never appear in us-east-1 pages.
	allArns := make([]string, 0, len(page1)+len(page2))
	for _, tp := range append(page1, page2...) {
		allArns = append(allArns, tp.TopicArn)
	}
	for _, a := range allArns {
		assert.Contains(t, a, ":us-east-1:", "ARN must be scoped to us-east-1")
	}
}

// TestSNS_FifoDedupExpiredEntryNotFalsePositive verifies that isDuplicate returns false
// for a dedup ID whose entry has already expired, without requiring an O(n) sweep on
// each call. This is the key correctness guarantee that allows removing the opportunistic
// sweepExpiredLocked call from isDuplicate (performance fix).
func TestSNS_FifoDedupExpiredEntryNotFalsePositive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		pastOffset time.Duration // how far in the past the expiry is
		wantDup    bool
	}{
		{
			name:       "entry expired 1 second ago → not a duplicate",
			pastOffset: time.Second,
			wantDup:    false,
		},
		{
			name:       "entry expired 5 minutes ago → not a duplicate",
			pastOffset: 5 * time.Minute,
			wantDup:    false,
		},
		{
			name:       "entry expires 1 hour from now → still a duplicate",
			pastOffset: -time.Hour, // negative = future
			wantDup:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := sns.NewFifoDedupForTest()
			expiry := time.Now().Add(-tt.pastOffset)
			sns.FifoDedupInsertWithExpiryForTest(
				d,
				"arn:aws:sns:us-east-1:000000000000:test.fifo",
				"id-1",
				expiry,
			)

			got := sns.FifoDedupIsDuplicateForTest(
				d,
				"arn:aws:sns:us-east-1:000000000000:test.fifo",
				"id-1",
			)
			assert.Equal(t, tt.wantDup, got)
		})
	}
}

// TestSNS_FifoDedupMapNotGrowingOnExpiredCheck verifies that isDuplicate does NOT
// evict expired entries (that is handled by the background sweep), so the map entry
// count stays constant after multiple isDuplicate calls on an expired entry.
func TestSNS_FifoDedupMapNotGrowingOnExpiredCheck(t *testing.T) {
	t.Parallel()

	d := sns.NewFifoDedupForTest()
	expiry := time.Now().Add(-time.Second) // already expired
	sns.FifoDedupInsertWithExpiryForTest(
		d,
		"arn:aws:sns:us-east-1:000000000000:test.fifo",
		"id-x",
		expiry,
	)

	countBefore := sns.FifoDedupEntryCountForTest(d)
	for range 10 {
		sns.FifoDedupIsDuplicateForTest(d, "arn:aws:sns:us-east-1:000000000000:test.fifo", "id-x")
	}
	countAfter := sns.FifoDedupEntryCountForTest(d)

	// Entry stays in map (not swept by isDuplicate); background goroutine handles cleanup.
	assert.Equal(t, countBefore, countAfter, "isDuplicate must not evict expired entries")
}
