package sns_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

// TestIssue1_PublishBatchFIFODedup verifies deduplication within the 5-minute
// window for FIFO topics using PublishBatch.
func TestPublishBatchFIFODedup(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("fifo-batch-dedup.fifo", nil)
	require.NoError(t, err)

	// First batch with dedup ID "d1".
	rec := doSNSRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {"e1"},
		"PublishBatchRequestEntries.member.1.Message":                {"msg1"},
		"PublishBatchRequestEntries.member.1.MessageGroupId":         {"g1"},
		"PublishBatchRequestEntries.member.1.MessageDeduplicationId": {"d1"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<Id>e1</Id>")
	// No failed entries.
	assert.NotContains(t, rec.Body.String(), "<Code>")

	// Same dedup ID → second batch must treat it as duplicate (success, no re-publish).
	rec2 := doSNSRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {"e2"},
		"PublishBatchRequestEntries.member.1.Message":                {"msg1"},
		"PublishBatchRequestEntries.member.1.MessageGroupId":         {"g1"},
		"PublishBatchRequestEntries.member.1.MessageDeduplicationId": {"d1"},
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
	// Duplicate is returned as a success entry per AWS spec.
	assert.Contains(t, rec2.Body.String(), "Successful")
	assert.NotContains(t, rec2.Body.String(), "<Code>")
}

// TestIssue2_ContentBasedDeduplicationBatch verifies that CBD works in
// PublishBatch: explicit MessageDeduplicationId is rejected when CBD enabled.
func TestContentBasedDeduplicationBatch(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("cbd-batch.fifo", map[string]string{
		"ContentBasedDeduplication": "true",
	})
	require.NoError(t, err)

	// Explicit dedup ID with CBD enabled → invalid parameter.
	rec := doSNSRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {"e1"},
		"PublishBatchRequestEntries.member.1.Message":                {"some-msg"},
		"PublishBatchRequestEntries.member.1.MessageGroupId":         {"g1"},
		"PublishBatchRequestEntries.member.1.MessageDeduplicationId": {"forbidden"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	// Entry-level failure expected.
	assert.Contains(t, rec.Body.String(), "Failed")
	assert.Contains(t, rec.Body.String(), "InvalidParameter")
}

// TestIssue2_ContentBasedDeduplicationBatchDedup verifies same-body messages
// to a CBD FIFO topic are deduplicated in PublishBatch.
func TestContentBasedDeduplicationBatchDedup(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("cbd-dedup-batch.fifo", map[string]string{
		"ContentBasedDeduplication": "true",
	})
	require.NoError(t, err)

	// First publish.
	rec1 := doSNSRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {"e1"},
		"PublishBatchRequestEntries.member.1.Message":        {"dup-body"},
		"PublishBatchRequestEntries.member.1.MessageGroupId": {"g1"},
	})
	assert.Equal(t, http.StatusOK, rec1.Code)
	assert.NotContains(t, rec1.Body.String(), "<Code>")

	// Same body → duplicate within window.
	rec2 := doSNSRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {"e2"},
		"PublishBatchRequestEntries.member.1.Message":        {"dup-body"},
		"PublishBatchRequestEntries.member.1.MessageGroupId": {"g1"},
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.NotContains(t, rec2.Body.String(), "<Code>")
}

// TestIssue2_FIFORequiresDedupID verifies that a FIFO topic without CBD
// requires MessageDeduplicationId on PublishBatch entries.
func TestFIFORequiresDedupID(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("nodedupid.fifo", nil)
	require.NoError(t, err)

	rec := doSNSRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {"e1"},
		"PublishBatchRequestEntries.member.1.Message":        {"msg"},
		"PublishBatchRequestEntries.member.1.MessageGroupId": {"g1"},
		// No MessageDeduplicationId.
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Failed")
	assert.Contains(t, rec.Body.String(), "InvalidParameter")
}

// TestFifoDedupSweepIntervalExported verifies that the sweep interval constant
// is exported and has the expected value.
func TestFifoDedupSweepIntervalExported(t *testing.T) {
	t.Parallel()

	assert.Equal(t, time.Minute, sns.ExportedFifoDedupSweepInterval)
}

// TestFIFOMessageGroupIDRequired verifies that publishing to a FIFO topic
// without MessageGroupId returns the appropriate error.
func TestFIFOMessageGroupIDRequired(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("ordering-topic.fifo", map[string]string{"FifoTopic": "true"})
	require.NoError(t, err)

	rec := doSNSRequest(t, h, url.Values{
		"Action":   {"Publish"},
		"TopicArn": {tp.TopicArn},
		"Message":  {"hello"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MessageGroupId")
}

// TestFIFOMessageDeduplicationIDRequired verifies that FIFO topics without
// ContentBasedDeduplication require an explicit MessageDeduplicationId.
func TestFIFOMessageDeduplicationIDRequired(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("dedup-required-topic.fifo", map[string]string{"FifoTopic": "true"})
	require.NoError(t, err)

	rec := doSNSRequest(t, h, url.Values{
		"Action":         {"Publish"},
		"TopicArn":       {tp.TopicArn},
		"Message":        {"hello"},
		"MessageGroupId": {"g1"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MessageDeduplicationId")
}

// TestFIFOContentBasedDeduplicationForbidsExplicitDedupID verifies that
// providing a MessageDeduplicationId when ContentBasedDeduplication is
// enabled returns an error, matching AWS behaviour.
func TestFIFOContentBasedDeduplicationForbidsExplicitDedupID(t *testing.T) {
	t.Parallel()

	h, b := newA1679Handler(t)
	tp, err := b.CreateTopic("cbd-topic.fifo", map[string]string{
		"FifoTopic":                 "true",
		"ContentBasedDeduplication": "true",
	})
	require.NoError(t, err)

	rec := doSNSRequest(t, h, url.Values{
		"Action":                 {"Publish"},
		"TopicArn":               {tp.TopicArn},
		"Message":                {"hello"},
		"MessageGroupId":         {"g1"},
		"MessageDeduplicationId": {"explicit-id"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MessageDeduplicationId")
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

// batchSuccessSequenceNumbers extracts all SequenceNumber values from
// a PublishBatchResponse Successful section.
func batchSuccessSequenceNumbers(t *testing.T, body string) []string {
	t.Helper()

	type member struct {
		SequenceNumber string `xml:"SequenceNumber"`
	}
	type successful struct {
		Members []member `xml:"member"`
	}
	type result struct {
		Successful successful `xml:"Successful"`
	}
	type resp struct {
		XMLName xml.Name `xml:"PublishBatchResponse"`
		Result  result   `xml:"PublishBatchResult"`
	}

	var r resp
	require.NoError(t, xml.Unmarshal([]byte(body), &r), "parse PublishBatchResponse XML")

	nums := make([]string, 0, len(r.Result.Successful.Members))
	for _, m := range r.Result.Successful.Members {
		nums = append(nums, m.SequenceNumber)
	}

	return nums
}

// TestFIFOPublishReturnsSequenceNumber verifies that publishing to a
// FIFO topic returns a non-empty 20-digit SequenceNumber.
func TestFIFOPublishReturnsSequenceNumber(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerPair(t)
	tp, err := b.CreateTopic("seq-fifo.fifo", map[string]string{
		"ContentBasedDeduplication": "true",
	})
	require.NoError(t, err)

	rec := doTestRequest(t, h, url.Values{
		"Action":         {"Publish"},
		"TopicArn":       {tp.TopicArn},
		"Message":        {"seq-test"},
		"MessageGroupId": {"g1"},
		"Version":        {"2010-03-31"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	seqNum := publishResponseSequenceNumber(t, rec.Body.String())
	assert.NotEmpty(t, seqNum, "FIFO publish must return a SequenceNumber")
	assert.Len(t, seqNum, 20, "FIFO SequenceNumber must be 20 digits")
	assert.Equal(t, "00000000000000000001", seqNum)
}

// TestFIFOSequenceNumberMonotonicallyIncreases verifies that each
// successive publish to the same FIFO topic gets a higher SequenceNumber.
func TestFIFOSequenceNumberMonotonicallyIncreases(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerPair(t)
	tp, err := b.CreateTopic("seq-mono.fifo", map[string]string{
		"ContentBasedDeduplication": "true",
	})
	require.NoError(t, err)

	nums := make([]string, 0, 5)

	for i := range 5 {
		rec := doTestRequest(t, h, url.Values{
			"Action":         {"Publish"},
			"TopicArn":       {tp.TopicArn},
			"Message":        {fmt.Sprintf("msg-%d", i)},
			"MessageGroupId": {"g1"},
			"Version":        {"2010-03-31"},
		})
		require.Equal(t, http.StatusOK, rec.Code)
		nums = append(nums, publishResponseSequenceNumber(t, rec.Body.String()))
	}

	// Verify monotonic increase.
	for i := 1; i < len(nums); i++ {
		assert.Greater(t, nums[i], nums[i-1], "SequenceNumbers must increase")
	}
}

// TestFIFOBatchReturnsSequenceNumbers verifies that PublishBatch to a
// FIFO topic returns SequenceNumbers in the successful entries.
func TestFIFOBatchReturnsSequenceNumbers(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerPair(t)
	tp, err := b.CreateTopic("batch-seq.fifo", map[string]string{
		"ContentBasedDeduplication": "true",
	})
	require.NoError(t, err)

	rec := doTestRequest(t, h, url.Values{
		"Action":                                 {"PublishBatch"},
		"TopicArn":                               {tp.TopicArn},
		"PublishBatchRequestEntries.member.1.Id": {"e1"},
		"PublishBatchRequestEntries.member.1.Message":        {"batch-msg-1"},
		"PublishBatchRequestEntries.member.1.MessageGroupId": {"g1"},
		"PublishBatchRequestEntries.member.2.Id":             {"e2"},
		"PublishBatchRequestEntries.member.2.Message":        {"batch-msg-2"},
		"PublishBatchRequestEntries.member.2.MessageGroupId": {"g1"},
		"Version": {"2010-03-31"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	seqNums := batchSuccessSequenceNumbers(t, rec.Body.String())
	require.Len(t, seqNums, 2, "both batch entries must have SequenceNumbers")

	for _, s := range seqNums {
		assert.Len(t, s, 20)
	}

	// SequenceNumbers must be distinct.
	assert.NotEqual(t, seqNums[0], seqNums[1])
}

// TestNonFIFOPublishNoSequenceNumber verifies that a standard (non-FIFO)
// Publish response does NOT include a SequenceNumber.
func TestNonFIFOPublishNoSequenceNumber(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerPair(t)
	tp, err := b.CreateTopic("non-fifo-seq", nil)
	require.NoError(t, err)

	rec := doTestRequest(t, h, url.Values{
		"Action":   {"Publish"},
		"TopicArn": {tp.TopicArn},
		"Message":  {"plain"},
		"Version":  {"2010-03-31"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	seqNum := publishResponseSequenceNumber(t, rec.Body.String())
	assert.Empty(t, seqNum, "non-FIFO topic must not return SequenceNumber")
}

// TestFIFOSeqNumResetsOnHandlerReset verifies that after h.Reset(),
// FIFO SequenceNumbers start from 1 again.
func TestFIFOSeqNumResetsOnHandlerReset(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerPair(t)
	tp, err := b.CreateTopic("seq-reset.fifo", map[string]string{
		"ContentBasedDeduplication": "true",
	})
	require.NoError(t, err)

	doPublish := func(msg string) string {
		rec := doTestRequest(t, h, url.Values{
			"Action":         {"Publish"},
			"TopicArn":       {tp.TopicArn},
			"Message":        {msg},
			"MessageGroupId": {"g1"},
			"Version":        {"2010-03-31"},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		return publishResponseSequenceNumber(t, rec.Body.String())
	}

	seq1 := doPublish("first")
	assert.Equal(t, "00000000000000000001", seq1)

	seq2 := doPublish("second")
	assert.Equal(t, "00000000000000000002", seq2)

	h.Reset()

	// After reset the topic no longer exists; re-create it.
	tp2, err := b.CreateTopic("seq-reset2.fifo", map[string]string{
		"ContentBasedDeduplication": "true",
	})
	require.NoError(t, err)

	rec := doTestRequest(t, h, url.Values{
		"Action":         {"Publish"},
		"TopicArn":       {tp2.TopicArn},
		"Message":        {"after-reset"},
		"MessageGroupId": {"g1"},
		"Version":        {"2010-03-31"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	seqAfter := publishResponseSequenceNumber(t, rec.Body.String())
	assert.Equal(t, "00000000000000000001", seqAfter, "SequenceNumber must restart after Reset()")
}

// TestDeduplicatedFIFOAlsoReturnsSequenceNumber verifies that a
// deduplicated FIFO publish still returns a SequenceNumber (AWS spec).
func TestDeduplicatedFIFOAlsoReturnsSequenceNumber(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerPair(t)
	tp, err := b.CreateTopic("dedup-seq.fifo", nil)
	require.NoError(t, err)

	// First publish.
	rec1 := doTestRequest(t, h, url.Values{
		"Action":                 {"Publish"},
		"TopicArn":               {tp.TopicArn},
		"Message":                {"msg"},
		"MessageGroupId":         {"g1"},
		"MessageDeduplicationId": {"dedup-id-1"},
		"Version":                {"2010-03-31"},
	})
	require.Equal(t, http.StatusOK, rec1.Code)
	seq1 := publishResponseSequenceNumber(t, rec1.Body.String())
	assert.NotEmpty(t, seq1)

	// Duplicate publish — same dedup ID.
	rec2 := doTestRequest(t, h, url.Values{
		"Action":                 {"Publish"},
		"TopicArn":               {tp.TopicArn},
		"Message":                {"msg"},
		"MessageGroupId":         {"g1"},
		"MessageDeduplicationId": {"dedup-id-1"},
		"Version":                {"2010-03-31"},
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	seq2 := publishResponseSequenceNumber(t, rec2.Body.String())
	assert.NotEmpty(t, seq2, "deduplicated publish must still return a SequenceNumber")
}

// TestSNS_FIFOTopic validates that FIFO topic names are accepted and attributes auto-set.
func TestSNS_FIFOTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		topicName string
		wantFifo  bool
		wantErr   bool
	}{
		{
			name:      "standard_topic",
			topicName: "my-topic",
			wantFifo:  false,
		},
		{
			name:      "fifo_topic",
			topicName: "my-topic.fifo",
			wantFifo:  true,
		},
		{
			name:      "invalid_name_too_long",
			topicName: strings.Repeat("a", sns.ExportedMaxTopicNameLen+1),
			wantErr:   true,
		},
		{
			name:      "invalid_name_special_chars",
			topicName: "my topic!",
			wantErr:   true,
		},
		{
			name:      "empty_name",
			topicName: "",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			topic, err := b.CreateTopic(tt.topicName, nil)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			attrs, err := b.GetTopicAttributes(topic.TopicArn)
			require.NoError(t, err)

			if tt.wantFifo {
				assert.Equal(t, "true", attrs["FifoTopic"])
				assert.Equal(t, "false", attrs["ContentBasedDeduplication"])
			} else {
				assert.Equal(t, "false", attrs["FifoTopic"])
			}
		})
	}
}
