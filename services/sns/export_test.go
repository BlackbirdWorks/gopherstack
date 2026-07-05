package sns

import "time"

// Exported for testing.

const (
	// ExportedPageSize exposes the internal pagination page size for test assertions.
	ExportedPageSize = pageSize

	// ExportedMaxPublishBatchEntries exposes the batch-size limit for test assertions.
	ExportedMaxPublishBatchEntries = maxPublishBatchEntries

	// ExportedFifoTopicSuffix exposes the FIFO topic suffix for test assertions.
	ExportedFifoTopicSuffix = fifoTopicSuffix

	// ExportedMaxTopicNameLen exposes the maximum topic name length for test assertions.
	ExportedMaxTopicNameLen = maxTopicNameLen

	// ExportedFifoDedupSweepInterval exposes the sweep cadence for test assertions.
	ExportedFifoDedupSweepInterval = fifoDedupSweepInterval

	// ExportedMaxArchivedMessagesPerTopic exposes the archive cap for test assertions.
	ExportedMaxArchivedMessagesPerTopic = maxArchivedMessagesPerTopic
)

// IsValidTopicNameForTest exposes the topic name validation function for testing.
func IsValidTopicNameForTest(name string) bool { return isValidTopicName(name) }

// CanonicalNotificationStringForTest exposes the canonical string builder for tests
// that verify RSA signature correctness.
func CanonicalNotificationStringForTest(
	msgID, topicARN, subject, message, timestamp string,
) string {
	return canonicalNotificationString(msgID, topicARN, subject, message, timestamp)
}

// MatchesFilterPolicyMessageBodyForTest exposes the MessageBody filter policy
// evaluator for unit tests.
func MatchesFilterPolicyMessageBodyForTest(policy string, message string) (bool, error) {
	parsed, err := parseFilterPolicy(policy)
	if err != nil {
		return false, err
	}

	return matchesFilterPolicyMessageBody(parsed, message), nil
}

// MatchesFilterPolicyAttributesForTest parses a FilterPolicy string and evaluates
// it against a set of message attributes (MessageAttributes scope). The attrs map
// is keyed by attribute name with values of [DataType, StringValue] so callers can
// exercise String/Number/String.Array matching without importing internal types.
func MatchesFilterPolicyAttributesForTest(policy string, attrs map[string][2]string) (bool, error) {
	parsed, err := parseFilterPolicy(policy)
	if err != nil {
		return false, err
	}

	ma := make(map[string]MessageAttribute, len(attrs))
	for name, dv := range attrs {
		ma[name] = MessageAttribute{DataType: dv[0], StringValue: dv[1]}
	}

	return matchesParsedFilterPolicy(parsed, ma), nil
}

// WaitDeliveriesForTest blocks until all in-flight HTTP delivery goroutines complete.
// Use this in tests after Publish to synchronize before asserting DLQ or delivery state.
func WaitDeliveriesForTest(b *InMemoryBackend) {
	b.deliveryWg.Wait()
}

// SigningCertURLForTest exposes the signer's certURL so tests can verify it
// reflects the correct region rather than a hardcoded us-east-1.
func SigningCertURLForTest(b *InMemoryBackend) string {
	return b.signer.certURL()
}

// NewFifoDedupForTest creates a fifoDeduplication for white-box unit tests.
func NewFifoDedupForTest() *fifoDeduplication { return newFifoDeduplication() }

// FifoDedupInsertWithExpiryForTest inserts a dedup entry with an explicit expiry timestamp,
// bypassing the record() method so tests can simulate already-expired entries without sleeping.
func FifoDedupInsertWithExpiryForTest(
	d *fifoDeduplication,
	topicArn, dedupID string,
	expiry time.Time,
) {
	d.mu.Lock()
	defer d.mu.Unlock()
	key := topicArn + "/" + dedupID
	d.entries[key] = expiry
	d.insertOrder = append(d.insertOrder, key)
}

// FifoDedupIsDuplicateForTest calls the internal isDuplicate method.
func FifoDedupIsDuplicateForTest(d *fifoDeduplication, topicArn, dedupID string) bool {
	return d.isDuplicate(topicArn, dedupID)
}

// FifoDedupEntryCountForTest returns the current number of entries in the dedup map.
func FifoDedupEntryCountForTest(d *fifoDeduplication) int {
	d.mu.Lock()
	defer d.mu.Unlock()

	return len(d.entries)
}

// AddOptedOutPhoneNumberForTest directly adds a phone number to the opted-out set,
// bypassing the AWS SNS mechanism (which requires the subscriber to reply STOP).
// Only use in tests that need to assert delivery skips opted-out numbers.
func AddOptedOutPhoneNumberForTest(b *InMemoryBackend, phoneNumber string) {
	b.mu.Lock("AddOptedOutPhoneNumberForTest")
	defer b.mu.Unlock()
	b.optedOutPhoneNumbers[phoneNumber] = true
}
