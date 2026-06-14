package sns

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
func CanonicalNotificationStringForTest(msgID, topicARN, subject, message, timestamp string) string {
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

// WaitDeliveriesForTest blocks until all in-flight HTTP delivery goroutines complete.
// Use this in tests after Publish to synchronize before asserting DLQ or delivery state.
func WaitDeliveriesForTest(b *InMemoryBackend) {
	b.deliveryWg.Wait()
}
