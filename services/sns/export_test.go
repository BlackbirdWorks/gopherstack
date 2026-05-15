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
)

// IsValidTopicNameForTest exposes the topic name validation function for testing.
func IsValidTopicNameForTest(name string) bool { return isValidTopicName(name) }

// CanonicalNotificationStringForTest exposes the canonical string builder for tests
// that verify RSA signature correctness.
func CanonicalNotificationStringForTest(msgID, topicARN, subject, message, timestamp string) string {
	return canonicalNotificationString(msgID, topicARN, subject, message, timestamp)
}
