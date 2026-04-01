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
)

// IsValidTopicNameForTest exposes the topic name validation function for testing.
func IsValidTopicNameForTest(name string) bool { return isValidTopicName(name) }
