package support

import "time"

// MaxOpenCases exposes the CaseCreationLimitExceeded open-case cap for tests.
const MaxOpenCases = maxOpenCases

// MaxAttachmentSetCreationsPerWindow exposes the AttachmentLimitExceeded
// rate-limit threshold for tests.
const MaxAttachmentSetCreationsPerWindow = maxAttachmentSetCreationsPerWindow

// MaxDescribeAttachmentCallsPerWindow exposes the
// DescribeAttachmentLimitExceeded rate-limit threshold for tests.
const MaxDescribeAttachmentCallsPerWindow = maxDescribeAttachmentCallsPerWindow

// SeedAttachmentSetCreationTimes injects n synthetic "now" timestamps into
// the attachment-set creation rate-limit window, letting tests exercise
// AttachmentLimitExceeded without looping n real AddAttachmentsToSet calls.
func SeedAttachmentSetCreationTimes(b *InMemoryBackend, n int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	for range n {
		b.attachmentSetCreationTimes = append(b.attachmentSetCreationTimes, now)
	}
}

// SeedDescribeAttachmentCallTimes injects n synthetic "now" timestamps into
// the DescribeAttachment rate-limit window, letting tests exercise
// DescribeAttachmentLimitExceeded without looping n real calls.
func SeedDescribeAttachmentCallTimes(b *InMemoryBackend, n int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	for range n {
		b.describeAttachmentCallTimes = append(b.describeAttachmentCallTimes, now)
	}
}

// CaseCount returns the number of cases in the backend.
func CaseCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.cases.Len()
}

// AttachmentCount returns the number of attachments in the backend.
func AttachmentCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.attachments.Len()
}

// CheckRefreshStatusCount returns the number of tracked refresh statuses.
func CheckRefreshStatusCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.checkRefreshStatuses.Len()
}

// CommunicationCount returns the total number of communications across all cases.
func CommunicationCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	total := 0

	for _, comms := range b.communications {
		total += len(comms)
	}

	return total
}

// HandlerOpsLen returns the number of supported operations in the handler.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
