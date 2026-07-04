package lambda

import (
	"strings"
	"time"
)

// splitByRecordAge partitions msgs into those within the MaximumRecordAge window
// and those that have expired. A non-positive maxAgeSecs disables age filtering
// (all messages are kept). Messages with an unknown SentTimestamp are kept.
func splitByRecordAge(msgs []*SQSMessage, maxAgeSecs int) ([]*SQSMessage, []*SQSMessage) {
	if maxAgeSecs <= 0 {
		return msgs, nil
	}

	var kept, expired []*SQSMessage

	cutoff := time.Now().Add(-time.Duration(maxAgeSecs) * time.Second).UnixMilli()

	for _, msg := range msgs {
		if msg.SentTimestampMillis > 0 && msg.SentTimestampMillis < cutoff {
			expired = append(expired, msg)

			continue
		}

		kept = append(kept, msg)
	}

	return kept, expired
}

// splitByFilter partitions msgs into those that satisfy the FilterCriteria and
// those that match no filter. A nil/empty criteria keeps every message.
func splitByFilter(fc *FilterCriteria, msgs []*SQSMessage) ([]*SQSMessage, []*SQSMessage) {
	if fc == nil || len(fc.Filters) == 0 {
		return msgs, nil
	}

	var matched, filtered []*SQSMessage

	for _, msg := range msgs {
		if eventFilterMatches(fc, sqsFilterView(msg)) {
			matched = append(matched, msg)

			continue
		}

		filtered = append(filtered, msg)
	}

	return matched, filtered
}

// receiptHandles extracts the receipt handles from a slice of messages.
func receiptHandles(msgs []*SQSMessage) []string {
	handles := make([]string, 0, len(msgs))
	for _, msg := range msgs {
		handles = append(handles, msg.ReceiptHandle)
	}

	return handles
}

// sqsBatchBuffer accumulates a partial SQS batch for a mapping while its
// MaximumBatchingWindow has not yet elapsed.
type sqsBatchBuffer struct {
	firstSeen time.Time
	index     map[string]int
	order     []*SQSMessage
}

// accumulateSQSBatch honors MaximumBatchingWindowInSeconds. When no window is
// configured it returns the incoming messages for immediate delivery. Otherwise
// it buffers messages (de-duplicating by message ID, refreshing receipt handles)
// and only returns a batch to flush once the batch reaches BatchSize or the
// window has elapsed since the first buffered message.
func (p *EventSourcePoller) accumulateSQSBatch(
	m *EventSourceMapping,
	incoming []*SQSMessage,
) ([]*SQSMessage, bool) {
	if m.MaximumBatchingWindowInSeconds <= 0 {
		return incoming, true
	}

	p.mu.Lock("accumulateSQSBatch")
	defer p.mu.Unlock()

	buf := p.sqsBatchBuffers[m.UUID]
	if buf == nil {
		buf = &sqsBatchBuffer{index: make(map[string]int)}
		p.sqsBatchBuffers[m.UUID] = buf
	}

	for _, msg := range incoming {
		if idx, ok := buf.index[msg.MessageID]; ok {
			buf.order[idx] = msg // refresh receipt handle for a redelivered message

			continue
		}

		buf.index[msg.MessageID] = len(buf.order)
		buf.order = append(buf.order, msg)
	}

	if len(buf.order) == 0 {
		return nil, false
	}

	if buf.firstSeen.IsZero() {
		buf.firstSeen = time.Now()
	}

	window := time.Duration(m.MaximumBatchingWindowInSeconds) * time.Second
	full := m.BatchSize > 0 && len(buf.order) >= m.BatchSize
	elapsed := time.Since(buf.firstSeen) >= window

	if !full && !elapsed {
		return nil, false
	}

	batch := buf.order
	delete(p.sqsBatchBuffers, m.UUID)

	return batch, true
}

// sqsRegionFromARN extracts the region segment from an SQS ARN of the form
// arn:aws:sqs:<region>:<account>:<queue>. It returns "" when the ARN is malformed.
func sqsRegionFromARN(arnStr string) string {
	const sqsARNPartCount = 6

	parts := strings.SplitN(arnStr, ":", sqsARNPartCount)
	if len(parts) < sqsARNPartCount || parts[0] != "arn" || parts[2] != "sqs" {
		return ""
	}

	return parts[3]
}
