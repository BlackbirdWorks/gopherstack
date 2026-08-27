package sqs

import (
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// resolveVisibilityTimeout returns the effective visibility timeout for a receive operation.
func resolveVisibilityTimeout(requested int, q *Queue) int {
	if requested >= 0 {
		return requested
	}

	if v, err := strconv.Atoi(q.Attributes[attrVisibilityTimeout]); err == nil {
		return v
	}

	return defaultVisibilityTimeout
}

// buildBlockedGroups returns the set of FIFO message group IDs that currently
// have at least one in-flight message. Messages in a blocked group must not be
// delivered until all earlier in-flight messages for that group are deleted,
// ensuring strict per-group ordering.
func buildBlockedGroups(inflight []*InFlightMessage) map[string]bool {
	blocked := make(map[string]bool)
	for _, inf := range inflight {
		if inf.Msg.MessageGroupID != "" {
			blocked[inf.Msg.MessageGroupID] = true
		}
	}

	return blocked
}

// prepareAndPickMessages consolidates reQueueExpired, expireRetainedMessages,
// drainToDLQ, and pickMessages into two focused passes (#54), reducing
// repeated full-slice walks. It also maintains q.inFlightByHandle (#56) and
// q.delayedCount (#59). Caller must hold q.mu (or the global write lock).
//
// Pass 1 sweeps inFlightMessages: discards retention-expired entries,
// re-queues visibility-expired entries. Pass 2 sweeps q.messages (including
// newly re-queued ones): discards retention-expired, drains to DLQ, picks up
// to maxMessages visible messages. maxMessages=0 performs cleanup only.
// requeueMessage returns msg to the pending queue after its visibility timeout
// is reset — either explicitly via ChangeMessageVisibility(0) or implicitly
// via expiry in sweepInFlight. Caller must hold q.mu.
//
// For FIFO queues this must NOT simply append to the end of q.messages: a
// message can be sent to a group while an earlier message from that SAME
// group is in flight (in-flight messages block further delivery but not
// further sends). If the earlier message is later reset/expired and appended
// to the tail, it would land behind the newer same-group message already
// sitting in q.messages, and the next receive would hand out the newer
// message first — violating AWS's strict per-message-group ordering
// guarantee. Reinserting by SequenceNumber restores the correct position.
// q.messages is otherwise kept in ascending SequenceNumber order (SendMessage
// only ever appends in send order, and pickVisibleMessages compacts in place
// without reordering), and SequenceNumber is a fixed-width zero-padded
// decimal string, so lexicographic comparison matches numeric order.
//
// Standard queues have no AWS ordering guarantee, so they take the O(1)
// append path unconditionally.
func requeueMessage(q *Queue, msg *Message) {
	if !q.IsFIFO {
		q.messages = append(q.messages, msg)

		return
	}

	idx := sort.Search(len(q.messages), func(i int) bool {
		return q.messages[i].SequenceNumber > msg.SequenceNumber
	})

	q.messages = append(q.messages, nil)
	copy(q.messages[idx+1:], q.messages[idx:])
	q.messages[idx] = msg
}

// sweepInFlight processes q.inFlightMessages: discards retention-expired entries and
// re-queues visibility-expired entries back onto q.messages. Caller must hold q.mu.
func sweepInFlight(q *Queue, cutoff, now time.Time) {
	changed := false
	newInFlight := q.inFlightMessages[:0]

	for _, inf := range q.inFlightMessages {
		if time.UnixMilli(inf.Msg.SentTimestamp).Before(cutoff) {
			delete(q.inFlightByHandle, inf.ReceiptHandle)
			changed = true

			continue
		}

		if now.After(inf.VisibleAt) {
			delete(q.inFlightByHandle, inf.ReceiptHandle)
			if !tryRouteToDLQ(q, inf.Msg, now) {
				requeueMessage(q, inf.Msg)
			}
			changed = true

			continue
		}

		newInFlight = append(newInFlight, inf)
	}

	if changed {
		q.inFlightMessages = newInFlight
	}
}

// pickVisibleMessages compacts q.messages in-place, routing DLQ-bound messages,
// skipping blocked FIFO groups, and picking up to maxMessages visible messages.
// Caller must hold q.mu.
func pickVisibleMessages(
	q *Queue,
	blockedGroups map[string]bool,
	maxMessages, vt int,
	now time.Time,
	cutoff time.Time,
	accountID string,
) []*Message {
	// nolint:prealloc,nolintlint // capacity tainted by user input — satisfies CodeQL
	result := make([]*Message, 0)
	j := 0

	for _, msg := range q.messages {
		if time.UnixMilli(msg.SentTimestamp).Before(cutoff) {
			continue
		}

		if tryRouteToDLQ(q, msg, now) {
			continue
		}

		if q.IsFIFO && msg.MessageGroupID != "" && blockedGroups[msg.MessageGroupID] {
			q.messages[j] = msg
			j++

			continue
		}

		if maxMessages > 0 && len(result) < maxMessages && !now.Before(msg.VisibleAt) {
			enqueueReceivedMessage(q, msg, blockedGroups, now, vt, accountID)
			result = append(result, msg)

			continue
		}

		q.messages[j] = msg
		j++
	}

	oldLen := len(q.messages)
	for k := j; k < oldLen; k++ {
		q.messages[k] = nil
	}
	q.messages = q.messages[:j]

	return result
}

// enqueueReceivedMessage stamps msg with a receipt handle, increments counters, registers it
// as in-flight on q, and marks its FIFO group as blocked. Caller must hold q.mu.
func enqueueReceivedMessage(
	q *Queue,
	msg *Message,
	blockedGroups map[string]bool,
	now time.Time,
	vt int,
	accountID string,
) {
	q.receiveGeneration++
	receipt := msg.MessageID + ":" + strconv.FormatUint(q.receiveGeneration, 10) + ":" + uuid.NewString()
	msg.ReceiptHandle = receipt
	msg.ApproximateReceiveCount++
	msg.Attributes[attrApproxReceiveCount] = strconv.Itoa(msg.ApproximateReceiveCount)

	if msg.ApproximateFirstReceiveTimestamp == 0 {
		msg.ApproximateFirstReceiveTimestamp = now.UnixMilli()
		msg.Attributes[attrApproxFirstReceiveTimestamp] = strconv.FormatInt(
			msg.ApproximateFirstReceiveTimestamp,
			10,
		)
		msg.Attributes[attrSenderID] = accountID
	}

	inf := &InFlightMessage{
		VisibleAt:     now.Add(time.Duration(vt) * time.Second),
		ReceiptHandle: receipt,
		Generation:    q.receiveGeneration,
		Msg:           msg,
	}
	q.inFlightMessages = append(q.inFlightMessages, inf)
	q.inFlightByHandle[receipt] = inf

	if q.IsFIFO && msg.MessageGroupID != "" {
		blockedGroups[msg.MessageGroupID] = true
	}
}

// tryFastPickMessages attempts O(maxMessages) head-picking when the leading messages
// are all visible, non-expired, and non-blocked. Returns (messages, true) on success.
func tryFastPickMessages(
	q *Queue,
	blockedGroups map[string]bool,
	maxMessages, vt int,
	now time.Time,
	cutoff time.Time,
	accountID string,
) ([]*Message, bool) {
	if maxMessages <= 0 || len(q.messages) == 0 {
		return nil, false
	}

	n := min(maxMessages, len(q.messages))
	for i := range n {
		msg := q.messages[i]
		if time.UnixMilli(msg.SentTimestamp).Before(cutoff) {
			return nil, false
		}
		if tryRouteToDLQ(q, msg, now) {
			return nil, false
		}
		if q.IsFIFO && msg.MessageGroupID != "" && blockedGroups[msg.MessageGroupID] {
			return nil, false
		}
		if now.Before(msg.VisibleAt) {
			return nil, false
		}
	}

	result := make([]*Message, n)
	for i := range n {
		msg := q.messages[i]
		enqueueReceivedMessage(q, msg, blockedGroups, now, vt, accountID)
		result[i] = msg
		q.messages[i] = nil
	}

	q.messages = q.messages[n:]

	return result, true
}

func prepareAndPickMessages(
	q *Queue,
	accountID string,
	maxMessages, vt int,
	now time.Time,
) []*Message {
	retentionSecs, err := strconv.Atoi(q.Attributes[attrMessageRetentionPeriod])
	if err != nil || retentionSecs <= 0 {
		retentionSecs = defaultMessageRetentionPeriod
	}

	cutoff := now.Add(-time.Duration(retentionSecs) * time.Second)

	// Pass 1: sweep inFlightMessages — discard retention-expired, re-queue visibility-expired.
	sweepInFlight(q, cutoff, now)

	// Pass 2: sweep q.messages (original + re-queued from Pass 1) in-place.
	var blockedGroups map[string]bool
	if q.IsFIFO {
		blockedGroups = buildBlockedGroups(q.inFlightMessages)
	}

	var result []*Message
	if fastResult, ok := tryFastPickMessages(q, blockedGroups, maxMessages, vt, now, cutoff, accountID); ok {
		result = fastResult
	} else {
		result = pickVisibleMessages(q, blockedGroups, maxMessages, vt, now, cutoff, accountID)
	}

	// Recompute delayedCount only when delayed messages are known to exist (#59).
	if q.delayedCount > 0 {
		q.delayedCount = 0
		for _, msg := range q.messages {
			if now.Before(msg.VisibleAt) {
				q.delayedCount++
			}
		}
	}

	return result
}

// ChangeMessageVisibility updates the visibility timeout for an in-flight message.
func (b *InMemoryBackend) ChangeMessageVisibility(input *ChangeMessageVisibilityInput) error {
	if input.VisibilityTimeout < 0 || input.VisibilityTimeout > maxVisibilityTimeoutSeconds {
		return ErrInvalidVisibilityTimeout
	}

	// #55: per-queue lock.
	b.mu.RLock("ChangeMessageVisibility")
	name := queueNameFromInput(input.QueueURL)
	q, ok := b.lookupQueueByName(input.Region, name)
	b.mu.RUnlock()

	if !ok {
		return ErrQueueNotFound
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	return changeVisibility(q, input.ReceiptHandle, input.VisibilityTimeout)
}

// changeVisibility updates the VisibleAt time for an in-flight message by receipt handle.
// When visibilityTimeout is 0 the message is immediately returned to the visible queue,
// matching the AWS behaviour where a zero timeout makes a message immediately available.
// Caller must hold q.mu.
func changeVisibility(q *Queue, receiptHandle string, visibilityTimeout int) error {
	// Use inFlightByHandle for lookup; fall back to linear scan if map not populated
	// (e.g., restored from snapshot before #56 was applied).
	inf, found := q.inFlightByHandle[receiptHandle]
	if !found {
		// Fallback: linear scan for compatibility with snapshots that predate #56.
		for _, candidate := range q.inFlightMessages {
			if candidate.ReceiptHandle == receiptHandle {
				inf = candidate
				found = true

				break
			}
		}
	}

	if !found {
		return ErrMessageNotInflight
	}

	if visibilityTimeout == 0 {
		// Move back to the visible queue immediately.
		now := time.Now()
		inf.Msg.VisibleAt = now
		if !tryRouteToDLQ(q, inf.Msg, now) {
			requeueMessage(q, inf.Msg)
		}
		delete(q.inFlightByHandle, receiptHandle)

		// Remove from inFlightMessages slice.
		for i, existing := range q.inFlightMessages {
			if existing == inf {
				last := len(q.inFlightMessages) - 1
				q.inFlightMessages[i] = q.inFlightMessages[last]
				q.inFlightMessages[last] = nil
				q.inFlightMessages = q.inFlightMessages[:last]

				break
			}
		}

		// Wake long-poll receivers that may be waiting for a message.
		old := q.notify
		q.notify = make(chan struct{})
		close(old)

		return nil
	}

	inf.VisibleAt = time.Now().Add(time.Duration(visibilityTimeout) * time.Second)

	return nil
}

// ChangeMessageVisibilityBatch updates visibility for a batch of in-flight messages.
func (b *InMemoryBackend) ChangeMessageVisibilityBatch(
	input *ChangeMessageVisibilityBatchInput,
) (*ChangeMessageVisibilityBatchOutput, error) {
	ids := make([]string, len(input.Entries))
	for i, e := range input.Entries {
		ids[i] = e.ID
	}

	if err := validateBatchEnvelope(ids); err != nil {
		return nil, err
	}

	// #55: per-queue lock.
	b.mu.RLock("ChangeMessageVisibilityBatch")
	name := queueNameFromInput(input.QueueURL)
	q, ok := b.lookupQueueByName(input.Region, name)
	b.mu.RUnlock()

	if !ok {
		return nil, ErrQueueNotFound
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	out := &ChangeMessageVisibilityBatchOutput{}

	for _, entry := range input.Entries {
		if entry.VisibilityTimeout < 0 || entry.VisibilityTimeout > maxVisibilityTimeoutSeconds {
			out.Failed = append(out.Failed, BatchErrorEntry{
				ID:          entry.ID,
				Code:        "InvalidParameterValue",
				Message:     "Value for parameter VisibilityTimeout is invalid. Reason: Must be between 0 and 43200, if provided.",
				SenderFault: true,
			})

			continue
		}

		if err := changeVisibility(q, entry.ReceiptHandle, entry.VisibilityTimeout); err != nil {
			out.Failed = append(out.Failed, BatchErrorEntry{
				ID:          entry.ID,
				Code:        "MessageNotInflight",
				Message:     err.Error(),
				SenderFault: true,
			})
		} else {
			out.Successful = append(out.Successful, BatchResultEntry{ID: entry.ID})
		}
	}

	return out, nil
}
