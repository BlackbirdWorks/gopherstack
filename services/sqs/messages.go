package sqs

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// buildInitialMessageAttributes returns the system-attribute map stamped on a
// new message at send-time. AWSTraceHeader is the one reserved name SQS
// accepts; we copy its StringValue verbatim so consumers see the original
// X-Ray trace context.
func buildInitialMessageAttributes(
	sentTS string, sysAttrs map[string]MessageAttributeValue,
) map[string]string {
	attrs := map[string]string{
		attrSentTimestamp:      sentTS,
		attrApproxReceiveCount: attrValZero,
	}

	if v, ok := sysAttrs[attrAWSTraceHeader]; ok && v.StringValue != "" {
		attrs[attrAWSTraceHeader] = v.StringValue
	}

	return attrs
}

// SendMessage adds a message to the specified queue.
func (b *InMemoryBackend) SendMessage(input *SendMessageInput) (*SendMessageOutput, error) {
	if input.MessageBody == "" {
		return nil, ErrInvalidMessageBody
	}

	if input.DelaySeconds < 0 || input.DelaySeconds > maxDelaySeconds {
		return nil, ErrInvalidDelaySeconds
	}

	if err := validateMessageAttributes(input.MessageAttributes); err != nil {
		return nil, err
	}

	md5Body := computeBodyChecksumMD5(input.MessageBody)
	sha256Body := computeSHA256(input.MessageBody)
	md5Attrs := computeMD5OfMessageAttributes(input.MessageAttributes)
	md5SysAttrs := computeMD5OfMessageAttributes(input.MessageSystemAttributes)
	msgID := uuid.New().String()

	// #55: resolve queue under global RLock, then mutate under per-queue lock.
	b.mu.RLock("SendMessage")
	name := queueNameFromInput(input.QueueURL)
	q, ok := b.lookupQueueByName(input.Region, name)
	b.mu.RUnlock()

	if !ok {
		return nil, ErrQueueNotFound
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	out, err := sendMessageLocked(q, input, md5Body, sha256Body, md5Attrs, md5SysAttrs, msgID, time.Now())
	if err != nil {
		return nil, err
	}

	go b.emitMetric("NumberOfMessagesSent", 1)

	return out, nil
}

// sendMessageLocked appends one message to an already-locked queue.
// md5Body, sha256Body, md5Attrs, and msgID must be pre-computed by the caller.
// Caller must hold q.mu (#55). Used by both SendMessage and SendMessageBatch (#58).
func sendMessageLocked(
	q *Queue,
	input *SendMessageInput,
	md5Body, sha256Body, md5Attrs, md5SysAttrs, msgID string,
	now time.Time,
) (*SendMessageOutput, error) {
	// SendMessage's top-level entry point already checks these three (empty
	// body, DelaySeconds range, message attribute shape) before calling in
	// here, but SendMessageBatch calls this function directly per entry with
	// no equivalent checks of its own. Without repeating them here, a batch
	// entry could carry an empty body, an out-of-range DelaySeconds (e.g.
	// negative or > 900), or a malformed/reserved-name message attribute and
	// have it silently accepted instead of surfaced as a per-entry
	// BatchResultErrorEntry, unlike real AWS.
	if input.MessageBody == "" {
		return nil, ErrInvalidMessageBody
	}

	if input.DelaySeconds < 0 || input.DelaySeconds > maxDelaySeconds {
		return nil, ErrInvalidDelaySeconds
	}

	if err := validateMessageAttributes(input.MessageAttributes); err != nil {
		return nil, err
	}

	if err := validateMessageSize(input.MessageBody, input.MessageAttributes, q); err != nil {
		return nil, err
	}

	if q.IsFIFO {
		if pre := preflightFIFOSend(q, input, md5Body, sha256Body, now); pre.Handled {
			return pre.Output, pre.Err
		}
	}

	sentTS := strconv.FormatInt(now.UnixMilli(), 10)

	var seqNum string
	if q.IsFIFO {
		q.fifoSeqCounter++
		seqNum = fmt.Sprintf("%020d", q.fifoSeqCounter)
	}

	msg := &Message{
		MessageID:                    msgID,
		Body:                         input.MessageBody,
		MD5OfBody:                    md5Body,
		MD5OfMessageAttributes:       md5Attrs,
		MD5OfMessageSystemAttributes: md5SysAttrs,
		MessageGroupID:               input.MessageGroupID,
		MessageDeduplicationID:       input.MessageDeduplicationID,
		SequenceNumber:               seqNum,
		SentTimestamp:                now.UnixMilli(),
		MessageAttributes:            input.MessageAttributes,
		Attributes: buildInitialMessageAttributes(
			sentTS,
			input.MessageSystemAttributes,
		),
		VisibleAt: resolveMessageVisibleAt(
			now,
			input.DelaySeconds,
			q.Attributes[attrDelaySeconds],
		),
	}

	if q.IsFIFO {
		msg.Attributes[attrSequenceNumber] = seqNum
		if input.MessageGroupID != "" {
			msg.Attributes[attrMessageGroupIDSys] = input.MessageGroupID
		}

		if input.MessageDeduplicationID != "" {
			msg.Attributes[attrMessageDeduplicationIDSys] = input.MessageDeduplicationID
		}

		storeDedup(
			q, input.MessageGroupID, input.MessageDeduplicationID,
			sha256Body, q.Attributes[attrContentBasedDeduplication], msgID, now,
		)
	}

	// #59: maintain delayed-message counter for O(1) GetQueueAttributes.
	if now.Before(msg.VisibleAt) {
		q.delayedCount++
	}

	q.messages = append(q.messages, msg)
	q.hasActivity.Store(true)

	// Broadcast to all long-polling receivers: close the current generation channel
	// (which unblocks all goroutines waiting on it) and replace it with a new one.
	// Any receiver holding a reference to the old closed channel will wake up, re-check
	// for messages under the lock, and (if no messages are found yet) capture the new
	// channel for the next wait. This provides fair wake-up for all concurrent receivers.
	old := q.notify
	q.notify = make(chan struct{})
	close(old)

	return &SendMessageOutput{
		MessageID:                    msgID,
		MD5OfBody:                    md5Body,
		MD5OfMessageAttributes:       md5Attrs,
		MD5OfMessageSystemAttributes: md5SysAttrs,
		SequenceNumber:               seqNum,
	}, nil
}

// validateMessageSize checks whether the total message size (body plus
// attribute names, values, and types) exceeds the queue's MaximumMessageSize.
// AWS SQS measures size as the sum of UTF-8 bytes across the body and every
// attribute name / type / value, matching the documented limit.
func validateMessageSize(body string, attrs map[string]MessageAttributeValue, q *Queue) error {
	maxSize := defaultMaxMessageSize

	if v, err := strconv.Atoi(q.Attributes[attrMaximumMessageSize]); err == nil && v > 0 {
		maxSize = v
	}

	total := len(body)
	for name, attr := range attrs {
		total += len(name) + len(attr.DataType) + len(attr.StringValue) + len(attr.BinaryValue)
	}

	if total > maxSize {
		return ErrMessageTooLarge
	}

	return nil
}

// isValidDataTypeBase reports whether base is one of the three AWS-defined
// message attribute base types: String, Number, or Binary.
func isValidDataTypeBase(base string) bool {
	return base == "String" || base == "Number" || base == "Binary"
}

// maxMessageAttributeCount is the AWS maximum number of user-defined message
// attributes per message. System attributes (MessageSystemAttributes) are separate.
const maxMessageAttributeCount = 10

// validateMessageAttributes checks that each message attribute has a recognised
// DataType and that the correct value field is populated.
// AWS rules:
//   - At most 10 user-defined message attributes per message
//   - Attribute names must not start with reserved prefixes "AWS." or "Amazon." (case-insensitive)
//   - DataType must be "String", "Number", "Binary", or "<base>.<custom-suffix>"
//   - String/Number attributes must supply StringValue
//   - Binary attributes must supply BinaryValue
func validateMessageAttributes(attrs map[string]MessageAttributeValue) error {
	if len(attrs) > maxMessageAttributeCount {
		return ErrInvalidMessageAttributeValue
	}

	for name, attr := range attrs {
		if isReservedMessageAttributeName(name) {
			return ErrInvalidMessageAttributeValue
		}

		base, _, _ := strings.Cut(attr.DataType, ".")
		if !isValidDataTypeBase(base) {
			return ErrInvalidMessageAttributeValue
		}

		if base == "Binary" {
			if len(attr.BinaryValue) == 0 {
				return ErrInvalidMessageAttributeValue
			}
		} else {
			if attr.StringValue == "" {
				return ErrInvalidMessageAttributeValue
			}
		}
	}

	return nil
}

// isReservedMessageAttributeName reports whether name starts with a reserved prefix.
// AWS SQS rejects user-defined message attribute names that start with "AWS." or
// "Amazon." (case-insensitive). These namespaces are reserved for system use.
func isReservedMessageAttributeName(name string) bool {
	lower := strings.ToLower(name)

	return strings.HasPrefix(lower, "aws.") || strings.HasPrefix(lower, "amazon.")
}

// resolveMessageVisibleAt computes the earliest time the message should be visible.
// Message-level delaySeconds takes precedence over the queue-level attribute.
// A zero [time.Time] return value means the message is immediately visible (no delay).
func resolveMessageVisibleAt(now time.Time, msgDelaySeconds int, queueDelayAttr string) time.Time {
	if msgDelaySeconds > 0 {
		return now.Add(time.Duration(msgDelaySeconds) * time.Second)
	}

	if qd, err := strconv.Atoi(queueDelayAttr); err == nil && qd > 0 {
		return now.Add(time.Duration(qd) * time.Second)
	}

	// Zero time means no delay — the message is immediately visible to consumers.
	return time.Time{}
}

// ReceiveMessage retrieves messages from the queue, with optional long-poll wait.
//
// Long polling uses a broadcast notify channel: SendMessage closes the current generation
// channel (waking all waiting receivers) and replaces it with a new one. This ensures
// all concurrent long-poll receivers are woken on each message arrival rather than just one.
// A 1-second recheck interval is also applied so that messages which reappear
// due to visibility-timeout expiry (reQueueExpired) are picked up promptly even
// when no new SendMessage occurs.
// validateReceiveInput validates and normalises the receive input, returning an
// error if any parameter is out of range.  It mutates MaxNumberOfMessages so
// that a zero value becomes the AWS default of 1.
func validateReceiveInput(input *ReceiveMessageInput) error {
	if input.WaitTimeSeconds < 0 || input.WaitTimeSeconds > maxReceiveMessageWaitTimeSeconds {
		return ErrInvalidWaitTime
	}

	// AWS accepts MaxNumberOfMessages in [1, 10]. Default to 1 when unset (0).
	if input.MaxNumberOfMessages == 0 {
		input.MaxNumberOfMessages = 1
	}

	if input.MaxNumberOfMessages < 1 || input.MaxNumberOfMessages > maxBatchSize {
		return ErrInvalidMaxMessages
	}

	// Validate VisibilityTimeout range here — centrally, in the backend — so
	// every caller gets the same AWS-accurate rejection regardless of which
	// protocol front-end is in use. Previously only the JSON handler
	// (handleReceiveMessage) checked this range; the Query (XML) protocol
	// path parsed the parameter and passed it straight through unchecked, so
	// an out-of-range VisibilityTimeout sent over the legacy Query API
	// silently produced a message that would effectively never become
	// visible again instead of the AWS InvalidParameterValue error.
	// NoVisibilityTimeout (-1) is the "unspecified, use the queue's default"
	// sentinel and is exempt from range checking.
	if input.VisibilityTimeout != NoVisibilityTimeout &&
		(input.VisibilityTimeout < 0 || input.VisibilityTimeout > maxVisibilityTimeoutSeconds) {
		return ErrInvalidVisibilityTimeout
	}

	return nil
}

func (b *InMemoryBackend) ReceiveMessage(
	input *ReceiveMessageInput,
) (*ReceiveMessageOutput, error) {
	if err := validateReceiveInput(input); err != nil {
		return nil, err
	}

	// If the caller did not specify a positive WaitTimeSeconds, apply the queue's
	// ReceiveMessageWaitTimeSeconds attribute as the default long-poll duration.
	// This mirrors the AWS behaviour where the queue-level attribute acts as the
	// default wait time for receives that omit the parameter.
	waitSecs := b.resolveWaitSeconds(input.QueueURL, input.WaitTimeSeconds)

	name := queueNameFromInput(input.QueueURL)
	deadline := time.Now().Add(time.Duration(waitSecs) * time.Second)

	const recheckInterval = time.Second

	timer := time.NewTimer(recheckInterval)
	defer timer.Stop()

	for {
		msgs, notifyCh, err := b.receiveOnce(name, input)
		if err != nil {
			return nil, err
		}

		if len(msgs) > 0 {
			count := float64(len(msgs))
			go b.emitMetric("NumberOfMessagesReceived", count)

			return &ReceiveMessageOutput{Messages: msgs}, nil
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return &ReceiveMessageOutput{}, nil
		}

		sleep := min(remaining, recheckInterval)

		// Stop and drain the timer before Reset to guarantee the channel is empty
		// before the next iteration begins. Both select arms leave the timer in a
		// consistent state: the notifyCh arm already stops and drains; the C arm
		// fires normally and drains. An explicit stop-and-drain here makes the
		// invariant explicit and safe against future loop restructuring.
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}

		timer.Reset(sleep)

		select {
		case <-notifyCh:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
		}
	}
}

// resolveWaitSeconds returns the effective long-poll wait duration.
// If requested is positive it is returned unchanged. Otherwise the queue's
// ReceiveMessageWaitTimeSeconds attribute is used as the default.
func (b *InMemoryBackend) resolveWaitSeconds(queueURL string, requested int) int {
	if requested > 0 {
		return requested
	}

	b.mu.RLock("resolveWaitSeconds")
	defer b.mu.RUnlock()

	if q, ok := b.lookupQueueByURL("", queueURL); ok {
		if v, err := strconv.Atoi(q.Attributes[attrReceiveMessageWaitTimeSeconds]); err == nil &&
			v > 0 {
			return v
		}
	}

	return 0
}

// receiveAttemptTTL is the AWS-specified window for ReceiveRequestAttemptID
// deduplication on FIFO queues (5 minutes).
const receiveAttemptTTL = 5 * time.Minute

// receiveOnce performs a single receive attempt under the per-queue lock (#55).
func (b *InMemoryBackend) receiveOnce(
	name string,
	input *ReceiveMessageInput,
) ([]*Message, chan struct{}, error) {
	// #55: resolve queue under global RLock, then mutate under per-queue lock.
	b.mu.RLock("receiveOnce")
	q, ok := b.lookupQueueByName(input.Region, name)
	b.mu.RUnlock()

	if !ok {
		return nil, nil, ErrQueueNotFound
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()

	// #54: single-pass prepareAndPickMessages replaces the four-pass sequence.
	if q.IsFIFO {
		pruneDedup(q, now)
		pruneReceiveAttempts(q, now)

		// FIFO exactly-once retry: if the caller repeats with the same
		// ReceiveRequestAttemptID within 5 minutes, return the cached result.
		if id := input.ReceiveRequestAttemptID; id != "" {
			if entry, found := q.receiveAttempts[id]; found && now.Before(entry.expiresAt) {
				return entry.msgs, q.notify, nil
			}
		}
	}

	maxMessages := input.MaxNumberOfMessages
	if maxMessages <= 0 {
		maxMessages = 1
	}
	if maxMessages > maxBatchSize {
		maxMessages = maxBatchSize
	}

	// AWS rejects ReceiveMessage when the queue is already at its in-flight cap
	// (120k for standard, 20k for FIFO) — clients see OverLimit and must wait.
	limit := maxInFlightStandard
	if q.IsFIFO {
		limit = maxInFlightFIFO
	}
	if len(q.inFlightMessages) >= limit {
		return nil, q.notify, ErrOverLimit
	}

	vt := resolveVisibilityTimeout(input.VisibilityTimeout, q)
	msgs := prepareAndPickMessages(q, b.accountID, maxMessages, vt, now)

	// Cache the result for FIFO ReceiveRequestAttemptID deduplication.
	if q.IsFIFO && input.ReceiveRequestAttemptID != "" && len(msgs) > 0 {
		if q.receiveAttempts == nil {
			q.receiveAttempts = make(map[string]*receiveAttemptEntry)
		}
		q.receiveAttempts[input.ReceiveRequestAttemptID] = &receiveAttemptEntry{
			msgs:      msgs,
			expiresAt: now.Add(receiveAttemptTTL),
		}
	}

	return msgs, q.notify, nil
}

// pruneReceiveAttempts removes expired ReceiveRequestAttemptID cache entries.
// Caller must hold b.mu (write).
func pruneReceiveAttempts(q *Queue, now time.Time) {
	for id, entry := range q.receiveAttempts {
		if !now.Before(entry.expiresAt) {
			delete(q.receiveAttempts, id)
		}
	}
}

// maxInFlightStandard / maxInFlightFIFO are AWS's per-queue caps for messages
// that have been received but not yet deleted or visibility-expired.
const (
	maxInFlightStandard = 120000
	maxInFlightFIFO     = 20000
)

// DeleteMessage removes an in-flight message by its receipt handle.
// Uses inFlightByHandle for O(1) lookup (#56) and per-queue lock (#55).
func (b *InMemoryBackend) DeleteMessage(input *DeleteMessageInput) error {
	// #55: resolve queue under global RLock, then mutate under per-queue lock.
	b.mu.RLock("DeleteMessage")
	name := queueNameFromInput(input.QueueURL)
	q, ok := b.lookupQueueByName(input.Region, name)
	b.mu.RUnlock()

	if !ok {
		return ErrQueueNotFound
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	// #56: O(1) lookup via receipt-handle index.
	inf, found := q.inFlightByHandle[input.ReceiptHandle]
	if !found {
		return ErrReceiptHandleInvalid
	}

	delete(q.inFlightByHandle, input.ReceiptHandle)

	// Swap-delete from the slice: find the entry by pointer and swap with last.
	for i, existing := range q.inFlightMessages {
		if existing == inf {
			last := len(q.inFlightMessages) - 1
			q.inFlightMessages[i] = q.inFlightMessages[last]
			q.inFlightMessages[last] = nil
			q.inFlightMessages = q.inFlightMessages[:last]

			break
		}
	}

	go b.emitMetric("NumberOfMessagesDeleted", 1)

	return nil
}

// isValidBatchEntryID reports whether id conforms to the AWS batch entry ID
// format: 1-80 characters from [A-Za-z0-9_-].
func isValidBatchEntryID(id string) bool {
	if id == "" || len(id) > maxQueueNameLength {
		return false
	}

	for _, c := range id {
		if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') &&
			(c < '0' || c > '9') && c != '_' && c != '-' {
			return false
		}
	}

	return true
}

// validateBatchEnvelope checks request-level batch constraints that apply
// identically to SendMessageBatch, DeleteMessageBatch, and
// ChangeMessageVisibilityBatch:
//  1. At least one entry (empty → EmptyBatchRequest)
//  2. At most 10 entries (> 10 → TooManyEntriesInBatchRequest)
//  3. All IDs distinct (duplicates → BatchEntryIdsNotDistinct)
//  4. Each ID matches ^[A-Za-z0-9_-]{1,80}$ (invalid → EmptyBatchRequest)
func validateBatchEnvelope(ids []string) error {
	if len(ids) == 0 {
		return ErrInvalidBatchEntry
	}

	if len(ids) > maxBatchSize {
		return ErrTooManyEntriesInBatch
	}

	seen := make(map[string]struct{}, len(ids))

	for _, id := range ids {
		if !isValidBatchEntryID(id) {
			return ErrInvalidBatchEntry
		}

		if _, dup := seen[id]; dup {
			return ErrBatchEntryIDsNotDistinct
		}

		seen[id] = struct{}{}
	}

	return nil
}

// batchEntryPrep holds pre-computed crypto digests and a generated message ID for one
// SendMessageBatch entry, computed outside the queue lock.
type batchEntryPrep struct {
	md5Body     string
	sha256Body  string
	md5Attrs    string
	md5SysAttrs string
	msgID       string
}

// processSendMessageBatchEntries iterates over batch entries (already lock-held on q),
// delegates to sendMessageLocked, and accumulates Successful/Failed results.
func processSendMessageBatchEntries(
	q *Queue,
	input *SendMessageBatchInput,
	preps []batchEntryPrep,
	now time.Time,
) *SendMessageBatchOutput {
	out := &SendMessageBatchOutput{}

	for i, entry := range input.Entries {
		p := preps[i]
		sendOut, err := sendMessageLocked(q, &SendMessageInput{
			QueueURL:                input.QueueURL,
			Region:                  input.Region,
			MessageBody:             entry.MessageBody,
			MessageGroupID:          entry.MessageGroupID,
			MessageDeduplicationID:  entry.MessageDeduplicationID,
			DelaySeconds:            entry.DelaySeconds,
			MessageAttributes:       entry.MessageAttributes,
			MessageSystemAttributes: entry.MessageSystemAttributes,
		}, p.md5Body, p.sha256Body, p.md5Attrs, p.md5SysAttrs, p.msgID, now)
		if err != nil {
			out.Failed = append(out.Failed, BatchResultErrorEntry{
				ID:          entry.ID,
				Code:        err.Error(),
				Message:     err.Error(),
				SenderFault: true,
			})

			continue
		}

		out.Successful = append(out.Successful, SendMessageBatchResultEntry{
			ID:                           entry.ID,
			MessageID:                    sendOut.MessageID,
			MD5OfBody:                    sendOut.MD5OfBody,
			MD5OfMessageAttributes:       sendOut.MD5OfMessageAttributes,
			MD5OfMessageSystemAttributes: sendOut.MD5OfMessageSystemAttributes,
			SequenceNumber:               sendOut.SequenceNumber,
		})
	}

	return out
}

// SendMessageBatch sends a batch of messages to the specified queue.
// Results in the Successful and Failed slices are returned in the same
// order as the corresponding entries in the input slice.
func (b *InMemoryBackend) SendMessageBatch(
	input *SendMessageBatchInput,
) (*SendMessageBatchOutput, error) {
	ids := make([]string, len(input.Entries))
	for i, e := range input.Entries {
		ids[i] = e.ID
	}

	if err := validateBatchEnvelope(ids); err != nil {
		return nil, err
	}

	// #55/#58: resolve queue once under global RLock, then hold per-queue lock
	// for the entire batch — eliminating N lock round-trips.
	b.mu.RLock("SendMessageBatch")
	q, queueExists := b.lookupQueueByName(input.Region, queueNameFromInput(input.QueueURL))
	b.mu.RUnlock()

	if !queueExists {
		return nil, ErrQueueNotFound
	}

	// AWS rejects the entire batch with BatchRequestTooLong when the combined
	// payload size of every entry that is itself within the per-message limit
	// (bodies plus attribute name + type + value bytes) would still exceed the
	// per-queue MaximumMessageSize (default 256 KiB). Entries that are
	// individually oversized are surfaced per-entry by validateMessageSize so
	// existing per-entry-failure semantics are preserved.
	totalBytes := 0
	allEntriesUnderLimit := true

	preps := make([]batchEntryPrep, len(input.Entries))

	for i, entry := range input.Entries {
		entryBytes := len(entry.MessageBody)
		for name, attr := range entry.MessageAttributes {
			entryBytes += len(
				name,
			) + len(
				attr.DataType,
			) + len(
				attr.StringValue,
			) + len(
				attr.BinaryValue,
			)
		}

		if entryBytes > defaultMaxMessageSize {
			allEntriesUnderLimit = false
		}

		totalBytes += entryBytes

		preps[i] = batchEntryPrep{
			md5Body:     computeBodyChecksumMD5(entry.MessageBody),
			sha256Body:  computeSHA256(entry.MessageBody),
			md5Attrs:    computeMD5OfMessageAttributes(entry.MessageAttributes),
			md5SysAttrs: computeMD5OfMessageAttributes(entry.MessageSystemAttributes),
			msgID:       uuid.New().String(),
		}
	}

	if allEntriesUnderLimit && totalBytes > defaultMaxMessageSize {
		return nil, ErrBatchRequestTooLong
	}

	now := time.Now()

	q.mu.Lock()
	defer q.mu.Unlock()

	// Process entries in input order; append results directly so Successful and
	// Failed slices already match the original entry order without sorting.
	out := processSendMessageBatchEntries(q, input, preps, now)

	go b.emitMetric("NumberOfMessagesSent", float64(len(out.Successful)))

	return out, nil
}

// DeleteMessageBatch deletes a batch of messages from the specified queue.
func (b *InMemoryBackend) DeleteMessageBatch(
	input *DeleteMessageBatchInput,
) (*DeleteMessageBatchOutput, error) {
	ids := make([]string, len(input.Entries))
	for i, e := range input.Entries {
		ids[i] = e.ID
	}

	if err := validateBatchEnvelope(ids); err != nil {
		return nil, err
	}

	// AWS returns QueueDoesNotExist at the batch level (not per-entry) when the
	// target queue does not exist.
	var queueExists bool

	func() {
		b.mu.RLock("DeleteMessageBatch.queueCheck")
		defer b.mu.RUnlock()

		_, queueExists = b.lookupQueueByName(input.Region, queueNameFromInput(input.QueueURL))
	}()

	if !queueExists {
		return nil, ErrQueueNotFound
	}

	out := &DeleteMessageBatchOutput{}

	for _, entry := range input.Entries {
		err := b.DeleteMessage(&DeleteMessageInput{
			QueueURL:      input.QueueURL,
			ReceiptHandle: entry.ReceiptHandle,
		})
		if err != nil {
			out.Failed = append(out.Failed, BatchResultErrorEntry{
				ID:          entry.ID,
				Code:        err.Error(),
				Message:     err.Error(),
				SenderFault: true,
			})

			continue
		}

		out.Successful = append(out.Successful, DeleteMessageBatchResultEntry{ID: entry.ID})
	}

	return out, nil
}

// ReceiveMessagesLocal is an internal method used by the ESM poller to pull
// messages from a queue without long-polling. It returns up to maxMessages
// visible messages, moving them to in-flight state using the queue's default
// visibility timeout.
func (b *InMemoryBackend) ReceiveMessagesLocal(
	queueURL string,
	maxMessages int,
) ([]*Message, error) {
	out, err := b.ReceiveMessage(&ReceiveMessageInput{
		QueueURL:            queueURL,
		MaxNumberOfMessages: maxMessages,
		WaitTimeSeconds:     0,
		VisibilityTimeout:   NoVisibilityTimeout,
	})
	if err != nil {
		return nil, err
	}

	return out.Messages, nil
}

// DeleteMessagesLocal is an internal method used by the ESM poller to delete
// successfully processed messages by their receipt handles.
func (b *InMemoryBackend) DeleteMessagesLocal(queueURL string, receiptHandles []string) error {
	for _, rh := range receiptHandles {
		if err := b.DeleteMessage(&DeleteMessageInput{
			QueueURL:      queueURL,
			ReceiptHandle: rh,
		}); err != nil {
			return err
		}
	}

	return nil
}
