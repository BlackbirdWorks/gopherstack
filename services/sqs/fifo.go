package sqs

import "time"

// fifoAPIMethod is one of the three FIFO API actions AWS meters
// independently: SendMessage, ReceiveMessage, DeleteMessage.
type fifoAPIMethod int

const (
	fifoMethodSend fifoAPIMethod = iota
	fifoMethodReceive
	fifoMethodDelete
)

// fifoThroughputKey is one independent budget: an API method plus a scope
// ("" for queue-wide, or a MessageGroupId under perMessageGroupId).
type fifoThroughputKey struct {
	scopeKey string
	method   fifoAPIMethod
}

// fifoRateWindow is a 1-second sliding window of call and message timestamps
// for one fifoThroughputKey (AWS: 300 calls/sec, 3,000 messages/sec batched).
type fifoRateWindow struct {
	calls    []time.Time
	messages []time.Time
}

const (
	fifoCallsPerSecond    = 300
	fifoMessagesPerSecond = 3000
)

// fifoThroughputScopeKey returns groupID under perMessageGroupId, else "" for
// the AWS-default queue-wide scope (unset attribute included).
func fifoThroughputScopeKey(q *Queue, groupID string) string {
	if q.Attributes[attrFifoThroughputLimit] == fifoThroughputLimitPerMessageGroupID {
		return groupID
	}

	return ""
}

// pruneRateWindow drops timestamps at or before cutoff, reusing times'
// backing array.
func pruneRateWindow(times []time.Time, cutoff time.Time) []time.Time {
	kept := times[:0]
	for _, t := range times {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}

	return kept
}

// checkFIFOThroughput consumes one call slot + msgCount message slots for
// (method, scopeKey), or returns ErrRequestThrottled leaving both unchanged.
// Caller must hold q.mu; now must be b.now(), not time.Now(), for determinism.
func checkFIFOThroughput(q *Queue, method fifoAPIMethod, scopeKey string, msgCount int, now time.Time) error {
	if q.fifoThroughput == nil {
		q.fifoThroughput = make(map[fifoThroughputKey]*fifoRateWindow)
	}

	key := fifoThroughputKey{method: method, scopeKey: scopeKey}

	w := q.fifoThroughput[key]
	if w == nil {
		w = &fifoRateWindow{}
		q.fifoThroughput[key] = w
	}

	cutoff := now.Add(-time.Second)
	w.calls = pruneRateWindow(w.calls, cutoff)
	w.messages = pruneRateWindow(w.messages, cutoff)

	if len(w.calls) >= fifoCallsPerSecond || len(w.messages)+msgCount > fifoMessagesPerSecond {
		return ErrRequestThrottled
	}

	w.calls = append(w.calls, now)
	for range msgCount {
		w.messages = append(w.messages, now)
	}

	return nil
}

// fifoThroughputPairingValid reports whether the effective FifoThroughputLimit/
// DeduplicationScope combination — incoming attributes overlaid on existing
// queue state — is legal. AWS: "The perMessageGroupId value is allowed only
// when the value for DeduplicationScope is messageGroup" (aws-sdk-go-v2/
// service/sqs@v1.46.4 api_op_SetQueueAttributes.go:179-180). existing is nil
// for CreateQueue, which has no prior state to merge.
func fifoThroughputPairingValid(existing, incoming map[string]string) bool {
	limit, ok := incoming[attrFifoThroughputLimit]
	if !ok {
		limit = existing[attrFifoThroughputLimit]
	}

	if limit != fifoThroughputLimitPerMessageGroupID {
		return true
	}

	scope, ok := incoming[attrDeduplicationScope]
	if !ok {
		scope = existing[attrDeduplicationScope]
	}

	// Unset scope defaults to messageGroup (see dedupKey), which is compatible.
	return scope == "" || scope == fifoDedupScopePerMessageGroup
}

// fifoPreflight is the outcome of preflightFIFOSend. handled=true means the
// caller should return Output/Err immediately; handled=false means proceed
// with normal send flow.
type fifoPreflight struct {
	Output  *SendMessageOutput
	Err     error
	Handled bool
}

// preflightFIFOSend validates FIFO params, throughput-limits, then dedups.
// checkThroughput is false when the batch caller already reserved the
// budget (computeFIFOSendThrottling). Caller must hold q.mu.
func preflightFIFOSend(
	q *Queue,
	input *SendMessageInput,
	md5Body, sha256Body string,
	checkThroughput bool,
	now time.Time,
) fifoPreflight {
	if err := validateFIFOParams(input, q); err != nil {
		return fifoPreflight{Err: err, Handled: true}
	}

	if checkThroughput {
		scopeKey := fifoThroughputScopeKey(q, input.MessageGroupID)
		if err := checkFIFOThroughput(q, fifoMethodSend, scopeKey, 1, now); err != nil {
			return fifoPreflight{Err: err, Handled: true}
		}
	}

	if out, dup := checkDedup(
		q,
		input.MessageGroupID,
		input.MessageDeduplicationID,
		md5Body,
		sha256Body,
		q.Attributes[attrContentBasedDeduplication],
		now,
	); dup {
		return fifoPreflight{Output: out, Handled: true}
	}

	return fifoPreflight{}
}

// validateFIFOParams validates FIFO-specific parameters for a SendMessage request.
// AWS requires MessageGroupID for all FIFO sends, and MessageDeduplicationID when
// ContentBasedDeduplication is disabled on the queue. FIFO queues do not support
// per-message delays; a non-zero DelaySeconds is rejected.
func validateFIFOParams(input *SendMessageInput, q *Queue) error {
	if input.MessageGroupID == "" {
		return ErrMissingMessageGroupID
	}

	if input.DelaySeconds > 0 {
		return ErrFIFODelayNotSupported
	}

	contentBasedDedup := q.Attributes[attrContentBasedDeduplication]
	if contentBasedDedup != attrValTrue && input.MessageDeduplicationID == "" {
		return ErrMissingDeduplicationID
	}

	return nil
}

// computeFIFOSendThrottling groups entries by throughput scope, consuming each
// scope's budget once per group (not per entry). Caller must hold q.mu.
func computeFIFOSendThrottling(q *Queue, entries []SendMessageBatchEntry, now time.Time) []bool {
	groups := make(map[string][]int)

	for i, entry := range entries {
		params := &SendMessageInput{
			MessageGroupID:         entry.MessageGroupID,
			MessageDeduplicationID: entry.MessageDeduplicationID,
			DelaySeconds:           entry.DelaySeconds,
		}
		if validateFIFOParams(params, q) != nil {
			continue
		}

		key := fifoThroughputScopeKey(q, entry.MessageGroupID)
		groups[key] = append(groups[key], i)
	}

	throttled := make([]bool, len(entries))

	for key, idxs := range groups {
		if checkFIFOThroughput(q, fifoMethodSend, key, len(idxs), now) != nil {
			for _, i := range idxs {
				throttled[i] = true
			}
		}
	}

	return throttled
}

// computeFIFODeleteThrottling is DeleteMessageBatch's analog of
// computeFIFOSendThrottling, scoping by each handle's in-flight MessageGroupId.
func computeFIFODeleteThrottling(q *Queue, entries []DeleteMessageBatchEntry, now time.Time) []bool {
	groups := make(map[string][]int)

	for i, entry := range entries {
		inf, found := q.inFlightByHandle[entry.ReceiptHandle]
		if !found {
			continue
		}

		key := fifoThroughputScopeKey(q, inf.Msg.MessageGroupID)
		groups[key] = append(groups[key], i)
	}

	throttled := make([]bool, len(entries))

	for key, idxs := range groups {
		if checkFIFOThroughput(q, fifoMethodDelete, key, len(idxs), now) != nil {
			for _, i := range idxs {
				throttled[i] = true
			}
		}
	}

	return throttled
}

// dedupKey returns the deduplication map key, respecting the queue's
// DeduplicationScope attribute. When scope is "queue" (queue-wide), only the
// effective dedup ID is used as the key. The default scope is "messageGroup",
// where the key is scoped per group to allow identical messages in different
// groups within the same 5-minute window.
func dedupKey(q *Queue, groupID, effectiveID string) string {
	if q.Attributes[attrDeduplicationScope] == fifoDedupScopeQueue {
		return effectiveID
	}

	// Default: messageGroup scope — key by group + dedupID.
	return groupID + "|" + effectiveID
}

// checkDedup checks for a duplicate FIFO message and returns the original output if found.
// now is the reference time used for window expiry comparison.
// md5Body is the MD5 hash of the body (for the wire-protocol MD5OfBody response field).
// bodyHash is the SHA-256 hash of the message body, used as the dedup key when
// ContentBasedDeduplication is enabled (AWS spec uses SHA-256, not MD5).
func checkDedup(
	q *Queue,
	groupID, dedupID, md5Body, bodyHash, contentBasedDedup string,
	now time.Time,
) (*SendMessageOutput, bool) {
	effectiveID := dedupID
	if effectiveID == "" && contentBasedDedup == attrValTrue {
		effectiveID = bodyHash
	}

	if effectiveID == "" {
		return nil, false
	}

	key := dedupKey(q, groupID, effectiveID)

	expiry, found := q.DeduplicationIDs[key]
	if !found {
		return nil, false
	}

	if !now.Before(expiry) {
		// Eagerly remove the expired entry inline. This keeps the deduplication map
		// lean without waiting for the next janitor sweep, reducing memory pressure
		// and speeding up subsequent lookups.
		delete(q.DeduplicationIDs, key)
		delete(q.deduplicationMsgIDs, key)

		return nil, false
	}

	origMsgID := q.deduplicationMsgIDs[key]

	return &SendMessageOutput{MessageID: origMsgID, MD5OfBody: md5Body}, true
}

// maxDedupEntriesPerQueue caps the per-queue deduplication maps to bound memory
// in the absence of a janitor sweep. AWS keeps the dedup window at 5 minutes and
// limits transactions per second well below this cap; once exceeded the oldest
// entries (by expiry) are evicted to make room.
const maxDedupEntriesPerQueue = 100_000

// storeDedup records a deduplication entry for a FIFO message. When the dedup
// map is at capacity, the entries closest to expiry are evicted first.
// bodyHash is the SHA-256 hash of the message body, used when ContentBasedDeduplication
// is enabled (AWS spec uses SHA-256, not MD5, for content-based dedup IDs).
func storeDedup(
	q *Queue,
	groupID, dedupID, bodyHash, contentBasedDedup, msgID string,
	now time.Time,
) {
	effectiveID := dedupID
	if effectiveID == "" && contentBasedDedup == attrValTrue {
		effectiveID = bodyHash
	}

	if effectiveID == "" {
		return
	}

	if len(q.DeduplicationIDs) >= maxDedupEntriesPerQueue {
		evictOldestDedup(q, len(q.DeduplicationIDs)-maxDedupEntriesPerQueue+1)
	}

	key := dedupKey(q, groupID, effectiveID)
	q.DeduplicationIDs[key] = now.Add(deduplicationWindowSecs * time.Second)
	q.deduplicationMsgIDs[key] = msgID
}

// evictOldestDedup removes up to n entries with the earliest expiry times.
// Linear scan is acceptable given maxDedupEntriesPerQueue is small enough that
// hitting the cap is the cold path (janitor normally prunes first).
func evictOldestDedup(q *Queue, n int) {
	for ; n > 0 && len(q.DeduplicationIDs) > 0; n-- {
		var oldestKey string
		var oldestExpiry time.Time
		first := true
		for k, exp := range q.DeduplicationIDs {
			if first || exp.Before(oldestExpiry) {
				oldestKey = k
				oldestExpiry = exp
				first = false
			}
		}
		delete(q.DeduplicationIDs, oldestKey)
		delete(q.deduplicationMsgIDs, oldestKey)
	}
}

// pruneDedup removes expired deduplication entries from a FIFO queue.
func pruneDedup(q *Queue, now time.Time) {
	for k, expiry := range q.DeduplicationIDs {
		if !now.Before(expiry) {
			delete(q.DeduplicationIDs, k)
			delete(q.deduplicationMsgIDs, k)
		}
	}
}
