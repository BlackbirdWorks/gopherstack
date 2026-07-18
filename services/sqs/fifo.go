package sqs

import "time"

// checkFIFOPerGroupRateLimit enforces the 300 TPS per-message-group AWS limit
// for FIFO queues running with FifoThroughputLimit=perMessageGroupId.
//
// Maintains a sliding 1-second window per group, pruning timestamps older
// than the window on each call. Returns ErrOverLimit when the window is
// already full; otherwise appends the new send and returns nil.
//
// Caller must hold b.mu (write). Allocates the per-queue map lazily.
func checkFIFOPerGroupRateLimit(q *Queue, group string, now time.Time) error {
	if group == "" {
		return nil
	}

	if q.fifoSendTimes == nil {
		q.fifoSendTimes = make(map[string][]time.Time)
	}

	cutoff := now.Add(-time.Second)
	prev := q.fifoSendTimes[group]
	kept := prev[:0]
	for _, t := range prev {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}

	if len(kept) >= fifoPerGroupTPS {
		q.fifoSendTimes[group] = kept

		return ErrOverLimit
	}

	q.fifoSendTimes[group] = append(kept, now)

	return nil
}

// fifoPreflight is the outcome of preflightFIFOSend. handled=true means the
// caller should return Output/Err immediately; handled=false means proceed
// with normal send flow.
type fifoPreflight struct {
	Output  *SendMessageOutput
	Err     error
	Handled bool
}

// preflightFIFOSend runs the FIFO-only preconditions a SendMessage must
// satisfy before the message is constructed: parameter validation, per-group
// throughput limiting, and content-based deduplication.
//
// Caller must already hold b.mu (write).
func preflightFIFOSend(
	q *Queue,
	input *SendMessageInput,
	md5Body, sha256Body string,
	now time.Time,
) fifoPreflight {
	if err := validateFIFOParams(input, q); err != nil {
		return fifoPreflight{Err: err, Handled: true}
	}

	if q.Attributes[attrFifoThroughputLimit] == fifoThroughputLimitPerMessageGroupID {
		if err := checkFIFOPerGroupRateLimit(q, input.MessageGroupID, now); err != nil {
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
