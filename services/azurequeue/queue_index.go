package azurequeue

import (
	"sort"
	"time"
)

// queueIndex is derived, never-persisted lookup state for a storedQueue.
type queueIndex struct {
	byID    map[string]*storedMessage
	hintAt  time.Time
	hintTil time.Time
	nextSeq uint64
	hintEnd int
	dupIDs  bool
}

func (q *storedQueue) resetIndex() {
	q.idx = queueIndex{}
}

// reindex rebuilds the derived index from Messages (insertion order).
func (q *storedQueue) reindex() {
	q.idx = queueIndex{byID: make(map[string]*storedMessage, len(q.Messages))}

	for _, msg := range q.Messages {
		q.indexMessage(msg)
	}
}

func (q *storedQueue) indexMessage(msg *storedMessage) {
	q.idx.nextSeq++
	msg.seq = q.idx.nextSeq

	if q.idx.byID == nil {
		q.idx.byID = make(map[string]*storedMessage)
	}

	if _, dup := q.idx.byID[msg.ID]; dup {
		q.idx.dupIDs = true

		return
	}

	q.idx.byID[msg.ID] = msg
}

func (q *storedQueue) add(msg *storedMessage) {
	q.Messages = append(q.Messages, msg)
	q.indexMessage(msg)
}

// find resolves a non-expired message by ID, returning its slice position.
func (q *storedQueue) find(messageID string, now time.Time) (int, *storedMessage, bool) {
	if !q.idx.dupIDs {
		msg := q.idx.byID[messageID]
		if msg == nil || msg.isExpired(now) {
			return 0, nil, false
		}

		i := sort.Search(len(q.Messages), func(i int) bool { return q.Messages[i].seq >= msg.seq })
		if i < len(q.Messages) && q.Messages[i] == msg {
			return i, msg, true
		}
	}

	for i, msg := range q.Messages {
		if msg.ID == messageID && !msg.isExpired(now) {
			return i, msg, true
		}
	}

	return 0, nil, false
}

func (q *storedQueue) removeAt(i int) {
	msg := q.Messages[i]
	last := len(q.Messages) - 1
	copy(q.Messages[i:], q.Messages[i+1:])
	q.Messages[last] = nil
	q.Messages = q.Messages[:last]

	if cur := q.idx.byID[msg.ID]; cur == msg {
		delete(q.idx.byID, msg.ID)
	}

	if i < q.idx.hintEnd {
		q.idx.hintEnd--
	}
}

// scanStart returns the position before which every message is known to be
// invisible or expired at now.
func (q *storedQueue) scanStart(now time.Time) int {
	h := &q.idx
	if h.hintEnd > 0 && h.hintEnd <= len(q.Messages) && !now.Before(h.hintAt) && now.Before(h.hintTil) {
		return h.hintEnd
	}

	return 0
}

// noteUpdated keeps the scan hint sound after msg at position i changed its
// visibility.
func (q *storedQueue) noteUpdated(i int, msg *storedMessage) {
	if i < q.idx.hintEnd && msg.NextVisibleTime.Before(q.idx.hintTil) {
		q.idx.hintTil = msg.NextVisibleTime
	}
}

// hintScan tracks the earliest time a skipped prefix could become visible.
type hintScan struct {
	until time.Time
	has   bool
}

func (h *hintScan) note(t time.Time) {
	if !h.has || t.Before(h.until) {
		h.until, h.has = t, true
	}
}

func (h *hintScan) bound() time.Time {
	if !h.has {
		return neverVisible()
	}

	return h.until
}

func neverVisible() time.Time {
	return time.Date(9999, time.December, 31, 0, 0, 0, 0, time.UTC)
}

func (q *storedQueue) setHint(end int, at time.Time, h hintScan) {
	q.idx.hintEnd, q.idx.hintAt, q.idx.hintTil = end, at, h.bound()
}

// beginScan returns the scan start position and a hintScan seeded from the
// reused prefix.
func (q *storedQueue) beginScan(now time.Time) (int, hintScan) {
	start := q.scanStart(now)
	if start == 0 {
		return 0, hintScan{}
	}

	return start, hintScan{until: q.idx.hintTil, has: true}
}
