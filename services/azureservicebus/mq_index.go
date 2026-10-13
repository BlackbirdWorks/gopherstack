package azureservicebus

import (
	"sort"
	"time"
)

// mqIndex is derived, never-persisted lookup state for a messageQueue's live
// list: an ID index plus a scan hint (the live-list prefix known to be
// locked or expired until hintTil).
type mqIndex struct {
	byID    map[string]*storedMessage
	hintAt  time.Time
	hintTil time.Time
	hintEnd int
	dup     bool
}

// reindexLive rebuilds the derived index from mq.Messages.
func (mq *messageQueue) reindexLive() {
	mq.idx = mqIndex{byID: make(map[string]*storedMessage, len(mq.Messages))}

	for _, msg := range mq.Messages {
		mq.indexLive(msg)
	}
}

func (mq *messageQueue) indexLive(msg *storedMessage) {
	if mq.idx.byID == nil {
		mq.idx.byID = make(map[string]*storedMessage)
	}

	if _, dup := mq.idx.byID[msg.MessageID]; dup {
		mq.idx.dup = true

		return
	}

	mq.idx.byID[msg.MessageID] = msg
}

func (mq *messageQueue) appendLive(msg *storedMessage) {
	mq.Messages = append(mq.Messages, msg)
	mq.indexLive(msg)
}

// findLocked resolves a currently-locked message by ID; semantics match
// findLockedMessageLocked.
func (mq *messageQueue) findLocked(deadLetter bool, messageID string, now time.Time) (int, *storedMessage, error) {
	if !deadLetter && !mq.idx.dup {
		if msg := mq.idx.byID[messageID]; msg != nil {
			i := sort.Search(len(mq.Messages), func(i int) bool {
				return mq.Messages[i].SequenceNumber >= msg.SequenceNumber
			})

			if i < len(mq.Messages) && mq.Messages[i] == msg {
				switch {
				case msg.isExpired(now):
					return 0, nil, ErrMessageNotFound
				case !msg.isLocked(now):
					return 0, nil, ErrMessageNotLocked
				default:
					return i, msg, nil
				}
			}
		}
	}

	return findLockedMessageLocked(mq.liveList(deadLetter), messageID, now)
}

// scanStart returns the live-list position before which every message is
// known to be locked or expired at now.
func (mq *messageQueue) scanStart(now time.Time) int {
	h := &mq.idx
	if h.hintEnd > 0 && h.hintEnd <= len(mq.Messages) && !now.Before(h.hintAt) && now.Before(h.hintTil) {
		return h.hintEnd
	}

	return 0
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
		return time.Date(9999, time.December, 31, 0, 0, 0, 0, time.UTC)
	}

	return h.until
}

func (mq *messageQueue) setHint(end int, at time.Time, h hintScan) {
	mq.idx.hintEnd, mq.idx.hintAt, mq.idx.hintTil = end, at, h.bound()
}

// beginScan returns the live-list scan start and a hintScan seeded from the
// reused prefix; dead-letter scans never use the hint.
func (mq *messageQueue) beginScan(now time.Time, deadLetter bool) (int, hintScan) {
	if deadLetter {
		return 0, hintScan{}
	}

	start := mq.scanStart(now)
	if start == 0 {
		return 0, hintScan{}
	}

	return start, hintScan{until: mq.idx.hintTil, has: true}
}
