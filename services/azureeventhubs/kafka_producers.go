package azureeventhubs

import (
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const maxTrackedProducers = 10000

type producerKey struct {
	ref HubRef
	pid int32
	id  int64
}

type producerState struct {
	lastBase  int64
	lastSeq   int32
	lastCount int32
	epoch     int16
	hasBatch  bool
}

// producerTable backs idempotent producers: it hands out producer IDs and rejects out-of-order or
// stale-epoch batches, answering a retried batch with its original offset.
type producerTable struct {
	mu     *lockmetrics.RWMutex
	epochs map[int64]int16
	state  map[producerKey]*producerState
	next   int64
}

func newProducerTable() *producerTable {
	return &producerTable{
		mu: lockmetrics.New("azureeventhubs.kafka.producers"), epochs: map[int64]int16{},
		state: map[producerKey]*producerState{}, next: 1,
	}
}

// allocate returns a producer ID and epoch: a new ID, or the next epoch of a known one.
func (t *producerTable) allocate(id int64, epoch int16) (int64, int16) {
	t.mu.Lock("allocate")
	defer t.mu.Unlock()

	if cur, ok := t.epochs[id]; ok && id >= 0 && epoch == cur {
		t.epochs[id] = cur + 1

		return id, cur + 1
	}

	if len(t.epochs) >= maxTrackedProducers {
		t.epochs = map[int64]int16{}
		t.state = map[producerKey]*producerState{}
	}

	id = t.next
	t.next++
	t.epochs[id] = 0

	return id, 0
}

type producerToken struct {
	key   producerKey
	epoch int16
	seq   int32
	count int32
	valid bool
}

// begin checks a batch. It returns an error code, or a non-negative offset when the batch repeats the last one.
func (t *producerTable) begin(ref HubRef, pid int32, b producedBatch) (producerToken, int16, int64) {
	if b.producerID < 0 {
		return producerToken{}, kerrNone, -1
	}

	t.mu.Lock("begin")
	defer t.mu.Unlock()

	known, ok := t.epochs[b.producerID]
	if !ok {
		return producerToken{}, kerrUnknownProducerID, -1
	}

	if b.producerEpoch < known {
		return producerToken{}, kerrInvalidProducerEpoch, -1
	}

	key := producerKey{ref: ref, pid: pid, id: b.producerID}
	tok := producerToken{
		key: key, epoch: b.producerEpoch, seq: b.firstSeq, valid: true,
		count: int32(len(b.events)), //nolint:gosec // bounded by the records per batch cap
	}

	st := t.state[key]
	if st == nil || st.epoch != b.producerEpoch {
		if b.firstSeq != 0 {
			return producerToken{}, kerrOutOfOrderSequence, -1
		}

		return tok, kerrNone, -1
	}

	switch {
	case st.hasBatch && b.firstSeq == st.lastSeq && tok.count == st.lastCount:
		return tok, kerrNone, st.lastBase
	case b.firstSeq == st.lastSeq+st.lastCount:
		return tok, kerrNone, -1
	}

	return producerToken{}, kerrOutOfOrderSequence, -1
}

func (t *producerTable) commit(tok producerToken, base int64) {
	if !tok.valid {
		return
	}

	t.mu.Lock("commit")
	defer t.mu.Unlock()

	t.state[tok.key] = &producerState{
		epoch: tok.epoch, lastSeq: tok.seq, lastCount: tok.count, lastBase: base, hasBatch: true,
	}
}
