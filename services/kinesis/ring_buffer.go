package kinesis

import (
	"encoding/json"
	"sort"
	"time"
)

// shardRecords is a bounded ring buffer for Kinesis shard records.
//
// Backing store is lazily allocated on first push, so empty shards have
// zero heap cost. Once allocated it is never grown beyond maxRecordsPerShard,
// giving O(1) push with no further allocations.
//
// The zero value is ready to use and serialises as `[]` (not `null`).
type shardRecords struct {
	buf  []*Record // lazily allocated; len(buf) == maxRecordsPerShard once created
	head int       // logical index 0 maps to buf[head]
	n    int       // number of valid elements (0 ≤ n ≤ maxRecordsPerShard)
}

// push appends rec to the ring buffer.
// If the buffer is already at capacity the oldest record is silently evicted.
func (r *shardRecords) push(rec *Record) {
	if r.buf == nil {
		r.buf = make([]*Record, maxRecordsPerShard)
	}

	if r.n < maxRecordsPerShard {
		r.buf[(r.head+r.n)%maxRecordsPerShard] = rec
		r.n++
	} else {
		// Full: overwrite the oldest slot and advance head.
		r.buf[r.head] = rec
		r.head = (r.head + 1) % maxRecordsPerShard
	}
}

// len returns the number of records currently held in the buffer.
func (r *shardRecords) len() int { return r.n }

// at returns the ith logical record (0 = oldest).
func (r *shardRecords) at(i int) *Record {
	return r.buf[(r.head+i)%maxRecordsPerShard]
}

// last returns the most recently pushed record, or nil if the buffer is empty.
func (r *shardRecords) last() *Record {
	if r.n == 0 {
		return nil
	}

	return r.at(r.n - 1)
}

// trimBefore removes all records whose ApproximateArrivalTimestamp is before
// cutoff. Because records are always appended in chronological order, trimming
// is a simple advance of the head pointer.
func (r *shardRecords) trimBefore(cutoff time.Time) int {
	trimmed := 0

	for r.n > 0 && r.at(0).ApproximateArrivalTimestamp.Before(cutoff) {
		r.buf[r.head] = nil // release the pointer for GC
		r.head = (r.head + 1) % maxRecordsPerShard
		r.n--
		trimmed++
	}

	return trimmed
}

// MarshalJSON serialises the ring buffer as an ordered JSON array.
// Always emits [] (not null) when empty to keep the persistence format stable.
func (r *shardRecords) MarshalJSON() ([]byte, error) {
	if r.n == 0 {
		return []byte("[]"), nil
	}

	// Materialise in logical order without allocating an intermediate slice.
	out := make([]*Record, r.n)
	for i := range r.n {
		out[i] = r.at(i)
	}

	return json.Marshal(out)
}

// UnmarshalJSON restores the ring buffer from a JSON array.
// After restore head = 0 and records occupy buf[0..n-1].
// Any previously held pointers are cleared to allow GC.
func (r *shardRecords) UnmarshalJSON(data []byte) error {
	var records []*Record
	if err := json.Unmarshal(data, &records); err != nil {
		return err
	}

	// Clear any stale pointers from a previous incarnation.
	if r.buf != nil {
		for i := range r.buf {
			r.buf[i] = nil
		}
	}

	r.head = 0
	r.n = 0

	for _, rec := range records {
		r.push(rec)
	}

	return nil
}

// findSequencePosition returns the record index for the given sequence number
// using binary search (O(log n)).
// Records within a shard are always appended in ascending sequence-number
// order, making them safe for binary search.
//
// If after is false the index of the first record whose sequence number is
// ≥ seqNum is returned.  If after is true and an exact match exists, the
// index after the match is returned; otherwise the same index as after=false.
func findSequencePosition(records *shardRecords, seqNum string, after bool) int {
	n := records.len()

	// sort.Search returns the smallest i in [0, n) for which f(i) is true.
	pos := sort.Search(n, func(i int) bool {
		return records.at(i).SequenceNumber >= seqNum
	})

	if !after {
		return pos
	}

	// after=true: if there is an exact match, step one past it.
	if pos < n && records.at(pos).SequenceNumber == seqNum {
		return pos + 1
	}

	return pos
}

// findTimestampPosition returns the index of the first record whose
// ApproximateArrivalTimestamp is not before ts (binary search, O(log n)).
// Returns records.len() if all records precede ts.
func findTimestampPosition(records *shardRecords, ts time.Time) int {
	n := records.len()

	return sort.Search(n, func(i int) bool {
		return !records.at(i).ApproximateArrivalTimestamp.Before(ts)
	})
}
