package ocistreaming

import (
	"errors"
	"sort"
	"strconv"
)

// PartitionRange is the readable offset window [Base, End) of one partition.
type PartitionRange struct {
	Partition string `json:"partition"`
	Base      int64  `json:"base"`
	End       int64  `json:"end"`
}

// Compartments lists the distinct compartments that hold pools or streams.
func (b *InMemoryBackend) Compartments() []string {
	b.mu.RLock("Compartments")
	defer b.mu.RUnlock()

	seen := map[string]bool{}

	for _, p := range b.pools {
		seen[p.CompartmentID] = true
	}

	for _, s := range b.streams {
		seen[s.CompartmentID] = true
	}

	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}

	sort.Strings(out)

	return out
}

// PartitionRanges reports every partition's offset window for a live stream.
func (b *InMemoryBackend) PartitionRanges(streamID string) ([]PartitionRange, error) {
	b.mu.RLock("PartitionRanges")
	defer b.mu.RUnlock()

	s, aerr := b.liveStream(streamID)
	if aerr != nil {
		return nil, aerr
	}

	out := make([]PartitionRange, 0, len(s.Parts))
	for i, p := range s.Parts {
		out = append(out, PartitionRange{
			Partition: strconv.Itoa(i), Base: p.Base, End: p.Base + int64(len(p.Messages)),
		})
	}

	return out, nil
}

// ErrAppendFailed is returned when AppendMessage's single message is rejected by the partition.
var ErrAppendFailed = errors.New("ocistreaming: message rejected")

// AppendMessage puts one message on a live stream, as PutMessages would; key may be empty.
func (b *InMemoryBackend) AppendMessage(streamID string, key, value []byte) error {
	res, err := b.PutMessages(streamID, putMessagesIn{Messages: []putEntryIn{{Key: key, Value: value}}})
	if err != nil {
		return err
	}

	if res.Failures > 0 {
		return ErrAppendFailed
	}

	return nil
}

// StreamActive reports whether id is an ACTIVE stream.
func (b *InMemoryBackend) StreamActive(id string) bool {
	b.mu.RLock("StreamActive")
	defer b.mu.RUnlock()

	_, aerr := b.activeStream(id)

	return aerr == nil
}
