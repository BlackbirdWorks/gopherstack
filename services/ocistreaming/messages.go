package ocistreaming

import (
	"strconv"
	"time"
)

const (
	maxMessageBytes   = 1 << 20
	maxRequestBytes   = 1 << 20
	maxKeyBytes       = 256
	maxPartitionBytes = 1 << 20
	maxGetLimit       = 10000
)

type putEntryIn struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type putMessagesIn struct {
	Messages []putEntryIn `json:"messages"`
}

type putEntryWire struct {
	Partition    *string    `json:"partition,omitempty"`
	Offset       *int64     `json:"offset,omitempty"`
	Timestamp    *time.Time `json:"timestamp,omitempty"`
	Error        *string    `json:"error,omitempty"`
	ErrorMessage *string    `json:"errorMessage,omitempty"`
}

type putResultWire struct {
	Entries  []putEntryWire `json:"entries"`
	Failures int            `json:"failures"`
}

type messageWire struct {
	Timestamp time.Time `json:"timestamp"`
	Stream    string    `json:"stream"`
	Partition string    `json:"partition"`
	Key       []byte    `json:"key"`
	Value     []byte    `json:"value"`
	Offset    int64     `json:"offset"`
}

// murmur2 is the Kafka default partitioner hash.
func murmur2(data []byte) uint32 {
	const (
		m    = 0x5bd1e995
		r    = 24
		seed = 0x9747b28c
	)

	n := len(data)
	h := uint32(seed) ^ uint32(n) //nolint:gosec // length bounded by the 256 byte key limit

	for i := 0; i+4 <= n; i += 4 {
		k := uint32(data[i]) | uint32(data[i+1])<<8 | uint32(data[i+2])<<16 | uint32(data[i+3])<<24
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}

	tail := data[n&^3:]
	switch len(tail) {
	case 3:
		h ^= uint32(tail[2]) << 16

		fallthrough
	case 2:
		h ^= uint32(tail[1]) << 8

		fallthrough
	case 1:
		h ^= uint32(tail[0])
		h *= m
	}

	h ^= h >> 13
	h *= m
	h ^= h >> 15

	return h
}

func (s *Stream) partitionFor(key []byte) int {
	if len(key) == 0 {
		s.RoundRobin++

		return (s.RoundRobin - 1) % len(s.Parts)
	}

	return int((murmur2(key) & 0x7fffffff) % uint32(len(s.Parts))) //nolint:gosec // bounded partition count
}

func (p *Partition) end() int64 { return p.Base + int64(len(p.Messages)) }

// PutMessages appends messages, choosing each partition by key hash (round-robin without a key).
func (b *InMemoryBackend) PutMessages(streamID string, in putMessagesIn) (putResultWire, error) {
	if len(in.Messages) == 0 {
		return putResultWire{}, errMissing("messages is required")
	}

	total := 0

	for _, m := range in.Messages {
		if m.Value == nil {
			return putResultWire{}, errMissing("every message requires a value")
		}

		total += len(m.Value) + len(m.Key)
	}

	if total > maxRequestBytes {
		return putResultWire{}, errTooLarge("The request exceeds the maximum size of 1 MiB")
	}

	b.mu.Lock("PutMessages")
	defer b.mu.Unlock()

	s, aerr := b.activeStream(streamID)
	if aerr != nil {
		return putResultWire{}, aerr
	}

	now := b.nowUTC()
	targets := make([]int, len(in.Messages))
	bytesPer := map[int]int{}

	for i, m := range in.Messages {
		if len(m.Key) > maxKeyBytes {
			targets[i] = -1

			continue
		}

		targets[i] = s.partitionFor(m.Key)
		bytesPer[targets[i]] += len(m.Value) + len(m.Key)
	}

	for p, n := range bytesPer {
		if s.Parts[p].used(now)+n > maxPartitionBytes {
			return putResultWire{}, errThrottled("Partition " + strconv.Itoa(p) + " exceeded the 1 MiB/s ingest limit")
		}
	}

	return appendEntries(s, in.Messages, targets, now), nil
}

func (p *Partition) used(now time.Time) int {
	if p.WindowSec != now.Unix() {
		return 0
	}

	return p.WindowBytes
}

func appendEntries(s *Stream, msgs []putEntryIn, targets []int, now time.Time) putResultWire {
	out := putResultWire{Entries: make([]putEntryWire, len(msgs))}

	for i, m := range msgs {
		if targets[i] < 0 {
			out.Failures++
			out.Entries[i] = putEntryWire{
				Error:        new("InvalidParameter"),
				ErrorMessage: new("The message key must be at most 256 bytes"),
			}

			continue
		}

		part := s.Parts[targets[i]]
		if part.WindowSec != now.Unix() {
			part.WindowSec, part.WindowBytes = now.Unix(), 0
		}

		part.WindowBytes += len(m.Value) + len(m.Key)
		off := part.end()
		part.Messages = append(part.Messages, Message{Time: now, Key: m.Key, Value: m.Value})
		out.Entries[i] = putEntryWire{Partition: new(strconv.Itoa(targets[i])), Offset: &off, Timestamp: new(now)}
	}

	return out
}

func (s *Stream) read(part int, from int64, limit int) []messageWire {
	p := s.Parts[part]
	from = max(from, p.Base)

	var out []messageWire

	for off := from; off < p.end() && len(out) < limit; off++ {
		m := p.Messages[off-p.Base]
		out = append(out, messageWire{
			Stream: s.Name, Partition: strconv.Itoa(part), Key: m.Key, Value: m.Value, Offset: off, Timestamp: m.Time,
		})
	}

	return out
}
