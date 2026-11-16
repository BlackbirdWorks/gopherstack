package azureeventhubs

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/blackbirdworks/gopherstack/pkgs/amqp1"
)

const (
	batchHeaderSize     = 61
	batchPrefixSize     = 12
	batchCRCStart       = 21
	batchCRCOffset      = 17
	batchMagic          = 2
	compressionMask     = 0x7
	compressionNone     = 0
	compressionGzip     = 1
	attrTransactional   = 0x10
	attrControl         = 0x20
	maxBatchInflated    = 16 << 20
	maxRecordsPerBatch  = 100000
	maxHeadersPerRecord = 1000
)

var (
	errBadBatch      = errors.New("malformed record batch")
	errCompression   = errors.New("unsupported compression type")
	errBatchTooLarge = errors.New("record batch too large")
	errUnsupported   = errors.New("unsupported record batch kind")
)

// producedBatch is one decoded record batch of a produce request.
type producedBatch struct {
	events        []NewEvent
	producerID    int64
	producerEpoch int16
	firstSeq      int32
}

// splitBatches decodes the batches in a produce partition payload.
func splitBatches(data []byte) ([]producedBatch, error) {
	var out []producedBatch

	for len(data) > 0 {
		if len(data) < batchPrefixSize {
			return nil, errBadBatch
		}

		size := int64(binary.BigEndian.Uint32(data[8:12])) + batchPrefixSize
		if size > int64(len(data)) || size < batchHeaderSize {
			return nil, errBadBatch
		}

		pb, err := decodeBatch(data[:size])
		if err != nil {
			return nil, err
		}

		out = append(out, pb)
		data = data[size:]
	}

	return out, nil
}

func decodeBatch(raw []byte) (producedBatch, error) {
	var rb kmsg.RecordBatch
	if err := rb.ReadFrom(raw); err != nil || rb.Magic != batchMagic {
		return producedBatch{}, errBadBatch
	}

	if rb.Attributes&(attrTransactional|attrControl) != 0 {
		return producedBatch{}, errUnsupported
	}

	sum := crc32.Checksum(raw[batchCRCStart:], crc32.MakeTable(crc32.Castagnoli))
	if int32(sum) != rb.CRC { //nolint:gosec // the wire field is the same 32 bits, signed
		return producedBatch{}, errBadBatch
	}

	records, err := inflate(rb.Records, rb.Attributes&compressionMask)
	if err != nil {
		return producedBatch{}, err
	}

	if rb.NumRecords < 0 || rb.NumRecords > maxRecordsPerBatch {
		return producedBatch{}, errBadBatch
	}

	evs := make([]NewEvent, 0, rb.NumRecords)

	for range rb.NumRecords {
		n, ok := recordSize(records)
		if !ok {
			return producedBatch{}, errBadBatch
		}

		var rec kmsg.Record
		if rerr := rec.ReadFrom(records[:n]); rerr != nil {
			return producedBatch{}, errBadBatch
		}

		records = records[n:]

		ev, eerr := eventOfRecord(&rec)
		if eerr != nil {
			return producedBatch{}, eerr
		}

		evs = append(evs, ev)
	}

	return producedBatch{
		events: evs, producerID: rb.ProducerID, producerEpoch: rb.ProducerEpoch, firstSeq: rb.FirstSequence,
	}, nil
}

func inflate(records []byte, codec int16) ([]byte, error) {
	switch codec {
	case compressionNone:
		return records, nil
	case compressionGzip:
		zr, err := gzip.NewReader(bytes.NewReader(records))
		if err != nil {
			return nil, errBadBatch
		}

		defer zr.Close()

		out, err := io.ReadAll(io.LimitReader(zr, maxBatchInflated+1))
		if err != nil {
			return nil, errBadBatch
		}

		if len(out) > maxBatchInflated {
			return nil, errBatchTooLarge
		}

		return out, nil
	}

	return nil, errCompression
}

// recordSize returns the wire size of the record at the start of b.
func recordSize(b []byte) (int, bool) {
	length, n := binary.Varint(b)
	if n <= 0 || length < 0 || int64(n)+length > int64(len(b)) {
		return 0, false
	}

	return n + int(length), true
}

func eventOfRecord(rec *kmsg.Record) (NewEvent, error) {
	if len(rec.Headers) > maxHeadersPerRecord {
		return NewEvent{}, errBadBatch
	}

	msg := &amqp1.Message{Data: [][]byte{rec.Value}}
	if len(rec.Headers) > 0 {
		msg.ApplicationProperties = amqp1.Map{}

		for _, h := range rec.Headers {
			msg.ApplicationProperties[h.Key] = h.Value
		}
	}

	key := string(rec.Key)
	if key != "" {
		msg.Annotations = amqp1.Map{amqp1.Symbol(annPartitionKey): key}
	}

	raw, err := msg.Encode()
	if err != nil {
		return NewEvent{}, fmt.Errorf("encode record: %w", err)
	}

	return NewEvent{Key: key, Raw: raw}, nil
}

// recordOfEvent converts a stored event to a Kafka record.
func recordOfEvent(lr logRecord, base int64, baseTime int64) (kmsg.Record, error) {
	msg, err := amqp1.DecodeMessage(lr.Raw)
	if err != nil {
		return kmsg.Record{}, fmt.Errorf("stored event: %w", err)
	}

	rec := kmsg.Record{
		TimestampDelta64: lr.Enqueued.UnixMilli() - baseTime,
		OffsetDelta:      int32(lr.Seq - base), //nolint:gosec // bounded by the fetch size
		Value:            valueOf(msg),
	}

	if lr.Key != "" {
		rec.Key = []byte(lr.Key)
	}

	for k, v := range msg.ApplicationProperties {
		name, ok := k.(string)
		if !ok {
			continue
		}

		rec.Headers = append(rec.Headers, kmsg.Header{Key: name, Value: headerBytes(v)})
	}

	sortHeaders(rec.Headers)

	return rec, nil
}

func valueOf(m *amqp1.Message) []byte {
	if len(m.Data) > 0 {
		return bytes.Join(m.Data, nil)
	}

	if !m.HasValue {
		return nil
	}

	switch v := m.Value.(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	case nil:
		return nil
	}

	return fmt.Appendf(nil, "%v", m.Value)
}

func headerBytes(v any) []byte {
	switch t := v.(type) {
	case []byte:
		return t
	case string:
		return []byte(t)
	case nil:
		return nil
	}

	return fmt.Appendf(nil, "%v", v)
}

func sortHeaders(h []kmsg.Header) {
	for i := 1; i < len(h); i++ {
		for j := i; j > 0 && h[j-1].Key > h[j].Key; j-- {
			h[j-1], h[j] = h[j], h[j-1]
		}
	}
}

// encodeBatch builds one uncompressed record batch holding recs, whose first offset is base.
func encodeBatch(recs []logRecord) ([]byte, error) {
	if len(recs) == 0 {
		return nil, nil
	}

	base := recs[0].Seq
	baseTime := recs[0].Enqueued.UnixMilli()
	maxTime := baseTime

	var body []byte

	for _, lr := range recs {
		rec, err := recordOfEvent(lr, base, baseTime)
		if err != nil {
			return nil, err
		}

		maxTime = max(maxTime, lr.Enqueued.UnixMilli())

		rec.Length = recordLength(&rec)
		body = rec.AppendTo(body)
	}

	rb := kmsg.RecordBatch{
		FirstOffset: base, PartitionLeaderEpoch: -1, Magic: batchMagic,
		LastOffsetDelta: int32(recs[len(recs)-1].Seq - base), //nolint:gosec // bounded by the fetch size
		FirstTimestamp:  baseTime, MaxTimestamp: maxTime, ProducerID: -1, ProducerEpoch: -1, FirstSequence: -1,
		NumRecords: int32(len(recs)), //nolint:gosec // bounded by the fetch size
		Records:    body,
	}

	out := rb.AppendTo(nil)
	binary.BigEndian.PutUint32(out[8:12], uint32(len(out)-batchPrefixSize)) //nolint:gosec // bounded by the fetch size
	binary.BigEndian.PutUint32(out[batchCRCOffset:batchCRCStart],
		crc32.Checksum(out[batchCRCStart:], crc32.MakeTable(crc32.Castagnoli)))

	return out, nil
}

// recordLength returns the Length field value of rec: the byte size of the record after the length varint.
func recordLength(rec *kmsg.Record) int32 {
	probe := *rec
	probe.Length = 0
	enc := probe.AppendTo(nil)

	return int32(len(enc) - varintLen(0)) //nolint:gosec // bounded by event size
}

func varintLen(v int32) int {
	var buf [binary.MaxVarintLen32]byte

	return binary.PutVarint(buf[:], int64(v))
}
