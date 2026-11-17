package ocistreaming

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"

	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	batchHeaderSize    = 61
	batchPrefixSize    = 12
	batchCRCStart      = 21
	batchCRCOffset     = 17
	batchMagic         = 2
	compressionMask    = 0x7
	compressionGzip    = 1
	attrTransactional  = 0x10
	attrControl        = 0x20
	maxBatchInflated   = 4 << 20
	maxRecordsPerBatch = 100000
)

var (
	errBadBatch      = errors.New("malformed record batch")
	errCompression   = errors.New("unsupported compression type")
	errBatchTooLarge = errors.New("record batch too large")
	errUnsupported   = errors.New("unsupported record batch kind")
)

func castagnoli() *crc32.Table { return crc32.MakeTable(crc32.Castagnoli) }

// splitBatches decodes the record batches of one produce partition; budget bounds the total inflated bytes.
func splitBatches(data []byte, budget *int64) ([]putEntryIn, error) {
	var out []putEntryIn

	for len(data) > 0 {
		if len(data) < batchPrefixSize {
			return nil, errBadBatch
		}

		size := int64(binary.BigEndian.Uint32(data[8:12])) + batchPrefixSize
		if size > int64(len(data)) || size < batchHeaderSize {
			return nil, errBadBatch
		}

		recs, err := decodeBatch(data[:size], budget)
		if err != nil {
			return nil, err
		}

		out = append(out, recs...)
		data = data[size:]
	}

	return out, nil
}

func decodeBatch(raw []byte, budget *int64) ([]putEntryIn, error) {
	var rb kmsg.RecordBatch
	if err := rb.ReadFrom(raw); err != nil || rb.Magic != batchMagic {
		return nil, errBadBatch
	}

	if rb.Attributes&(attrTransactional|attrControl) != 0 || rb.ProducerID >= 0 {
		return nil, errUnsupported
	}

	if int32(crc32.Checksum(raw[batchCRCStart:], castagnoli())) != rb.CRC { //nolint:gosec // same 32 bits, signed
		return nil, errBadBatch
	}

	records, err := inflate(rb.Records, rb.Attributes&compressionMask, budget)
	if err != nil {
		return nil, err
	}

	if rb.NumRecords < 0 || rb.NumRecords > maxRecordsPerBatch {
		return nil, errBadBatch
	}

	out := make([]putEntryIn, 0, min(int(rb.NumRecords), len(records)))

	for range rb.NumRecords {
		n, ok := recordSize(records)
		if !ok {
			return nil, errBadBatch
		}

		var rec kmsg.Record
		if rerr := rec.ReadFrom(records[:n]); rerr != nil {
			return nil, errBadBatch
		}

		records = records[n:]
		val := rec.Value

		if val == nil {
			val = []byte{}
		}

		out = append(out, putEntryIn{Key: rec.Key, Value: val})
	}

	if len(records) != 0 {
		return nil, errBadBatch
	}

	return out, nil
}

func inflate(records []byte, codec int16, budget *int64) ([]byte, error) {
	switch codec {
	case 0:
		if int64(len(records)) > *budget {
			return nil, errBatchTooLarge
		}

		*budget -= int64(len(records))

		return records, nil
	case compressionGzip:
		zr, err := gzip.NewReader(bytes.NewReader(records))
		if err != nil {
			return nil, errBadBatch
		}

		defer zr.Close()

		out, err := io.ReadAll(io.LimitReader(zr, *budget+1))
		if err != nil {
			return nil, errBadBatch
		}

		if int64(len(out)) > *budget {
			return nil, errBatchTooLarge
		}

		*budget -= int64(len(out))

		return out, nil
	}

	return nil, errCompression
}

func recordSize(b []byte) (int, bool) {
	length, n := binary.Varint(b)
	if n <= 0 || length < 0 || int64(n)+length > int64(len(b)) {
		return 0, false
	}

	return n + int(length), true
}

// encodeBatch builds one uncompressed record batch for msgs starting at offset base.
func encodeBatch(msgs []Message, base int64) []byte {
	if len(msgs) == 0 {
		return nil
	}

	baseTime := msgs[0].Time.UnixMilli()
	maxTime := baseTime

	var body []byte

	for i, m := range msgs {
		rec := kmsg.Record{
			TimestampDelta64: m.Time.UnixMilli() - baseTime,
			OffsetDelta:      int32(i),
			Key:              m.Key, Value: m.Value,
		}
		maxTime = max(maxTime, m.Time.UnixMilli())
		rec.Length = recordLength(&rec)
		body = rec.AppendTo(body)
	}

	rb := kmsg.RecordBatch{
		FirstOffset: base, PartitionLeaderEpoch: -1, Magic: batchMagic,
		LastOffsetDelta: int32(len(msgs) - 1), //nolint:gosec // bounded by the fetch size
		FirstTimestamp:  baseTime, MaxTimestamp: maxTime, ProducerID: -1, ProducerEpoch: -1, FirstSequence: -1,
		NumRecords: int32(len(msgs)), //nolint:gosec // bounded by the fetch size
		Records:    body,
	}

	out := rb.AppendTo(nil)
	binary.BigEndian.PutUint32(out[8:12], uint32(len(out)-batchPrefixSize)) //nolint:gosec // bounded by the fetch size
	binary.BigEndian.PutUint32(out[batchCRCOffset:batchCRCStart], crc32.Checksum(out[batchCRCStart:], castagnoli()))

	return out
}

func recordLength(rec *kmsg.Record) int32 {
	probe := *rec
	probe.Length = 0
	enc := probe.AppendTo(nil)

	var buf [binary.MaxVarintLen32]byte

	return int32(len(enc) - binary.PutVarint(buf[:], 0)) //nolint:gosec // bounded by message size
}
