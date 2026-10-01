package s3

import (
	"bytes"
	"strings"
)

// selectScanRange is the optional ScanRange element of a SelectObjectContent request.
type selectScanRange struct {
	Start *int64 `xml:"Start"`
	End   *int64 `xml:"End"`
}

// resolve returns the inclusive byte window for an object of size bytes.
func (s *selectScanRange) resolve(size int64) (int64, int64) {
	last := size - 1
	switch {
	case s.Start == nil && s.End != nil:
		return max(size-max(*s.End, 0), 0), last
	case s.End == nil:
		return max(*s.Start, 0), last
	default:
		return max(*s.Start, 0), min(*s.End, last)
	}
}

// applySelectScanRange keeps only the records whose first byte lies inside the
// scan range. A CSV header row is always kept so FileHeaderInfo still resolves.
func applySelectScanRange(data []byte, req *selectRequest) []byte {
	sr := req.ScanRange
	if sr == nil || (sr.Start == nil && sr.End == nil) {
		return data
	}

	delim := []byte("\n")
	keepHeader := false

	switch {
	case req.InputSerialization.JSON != nil:
		if !strings.EqualFold(req.InputSerialization.JSON.Type, "LINES") {
			return data
		}
	default:
		opts := resolveCSVInputOptions(req.InputSerialization.CSV)
		delim = []byte(opts.recordDelim)
		hdr := csvFileHeaderInfo(req.InputSerialization.CSV)
		keepHeader = hdr == csvFileHeaderInfoUse || hdr == "IGNORE"
	}

	start, end := sr.resolve(int64(len(data)))

	var out []byte

	pos := int64(0)
	first := true

	for pos < int64(len(data)) {
		recEnd := int64(len(data))
		next := recEnd

		if i := bytes.Index(data[pos:], delim); i >= 0 {
			recEnd = pos + int64(i) + int64(len(delim))
			next = recEnd
		}

		inRange := pos >= start && pos <= end
		if inRange || (first && keepHeader) {
			out = append(out, data[pos:recEnd]...)
		}

		first = false
		pos = next
	}

	return out
}

const compressionNone = "NONE"
