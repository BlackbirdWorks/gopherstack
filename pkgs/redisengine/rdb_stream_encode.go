package redisengine

import (
	"bytes"
	"encoding/binary"
	"slices"
	"sort"
)

const (
	defaultNodeEntries = 100
	defaultNodeBytes   = 4096
	lpMaxCount         = 0xffff
	lpInt7Max          = 127
	lpInt13Min         = -4096
	lpInt13Max         = 4095
	lpInt16Min         = -32768
	lpInt16Max         = 32767
	lpInt24Min         = -8388608
	lpInt24Max         = 8388607
	lpInt32Min         = -2147483648
	lpInt32Max         = 2147483647
	lpStr6Max          = 63
	lpStr12Max         = 4095
	lpTagInt16         = 0xf1
	lpTagInt24         = 0xf2
	lpTagInt32         = 0xf3
	lpTagInt64         = 0xf4
	lpHeaderBytes      = 6
	lpBackShift        = 7
	lpBackCont         = 0x80
	lpBackMask         = 0x7f
	nodeEntryOverhead  = 8
)

type streamLimits struct {
	nodeBytes   int
	nodeEntries int
}

func defaultStreamLimits() streamLimits {
	return streamLimits{nodeBytes: defaultNodeBytes, nodeEntries: defaultNodeEntries}
}

type lpBuilder struct {
	body []byte
	n    int
}

func (b *lpBuilder) entry(enc []byte) {
	b.body = append(b.body, enc...)
	b.body = append(b.body, lpBackLenBytes(len(enc))...)
	b.n++
}

func (b *lpBuilder) int(v int64) { b.entry(lpEncodeInt(v)) }

func (b *lpBuilder) str(s string) { b.entry(lpEncodeString(s)) }

func (b *lpBuilder) bytes() []byte {
	total := lpHeaderBytes + len(b.body) + 1
	out := make([]byte, lpHeaderBytes, total)

	binary.LittleEndian.PutUint32(out, uint32(total)) //nolint:gosec // bounded by the node size limits

	count := min(b.n, lpMaxCount)
	binary.LittleEndian.PutUint16(out[size32:], uint16(count)) //nolint:gosec // clamped to 16 bits above

	out = append(out, b.body...)

	return append(out, lpEnd)
}

func lpEncodeInt(v int64) []byte {
	switch {
	case v >= 0 && v <= lpInt7Max:
		return []byte{byte(v)}
	case v >= lpInt13Min && v <= lpInt13Max:
		u := uint16(v) & (lpInt13Range - 1) //nolint:gosec // 13-bit two's complement

		return []byte{lpTag13Int | byte(u>>bitsPerByte&byteMask), byte(u & byteMask)}
	case v >= lpInt16Min && v <= lpInt16Max:
		return lpWideInt(lpTagInt16, v, int16Bytes)
	case v >= lpInt24Min && v <= lpInt24Max:
		return lpWideInt(lpTagInt24, v, int24Bytes)
	case v >= lpInt32Min && v <= lpInt32Max:
		return lpWideInt(lpTagInt32, v, size32)
	default:
		return lpWideInt(lpTagInt64, v, size64)
	}
}

func lpWideInt(tag byte, v int64, width int) []byte {
	out := make([]byte, 1+width)
	out[0] = tag

	u := uint64(v) //nolint:gosec // two's complement bytes
	for i := range width {
		out[1+i] = byte(u >> (bitsPerByte * i) & byteMask)
	}

	return out
}

func lpEncodeString(s string) []byte {
	n := len(s)

	switch {
	case n <= lpStr6Max:
		return append([]byte{lpTag6Str | byte(n)}, s...)
	case n <= lpStr12Max:
		return append([]byte{lpTag12Str | byte(n>>bitsPerByte&byteMask), byte(n & byteMask)}, s...)
	default:
		out := make([]byte, 1+size32, 1+size32+n)
		out[0] = lpTag32Str

		binary.LittleEndian.PutUint32(out[1:], uint32(n)) //nolint:gosec // node strings are bounded by the input

		return append(out, s...)
	}
}

func lpBackLenBytes(n int) []byte {
	l := lpBackLen(n)
	out := make([]byte, l)

	for i := range l {
		shift := lpBackShift * (l - 1 - i)
		out[i] = byte(n >> shift & lpBackMask)

		if i > 0 {
			out[i] |= lpBackCont
		}
	}

	return out
}

// streamNodes splits a stream into listpack nodes, each keyed by its first entry ID.
func streamNodes(entries []streamEntry, lim streamLimits) ([]streamID, [][]byte) {
	var (
		ids   []streamID
		blobs [][]byte
	)

	for i := 0; i < len(entries); {
		j := nodeEnd(entries, i, lim)
		master, _ := parseStreamID(entries[i].ID)

		ids = append(ids, master)
		blobs = append(blobs, encodeStreamNode(master, entries[i:j]))
		i = j
	}

	return ids, blobs
}

func nodeEnd(entries []streamEntry, from int, lim streamLimits) int {
	size, j := 0, from

	for j < len(entries) {
		if j > from {
			if lim.nodeEntries > 0 && j-from >= lim.nodeEntries {
				break
			}

			if lim.nodeBytes > 0 && size+entrySize(entries[j]) > lim.nodeBytes {
				break
			}
		}

		size += entrySize(entries[j])
		j++
	}

	return j
}

func entrySize(e streamEntry) int {
	n := nodeEntryOverhead

	for _, v := range e.Values {
		n += len(v) + 1
	}

	return n
}

func encodeStreamNode(master streamID, entries []streamEntry) []byte {
	var b lpBuilder

	fields := entryFields(entries[0])

	b.int(int64(len(entries)))
	b.int(0)
	b.int(int64(len(fields)))

	for _, f := range fields {
		b.str(f)
	}

	b.int(0)

	for _, e := range entries {
		appendStreamEntry(&b, master, fields, e)
	}

	return b.bytes()
}

func entryFields(e streamEntry) []string {
	out := make([]string, 0, len(e.Values)/idParts)
	for i := 0; i+1 < len(e.Values); i += idParts {
		out = append(out, e.Values[i])
	}

	return out
}

func appendStreamEntry(b *lpBuilder, master streamID, fields []string, e streamEntry) {
	id, _ := parseStreamID(e.ID)
	own := entryFields(e)
	same := slices.Equal(own, fields)

	flags := int64(0)
	if same {
		flags = streamFlagSameFields
	}

	b.int(flags)
	b.int(int64(id.ms - master.ms))   //nolint:gosec // entries are sorted, so the diff is small and non-negative
	b.int(int64(id.seq - master.seq)) //nolint:gosec // as above

	count := int64(len(own)) + streamEntryFixed

	if !same {
		b.int(int64(len(own)))

		count += int64(len(own)) + 1
	}

	for i := 0; i+1 < len(e.Values); i += idParts {
		if !same {
			b.str(e.Values[i])
		}

		b.str(e.Values[i+1])
	}

	b.int(count)
}

// putStreamValue writes a stream as RDB_TYPE_STREAM_LISTPACKS_3.
func putStreamValue(w *bytes.Buffer, kd *keyDump, lim streamLimits) {
	ids, blobs := streamNodes(kd.Stream, lim)

	putLen(w, uint64(len(ids)))

	for i, id := range ids {
		putString(w, string(id.raw()))
		putString(w, string(blobs[i]))
	}

	last, first := streamBounds(kd)

	putLen(w, uint64(len(kd.Stream)))
	putIDPair(w, last)
	putIDPair(w, first)
	putIDPair(w, streamID{})
	putLen(w, uint64(len(kd.Stream)))

	groups := kd.Groups
	sort.SliceStable(groups, func(i, j int) bool { return groups[i].Name < groups[j].Name })

	putLen(w, uint64(len(groups)))

	for i := range groups {
		putStreamGroup(w, &groups[i])
	}
}

func streamBounds(kd *keyDump) (streamID, streamID) {
	var last, first streamID

	if len(kd.Stream) > 0 {
		first, _ = parseStreamID(kd.Stream[0].ID)
		last, _ = parseStreamID(kd.Stream[len(kd.Stream)-1].ID)
	}

	if declared, ok := parseStreamID(kd.StreamLastID); ok && greaterID(declared, last) {
		last = declared
	}

	return last, first
}

func greaterID(a, b streamID) bool { return a.ms > b.ms || a.ms == b.ms && a.seq > b.seq }

func putIDPair(w *bytes.Buffer, id streamID) {
	putLen(w, id.ms)
	putLen(w, id.seq)
}

func putMillis(w *bytes.Buffer, ms int64) {
	var b [size64]byte

	binary.LittleEndian.PutUint64(b[:], uint64(ms)) //nolint:gosec // signed values are stored two's complement
	w.Write(b[:])
}

func putStreamGroup(w *bytes.Buffer, g *streamGroupDump) {
	last, _ := parseStreamID(g.LastID)

	putString(w, g.Name)
	putIDPair(w, last)
	putLen(w, ^uint64(0))

	pel := slices.Clone(g.Pending)
	sort.SliceStable(pel, func(i, j int) bool {
		a, _ := parseStreamID(pel[i].ID)
		b, _ := parseStreamID(pel[j].ID)

		return greaterID(b, a)
	})

	putLen(w, uint64(len(pel)))

	for _, p := range pel {
		id, _ := parseStreamID(p.ID)

		w.Write(id.raw())
		putMillis(w, p.DeliveryMS)
		putLen(w, uint64(max(p.Count, 1)))
	}

	putLen(w, uint64(len(g.Consumers)))

	for _, c := range g.Consumers {
		putString(w, c.Name)
		putMillis(w, c.SeenMS)
		putMillis(w, c.ActiveMS)

		var owned []streamPendingDump

		for _, p := range pel {
			if p.Consumer == c.Name {
				owned = append(owned, p)
			}
		}

		putLen(w, uint64(len(owned)))

		for _, p := range owned {
			id, _ := parseStreamID(p.ID)
			w.Write(id.raw())
		}
	}
}
