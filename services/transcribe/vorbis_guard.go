package transcribe

import (
	"encoding/binary"
	"errors"
	"math"
)

const (
	oggPageHeader    = 27
	oggSegmentsAt    = 26
	vorbisHeaders    = 3
	vorbisMagicLen   = 7
	vorbisCommentTyp = 3
	vorbisSetupTyp   = 5
	vorbisCodebookID = 0x564342
	vorbisHeapBudget = 16 << 20
	bytesPerUint32   = 4
	huffmanBytes     = 9
	maxLacing        = 255
	bitIndexMask     = 7
	lengthBitCount   = 5
)

var errVorbisHeader = errors.New("hostile Vorbis header")

// guardVorbisHeaders rejects comment and codebook headers that declare more memory than the file can back,
// because the Vorbis decoder allocates from header-declared counts before reading the data.
func guardVorbisHeaders(data []byte) error {
	packets := oggHeaderPackets(data)

	for _, p := range packets {
		if len(p) < vorbisMagicLen || string(p[1:vorbisMagicLen]) != "vorbis" {
			continue
		}

		var err error

		switch p[0] {
		case vorbisCommentTyp:
			err = guardVorbisComments(p[vorbisMagicLen:])
		case vorbisSetupTyp:
			err = guardVorbisCodebooks(p[vorbisMagicLen:])
		}

		if err != nil {
			return err
		}
	}

	return nil
}

// oggHeaderPackets reassembles the first three packets of a logical stream.
func oggHeaderPackets(data []byte) [][]byte {
	var (
		packets [][]byte
		cur     []byte
	)

	pos := 0

	for pos+oggPageHeader <= len(data) && len(packets) < vorbisHeaders {
		if string(data[pos:pos+4]) != "OggS" {
			break
		}

		nseg := int(data[pos+oggSegmentsAt])
		body := pos + oggPageHeader + nseg

		if body > len(data) {
			break
		}

		for _, lace := range data[pos+oggPageHeader : body] {
			end := min(body+int(lace), len(data))
			cur = append(cur, data[body:end]...)
			body = end

			if lace < maxLacing {
				packets = append(packets, cur)
				cur = nil

				if len(packets) == vorbisHeaders {
					break
				}
			}
		}

		pos = body
	}

	return packets
}

func guardVorbisComments(h []byte) error {
	if len(h) < bytesPerUint32 {
		return errVorbisHeader
	}

	vendor := uint64(binary.LittleEndian.Uint32(h))
	if vendor+bytesPerUint32 > uint64(len(h)) {
		return errVorbisHeader
	}

	h = h[bytesPerUint32+vendor:]
	if len(h) < bytesPerUint32 {
		return errVorbisHeader
	}

	count := uint64(binary.LittleEndian.Uint32(h))
	if count*bytesPerUint32 > uint64(len(h)) {
		return errVorbisHeader
	}

	return nil
}

type vorbisBits struct {
	data []byte
	pos  uint64
}

func (r *vorbisBits) left() uint64 { return uint64(len(r.data))*bitsPerByte - r.pos }

func (r *vorbisBits) read(n uint) (uint64, bool) {
	if uint64(n) > r.left() {
		return 0, false
	}

	var v uint64

	for i := range n {
		bit := (r.data[r.pos>>3] >> (r.pos & bitIndexMask)) & 1
		v |= uint64(bit) << i
		r.pos++
	}

	return v, true
}

func (r *vorbisBits) skip(n uint64) bool {
	if n > r.left() {
		return false
	}

	r.pos += n

	return true
}

func guardVorbisCodebooks(h []byte) error {
	r := &vorbisBits{data: h}

	count, ok := r.read(bitsPerByte)
	if !ok {
		return errVorbisHeader
	}

	var heap uint64

	for range count + 1 {
		cost, err := guardCodebook(r)
		if err != nil {
			return err
		}

		heap += cost
		if heap > vorbisHeapBudget {
			return errVorbisHeader
		}
	}

	return nil
}

// guardCodebook walks one codebook and returns the decoder's allocation for it.
func guardCodebook(r *vorbisBits) (uint64, error) {
	const (
		patternBits = 24
		dimBits     = 16
		entryBits   = 24
	)

	pattern, ok1 := r.read(patternBits)
	dims, ok2 := r.read(dimBits)
	entries, ok3 := r.read(entryBits)
	ordered, ok4 := r.read(1)

	if !ok1 || !ok2 || !ok3 || !ok4 || pattern != vorbisCodebookID {
		return 0, errVorbisHeader
	}

	var lengthsOK bool

	if ordered == 0 {
		lengthsOK = skipUnorderedLengths(r, entries)
	} else {
		lengthsOK = skipOrderedLengths(r, entries)
	}

	if !lengthsOK {
		return 0, errVorbisHeader
	}

	lookup, err := guardLookup(r, entries, dims)
	if err != nil {
		return 0, err
	}

	return entries*huffmanBytes + lookup, nil
}

// guardLookup skips a codebook's value table and returns the memory the decoder builds from it.
func guardLookup(r *vorbisBits, entries, dims uint64) (uint64, error) {
	const (
		kindBits      = 4
		valueBitsBits = 4
		floatBits     = 64
	)

	kind, ok := r.read(kindBits)
	if !ok || kind > 2 {
		return 0, errVorbisHeader
	}

	if kind == 0 {
		return 0, nil
	}

	if !r.skip(floatBits) {
		return 0, errVorbisHeader
	}

	valueBits, ok := r.read(valueBitsBits)
	if !ok || !r.skip(1) || dims == 0 {
		return 0, errVorbisHeader
	}

	values := entries * dims
	if kind == 1 {
		values = lookup1Count(entries, dims)
	}

	if !r.skip(values * (valueBits + 1)) {
		return 0, errVorbisHeader
	}

	return (entries*dims + values) * bytesPerUint32, nil
}

func skipUnorderedLengths(r *vorbisBits, entries uint64) bool {
	sparse, ok := r.read(1)
	if !ok {
		return false
	}

	if sparse == 0 {
		return r.skip(entries * lengthBitCount)
	}

	for range entries {
		flag, flagOK := r.read(1)
		if !flagOK || flag == 1 && !r.skip(lengthBitCount) {
			return false
		}
	}

	return true
}

func skipOrderedLengths(r *vorbisBits, entries uint64) bool {
	if !r.skip(lengthBitCount) {
		return false
	}

	var cur uint64

	for cur < entries {
		bits := uint(0)
		for rest := entries - cur; rest > 0; rest >>= 1 {
			bits++
		}

		n, ok := r.read(bits)
		if !ok {
			return false
		}

		cur += n
	}

	return true
}

// lookup1Count is the largest v with v^dims <= entries.
func lookup1Count(entries, dims uint64) uint64 {
	v := uint64(math.Pow(float64(entries), 1/float64(dims)))

	for math.Pow(float64(v+1), float64(dims)) <= float64(entries) {
		v++
	}

	for v > 0 && math.Pow(float64(v), float64(dims)) > float64(entries) {
		v--
	}

	return v
}
