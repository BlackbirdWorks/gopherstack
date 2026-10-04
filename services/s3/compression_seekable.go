package s3

import (
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
)

const (
	seekableFrameBytes = 256 << 10
	// seekableMinBytes is the size from which bodies are stored as indexed frames.
	seekableMinBytes = 1 << 20
	// seekableMaxFrames bounds the index a stored blob may declare.
	seekableMaxFrames = 1 << 16
	// seekableMaxFrameRaw bounds one frame's decoded size.
	seekableMaxFrameRaw = 8 << 20
	seekableMagic       = 0x8F92EAB1
	skippableMagic      = 0x184D2A5E
	seekableEntryBytes  = 8
	seekableFooterBytes = 9
	skippableHeadBytes  = 8
	seekableReservedMsk = 0x7C
	seekableChecksumBit = 0x80
	seekableChecksumLen = 4
)

var errSeekableFrame = errors.New("s3: corrupt zstd frame")

// seekableIndex maps decoded offsets to frames of a seekable zstd blob.
type seekableIndex struct {
	rawOff  []int64
	compOff []int64
}

func (x *seekableIndex) frames() int { return len(x.rawOff) - 1 }

func (x *seekableIndex) total() int64 { return x.rawOff[len(x.rawOff)-1] }

// frameAt returns the frame holding decoded offset off, which must be < total.
func (x *seekableIndex) frameAt(off int64) int {
	return sort.Search(x.frames(), func(i int) bool { return x.rawOff[i+1] > off })
}

// parseSeekableIndex validates the trailing seek table of blob. wantSize < 0
// skips the decoded-size check. Any inconsistency yields ok=false.
func parseSeekableIndex(blob []byte, wantSize int64) (*seekableIndex, bool) {
	n := len(blob)
	if n < seekableFooterBytes+skippableHeadBytes ||
		binary.LittleEndian.Uint32(blob[n-4:]) != seekableMagic {
		return nil, false
	}

	desc := blob[n-5]
	if desc&seekableReservedMsk != 0 {
		return nil, false
	}

	entry := seekableEntryBytes
	if desc&seekableChecksumBit != 0 {
		entry += seekableChecksumLen
	}

	frames := int64(binary.LittleEndian.Uint32(blob[n-seekableFooterBytes:]))
	if frames == 0 || frames > seekableMaxFrames {
		return nil, false
	}

	payload := frames*int64(entry) + seekableFooterBytes
	start := int64(n) - payload - skippableHeadBytes

	if start < 0 || binary.LittleEndian.Uint32(blob[start:]) != skippableMagic ||
		int64(binary.LittleEndian.Uint32(blob[start+4:])) != payload {
		return nil, false
	}

	return readSeekableEntries(blob, start, int(frames), entry, wantSize)
}

func readSeekableEntries(blob []byte, start int64, frames, entry int, wantSize int64) (*seekableIndex, bool) {
	idx := &seekableIndex{rawOff: make([]int64, frames+1), compOff: make([]int64, frames+1)}
	pos := start + skippableHeadBytes

	for i := range frames {
		comp := int64(binary.LittleEndian.Uint32(blob[pos:]))
		raw := int64(binary.LittleEndian.Uint32(blob[pos+4:]))
		pos += int64(entry)

		if raw > seekableMaxFrameRaw || comp < int64(len(zstdMagic)) {
			return nil, false
		}

		idx.compOff[i+1] = idx.compOff[i] + comp
		idx.rawOff[i+1] = idx.rawOff[i] + raw

		if idx.compOff[i+1] > start || idx.rawOff[i+1] > zstdMaxObject ||
			!isZstd(blob[idx.compOff[i]:]) {
			return nil, false
		}
	}

	if idx.rawOff[frames] == 0 || idx.compOff[frames] != start || (wantSize >= 0 && idx.rawOff[frames] != wantSize) {
		return nil, false
	}

	return idx, true
}

// encodeSeekable compresses parts as fixed-size independent zstd frames
// followed by a seekable-format index; the blob stays a valid zstd stream.
func (c *ZstdCompressor) encodeSeekable(parts [][]byte, total int) ([]byte, error) {
	enc, err := c.encoder()
	if err != nil {
		return nil, err
	}

	starts := make([]int, len(parts))
	for i := range parts {
		if i > 0 {
			starts[i] = starts[i-1] + len(parts[i-1])
		}
	}

	nFrames := (total + seekableFrameBytes - 1) / seekableFrameBytes
	frames := make([][]byte, nFrames)
	var (
		next atomic.Int64
		wg   sync.WaitGroup
	)

	for range min(runtime.GOMAXPROCS(0), nFrames) {
		wg.Go(func() {
			var join, out []byte

			for {
				i := int(next.Add(1)) - 1
				if i >= nFrames {
					return
				}

				lo := i * seekableFrameBytes
				in, buf := frameInput(parts, starts, lo, min(lo+seekableFrameBytes, total), join)
				join = buf
				out = enc.EncodeAll(in, out[:0])
				frames[i] = append([]byte(nil), out...)
			}
		})
	}

	wg.Wait()

	return assembleSeekable(frames, total), nil
}

// frameInput returns decoded bytes [lo,hi): a subslice when they sit inside one
// part, else a copy into join. The second result is join for reuse.
func frameInput(parts [][]byte, starts []int, lo, hi int, join []byte) ([]byte, []byte) {
	pi := sort.Search(len(parts), func(i int) bool { return starts[i]+len(parts[i]) > lo })
	if hi <= starts[pi]+len(parts[pi]) {
		return parts[pi][lo-starts[pi] : hi-starts[pi]], join
	}

	join = join[:0]

	for ; pi < len(parts) && starts[pi] < hi; pi++ {
		from := max(lo-starts[pi], 0)
		to := min(hi-starts[pi], len(parts[pi]))
		join = append(join, parts[pi][from:to]...)
	}

	return join, join
}

func assembleSeekable(frames [][]byte, total int) []byte {
	comp := 0
	for _, f := range frames {
		comp += len(f)
	}

	payload := len(frames)*seekableEntryBytes + seekableFooterBytes
	out := make([]byte, 0, comp+skippableHeadBytes+payload)

	for _, f := range frames {
		out = append(out, f...)
	}

	out = binary.LittleEndian.AppendUint32(out, skippableMagic)
	out = binary.LittleEndian.AppendUint32(out, uint32(payload)) //nolint:gosec // bounded by seekableMaxFrames

	for i, f := range frames {
		raw := min(seekableFrameBytes, total-i*seekableFrameBytes)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(f))) //nolint:gosec // frame under 4 GiB
		out = binary.LittleEndian.AppendUint32(out, uint32(raw))    //nolint:gosec // raw <= seekableFrameBytes
	}

	out = binary.LittleEndian.AppendUint32(out, uint32(len(frames))) //nolint:gosec // bounded by seekableMaxFrames
	out = append(out, 0)

	return binary.LittleEndian.AppendUint32(out, seekableMagic)
}

// DecompressRange decodes only decoded bytes [start,end] of a seekable blob
// holding size bytes. ok is false when the blob has no valid index.
func (c *ZstdCompressor) DecompressRange(data []byte, size, start, end int64) ([]byte, bool, error) {
	if !isZstd(data) || start < 0 || end < start || end >= size {
		return nil, false, nil
	}

	idx, ok := parseSeekableIndex(data, size)
	if !ok {
		return nil, false, nil
	}

	out, err := c.decodeWindow(data, idx, start, end)

	return out, true, err
}

func (c *ZstdCompressor) decodeWindow(data []byte, idx *seekableIndex, start, end int64) ([]byte, error) {
	first, last := idx.frameAt(start), idx.frameAt(end)
	out := make([]byte, end-start+1)
	errs := make([]error, last-first+1)

	var (
		next atomic.Int64
		wg   sync.WaitGroup
	)

	for range min(runtime.GOMAXPROCS(0), last-first+1) {
		wg.Go(func() {
			for {
				i := int(next.Add(1)) - 1
				if i >= len(errs) {
					return
				}

				errs[i] = c.decodeFrame(data, idx, first+i, start, end, out)
			}
		})
	}

	wg.Wait()

	for _, e := range errs {
		if e != nil {
			return nil, e
		}
	}

	return out, nil
}

// decodeFrame decodes frame f and copies its overlap with [start,end] into out.
func (c *ZstdCompressor) decodeFrame(data []byte, idx *seekableIndex, f int, start, end int64, out []byte) error {
	dec, err := c.decoder()
	if err != nil {
		return err
	}

	fs, fe := idx.rawOff[f], idx.rawOff[f+1]
	lo, hi := max(start, fs), min(end+1, fe)
	frame := data[idx.compOff[f]:idx.compOff[f+1]]

	if lo == fs && hi == fe {
		dst := out[fs-start : fe-start : fe-start]

		res, dErr := dec.DecodeAll(frame, dst[:0])
		if dErr != nil || int64(len(res)) != fe-fs {
			return fmt.Errorf("%w: frame %d", errSeekableFrame, f)
		}

		if len(res) > 0 && &res[0] != &dst[0] {
			copy(dst, res)
		}

		return nil
	}

	buf := c.getScratch()

	res, dErr := dec.DecodeAll(frame, buf)
	if dErr != nil || int64(len(res)) != fe-fs {
		return fmt.Errorf("%w: frame %d", errSeekableFrame, f)
	}

	copy(out[lo-start:hi-start], res[lo-fs:hi-fs])
	c.putScratch(res)

	return nil
}
