package transcribe

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/hajimehoshi/go-mp3"
	"github.com/jfreymuth/oggvorbis"
	"github.com/mewkiz/flac"
	"github.com/mewkiz/flac/frame"
)

const (
	engineSampleRate  = 16000
	maxAudioSeconds   = 4 * 3600
	decodeChunkFrames = 4096
	mp3FrameBytes     = 4
	mp3ScanLimit      = 1 << 16
	id3v2HeaderLen    = 10
	syncsafeBits      = 7
	wavFmtExtMin      = 26
	wavFmtExtSubTag   = 24
	maxPCMWidth       = 4
	float32Width      = 4
	float64Width      = 8
	g711Mantissa      = 0x0F
	g711Segment       = 0x70
	g711SegShift      = 4
	aLawXor           = 0x55
	syncsafeMask      = 0x7F
	unsigned8Bias     = 128
	g711MuShift       = 3
	pcmSampleBytes    = 2
)

const (
	wavFormatFloat = 3
	wavFormatALaw  = 6
	wavFormatMuLaw = 7
)

var (
	errDecode      = errors.New("audio decode failed")
	errAudioLength = errors.New("audio longer than the batch limit")
)

// audioSource yields interleaved 16-bit frames; read may return frames together with io.EOF.
type audioSource interface {
	rate() int
	channels() int
	// frames is the total frame count when the container header states it, else -1.
	frames() int64
	read(dst []int16) (int, error)
}

// decodedAudio is mono-or-per-channel 16-bit little-endian PCM at engineSampleRate.
type decodedAudio struct {
	channels [][]byte
	millis   int
}

func safely(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", errDecode, r)
		}
	}()

	return fn()
}

// openAudio builds a source for a sniffed container; formats without a decoder return errDecode.
func openAudio(format string, data []byte) (audioSource, error) {
	var src audioSource

	err := safely(func() error {
		var openErr error

		switch format {
		case mediaWAV:
			src, openErr = openWAV(data)
		case mediaFLAC:
			src, openErr = openFLAC(data)
		case mediaMP3:
			src, openErr = openMP3(data)
		case mediaOGG:
			src, openErr = openOGG(data)
		default:
			openErr = fmt.Errorf("%w: no decoder for %s", errDecode, format)
		}

		return openErr
	})
	if err != nil {
		return nil, err
	}

	return src, nil
}

func decodable(format string) bool {
	switch format {
	case mediaWAV, mediaFLAC, mediaMP3, mediaOGG:
		return true
	default:
		return false
	}
}

// decodeAudio streams src into 16 kHz 16-bit PCM, downmixing to one channel when mono is set;
// more than maxSeconds of source audio yields errAudioLength.
func decodeAudio(src audioSource, mono bool, maxSeconds int) (decodedAudio, error) {
	srcCh := src.channels()
	outCh := srcCh

	if mono {
		outCh = 1
	}

	mix := &mixer{srcCh: srcCh, mono: mono, rs: make([]resampler, outCh), out: make([][]byte, outCh)}

	for c := range mix.rs {
		mix.rs[c] = newResampler(src.rate(), engineSampleRate)
	}

	buf := make([]int16, decodeChunkFrames*srcCh)
	limit := int64(maxSeconds) * int64(src.rate())

	var total int64

	for {
		var n int

		err := safely(func() error {
			var readErr error

			n, readErr = src.read(buf)

			return readErr
		})

		mix.push(buf, n)

		total += int64(n)
		if total > limit {
			return decodedAudio{}, errAudioLength
		}

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return decodedAudio{}, err
		}
	}

	return decodedAudio{channels: mix.out, millis: int(total * millisPerSecond / int64(src.rate()))}, nil
}

// mixer resamples interleaved frames into per-channel output, averaging channels when mono.
type mixer struct {
	rs    []resampler
	out   [][]byte
	srcCh int
	mono  bool
}

func (m *mixer) push(buf []int16, frames int) {
	for i := range frames {
		frame := buf[i*m.srcCh : (i+1)*m.srcCh]

		if !m.mono {
			for c, v := range frame {
				m.out[c] = m.rs[c].push(v, m.out[c])
			}

			continue
		}

		sum := 0
		for _, v := range frame {
			sum += int(v)
		}

		m.out[0] = m.rs[0].push(int16(sum/m.srcCh), m.out[0]) //nolint:gosec // an average of int16 fits int16
	}
}

// resampler converts a mono stream to a target rate: box-average (area) filtering when
// decimating, linear interpolation when interpolating.
type resampler struct {
	step   float64
	acc    float64
	need   float64
	prev   float64
	pos    float64
	primed bool
}

func newResampler(from, to int) resampler {
	step := float64(from) / float64(to)

	return resampler{step: step, need: step}
}

func (r *resampler) push(x int16, dst []byte) []byte {
	const eps = 1e-9

	v := float64(x)

	switch {
	case r.step == 1:
		return binary.LittleEndian.AppendUint16(dst, uint16(x)) //nolint:gosec // int16 bit reinterpretation
	case r.step > 1:
		avail := 1.0

		for avail > eps {
			take := math.Min(avail, r.need)
			r.acc += v * take
			r.need -= take
			avail -= take

			if r.need <= eps {
				dst = appendClamped(dst, r.acc/r.step)
				r.acc = 0
				r.need = r.step
			}
		}

		return dst
	default:
		if !r.primed {
			r.primed = true
			r.prev = v

			return dst
		}

		for r.pos < 1 {
			dst = appendClamped(dst, r.prev+(v-r.prev)*r.pos)
			r.pos += r.step
		}

		r.pos--
		r.prev = v

		return dst
	}
}

func appendClamped(dst []byte, v float64) []byte {
	return binary.LittleEndian.AppendUint16(dst, uint16(clampInt16(v))) //nolint:gosec // int16 bit reinterpretation
}

func clampInt16(v float64) int16 {
	return int16(math.Max(math.MinInt16, math.Min(math.MaxInt16, math.Round(v))))
}

type wavSource struct {
	info  wavInfo
	codec int
	width int
	pos   int
}

func wavCodecWidthOK(codec, width int) bool {
	switch codec {
	case wavFormatPCM:
		return width >= 1 && width <= maxPCMWidth
	case wavFormatFloat:
		return width == float32Width || width == float64Width
	case wavFormatALaw, wavFormatMuLaw:
		return width == 1
	default:
		return false
	}
}

func openWAV(data []byte) (audioSource, error) {
	info, err := parseWAV(data)
	if err != nil {
		return nil, err
	}

	codec := info.codec()
	width := info.bits / bitsPerByte

	if info.channels <= 0 || info.sampleRate <= 0 || info.bits%bitsPerByte != 0 || !wavCodecWidthOK(codec, width) {
		return nil, fmt.Errorf("%w: unsupported WAV encoding tag %d at %d bits", errDecode, codec, info.bits)
	}

	return &wavSource{info: info, codec: codec, width: width}, nil
}

func (s *wavSource) rate() int     { return s.info.sampleRate }
func (s *wavSource) channels() int { return s.info.channels }

func (s *wavSource) frames() int64 { return int64(len(s.info.pcm) / (s.info.channels * s.width)) }

func (s *wavSource) read(dst []int16) (int, error) {
	frameBytes := s.info.channels * s.width
	n := min(len(dst)/s.info.channels, (len(s.info.pcm)-s.pos)/frameBytes)

	for i := range n * s.info.channels {
		dst[i] = s.sample(s.info.pcm[s.pos : s.pos+s.width])
		s.pos += s.width
	}

	if n == 0 {
		return 0, io.EOF
	}

	return n, nil
}

func (s *wavSource) sample(b []byte) int16 {
	switch s.codec {
	case wavFormatALaw:
		return decodeALaw(b[0])
	case wavFormatMuLaw:
		return decodeMuLaw(b[0])
	case wavFormatFloat:
		if s.width == float32Width {
			return clampInt16(float64(math.Float32frombits(binary.LittleEndian.Uint32(b))) * math.MaxInt16)
		}

		return clampInt16(math.Float64frombits(binary.LittleEndian.Uint64(b)) * math.MaxInt16)
	}

	if s.width == 1 {
		return int16(int(b[0])-unsigned8Bias) << bitsPerByte //nolint:gosec // 8-bit sample, fits int16
	}

	return int16(binary.LittleEndian.Uint16(b[s.width-2:])) //nolint:gosec // top two bytes of a signed sample
}

func decodeMuLaw(b byte) int16 {
	const bias = 0x84

	u := ^b
	t := (int(u&g711Mantissa) << g711MuShift) + bias
	t <<= (u & g711Segment) >> g711SegShift

	if u&0x80 != 0 {
		return int16(bias - t) //nolint:gosec // G.711 magnitude fits int16
	}

	return int16(t - bias) //nolint:gosec // G.711 magnitude fits int16
}

func decodeALaw(b byte) int16 {
	a := b ^ aLawXor
	t := int(a&g711Mantissa) << g711SegShift
	seg := (a & g711Segment) >> g711SegShift

	switch seg {
	case 0:
		t += 8
	case 1:
		t += 0x108
	default:
		t += 0x108
		t <<= seg - 1
	}

	if a&0x80 != 0 {
		return int16(t)
	}

	return int16(-t)
}

type flacSource struct {
	stream  *flac.Stream
	pending []int16
	shift   int
	ch      int
}

func openFLAC(data []byte) (audioSource, error) {
	stream, err := flac.New(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errDecode, err)
	}

	info := stream.Info
	if info.SampleRate == 0 || info.NChannels == 0 || info.BitsPerSample == 0 {
		return nil, fmt.Errorf("%w: empty FLAC stream info", errDecode)
	}

	return &flacSource{stream: stream, ch: int(info.NChannels), shift: int(info.BitsPerSample) - pcmBits}, nil
}

func (s *flacSource) rate() int     { return int(s.stream.Info.SampleRate) }
func (s *flacSource) channels() int { return s.ch }

func (s *flacSource) frames() int64 {
	if s.stream.Info.NSamples == 0 || s.stream.Info.NSamples > math.MaxInt64 {
		return -1
	}

	return int64(s.stream.Info.NSamples)
}

func (s *flacSource) read(dst []int16) (int, error) {
	if len(s.pending) == 0 {
		pcm, err := s.nextFrame()
		if err != nil {
			return 0, err
		}

		s.pending = pcm
	}

	n := min(len(dst)/s.ch, len(s.pending)/s.ch)
	copy(dst, s.pending[:n*s.ch])
	s.pending = s.pending[n*s.ch:]

	return n, nil
}

func (s *flacSource) nextFrame() ([]int16, error) {
	f, err := s.stream.ParseNext()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}

		return nil, fmt.Errorf("%w: %w", errDecode, err)
	}

	if len(f.Subframes) != s.ch {
		return nil, fmt.Errorf("%w: FLAC frame has %d channels, want %d", errDecode, len(f.Subframes), s.ch)
	}

	for _, sf := range f.Subframes {
		if len(sf.Samples) != f.Subframes[0].NSamples {
			return nil, fmt.Errorf("%w: FLAC subframe length mismatch", errDecode)
		}
	}

	return s.interleave(f), nil
}

func (s *flacSource) interleave(f *frame.Frame) []int16 {
	n := f.Subframes[0].NSamples
	out := make([]int16, 0, n*s.ch)

	for i := range n {
		for c := range s.ch {
			v := f.Subframes[c].Samples[i]
			if s.shift >= 0 {
				v >>= s.shift
			} else {
				v <<= -s.shift
			}

			out = append(out, int16(v)) //nolint:gosec // shifted to 16-bit range
		}
	}

	return out
}

type mp3Source struct {
	dec  *mp3.Decoder
	raw  []byte
	mono bool
}

func openMP3(data []byte) (audioSource, error) {
	dec, err := mp3.NewDecoder(struct{ io.Reader }{bytes.NewReader(data)})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errDecode, err)
	}

	return &mp3Source{dec: dec, mono: mp3IsMono(data)}, nil
}

func (s *mp3Source) rate() int { return s.dec.SampleRate() }

func (s *mp3Source) channels() int {
	if s.mono {
		return 1
	}

	return dualChannel
}

func (s *mp3Source) frames() int64 { return -1 }

func (s *mp3Source) read(dst []int16) (int, error) {
	ch := s.channels()
	want := len(dst) / ch

	if cap(s.raw) < want*mp3FrameBytes {
		s.raw = make([]byte, want*mp3FrameBytes)
	}

	raw := s.raw[:want*mp3FrameBytes]
	got, err := io.ReadFull(s.dec, raw)
	n := got / mp3FrameBytes

	for i := range n {
		for c := range ch {
			off := i*mp3FrameBytes + c*pcmSampleBytes
			dst[i*ch+c] = int16(binary.LittleEndian.Uint16(raw[off:])) //nolint:gosec // PCM reinterpretation
		}
	}

	if errors.Is(err, io.ErrUnexpectedEOF) {
		err = io.EOF
	}

	if err != nil && !errors.Is(err, io.EOF) {
		return n, fmt.Errorf("%w: %w", errDecode, err)
	}

	return n, err
}

// mp3IsMono reads the channel mode of the first frame header; go-mp3 always emits stereo.
func mp3IsMono(data []byte) bool {
	const (
		monoMode   = 3
		modeShift  = 6
		headerLen  = 4
		layerMask  = 0x06
		layerNone  = 0
		versionBad = 0x18
	)

	pos := 0

	if len(data) >= id3v2HeaderLen && bytes.HasPrefix(data, []byte("ID3")) {
		size := 0
		for _, b := range data[6:id3v2HeaderLen] {
			size = size<<syncsafeBits | int(b&syncsafeMask)
		}

		pos = id3v2HeaderLen + size
	}

	end := min(len(data)-headerLen, pos+mp3ScanLimit)

	for i := pos; i <= end; i++ {
		if data[i] == 0xFF && data[i+1]&0xE0 == 0xE0 && data[i+1]&layerMask != layerNone &&
			data[i+1]&versionBad != 0x08 {
			return data[i+3]>>modeShift == monoMode
		}
	}

	return false
}

type oggSource struct {
	rd     *oggvorbis.Reader
	buf    []float32
	length int64
}

func openOGG(data []byte) (audioSource, error) {
	rd, err := oggvorbis.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errDecode, err)
	}

	if rd.Channels() <= 0 || rd.SampleRate() <= 0 {
		return nil, fmt.Errorf("%w: empty Vorbis stream", errDecode)
	}

	return &oggSource{rd: rd, length: rd.Length()}, nil
}

func (s *oggSource) rate() int     { return s.rd.SampleRate() }
func (s *oggSource) channels() int { return s.rd.Channels() }

func (s *oggSource) frames() int64 {
	if s.length <= 0 {
		return -1
	}

	return s.length
}

func (s *oggSource) read(dst []int16) (int, error) {
	if cap(s.buf) < len(dst) {
		s.buf = make([]float32, len(dst))
	}

	n, err := s.rd.Read(s.buf[:len(dst)])
	for i := range n {
		dst[i] = clampInt16(float64(s.buf[i]) * math.MaxInt16)
	}

	if err != nil && !errors.Is(err, io.EOF) {
		return n / s.channels(), fmt.Errorf("%w: %w", errDecode, err)
	}

	return n / s.channels(), err
}
