package transcribe

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"os"
	"sync"
	"testing"

	"github.com/mewkiz/flac"
	"github.com/mewkiz/flac/frame"
	"github.com/mewkiz/flac/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const toneHz = 440

func toneSamples(rate int, seconds float64) []float64 {
	n := int(float64(rate) * seconds)
	out := make([]float64, n)

	for i := range n {
		out[i] = math.Sin(2 * math.Pi * toneHz * float64(i) / float64(rate))
	}

	return out
}

func pcmInt16(b []byte) []int16 {
	out := make([]int16, len(b)/2)
	for i := range out {
		out[i] = int16(binary.LittleEndian.Uint16(b[i*2:]))
	}

	return out
}

// goertzel returns the signal power at hz relative to the total power (1 for a pure tone at hz).
func goertzel(samples []int16) float64 {
	w := 2 * math.Pi * toneHz / float64(engineSampleRate)
	coeff := 2 * math.Cos(w)

	var s1, s2, total float64

	for _, v := range samples {
		x := float64(v)
		s0 := x + coeff*s1 - s2
		s2, s1 = s1, s0
		total += x * x
	}

	if total == 0 {
		return 0
	}

	power := s1*s1 + s2*s2 - coeff*s1*s2

	return 2 * power / (float64(len(samples)) * total)
}

type wavSpec struct {
	tag, bits, channels, rate int
	extensible                bool
}

func buildWAV(spec wavSpec, tone []float64) []byte {
	width := spec.bits / 8
	data := make([]byte, 0, len(tone)*spec.channels*width)

	for _, v := range tone {
		for range spec.channels {
			switch {
			case spec.tag == 3 && spec.bits == 32:
				data = binary.LittleEndian.AppendUint32(data, math.Float32bits(float32(v*0.5)))
			case spec.tag == 3:
				data = binary.LittleEndian.AppendUint64(data, math.Float64bits(v*0.5))
			case spec.tag == 6:
				data = append(data, encodeG711(int16(v*16000), true))
			case spec.tag == 7:
				data = append(data, encodeG711(int16(v*16000), false))
			case spec.bits == 8:
				data = append(data, byte(int(v*60)+128))
			default:
				x := int64(v * 0.5 * float64(int64(1)<<(spec.bits-1)))
				for i := range width {
					data = append(data, byte(x>>(8*i)))
				}
			}
		}
	}

	fmtLen := 16
	if spec.extensible {
		fmtLen = 40
	}

	tag := spec.tag
	if spec.extensible {
		tag = wavFormatExtensible
	}

	out := []byte("RIFF")
	out = binary.LittleEndian.AppendUint32(out, uint32(4+8+fmtLen+8+len(data)))
	out = append(out, "WAVEfmt "...)
	out = binary.LittleEndian.AppendUint32(out, uint32(fmtLen))
	out = binary.LittleEndian.AppendUint16(out, uint16(tag))
	out = binary.LittleEndian.AppendUint16(out, uint16(spec.channels))
	out = binary.LittleEndian.AppendUint32(out, uint32(spec.rate))
	out = binary.LittleEndian.AppendUint32(out, uint32(spec.rate*spec.channels*width))
	out = binary.LittleEndian.AppendUint16(out, uint16(spec.channels*width))
	out = binary.LittleEndian.AppendUint16(out, uint16(spec.bits))

	if spec.extensible {
		out = binary.LittleEndian.AppendUint16(out, 22)
		out = binary.LittleEndian.AppendUint16(out, uint16(spec.bits))
		out = binary.LittleEndian.AppendUint32(out, 0)
		out = binary.LittleEndian.AppendUint16(out, uint16(spec.tag))
		out = append(out, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x80, 0x00, 0x00, 0xAA, 0x00, 0x38, 0x9B, 0x71)
	}

	out = append(out, "data"...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(data)))

	return append(out, data...)
}

// encodeG711 is the reference G.711 encoder, used only to build fixtures.
func encodeG711(pcm int16, alaw bool) byte {
	best, bestErr := byte(0), math.MaxInt

	for c := range 256 {
		var got int16
		if alaw {
			got = decodeALaw(byte(c))
		} else {
			got = decodeMuLaw(byte(c))
		}

		if d := int(math.Abs(float64(int(got) - int(pcm)))); d < bestErr {
			best, bestErr = byte(c), d
		}
	}

	return best
}

func decodeBytes(t *testing.T, media []byte, format string, mono bool) (audioSource, decodedAudio) {
	t.Helper()

	src, err := openAudio(format, media)
	require.NoError(t, err)

	audio, err := decodeAudio(src, mono, maxAudioSeconds)
	require.NoError(t, err)

	return src, audio
}

func TestDecodeWAVVariants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		spec wavSpec
	}{
		{name: "pcm8", spec: wavSpec{tag: 1, bits: 8, channels: 1, rate: 16000}},
		{name: "pcm16", spec: wavSpec{tag: 1, bits: 16, channels: 1, rate: 16000}},
		{name: "pcm24", spec: wavSpec{tag: 1, bits: 24, channels: 1, rate: 16000}},
		{name: "pcm32", spec: wavSpec{tag: 1, bits: 32, channels: 1, rate: 16000}},
		{name: "float32", spec: wavSpec{tag: 3, bits: 32, channels: 1, rate: 16000}},
		{name: "float64", spec: wavSpec{tag: 3, bits: 64, channels: 1, rate: 16000}},
		{name: "alaw", spec: wavSpec{tag: 6, bits: 8, channels: 1, rate: 16000}},
		{name: "mulaw", spec: wavSpec{tag: 7, bits: 8, channels: 1, rate: 16000}},
		{name: "extensible pcm24", spec: wavSpec{tag: 1, bits: 24, channels: 1, rate: 16000, extensible: true}},
		{name: "extensible float32", spec: wavSpec{tag: 3, bits: 32, channels: 1, rate: 16000, extensible: true}},
		{name: "stereo downmix", spec: wavSpec{tag: 1, bits: 16, channels: 2, rate: 16000}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			media := buildWAV(tt.spec, toneSamples(tt.spec.rate, 0.5))
			src, audio := decodeBytes(t, media, mediaWAV, true)

			require.Len(t, audio.channels, 1)
			assert.Equal(t, tt.spec.channels, src.channels())
			assert.Equal(t, 500, audio.millis)

			samples := pcmInt16(audio.channels[0])
			assert.Len(t, samples, 8000)
			assert.Greater(t, goertzel(samples), 0.9)

			peak := 0.0
			for _, v := range samples {
				peak = math.Max(peak, math.Abs(float64(v)))
			}

			assert.Greater(t, peak, 1000.0)
		})
	}
}

func TestDecodeWAVRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		spec wavSpec
	}{
		{name: "adpcm", spec: wavSpec{tag: 2, bits: 4, channels: 1, rate: 16000}},
		{name: "pcm12", spec: wavSpec{tag: 1, bits: 12, channels: 1, rate: 16000}},
		{name: "alaw16", spec: wavSpec{tag: 6, bits: 16, channels: 1, rate: 16000}},
		{name: "zero rate", spec: wavSpec{tag: 1, bits: 16, channels: 1, rate: 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := openAudio(mediaWAV, buildWAV(tt.spec, toneSamples(16000, 0.01)))
			require.ErrorIs(t, err, errDecode)
		})
	}
}

func TestResamplerRates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		rate int
	}{
		{name: "8k up", rate: 8000},
		{name: "11k up", rate: 11025},
		{name: "22k down", rate: 22050},
		{name: "44k down", rate: 44100},
		{name: "48k down", rate: 48000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			media := buildWAV(wavSpec{tag: 1, bits: 16, channels: 1, rate: tt.rate}, toneSamples(tt.rate, 1))
			_, audio := decodeBytes(t, media, mediaWAV, true)
			samples := pcmInt16(audio.channels[0])

			assert.InDelta(t, engineSampleRate, len(samples), 2)
			assert.Greater(t, goertzel(samples), 0.9)
		})
	}
}

func TestDecodeKeepsChannelsSeparate(t *testing.T) {
	t.Parallel()

	const rate = 16000

	left := toneSamples(rate, 0.25)
	data := make([]byte, 0, len(left)*4)

	for _, v := range left {
		data = binary.LittleEndian.AppendUint16(data, uint16(int16(v*8000)))
		data = binary.LittleEndian.AppendUint16(data, 0)
	}

	stereo := buildWAV(wavSpec{tag: 1, bits: 16, channels: 2, rate: rate}, nil)
	stereo = append(stereo[:len(stereo)-4], binary.LittleEndian.AppendUint32(nil, uint32(len(data)))...)
	stereo = append(stereo, data...)

	_, audio := decodeBytes(t, stereo, mediaWAV, false)
	require.Len(t, audio.channels, 2)
	assert.Greater(t, goertzel(pcmInt16(audio.channels[0])), 0.9)

	for _, v := range pcmInt16(audio.channels[1]) {
		require.Zero(t, v)
	}
}

func encodeFLAC(t *testing.T, rate, bits int, channels [][]int32) []byte {
	t.Helper()

	n := len(channels[0])
	info := &meta.StreamInfo{
		BlockSizeMin: uint16(n), BlockSizeMax: uint16(n), SampleRate: uint32(rate),
		NChannels: uint8(len(channels)), BitsPerSample: uint8(bits), NSamples: uint64(n),
	}

	var buf bytes.Buffer

	enc, err := flac.NewEncoder(&buf, info)
	require.NoError(t, err)

	layout := frame.ChannelsMono

	switch len(channels) {
	case 2:
		layout = frame.ChannelsLR
	case 3:
		layout = frame.ChannelsLRC
	}

	f := &frame.Frame{
		HasFixedBlockSize: true, BlockSize: uint16(n), SampleRate: uint32(rate), Channels: layout,
		BitsPerSample: uint8(bits)}

	for _, c := range channels {
		f.Subframes = append(f.Subframes, &frame.Subframe{
			Pred: frame.PredVerbatim, Samples: c, NSamples: n,
		})
	}

	require.NoError(t, enc.WriteFrame(f))
	require.NoError(t, enc.Close())

	return buf.Bytes()
}

func intTone(rate int, seconds float64, bits int) []int32 {
	tone := toneSamples(rate, seconds)
	out := make([]int32, len(tone))

	for i, v := range tone {
		out[i] = int32(v * 0.5 * float64(int64(1)<<(bits-1)))
	}

	return out
}

func TestDecodeFLAC(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rate     int
		bits     int
		channels int
		samples  int
	}{
		{name: "mono16", rate: 16000, bits: 16, channels: 1, samples: 8000},
		{name: "mono24", rate: 16000, bits: 24, channels: 1, samples: 8000},
		{name: "stereo16 48k", rate: 48000, bits: 16, channels: 2, samples: 8000},
		{name: "mono8 8k", rate: 8000, bits: 8, channels: 1, samples: 8000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			chans := make([][]int32, tt.channels)
			for c := range chans {
				chans[c] = intTone(tt.rate, 0.5, tt.bits)
			}

			media := encodeFLAC(t, tt.rate, tt.bits, chans)
			require.Equal(t, mediaFLAC, sniffMediaFormat(media))

			src, audio := decodeBytes(t, media, mediaFLAC, true)
			assert.Equal(t, tt.rate, src.rate())
			assert.Equal(t, tt.channels, src.channels())
			assert.Equal(t, int64(tt.rate/2), src.frames())
			assert.Equal(t, 500, audio.millis)

			samples := pcmInt16(audio.channels[0])
			assert.InDelta(t, tt.samples, len(samples), 2)
			assert.Greater(t, goertzel(samples), 0.9)
		})
	}
}

func silentMP3(frames int, mono bool) []byte {
	const (
		frameLen     = 417
		monoSideInfo = 17
	)

	header := []byte{0xFF, 0xFB, 0x90, 0x00}
	if mono {
		header[3] = 0xC0
	}

	out := make([]byte, 0, frames*frameLen)

	for range frames {
		f := make([]byte, frameLen)
		copy(f, header)
		out = append(out, f...)
	}

	return out
}

func TestDecodeMP3(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mono     bool
		channels int
	}{
		{name: "stereo", channels: 2},
		{name: "mono", mono: true, channels: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			const frames = 40

			media := silentMP3(frames, tt.mono)
			require.Equal(t, mediaMP3, sniffMediaFormat(media))

			src, audio := decodeBytes(t, media, mediaMP3, false)
			assert.Equal(t, 44100, src.rate())
			assert.Equal(t, tt.channels, src.channels())
			require.Len(t, audio.channels, tt.channels)

			want := frames * 1152 * engineSampleRate / 44100
			assert.InDelta(t, want, len(audio.channels[0])/2, float64(want)/10)
			assert.Greater(t, audio.millis, 900)
		})
	}
}

// testdata/vorbis.ogg is testdata/test.ogg from github.com/jfreymuth/oggvorbis v1.0.5 (MIT),
// copied from the module cache; no pure-Go Vorbis encoder or ogg tool exists on the build host.
func TestDecodeOggVorbis(t *testing.T) {
	t.Parallel()

	media, err := os.ReadFile("testdata/vorbis.ogg")
	require.NoError(t, err)
	require.Equal(t, mediaOGG, sniffMediaFormat(media))

	src, audio := decodeBytes(t, media, mediaOGG, true)
	require.Positive(t, src.rate())
	require.Positive(t, src.frames())

	want := int(src.frames() * engineSampleRate / int64(src.rate()))
	require.Len(t, audio.channels, 1)

	samples := pcmInt16(audio.channels[0])
	assert.InDelta(t, want, len(samples), float64(want)/20+2)

	var energy float64
	for _, v := range samples {
		energy += float64(v) * float64(v)
	}

	assert.Positive(t, energy)
}

func TestDecodeCorruptInputs(t *testing.T) {
	t.Parallel()

	flacOK := encodeFLAC(t, 16000, 16, [][]int32{intTone(16000, 0.5, 16)})
	oggOK, err := os.ReadFile("testdata/vorbis.ogg")
	require.NoError(t, err)

	garbage := bytes.Repeat([]byte{0xA5, 0x5A, 0x00, 0xFF}, 512)

	tests := []struct {
		name       string
		format     string
		data       []byte
		failOnOpen bool
	}{
		{name: "flac truncated mid-frame", format: mediaFLAC, data: flacOK[:len(flacOK)-2000]},
		{name: "flac magic only", format: mediaFLAC, data: []byte("fLaC"), failOnOpen: true},
		{name: "flac garbage", format: mediaFLAC, data: append([]byte("fLaC"), garbage...), failOnOpen: true},
		{name: "ogg magic only", format: mediaOGG, data: []byte("OggS"), failOnOpen: true},
		{name: "ogg garbage", format: mediaOGG, data: append([]byte("OggS"), garbage...), failOnOpen: true},
		{name: "ogg truncated headers", format: mediaOGG, data: oggOK[:200], failOnOpen: true},
		{name: "mp3 garbage", format: mediaMP3, data: append([]byte{0xFF, 0xFB}, garbage...), failOnOpen: true},
		{name: "mp3 id3 only", format: mediaMP3, data: []byte("ID3\x04\x00\x00\x00\x00\x00\x00"), failOnOpen: true},
		{name: "wav empty data", format: mediaWAV, data: []byte("RIFF\x00\x00\x00\x00WAVE"), failOnOpen: true},
		{
			name: "mp4 no decoder", format: mediaMP4, failOnOpen: true,
			data: []byte("\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src, openErr := openAudio(tt.format, tt.data)
			if tt.failOnOpen {
				require.Error(t, openErr)

				return
			}

			require.NoError(t, openErr)

			_, decErr := decodeAudio(src, true, maxAudioSeconds)
			require.ErrorIs(t, decErr, errDecode)
		})
	}
}

type panicSource struct{}

func (panicSource) rate() int                 { return engineSampleRate }
func (panicSource) channels() int             { return 1 }
func (panicSource) frames() int64             { return -1 }
func (panicSource) read([]int16) (int, error) { panic("decoder exploded") }

type endlessSource struct{ rateHz int }

func (e endlessSource) rate() int   { return e.rateHz }
func (endlessSource) channels() int { return 1 }
func (endlessSource) frames() int64 { return -1 }
func (endlessSource) read(dst []int16) (int, error) {
	clear(dst)

	return len(dst), nil
}

func TestDecodeAudioBoundaries(t *testing.T) {
	t.Parallel()

	t.Run("panic becomes error", func(t *testing.T) {
		t.Parallel()

		_, err := decodeAudio(panicSource{}, true, maxAudioSeconds)
		require.ErrorIs(t, err, errDecode)
		assert.Contains(t, err.Error(), "decoder exploded")
	})

	t.Run("duration cap", func(t *testing.T) {
		t.Parallel()

		_, err := decodeAudio(endlessSource{rateHz: 8000}, true, 2)
		require.ErrorIs(t, err, errAudioLength)
	})

	t.Run("exactly at cap", func(t *testing.T) {
		t.Parallel()

		media := buildWAV(wavSpec{tag: 1, bits: 16, channels: 1, rate: 8000}, toneSamples(8000, 2))
		src, err := openAudio(mediaWAV, media)
		require.NoError(t, err)

		audio, err := decodeAudio(src, true, 2)
		require.NoError(t, err)
		assert.Equal(t, 2000, audio.millis)
	})

	t.Run("limit is four hours", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, 14400, maxAudioSeconds)
	})
}

type recordingEngine struct {
	rates []int
	sizes []int
	mu    sync.Mutex
}

func (*recordingEngine) SupportsLanguage(string) bool { return true }

func (e *recordingEngine) Recognize(_ context.Context, _ string, rate int, pcm []byte) ([]Word, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.rates = append(e.rates, rate)
	e.sizes = append(e.sizes, len(pcm))

	return []Word{{Text: "hi", Start: 0.1, End: 0.2, Confidence: 1}}, nil
}

func TestRunPipelineDecodesContainers(t *testing.T) {
	t.Parallel()

	oggOK, err := os.ReadFile("testdata/vorbis.ogg")
	require.NoError(t, err)

	flac16 := encodeFLAC(t, 16000, 16, [][]int32{intTone(16000, 0.5, 16)})
	flacStereo := encodeFLAC(t, 16000, 16, [][]int32{intTone(16000, 0.5, 16), intTone(16000, 0.5, 16)})
	short := intTone(16000, 0.1, 16)
	flac3ch := encodeFLAC(t, 16000, 16, [][]int32{short, short, short})
	wav24 := buildWAV(wavSpec{tag: 1, bits: 24, channels: 1, rate: 44100}, toneSamples(44100, 0.5))
	truncated := flac16[:len(flac16)-2000]

	tests := []struct {
		name       string
		media      []byte
		format     string
		kind       jobKind
		wantFail   string
		wantSizes  []int
		sampleRate int32
	}{
		{name: "flac", media: flac16, format: "flac", kind: kindTranscription, wantSizes: []int{8000}},
		{
			name: "flac stereo mono mix", media: flacStereo, format: "flac", kind: kindTranscription,
			wantSizes: []int{8000},
		},
		{
			name: "flac call analytics keeps channels", media: flacStereo, format: "flac", kind: kindCallAnalytics,
			wantSizes: []int{8000, 8000},
		},
		{name: "wav24 resampled", media: wav24, format: "wav", kind: kindTranscription, wantSizes: []int{8000}},
		{name: "ogg", media: oggOK, format: "ogg", kind: kindTranscription},
		{
			name:     "ogg format mismatch",
			media:    oggOK,
			format:   "mp3",
			kind:     kindTranscription,
			wantFail: failFormatMismatch,
		},
		{
			name: "flac rate mismatch", media: flac16, format: "flac", kind: kindTranscription,
			sampleRate: 44100, wantFail: failSampleRate,
		},
		{
			name:       "flac rate match",
			media:      flac16,
			format:     "flac",
			kind:       kindTranscription,
			sampleRate: 16000,
			wantSizes:  []int{8000},
		},
		{name: "flac three channels", media: flac3ch, format: "flac", kind: kindTranscription, wantFail: failChannels},
		{
			name:     "flac truncated",
			media:    truncated,
			format:   "flac",
			kind:     kindTranscription,
			wantFail: failUnsupportedFormat,
		},
		{
			name: "mp4 undecodable", media: []byte("\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00"), format: "mp4",
			kind: kindTranscription, wantFail: "cannot decode mp4 media",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			eng := &recordingEngine{}
			deps := pipelineDeps{reader: filterMedia{"b/a": tt.media}, engine: eng, accountID: "123456789012"}
			spec := jobSpec{
				Kind: tt.kind, Name: "j", Language: "en-US", MediaURI: "s3://b/a", MediaFormat: tt.format,
				SampleRate: tt.sampleRate,
			}

			out := runPipeline(t.Context(), deps, spec)

			if tt.wantFail != "" {
				assert.Contains(t, out.failure, tt.wantFail)
				assert.Empty(t, eng.sizes)

				return
			}

			require.Empty(t, out.failure)
			require.NotEmpty(t, out.files)

			for _, r := range eng.rates {
				assert.Equal(t, engineSampleRate, r)
			}

			if tt.wantSizes != nil {
				require.Len(t, eng.sizes, len(tt.wantSizes))

				for i, n := range tt.wantSizes {
					assert.InDelta(t, n, eng.sizes[i]/2, 2)
				}
			}
		})
	}
}

func TestRunPipelineWithoutEngineValidatesHeaders(t *testing.T) {
	t.Parallel()

	flac16 := encodeFLAC(t, 16000, 16, [][]int32{intTone(16000, 0.5, 16)})

	tests := []struct {
		name     string
		wantFail string
		media    []byte
	}{
		{name: "valid flac", media: flac16},
		{name: "corrupt flac header", media: append([]byte("fLaC"), 0xFF, 0xFF), wantFail: failUnsupportedFormat},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := pipelineDeps{reader: filterMedia{"b/a": tt.media}, accountID: "1"}
			spec := jobSpec{
				Kind: kindTranscription, Name: "j", Language: "en-US", MediaURI: "s3://b/a", MediaFormat: "flac",
			}
			out := runPipeline(t.Context(), deps, spec)

			if tt.wantFail != "" {
				assert.Equal(t, tt.wantFail, out.failure)

				return
			}

			assert.Empty(t, out.failure)
		})
	}
}
