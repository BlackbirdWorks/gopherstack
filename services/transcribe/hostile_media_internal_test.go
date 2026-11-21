package transcribe

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingSource struct {
	served *int64
	rateHz int
}

func (c countingSource) rate() int   { return c.rateHz }
func (countingSource) channels() int { return 1 }
func (countingSource) frames() int64 { return -1 }
func (c countingSource) read(dst []int16) (int, error) {
	clear(dst)

	*c.served += int64(len(dst))

	return len(dst), nil
}

type stalledSource struct{}

func (stalledSource) rate() int                 { return engineSampleRate }
func (stalledSource) channels() int             { return 1 }
func (stalledSource) frames() int64             { return -1 }
func (stalledSource) read([]int16) (int, error) { return 0, nil }

func TestDecodeAudioBoundsOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		rateHz int
	}{
		{name: "one_hz_upsamples_16000x", rateHz: 1},
		{name: "native_rate", rateHz: engineSampleRate},
		{name: "high_rate", rateHz: 192000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var served int64

			_, err := decodeAudio(
				t.Context(),
				countingSource{served: &served, rateHz: tt.rateHz},
				true,
				maxAudioSeconds,
			)
			require.ErrorIs(t, err, errAudioLength)

			producedBytes := served * engineSampleRate / int64(tt.rateHz) * pcmSampleBytes
			assert.LessOrEqual(
				t,
				producedBytes,
				int64(maxDecodedBytes)+decodeChunkFrames*engineSampleRate*pcmSampleBytes,
			)
		})
	}
}

func TestDecodeAudioStops(t *testing.T) {
	t.Parallel()

	t.Run("cancelled_context", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		var served int64

		_, err := decodeAudio(ctx, countingSource{served: &served, rateHz: engineSampleRate}, true, maxAudioSeconds)
		require.ErrorIs(t, err, context.Canceled)
		assert.Zero(t, served)
	})

	t.Run("no_progress", func(t *testing.T) {
		t.Parallel()

		_, err := decodeAudio(t.Context(), stalledSource{}, true, maxAudioSeconds)
		require.ErrorIs(t, err, errDecode)
	})
}

func wavWithFmt(channels, rate, bits int, dataSize uint32, data []byte) []byte {
	out := []byte("RIFF")
	out = binary.LittleEndian.AppendUint32(out, 0xFFFFFFFF)
	out = append(out, "WAVEfmt "...)
	out = binary.LittleEndian.AppendUint32(out, 16)
	out = binary.LittleEndian.AppendUint16(out, 1)
	out = binary.LittleEndian.AppendUint16(out, uint16(channels))
	out = binary.LittleEndian.AppendUint32(out, uint32(rate))
	out = binary.LittleEndian.AppendUint32(out, 0)
	out = binary.LittleEndian.AppendUint16(out, 0)
	out = binary.LittleEndian.AppendUint16(out, uint16(bits))
	out = append(out, "data"...)
	out = binary.LittleEndian.AppendUint32(out, dataSize)

	return append(out, data...)
}

func TestHostileWAVHeaders(t *testing.T) {
	t.Parallel()

	body := make([]byte, 64)

	tests := []struct {
		name string
		wav  []byte
	}{
		{name: "data_size_4gib", wav: wavWithFmt(1, 8000, 16, 0xFFFFFFFF, body)},
		{name: "zero_channels", wav: wavWithFmt(0, 8000, 16, 64, body)},
		{name: "zero_rate", wav: wavWithFmt(1, 0, 16, 64, body)},
		{name: "zero_bits", wav: wavWithFmt(1, 8000, 0, 64, body)},
		{name: "65535_channels", wav: wavWithFmt(65535, 8000, 16, 64, body)},
		{name: "rate_4g", wav: wavWithFmt(1, 0xFFFFFFFF, 16, 64, body)},
		{name: "rate_1", wav: wavWithFmt(1, 1, 16, 64, body)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src, err := openAudio(mediaWAV, tt.wav)
			if err != nil {
				require.ErrorIs(t, err, errDecode)

				return
			}

			if src.channels() > dualChannel {
				_, err = decodeAudio(t.Context(), src, false, maxAudioSeconds)
				require.ErrorIs(t, err, errDecode)

				return
			}

			_, err = decodeAudio(t.Context(), src, true, maxAudioSeconds)
			require.NoError(t, err)
		})
	}
}

func TestFloatAndPCMConversion(t *testing.T) {
	t.Parallel()

	f32 := func(vs ...float32) []byte {
		var b []byte
		for _, v := range vs {
			b = binary.LittleEndian.AppendUint32(b, math.Float32bits(v))
		}

		return b
	}
	f64 := func(vs ...float64) []byte {
		var b []byte
		for _, v := range vs {
			b = binary.LittleEndian.AppendUint64(b, math.Float64bits(v))
		}

		return b
	}

	tests := []struct {
		name  string
		in    []byte
		want  []int16
		codec int
		width int
	}{
		{
			name: "float32_specials", codec: wavFormatFloat, width: 4,
			in: f32(
				float32(math.NaN()), float32(math.Inf(1)), float32(math.Inf(-1)), 1e30, -1e30, 1, -1, 0,
			),
			want: []int16{0, 32767, -32768, 32767, -32768, 32767, -32767, 0},
		},
		{
			name: "float64_specials", codec: wavFormatFloat, width: 8,
			in:   f64(math.NaN(), math.Inf(1), math.Inf(-1), 2, 0.5),
			want: []int16{0, 32767, -32768, 32767, 16384},
		},
		{
			name: "pcm8_bias", codec: wavFormatPCM, width: 1,
			in: []byte{0, 128, 255}, want: []int16{-32768, 0, 32512},
		},
		{
			name: "pcm24_sign", codec: wavFormatPCM, width: 3,
			in:   []byte{0x00, 0x00, 0x80, 0xFF, 0xFF, 0x7F, 0xFF, 0xFF, 0xFF},
			want: []int16{-32768, 32767, -1},
		},
		{
			name: "pcm32_extremes", codec: wavFormatPCM, width: 4,
			in:   []byte{0, 0, 0, 0x80, 0xFF, 0xFF, 0xFF, 0x7F},
			want: []int16{-32768, 32767},
		},
		{
			name: "mulaw_reference", codec: wavFormatMuLaw, width: 1,
			in: []byte{0xFF, 0x7F, 0x00, 0x80}, want: []int16{0, 0, -32124, 32124},
		},
		{
			name: "alaw_reference", codec: wavFormatALaw, width: 1,
			in: []byte{0xD5, 0x55, 0xAA, 0x2A}, want: []int16{8, -8, 32256, -32256},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := &wavSource{codec: tt.codec, width: tt.width}
			got := make([]int16, 0, len(tt.want))

			for off := 0; off < len(tt.in); off += tt.width {
				got = append(got, s.sample(tt.in[off:off+tt.width]))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestResamplerMatchesReference(t *testing.T) {
	t.Parallel()

	const frames = 96000 * 2

	rates := []int{8000, 11025, 16000, 22050, 32000, 44100, 48000, 96000}
	chunks := []int{1, 7, decodeChunkFrames, 100000}

	input := make([]int16, frames)
	for i := range input {
		input[i] = int16(20000 * math.Sin(float64(i)*0.01) * math.Cos(float64(i)*0.0007))
	}

	for _, rate := range rates {
		for _, chunk := range chunks {
			t.Run(fmt.Sprintf("%d_hz_chunk_%d", rate, chunk), func(t *testing.T) {
				t.Parallel()

				n := frames * rate / 96000
				in := input[:n]
				want := referenceResample(in, rate)

				r := newResampler(rate, engineSampleRate)

				var out []byte

				for off := 0; off < len(in); off += chunk {
					for _, v := range in[off:min(off+chunk, len(in))] {
						out = r.push(v, out)
					}
				}

				got := pcmInt16(out)
				require.InDelta(t, len(want), len(got), 1, "rate %d chunk %d", rate, chunk)

				for i := range min(len(want), len(got)) {
					require.InDelta(t, want[i], got[i], 1, "rate %d sample %d", rate, i)
				}
			})
		}
	}
}

func referenceResample(in []int16, rate int) []int16 {
	step := float64(rate) / engineSampleRate
	prefix := make([]float64, len(in)+1)

	for i, v := range in {
		prefix[i+1] = prefix[i] + float64(v)
	}

	area := func(x float64) float64 {
		i := int(x)
		if i >= len(in) {
			return prefix[len(in)]
		}

		return prefix[i] + float64(in[i])*(x-float64(i))
	}

	var out []int16

	if step >= 1 {
		for j := 0; float64(j+1)*step <= float64(len(in))+1e-6; j++ {
			out = append(out, clampInt16((area(float64(j+1)*step)-area(float64(j)*step))/step))
		}

		return out
	}

	for j := 0; ; j++ {
		t := float64(j) * step
		i := int(t)

		if i+1 > len(in)-1 {
			return out
		}

		out = append(out, clampInt16(float64(in[i])+(float64(in[i+1])-float64(in[i]))*(t-float64(i))))
	}
}

//nolint:paralleltest // measures process-wide allocation
func TestMP3HostileHeaders(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{name: "id3_declares_256mib", data: append([]byte("ID3\x04\x00\x00\x7f\x7f\x7f\x7f"), make([]byte, 32)...)},
		{name: "free_format_frame", data: append([]byte{0xFF, 0xFB, 0x00, 0x40}, make([]byte, 4096)...)},
		{name: "all_ones", data: func() []byte {
			b := make([]byte, 4096)
			for i := range b {
				b[i] = 0xFF
			}

			return b
		}()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { //nolint:paralleltest // measures process-wide allocation
			var before, after runtime.MemStats

			runtime.GC()
			runtime.ReadMemStats(&before)

			src, err := openAudio(mediaMP3, tt.data)
			if err == nil {
				_, err = decodeAudio(t.Context(), src, true, maxAudioSeconds)
			}

			runtime.ReadMemStats(&after)

			if err != nil {
				require.ErrorIs(t, err, errDecode)
			}

			assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(8<<20))
		})
	}
}

func patchFLACInfo(data []byte, rate, channels, bits, samples uint64) []byte {
	out := append([]byte(nil), data...)
	word := rate<<44 | (channels-1)<<41 | (bits-1)<<36 | samples
	binary.BigEndian.PutUint64(out[18:26], word)

	return out
}

func TestHostileFLACHeaders(t *testing.T) {
	t.Parallel()

	base := encodeFLAC(t, 16000, 16, [][]int32{intTone(16000, 0.2, 16)})

	tests := []struct {
		name  string
		media []byte
	}{
		{name: "samples_2_36", media: patchFLACInfo(base, 16000, 1, 16, 1<<36-1)},
		{name: "eight_channels", media: patchFLACInfo(base, 16000, 8, 16, 3200)},
		{name: "bits_32", media: patchFLACInfo(base, 16000, 1, 32, 3200)},
		{name: "bits_4", media: patchFLACInfo(base, 16000, 1, 4, 3200)},
		{name: "rate_max", media: patchFLACInfo(base, 1<<20-1, 1, 16, 3200)},
		{name: "rate_zero", media: patchFLACInfo(base, 0, 1, 16, 3200)},
		{name: "truncated_midframe", media: base[:len(base)/2]},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src, err := openAudio(mediaFLAC, tt.media)
			if err != nil {
				require.ErrorIs(t, err, errDecode)

				return
			}

			if src.channels() > dualChannel {
				return
			}

			_, err = decodeAudio(t.Context(), src, true, maxAudioSeconds)
			if err != nil {
				require.ErrorIs(t, err, errDecode)
			}
		})
	}
}
