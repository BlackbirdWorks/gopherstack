package transcribe

import (
	"encoding/binary"
	"os"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func oggPages(packets ...[]byte) []byte {
	var out []byte

	for seq, p := range packets {
		var lacing []byte

		for rest := len(p); ; rest -= maxLacing {
			if rest >= maxLacing {
				lacing = append(lacing, maxLacing)

				if rest == maxLacing {
					lacing = append(lacing, 0)

					break
				}

				continue
			}

			lacing = append(lacing, byte(rest))

			break
		}

		for len(lacing) > maxLacing {
			lacing = lacing[:maxLacing]
		}

		hdr := make([]byte, oggPageHeader, oggPageHeader+len(lacing)+len(p))
		copy(hdr, "OggS")
		binary.LittleEndian.PutUint32(hdr[18:], uint32(seq))
		hdr[oggSegmentsAt] = byte(len(lacing))

		page := append(append(hdr, lacing...), p...)
		binary.LittleEndian.PutUint32(page[22:], oggCRC(page))
		out = append(out, page...)
	}

	return out
}

func oggCRC(page []byte) uint32 {
	var crc uint32

	for _, b := range page {
		crc ^= uint32(b) << 24

		for range 8 {
			if crc&0x80000000 != 0 {
				crc = crc<<1 ^ 0x04c11db7
			} else {
				crc <<= 1
			}
		}
	}

	return crc
}

type bitWriter struct {
	buf  []byte
	used uint
}

func (w *bitWriter) put(v uint64, n uint) {
	for i := range n {
		if w.used%8 == 0 {
			w.buf = append(w.buf, 0)
		}

		w.buf[len(w.buf)-1] |= byte((v>>i)&1) << (w.used % 8)
		w.used++
	}
}

func commentPacket(vendorLen, count uint32, body []byte) []byte {
	p := append([]byte{vorbisCommentTyp}, "vorbis"...)
	p = binary.LittleEndian.AppendUint32(p, vendorLen)
	p = append(p, make([]byte, min(vendorLen, 4))...)
	p = binary.LittleEndian.AppendUint32(p, count)

	return append(p, body...)
}

func setupPacket(books func(w *bitWriter)) []byte {
	w := &bitWriter{}
	books(w)

	return append(append([]byte{vorbisSetupTyp}, "vorbis"...), w.buf...)
}

func orderedBook(w *bitWriter, entries, dims uint64, lookup uint64) {
	w.put(vorbisCodebookID, 24)
	w.put(dims, 16)
	w.put(entries, 24)
	w.put(1, 1)
	w.put(0, 5)

	for cur := uint64(0); cur < entries; {
		bits := uint(0)
		for rest := entries - cur; rest > 0; rest >>= 1 {
			bits++
		}

		w.put(entries-cur, bits)

		cur = entries
	}

	w.put(lookup, 4)

	if lookup != 0 {
		w.put(0, 64)
		w.put(0, 4)
		w.put(0, 1)
	}
}

func TestGuardVorbisHeaders(t *testing.T) {
	t.Parallel()

	ident := append([]byte{1}, "vorbis"...)

	tests := []struct {
		name    string
		packets [][]byte
		wantErr bool
	}{
		{
			name:    "comment_count_4g",
			packets: [][]byte{ident, commentPacket(4, 0xFFFFFFFF, nil)},
			wantErr: true,
		},
		{
			name:    "comment_vendor_4g",
			packets: [][]byte{ident, commentPacket(0xFFFFFFFF, 0, nil)},
			wantErr: true,
		},
		{
			name:    "comment_ok",
			packets: [][]byte{ident, commentPacket(4, 1, []byte{0, 0, 0, 0})},
			wantErr: false,
		},
		{
			name: "codebook_16m_entries_lookup2_dims_65535",
			packets: [][]byte{ident, commentPacket(4, 0, nil), setupPacket(func(w *bitWriter) {
				w.put(0, 8)
				orderedBook(w, 1<<24-1, 65535, 2)
			})},
			wantErr: true,
		},
		{
			name: "codebook_many_ordered_books",
			packets: [][]byte{ident, commentPacket(4, 0, nil), setupPacket(func(w *bitWriter) {
				w.put(255, 8)

				for range 256 {
					orderedBook(w, 1<<24-1, 1, 0)
				}
			})},
			wantErr: true,
		},
		{
			name: "codebook_truncated",
			packets: [][]byte{ident, commentPacket(4, 0, nil), setupPacket(func(w *bitWriter) {
				w.put(0, 8)
				w.put(vorbisCodebookID, 24)
			})},
			wantErr: true,
		},
		{
			name: "codebook_small_ok",
			packets: [][]byte{ident, commentPacket(4, 0, nil), setupPacket(func(w *bitWriter) {
				w.put(0, 8)
				orderedBook(w, 16, 2, 1)
			})},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := guardVorbisHeaders(oggPages(tt.packets...))
			if tt.wantErr {
				require.ErrorIs(t, err, errVorbisHeader)

				return
			}

			require.NoError(t, err)
		})
	}
}

//nolint:paralleltest // measures process-wide allocation
func TestOpenOGGRejectsHostileHeaderCheaply(t *testing.T) {
	ident := append([]byte{1}, "vorbis"...)
	data := oggPages(ident, commentPacket(4, 0xFFFFFFFF, nil))

	var before, after runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&before)

	_, err := openAudio(mediaOGG, data)

	runtime.ReadMemStats(&after)

	require.ErrorIs(t, err, errDecode)
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(4<<20))
}

func TestGuardAcceptsRealVorbis(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("testdata/vorbis.ogg")
	require.NoError(t, err)
	require.NoError(t, guardVorbisHeaders(data))
}

func validIdent() []byte {
	p := append([]byte{1}, "vorbis"...)
	p = binary.LittleEndian.AppendUint32(p, 0)
	p = append(p, 1)
	p = binary.LittleEndian.AppendUint32(p, engineSampleRate)
	p = append(p, make([]byte, 12)...)
	p = append(p, 0xB8, 1)

	return p
}

//nolint:paralleltest // measures process-wide allocation
func TestOpenOGGRejectsHostileCodebookCheaply(t *testing.T) {
	setup := setupPacket(func(w *bitWriter) {
		w.put(0, 8)
		orderedBook(w, 1<<24-1, 65535, 2)
	})
	data := oggPages(validIdent(), commentPacket(4, 0, nil), setup)

	var before, after runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&before)

	_, err := openAudio(mediaOGG, data)

	runtime.ReadMemStats(&after)

	require.ErrorIs(t, err, errDecode)
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(4<<20))
}
