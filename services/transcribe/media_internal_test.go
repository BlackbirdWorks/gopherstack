package transcribe

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMediaURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		uri        string
		wantBucket string
		wantKey    string
		wantOK     bool
	}{
		{name: "s3", uri: "s3://bkt/a/b.wav", wantBucket: "bkt", wantKey: "a/b.wav", wantOK: true},
		{name: "s3 no key", uri: "s3://bkt", wantOK: false},
		{
			name:       "path style",
			uri:        "https://s3.us-east-1.amazonaws.com/bkt/a.mp3",
			wantBucket: "bkt",
			wantKey:    "a.mp3",
			wantOK:     true,
		},
		{
			name: "virtual hosted", uri: "https://bkt.s3.us-west-2.amazonaws.com/a/b.flac",
			wantBucket: "bkt", wantKey: "a/b.flac", wantOK: true,
		},
		{name: "emulator", uri: "http://localhost:8000/bkt/a.wav", wantBucket: "bkt", wantKey: "a.wav", wantOK: true},
		{name: "garbage", uri: "not a uri", wantOK: false},
		{name: "ftp", uri: "ftp://h/b/k", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bucket, key, ok := parseMediaURI(tt.uri)
			assert.Equal(t, tt.wantOK, ok)

			if tt.wantOK {
				assert.Equal(t, tt.wantBucket, bucket)
				assert.Equal(t, tt.wantKey, key)
			}
		})
	}
}

func TestSniffMediaFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		data []byte
	}{
		{name: "wav", data: append([]byte("RIFF\x00\x00\x00\x00WAVE"), 0), want: "wav"},
		{name: "riff not wave", data: []byte("RIFF\x00\x00\x00\x00AVI LIST"), want: ""},
		{name: "flac", data: []byte("fLaC\x00"), want: "flac"},
		{name: "ogg", data: []byte("OggS\x00"), want: "ogg"},
		{name: "mp3 id3", data: []byte("ID3\x04"), want: "mp3"},
		{name: "mp3 sync", data: []byte{0xFF, 0xFB, 0x90}, want: "mp3"},
		{name: "mp4", data: []byte("\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00"), want: "mp4"},
		{name: "amr", data: []byte("#!AMR\n"), want: "amr"},
		{name: "webm", data: []byte{0x1A, 0x45, 0xDF, 0xA3, 0}, want: "webm"},
		{name: "text", data: []byte("hello there, this is text"), want: ""},
		{name: "empty", data: nil, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, sniffMediaFormat(tt.data))
		})
	}
}

func TestParseWAV(t *testing.T) {
	t.Parallel()

	stereo := func() []byte {
		b := []byte("RIFF\x00\x00\x00\x00WAVEfmt \x10\x00\x00\x00")
		b = append(b, 1, 0, 2, 0, 0x80, 0x3E, 0, 0, 0, 0xFA, 0, 0, 4, 0, 16, 0)
		b = append(b, "LIST\x02\x00\x00\x00ab"...)
		b = append(b, "data\x08\x00\x00\x00"...)

		return append(b, 1, 0, 2, 0, 3, 0, 4, 0)
	}()

	info, err := parseWAV(stereo)
	require.NoError(t, err)
	assert.True(t, info.isPCM16())
	assert.Equal(t, 16000, info.sampleRate)
	assert.Equal(t, 2, info.channels)

	ch := info.channelPCM()
	assert.Equal(t, [][]byte{{1, 0, 3, 0}, {2, 0, 4, 0}}, ch)
	assert.Equal(t, []byte{1, 0}[:2], downmix([][]byte{{1, 0}, {1, 0}})[:2])
	assert.Equal(t, 0, info.durationMillis())

	for _, bad := range [][]byte{nil, []byte("RIFF"), []byte("RIFF\x00\x00\x00\x00WAVEdata\x00\x00\x00\x00")} {
		_, err = parseWAV(bad)
		require.ErrorIs(t, err, errBadWAV)
	}
}
