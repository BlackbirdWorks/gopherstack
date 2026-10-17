package rds //nolint:testpackage // white-box access to the wire codec.

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildMyResponse(caps uint32, user string, auth []byte, db, plugin string) []byte {
	out := binary.LittleEndian.AppendUint32(nil, caps)
	out = binary.LittleEndian.AppendUint32(out, 1<<24)
	out = append(out, myCharsetUTF8MB4)
	out = append(out, make([]byte, myFillerLen)...)
	out = append(out, user...)
	out = append(out, 0)

	switch {
	case caps&myCapLenencData != 0:
		out = append(out, myLenencTwo)
		out = binary.LittleEndian.AppendUint16(out, uint16(len(auth)))
	case caps&myCapSecureConn != 0:
		out = append(out, byte(len(auth)))
	}

	out = append(out, auth...)

	if caps&myCapConnectWithDB != 0 {
		out = append(append(out, db...), 0)
	}

	if caps&myCapPluginAuth != 0 {
		out = append(append(out, plugin...), 0)
	}

	return out
}

func TestMyPacketRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload []byte
		seq     byte
	}{
		{name: "empty", payload: nil, seq: 0},
		{name: "small", payload: []byte("hello"), seq: 3},
		{name: "token sized", payload: bytes.Repeat([]byte{'x'}, 2048), seq: 255},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			err := writeMyPacket(&buf, tt.seq, tt.payload)
			require.NoError(t, err)

			seq, got, err := readMyPacket(&buf)
			require.NoError(t, err)
			assert.Equal(t, tt.seq, seq)
			assert.Equal(t, len(tt.payload), len(got))
			assert.True(t, bytes.Equal(tt.payload, got))
		})
	}
}

func TestMyPacketRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   []byte
	}{
		{name: "short header", in: []byte{1, 0}},
		{name: "truncated body", in: []byte{5, 0, 0, 0, 'a'}},
		{name: "oversized", in: []byte{0xff, 0xff, 0xff, 0}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := readMyPacket(bytes.NewReader(tt.in))
			require.Error(t, err)
		})
	}
}

func TestMyGreeting(t *testing.T) {
	t.Parallel()

	scramble := bytes.Repeat([]byte{7}, myScrambleLen)
	caps := uint32(myCapProtocol41 | myCapPluginAuth | myCapSSL | myCapSecureConn)
	g := encodeMyGreeting("8.4.0", 9, scramble, caps, myPluginClear)

	assert.Equal(t, byte(myProtocolV10), g[0])
	assert.Equal(t, "8.4.0", string(g[1:6]))
	assert.Equal(t, byte(0), g[6])
	assert.Equal(t, uint32(9), binary.LittleEndian.Uint32(g[7:]))
	assert.True(t, bytes.HasSuffix(g, append([]byte(myPluginClear), 0)))

	lo := uint32(binary.LittleEndian.Uint16(g[7+4+8+1:]))
	hi := uint32(binary.LittleEndian.Uint16(g[7+4+8+1+2+1+2:]))
	assert.Equal(t, caps, lo|hi<<16)
}

func TestMyHandshakeResponseDecode(t *testing.T) {
	t.Parallel()

	token := []byte(strings.Repeat("t", 1200) + "\x00")
	full := uint32(myCapProtocol41 | myCapSecureConn | myCapPluginAuth | myCapConnectWithDB | myCapLenencData)

	tests := []struct {
		name     string
		wantUser string
		wantDB   string
		in       []byte
		wantAuth []byte
		wantErr  bool
	}{
		{
			name: "lenenc token with db", in: buildMyResponse(full, "iamuser", token, "appdb", myPluginClear),
			wantUser: "iamuser", wantDB: "appdb", wantAuth: token,
		},
		{
			name:     "secure connection no db",
			in:       buildMyResponse(myCapProtocol41|myCapSecureConn|myCapPluginAuth, "bob", []byte("pw\x00"), "", "x"),
			wantUser: "bob", wantAuth: []byte("pw\x00"),
		},
		{
			name: "ssl request prefix",
			in:   buildMyResponse(myCapProtocol41|myCapSSL, "", nil, "", "")[:32],
		},
		{name: "too short", in: []byte{1, 2, 3}, wantErr: true},
		{name: "not protocol 41", in: buildMyResponse(myCapSecureConn, "u", []byte("p"), "", ""), wantErr: true},
		{
			name:    "lenenc overruns",
			in:      buildMyResponse(full, "u", token, "d", "p")[:60],
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := decodeMyHandshakeResponse(tt.in)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantUser, got.user)
			assert.Equal(t, tt.wantDB, got.database)
			assert.True(t, bytes.Equal(tt.wantAuth, got.authData))
		})
	}
}

func TestMyErrAndOK(t *testing.T) {
	t.Parallel()

	e := encodeMyErr(myErrAccessDenied, myStateDenied, "denied")
	assert.Equal(t, byte(myErrMarker), e[0])
	assert.Equal(t, uint16(myErrAccessDenied), binary.LittleEndian.Uint16(e[1:]))
	assert.Equal(t, "#28000denied", string(e[3:]))

	assert.Equal(t, byte(0), encodeMyOK()[0])
}
