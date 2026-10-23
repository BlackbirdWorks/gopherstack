package amqp1_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/amqp1"
)

func TestCodecRoundTrip(t *testing.T) {
	t.Parallel()

	ts := time.UnixMilli(1700000000123).UTC()

	tests := []struct {
		in   any
		name string
	}{
		{name: "null", in: nil},
		{name: "bool", in: true},
		{name: "string", in: "hello"},
		{name: "longstring", in: string(make([]byte, 300))},
		{name: "binary", in: []byte{1, 2, 3}},
		{name: "symbol", in: amqp1.Symbol("x-opt")},
		{name: "int32", in: int32(-5)},
		{name: "int64", in: int64(1 << 40)},
		{name: "uint32", in: uint32(7)},
		{name: "timestamp", in: ts},
		{name: "list", in: amqp1.List{"a", int32(1), nil}},
		{name: "map", in: amqp1.Map{"k": int64(3)}},
		{name: "described", in: amqp1.Described{Descriptor: uint64(0x70), Value: amqp1.List{true}}},
		{name: "long array", in: []int64{1, 2, 3}},
		{name: "time array", in: []time.Time{ts}},
		{name: "uuid array", in: []amqp1.UUID{{1}, {2}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, err := amqp1.Marshal(nil, tt.in)
			require.NoError(t, err)

			got, rest, err := amqp1.Unmarshal(b)
			require.NoError(t, err)
			assert.Empty(t, rest)
			assert.Equal(t, tt.in, got)
		})
	}
}

func TestUnmarshalRejectsMalformed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   []byte
	}{
		{name: "empty", in: nil},
		{name: "unknown code", in: []byte{0x01}},
		{name: "short string", in: []byte{0xa1, 5, 'a'}},
		{name: "huge list count", in: []byte{0xd0, 0, 0, 0, 4, 0xff, 0xff, 0xff, 0xff}},
		{name: "odd map", in: []byte{0xc1, 2, 1, 0x40}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := amqp1.Unmarshal(tt.in)
			assert.Error(t, err)
		})
	}
}
