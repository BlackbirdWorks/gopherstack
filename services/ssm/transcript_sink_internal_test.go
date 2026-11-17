package ssm

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTranscriptSinkExpiresUnclaimed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		age     time.Duration
		wantNew bool
	}{
		{name: "stale entries make room", age: 2 * transcriptSinkTTL, wantNew: true},
		{name: "fresh entries keep the cap", age: time.Minute, wantNew: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend()
			noop := func(string, []byte) {}

			for i := range maxTranscriptSinks {
				b.addTranscriptSink("s"+strconv.Itoa(i), noop)
			}

			for id, e := range b.transcriptSinks {
				e.at = timeNow().Add(-tt.age)
				b.transcriptSinks[id] = e
			}

			b.addTranscriptSink("new", noop)
			assert.Equal(t, tt.wantNew, b.takeTranscriptSink("new") != nil)
		})
	}
}

func TestTranscriptBufferKeepsFirstMegabyte(t *testing.T) {
	t.Parallel()

	var tb transcriptBuffer

	chunk := make([]byte, 300<<10)
	for range 5 {
		tb.add(chunk)
	}

	assert.Len(t, tb.data, maxTranscriptBytes)
}
