package gcpbigtable_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpbigtable"
)

func TestSampleRowKeysIsNotRescannedPerCall(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b := gcpbigtable.NewInMemoryBackend()
	b.SetClock(func() time.Time { return now })

	e := newEnvWithBackend(t, b)
	tbl := e.seeded(t, "t", "cf")
	val := strings.Repeat("x", 100*1024)

	for i := range 40 {
		put(t, tbl, seedCell{fmt.Sprintf("r%03d", i), "cf", "q", val, 1000})
	}

	end := func() int64 {
		got := recvSamples(t, e)

		return got[len(got)-1].GetOffsetBytes()
	}

	first := end()

	put(t, tbl, seedCell{"r000", "cf", "q2", strings.Repeat("y", 3<<20), 1000})
	assert.Equal(t, first, end(), "an unchanged key set reuses the samples within the TTL")

	now = now.Add(time.Minute)
	assert.Greater(t, end(), first+3<<20-1, "the samples are rebuilt once they expire")

	rebuilt := end()

	put(t, tbl, seedCell{"zzz", "cf", "q", val, 1000})
	assert.Greater(t, end(), rebuilt, "a new row key invalidates the samples at once")
	require.NotZero(t, rebuilt)
}
