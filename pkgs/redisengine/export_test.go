package redisengine

import (
	"strconv"
	"time"
)

// CRC64 exposes the RDB checksum for tests.
var CRC64 = crc64Jones //nolint:gochecknoglobals // test hook

// LZFDecompress exposes the LZF decoder for tests.
var LZFDecompress = lzfDecompress //nolint:gochecknoglobals // test hook

// SetClock replaces the tuner's clock and the server's TTL clock so tests control time.
func SetClock(e *Engine, now func() time.Time) {
	e.tuner.mu.Lock()
	e.tuner.clock = now
	e.tuner.mu.Unlock()
}

// Sweep runs one expiry pass of the tuner.
func Sweep(e *Engine) {
	e.tuner.mu.Lock()
	e.tuner.expireLocked()
	e.tuner.mu.Unlock()
}

// StreamNodeCount reports how many listpack nodes stream entries are split into under the limits.
func StreamNodeCount(entries, nodeBytes, nodeEntries int) int {
	list := make([]streamEntry, entries)
	for i := range list {
		list[i] = streamEntry{ID: "1-" + strconv.Itoa(i), Values: []string{"f", "v"}}
	}

	ids, _ := streamNodes(list, streamLimits{nodeBytes: nodeBytes, nodeEntries: nodeEntries})

	return len(ids)
}
