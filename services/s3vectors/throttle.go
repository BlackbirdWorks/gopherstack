package s3vectors

import (
	"net/http"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// Per-index write limits from s3-vectors-limitations.html: PutVectors plus DeleteVectors requests, and vectors changed.
const (
	maxWriteRequestsPerSecond = 1000
	maxWriteVectorsPerSecond  = 2500
)

type writeWindow struct {
	start    time.Time
	requests int
	vectors  int
}

// admitWriteLocked counts a write of n vectors against the index's one-second window and throttles past the limits.
func (b *InMemoryBackend) admitWriteLocked(ix *Index, n int) error {
	now := b.now()
	key := indexKey(ix)

	w := b.writes[key]
	if w == nil || now.Sub(w.start) >= time.Second {
		w = &writeWindow{start: now}
		b.writes[key] = w
	}

	if w.requests+1 > maxWriteRequestsPerSecond || w.vectors+n > maxWriteVectorsPerSecond {
		return restroute.NewError(
			http.StatusTooManyRequests, "TooManyRequestsException", "The request was denied due to request throttling",
		)
	}

	w.requests++
	w.vectors += n

	return nil
}
