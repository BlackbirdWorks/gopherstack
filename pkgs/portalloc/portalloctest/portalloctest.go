// Package portalloctest hands tests port allocators over disjoint, probed-free blocks.
package portalloctest

import (
	"context"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	// Below the Linux ephemeral range (32768+) so outbound connections never squat on a block.
	base  = 30000
	limit = 32700
)

//nolint:gochecknoglobals // one cursor per test process keeps every caller's block disjoint
var cursor atomic.Int64

//nolint:gochecknoglobals // paired with cursor: spreads concurrent test binaries across the range
var seeded atomic.Bool

// New returns an allocator over span ports that no other caller in this process holds
// and nothing is currently listening on. It fails the test if no such block exists.
func New(tb testing.TB, span int) *portalloc.Allocator {
	tb.Helper()

	if seeded.CompareAndSwap(false, true) {
		cursor.Store(int64(os.Getpid()%((limit-base)/span)) * int64(span))
	}

	blocks := (limit - base) / span
	for range blocks {
		off := int((cursor.Add(int64(span)) - int64(span)) % int64(blocks*span))
		start := base + off

		if free(tb.Context(), start, span) {
			a, err := portalloc.New(start, start+span)
			if err != nil {
				tb.Fatalf("portalloctest: %v", err)
			}

			return a
		}
	}

	tb.Fatalf("portalloctest: no free block of %d ports in [%d, %d)", span, base, limit)

	return nil
}

func free(ctx context.Context, start, n int) bool {
	var lc net.ListenConfig

	for p := start; p < start+n; p++ {
		ln, err := lc.Listen(ctx, "tcp", ":"+strconv.Itoa(p))
		if err != nil {
			return false
		}

		_ = ln.Close()
	}

	return true
}
