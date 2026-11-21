package azurefile

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type framedStep struct {
	want   map[string]string
	canon  map[string][]string
	method string
	target string
}

func req(method, target string, headers []string, tail string) string {
	return method + " " + target + " HTTP/1.1\r\nHost: h\r\n" + strings.Join(headers, "\r\n") + "\r\n\r\n" + tail
}

func chunked(parts ...string) string {
	var sb strings.Builder

	for _, p := range parts {
		fmt.Fprintf(&sb, "%x\r\n%s\r\n", len(p), p)
	}

	return sb.String()
}

func queued(t *headTracker) int {
	if q := t.queue.Load(); q != nil {
		return len(*q)
	}

	return 0
}

func deliver(t *headTracker, stream, mode string) {
	switch mode {
	case "whole":
		t.feed([]byte(stream))
	case "bytewise":
		for i := range len(stream) {
			t.feed([]byte(stream[i : i+1]))
		}
	default:
		for i := 0; i < len(stream); i += 7 {
			t.feed([]byte(stream[i:min(i+7, len(stream))]))
		}
	}
}

func TestHeadTrackerFraming(t *testing.T) {
	t.Parallel()

	fake := req("PUT", "/b", []string{"x-ms-meta-EVIL: 1"}, "")
	meta := func(k, v string) map[string][]string { return map[string][]string{k: {v}} }
	second := req("PUT", "/b", []string{"x-ms-meta-lower: 2"}, "")
	lowerStep := framedStep{
		method: "PUT", target: "/b", canon: meta("X-Ms-Meta-Lower", "2"),
		want: map[string]string{"X-Ms-Meta-Lower": "x-ms-meta-lower"},
	}
	lowerSeen := framedStep{method: "PUT", target: "/b", canon: meta("X-Ms-Meta-Lower", "2")}
	evilSeen := framedStep{method: "PUT", target: "/b", canon: meta("X-Ms-Meta-Evil", "1")}

	tests := []struct {
		name      string
		stream    string
		steps     []framedStep
		forbidden []framedStep
		broken    bool
	}{
		{
			name: "body that looks like a request",
			stream: req("PUT", "/a", []string{"x-ms-meta-aaA: 1", fmt.Sprintf("Content-Length: %d", len(fake))}, fake) +
				req("PUT", "/b", []string{"x-ms-meta-lower: 2"}, ""),
			steps: []framedStep{
				{
					method: "PUT",
					target: "/a",
					canon:  meta("X-Ms-Meta-Aaa", "1"),
					want:   map[string]string{"X-Ms-Meta-Aaa": "x-ms-meta-aaA"},
				},
				lowerStep,
			},
			forbidden: []framedStep{evilSeen},
		},
		{
			name: "chunked body with extension and trailer",
			stream: req(
				"PUT",
				"/a",
				[]string{"x-ms-meta-aaA: 1", "Transfer-Encoding: chunked"},
				"5;ext=1\r\n"+fake[:5]+"\r\n"+fmt.Sprintf(
					"%x\r\n%s\r\n",
					len(fake)-5,
					fake[5:],
				)+"0\r\nTrailer: x\r\n\r\n",
			) +
				req("PUT", "/b", []string{"x-ms-meta-lower: 2"}, ""),
			steps: []framedStep{
				lowerStep,
			},
			forbidden: []framedStep{evilSeen},
		},
		{
			name: "content-length and transfer-encoding fail safe",
			stream: req(
				"PUT",
				"/a",
				[]string{"Content-Length: 5", "Transfer-Encoding: chunked"},
				chunked(fake)+"0\r\n\r\n",
			) +
				req(
					"PUT",
					"/b",
					[]string{"x-ms-meta-lower: 2"},
					"",
				),
			broken:    true,
			forbidden: []framedStep{evilSeen, lowerSeen},
		},
		{
			name:      "duplicate content-length",
			stream:    req("PUT", "/a", []string{"Content-Length: 0", "Content-Length: 0"}, "") + second,
			broken:    true,
			forbidden: []framedStep{lowerSeen},
		},
		{
			name:      "list content-length",
			stream:    req("PUT", "/a", []string{"Content-Length: 5, 5"}, "") + second,
			broken:    true,
			forbidden: []framedStep{lowerSeen},
		},
		{
			name:      "obs-fold",
			stream:    req("PUT", "/a", []string{"X-Folded: a", " b"}, "") + second,
			broken:    true,
			forbidden: []framedStep{lowerSeen},
		},
		{
			name:      "bare lf",
			stream:    "PUT /a HTTP/1.1\nHost: h\n\n" + second,
			broken:    true,
			forbidden: []framedStep{lowerSeen},
		},
		{
			name:      "unsupported transfer coding",
			stream:    req("PUT", "/a", []string{"Transfer-Encoding: gzip, chunked"}, "") + second,
			broken:    true,
			forbidden: []framedStep{lowerSeen},
		},
		{
			name:      "upgrade and connect",
			stream:    req("GET", "/a", []string{"Upgrade: websocket"}, "") + second,
			broken:    true,
			forbidden: []framedStep{lowerSeen},
		},
		{
			name:      "http/1.0",
			stream:    "PUT /b HTTP/1.0\r\nx-ms-meta-lower: 2\r\n\r\n",
			broken:    true,
			forbidden: []framedStep{lowerSeen},
		},
		{
			name:      "oversized chunk size",
			stream:    req("PUT", "/a", []string{"Transfer-Encoding: chunked"}, "7fffffffffffffff\r\nx") + second,
			forbidden: []framedStep{lowerSeen},
			broken:    true,
		},
		{
			name:   "same names different value is never matched",
			stream: req("PUT", "/b", []string{"x-ms-meta-FOO: 1"}, ""),
			forbidden: []framedStep{
				{method: "PUT", target: "/b", canon: meta("X-Ms-Meta-Foo", "2")},
				{method: "GET", target: "/b", canon: meta("X-Ms-Meta-Foo", "1")},
				{method: "PUT", target: "/c", canon: meta("X-Ms-Meta-Foo", "1")},
			},
			steps: []framedStep{
				{
					method: "PUT",
					target: "/b",
					canon:  meta("X-Ms-Meta-Foo", "1"),
					want:   map[string]string{"X-Ms-Meta-Foo": "x-ms-meta-FOO"},
				},
			},
		},
		{
			name:   "names differing in case are not renamed",
			stream: req("PUT", "/b", []string{"x-ms-meta-foo: 1", "x-ms-meta-FOO: 2"}, ""),
			steps: []framedStep{
				{
					method: "PUT",
					target: "/b",
					canon:  map[string][]string{"X-Ms-Meta-Foo": {"1", "2"}},
					want:   map[string]string{},
				},
			},
		},
	}

	for _, tt := range tests {
		for _, mode := range []string{"whole", "bytewise", "split"} {
			t.Run(tt.name+"/"+mode, func(t *testing.T) {
				t.Parallel()

				tr := newHeadTracker()
				deliver(tr, tt.stream, mode)

				for _, f := range tt.forbidden {
					assert.Nil(t, tr.match(f.method, f.target, f.canon))
				}

				if tt.broken {
					assert.Equal(t, phaseBroken, tr.phase)
					assert.Zero(t, queued(tr))
				}

				for _, s := range tt.steps {
					assert.Equal(t, s.want, tr.match(s.method, s.target, s.canon))
				}
			})
		}
	}
}

func TestHeadTrackerBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		stream string
		queued int
		broken bool
	}{
		{
			name:   "head larger than the buffer",
			stream: "PUT /a HTTP/1.1\r\nX-Big: " + strings.Repeat("a", maxHeadBytes+1),
			broken: true,
		},
		{
			name:   "metadata over the byte cap is not recorded",
			stream: req("PUT", "/a", []string{"x-ms-meta-a: " + strings.Repeat("v", maxMetaBytes+1)}, ""),
		},
		{
			name:   "queue is capped",
			stream: strings.Repeat(req("PUT", "/a", []string{"x-ms-meta-a: 1"}, ""), maxQueuedHeads*3),
			queued: maxQueuedHeads,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tr := newHeadTracker()
			deliver(tr, tt.stream, "split")
			assert.Equal(t, tt.broken, tr.phase == phaseBroken)

			assert.Equal(t, tt.queued, queued(tr))
		})
	}
}
