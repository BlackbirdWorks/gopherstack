package compress

import (
	"hash/crc32"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type writeState int

const (
	stateUndecided writeState = iota
	statePassthrough
	stateCompressing
)

type responseWriter struct {
	http.ResponseWriter
	// dst is the body sink; handler bytes are forwarded as-is, never inspected.
	dst      io.Writer
	c        *Compressor
	w        encoder
	key      string
	enc      Encoding
	buf      []byte
	status   int
	state    writeState
	buffered bool
	started  bool
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (rw *responseWriter) Unwrap() http.ResponseWriter { return rw.ResponseWriter }

func (rw *responseWriter) WriteHeader(code int) {
	if rw.started {
		return
	}

	if code >= http.StatusContinue && code < http.StatusOK && code != http.StatusSwitchingProtocols {
		rw.ResponseWriter.WriteHeader(code)

		return
	}

	rw.started, rw.status = true, code
	h := rw.Header()

	if !statusHasCompressibleBody(code) || h.Get("Content-Encoding") != "" ||
		strings.Contains(strings.ToLower(h.Get("Cache-Control")), "no-transform") {
		rw.passthrough()

		return
	}

	if ct := h.Get("Content-Type"); ct == "" || !Compressible(ct) {
		rw.passthrough()

		return
	}

	addVary(h)

	if rw.c.cfg.RewriteCRC32 && h.Get(crc32Header) != "" {
		rw.buffered = true
	}

	if n, err := strconv.Atoi(h.Get("Content-Length")); err == nil && n < rw.c.cfg.MinSize {
		rw.passthrough()
	}
}

func statusHasCompressibleBody(code int) bool {
	return code >= http.StatusOK && code != http.StatusNoContent &&
		code != http.StatusResetContent && code != http.StatusPartialContent &&
		code != http.StatusNotModified
}

func (rw *responseWriter) passthrough() {
	rw.state = statePassthrough
	rw.ResponseWriter.WriteHeader(rw.status)
}

func (rw *responseWriter) Write(p []byte) (int, error) {
	if !rw.started {
		rw.WriteHeader(http.StatusOK)
	}

	switch rw.state {
	case statePassthrough:
		return rw.dst.Write(p)
	case stateCompressing:
		return rw.w.Write(p)
	case stateUndecided:
	}

	if rw.buffered || len(rw.buf)+len(p) < rw.c.cfg.MinSize {
		rw.buf = append(rw.buf, p...)

		return len(p), nil
	}

	data := p
	if len(rw.buf) > 0 {
		rw.buf = append(rw.buf, p...)
		data = rw.buf
	}

	rw.buf = nil
	rw.decideStreaming(data)

	return len(p), nil
}

// decideStreaming runs only for handler-set compressible Content-Types; nothing is sniffed.
func (rw *responseWriter) decideStreaming(data []byte) {
	rw.startCompressing()
	_, _ = rw.w.Write(data)
}

func (rw *responseWriter) flushIdentity() {
	rw.passthrough()

	if len(rw.buf) > 0 {
		_, _ = rw.dst.Write(rw.buf)
		rw.buf = nil
	}
}

func (rw *responseWriter) setCompressedHeaders() {
	h := rw.Header()
	h.Del("Content-Length")
	h.Del("Content-MD5")
	h.Del("Accept-Ranges")
	h.Set("Content-Encoding", string(rw.enc))

	if et := h.Get("ETag"); et != "" && !strings.HasPrefix(et, "W/") {
		h.Set("ETag", "W/"+et)
	}
}

func (rw *responseWriter) startCompressing() {
	rw.setCompressedHeaders()
	rw.state = stateCompressing
	rw.ResponseWriter.WriteHeader(rw.status)
	rw.w = rw.c.getEncoder(rw.enc, rw.dst)
}

// FlushError flushes pending data; an undecided stream is sent uncompressed.
func (rw *responseWriter) FlushError() error {
	if !rw.started {
		rw.WriteHeader(http.StatusOK)
	}

	switch rw.state {
	case stateUndecided:
		if rw.buffered {
			return nil
		}

		rw.flushIdentity()
	case stateCompressing:
		if err := rw.w.Flush(); err != nil {
			return err //nolint:wrapcheck // encoder error passes through
		}
	case statePassthrough:
	}

	return http.NewResponseController(rw.ResponseWriter).Flush() //nolint:wrapcheck // passthrough
}

// Flush implements http.Flusher.
func (rw *responseWriter) Flush() { _ = rw.FlushError() }

func (rw *responseWriter) finish() {
	switch rw.state {
	case stateCompressing:
		_ = rw.w.Close()
		rw.c.putEncoder(rw.enc, rw.w)
		rw.w = nil
	case stateUndecided:
		if !rw.started {
			return
		}

		rw.finishUndecided()
	case statePassthrough:
	}
}

func (rw *responseWriter) finishUndecided() {
	if len(rw.buf) < rw.c.cfg.MinSize {
		rw.flushIdentity()

		return
	}

	out := rw.c.encodeAll(rw.enc, rw.buf)
	if len(out) >= len(rw.buf) {
		rw.flushIdentity()

		return
	}

	rw.setCompressedHeaders()

	h := rw.Header()
	if h.Get(crc32Header) != "" && rw.c.cfg.RewriteCRC32 {
		h.Set(crc32Header, strconv.FormatUint(uint64(crc32.ChecksumIEEE(out)), 10))
	}

	h.Set("Content-Length", strconv.Itoa(len(out)))

	if rw.key != "" && rw.status == http.StatusOK {
		rw.c.store(rw.key, h, out)
	}

	rw.state = statePassthrough
	rw.ResponseWriter.WriteHeader(rw.status)
	_, _ = rw.dst.Write(out)
	rw.buf = nil
}
