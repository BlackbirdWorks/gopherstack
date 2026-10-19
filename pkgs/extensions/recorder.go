package extensions

import (
	"bytes"
	"maps"
	"net/http"
)

// Recorder buffers a response so post hooks can inspect and rewrite it before Commit.
type Recorder struct {
	w      http.ResponseWriter
	hdr    http.Header
	body   bytes.Buffer
	status int
}

// NewRecorder wraps w; nothing reaches w until Commit.
func NewRecorder(w http.ResponseWriter) *Recorder {
	return &Recorder{w: w, hdr: w.Header().Clone()}
}

// Header returns the buffered headers.
func (r *Recorder) Header() http.Header { return r.hdr }

// WriteHeader records the status.
func (r *Recorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
}

// Write buffers body bytes.
func (r *Recorder) Write(b []byte) (int, error) {
	r.WriteHeader(http.StatusOK)

	return r.body.Write(b)
}

// Status is the recorded status (200 when none was written).
func (r *Recorder) Status() int {
	if r.status == 0 {
		return http.StatusOK
	}

	return r.status
}

func (r *Recorder) bodyForHook() ([]byte, bool) {
	if r.body.Len() > maxHookBody {
		return nil, true
	}

	return r.body.Bytes(), false
}

func (r *Recorder) apply(p postReply) {
	if p.Status != 0 {
		r.status = p.Status
	}

	for k, v := range p.SetHeaders {
		r.hdr.Set(k, v)
	}

	if p.Body != nil {
		r.body.Reset()
		_, _ = r.body.Write(*p.Body)
		r.hdr.Del("Content-Length")
	}
}

// Commit writes the buffered response to the underlying writer.
func (r *Recorder) Commit() {
	dst := r.w.Header()

	for k := range dst {
		delete(dst, k)
	}

	maps.Copy(dst, r.hdr)

	r.w.WriteHeader(r.Status())
	_, _ = r.w.Write(r.body.Bytes())
}
