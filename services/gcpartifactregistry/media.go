package gcpartifactregistry

import (
	"context"
	"encoding/json"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
)

const (
	uploadTypeParam    = "uploadType"
	uploadIDParam      = "upload_id"
	uploadMultipart    = "multipart"
	uploadMedia        = "media"
	uploadResumable    = "resumable"
	sessionTTL         = 7 * 24 * time.Hour
	statusResumeIncomp = 308
)

// uploadKind is one media-upload endpoint: its path suffix and the handler that stores the bytes.
type uploadKind struct {
	suffix string
	store  func(b *InMemoryBackend, parent string, meta, data []byte) (*longrunningpb.Operation, error)
}

func uploadKinds() []uploadKind {
	return []uploadKind{
		{"/genericArtifacts:create", storeGeneric},
		{"/aptArtifacts:create", storeApt},
		{"/yumArtifacts:create", storeYum},
		{"/goModules:create", storeGoModule},
	}
}

func storeGeneric(b *InMemoryBackend, parent string, meta, data []byte) (*longrunningpb.Operation, error) {
	var req uploadReq
	if len(meta) > 0 {
		if err := json.Unmarshal(meta, &req); err != nil {
			return nil, errInvalid("The upload metadata is not valid JSON: %v", err)
		}
	}

	return b.UploadGenericArtifact(context.Background(), parent, req, data)
}

// uploadSession is a resumable upload in progress.
type uploadSession struct {
	created time.Time
	parent  string
	suffix  string
	meta    []byte
	buf     []byte
}

func matchUpload(path string) (uploadKind, bool) {
	if !strings.HasPrefix(path, uploadPrefix) {
		return uploadKind{}, false
	}

	for _, k := range uploadKinds() {
		if strings.HasSuffix(path, k.suffix) {
			return k, true
		}
	}

	return uploadKind{}, false
}

func parentOf(path string, k uploadKind) string {
	return strings.TrimSuffix(strings.TrimPrefix(path, uploadPrefix), k.suffix)
}

func readBody(r *http.Request, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		return nil, errInvalid("Reading the upload failed: %v", err)
	}

	if int64(len(data)) > limit {
		return nil, status.Error(codes.ResourceExhausted, "The file is larger than the emulator accepts.")
	}

	return data, nil
}

// readMultipartParts returns the JSON metadata part and the media part of a multipart upload.
func readMultipartParts(r *http.Request) ([]byte, []byte, error) {
	mt, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.HasPrefix(mt, "multipart/") || params["boundary"] == "" {
		return nil, nil, errInvalid("The upload must be a multipart request (uploadType=multipart).")
	}

	mr := multipart.NewReader(r.Body, params["boundary"])

	metaPart, err := mr.NextPart()
	if err != nil {
		return nil, nil, errInvalid("%v", errNoData)
	}

	meta, err := io.ReadAll(io.LimitReader(metaPart, 1<<20))
	if err != nil {
		return nil, nil, errInvalid("Reading the upload metadata failed: %v", err)
	}

	mediaPart, err := mr.NextPart()
	if err != nil {
		return nil, nil, errInvalid("%v", errNoData)
	}

	data, err := io.ReadAll(io.LimitReader(mediaPart, maxGenericBytes+1))
	if err != nil {
		return nil, nil, errInvalid("Reading the upload failed: %v", err)
	}

	if len(data) > maxGenericBytes {
		return nil, nil, status.Error(codes.ResourceExhausted, "The file is larger than the emulator accepts.")
	}

	return meta, data, nil
}

func writeOperation(w http.ResponseWriter, op *longrunningpb.Operation) {
	raw, err := protojson.Marshal(op)
	if err != nil {
		writeRESTError(w, errInternal(err))

		return
	}

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	_, _ = w.Write([]byte(`{"operation":` + string(raw) + `}`)) //nolint:gosec // JSON response
}

func (b *InMemoryBackend) serveMediaUpload(w http.ResponseWriter, r *http.Request, k uploadKind) {
	parent := parentOf(r.URL.Path, k)

	switch r.URL.Query().Get(uploadTypeParam) {
	case uploadResumable:
		b.serveResumable(w, r, k, parent)
	case uploadMedia:
		data, err := readBody(r, maxGenericBytes)
		if err != nil {
			writeRESTError(w, err)

			return
		}

		b.finishUpload(w, k, parent, nil, data)
	case uploadMultipart, "":
		meta, data, err := readMultipartParts(r)
		if err != nil {
			writeRESTError(w, err)

			return
		}

		b.finishUpload(w, k, parent, meta, data)
	default:
		writeRESTError(w, errInvalid("uploadType %q is not supported.", r.URL.Query().Get(uploadTypeParam)))
	}
}

func (b *InMemoryBackend) finishUpload(w http.ResponseWriter, k uploadKind, parent string, meta, data []byte) {
	op, err := k.store(b, parent, meta, data)
	if err != nil {
		writeRESTError(w, err)

		return
	}

	writeOperation(w, op)
}

func (b *InMemoryBackend) serveResumable(w http.ResponseWriter, r *http.Request, k uploadKind, parent string) {
	switch {
	case r.URL.Query().Get(uploadIDParam) != "":
		b.putResumable(w, r, k)
	case r.Method == http.MethodPost:
		meta, err := readBody(r, 1<<20)
		if err != nil {
			writeRESTError(w, err)

			return
		}

		id := uuid.NewString()
		now := b.now().UTC()

		b.mu.Lock("serveResumable")
		b.sweepSessionsLocked(now)
		b.sessions[id] = &uploadSession{created: now, parent: parent, suffix: k.suffix, meta: meta}
		b.mu.Unlock()

		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}

		q := r.URL.Query()
		q.Set(uploadIDParam, id)

		w.Header().Set("Location", scheme+"://"+r.Host+r.URL.Path+"?"+q.Encode())
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
	default:
		writeRESTError(w, errInvalid("Method %s is not supported on %s.", r.Method, r.URL.Path))
	}
}

func (b *InMemoryBackend) sweepSessionsLocked(now time.Time) {
	for id, s := range b.sessions {
		if now.Sub(s.created) > sessionTTL {
			delete(b.sessions, id)
		}
	}
}

// parseContentRange reads "bytes a-b/total" (total -1 when "*") or "bytes */total".
func parseContentRange(h string) (start, end, total int64, query bool, err error) {
	spec, ok := strings.CutPrefix(h, "bytes ")
	if !ok {
		return 0, 0, 0, false, errInvalid("Malformed Content-Range %q.", h)
	}

	rng, tot, ok := strings.Cut(spec, "/")
	if !ok {
		return 0, 0, 0, false, errInvalid("Malformed Content-Range %q.", h)
	}

	total = -1

	if tot != "*" {
		if total, err = strconv.ParseInt(tot, 10, 64); err != nil {
			return 0, 0, 0, false, errInvalid("Malformed Content-Range %q.", h)
		}
	}

	if rng == "*" {
		return 0, 0, total, true, nil
	}

	a, z, ok := strings.Cut(rng, "-")
	if !ok {
		return 0, 0, 0, false, errInvalid("Malformed Content-Range %q.", h)
	}

	if start, err = strconv.ParseInt(a, 10, 64); err == nil {
		end, err = strconv.ParseInt(z, 10, 64)
	}

	if err != nil || end < start {
		return 0, 0, 0, false, errInvalid("Malformed Content-Range %q.", h)
	}

	return start, end, total, false, nil
}

// resumeRange answers an incomplete upload with 308, or with 200 and X-Http-Status-Code-Override when the
// client sent X-Guploader-No-308.
func resumeRange(w http.ResponseWriter, r *http.Request, have int) {
	if have > 0 {
		w.Header().Set("Range", "bytes=0-"+strconv.Itoa(have-1))
	}

	w.Header().Set("Content-Length", "0")

	if r.Header.Get("X-Guploader-No-308") != "" {
		w.Header().Set("X-Http-Status-Code-Override", strconv.Itoa(statusResumeIncomp))
		w.WriteHeader(http.StatusOK)

		return
	}

	w.WriteHeader(statusResumeIncomp)
}

func (b *InMemoryBackend) putResumable(w http.ResponseWriter, r *http.Request, k uploadKind) {
	id := r.URL.Query().Get(uploadIDParam)

	chunk, err := readBody(r, maxGenericBytes)
	if err != nil {
		writeRESTError(w, err)

		return
	}

	start, end, total, query, err := b.contentRange(r, len(chunk))
	if err != nil {
		writeRESTError(w, err)

		return
	}

	b.mu.Lock("putResumable")

	s, ok := b.sessions[id]
	if !ok || s.suffix != k.suffix {
		b.mu.Unlock()
		writeRESTError(w, errNotFound("Upload session %q not found.", id))

		return
	}

	have := len(s.buf)

	if query || start != int64(have) {
		b.mu.Unlock()
		resumeRange(w, r, have)

		return
	}

	s.buf = append(s.buf, chunk...)
	complete := total >= 0 && int64(len(s.buf)) == total && end+1 == total || total >= 0 && len(chunk) == 0 && total == 0

	if int64(len(s.buf)) > maxGenericBytes {
		delete(b.sessions, id)
		b.mu.Unlock()
		writeRESTError(w, status.Error(codes.ResourceExhausted, "The file is larger than the emulator accepts."))

		return
	}

	if !complete {
		got := len(s.buf)
		b.mu.Unlock()
		resumeRange(w, r, got)

		return
	}

	delete(b.sessions, id)
	b.mu.Unlock()

	b.finishUpload(w, k, s.parent, s.meta, s.buf)
}

func (b *InMemoryBackend) contentRange(r *http.Request, n int) (int64, int64, int64, bool, error) {
	h := r.Header.Get("Content-Range")
	if h == "" {
		return 0, int64(n) - 1, int64(n), false, nil
	}

	start, end, total, query, err := parseContentRange(h)
	if err != nil {
		return 0, 0, 0, false, err
	}

	if !query && end-start+1 != int64(n) {
		return 0, 0, 0, false, errInvalid("Content-Range %q does not match the %d bytes sent.", h, n)
	}

	return start, end, total, query, nil
}
