package gcpartifactregistry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"
	"unicode"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest"
)

const (
	uploadPrefix   = "/upload/v1/"
	uploadSuffix   = "/genericArtifacts:create"
	downloadSuffix = ":download"
	maxBlobBytes   = 1 << 30
)

var errNoData = errors.New("multipart upload needs a metadata part and a media part")

// codeName turns a gRPC code such as FailedPrecondition into FAILED_PRECONDITION.
func codeName(c codes.Code) string {
	var out []rune

	for i, r := range c.String() {
		if i > 0 && unicode.IsUpper(r) {
			out = append(out, '_')
		}

		out = append(out, unicode.ToUpper(r))
	}

	return string(out)
}

func writeRESTError(w http.ResponseWriter, err error) {
	st, _ := status.FromError(err)

	body, _ := json.Marshal(map[string]any{"error": map[string]any{
		"code": gcprest.HTTPStatus(st.Code()), "message": st.Message(), "status": codeName(st.Code()),
	}})

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(gcprest.HTTPStatus(st.Code()))
	_, _ = w.Write(body)
}

// FileExtras routes the media endpoints that have no gRPC method: the generic artifact upload and file download.
func (b *InMemoryBackend) FileExtras() gcprest.Extra {
	return gcprest.Extra{
		Match: func(path string) bool {
			return (strings.HasPrefix(path, uploadPrefix) && strings.HasSuffix(path, uploadSuffix)) ||
				(strings.HasPrefix(path, "/v1/") && strings.HasSuffix(path, downloadSuffix))
		},
		Handler: http.HandlerFunc(b.serveFileMedia),
	}
}

func (b *InMemoryBackend) serveFileMedia(w http.ResponseWriter, r *http.Request) {
	escaped := r.URL.EscapedPath()

	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(escaped, uploadSuffix):
		b.serveUpload(w, r, strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, uploadPrefix), uploadSuffix))
	case r.Method == http.MethodGet && strings.HasSuffix(escaped, downloadSuffix):
		b.serveDownload(w, r, strings.TrimSuffix(strings.TrimPrefix(escaped, "/v1/"), downloadSuffix))
	default:
		writeRESTError(w, errInvalid("Method %s is not supported on %s.", r.Method, r.URL.Path))
	}
}

func readMultipart(r *http.Request) (uploadReq, []byte, error) {
	mt, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.HasPrefix(mt, "multipart/") || params["boundary"] == "" {
		return uploadReq{}, nil, errInvalid("The upload must be a multipart request (uploadType=multipart).")
	}

	body := http.MaxBytesReader(nil, r.Body, maxGenericBytes+1<<20)
	mr := multipart.NewReader(body, params["boundary"])

	meta, err := mr.NextPart()
	if err != nil {
		return uploadReq{}, nil, errInvalid("%v", errNoData)
	}

	var req uploadReq
	if err = json.NewDecoder(meta).Decode(&req); err != nil {
		return uploadReq{}, nil, errInvalid("The upload metadata is not valid JSON: %v", err)
	}

	media, err := mr.NextPart()
	if err != nil {
		return uploadReq{}, nil, errInvalid("%v", errNoData)
	}

	data, err := io.ReadAll(io.LimitReader(media, maxGenericBytes+1))
	if err != nil {
		return uploadReq{}, nil, errInvalid("Reading the upload failed: %v", err)
	}

	if len(data) > maxGenericBytes {
		return uploadReq{}, nil, status.Error(codes.ResourceExhausted, "The file is larger than the emulator accepts.")
	}

	return req, data, nil
}

func (b *InMemoryBackend) serveUpload(w http.ResponseWriter, r *http.Request, parent string) {
	if t := r.URL.Query().Get("uploadType"); t != "" && t != "multipart" {
		writeRESTError(w, errInvalid("uploadType %q is not supported: use multipart.", t))

		return
	}

	req, data, err := readMultipart(r)
	if err != nil {
		writeRESTError(w, err)

		return
	}

	op, err := b.UploadGenericArtifact(r.Context(), parent, req, data)
	if err != nil {
		writeRESTError(w, err)

		return
	}

	raw, err := protojson.Marshal(op)
	if err != nil {
		writeRESTError(w, errInternal(err))

		return
	}

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	_, _ = w.Write([]byte(`{"operation":` + string(raw) + `}`)) //nolint:gosec // JSON response
}

func (b *InMemoryBackend) serveDownload(w http.ResponseWriter, r *http.Request, name string) {
	if r.URL.Query().Get("alt") != "media" {
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		_, _ = w.Write([]byte("{}"))

		return
	}

	data, err := b.fileContent(r.Context(), name)
	if err != nil {
		writeRESTError(w, err)

		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(data) //nolint:gosec // octet-stream download, never rendered
}

// fileContent reads the bytes of a file, unless a download rule denies its package.
func (b *InMemoryBackend) fileContent(ctx context.Context, name string) ([]byte, error) {
	fr, err := parseFile(name)
	if err != nil {
		return nil, err
	}

	f, err := b.GetFile(ctx, name)
	if err != nil {
		return nil, err
	}

	pkgName, _, _ := strings.Cut(f.GetOwner(), "/versions/")

	pkg, err := parsePackage(pkgName)
	if err != nil {
		return nil, errInternal(err)
	}

	if b.downloadDenied(ctx, fr.repo.project, fr.repo.id, pkg.image) {
		return nil, status.Error(codes.PermissionDenied, "A download rule denies access to this package.")
	}

	generic, err := b.storesFiles(fr.repo)
	if err != nil {
		return nil, err
	}

	if generic {
		b.mu.RLock("fileContent")
		defer b.mu.RUnlock()

		return bytes.Clone(b.genFiles[f.GetName()].Data), nil
	}

	return b.dockerFileContent(ctx, fr, pkg, f)
}

func (b *InMemoryBackend) dockerFileContent(
	ctx context.Context, fr fileRef, pkg pkgRef, f *artifactregistrypb.File,
) ([]byte, error) {
	c := b.client()
	repo := fr.repo.dockerKey() + "/" + pkg.image

	if strings.HasSuffix(f.GetOwner(), "/versions/"+fr.id) {
		m, err := c.GetManifest(ctx, repo, fr.id)
		if err != nil {
			return nil, errNotFound("File %q not found.", f.GetName())
		}

		return m.Body, nil
	}

	data, err := c.GetBlob(ctx, repo, fr.id, maxBlobBytes)
	if err != nil {
		return nil, errNotFound("File %q not found.", f.GetName())
	}

	return data, nil
}
