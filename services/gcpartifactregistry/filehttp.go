package gcpartifactregistry

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"net/http"
	"strings"
	"unicode"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	uploadPrefix   = "/upload/v1/"
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
	}}, jsonx.V1())

	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(gcprest.HTTPStatus(st.Code()))
	_, _ = w.Write(body)
}

// FileExtras routes the media endpoints that have no gRPC method: the generic artifact upload and file download.
func (b *InMemoryBackend) FileExtras() gcprest.Extra {
	return gcprest.Extra{
		Match: func(path string) bool {
			_, isUpload := matchUpload(path)

			return isUpload || (strings.HasPrefix(path, "/v1/") && strings.HasSuffix(path, downloadSuffix))
		},
		Handler: http.HandlerFunc(b.serveFileMedia),
	}
}

func (b *InMemoryBackend) serveFileMedia(w http.ResponseWriter, r *http.Request) {
	escaped := r.URL.EscapedPath()

	switch {
	case isMediaUpload(r):
		k, _ := matchUpload(r.URL.Path)
		b.serveMediaUpload(w, r, k)
	case r.Method == http.MethodGet && strings.HasSuffix(escaped, downloadSuffix):
		b.serveDownload(w, r, strings.TrimSuffix(strings.TrimPrefix(escaped, "/v1/"), downloadSuffix))
	default:
		writeRESTError(w, errInvalid("Method %s is not supported on %s.", r.Method, r.URL.Path))
	}
}

func isMediaUpload(r *http.Request) bool {
	_, ok := matchUpload(r.URL.Path)

	return ok && (r.Method == http.MethodPost || r.Method == http.MethodPut)
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

	if b.downloadDenied(ctx, fr.repo.project, fr.repo.id, b.fileResource(pkg.image, f.GetOwner())) {
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
