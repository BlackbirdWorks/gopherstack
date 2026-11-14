package neptune

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
	"github.com/blackbirdworks/gopherstack/services/neptune/sparql"
)

// S3Reader is the slice of the S3 backend the RDF bulk loader and SPARQL LOAD need.
type S3Reader interface {
	GetObject(ctx context.Context, input *sdk_s3.GetObjectInput) (*sdk_s3.GetObjectOutput, error)
	ListObjectsV2(ctx context.Context, input *sdk_s3.ListObjectsV2Input) (*sdk_s3.ListObjectsV2Output, error)
}

var (
	errNoS3     = errors.New("S3 is not available in this emulator instance")
	errNotS3URI = errors.New("source is not an Amazon S3 URI")
)

// rdfService is one cluster's RDF data plane: the triple store, SPARQL query tracking and the bulk loader.
type rdfService struct {
	store   *sparql.Store
	queries *queryTracker
	loader  *loaderState
	graph   *opencypher.Graph
	s3      func() S3Reader
}

func newRDFService(s3 func() S3Reader) *rdfService {
	return &rdfService{
		store: sparql.NewStore(), queries: newQueryTracker(), loader: newLoaderState(), s3: s3,
	}
}

// SetS3Backend wires S3 so the Neptune bulk loader and SPARQL LOAD can read objects.
func (b *InMemoryBackend) SetS3Backend(s S3Reader) {
	b.mu.Lock("SetS3Backend")
	defer b.mu.Unlock()

	b.s3 = s
}

func (b *InMemoryBackend) s3Reader() S3Reader {
	b.mu.RLock("neptuneS3Reader")
	defer b.mu.RUnlock()

	return b.s3
}

// rdfLocked returns the cluster's RDF service, creating it on first use. Callers hold b.mu.
func (b *InMemoryBackend) rdfLocked(key string) *rdfService {
	r, ok := b.rdf[key]
	if !ok {
		r = newRDFService(b.s3Reader)
		b.rdf[key] = r
	}

	return r
}

func (b *InMemoryBackend) exportRDFLocked() map[string]string {
	out := make(map[string]string, len(b.rdf))

	for key, r := range b.rdf {
		if r.store.Len() > 0 {
			out[key] = string(r.store.Export())
		}
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

func (b *InMemoryBackend) importRDFLocked(ctx context.Context, dumps map[string]string) {
	b.rdf = make(map[string]*rdfService, len(dumps))

	for key, data := range dumps {
		r := newRDFService(b.s3Reader)
		if err := r.store.Import([]byte(data)); err != nil {
			logger.Load(ctx).WarnContext(ctx, "neptune: RDF store import failed", "cluster", key, "error", err)

			continue
		}

		b.rdf[key] = r
	}
}

// parseS3Source splits an s3:// or https S3 URL into bucket and key prefix.
func parseS3Source(src string) (string, string, error) {
	u, err := url.Parse(src)
	if err != nil {
		return "", "", fmt.Errorf("invalid source %q: %w", src, err)
	}

	switch u.Scheme {
	case "s3":
		return u.Host, strings.TrimPrefix(u.Path, "/"), nil
	case "http", "https":
		if host := u.Hostname(); host == "s3.amazonaws.com" || (strings.HasPrefix(host, "s3.") &&
			strings.HasSuffix(host, ".amazonaws.com")) {
			bucket, key, _ := strings.Cut(strings.TrimPrefix(u.Path, "/"), "/")

			return bucket, key, nil
		}
	}

	return "", "", fmt.Errorf("%w: %q", errNotS3URI, src)
}

// fetch reads the object behind an S3 URL (the SPARQL LOAD source).
func (r *rdfService) fetch(ctx context.Context, src string) ([]byte, error) {
	bucket, key, err := parseS3Source(src)
	if err != nil {
		return nil, err
	}

	return r.readObject(ctx, bucket, key)
}

func (r *rdfService) readObject(ctx context.Context, bucket, key string) ([]byte, error) {
	var s3 S3Reader
	if r.s3 != nil {
		s3 = r.s3()
	}

	if s3 == nil {
		return nil, errNoS3
	}

	out, err := s3.GetObject(ctx, &sdk_s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return nil, fmt.Errorf("read s3://%s/%s: %w", bucket, key, err)
	}

	defer out.Body.Close()

	return io.ReadAll(out.Body)
}
