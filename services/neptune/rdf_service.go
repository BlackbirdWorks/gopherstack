package neptune

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
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

	errNotLoadURI      = errors.New("LOAD source must be an http(s) or Amazon S3 URL")
	errLoadFetch       = errors.New("could not fetch the LOAD source")
	errNotServiceURI   = errors.New("SERVICE endpoint must be an http(s) URL")
	errServiceFailed   = errors.New("remote SPARQL endpoint rejected the query")
	errLoadDestination = errors.New("host is not permitted; set NEPTUNE_ALLOW_EXTERNAL_HTTP=true")
)

const (
	schemeHTTP           = "http"
	schemeHTTPS          = "https"
	envAllowExternalHTTP = "NEPTUNE_ALLOW_EXTERNAL_HTTP"
	loadMaxBytes         = 1 << 30
	loadFetchTimeout     = 5 * time.Minute
)

// rdfService is one cluster's RDF data plane: the triple store, SPARQL query tracking and the bulk loader.
type rdfService struct {
	store    *sparql.Store
	queries  *queryTracker
	loader   *loaderState
	graph    *opencypher.Graph
	s3       func() S3Reader
	web      *http.Client
	roleAuth func() roleauth.Authorizer
	info     func() engineInfo
	rdfStats *statsState
	rebuilds *rebuildState
	pgStats  *statsState
	cpus     func() int
	pgIDs    pgIDSpaces
	webOnce  sync.Once
}

func newRDFService(s3 func() S3Reader) *rdfService {
	return &rdfService{
		store: sparql.NewStore(), queries: newQueryTracker(), loader: newLoaderState(), s3: s3,
		rdfStats: newStatsState(), pgStats: newStatsState(), rebuilds: newRebuildState(),
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

func (b *InMemoryBackend) newClusterRDF() *rdfService {
	r := newRDFService(b.s3Reader)
	r.roleAuth, r.cpus = b.roleAuthorizer, b.loaderCPUCount

	return r
}

// rdfLocked returns the cluster's RDF service, creating it on first use. Callers hold b.mu.
func (b *InMemoryBackend) rdfLocked(key string) *rdfService {
	r, ok := b.rdf[key]
	if !ok {
		r = b.newClusterRDF()
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
		r := b.newClusterRDF()
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
	case schemeHTTP, schemeHTTPS:
		if host := u.Hostname(); host == "s3.amazonaws.com" || (strings.HasPrefix(host, "s3.") &&
			strings.HasSuffix(host, ".amazonaws.com")) {
			bucket, key, _ := strings.Cut(strings.TrimPrefix(u.Path, "/"), "/")

			return bucket, key, nil
		}
	}

	return "", "", fmt.Errorf("%w: %q", errNotS3URI, src)
}

// fetchDoc reads a SPARQL LOAD source: an S3 object, or an http(s) document on a web server.
func (r *rdfService) fetchDoc(ctx context.Context, src string) (sparql.Document, error) {
	if bucket, key, err := parseS3Source(src); err == nil {
		return r.readObjectDoc(ctx, bucket, key)
	}

	u, err := url.Parse(src)
	if err != nil || (u.Scheme != schemeHTTP && u.Scheme != schemeHTTPS) || u.Host == "" {
		return sparql.Document{}, fmt.Errorf("%w: %q", errNotLoadURI, src)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src, http.NoBody)
	if err != nil {
		return sparql.Document{}, err
	}

	resp, err := r.webClient().Do(req)
	if err != nil {
		return sparql.Document{}, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return sparql.Document{}, fmt.Errorf("%w: HTTP %d", errLoadFetch, resp.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, loadMaxBytes))

	return sparql.Document{Data: data, ContentType: resp.Header.Get("Content-Type")}, err
}

// service runs one SERVICE sub-query against a remote SPARQL endpoint (read-only: SELECT results only).
func (r *rdfService) service(ctx context.Context, endpoint, query string) (*sparql.Result, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != schemeHTTP && u.Scheme != schemeHTTPS) || u.Host == "" {
		return nil, fmt.Errorf("%w: %q", errNotServiceURI, endpoint)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint,
		strings.NewReader(url.Values{"query": {query}}.Encode()))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", mimeForm)
	req.Header.Set("Accept", mimeSparqlJSON)

	resp, err := r.webClient().Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, loadMaxBytes))
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: HTTP %d", errServiceFailed, resp.StatusCode)
	}

	return sparql.ParseSelectJSON(body)
}

func (r *rdfService) cpuCount() int {
	if r.cpus != nil {
		return r.cpus()
	}

	return runtime.NumCPU()
}

func (r *rdfService) webClient() *http.Client {
	r.webOnce.Do(func() {
		tr := httpclient.NewTransport()
		tr.DialContext = httpclient.LoopbackDialer(loadExternalAllowed, errLoadDestination).DialContext
		r.web = &http.Client{Transport: tr, Timeout: loadFetchTimeout}
	})

	return r.web
}

func loadExternalAllowed() bool { return strings.EqualFold(os.Getenv(envAllowExternalHTTP), "true") }

func (r *rdfService) readObjectDoc(ctx context.Context, bucket, key string) (sparql.Document, error) {
	var s3 S3Reader
	if r.s3 != nil {
		s3 = r.s3()
	}

	if s3 == nil {
		return sparql.Document{}, errNoS3
	}

	out, err := s3.GetObject(ctx, &sdk_s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return sparql.Document{}, fmt.Errorf("read s3://%s/%s: %w", bucket, key, err)
	}

	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)

	return sparql.Document{Data: data, ContentType: aws.ToString(out.ContentType)}, err
}
