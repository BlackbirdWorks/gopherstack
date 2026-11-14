package container

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	archivePathPrefix  = "/archives/"
	bearerPrefix       = "Bearer "
	codeReadHeaderWait = 10 * time.Second
	codeIDBytes        = 16
	codeTokenBytes     = 32
)

// ErrUnsafeArchive is returned for a zip with absolute or parent-escaping entry names.
var ErrUnsafeArchive = errors.New("archive has an unsafe entry name")

type codeBlob struct {
	token string
	data  []byte
}

// codeServer hands archives to the fetch initContainers of pods this process creates. Each pod gets its own
// token, valid only for its own archives and revoked once the pod is up or gone.
type codeServer struct {
	mu        *lockmetrics.RWMutex
	blobs     map[string]codeBlob
	ln        net.Listener
	srv       *http.Server
	advertise string
}

func newCodeServer(ln net.Listener, advertise string) *codeServer {
	return &codeServer{
		mu:        lockmetrics.New("container.k8s.code"),
		blobs:     map[string]codeBlob{},
		ln:        ln,
		advertise: advertise,
	}
}

func (c *codeServer) start() {
	c.srv = &http.Server{Handler: http.HandlerFunc(c.serve), ReadHeaderTimeout: codeReadHeaderWait}

	go func() { _ = c.srv.Serve(c.ln) }()
}

func (c *codeServer) close(ctx context.Context) {
	if c.srv != nil {
		_ = c.srv.Shutdown(ctx)
	}
}

func (c *codeServer) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, archivePathPrefix) {
		http.NotFound(w, r)

		return
	}

	id := strings.TrimPrefix(r.URL.Path, archivePathPrefix)

	c.mu.RLock("serve")
	blob, ok := c.blobs[id]
	c.mu.RUnlock()

	got := r.Header.Get("Authorization")
	if !ok || !strings.HasPrefix(got, bearerPrefix) ||
		subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(got, bearerPrefix)), []byte(blob.token)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)

		return
	}

	w.Header().Set("Content-Type", "application/zip")
	_, _ = w.Write(blob.data)
}

// grant is a pod's fetch credentials: the token and one URL per archive, in order.
type grant struct {
	token string
	urls  []string
	ids   []string
}

// register stores the archives under a fresh token and returns the credentials to give the pod.
func (c *codeServer) register(archives []Archive) (grant, error) {
	token, err := randomToken(codeTokenBytes)
	if err != nil {
		return grant{}, err
	}

	g := grant{token: token}

	c.mu.Lock("register")
	defer c.mu.Unlock()

	for _, a := range archives {
		id, idErr := randomToken(codeIDBytes)
		if idErr != nil {
			for _, done := range g.ids {
				delete(c.blobs, done)
			}

			return grant{}, idErr
		}

		c.blobs[id] = codeBlob{token: token, data: a.Zip}
		g.ids = append(g.ids, id)
		g.urls = append(g.urls, "http://"+c.advertise+archivePathPrefix+id)
	}

	return g, nil
}

func (c *codeServer) revoke(g grant) {
	c.mu.Lock("revoke")
	defer c.mu.Unlock()

	for _, id := range g.ids {
		delete(c.blobs, id)
	}
}

func randomToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("random token: %w", err)
	}

	return hex.EncodeToString(b), nil
}

// validateArchive rejects zips whose entries could escape the extraction directory.
func validateArchive(data []byte) error {
	r, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return fmt.Errorf("open archive: %w", err)
	}

	for _, f := range r.File {
		name := strings.ReplaceAll(f.Name, "\\", "/")
		if strings.HasPrefix(name, "/") || path.Clean(name) == ".." || strings.HasPrefix(path.Clean(name), "../") {
			return fmt.Errorf("%w: %q", ErrUnsafeArchive, f.Name)
		}
	}

	return nil
}
