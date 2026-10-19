package pods

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// Format identifies the pod archive layout.
const Format = "gopherstack-pod/v1"

const (
	manifestName    = "manifest.json"
	serviceDir      = "services/"
	maxEntryBytes   = 2 << 30
	versionDigits   = 6
	zipExt          = ".zip"
	manifestExt     = ".json"
	versionKeyStart = "v"
)

var (
	// ErrInvalidName is returned for pod names that are unsafe as storage keys.
	ErrInvalidName = errors.New("invalid pod name")
	// ErrCorrupt is returned when a stored pod archive cannot be read.
	ErrCorrupt = errors.New("pod archive is corrupt")
	// ErrBucketRequired is returned when an S3 store is configured without a bucket.
	ErrBucketRequired = errors.New("pods: s3 bucket is required")

	nameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
)

// Manifest describes one saved pod version.
type Manifest struct {
	CreatedAt time.Time `json:"createdAt"`
	Name      string    `json:"name"`
	Format    string    `json:"format"`
	Message   string    `json:"message,omitempty"`
	Services  []string  `json:"services"`
	Version   int       `json:"version"`
	Size      int64     `json:"size"`
}

// Summary is one entry of the pod listing.
type Summary struct {
	LatestAt time.Time `json:"latestAt"`
	Name     string    `json:"name"`
	Versions int       `json:"versions"`
	Latest   int       `json:"latest"`
}

// Service implements pod operations over a BlobStore.
type Service struct {
	store BlobStore
	mu    *lockmetrics.RWMutex
	now   func() time.Time
}

// New returns a Service backed by store.
func New(store BlobStore) *Service {
	return &Service{store: store, mu: lockmetrics.New("pods"), now: time.Now}
}

// ValidName reports whether name is a legal pod name.
func ValidName(name string) bool { return nameRe.MatchString(name) }

func zipKey(name string, v int) string {
	return fmt.Sprintf("%s/v%0*d%s", name, versionDigits, v, zipExt)
}
func manifestKey(name string, v int) string {
	return fmt.Sprintf("%s/v%0*d%s", name, versionDigits, v, manifestExt)
}

func parseVersion(name, key string) (int, bool) {
	rest, ok := strings.CutPrefix(key, name+"/"+versionKeyStart)
	if !ok {
		return 0, false
	}

	rest, ok = strings.CutSuffix(rest, manifestExt)
	if !ok {
		return 0, false
	}

	v, err := strconv.Atoi(rest)

	return v, err == nil && v > 0
}

func (s *Service) versionNumbers(ctx context.Context, name string) ([]int, error) {
	keys, err := s.store.List(ctx, name+"/")
	if err != nil {
		return nil, fmt.Errorf("list pod versions: %w", err)
	}

	var out []int

	for _, k := range keys {
		if v, ok := parseVersion(name, k); ok {
			out = append(out, v)
		}
	}

	sort.Ints(out)

	return out, nil
}

// Save stores snaps (service name to raw snapshot) as the next version of pod name.
func (s *Service) Save(ctx context.Context, name, message string, snaps map[string][]byte) (Manifest, error) {
	if !ValidName(name) {
		return Manifest{}, ErrInvalidName
	}

	s.mu.Lock("Save")
	defer s.mu.Unlock()

	vers, err := s.versionNumbers(ctx, name)
	if err != nil {
		return Manifest{}, err
	}

	next := 1
	if len(vers) > 0 {
		next = vers[len(vers)-1] + 1
	}

	names := make([]string, 0, len(snaps))
	for n := range snaps {
		names = append(names, n)
	}

	sort.Strings(names)

	m := Manifest{
		Name: name, Version: next, CreatedAt: s.now().UTC(), Message: message,
		Services: names, Format: Format,
	}

	archive, err := buildArchive(&m, snaps)
	if err != nil {
		return Manifest{}, err
	}

	m.Size = int64(len(archive))

	meta, err := json.Marshal(m)
	if err != nil {
		return Manifest{}, fmt.Errorf("marshal manifest: %w", err)
	}

	if err = s.store.Put(ctx, zipKey(name, next), archive); err != nil {
		return Manifest{}, fmt.Errorf("store pod archive: %w", err)
	}

	if err = s.store.Put(ctx, manifestKey(name, next), meta); err != nil {
		_ = s.store.Delete(ctx, zipKey(name, next))

		return Manifest{}, fmt.Errorf("store pod manifest: %w", err)
	}

	return m, nil
}

func buildArchive(m *Manifest, snaps map[string][]byte) ([]byte, error) {
	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	meta, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal manifest: %w", err)
	}

	files := map[string][]byte{manifestName: meta}
	for n, d := range snaps {
		files[serviceDir+n+".json"] = d
	}

	order := make([]string, 0, len(files))
	for f := range files {
		order = append(order, f)
	}

	sort.Strings(order)

	for _, f := range order {
		w, cErr := zw.Create(f)
		if cErr != nil {
			return nil, fmt.Errorf("zip create: %w", cErr)
		}

		if _, wErr := w.Write(files[f]); wErr != nil {
			return nil, fmt.Errorf("zip write: %w", wErr)
		}
	}

	if err = zw.Close(); err != nil {
		return nil, fmt.Errorf("zip close: %w", err)
	}

	return buf.Bytes(), nil
}

// resolve turns version 0 into the latest version number.
func (s *Service) resolve(ctx context.Context, name string, version int) (int, error) {
	if !ValidName(name) {
		return 0, ErrInvalidName
	}

	vers, err := s.versionNumbers(ctx, name)
	if err != nil {
		return 0, err
	}

	if len(vers) == 0 {
		return 0, ErrNotFound
	}

	if version <= 0 {
		return vers[len(vers)-1], nil
	}

	for _, v := range vers {
		if v == version {
			return v, nil
		}
	}

	return 0, ErrNotFound
}

// Inspect returns the manifest of a version (0 = latest).
func (s *Service) Inspect(ctx context.Context, name string, version int) (Manifest, error) {
	s.mu.RLock("Inspect")
	defer s.mu.RUnlock()

	v, err := s.resolve(ctx, name, version)
	if err != nil {
		return Manifest{}, err
	}

	return s.readManifest(ctx, name, v)
}

func (s *Service) readManifest(ctx context.Context, name string, v int) (Manifest, error) {
	raw, err := s.store.Get(ctx, manifestKey(name, v))
	if err != nil {
		return Manifest{}, err
	}

	var m Manifest
	if err = json.Unmarshal(raw, &m); err != nil {
		return Manifest{}, fmt.Errorf("%w: manifest: %w", ErrCorrupt, err)
	}

	return m, nil
}

// Load returns the manifest and per-service snapshots of a version (0 = latest).
// When services is non-empty only those services are returned.
func (s *Service) Load(
	ctx context.Context, name string, version int, services []string,
) (Manifest, map[string][]byte, error) {
	s.mu.RLock("Load")
	defer s.mu.RUnlock()

	v, err := s.resolve(ctx, name, version)
	if err != nil {
		return Manifest{}, nil, err
	}

	m, err := s.readManifest(ctx, name, v)
	if err != nil {
		return Manifest{}, nil, err
	}

	raw, err := s.store.Get(ctx, zipKey(name, v))
	if err != nil {
		return Manifest{}, nil, err
	}

	snaps, err := readArchive(raw)
	if err != nil {
		return Manifest{}, nil, err
	}

	if len(services) > 0 {
		want := make(map[string]bool, len(services))
		for _, n := range services {
			want[strs.Fold(n)] = true
		}

		for n := range snaps {
			if !want[strs.Fold(n)] {
				delete(snaps, n)
			}
		}

		have := make(map[string]bool, len(snaps))
		for n := range snaps {
			have[strs.Fold(n)] = true
		}

		for _, n := range services {
			if !have[strs.Fold(n)] {
				return Manifest{}, nil, fmt.Errorf("%w: service %q not in pod %s v%d", ErrNotFound, n, name, v)
			}
		}
	}

	return m, snaps, nil
}

func readArchive(raw []byte) (map[string][]byte, error) {
	zr, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCorrupt, err)
	}

	snaps := make(map[string][]byte)

	for _, f := range zr.File {
		n, ok := strings.CutPrefix(f.Name, serviceDir)
		if !ok {
			continue
		}

		n, ok = strings.CutSuffix(n, ".json")
		if !ok || n == "" || strings.Contains(n, "/") {
			continue
		}

		rc, oErr := f.Open()
		if oErr != nil {
			return nil, fmt.Errorf("%w: %w", ErrCorrupt, oErr)
		}

		data, rErr := io.ReadAll(io.LimitReader(rc, maxEntryBytes))
		_ = rc.Close()

		if rErr != nil {
			return nil, fmt.Errorf("%w: %w", ErrCorrupt, rErr)
		}

		snaps[n] = data
	}

	return snaps, nil
}

// Versions lists all manifests of a pod, oldest first.
func (s *Service) Versions(ctx context.Context, name string) ([]Manifest, error) {
	s.mu.RLock("Versions")
	defer s.mu.RUnlock()

	if !ValidName(name) {
		return nil, ErrInvalidName
	}

	vers, err := s.versionNumbers(ctx, name)
	if err != nil {
		return nil, err
	}

	if len(vers) == 0 {
		return nil, ErrNotFound
	}

	out := make([]Manifest, 0, len(vers))

	for _, v := range vers {
		m, mErr := s.readManifest(ctx, name, v)
		if mErr != nil {
			return nil, mErr
		}

		out = append(out, m)
	}

	return out, nil
}

// List summarises every pod in the store.
func (s *Service) List(ctx context.Context) ([]Summary, error) {
	s.mu.RLock("List")
	defer s.mu.RUnlock()

	keys, err := s.store.List(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}

	seen := map[string]bool{}

	var names []string

	for _, k := range keys {
		n, _, ok := strings.Cut(k, "/")
		if ok && ValidName(n) && !seen[n] {
			seen[n] = true
			names = append(names, n)
		}
	}

	out := make([]Summary, 0, len(names))

	for _, n := range names {
		vers, vErr := s.versionNumbers(ctx, n)
		if vErr != nil {
			return nil, vErr
		}

		if len(vers) == 0 {
			continue
		}

		latest := vers[len(vers)-1]
		sum := Summary{Name: n, Versions: len(vers), Latest: latest}

		if m, mErr := s.readManifest(ctx, n, latest); mErr == nil {
			sum.LatestAt = m.CreatedAt
		}

		out = append(out, sum)
	}

	return out, nil
}

// Delete removes one version, or the whole pod when version is 0.
func (s *Service) Delete(ctx context.Context, name string, version int) error {
	s.mu.Lock("Delete")
	defer s.mu.Unlock()

	if !ValidName(name) {
		return ErrInvalidName
	}

	vers, err := s.versionNumbers(ctx, name)
	if err != nil {
		return err
	}

	if len(vers) == 0 {
		return ErrNotFound
	}

	if version > 0 {
		found := false

		for _, v := range vers {
			found = found || v == version
		}

		if !found {
			return ErrNotFound
		}

		vers = []int{version}
	}

	for _, v := range vers {
		if err = s.store.Delete(ctx, manifestKey(name, v)); err != nil {
			return err
		}

		if err = s.store.Delete(ctx, zipKey(name, v)); err != nil {
			return err
		}
	}

	return nil
}
