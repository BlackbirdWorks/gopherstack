package gcpcloudbuild

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

var (
	errManifest     = errors.New("invalid source manifest")
	errConnectedSrc = errors.New(
		"connectedRepository needs a Cloud Build repository connection " +
			"(projects/*/locations/*/connections/*/repositories/*); the emulator has no connections API to resolve it",
	)
)

// manifestEntry is one destination file of a gcs-fetcher source manifest.
type manifestEntry struct {
	SourceURL string `json:"sourceUrl"`
	SHA1Sum   string `json:"sha1sum"` // accepted; gcs-fetcher itself does not verify it yet
}

// readObject reads a GCS object at an optional generation and reports the generation read (0 when the store has
// no generations).
func (r *runner) readObject(bucket, object string, gen int64) ([]byte, int64, error) {
	if r.svc.Store == nil {
		return nil, 0, errNoStore
	}

	if g, ok := r.svc.Store.(GenerationReader); ok {
		return g.ReadObjectGeneration(bucket, object, gen) //nolint:wrapcheck // names the object already
	}

	data, err := r.svc.Store.ReadObject(bucket, object)

	return data, 0, err //nolint:wrapcheck // names the object already
}

func (r *runner) setProvenance(fn func(*cloudbuildpb.SourceProvenance)) {
	r.b.update(r.ref, func(pb *cloudbuildpb.Build) {
		if pb.GetSourceProvenance() == nil {
			pb.SourceProvenance = &cloudbuildpb.SourceProvenance{}
		}

		fn(pb.GetSourceProvenance())
	})
}

func gsKey(bucket, object string, gen int64) string {
	k := "gs://" + bucket + "/" + object
	if gen > 0 {
		k += "#" + strconv.FormatInt(gen, 10)
	}

	return k
}

func (r *runner) recordHashes(key string, data []byte) {
	fh := r.hashSet(data)
	if fh == nil {
		return
	}

	r.setProvenance(func(p *cloudbuildpb.SourceProvenance) {
		if p.FileHashes == nil {
			p.FileHashes = map[string]*cloudbuildpb.FileHashes{}
		}

		p.FileHashes[key] = fh
	})
}

func (r *runner) fetchStorage(ctx context.Context, ss *cloudbuildpb.StorageSource) error {
	r.log.system(fmt.Sprintf("Fetching storage object: gs://%s/%s", ss.GetBucket(), ss.GetObject()))

	data, gen, err := r.readObject(ss.GetBucket(), ss.GetObject(), ss.GetGeneration())
	if err != nil {
		return fmt.Errorf("read gs://%s/%s: %w", ss.GetBucket(), ss.GetObject(), err)
	}

	if err = ctx.Err(); err != nil {
		return fmt.Errorf("fetch source: %w", err)
	}

	if err = extractArchive(data, r.work); err != nil {
		return err
	}

	r.setProvenance(func(p *cloudbuildpb.SourceProvenance) {
		p.ResolvedStorageSource = &cloudbuildpb.StorageSource{
			Bucket: ss.GetBucket(), Object: ss.GetObject(), Generation: gen,
			SourceFetcher: ss.GetSourceFetcher(),
		}
	})
	r.recordHashes(gsKey(ss.GetBucket(), ss.GetObject(), gen), data)

	return nil
}

// fetchManifest materialises a gcs-fetcher source manifest: a JSON object mapping destination paths to
// {"sourceUrl": "gs://bucket/object[#generation]", "sha1sum": "<hex>"}.
func (r *runner) fetchManifest(ctx context.Context, m *cloudbuildpb.StorageSourceManifest) error {
	r.log.system(fmt.Sprintf("Fetching storage source manifest: gs://%s/%s", m.GetBucket(), m.GetObject()))

	raw, gen, err := r.readObject(m.GetBucket(), m.GetObject(), m.GetGeneration())
	if err != nil {
		return fmt.Errorf("read manifest gs://%s/%s: %w", m.GetBucket(), m.GetObject(), err)
	}

	var entries map[string]manifestEntry
	if err = json.Unmarshal(raw, &entries, jsonx.V1()); err != nil {
		return fmt.Errorf("%w: %w", errManifest, err)
	}

	names := make([]string, 0, len(entries))
	for n := range entries {
		names = append(names, n)
	}

	sort.Strings(names)

	e := &extractor{dest: r.work}

	for _, name := range names {
		if err = ctx.Err(); err != nil {
			return fmt.Errorf("fetch source: %w", err)
		}

		if err = r.manifestFile(e, name, entries[name]); err != nil {
			return err
		}
	}

	r.setProvenance(func(p *cloudbuildpb.SourceProvenance) {
		p.ResolvedStorageSourceManifest = &cloudbuildpb.StorageSourceManifest{
			Bucket: m.GetBucket(), Object: m.GetObject(), Generation: gen,
		}
	})
	r.recordHashes(gsKey(m.GetBucket(), m.GetObject(), gen), raw)

	return nil
}

func (r *runner) manifestFile(e *extractor, name string, ent manifestEntry) error {
	u, err := url.Parse(ent.SourceURL)
	if err != nil || u.Scheme != "gs" || u.Host == "" || strings.TrimPrefix(u.Path, "/") == "" {
		return fmt.Errorf("%w: %q has sourceUrl %q", errManifest, name, ent.SourceURL)
	}

	var gen int64

	if u.Fragment != "" {
		if gen, err = strconv.ParseInt(u.Fragment, 10, 64); err != nil {
			return fmt.Errorf("%w: %q has generation %q", errManifest, name, u.Fragment)
		}
	}

	data, _, err := r.readObject(u.Host, strings.TrimPrefix(u.Path, "/"), gen)
	if err != nil {
		return fmt.Errorf("read %s for %q: %w", ent.SourceURL, name, err)
	}

	return e.writeData(name, data)
}

// writeData stores an in-memory file under the extractor's destination with the same checks as archive entries.
func (e *extractor) writeData(name string, data []byte) error {
	if err := e.account(int64(len(data))); err != nil {
		return err
	}

	path, err := target(e.dest, name)
	if err != nil || path == e.dest {
		return fmt.Errorf("%w: %q", errUnsafePath, name)
	}

	if err = os.MkdirAll(filepath.Dir(path), dirPerm); err != nil {
		return fmt.Errorf("create %q: %w", filepath.Dir(name), err)
	}

	if err = os.WriteFile(path, data, filePerm); err != nil {
		return fmt.Errorf("write %q: %w", name, err)
	}

	return nil
}
