package gcpcloudbuild_test

import (
	"context"
	"crypto/md5"  //nolint:gosec // asserting the documented MD5 hash type
	"crypto/sha1" //nolint:gosec // gcs-fetcher manifests carry SHA-1
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
)

const storeGeneration = 7

// genStore is a fake object store that reports object generations.
type genStore struct{ *fakeStore }

func (g genStore) ReadObjectGeneration(bucket, object string, gen int64) ([]byte, int64, error) {
	if gen != 0 && gen != storeGeneration {
		return nil, 0, errors.New("generation not found")
	}

	data, err := g.ReadObject(bucket, object)

	return data, storeGeneration, err
}

func sha1Hex(s string) string {
	sum := sha1.Sum([]byte(s)) //nolint:gosec // test data

	return hex.EncodeToString(sum[:])
}

func manifestSource(gen int64) *cloudbuildpb.Source {
	return &cloudbuildpb.Source{Source: &cloudbuildpb.Source_StorageSourceManifest{
		StorageSourceManifest: &cloudbuildpb.StorageSourceManifest{Bucket: "m", Object: "manifest.json", Generation: gen},
	}}
}

func TestStorageSourceManifest(t *testing.T) {
	t.Parallel()

	good := fmt.Sprintf(`{"Dockerfile":{"sourceUrl":"gs://m/blob1","sha1sum":%q},`+
		`"cmd/main.go":{"sourceUrl":"gs://m/blob2#7","sha1sum":"<sha-1 digest>"}}`, sha1Hex("FROM scratch"))

	tests := []struct {
		name     string
		manifest string
		gen      int64
		wantErr  string
	}{
		{name: "materialised", manifest: good},
		{name: "pinned generation", manifest: good, gen: storeGeneration},
		{name: "unknown generation", manifest: good, gen: 99, wantErr: "read manifest gs://m/manifest.json"},
		{name: "not json", manifest: `[1]`, wantErr: "invalid source manifest"},
		{
			name:     "sha1 mismatch",
			manifest: `{"Dockerfile":{"sourceUrl":"gs://m/blob1","sha1sum":"` + strings.Repeat("0", 40) + `"}}`,
			wantErr:  "sha1sum mismatch",
		},
		{
			name:     "escaping path",
			manifest: `{"../evil":{"sourceUrl":"gs://m/blob1"}}`,
			wantErr:  "escapes the workspace",
		},
		{name: "bad url", manifest: `{"a":{"sourceUrl":"http://x/y"}}`, wantErr: "invalid source manifest"},
		{name: "missing blob", manifest: `{"a":{"sourceUrl":"gs://m/none"}}`, wantErr: "read gs://m/none"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var seen string

			e := newEnv(t, map[string]behavior{"img": func(_ context.Context, spec container.Spec, _ io.Writer) int {
				host, _, _ := strings.Cut(spec.Mounts[0], ":")
				a, _ := os.ReadFile(filepath.Join(host, "Dockerfile"))
				b, _ := os.ReadFile(filepath.Join(host, "cmd", "main.go"))
				seen = string(a) + "|" + string(b)

				return 0
			}})
			e.configure(func(s *gcpcloudbuild.Services) { s.Store = genStore{e.store} })

			require.NoError(t, e.store.WriteObject("m", "manifest.json", []byte(tt.manifest), ""))
			require.NoError(t, e.store.WriteObject("m", "blob1", []byte("FROM scratch"), ""))
			require.NoError(t, e.store.WriteObject("m", "blob2", []byte("package main"), ""))

			b := buildOf(step("img"))
			b.Source = manifestSource(tt.gen)
			b.Options = &cloudbuildpb.BuildOptions{SourceProvenanceHash: []cloudbuildpb.Hash_HashType{
				cloudbuildpb.Hash_SHA256, cloudbuildpb.Hash_MD5,
			}}

			got := e.run(t, b)

			if tt.wantErr != "" {
				assert.Equal(t, cloudbuildpb.Build_FAILURE, got.GetStatus())
				assert.Contains(t, got.GetStatusDetail(), tt.wantErr)
				assert.Equal(t, cloudbuildpb.Build_FailureInfo_FETCH_SOURCE_FAILED, got.GetFailureInfo().GetType())

				return
			}

			require.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus(), got.GetStatusDetail())
			assert.Equal(t, "FROM scratch|package main", seen)

			prov := got.GetSourceProvenance()
			assert.Equal(t, int64(storeGeneration), prov.GetResolvedStorageSourceManifest().GetGeneration())

			fh := prov.GetFileHashes()["gs://m/manifest.json#7"].GetFileHash()
			require.Len(t, fh, 2)

			sum := sha256.Sum256([]byte(tt.manifest))
			md := md5.Sum([]byte(tt.manifest)) //nolint:gosec // see import
			assert.Equal(t, cloudbuildpb.Hash_SHA256, fh[0].GetType())
			assert.Equal(t, sum[:], fh[0].GetValue())
			assert.Equal(t, cloudbuildpb.Hash_MD5, fh[1].GetType())
			assert.Equal(t, md[:], fh[1].GetValue())
			assert.NotNil(t, got.GetTiming()["FETCHSOURCE"])
		})
	}
}

func TestStorageSourceProvenance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		hashes   []cloudbuildpb.Hash_HashType
		store    bool
		wantKey  string
		wantGen  int64
		wantHash int
	}{
		{name: "generation and hashes", store: true, wantKey: "gs://s/a.tar.gz#7", wantGen: storeGeneration,
			hashes: []cloudbuildpb.Hash_HashType{cloudbuildpb.Hash_SHA256, cloudbuildpb.Hash_MD5}, wantHash: 2},
		{name: "no hashes requested", store: true, wantGen: storeGeneration},
		{name: "store without generations", wantKey: "gs://s/a.tar.gz",
			hashes: []cloudbuildpb.Hash_HashType{cloudbuildpb.Hash_SHA256}, wantHash: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, map[string]behavior{"img": exits(0)})
			if tt.store {
				e.configure(func(s *gcpcloudbuild.Services) { s.Store = genStore{e.store} })
			}

			archive := tarGz(t, map[string]string{"f": "x"})
			require.NoError(t, e.store.WriteObject("s", "a.tar.gz", archive, ""))

			b := buildOf(step("img"))
			b.Source = &cloudbuildpb.Source{Source: &cloudbuildpb.Source_StorageSource{
				StorageSource: &cloudbuildpb.StorageSource{Bucket: "s", Object: "a.tar.gz"},
			}}
			b.Options = &cloudbuildpb.BuildOptions{SourceProvenanceHash: tt.hashes}

			got := e.run(t, b)
			require.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus(), got.GetStatusDetail())

			prov := got.GetSourceProvenance()
			assert.Equal(t, tt.wantGen, prov.GetResolvedStorageSource().GetGeneration())
			assert.Equal(t, "a.tar.gz", prov.GetResolvedStorageSource().GetObject())

			if tt.wantHash == 0 {
				assert.Empty(t, prov.GetFileHashes())

				return
			}

			assert.Len(t, prov.GetFileHashes()[tt.wantKey].GetFileHash(), tt.wantHash)
		})
	}
}
