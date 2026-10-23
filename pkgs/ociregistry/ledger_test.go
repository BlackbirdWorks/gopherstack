package ociregistry_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
)

func epochTime() time.Time { return time.Unix(1700000000, 0).UTC() }

func TestLedgerRecord(t *testing.T) {
	t.Parallel()

	l := ociregistry.Ledger{}
	l.RecordPut("app", "v1", "sha256:a", epochTime())
	l.RecordPut("app", "v2", "sha256:b", epochTime().Add(time.Minute))
	l.RecordPut("app", "sha256:c", "sha256:c", epochTime().Add(2*time.Minute))
	l.RecordPut("app", "v1", "sha256:b", epochTime().Add(3*time.Minute))
	l.RecordPut("ignored", "v1", "", epochTime())

	r := l["app"]
	require.NotNil(t, r)
	assert.NotContains(t, l, "ignored")
	assert.Len(t, r.Manifests, 3)
	assert.Equal(t, "sha256:b", r.Tags["v1"].Digest, "a retagged tag moves")
	assert.Equal(t, epochTime(), r.Tags["v1"].Created, "a retagged tag keeps its creation time")
	assert.Equal(t, epochTime().Add(3*time.Minute), r.Tags["v1"].Updated)
	assert.Equal(t, epochTime().Add(3*time.Minute), r.Manifests["sha256:b"].Updated)
	assert.Equal(t, epochTime().Add(time.Minute), r.Manifests["sha256:b"].Created)
	assert.Len(t, r.Tags, 2, "pushing by digest creates no tag")

	l.RecordDelete("app", "v2")
	assert.Len(t, l["app"].Tags, 1)
	assert.Len(t, l["app"].Manifests, 3, "deleting a tag keeps the manifest")

	l.RecordDelete("app", "sha256:b")
	assert.Empty(t, l["app"].Tags, "deleting a manifest drops its tags")

	l.RecordDelete("app", "sha256:a")
	l.RecordDelete("app", "sha256:c")
	assert.NotContains(t, l, "app", "a repository disappears with its last manifest")

	l.RecordDelete("never", "x")
}

func TestLedgerAllowed(t *testing.T) {
	t.Parallel()

	l := ociregistry.Ledger{}
	l.RecordPut("app", "v1", "sha256:a", epochTime())
	l["app"].Tags["v1"].Attrs.DeleteDisabled = true
	l["app"].Manifests["sha256:a"].Attrs.WriteDisabled = true

	tests := []struct {
		name string
		repo string
		ref  string
		op   string
		want bool
	}{
		{"locked tag delete", "app", "v1", ociregistry.OpDelete, false},
		{"digest delete honours tags", "app", "sha256:a", ociregistry.OpDelete, false},
		{"locked manifest write", "app", "sha256:a", ociregistry.OpWrite, false},
		{"unlocked tag write", "app", "v1", ociregistry.OpWrite, true},
		{"read", "app", "v1", ociregistry.OpRead, true},
		{"unknown tag", "app", "other", ociregistry.OpDelete, true},
		{"unknown repo", "nope", "v1", ociregistry.OpDelete, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, l.Allowed(tt.repo, tt.ref, tt.op))
		})
	}
}

func TestLedgerRepositoryAttrs(t *testing.T) {
	t.Parallel()

	l := ociregistry.Ledger{}
	l.RecordPut("app", "v1", "sha256:a", epochTime())
	l["app"].Attrs.ReadDisabled = true

	assert.False(t, l.Allowed("app", "v1", ociregistry.OpRead), "repository attributes apply to every reference")
	assert.True(t, l.Allowed("app", "v1", ociregistry.OpWrite))
}

func TestLedgerNormalize(t *testing.T) {
	t.Parallel()

	ok := ociregistry.Ledger{"a": &ociregistry.RepoEntry{}}
	require.NoError(t, ok.Normalize())
	assert.NotNil(t, ok["a"].Manifests)
	assert.NotNil(t, ok["a"].Tags)

	require.Error(t, ociregistry.Ledger{"a": nil}.Normalize())
}

func TestSplitManifestPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path string
		repo string
		ref  string
		ok   bool
	}{
		{"/v2/app/manifests/v1", "app", "v1", true},
		{"/v2/team/app/manifests/sha256:abc", "team/app", "sha256:abc", true},
		{"/v2/a/manifests/b/manifests/c", "a/manifests/b", "c", true},
		{"/v2/app/blobs/sha256:abc", "", "", false},
		{"/v2/manifests/x", "", "", false},
		{"/v2/", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()

			repo, ref, ok := ociregistry.SplitManifestPath(tt.path)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.repo, repo)
			assert.Equal(t, tt.ref, ref)
		})
	}
}

type observed struct {
	name       string
	method     string
	path       string
	wantPut    string
	wantDelete string
	wantOp     string
	status     int
	wantCode   int
	deny       bool
}

func TestServeObserved(t *testing.T) {
	t.Parallel()

	reply := func(status int) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Docker-Content-Digest", "sha256:d")
			w.WriteHeader(status)
		})
	}

	const manifest = "/v2/app/manifests/v1"

	tests := []observed{
		{
			name: "put 201", method: http.MethodPut, path: manifest, status: http.StatusCreated,
			wantPut: "app v1 sha256:d", wantOp: ociregistry.OpWrite, wantCode: http.StatusCreated,
		},
		{
			name: "put failed", method: http.MethodPut, path: manifest, status: http.StatusBadRequest,
			wantOp: ociregistry.OpWrite, wantCode: http.StatusBadRequest,
		},
		{
			name: "delete 202", method: http.MethodDelete, path: manifest, status: http.StatusAccepted,
			wantDelete: "app v1", wantOp: ociregistry.OpDelete, wantCode: http.StatusAccepted,
		},
		{
			name: "get", method: http.MethodGet, path: manifest, status: http.StatusOK,
			wantOp: ociregistry.OpRead, wantCode: http.StatusOK,
		},
		{
			name: "not a manifest", method: http.MethodPut, path: "/v2/app/blobs/uploads/",
			status: http.StatusCreated, wantCode: http.StatusCreated,
		},
		{
			name: "denied", method: http.MethodDelete, path: manifest, status: http.StatusAccepted,
			deny: true, wantOp: ociregistry.OpDelete, wantCode: http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var put, del, op string

			obs := ociregistry.Observer{
				Permit: func(w http.ResponseWriter, _, _, o string) bool {
					op = o
					if tt.deny {
						w.WriteHeader(http.StatusForbidden)
					}

					return !tt.deny
				},
				Put:    func(repo, ref, digest string) { put = repo + " " + ref + " " + digest },
				Delete: func(repo, ref string) { del = repo + " " + ref },
			}

			req := httptest.NewRequestWithContext(t.Context(), tt.method, tt.path, http.NoBody)
			rec := httptest.NewRecorder()
			ociregistry.ServeObserved(reply(tt.status), rec, req, obs)

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Equal(t, tt.wantPut, put)
			assert.Equal(t, tt.wantDelete, del)
			assert.Equal(t, tt.wantOp, op)
		})
	}

	t.Run("zero observer", func(t *testing.T) {
		t.Parallel()

		req := httptest.NewRequestWithContext(t.Context(), http.MethodPut, manifest, http.NoBody)
		rec := httptest.NewRecorder()
		ociregistry.ServeObserved(reply(http.StatusCreated), rec, req, ociregistry.Observer{})
		assert.Equal(t, http.StatusCreated, rec.Code)
	})
}
