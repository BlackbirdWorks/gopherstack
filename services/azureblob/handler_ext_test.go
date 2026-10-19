package azureblob_test

import (
	"maps"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureblob"
)

const (
	cPath    = "/" + testAccount + "/c1"
	cPathQ   = cPath + "?restype=container"
	blobPath = cPath + "/dir/b.txt"
	putBlob  = "BlockBlob"
)

func seedBlob(t *testing.T, h *azureblob.Handler, containerHeaders map[string]string) {
	t.Helper()

	rec := doRequest(t, h, http.MethodPut, cPathQ, nil, containerHeaders)
	require.Equal(t, http.StatusCreated, rec.Code)

	rec = doRequest(t, h, http.MethodPut, blobPath, []byte("data"), map[string]string{"x-ms-blob-type": putBlob})
	require.Equal(t, http.StatusCreated, rec.Code)
}

func TestMetadataValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		headers map[string]string
		code    string
		status  int
	}{
		{
			name: "bad_blob_metadata_name", path: blobPath,
			headers: map[string]string{"x-ms-blob-type": putBlob, "x-ms-meta-bad-name": "v"},
			status:  http.StatusBadRequest, code: "InvalidMetadata",
		},
		{
			name: "bad_blob_tier", path: blobPath,
			headers: map[string]string{"x-ms-blob-type": putBlob, "x-ms-access-tier": "Lukewarm"},
			status:  http.StatusBadRequest, code: "InvalidHeaderValue",
		},
		{
			name: "bad_md5", path: blobPath,
			headers: map[string]string{"x-ms-blob-type": putBlob, "x-ms-blob-content-md5": "abc"},
			status:  http.StatusBadRequest, code: "InvalidHeaderValue",
		},
		{
			name: "bad_public_access", path: cPathQ,
			headers: map[string]string{"x-ms-blob-public-access": "everyone"},
			status:  http.StatusBadRequest, code: "InvalidHeaderValue",
		},
		{
			name: "bad_container_metadata", path: cPathQ,
			headers: map[string]string{"x-ms-meta-1x": "v"},
			status:  http.StatusBadRequest, code: "InvalidMetadata",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, http.MethodPut, cPathQ, nil, nil)

			rec := doRequest(t, h, http.MethodPut, tt.path, []byte("x"), tt.headers)
			assert.Equal(t, tt.status, rec.Code)
			assert.Contains(t, rec.Body.String(), "<Code>"+tt.code+"</Code>")
		})
	}
}

func TestBlobOpsErrorsAndHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		headers map[string]string
		name    string
		method  string
		query   string
		code    string
		status  int
	}{
		{
			name: "tier_missing_header", method: http.MethodPut, query: "?comp=tier",
			status: 400, code: "MissingRequiredHeader",
		},
		{
			name: "tier_invalid", method: http.MethodPut, query: "?comp=tier",
			headers: map[string]string{"x-ms-access-tier": "P10"}, status: 400, code: "InvalidHeaderValue",
		},
		{
			name: "delete_bad_snapshot_mode", method: http.MethodDelete,
			headers: map[string]string{"x-ms-delete-snapshots": "all"}, status: 400, code: "InvalidHeaderValue",
		},
		{
			name: "head_unknown_snapshot", method: http.MethodHead,
			query: "?snapshot=2020-01-01T00:00:00.0000000Z", status: 404,
		},
		{
			name: "delete_unknown_snapshot", method: http.MethodDelete, query: "?snapshot=nope",
			status: 404, code: "BlobNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			seedBlob(t, h, nil)

			rec := doRequest(t, h, tt.method, blobPath+tt.query, nil, tt.headers)
			assert.Equal(t, tt.status, rec.Code)

			if tt.code != "" {
				assert.Contains(t, rec.Body.String(), "<Code>"+tt.code+"</Code>")
			}
		})
	}
}

func TestBlobTierAndSnapshotFlow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedBlob(t, h, nil)

	rec := doRequest(t, h, http.MethodHead, blobPath, nil, nil)
	assert.Equal(t, "Hot", rec.Header().Get("X-Ms-Access-Tier"))
	assert.Equal(t, "true", rec.Header().Get("X-Ms-Access-Tier-Inferred"))

	rec = doRequest(t, h, http.MethodPut, blobPath+"?comp=tier", nil, map[string]string{"x-ms-access-tier": "Archive"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, blobPath, nil, nil)
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "BlobArchived")

	rec = doRequest(t, h, http.MethodPut, blobPath+"?comp=tier", nil, map[string]string{"x-ms-access-tier": "Cold"})
	assert.Equal(t, http.StatusAccepted, rec.Code, "rehydrating from archive")

	rec = doRequest(t, h, http.MethodHead, blobPath, nil, nil)
	assert.Equal(t, "Cold", rec.Header().Get("X-Ms-Access-Tier"))
	assert.Empty(t, rec.Header().Get("X-Ms-Access-Tier-Inferred"))

	rec = doRequest(t, h, http.MethodPut, blobPath+"?comp=snapshot", nil, map[string]string{"x-ms-meta-snap": "yes"})
	require.Equal(t, http.StatusCreated, rec.Code)

	snap := rec.Header().Get("X-Ms-Snapshot")
	require.NotEmpty(t, snap)

	rec = doRequest(t, h, http.MethodGet, blobPath+"?snapshot="+snap, nil, nil)
	assert.Equal(t, "data", rec.Body.String())
	assert.Equal(t, snap, rec.Header().Get("X-Ms-Snapshot"))
	assert.Equal(t, "yes", rec.Header().Get("X-Ms-Meta-Snap"))

	rec = doRequest(t, h, http.MethodDelete, blobPath, nil, nil)
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "SnapshotsPresent")

	rec = doRequest(t, h, http.MethodDelete, blobPath, nil, map[string]string{"x-ms-delete-snapshots": "only"})
	assert.Equal(t, http.StatusAccepted, rec.Code)

	rec = doRequest(t, h, http.MethodHead, blobPath+"?snapshot="+snap, nil, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = doRequest(t, h, http.MethodHead, blobPath, nil, nil)
	assert.Equal(t, http.StatusOK, rec.Code, "only mode keeps the base blob")
}

func TestCopyBlob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		headers    map[string]string
		name       string
		source     string
		wantMeta   string
		wantStatus int
	}{
		{name: "inherits_metadata", source: "http://h" + blobPath, wantStatus: 202, wantMeta: "orig"},
		{
			name: "overrides_metadata", source: "http://h" + blobPath,
			headers: map[string]string{"x-ms-meta-k": "new"}, wantStatus: 202, wantMeta: "new",
		},
		{name: "missing_source", source: "http://h" + cPath + "/nope", wantStatus: 404},
		{name: "other_account", source: "http://h/other" + cPath + "/dir/b.txt", wantStatus: 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			seedBlob(t, h, nil)
			doRequest(t, h, http.MethodPut, blobPath+"?comp=metadata", nil, map[string]string{"x-ms-meta-k": "orig"})

			headers := map[string]string{"x-ms-copy-source": tt.source}
			maps.Copy(headers, tt.headers)

			rec := doRequest(t, h, http.MethodPut, cPath+"/copy.txt", nil, headers)
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusAccepted {
				return
			}

			assert.Equal(t, "success", rec.Header().Get("X-Ms-Copy-Status"))
			assert.NotEmpty(t, rec.Header().Get("X-Ms-Copy-Id"))

			rec = doRequest(t, h, http.MethodGet, cPath+"/copy.txt", nil, nil)
			assert.Equal(t, "data", rec.Body.String())
			assert.Equal(t, tt.wantMeta, rec.Header().Get("X-Ms-Meta-K"))
		})
	}
}

func TestListBlobsQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  []string
		not   []string
		code  int
	}{
		{
			name: "delimiter_root", query: "&delimiter=/", code: 200,
			want: []string{"<BlobPrefix><Name>dir/</Name></BlobPrefix>", "<Delimiter>/</Delimiter>"},
			not:  []string{"<Name>dir/b.txt</Name>"},
		},
		{
			name: "prefix_only", query: "&prefix=dir/", code: 200,
			want: []string{"<Name>dir/b.txt</Name>", "<Prefix>dir/</Prefix>"},
		},
		{
			name: "maxresults_marker", query: "&maxresults=1", code: 200,
			want: []string{"<MaxResults>1</MaxResults>", "<NextMarker>"},
		},
		{name: "bad_maxresults", query: "&maxresults=0", code: 400},
		{name: "bad_marker", query: "&marker=!!", code: 400},
		{name: "metadata_included", query: "", code: 200, want: []string{"<Metadata><k>v</k></Metadata>"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			seedBlob(t, h, nil)
			doRequest(t, h, http.MethodPut, blobPath+"?comp=metadata", nil, map[string]string{"x-ms-meta-k": "v"})
			doRequest(t, h, http.MethodPut, cPath+"/top.txt", []byte("t"), map[string]string{"x-ms-blob-type": putBlob})

			rec := doRequest(t, h, http.MethodGet, cPathQ+"&comp=list"+tt.query, nil, nil)
			require.Equal(t, tt.code, rec.Code)

			for _, w := range tt.want {
				assert.Contains(t, rec.Body.String(), w)
			}

			for _, n := range tt.not {
				assert.NotContains(t, rec.Body.String(), n)
			}
		})
	}
}

func TestAnonymousAccessEnforcement(t *testing.T) {
	t.Parallel()

	anon := map[string]string{"Authorization": ""}

	tests := []struct {
		name   string
		access string
		method string
		path   string
		want   int
	}{
		{name: "private_blob_read", access: "", method: http.MethodGet, path: blobPath, want: 404},
		{name: "blob_level_blob_read", access: "blob", method: http.MethodGet, path: blobPath, want: 200},
		{name: "blob_level_head", access: "blob", method: http.MethodHead, path: blobPath, want: 200},
		{name: "blob_level_no_list", access: "blob", method: http.MethodGet, path: cPathQ + "&comp=list", want: 404},
		{
			name:   "container_level_list",
			access: "container",
			method: http.MethodGet,
			path:   cPathQ + "&comp=list",
			want:   200,
		},
		{name: "container_level_read", access: "container", method: http.MethodGet, path: blobPath, want: 200},
		{name: "container_level_props", access: "container", method: http.MethodGet, path: cPathQ, want: 200},
		{
			name:   "container_level_no_acl",
			access: "container",
			method: http.MethodGet,
			path:   cPathQ + "&comp=acl",
			want:   404,
		},
		{name: "write_denied", access: "container", method: http.MethodDelete, path: blobPath, want: 401},
		{
			name:   "account_denied",
			access: "container",
			method: http.MethodGet,
			path:   "/" + testAccount + "?comp=list",
			want:   401,
		},
		{name: "sas_allowed", access: "", method: http.MethodGet, path: blobPath + "?sig=abc", want: 200},
		{name: "missing_container", access: "", method: http.MethodGet, path: "/" + testAccount + "/nope/b", want: 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			seedBlob(t, h, map[string]string{"x-ms-blob-public-access": tt.access})
			h.EnforcePublicAccess = true

			rec := doRequest(t, h, tt.method, tt.path, nil, anon)
			assert.Equal(t, tt.want, rec.Code)
		})
	}
}

func TestAnonymousAllowedWhenNotEnforced(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedBlob(t, h, nil)

	rec := doRequest(t, h, http.MethodGet, blobPath, nil, map[string]string{"Authorization": ""})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestContainerACLXML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want int
	}{
		{
			name: "valid",
			body: `<SignedIdentifiers><SignedIdentifier><Id>p1</Id><AccessPolicy>` +
				`<Permission>rl</Permission></AccessPolicy></SignedIdentifier></SignedIdentifiers>`,
			want: 200,
		},
		{name: "empty_body_clears", body: "", want: 200},
		{name: "malformed", body: "<SignedIdentifiers", want: 400},
		{
			name: "empty_id",
			body: `<SignedIdentifiers><SignedIdentifier><Id></Id></SignedIdentifier></SignedIdentifiers>`,
			want: 400,
		},
		{
			name: "too_many", want: 400,
			body: "<SignedIdentifiers>" + strings.Repeat(`<SignedIdentifier><Id>a</Id></SignedIdentifier>`, 6) +
				"</SignedIdentifiers>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			seedBlob(t, h, nil)

			rec := doRequest(t, h, http.MethodPut, cPathQ+"&comp=acl", []byte(tt.body),
				map[string]string{"x-ms-blob-public-access": "blob"})
			require.Equal(t, tt.want, rec.Code)

			if tt.want != http.StatusOK {
				return
			}

			rec = doRequest(t, h, http.MethodGet, cPathQ+"&comp=acl", nil, nil)
			assert.Equal(t, "blob", rec.Header().Get("X-Ms-Blob-Public-Access"))
			assert.Equal(t, strings.Contains(tt.body, "p1"), strings.Contains(rec.Body.String(), "<Id>p1</Id>"))
		})
	}
}

func TestListContainersIncludeMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedBlob(t, h, map[string]string{"x-ms-meta-owner": "me", "x-ms-blob-public-access": "container"})

	rec := doRequest(t, h, http.MethodGet, "/"+testAccount+"?comp=list&include=metadata", nil, nil)
	assert.Contains(t, rec.Body.String(), "<Metadata><owner>me</owner></Metadata>")
	assert.Contains(t, rec.Body.String(), "<PublicAccess>container</PublicAccess>")

	rec = doRequest(t, h, http.MethodGet, "/"+testAccount+"?comp=list", nil, nil)
	assert.NotContains(t, rec.Body.String(), "<Metadata>")
}

func TestStoreSnapshotPersistsNewState(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	b := azureblob.NewInMemoryBackend()
	_, err := b.CreateContainerWith(
		"c",
		azureblob.ContainerOptions{Metadata: map[string]string{"m": "1"}, PublicAccess: "blob"},
	)
	require.NoError(t, err)
	_, err = b.SetContainerACL("c", "blob", []azureblob.SignedIdentifier{{ID: "p", Permission: "r"}})
	require.NoError(t, err)
	_, err = b.PutBlobWith("c", "b", []byte("x"), azureblob.BlobAttrs{
		ContentType:  "text/plain",
		CacheControl: "no-cache",
		Metadata:     map[string]string{"k": "v"},
		AccessTier:   azureblob.TierCool,
	})
	require.NoError(t, err)

	snap, err := b.SnapshotBlob("c", "b", nil)
	require.NoError(t, err)

	restored := azureblob.NewInMemoryBackend()
	require.NoError(t, restored.Restore(ctx, b.Snapshot(ctx)))

	ci, err := restored.GetContainer("c")
	require.NoError(t, err)
	assert.Equal(t, "blob", ci.PublicAccess)
	assert.Equal(t, map[string]string{"m": "1"}, ci.Metadata)
	assert.Equal(t, "p", ci.ACL[0].ID)

	info, err := restored.HeadBlobAt("c", "b", "")
	require.NoError(t, err)
	assert.Equal(t, "no-cache", info.CacheControl)
	assert.Equal(t, azureblob.TierCool, info.AccessTier)
	assert.False(t, info.AccessTierInferred)
	assert.Equal(t, "v", info.Metadata["k"])

	_, err = restored.HeadBlobAt("c", "b", snap.Snapshot)
	require.NoError(t, err)
}

func TestRestoreLegacySnapshotShape(t *testing.T) {
	t.Parallel()

	legacy := `{"version":1,"containers":{"c":{"Name":"c","CreatedAt":"2026-01-02T03:04:05Z","Blobs":{"b":` +
		`{"Name":"b","ContentType":"text/plain","ETag":"\"e\"","LastModified":"2026-01-02T03:04:05Z","Data":"aGk="}}}}}`

	b := azureblob.NewInMemoryBackend()
	require.NoError(t, b.Restore(t.Context(), []byte(legacy)))

	ci, err := b.GetContainer("c")
	require.NoError(t, err)
	assert.False(t, ci.LastModified.IsZero())
	assert.NotEmpty(t, ci.ETag)

	info, data, err := b.GetBlobAt("c", "b", "")
	require.NoError(t, err)
	assert.Equal(t, "hi", string(data))
	assert.Equal(t, azureblob.TierHot, info.AccessTier)
	assert.True(t, info.AccessTierInferred)
}
