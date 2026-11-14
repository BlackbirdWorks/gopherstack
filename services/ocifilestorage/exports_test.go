package ocifilestorage_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/filestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExportDefaultsAndLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	mt := e.createMT(t, nil)
	fs := e.createFS(t, nil)

	x := e.createExport(t, mt.ExportSetId, fs.Id, "/data", nil)
	assert.Equal(t, filestorage.ExportLifecycleStateActive, x.LifecycleState)
	require.Len(t, x.ExportOptions, 1)

	o := x.ExportOptions[0]
	assert.Equal(t, "0.0.0.0/0", *o.Source)
	assert.False(t, *o.RequirePrivilegedSourcePort)
	assert.Equal(t, filestorage.ClientOptionsAccessWrite, o.Access)
	assert.Equal(t, filestorage.ClientOptionsIdentitySquashNone, o.IdentitySquash)
	assert.Equal(t, int64(65534), *o.AnonymousUid)
	assert.Equal(t, []filestorage.ClientOptionsAllowedAuthEnum{filestorage.ClientOptionsAllowedAuthSys}, o.AllowedAuth)

	assert.False(t, *e.getFS(t, fs.Id).IsTargetable, "an exported file system is not targetable")

	list, err := e.client.ListExports(t.Context(), filestorage.ListExportsRequest{FileSystemId: fs.Id})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, "/data", *list.Items[0].Path)

	upd, err := e.client.UpdateExport(t.Context(), filestorage.UpdateExportRequest{
		ExportId: x.Id, ExportOptions: []filestorage.ClientOptions{{Source: new("10.0.0.0/16")}},
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		filestorage.ClientOptionsIdentitySquashRoot,
		upd.ExportOptions[0].IdentitySquash,
		"per-option defaults apply",
	)
	assert.True(t, *upd.ExportOptions[0].RequirePrivilegedSourcePort)

	_, err = e.client.DeleteFileSystem(t.Context(), filestorage.DeleteFileSystemRequest{FileSystemId: fs.Id})
	assertStatus(t, err, http.StatusConflict, "Conflict")

	_, err = e.client.DeleteExport(t.Context(), filestorage.DeleteExportRequest{ExportId: x.Id})
	require.NoError(t, err)
	e.settle()

	_, err = e.client.DeleteFileSystem(t.Context(), filestorage.DeleteFileSystemRequest{FileSystemId: fs.Id})
	require.NoError(t, err)
}

func TestExportPathRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		existing string
		path     string
		code     string
		status   int
	}{
		{name: "ok", path: "/a", status: 200},
		{name: "no-slash", path: "a", status: 400, code: "InvalidParameter"},
		{name: "trailing-slash", path: "/a/", status: 400, code: "InvalidParameter"},
		{name: "dotdot", path: "/a/../b", status: 400, code: "InvalidParameter"},
		{name: "dot", path: "/a/./b", status: 400, code: "InvalidParameter"},
		{name: "empty-element", path: "/a//b", status: 400, code: "InvalidParameter"},
		{name: "long-element", path: "/" + repeat('x', 256), status: 400, code: "InvalidParameter"},
		{name: "same", existing: "/a", path: "/a", status: 409, code: "Conflict"},
		{name: "nested", existing: "/a", path: "/a/b", status: 409, code: "Conflict"},
		{name: "parent", existing: "/a/b", path: "/a", status: 409, code: "Conflict"},
		{name: "sibling", existing: "/a", path: "/ab", status: 200},
		{name: "root-vs-other", existing: "/a", path: "/", status: 409, code: "Conflict"},
		{name: "other-vs-root", existing: "/", path: "/a", status: 409, code: "Conflict"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			mt := e.createMT(t, nil)

			if tc.existing != "" {
				e.createExport(t, mt.ExportSetId, e.createFS(t, nil).Id, tc.existing, nil)
			}

			fs := e.createFS(t, nil)
			_, err := e.client.CreateExport(t.Context(), filestorage.CreateExportRequest{
				ExportSetId: mt.ExportSetId, FileSystemId: fs.Id, Path: new(tc.path),
			})

			if tc.status == http.StatusOK {
				require.NoError(t, err)

				return
			}

			assertStatus(t, err, tc.status, tc.code)
		})
	}
}

func repeat(c byte, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = c
	}

	return string(b)
}

func TestExportOptionRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []filestorage.ClientOptions
		ok   bool
	}{
		{name: "v4-cidr", opts: []filestorage.ClientOptions{{Source: new("10.0.0.0/8")}}, ok: true},
		{name: "v6", opts: []filestorage.ClientOptions{{Source: new("fd00::/8")}}, ok: true},
		{name: "single-ip", opts: []filestorage.ClientOptions{{Source: new("10.1.2.3")}}, ok: true},
		{name: "hidden", opts: []filestorage.ClientOptions{}, ok: true},
		{name: "bad-source", opts: []filestorage.ClientOptions{{Source: new("not-an-ip")}}},
		{name: "bad-access", opts: []filestorage.ClientOptions{{Source: new("10.0.0.0/8"), Access: "EXEC"}}},
		{name: "bad-squash", opts: []filestorage.ClientOptions{{Source: new("10.0.0.0/8"), IdentitySquash: "SOME"}}},
		{name: "bad-uid", opts: []filestorage.ClientOptions{{Source: new("10.0.0.0/8"), AnonymousUid: new(int64(-1))}}},
		{
			name: "uid-too-big",
			opts: []filestorage.ClientOptions{{Source: new("10.0.0.0/8"), AnonymousGid: new(int64(1) << 33)}},
		},
		{
			name: "kerberos-disabled",
			opts: []filestorage.ClientOptions{
				{Source: new("10.0.0.0/8"), AllowedAuth: []filestorage.ClientOptionsAllowedAuthEnum{"KRB5"}},
			},
		},
		{
			name: "bad-auth",
			opts: []filestorage.ClientOptions{
				{Source: new("10.0.0.0/8"), AllowedAuth: []filestorage.ClientOptionsAllowedAuthEnum{"TLS"}},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			mt := e.createMT(t, nil)
			fs := e.createFS(t, nil)

			_, err := e.client.CreateExport(t.Context(), filestorage.CreateExportRequest{
				ExportSetId: mt.ExportSetId, FileSystemId: fs.Id, Path: new("/x"), ExportOptions: tc.opts,
			})

			if tc.ok {
				require.NoError(t, err)

				return
			}

			assertStatus(t, err, http.StatusBadRequest, "InvalidParameter")
		})
	}

	t.Run("too-many", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		mt := e.createMT(t, nil)
		fs := e.createFS(t, nil)

		opts := make([]filestorage.ClientOptions, 101)
		for i := range opts {
			opts[i] = filestorage.ClientOptions{Source: new("10.0.0.0/8")}
		}

		_, err := e.client.CreateExport(t.Context(), filestorage.CreateExportRequest{
			ExportSetId: mt.ExportSetId, FileSystemId: fs.Id, Path: new("/x"), ExportOptions: opts,
		})
		assertStatus(t, err, http.StatusBadRequest, "InvalidParameter")
	})
}

func TestExportTargetRules(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	mt := e.createMT(t, nil)
	fsOther := e.createFS(t, func(d *filestorage.CreateFileSystemDetails) { d.AvailabilityDomain = new(otherAD) })
	fs := e.createFS(t, nil)

	create := func(set, fsID *string, path string) error {
		_, err := e.client.CreateExport(t.Context(), filestorage.CreateExportRequest{
			ExportSetId: set, FileSystemId: fsID, Path: new(path),
		})

		return err
	}

	assertStatus(t, create(nil, fs.Id, "/a"), http.StatusBadRequest, "MissingParameter")
	assertStatus(t, create(mt.ExportSetId, fsOther.Id, "/a"), http.StatusBadRequest, "InvalidParameter")
	assertStatus(
		t,
		create(new("ocid1.exportset.oc1.iad.nope"), fs.Id, "/a"),
		http.StatusNotFound,
		"NotAuthorizedOrNotFound",
	)
	assertStatus(
		t,
		create(mt.ExportSetId, new("ocid1.filesystem.oc1.iad.nope"), "/a"),
		http.StatusNotFound,
		"NotAuthorizedOrNotFound",
	)

	require.NoError(t, create(mt.ExportSetId, fs.Id, "/a"))
	assertStatus(t, create(mt.ExportSetId, fs.Id, "/b"), http.StatusConflict, "Conflict")
}

func TestExportLockAndRetry(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	mt := e.createMT(t, nil)
	fs := e.createFS(t, nil)
	x := e.createExport(t, mt.ExportSetId, fs.Id, "/a", nil)

	_, err := e.client.AddExportLock(t.Context(), filestorage.AddExportLockRequest{
		ExportId: x.Id, AddExportLockDetails: filestorage.ResourceLock{Type: filestorage.ResourceLockTypeFull},
	})
	require.NoError(t, err)

	_, err = e.client.UpdateExport(t.Context(), filestorage.UpdateExportRequest{
		ExportId: x.Id, ExportOptions: []filestorage.ClientOptions{},
	})
	assertStatus(t, err, http.StatusConflict, "Conflict")

	_, err = e.client.DeleteExport(
		t.Context(),
		filestorage.DeleteExportRequest{ExportId: x.Id, IsLockOverride: new(true)},
	)
	require.NoError(t, err)
}
