package ocifilestorage_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/filestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileSystemLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	resp, err := e.client.CreateFileSystem(t.Context(), filestorage.CreateFileSystemRequest{
		AvailabilityDomain: new(testAD), CompartmentId: new(testCompartment), DisplayName: new("data"),
		FreeformTags: map[string]string{"env": "test"},
	})
	require.NoError(t, err)
	assert.Equal(t, filestorage.FileSystemLifecycleStateCreating, resp.LifecycleState)
	assert.NotEmpty(t, *resp.Etag)
	assert.Contains(t, *resp.Id, "ocid1.filesystem.oc1.iad.")
	assert.Equal(t, int64(0), *resp.MeteredBytes)
	assert.False(t, *resp.IsCloneParent)
	assert.True(t, *resp.IsTargetable)

	e.settle()

	cur, err := e.client.GetFileSystem(t.Context(), filestorage.GetFileSystemRequest{FileSystemId: resp.Id})
	require.NoError(t, err)

	got := cur.FileSystem
	assert.Equal(t, filestorage.FileSystemLifecycleStateActive, got.LifecycleState)
	assert.Equal(t, map[string]string{"env": "test"}, got.FreeformTags)

	upd, err := e.client.UpdateFileSystem(t.Context(), filestorage.UpdateFileSystemRequest{
		FileSystemId: resp.Id, IfMatch: cur.Etag,
		DisplayName: new("renamed"),
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *upd.DisplayName)
	assert.NotEqual(t, *cur.Etag, *upd.Etag)

	_, err = e.client.UpdateFileSystem(t.Context(), filestorage.UpdateFileSystemRequest{
		FileSystemId: resp.Id, IfMatch: cur.Etag,
		DisplayName: new("stale"),
	})
	assertStatus(t, err, http.StatusPreconditionFailed, "NoEtagMatch")

	_, err = e.client.DeleteFileSystem(t.Context(), filestorage.DeleteFileSystemRequest{FileSystemId: resp.Id})
	require.NoError(t, err)
	assert.Equal(t, filestorage.FileSystemLifecycleStateDeleting, e.getFS(t, resp.Id).LifecycleState)

	e.settle()
	assert.Equal(t, filestorage.FileSystemLifecycleStateDeleted, e.getFS(t, resp.Id).LifecycleState)

	_, err = e.client.UpdateFileSystem(t.Context(), filestorage.UpdateFileSystemRequest{
		FileSystemId: resp.Id, DisplayName: new("x"),
	})
	assertStatus(t, err, http.StatusNotFound, "NotAuthorizedOrNotFound")

	e.clock.Advance(2 * time.Hour)
	e.backend.Sweep()

	_, err = e.client.GetFileSystem(t.Context(), filestorage.GetFileSystemRequest{FileSystemId: resp.Id})
	assertStatus(t, err, http.StatusNotFound, "NotAuthorizedOrNotFound")
}

func TestCreateFileSystemRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*filestorage.CreateFileSystemDetails)
		name   string
		code   string
		status int
	}{
		{
			name:   "no-ad",
			mutate: func(d *filestorage.CreateFileSystemDetails) { d.AvailabilityDomain = new("") },
			status: 400,
			code:   "MissingParameter",
		},
		{
			name:   "no-compartment",
			mutate: func(d *filestorage.CreateFileSystemDetails) { d.CompartmentId = new("") },
			status: 400,
			code:   "MissingParameter",
		},
		{
			name:   "unknown-compartment",
			mutate: func(d *filestorage.CreateFileSystemDetails) { d.CompartmentId = new("ocid1.compartment.oc1..nope") },
			status: 404,
			code:   "NotAuthorizedOrNotFound",
		},
		{name: "long-name", mutate: func(d *filestorage.CreateFileSystemDetails) {
			b := make([]byte, 256)
			for i := range b {
				b[i] = 'a'
			}
			d.DisplayName = new(string(b))
		}, status: 400, code: "InvalidParameter"},
		{name: "unknown-policy", mutate: func(d *filestorage.CreateFileSystemDetails) {
			d.FilesystemSnapshotPolicyId = new("ocid1.filesystemsnapshotpolicy.oc1.iad.nope")
		}, status: 404, code: "NotAuthorizedOrNotFound"},
		{
			name:   "unknown-snapshot",
			mutate: func(d *filestorage.CreateFileSystemDetails) { d.SourceSnapshotId = new("ocid1.snapshot.oc1.iad.nope") },
			status: 404,
			code:   "NotAuthorizedOrNotFound",
		},
		{name: "bad-lock", mutate: func(d *filestorage.CreateFileSystemDetails) {
			d.Locks = []filestorage.ResourceLock{{Type: "BOGUS"}}
		}, status: 400, code: "InvalidParameter"},
		{name: "attach-without-source", mutate: func(d *filestorage.CreateFileSystemDetails) {
			d.CloneAttachStatus = filestorage.CreateFileSystemDetailsCloneAttachStatusDetach
		}, status: 400, code: "InvalidParameter"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			d := filestorage.CreateFileSystemDetails{
				AvailabilityDomain: new(testAD),
				CompartmentId:      new(testCompartment),
			}
			tc.mutate(&d)

			_, err := e.client.CreateFileSystem(
				t.Context(),
				filestorage.CreateFileSystemRequest{CreateFileSystemDetails: d},
			)
			assertStatus(t, err, tc.status, tc.code)
		})
	}
}

func TestFileSystemRetryToken(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	d := filestorage.CreateFileSystemDetails{AvailabilityDomain: new(testAD), CompartmentId: new(testCompartment)}

	first, err := e.client.CreateFileSystem(
		t.Context(),
		filestorage.CreateFileSystemRequest{CreateFileSystemDetails: d, OpcRetryToken: new("tok")},
	)
	require.NoError(t, err)

	again, err := e.client.CreateFileSystem(
		t.Context(),
		filestorage.CreateFileSystemRequest{CreateFileSystemDetails: d, OpcRetryToken: new("tok")},
	)
	require.NoError(t, err)
	assert.Equal(t, *first.Id, *again.Id)

	other, err := e.client.CreateFileSystem(
		t.Context(),
		filestorage.CreateFileSystemRequest{CreateFileSystemDetails: d},
	)
	require.NoError(t, err)
	assert.NotEqual(t, *first.Id, *other.Id)
}

func TestListFileSystems(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	a := e.createFS(t, func(d *filestorage.CreateFileSystemDetails) { d.DisplayName = new("alpha") })
	e.createFS(t, func(d *filestorage.CreateFileSystemDetails) { d.DisplayName = new("beta") })
	e.createFS(t, func(d *filestorage.CreateFileSystemDetails) {
		d.DisplayName = new("gamma")
		d.AvailabilityDomain = new(otherAD)
	})

	tests := []struct {
		mutate func(*filestorage.ListFileSystemsRequest)
		name   string
		want   []string
	}{
		{name: "ad", mutate: func(*filestorage.ListFileSystemsRequest) {}, want: []string{"beta", "alpha"}},
		{
			name:   "other-ad",
			mutate: func(r *filestorage.ListFileSystemsRequest) { r.AvailabilityDomain = new(otherAD) },
			want:   []string{"gamma"},
		},
		{
			name:   "by-name",
			mutate: func(r *filestorage.ListFileSystemsRequest) { r.DisplayName = new("alpha") },
			want:   []string{"alpha"},
		},
		{name: "by-id", mutate: func(r *filestorage.ListFileSystemsRequest) { r.Id = a.Id }, want: []string{"alpha"}},
		{
			name:   "sort-name",
			mutate: func(r *filestorage.ListFileSystemsRequest) { r.SortBy = filestorage.ListFileSystemsSortByDisplayname },
			want:   []string{"alpha", "beta"},
		},
		{name: "sort-name-desc", mutate: func(r *filestorage.ListFileSystemsRequest) {
			r.SortBy = filestorage.ListFileSystemsSortByDisplayname
			r.SortOrder = filestorage.ListFileSystemsSortOrderDesc
		}, want: []string{"beta", "alpha"}},
		{name: "state-none", mutate: func(r *filestorage.ListFileSystemsRequest) {
			r.LifecycleState = filestorage.ListFileSystemsLifecycleStateFailed
		}, want: []string{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := filestorage.ListFileSystemsRequest{
				CompartmentId:      new(testCompartment),
				AvailabilityDomain: new(testAD),
			}
			tc.mutate(&req)

			resp, err := e.client.ListFileSystems(t.Context(), req)
			require.NoError(t, err)

			got := make([]string, 0, len(resp.Items))
			for _, it := range resp.Items {
				got = append(got, *it.DisplayName)
			}

			assert.Equal(t, tc.want, got)
		})
	}

	t.Run("paging", func(t *testing.T) {
		t.Parallel()

		r1, err := e.client.ListFileSystems(t.Context(), filestorage.ListFileSystemsRequest{
			CompartmentId: new(testCompartment), AvailabilityDomain: new(testAD), Limit: new(1),
		})
		require.NoError(t, err)
		require.Len(t, r1.Items, 1)
		require.NotNil(t, r1.OpcNextPage)

		r2, err := e.client.ListFileSystems(t.Context(), filestorage.ListFileSystemsRequest{
			CompartmentId: new(testCompartment), AvailabilityDomain: new(testAD), Limit: new(1), Page: r1.OpcNextPage,
		})
		require.NoError(t, err)
		require.Len(t, r2.Items, 1)
		assert.NotEqual(t, *r1.Items[0].Id, *r2.Items[0].Id)
	})

	t.Run("missing-ad", func(t *testing.T) {
		t.Parallel()

		resp, err := e.client.ListFileSystems(
			t.Context(),
			filestorage.ListFileSystemsRequest{CompartmentId: new(testCompartment)},
		)
		require.Error(t, err)
		_ = resp
	})
}

func TestFileSystemLocks(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	fs := e.createFS(t, nil)

	added, err := e.client.AddFileSystemLock(t.Context(), filestorage.AddFileSystemLockRequest{
		FileSystemId: fs.Id,
		AddFileSystemLockDetails: filestorage.ResourceLock{
			Type:    filestorage.ResourceLockTypeFull,
			Message: new("hold"),
		},
	})
	require.NoError(t, err)
	require.Len(t, added.Locks, 1)
	assert.NotNil(t, added.Locks[0].TimeCreated)

	_, err = e.client.AddFileSystemLock(t.Context(), filestorage.AddFileSystemLockRequest{
		FileSystemId: fs.Id, AddFileSystemLockDetails: filestorage.ResourceLock{Type: filestorage.ResourceLockTypeFull},
	})
	assertStatus(t, err, http.StatusConflict, "Conflict")

	_, err = e.client.UpdateFileSystem(t.Context(), filestorage.UpdateFileSystemRequest{
		FileSystemId: fs.Id, DisplayName: new("x"),
	})
	assertStatus(t, err, http.StatusConflict, "Conflict")

	_, err = e.client.UpdateFileSystem(t.Context(), filestorage.UpdateFileSystemRequest{
		FileSystemId: fs.Id, IsLockOverride: new(true),
		DisplayName: new("override"),
	})
	require.NoError(t, err)

	_, err = e.client.DeleteFileSystem(t.Context(), filestorage.DeleteFileSystemRequest{FileSystemId: fs.Id})
	assertStatus(t, err, http.StatusConflict, "Conflict")

	removed, err := e.client.RemoveFileSystemLock(t.Context(), filestorage.RemoveFileSystemLockRequest{
		FileSystemId:                fs.Id,
		RemoveFileSystemLockDetails: filestorage.ResourceLock{Type: filestorage.ResourceLockTypeFull},
	})
	require.NoError(t, err)
	assert.Empty(t, removed.Locks)

	_, err = e.client.RemoveFileSystemLock(t.Context(), filestorage.RemoveFileSystemLockRequest{
		FileSystemId:                fs.Id,
		RemoveFileSystemLockDetails: filestorage.ResourceLock{Type: filestorage.ResourceLockTypeFull},
	})
	assertStatus(t, err, http.StatusNotFound, "NotAuthorizedOrNotFound")

	_, err = e.client.DeleteFileSystem(t.Context(), filestorage.DeleteFileSystemRequest{FileSystemId: fs.Id})
	require.NoError(t, err)
}

func TestChangeFileSystemCompartment(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	fs := e.createFS(t, nil)

	_, err := e.client.ChangeFileSystemCompartment(t.Context(), filestorage.ChangeFileSystemCompartmentRequest{
		FileSystemId:  fs.Id,
		CompartmentId: new(otherCompart),
	})
	require.NoError(t, err)
	assert.Equal(t, otherCompart, *e.getFS(t, fs.Id).CompartmentId)

	_, err = e.client.ChangeFileSystemCompartment(t.Context(), filestorage.ChangeFileSystemCompartmentRequest{
		FileSystemId:  fs.Id,
		CompartmentId: new("ocid1.compartment.oc1..nope"),
	})
	assertStatus(t, err, http.StatusNotFound, "NotAuthorizedOrNotFound")
}

func TestEstimateReplication(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	fs := e.createFS(t, nil)

	resp, err := e.client.EstimateReplication(
		t.Context(),
		filestorage.EstimateReplicationRequest{FileSystemId: fs.Id, ChangeRateInMBps: new(5)},
	)
	require.NoError(t, err)
	assert.True(t, *resp.IsReplicationSupported)
	assert.Equal(t, 5, *resp.ChangeRateInMBps)
	assert.Equal(t, 15, *resp.MinimumSupportedIntervalInMinutes)

	_, err = e.client.EstimateReplication(
		t.Context(),
		filestorage.EstimateReplicationRequest{FileSystemId: new("ocid1.filesystem.oc1.iad.nope")},
	)
	assertStatus(t, err, http.StatusNotFound, "NotAuthorizedOrNotFound")
}

func TestAuthEnforcement(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{enforce: true})

	_, err := e.client.ListFileSystems(
		t.Context(),
		filestorage.ListFileSystemsRequest{CompartmentId: new(testCompartment), AvailabilityDomain: new(testAD)},
	)
	assertStatus(t, err, http.StatusUnauthorized, "NotAuthenticated")

	_, err = e.backend.Auth.Register(testTenancy, testUser, e.kp.pubPEM)
	require.NoError(t, err)

	_, err = e.client.ListFileSystems(
		t.Context(),
		filestorage.ListFileSystemsRequest{CompartmentId: new(testCompartment), AvailabilityDomain: new(testAD)},
	)
	require.NoError(t, err)
}
