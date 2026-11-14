package ocifilestorage_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/filestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	fs := e.createFS(t, nil)

	sn := e.createSnapshot(t, fs.Id, "Sunday")
	assert.Equal(t, filestorage.SnapshotLifecycleStateActive, sn.LifecycleState)
	assert.Equal(t, filestorage.SnapshotSnapshotTypeUser, sn.SnapshotType)
	assert.Equal(t, *sn.Id, *sn.ProvenanceId)
	assert.False(t, *sn.IsCloneSource)
	assert.False(
		t,
		e.getFS(t, fs.Id).IsTargetable != nil && *e.getFS(t, fs.Id).IsTargetable,
		"a user snapshot makes the file system untargetable",
	)

	_, err := e.client.CreateSnapshot(t.Context(), filestorage.CreateSnapshotRequest{
		FileSystemId: fs.Id, Name: new("Sunday"),
	})
	assertStatus(t, err, http.StatusConflict, "Conflict")

	list, err := e.client.ListSnapshots(t.Context(), filestorage.ListSnapshotsRequest{FileSystemId: fs.Id})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, "Sunday", *list.Items[0].Name)

	exp := common.SDKTime{Time: e.clock.Now().Add(time.Hour)}
	upd, err := e.client.UpdateSnapshot(t.Context(), filestorage.UpdateSnapshotRequest{
		SnapshotId: sn.Id, ExpirationTime: &exp, FreeformTags: map[string]string{"k": "v"},
	})
	require.NoError(t, err)
	assert.Equal(t, "v", upd.FreeformTags["k"])
	require.NotNil(t, upd.ExpirationTime)

	e.clock.Advance(2 * time.Hour)
	e.backend.Sweep()

	got, err := e.client.GetSnapshot(t.Context(), filestorage.GetSnapshotRequest{SnapshotId: sn.Id})
	require.NoError(t, err)
	assert.Equal(t, filestorage.SnapshotLifecycleStateDeleting, got.LifecycleState, "expired snapshots are deleted")

	_, err = e.client.ListSnapshots(t.Context(), filestorage.ListSnapshotsRequest{})
	assertStatus(t, err, http.StatusBadRequest, "MissingParameter")
}

func TestSnapshotRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		details func(fs *string) filestorage.CreateSnapshotDetails
		name    string
		code    string
		status  int
	}{
		{name: "no-fs", details: func(*string) filestorage.CreateSnapshotDetails {
			return filestorage.CreateSnapshotDetails{Name: new("a")}
		}, status: 400, code: "MissingParameter"},
		{name: "no-name", details: func(fs *string) filestorage.CreateSnapshotDetails {
			return filestorage.CreateSnapshotDetails{FileSystemId: fs}
		}, status: 400, code: "MissingParameter"},
		{name: "slash", details: func(fs *string) filestorage.CreateSnapshotDetails {
			return filestorage.CreateSnapshotDetails{FileSystemId: fs, Name: new("a/b")}
		}, status: 400, code: "InvalidParameter"},
		{name: "dotdot", details: func(fs *string) filestorage.CreateSnapshotDetails {
			return filestorage.CreateSnapshotDetails{FileSystemId: fs, Name: new("..")}
		}, status: 400, code: "InvalidParameter"},
		{name: "unknown-fs", details: func(*string) filestorage.CreateSnapshotDetails {
			return filestorage.CreateSnapshotDetails{FileSystemId: new("ocid1.filesystem.oc1.iad.x"), Name: new("a")}
		}, status: 404, code: "NotAuthorizedOrNotFound"},
		{name: "past-expiry", details: func(fs *string) filestorage.CreateSnapshotDetails {
			return filestorage.CreateSnapshotDetails{
				FileSystemId:   fs,
				Name:           new("a"),
				ExpirationTime: &common.SDKTime{Time: time.Now().Add(-time.Hour)},
			}
		}, status: 400, code: "InvalidParameter"},
		{name: "bad-lock-mode", details: func(fs *string) filestorage.CreateSnapshotDetails {
			return filestorage.CreateSnapshotDetails{
				FileSystemId:        fs,
				Name:                new("a"),
				LockDurationDetails: &filestorage.LockDurationDetails{LockMode: "NOPE"},
			}
		}, status: 400, code: "InvalidParameter"},
		{name: "cooloff-governance", details: func(fs *string) filestorage.CreateSnapshotDetails {
			return filestorage.CreateSnapshotDetails{
				FileSystemId: fs,
				Name:         new("a"),
				LockDurationDetails: &filestorage.LockDurationDetails{
					LockMode: filestorage.LockDurationDetailsLockModeGovernance, CoolOffDuration: new(3),
				},
			}
		}, status: 400, code: "InvalidParameter"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			fs := e.createFS(t, nil)

			_, err := e.client.CreateSnapshot(
				t.Context(),
				filestorage.CreateSnapshotRequest{CreateSnapshotDetails: tc.details(fs.Id)},
			)
			assertStatus(t, err, tc.status, tc.code)
		})
	}
}

func TestSnapshotDurationLock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		details  filestorage.LockDurationDetails
		name     string
		after    time.Duration
		override bool
		wantDel  bool
	}{
		{
			name:    "governance-held",
			details: filestorage.LockDurationDetails{LockMode: "GOVERNANCE", LockDuration: new(10)},
			wantDel: false,
		},
		{
			name:     "governance-override",
			details:  filestorage.LockDurationDetails{LockMode: "GOVERNANCE", LockDuration: new(10)},
			override: true,
			wantDel:  true,
		},
		{
			name:    "governance-elapsed",
			details: filestorage.LockDurationDetails{LockMode: "GOVERNANCE", LockDuration: new(1)},
			after:   25 * time.Hour,
			wantDel: true,
		},
		{
			name: "compliance-cooloff",
			details: filestorage.LockDurationDetails{
				LockMode:        "COMPLIANCE",
				LockDuration:    new(10),
				CoolOffDuration: new(1),
			},
			wantDel: true,
		},
		{
			name: "compliance-immutable",
			details: filestorage.LockDurationDetails{
				LockMode:        "COMPLIANCE",
				LockDuration:    new(10),
				CoolOffDuration: new(1),
			},
			after:    25 * time.Hour,
			override: true,
			wantDel:  false,
		},
		{
			name:    "legal-hold",
			details: filestorage.LockDurationDetails{LockMode: "GOVERNANCE", LockDuration: new(0)},
			after:   1000 * time.Hour,
			wantDel: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			fs := e.createFS(t, nil)

			resp, err := e.client.CreateSnapshot(t.Context(), filestorage.CreateSnapshotRequest{
				FileSystemId: fs.Id, Name: new("locked"), LockDurationDetails: &tc.details,
			})
			require.NoError(t, err)
			require.NotNil(t, resp.TimeLocked)

			e.clock.Advance(tc.after)

			_, err = e.client.DeleteSnapshot(
				t.Context(),
				filestorage.DeleteSnapshotRequest{SnapshotId: resp.Id, IsLockOverride: new(tc.override)},
			)
			if tc.wantDel {
				require.NoError(t, err)

				return
			}

			assertStatus(t, err, http.StatusConflict, "Conflict")
		})
	}
}

func TestSnapshotResourceLock(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	fs := e.createFS(t, nil)
	sn := e.createSnapshot(t, fs.Id, "s")

	_, err := e.client.AddSnapshotLock(t.Context(), filestorage.AddSnapshotLockRequest{
		SnapshotId: sn.Id, AddSnapshotLockDetails: filestorage.ResourceLock{Type: filestorage.ResourceLockTypeDelete},
	})
	require.NoError(t, err)

	_, err = e.client.DeleteSnapshot(t.Context(), filestorage.DeleteSnapshotRequest{SnapshotId: sn.Id})
	assertStatus(t, err, http.StatusConflict, "Conflict")

	_, err = e.client.UpdateSnapshot(t.Context(), filestorage.UpdateSnapshotRequest{
		SnapshotId: sn.Id, FreeformTags: map[string]string{"a": "b"},
	})
	require.NoError(t, err, "a DELETE lock leaves updates open")

	_, err = e.client.RemoveSnapshotLock(t.Context(), filestorage.RemoveSnapshotLockRequest{
		SnapshotId:                sn.Id,
		RemoveSnapshotLockDetails: filestorage.ResourceLock{Type: filestorage.ResourceLockTypeDelete},
	})
	require.NoError(t, err)

	_, err = e.client.DeleteSnapshot(t.Context(), filestorage.DeleteSnapshotRequest{SnapshotId: sn.Id})
	require.NoError(t, err)
}

func TestCloneFromSnapshot(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	parent := e.createFS(t, nil)
	sn := e.createSnapshot(t, parent.Id, "base")

	resp, err := e.client.CreateFileSystem(t.Context(), filestorage.CreateFileSystemRequest{
		AvailabilityDomain: new(testAD), CompartmentId: new(testCompartment), SourceSnapshotId: sn.Id,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.SourceDetails)
	assert.Equal(t, *parent.Id, *resp.SourceDetails.ParentFileSystemId)
	assert.Equal(t, *sn.Id, *resp.SourceDetails.SourceSnapshotId)
	assert.Equal(t, filestorage.FileSystemCloneAttachStatusAttached, resp.CloneAttachStatus)
	assert.False(t, *resp.IsHydrated, "hydration runs on the clock")

	assert.True(t, *e.getFS(t, parent.Id).IsCloneParent)
	assert.Equal(t, 1, *e.getFS(t, parent.Id).CloneCount)

	gotSnap, err := e.client.GetSnapshot(t.Context(), filestorage.GetSnapshotRequest{SnapshotId: sn.Id})
	require.NoError(t, err)
	assert.True(t, *gotSnap.IsCloneSource)

	e.clock.Advance(time.Minute)
	e.backend.Sweep()
	assert.True(t, *e.getFS(t, resp.Id).IsHydrated)

	_, err = e.client.DeleteFileSystem(t.Context(), filestorage.DeleteFileSystemRequest{FileSystemId: parent.Id})
	assertStatus(t, err, http.StatusConflict, "Conflict")

	_, err = e.client.DetachClone(t.Context(), filestorage.DetachCloneRequest{FileSystemId: resp.Id})
	require.NoError(t, err)
	assert.Equal(t, filestorage.FileSystemCloneAttachStatusDetaching, e.getFS(t, resp.Id).CloneAttachStatus)

	e.settle()
	assert.Equal(t, filestorage.FileSystemCloneAttachStatusDetached, e.getFS(t, resp.Id).CloneAttachStatus)
	assert.Equal(t, 0, *e.getFS(t, parent.Id).CloneCount)

	_, err = e.client.DetachClone(t.Context(), filestorage.DetachCloneRequest{FileSystemId: resp.Id})
	assertStatus(t, err, http.StatusConflict, "IncorrectState")

	_, err = e.client.DeleteFileSystem(t.Context(), filestorage.DeleteFileSystemRequest{FileSystemId: parent.Id})
	require.NoError(t, err)
	e.settle()

	snap, err := e.client.GetSnapshot(t.Context(), filestorage.GetSnapshotRequest{SnapshotId: sn.Id})
	require.NoError(t, err)
	assert.Equal(
		t,
		filestorage.SnapshotLifecycleStateDeleted,
		snap.LifecycleState,
		"snapshots go with their file system",
	)
}

func TestCloneRejects(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	parent := e.createFS(t, nil)
	sn := e.createSnapshot(t, parent.Id, "base")

	_, err := e.client.CreateFileSystem(t.Context(), filestorage.CreateFileSystemRequest{
		AvailabilityDomain: new(otherAD), CompartmentId: new(testCompartment), SourceSnapshotId: sn.Id,
	})
	assertStatus(t, err, http.StatusBadRequest, "InvalidParameter")

	auto, err := e.client.CreateFileSystem(t.Context(), filestorage.CreateFileSystemRequest{
		AvailabilityDomain: new(testAD), CompartmentId: new(testCompartment), SourceSnapshotId: sn.Id,
		CloneAttachStatus: filestorage.CreateFileSystemDetailsCloneAttachStatusDetach,
	})
	require.NoError(t, err)

	e.clock.Advance(time.Minute)
	e.backend.Sweep()
	assert.Equal(
		t,
		filestorage.FileSystemCloneAttachStatusDetached,
		e.getFS(t, auto.Id).CloneAttachStatus,
		"DETACH detaches after hydration",
	)
}
