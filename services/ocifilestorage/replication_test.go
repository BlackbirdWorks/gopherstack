package ocifilestorage_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/filestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func (e *env) createReplication(t *testing.T, src, dst *string) filestorage.Replication {
	t.Helper()

	resp, err := e.client.CreateReplication(t.Context(), filestorage.CreateReplicationRequest{
		CompartmentId: new(
			testCompartment,
		),
		SourceId:            src,
		TargetId:            dst,
		DisplayName:         new("rep"),
		ReplicationInterval: new(int64(15)),
	})
	require.NoError(t, err)

	return resp.Replication
}

func (e *env) getReplication(t *testing.T, id *string) filestorage.Replication {
	t.Helper()

	resp, err := e.client.GetReplication(t.Context(), filestorage.GetReplicationRequest{ReplicationId: id})
	require.NoError(t, err)

	return resp.Replication
}

func TestReplicationCycles(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	src := e.createFS(t, func(d *filestorage.CreateFileSystemDetails) { d.DisplayName = new("src") })
	dst := e.createFS(t, func(d *filestorage.CreateFileSystemDetails) {
		d.DisplayName = new("dst")
		d.AvailabilityDomain = new(otherAD)
	})

	rep := e.createReplication(t, src.Id, dst.Id)
	assert.Equal(t, filestorage.ReplicationLifecycleStateCreating, rep.LifecycleState)
	assert.Equal(t, int64(15), *rep.ReplicationInterval)
	assert.Equal(t, testAD, *rep.AvailabilityDomain)
	assert.NotEmpty(t, *rep.ReplicationTargetId)

	assert.Equal(t, 1, *e.getFS(t, src.Id).ReplicationSourceCount)
	assert.Equal(t, *rep.ReplicationTargetId, *e.getFS(t, dst.Id).ReplicationTargetId)
	assert.False(t, *e.getFS(t, dst.Id).IsTargetable)

	e.settle()
	assert.Equal(t, filestorage.ReplicationDeltaStatusCapturing, e.getReplication(t, rep.Id).DeltaStatus)

	for _, want := range []filestorage.ReplicationDeltaStatusEnum{
		filestorage.ReplicationDeltaStatusTransferring,
		filestorage.ReplicationDeltaStatusApplying,
		filestorage.ReplicationDeltaStatusIdle,
	} {
		e.clock.Advance(3 * time.Second)
		e.backend.Sweep()
		assert.Equal(t, want, e.getReplication(t, rep.Id).DeltaStatus)
	}

	done := e.getReplication(t, rep.Id)
	assert.Equal(t, filestorage.ReplicationLifecycleStateActive, done.LifecycleState)
	assert.NotNil(t, done.RecoveryPointTime)
	assert.NotEmpty(t, *done.LastSnapshotId)

	tgt, err := e.client.GetReplicationTarget(
		t.Context(),
		filestorage.GetReplicationTargetRequest{ReplicationTargetId: rep.ReplicationTargetId},
	)
	require.NoError(t, err)
	assert.Equal(t, *rep.Id, *tgt.ReplicationId)
	assert.Equal(t, *done.LastSnapshotId, *tgt.LastSnapshotId)
	assert.Equal(t, otherAD, *tgt.AvailabilityDomain, "the target lives in the target file system's AD")

	e.clock.Advance(16 * time.Minute)
	e.backend.Sweep()
	assert.Equal(
		t,
		filestorage.ReplicationDeltaStatusCapturing,
		e.getReplication(t, rep.Id).DeltaStatus,
		"the next cycle starts after the interval",
	)
}

func TestReplicationRejects(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	src := e.createFS(t, nil)
	dst := e.createFS(t, nil)
	exported := e.createFS(t, nil)
	mt := e.createMT(t, nil)
	e.createExport(t, mt.ExportSetId, exported.Id, "/x", nil)
	withSnap := e.createFS(t, nil)
	e.createSnapshot(t, withSnap.Id, "s")

	create := func(s, d *string, interval *int64) error {
		_, err := e.client.CreateReplication(t.Context(), filestorage.CreateReplicationRequest{
			CompartmentId: new(testCompartment), SourceId: s, TargetId: d, ReplicationInterval: interval,
		})

		return err
	}

	assertStatus(t, create(src.Id, src.Id, nil), http.StatusBadRequest, "InvalidParameter")
	assertStatus(t, create(src.Id, dst.Id, new(int64(5))), http.StatusBadRequest, "InvalidParameter")
	assertStatus(t, create(src.Id, exported.Id, nil), http.StatusConflict, "Conflict")
	assertStatus(t, create(src.Id, withSnap.Id, nil), http.StatusConflict, "Conflict")
	assertStatus(
		t,
		create(src.Id, new("ocid1.filesystem.oc1.iad.nope"), nil),
		http.StatusNotFound,
		"NotAuthorizedOrNotFound",
	)
	assertStatus(t, create(nil, dst.Id, nil), http.StatusBadRequest, "MissingParameter")

	require.NoError(t, create(src.Id, dst.Id, nil))
	assertStatus(t, create(src.Id, dst.Id, nil), http.StatusConflict, "Conflict")

	third := e.createFS(t, nil)
	assertStatus(t, create(dst.Id, third.Id, nil), http.StatusConflict, "Conflict")
}

func TestReplicationUpdateAndDelete(t *testing.T) {
	t.Parallel()

	modes := []filestorage.DeleteReplicationDeleteModeEnum{
		"",
		filestorage.DeleteReplicationDeleteModeFinishCycleIfCapturingOrApplying,
		filestorage.DeleteReplicationDeleteModeOneMoreCycle,
		filestorage.DeleteReplicationDeleteModeFinishCycleIfApplying,
	}

	for _, mode := range modes {
		t.Run(string(mode)+"-mode", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			src, dst := e.createFS(t, nil), e.createFS(t, nil)
			rep := e.createReplication(t, src.Id, dst.Id)
			e.settle()

			upd, err := e.client.UpdateReplication(t.Context(), filestorage.UpdateReplicationRequest{
				ReplicationId: rep.Id, DisplayName: new("renamed"), ReplicationInterval: new(int64(30)),
			})
			require.NoError(t, err)
			assert.Equal(t, int64(30), *upd.ReplicationInterval)

			_, err = e.client.UpdateReplication(t.Context(), filestorage.UpdateReplicationRequest{
				ReplicationId: rep.Id, ReplicationInterval: new(int64(1)),
			})
			assertStatus(t, err, http.StatusBadRequest, "InvalidParameter")

			_, err = e.client.DeleteReplicationTarget(
				t.Context(),
				filestorage.DeleteReplicationTargetRequest{ReplicationTargetId: rep.ReplicationTargetId},
			)
			assertStatus(t, err, http.StatusConflict, "Conflict")

			_, err = e.client.DeleteReplication(
				t.Context(),
				filestorage.DeleteReplicationRequest{ReplicationId: rep.Id, DeleteMode: mode},
			)
			require.NoError(t, err)

			for range 6 {
				e.clock.Advance(3 * time.Second)
				e.backend.Sweep()
			}

			assert.Equal(t, filestorage.ReplicationLifecycleStateDeleted, e.getReplication(t, rep.Id).LifecycleState)
			assert.Nil(t, e.getFS(t, dst.Id).ReplicationTargetId)
			assert.Equal(t, 0, *e.getFS(t, src.Id).ReplicationSourceCount)
			assert.True(t, *e.getFS(t, dst.Id).IsTargetable)

			tgt, err := e.client.GetReplicationTarget(
				t.Context(),
				filestorage.GetReplicationTargetRequest{ReplicationTargetId: rep.ReplicationTargetId},
			)
			require.NoError(t, err)
			assert.Equal(t, filestorage.ReplicationTargetLifecycleStateDeleted, tgt.LifecycleState)
		})
	}

	t.Run("bad-mode", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		src, dst := e.createFS(t, nil), e.createFS(t, nil)
		rep := e.createReplication(t, src.Id, dst.Id)

		req, err := http.NewRequestWithContext(
			t.Context(),
			http.MethodDelete,
			e.base+"/20171215/replications/"+*rep.Id+"?deleteMode=SOMETIME",
			http.NoBody,
		)
		require.NoError(t, err)

		resp, err := testclient.New(t).Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})
}

func TestReplicationListsAndGuards(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	src, dst := e.createFS(t, nil), e.createFS(t, nil)
	rep := e.createReplication(t, src.Id, dst.Id)

	reps, err := e.client.ListReplications(t.Context(), filestorage.ListReplicationsRequest{
		CompartmentId: new(testCompartment), AvailabilityDomain: new(testAD), FileSystemId: dst.Id,
	})
	require.NoError(t, err)
	require.Len(t, reps.Items, 1)
	assert.Equal(t, *rep.Id, *reps.Items[0].Id)

	tgts, err := e.client.ListReplicationTargets(t.Context(), filestorage.ListReplicationTargetsRequest{
		CompartmentId: new(testCompartment), AvailabilityDomain: new(testAD),
	})
	require.NoError(t, err)
	assert.Len(t, tgts.Items, 1)

	_, err = e.client.DeleteFileSystem(t.Context(), filestorage.DeleteFileSystemRequest{FileSystemId: src.Id})
	assertStatus(t, err, http.StatusConflict, "Conflict")

	_, err = e.client.AddReplicationLock(t.Context(), filestorage.AddReplicationLockRequest{
		ReplicationId:             rep.Id,
		AddReplicationLockDetails: filestorage.ResourceLock{Type: filestorage.ResourceLockTypeFull},
	})
	require.NoError(t, err)

	_, err = e.client.DeleteReplication(t.Context(), filestorage.DeleteReplicationRequest{ReplicationId: rep.Id})
	assertStatus(t, err, http.StatusConflict, "Conflict")

	_, err = e.client.ChangeReplicationCompartment(t.Context(), filestorage.ChangeReplicationCompartmentRequest{
		ReplicationId: rep.Id, CompartmentId: new(otherCompart),
	})
	assertStatus(t, err, http.StatusConflict, "Conflict")
}

func TestReplicationLimits(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	src := e.createFS(t, nil)
	pol := e.createPolicy(t, nil)
	withPolicy := e.createFS(t, func(d *filestorage.CreateFileSystemDetails) {
		d.FilesystemSnapshotPolicyId = pol.Id
	})

	create := func(s, d *string) error {
		_, err := e.client.CreateReplication(t.Context(), filestorage.CreateReplicationRequest{
			CompartmentId: new(testCompartment), SourceId: s, TargetId: d,
		})

		return err
	}

	assertStatus(t, create(src.Id, withPolicy.Id), http.StatusConflict, "Conflict")

	for range 3 {
		require.NoError(t, create(src.Id, e.createFS(t, nil).Id))
	}

	assertStatus(t, create(src.Id, e.createFS(t, nil).Id), http.StatusBadRequest, "LimitExceeded")
}
