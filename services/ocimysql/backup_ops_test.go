package ocimysql_test

import (
	"net/http"
	"testing"
	"time"

	my "github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const remoteRegion = "us-phoenix-1"

func TestCopyBackup(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	db := e.mkDB(t, "src", nil)
	src := e.mkBackup(t, db.Id, "original")

	copyReq := func(mod func(*my.CopyBackupDetails)) (my.CopyBackupResponse, error) {
		d := my.CopyBackupDetails{
			CompartmentId: new(testCompartment), SourceBackupId: src.Id, SourceRegion: new(remoteRegion),
		}
		if mod != nil {
			mod(&d)
		}

		return e.bk.CopyBackup(ctx, my.CopyBackupRequest{CopyBackupDetails: d})
	}

	cp, err := copyReq(func(d *my.CopyBackupDetails) {
		d.DisplayName, d.BackupCopyRetentionInDays = new("copy"), new(30)
	})
	require.NoError(t, err)
	require.NotNil(t, cp.OpcWorkRequestId)
	assert.NotEqual(t, *src.Id, *cp.Id)
	assert.Equal(t, "copy", *cp.DisplayName)
	assert.Equal(t, 30, *cp.RetentionInDays)
	assert.Equal(t, *src.Id, *cp.ImmediateSourceBackupId)
	assert.Equal(t, *src.Id, *cp.OriginalSourceBackupId)
	assert.NotNil(t, cp.TimeCopyCreated)
	assert.Equal(t, *db.Id, *cp.DbSystemId)

	wr, err := e.wr.GetWorkRequest(ctx, my.GetWorkRequestRequest{WorkRequestId: cp.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, my.WorkRequestOperationTypeCopyBackup, wr.OperationType)

	e.settle()

	got, err := e.bk.GetBackup(ctx, my.GetBackupRequest{BackupId: cp.Id})
	require.NoError(t, err)
	assert.Equal(t, my.BackupLifecycleStateActive, got.LifecycleState)

	again, err := copyReq(nil)
	require.NoError(t, err)
	assert.Equal(t, "original", *again.DisplayName, "the name defaults to the source's")

	e.settle()

	chain, err := e.bk.CopyBackup(ctx, my.CopyBackupRequest{
		CompartmentId: new(testCompartment), SourceBackupId: cp.Id, SourceRegion: new(remoteRegion)})
	require.NoError(t, err)
	assert.Equal(t, *cp.Id, *chain.ImmediateSourceBackupId)
	assert.Equal(t, *src.Id, *chain.OriginalSourceBackupId, "the original source is kept along a chain of copies")
}

func TestCopyBackupErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod    func(d *my.CopyBackupDetails)
		name   string
		status int
	}{
		{func(d *my.CopyBackupDetails) { d.SourceBackupId = nil }, "missing source", http.StatusBadRequest},
		{
			func(d *my.CopyBackupDetails) { d.SourceBackupId = new("ocid1.mysqlbackup.oc1.iad.nope") },
			"unknown source",
			http.StatusNotFound,
		},
		{func(d *my.CopyBackupDetails) { d.SourceRegion = new("us-ashburn-1") }, "same region", http.StatusBadRequest},
		{
			func(d *my.CopyBackupDetails) { d.CompartmentId = new(otherCompart) },
			"other compartment",
			http.StatusBadRequest,
		},
		{func(d *my.CopyBackupDetails) { d.BackupCopyRetentionInDays = new(400) }, "retention", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			db := e.mkDB(t, "src", nil)
			src := e.mkBackup(t, db.Id, "original")

			d := my.CopyBackupDetails{
				CompartmentId: new(testCompartment), SourceBackupId: src.Id, SourceRegion: new(remoteRegion),
			}
			tt.mod(&d)

			_, err := e.bk.CopyBackup(t.Context(), my.CopyBackupRequest{CopyBackupDetails: d})
			assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestCopyBackupCarriesData(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	ctx := t.Context()
	db := e.mkEngineDB(t, "src", nil)
	l.addTable(*db.Id, testTable)

	created, err := e.bk.CreateBackup(ctx, my.CreateBackupRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	src := e.awaitBackup(t, created.Id)

	cp, err := e.bk.CopyBackup(ctx, my.CopyBackupRequest{
		CompartmentId: new(testCompartment), SourceBackupId: src.Id, SourceRegion: new(remoteRegion)})
	require.NoError(t, err)
	assert.Equal(t, my.BackupLifecycleStateActive, cp.LifecycleState, "the data is copied with it")

	clone := e.createDB(t, func() my.CreateDbSystemDetails {
		d := dbDetails("from-copy")
		d.AdminUsername, d.AdminPassword = nil, nil
		d.Source = my.CreateDbSystemSourceFromBackupDetails{BackupId: cp.Id}

		return d
	}())
	e.awaitState(t, clone.Id, my.DbSystemLifecycleStateActive)

	l.mu.Lock()
	defer l.mu.Unlock()

	assert.Equal(t, []string{testTable}, l.insts[*clone.Id].tables, "a copied backup restores the same data")
}

func TestValidateBackupStateOnly(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	db := e.mkDB(t, "src", nil)
	bk := e.mkBackup(t, db.Id, "snap")

	require.NotNil(t, bk.BackupValidationDetails)
	assert.Equal(t, my.BackupValidationDetailsValidationStatusNotValidated, bk.BackupValidationDetails.ValidationStatus)

	got, err := e.bk.ValidateBackup(ctx, my.ValidateBackupRequest{
		BackupId: bk.Id, IsPreparedBackupRequired: new(false),
	})
	require.NoError(t, err)
	require.NotNil(t, got.OpcWorkRequestId)
	require.NotNil(t, got.Etag)

	v := got.BackupValidationDetails
	require.NotNil(t, v)
	assert.Equal(t, my.BackupValidationDetailsValidationStatusNeedsAttention, v.ValidationStatus,
		"without a database engine there is nothing to restore the backup onto")
	assert.NotEmpty(t, *v.ErrorMessage)
	assert.NotNil(t, v.TimeLastValidated)

	_, err = e.bk.ValidateBackup(ctx, my.ValidateBackupRequest{
		BackupId: bk.Id, IfMatch: new("stale"),
		IsPreparedBackupRequired: new(false),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.bk.ValidateBackup(ctx, my.ValidateBackupRequest{
		BackupId:                 new("ocid1.mysqlbackup.oc1.iad.nope"),
		IsPreparedBackupRequired: new(false),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestValidateBackupEngine(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	ctx := t.Context()
	db := e.mkEngineDB(t, "src", nil)
	l.addTable(*db.Id, testTable)

	created, err := e.bk.CreateBackup(ctx, my.CreateBackupRequest{DbSystemId: db.Id})
	require.NoError(t, err)
	bk := e.awaitBackup(t, created.Id)

	_, err = e.bk.ValidateBackup(ctx, my.ValidateBackupRequest{
		BackupId: bk.Id, IsPreparedBackupRequired: new(true),
	})
	require.NoError(t, err)

	var v *my.BackupValidationDetails

	require.Eventually(t, func() bool {
		got, gerr := e.bk.GetBackup(ctx, my.GetBackupRequest{BackupId: bk.Id})
		v = got.BackupValidationDetails

		return gerr == nil && v != nil && v.ValidationStatus == my.BackupValidationDetailsValidationStatusValidated
	}, e.waitFor(), 20*time.Millisecond)

	assert.Equal(t, my.BackupValidationDetailsBackupPreparationStatusPrepared, v.BackupPreparationStatus)
	assert.NotNil(t, v.PreparedBackupDetails.TimePrepared)
	assert.NotEmpty(t, *v.EstimatedRestoreDuration)

	l.mu.Lock()
	defer l.mu.Unlock()

	last := l.specs[len(l.specs)-1]
	assert.NotEqual(t, *db.Id, last.ID, "the backup is restored onto a throwaway server")
	assert.True(t, l.insts[last.ID].closed)
	assert.True(t, l.insts[last.ID].purged, "which is removed with its volume")
	assert.Equal(t, []string{testTable}, l.insts[last.ID].tables)
}
