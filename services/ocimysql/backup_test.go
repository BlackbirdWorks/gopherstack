package ocimysql_test

import (
	"net/http"
	"testing"
	"time"

	my "github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) mkBackup(t *testing.T, dbID *string, name string) my.Backup {
	t.Helper()

	resp, err := e.bk.CreateBackup(t.Context(), my.CreateBackupRequest{
		DbSystemId: dbID, DisplayName: new(name),
	})
	require.NoError(t, err)
	require.NotNil(t, resp.OpcWorkRequestId)
	assert.Equal(t, my.BackupLifecycleStateCreating, resp.LifecycleState)
	e.settle()

	got, err := e.bk.GetBackup(t.Context(), my.GetBackupRequest{BackupId: resp.Id})
	require.NoError(t, err)

	return got.Backup
}

func TestBackupLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	db := e.mkDB(t, "src", nil)

	created, err := e.bk.CreateBackup(ctx, my.CreateBackupRequest{
		DbSystemId: db.Id, DisplayName: new("nightly"), RetentionInDays: new(10),
		FreeformTags: map[string]string{"k": "v"},
	})
	require.NoError(t, err)

	wr, err := e.wr.GetWorkRequest(ctx, my.GetWorkRequestRequest{WorkRequestId: created.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, my.WorkRequestOperationTypeCreateBackup, wr.OperationType)

	e.settle()

	got, err := e.bk.GetBackup(ctx, my.GetBackupRequest{BackupId: created.Id})
	require.NoError(t, err)
	bk := got.Backup
	assert.Equal(t, my.BackupLifecycleStateActive, bk.LifecycleState)
	assert.Equal(t, my.BackupCreationTypeManual, bk.CreationType)
	assert.Equal(t, my.BackupBackupTypeFull, bk.BackupType)
	assert.Equal(t, db.Id, bk.DbSystemId)
	assert.Equal(t, 10, *bk.RetentionInDays)
	assert.Equal(t, shapeSmall, *bk.ShapeName)
	assert.Equal(t, "8.4.6", *bk.MysqlVersion)
	assert.Equal(t, adminUser, *bk.DbSystemSnapshot.AdminUsername)
	assert.Equal(t, "v", bk.FreeformTags["k"])
	require.NotNil(t, got.Etag)

	_, err = e.bk.UpdateBackup(ctx, my.UpdateBackupRequest{
		BackupId: bk.Id, IfMatch: new("stale"), DisplayName: new("x"),
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	upd, err := e.bk.UpdateBackup(ctx, my.UpdateBackupRequest{
		BackupId: bk.Id, IfMatch: got.Etag,
		DisplayName: new("renamed"), Description: new("desc"), RetentionInDays: new(20),
	})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *upd.DisplayName)
	assert.Equal(t, 20, *upd.RetentionInDays)

	lst, err := e.bk.ListBackups(ctx, my.ListBackupsRequest{CompartmentId: new(testCompartment), DbSystemId: db.Id})
	require.NoError(t, err)
	require.Len(t, lst.Items, 1)
	assert.Equal(t, "desc", *lst.Items[0].Description)

	del, err := e.bk.DeleteBackup(ctx, my.DeleteBackupRequest{BackupId: bk.Id})
	require.NoError(t, err)
	require.NotNil(t, del.OpcWorkRequestId)

	mid, err := e.bk.GetBackup(ctx, my.GetBackupRequest{BackupId: bk.Id})
	require.NoError(t, err)
	assert.Equal(t, my.BackupLifecycleStateDeleting, mid.LifecycleState)

	_, err = e.bk.DeleteBackup(ctx, my.DeleteBackupRequest{BackupId: bk.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	e.settle()

	gone, err := e.bk.GetBackup(ctx, my.GetBackupRequest{BackupId: bk.Id})
	require.NoError(t, err)
	assert.Equal(t, my.BackupLifecycleStateDeleted, gone.LifecycleState)

	lst, err = e.bk.ListBackups(ctx, my.ListBackupsRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	assert.Empty(t, lst.Items)

	_, err = e.bk.UpdateBackup(ctx, my.UpdateBackupRequest{
		BackupId: bk.Id, DisplayName: new("late"),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestCreateBackupValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		det    func(db my.DbSystem) my.CreateBackupDetails
		code   string
		status int
	}{
		{
			"no db system",
			func(my.DbSystem) my.CreateBackupDetails { return my.CreateBackupDetails{} },
			"MissingParameter",
			400,
		},
		{"unknown db system", func(my.DbSystem) my.CreateBackupDetails {
			return my.CreateBackupDetails{DbSystemId: new("ocid1.mysqldbsystem.oc1.iad.nope")}
		}, "NotAuthorizedOrNotFound", 404},
		{"bad retention", func(db my.DbSystem) my.CreateBackupDetails {
			return my.CreateBackupDetails{DbSystemId: db.Id, RetentionInDays: new(0)}
		}, "InvalidParameter", 400},
		{"long name", func(db my.DbSystem) my.CreateBackupDetails {
			return my.CreateBackupDetails{DbSystemId: db.Id, DisplayName: new(string(make([]byte, 300)))}
		}, "InvalidParameter", 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			db := e.mkDB(t, "src", nil)

			_, err := e.bk.CreateBackup(t.Context(), my.CreateBackupRequest{CreateBackupDetails: tt.det(db)})
			se := svcErr(t, err)
			assert.Equal(t, tt.status, se.GetHTTPStatusCode())
			assert.Equal(t, tt.code, se.GetCode())
		})
	}

	e := newEnv(t, envOpts{})
	db := e.mkDB(t, "stopped", nil)

	_, err := e.db.StopDbSystem(t.Context(), my.StopDbSystemRequest{
		DbSystemId: db.Id, ShutdownType: my.InnoDbShutdownModeFast,
	})
	require.NoError(t, err)

	_, err = e.bk.CreateBackup(
		t.Context(),
		my.CreateBackupRequest{DbSystemId: db.Id},
	)
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "backups need an ACTIVE DB system")
}

func TestRestoreFromBackup(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	src := e.mkDB(t, "src", func(d *my.CreateDbSystemDetails) {
		d.DataStorageSizeInGBs = new(120)
		d.ShapeName = new(shapeBig)
	})
	bk := e.mkBackup(t, src.Id, "snap")

	restored := e.mkDB(t, "copy", func(d *my.CreateDbSystemDetails) {
		d.AdminUsername, d.AdminPassword = nil, nil
		d.Source = my.CreateDbSystemSourceFromBackupDetails{BackupId: bk.Id}
		d.ShapeName = new(shapeBig)
	})

	require.IsType(t, my.DbSystemSourceFromBackup{}, restored.Source)
	assert.Equal(t, *bk.Id, *restored.Source.(my.DbSystemSourceFromBackup).BackupId)
	assert.Equal(t, 120, *restored.DataStorageSizeInGBs, "size follows the backup")
	assert.NotEqual(t, *src.IpAddress, *restored.IpAddress)

	override := e.mkDB(t, "copy2", func(d *my.CreateDbSystemDetails) {
		d.Source = my.CreateDbSystemSourceFromBackupDetails{BackupId: bk.Id}
		d.AdminPassword = new("Other#Passw0rd")
	})
	assert.Equal(t, my.DbSystemLifecycleStateActive, override.LifecycleState)

	_, err := e.db.CreateDbSystem(ctx, my.CreateDbSystemRequest{CreateDbSystemDetails: func() my.CreateDbSystemDetails {
		d := dbDetails("weak")
		d.Source = my.CreateDbSystemSourceFromBackupDetails{BackupId: bk.Id}
		d.AdminPassword = new("weak")

		return d
	}()})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.bk.DeleteBackup(ctx, my.DeleteBackupRequest{BackupId: bk.Id})
	require.NoError(t, err)

	_, err = e.db.CreateDbSystem(ctx, my.CreateDbSystemRequest{CreateDbSystemDetails: func() my.CreateDbSystemDetails {
		d := dbDetails("late")
		d.Source = my.CreateDbSystemSourceFromBackupDetails{BackupId: bk.Id}

		return d
	}()})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode(), "a DELETING backup is not restorable")
}

type autoBackupStart struct {
	start time.Time
	name  string
}

func autoBackupStarts() []autoBackupStart {
	return []autoBackupStart{
		{name: "0010", start: time.Date(2026, 3, 10, 0, 10, 0, 0, time.UTC)},
		{name: "1200", start: time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)},
		{name: "2242", start: time.Date(2026, 3, 10, 22, 42, 0, 0, time.UTC)},
		{name: "2359", start: time.Date(2026, 3, 10, 23, 59, 0, 0, time.UTC)},
	}
}

func TestAutomaticBackupsAndRetention(t *testing.T) {
	t.Parallel()

	for _, tt := range autoBackupStarts() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{start: tt.start})
			ctx := t.Context()

			window := e.clock.Now().Add(time.Hour).Format("15:04")
			db := e.mkDB(t, "auto", func(d *my.CreateDbSystemDetails) {
				d.BackupPolicy = &my.CreateBackupPolicyDetails{WindowStartTime: &window, RetentionInDays: new(2)}
			})

			e.backend.Sweep()

			lst, err := e.bk.ListBackups(ctx, my.ListBackupsRequest{CompartmentId: new(testCompartment)})
			require.NoError(t, err)
			assert.Empty(t, lst.Items, "the window has not opened yet")

			e.clock.Advance(2 * time.Hour)
			e.backend.Sweep()
			e.backend.Sweep()
			e.settle()

			lst, err = e.bk.ListBackups(ctx, my.ListBackupsRequest{
				CompartmentId: new(testCompartment), CreationType: my.BackupCreationTypeAutomatic,
			})
			require.NoError(t, err)
			require.Len(t, lst.Items, 1, "one automatic backup per window")
			assert.Equal(t, db.Id, lst.Items[0].DbSystemId)
			assert.Equal(t, 2, *lst.Items[0].RetentionInDays)

			e.clock.Advance(3 * 24 * time.Hour)
			e.backend.Sweep()

			lst, err = e.bk.ListBackups(ctx, my.ListBackupsRequest{
				CompartmentId: new(testCompartment), LifecycleState: my.BackupLifecycleStateDeleted,
			})
			require.NoError(t, err)
			assert.NotEmpty(t, lst.Items, "backups past their retention are deleted")
		})
	}
}

func TestDeletionPolicyBackupRetention(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		retain   my.CreateDeletionPolicyDetailsAutomaticBackupRetentionEnum
		wantLive int
	}{
		{"delete automatic backups", my.CreateDeletionPolicyDetailsAutomaticBackupRetentionDelete, 1},
		{"retain automatic backups", my.CreateDeletionPolicyDetailsAutomaticBackupRetentionRetain, 2},
	}

	for _, tt := range tests {
		for _, st := range autoBackupStarts() {
			t.Run(tt.name+" "+st.name, func(t *testing.T) {
				t.Parallel()

				e := newEnv(t, envOpts{start: st.start})
				window := e.clock.Now().Add(time.Hour).Format("15:04")
				db := e.mkDB(t, "pol", func(d *my.CreateDbSystemDetails) {
					d.BackupPolicy = &my.CreateBackupPolicyDetails{WindowStartTime: &window}
					d.DeletionPolicy = &my.CreateDeletionPolicyDetails{AutomaticBackupRetention: tt.retain}
				})
				e.mkBackup(t, db.Id, "manual")

				e.clock.Advance(2 * time.Hour)
				e.backend.Sweep()
				e.settle()

				_, err := e.db.DeleteDbSystem(t.Context(), my.DeleteDbSystemRequest{DbSystemId: db.Id})
				require.NoError(t, err)

				lst, err := e.bk.ListBackups(t.Context(), my.ListBackupsRequest{CompartmentId: new(testCompartment)})
				require.NoError(t, err)
				assert.Len(t, lst.Items, tt.wantLive)
			})
		}
	}
}

func TestChangeBackupCompartment(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	db := e.mkDB(t, "src", nil)
	bk := e.mkBackup(t, db.Id, "mv")

	_, err := e.bk.ChangeBackupCompartment(t.Context(), my.ChangeBackupCompartmentRequest{
		BackupId:      bk.Id,
		CompartmentId: new(otherCompart),
	})
	require.NoError(t, err)

	moved, err := e.bk.GetBackup(t.Context(), my.GetBackupRequest{BackupId: bk.Id})
	require.NoError(t, err)
	assert.Equal(t, otherCompart, *moved.CompartmentId)

	_, err = e.bk.ChangeBackupCompartment(t.Context(), my.ChangeBackupCompartmentRequest{
		BackupId:      new("ocid1.mysqlbackup.oc1.iad.nope"),
		CompartmentId: new(otherCompart),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}
