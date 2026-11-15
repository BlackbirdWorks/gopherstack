package ocimysql_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	my "github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withPitr(d *my.CreateDbSystemDetails) {
	d.BackupPolicy = &my.CreateBackupPolicyDetails{
		IsEnabled: new(true), RetentionInDays: new(7), PitrPolicy: &my.PitrPolicy{IsEnabled: new(true)},
	}
}

func pitrSource(db *string, at *time.Time) my.CreateDbSystemSourceFromPitrDetails {
	src := my.CreateDbSystemSourceFromPitrDetails{DbSystemId: db}
	if at != nil {
		src.RecoveryPoint = &common.SDKTime{Time: *at}
	}

	return src
}

func (e *env) createFromPitr(t *testing.T, name string, src my.CreateDbSystemSourceFromPitrDetails) (
	my.CreateDbSystemResponse, error,
) {
	t.Helper()

	d := dbDetails(name)
	d.AdminUsername, d.AdminPassword = nil, nil
	d.Source = src

	return e.db.CreateDbSystem(t.Context(), my.CreateDbSystemRequest{CreateDbSystemDetails: d})
}

func TestPitrWindow(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	db := e.mkDB(t, "pitr", withPitr)
	require.NotNil(t, db.PointInTimeRecoveryDetails)

	first := db.PointInTimeRecoveryDetails.TimeEarliestRecoveryPoint.Time
	e.clock.Advance(48 * time.Hour)

	later := e.getDB(t, db.Id).PointInTimeRecoveryDetails
	assert.Equal(t, first, later.TimeEarliestRecoveryPoint.Time, "recovery reaches back to when PITR was enabled")
	assert.Equal(t, e.clock.Now(), later.TimeLatestRecoveryPoint.Time)

	e.clock.Advance(10 * 24 * time.Hour)

	capped := e.getDB(t, db.Id).PointInTimeRecoveryDetails
	assert.Equal(t, e.clock.Now().AddDate(0, 0, -7), capped.TimeEarliestRecoveryPoint.Time,
		"and no further than the backup retention period")
}

func TestPitrPolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod  func(*my.CreateDbSystemDetails)
		name string
	}{
		{func(d *my.CreateDbSystemDetails) { withPitr(d); d.ShapeName = new("MySQL.Free") }, "free shape"},
		{func(d *my.CreateDbSystemDetails) {
			d.BackupPolicy = &my.CreateBackupPolicyDetails{
				IsEnabled: new(false), PitrPolicy: &my.PitrPolicy{IsEnabled: new(true)},
			}
		}, "backups off"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			d := dbDetails("bad")
			tt.mod(&d)

			_, err := e.db.CreateDbSystem(t.Context(), my.CreateDbSystemRequest{CreateDbSystemDetails: d})
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestPitrToggle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	db := e.mkDB(t, "plain", nil)
	assert.Nil(t, db.PointInTimeRecoveryDetails)

	e.clock.Advance(time.Hour)

	_, err := e.db.UpdateDbSystem(ctx, my.UpdateDbSystemRequest{
		DbSystemId:   db.Id,
		BackupPolicy: &my.UpdateBackupPolicyDetails{PitrPolicy: &my.PitrPolicy{IsEnabled: new(true)}},
	})
	require.NoError(t, err)

	on := e.getDB(t, db.Id)
	require.NotNil(t, on.PointInTimeRecoveryDetails)
	assert.Equal(t, e.clock.Now(), on.PointInTimeRecoveryDetails.TimeEarliestRecoveryPoint.Time,
		"enabling PITR starts the window now, not at creation")

	_, err = e.db.UpdateDbSystem(ctx, my.UpdateDbSystemRequest{
		DbSystemId:   db.Id,
		BackupPolicy: &my.UpdateBackupPolicyDetails{PitrPolicy: &my.PitrPolicy{IsEnabled: new(false)}},
	})
	require.NoError(t, err)
	assert.Nil(t, e.getDB(t, db.Id).PointInTimeRecoveryDetails)
}

func TestCreateFromPitr(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	src := e.mkDB(t, "src", func(d *my.CreateDbSystemDetails) { withPitr(d); d.MysqlVersion = new("8.4.5") })
	e.clock.Advance(2 * time.Hour)

	at := e.clock.Now().Add(-time.Hour)
	resp, err := e.createFromPitr(t, "clone", pitrSource(src.Id, &at))
	require.NoError(t, err)

	got, ok := resp.Source.(my.DbSystemSourceFromPitr)
	require.True(t, ok)
	assert.Equal(t, *src.Id, *got.DbSystemId)
	assert.Equal(t, at, got.RecoveryPoint.Time)
	assert.Equal(t, "8.4.5", *resp.MysqlVersion, "the version defaults to the source's")

	e.settle()

	clone := e.getDB(t, resp.Id)
	assert.Equal(t, my.DbSystemLifecycleStateActive, clone.LifecycleState)
	assert.Equal(t, *src.DataStorageSizeInGBs, *clone.DataStorageSizeInGBs)

	latest, err := e.createFromPitr(t, "latest", pitrSource(src.Id, nil))
	require.NoError(t, err)
	assert.Nil(t, latest.Source.(my.DbSystemSourceFromPitr).RecoveryPoint)

	_, err = e.db.DeleteDbSystem(ctx, my.DeleteDbSystemRequest{DbSystemId: src.Id})
	require.NoError(t, err)
}

func TestCreateFromPitrErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source func(e *env, src, plain my.DbSystem) (my.CreateDbSystemSourceFromPitrDetails, string)
		name   string
		status int
	}{
		{func(*env, my.DbSystem, my.DbSystem) (my.CreateDbSystemSourceFromPitrDetails, string) {
			return my.CreateDbSystemSourceFromPitrDetails{}, ""
		}, "missing system", http.StatusBadRequest},
		{func(*env, my.DbSystem, my.DbSystem) (my.CreateDbSystemSourceFromPitrDetails, string) {
			return pitrSource(new("ocid1.mysqldbsystem.oc1.iad.nope"), nil), ""
		}, "unknown system", http.StatusBadRequest},
		{func(_ *env, _, plain my.DbSystem) (my.CreateDbSystemSourceFromPitrDetails, string) {
			return pitrSource(plain.Id, nil), ""
		}, "pitr off", http.StatusBadRequest},
		{func(e *env, src, _ my.DbSystem) (my.CreateDbSystemSourceFromPitrDetails, string) {
			at := e.clock.Now().Add(time.Hour)

			return pitrSource(src.Id, &at), ""
		}, "future point", http.StatusBadRequest},
		{func(e *env, src, _ my.DbSystem) (my.CreateDbSystemSourceFromPitrDetails, string) {
			at := e.clock.Now().Add(-30 * 24 * time.Hour)

			return pitrSource(src.Id, &at), ""
		}, "before the window", http.StatusBadRequest},
		{func(_ *env, src, _ my.DbSystem) (my.CreateDbSystemSourceFromPitrDetails, string) {
			return pitrSource(src.Id, nil), "8.0.39"
		}, "older version", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			src := e.mkDB(t, "src", func(d *my.CreateDbSystemDetails) { withPitr(d); d.MysqlVersion = new("8.4.5") })
			plain := e.mkDB(t, "plain", nil)
			e.clock.Advance(time.Hour)

			s, version := tt.source(e, src, plain)
			d := dbDetails("clone")
			d.AdminUsername, d.AdminPassword, d.Source = nil, nil, s

			if version != "" {
				d.MysqlVersion = &version
			}

			_, err := e.db.CreateDbSystem(t.Context(), my.CreateDbSystemRequest{CreateDbSystemDetails: d})
			assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestPitrStoppedAndDeletedSource(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	src := e.mkDB(t, "src", func(d *my.CreateDbSystemDetails) {
		withPitr(d)
		d.DeletionPolicy = &my.CreateDeletionPolicyDetails{
			AutomaticBackupRetention: my.CreateDeletionPolicyDetailsAutomaticBackupRetentionRetain,
		}
	})

	e.clock.Advance(time.Hour)

	_, err := e.db.StopDbSystem(
		ctx,
		my.StopDbSystemRequest{DbSystemId: src.Id, ShutdownType: my.InnoDbShutdownModeFast},
	)
	require.NoError(t, err)
	e.settle()

	stoppedAt := e.clock.Now()

	e.clock.Advance(time.Hour)

	frozen := e.getDB(t, src.Id).PointInTimeRecoveryDetails
	assert.Equal(t, stoppedAt.Add(-5*time.Second), frozen.TimeLatestRecoveryPoint.Time,
		"a stopped DB system's latest recovery point does not move")

	late := e.clock.Now()
	_, err = e.createFromPitr(t, "late", pitrSource(src.Id, &late))
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.createFromPitr(t, "from stopped", pitrSource(src.Id, nil))
	require.NoError(t, err, "a stopped DB system can be recovered from")

	_, err = e.db.StartDbSystem(ctx, my.StartDbSystemRequest{DbSystemId: src.Id})
	require.NoError(t, err)
	e.settle()

	assert.Equal(t, e.clock.Now(), e.getDB(t, src.Id).PointInTimeRecoveryDetails.TimeLatestRecoveryPoint.Time)

	_, err = e.db.DeleteDbSystem(ctx, my.DeleteDbSystemRequest{DbSystemId: src.Id})
	require.NoError(t, err)
	e.settle()

	_, err = e.createFromPitr(t, "from deleted", pitrSource(src.Id, nil))
	require.NoError(t, err, "retained automatic backups keep a deleted DB system recoverable")

	drop := e.mkDB(t, "drop", withPitr)
	_, err = e.db.DeleteDbSystem(ctx, my.DeleteDbSystemRequest{DbSystemId: drop.Id})
	require.NoError(t, err)
	e.settle()

	_, err = e.createFromPitr(t, "from dropped", pitrSource(drop.Id, nil))
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(),
		"automatic backups deleted with the DB system leave nothing to recover from")
}

func TestPitrEngine(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	src := e.mkEngineDB(t, "src", withPitr)

	l.mu.Lock()
	assert.True(t, l.insts[*src.Id].pitrOn, "PITR is switched on when the server starts")
	l.mu.Unlock()

	assert.NotNil(t, src.PointInTimeRecoveryDetails)

	e.clock.Advance(2 * time.Second)

	at := e.clock.Now()
	e.clock.Advance(time.Hour)

	resp, err := e.createFromPitr(t, "clone", pitrSource(src.Id, &at))
	require.NoError(t, err)

	clone := e.awaitState(t, resp.Id, my.DbSystemLifecycleStateActive)

	l.mu.Lock()
	srcInst, cloneInst := l.insts[*src.Id], l.insts[*clone.Id]
	spec := l.specs[len(l.specs)-1]
	l.mu.Unlock()

	l.mu.Lock()
	defer l.mu.Unlock()

	assert.Equal(t, at, srcInst.prepAt, "the source plans the replay up to the recovery point")
	require.NotNil(t, cloneInst.applied, "the new server replays the source's binary logs")
	assert.Equal(t, at, cloneInst.applied.At)
	assert.Equal(t, "recovery-net", spec.Network, "the new server joins the source's network")
	assert.True(t, spec.Fresh)
	assert.NotZero(t, spec.ServerID)
	assert.Equal(t, adminPass, cloneInst.admins[adminUser], "the administrator is inherited from the source")
}

func TestPitrEngineToggle(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	ctx := t.Context()
	db := e.mkEngineDB(t, "plain", nil)

	l.mu.Lock()
	assert.False(t, l.insts[*db.Id].pitrOn)
	l.mu.Unlock()

	toggle := func(on bool) {
		_, err := e.db.UpdateDbSystem(ctx, my.UpdateDbSystemRequest{
			DbSystemId:   db.Id,
			BackupPolicy: &my.UpdateBackupPolicyDetails{PitrPolicy: &my.PitrPolicy{IsEnabled: &on}},
		})
		require.NoError(t, err)
		e.awaitState(t, db.Id, my.DbSystemLifecycleStateActive)
	}

	toggle(true)

	l.mu.Lock()
	assert.True(t, l.insts[*db.Id].pitrOn, "enabling PITR takes a recovery base on the server")
	l.mu.Unlock()

	toggle(false)

	l.mu.Lock()
	assert.False(t, l.insts[*db.Id].pitrOn)
	assert.Equal(t, 1, l.insts[*db.Id].pitrOff)
	l.mu.Unlock()
}
