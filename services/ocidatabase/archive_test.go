package ocidatabase_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeArchive stands in for the Data Pump dump of a running database.
type fakeArchive struct {
	imports [][]byte
	dump    []byte
	mu      sync.Mutex
	scn     int64
	failOn  bool
}

func (f *fakeArchive) setDump(b []byte, scn int64) {
	f.mu.Lock()
	f.dump, f.scn = b, scn
	f.mu.Unlock()
}

func (f *fakeArchive) importsSeen() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([][]byte(nil), f.imports...)
}

func (i *echoInstance) Export(_ context.Context, w io.Writer) (int64, error) {
	a := &i.launcher.arch
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.failOn {
		return 0, errFake
	}

	_, err := w.Write(a.dump)

	return a.scn, err
}

func (i *echoInstance) Import(_ context.Context, r io.Reader) error {
	a := &i.launcher.arch

	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.failOn {
		return errFake
	}

	a.imports = append(a.imports, b)

	return nil
}

// settled waits for the backend's background work, then checks the database is AVAILABLE.
func (e *env) settled(t *testing.T, id *string) database.AutonomousDatabase {
	t.Helper()

	e.backend.Idle()

	got := e.get(t, id)
	require.Equal(t, database.AutonomousDatabaseLifecycleStateAvailable, got.LifecycleState, "%v", got.LifecycleDetails)

	return got
}

func (e *env) dataBackup(t *testing.T, id *string, name string) database.AutonomousDatabaseBackup {
	t.Helper()

	resp, err := e.client.CreateAutonomousDatabaseBackup(t.Context(), database.CreateAutonomousDatabaseBackupRequest{
		AutonomousDatabaseId: id, DisplayName: new(name),
	})
	require.NoError(t, err)
	e.settled(t, id)

	return resp.AutonomousDatabaseBackup
}

func (e *env) backupState(t *testing.T, id *string) database.AutonomousDatabaseBackup {
	t.Helper()

	got, err := e.client.GetAutonomousDatabaseBackup(t.Context(),
		database.GetAutonomousDatabaseBackupRequest{AutonomousDatabaseBackupId: id})
	require.NoError(t, err)

	return got.AutonomousDatabaseBackup
}

func (e *env) restoreLatest(t *testing.T, id *string, d database.RestoreAutonomousDatabaseDetails) {
	t.Helper()

	_, err := e.client.RestoreAutonomousDatabase(t.Context(), database.RestoreAutonomousDatabaseRequest{
		AutonomousDatabaseId: id, RestoreAutonomousDatabaseDetails: d,
	})
	require.NoError(t, err)
	e.settled(t, id)
}

func TestBackupCapturesDataAndRestoreReloadsIt(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	db := e.mustCreate(t, createOpts{})
	e.settled(t, db.Id)

	l.arch.setDump([]byte("dump-one"), 100)
	first := e.dataBackup(t, db.Id, "one")
	assert.Equal(t, database.AutonomousDatabaseBackupLifecycleStateActive, e.backupState(t, first.Id).LifecycleState)

	e.clock.Advance(time.Hour)
	l.arch.setDump([]byte("dump-two"), 200)
	e.dataBackup(t, db.Id, "two")

	e.restoreLatest(t, db.Id, database.RestoreAutonomousDatabaseDetails{Latest: new(true)})
	assert.Equal(t, [][]byte{[]byte("dump-two")}, l.arch.importsSeen())

	e.restoreLatest(t, db.Id, database.RestoreAutonomousDatabaseDetails{DatabaseSCN: new("150")})
	assert.Equal(t, [][]byte{[]byte("dump-two"), []byte("dump-one")}, l.arch.importsSeen(),
		"newest backup at or below the SCN")
}

func TestRestoreWithoutMatchingBackupChangesNothing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		detail func(created time.Time) database.RestoreAutonomousDatabaseDetails
		name   string
	}{
		{name: "timestamp before backup", detail: func(c time.Time) database.RestoreAutonomousDatabaseDetails {
			return database.RestoreAutonomousDatabaseDetails{Timestamp: &common.SDKTime{Time: c.Add(time.Second)}}
		}},
		{name: "scn below backup", detail: func(time.Time) database.RestoreAutonomousDatabaseDetails {
			return database.RestoreAutonomousDatabaseDetails{DatabaseSCN: new("50")}
		}},
		{name: "scn not a number", detail: func(time.Time) database.RestoreAutonomousDatabaseDetails {
			return database.RestoreAutonomousDatabaseDetails{DatabaseSCN: new("abc")}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, l := newEngineEnv(t)
			db := e.mustCreate(t, createOpts{})
			created := e.settled(t, db.Id)

			e.clock.Advance(time.Hour)
			l.arch.setDump([]byte("late"), 100)
			e.dataBackup(t, db.Id, "late")

			e.restoreLatest(t, db.Id, tt.detail(created.TimeCreated.Time))
			assert.Empty(t, l.arch.importsSeen())
		})
	}
}

func TestBackupFailureAndLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(e *env, l *echoLauncher)
		name  string
	}{
		{name: "export fails", setup: func(_ *env, l *echoLauncher) { l.arch.failOn = true }},
		{name: "over the size cap", setup: func(e *env, _ *echoLauncher) { e.backend.SetDumpLimit(3) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, l := newEngineEnv(t)
			db := e.mustCreate(t, createOpts{})
			e.settled(t, db.Id)

			l.arch.setDump([]byte("dump-bigger-than-three"), 1)
			tt.setup(e, l)

			bk := e.dataBackup(t, db.Id, "doomed")
			got := e.backupState(t, bk.Id)
			assert.Equal(t, database.AutonomousDatabaseBackupLifecycleStateFailed, got.LifecycleState)
			assert.NotEmpty(t, *got.LifecycleDetails)
			assert.Equal(t, database.AutonomousDatabaseLifecycleStateAvailable, e.get(t, db.Id).LifecycleState)
		})
	}
}

func TestRestoreRejectsTamperedBackup(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	e, l := newEngineEnv(t)
	e.backend.SetDumpDir(dir)

	db := e.mustCreate(t, createOpts{})
	e.settled(t, db.Id)

	l.arch.setDump([]byte("original"), 10)
	bk := e.dataBackup(t, db.Id, "kept")

	require.NoError(t, os.WriteFile(filepath.Join(dir, *bk.Id+".dmp"), []byte("tampered"), 0o600))

	resp, err := e.client.RestoreAutonomousDatabase(t.Context(), database.RestoreAutonomousDatabaseRequest{
		AutonomousDatabaseId: db.Id,
		Latest:               new(true),
	})
	require.NoError(t, err)
	e.settled(t, db.Id)

	assert.Equal(t, "FAILED", string(e.wr(t, resp.OpcWorkRequestId).Status))
	assert.Empty(t, l.arch.importsSeen())
}

func TestDeleteBackupRemovesItsFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	e, l := newEngineEnv(t)
	e.backend.SetDumpDir(dir)

	db := e.mustCreate(t, createOpts{})
	e.settled(t, db.Id)

	l.arch.setDump([]byte("data"), 10)
	bk := e.dataBackup(t, db.Id, "gone")

	path := filepath.Join(dir, *bk.Id+".dmp")
	require.FileExists(t, path)

	_, err := e.client.DeleteAutonomousDatabaseBackup(t.Context(),
		database.DeleteAutonomousDatabaseBackupRequest{AutonomousDatabaseBackupId: bk.Id})
	require.NoError(t, err)
	assert.NoFileExists(t, path)
}

func TestBackupDumpRecordSurvivesSnapshot(t *testing.T) {
	t.Parallel()

	e, l := newEngineEnv(t)
	db := e.mustCreate(t, createOpts{})
	e.settled(t, db.Id)

	l.arch.setDump([]byte("data"), 77)
	bk := e.dataBackup(t, db.Id, "kept")

	snap := e.backend.Snapshot(t.Context())
	assert.Contains(t, string(snap), `"dumpSha256"`)
	assert.Contains(t, string(snap), `"scn":77`)

	e2 := newEnv(t, envOpts{})
	require.NoError(t, e2.backend.Restore(t.Context(), snap))

	got := e2.backupState(t, bk.Id)
	assert.Equal(t, database.AutonomousDatabaseBackupLifecycleStateActive, got.LifecycleState)
}

func TestRestoreFailsWhenBackupFileIsGone(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	e, l := newEngineEnv(t)
	e.backend.SetDumpDir(dir)

	db := e.mustCreate(t, createOpts{})
	e.settled(t, db.Id)

	l.arch.setDump([]byte("data"), 10)
	bk := e.dataBackup(t, db.Id, "lost")
	require.NoError(t, os.Remove(filepath.Join(dir, *bk.Id+".dmp")))

	resp, err := e.client.RestoreAutonomousDatabase(t.Context(), database.RestoreAutonomousDatabaseRequest{
		AutonomousDatabaseId: db.Id,
		Latest:               new(true),
	})
	require.NoError(t, err)
	e.settled(t, db.Id)

	assert.Equal(t, "FAILED", string(e.wr(t, resp.OpcWorkRequestId).Status))
	assert.Empty(t, l.arch.importsSeen())
}

func TestBackupFilesFollowTheDumpDirectoryOwner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		stop  func(e *env)
		name  string
		keeps bool
	}{
		{name: "close keeps an operator directory", keeps: true, stop: func(e *env) { e.backend.Close() }},
		{name: "reset empties an operator directory", keeps: false, stop: func(e *env) { e.backend.Reset() }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := filepath.Join(t.TempDir(), "dumps")
			e, l := newEngineEnv(t)
			e.backend.SetDumpDir(dir)

			db := e.mustCreate(t, createOpts{})
			e.settled(t, db.Id)

			l.arch.setDump([]byte("data"), 10)
			bk := e.dataBackup(t, db.Id, "kept")
			path := filepath.Join(dir, *bk.Id+".dmp")
			require.FileExists(t, path)

			tt.stop(e)

			if tt.keeps {
				assert.FileExists(t, path)
			} else {
				assert.NoFileExists(t, path)
			}
		})
	}
}

func TestAdminPasswordRejectsControlCharacters(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	_, err := e.create(t, createOpts{mutate: func(d *database.CreateAutonomousDatabaseDetails) {
		d.AdminPassword = new("Passw0rd_ok_12\nschemas=SYS")
	}})
	require.Error(t, err)
}
