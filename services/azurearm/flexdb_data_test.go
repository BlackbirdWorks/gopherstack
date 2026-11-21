package azurearm_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/mysql/armmysqlflexibleservers/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/postgresql/armpostgresqlflexibleservers/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

type flexClock struct {
	t    time.Time
	step time.Duration
	mu   sync.Mutex
}

func newFlexClock() *flexClock { return &flexClock{t: time.Now().UTC().Truncate(time.Second)} }

func (c *flexClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = c.t.Add(c.step)

	return c.t
}

func (c *flexClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// flexKindCase drives the same scenario against both flexible server flavours.
type flexKindCase struct {
	create  func(t *testing.T, s *flexStack, name string) string
	restore func(t *testing.T, s *flexStack, name, srcID string, at time.Time) error
	replica func(t *testing.T, s *flexStack, name, srcID string) error
	remove  func(t *testing.T, s *flexStack, name string) error
	promote func(t *testing.T, s *flexStack, name string) error
	ns      string
	api     string
	name    string
}

func flexKindCases() []flexKindCase {
	return []flexKindCase{
		{
			name: "postgres", ns: pgNS, api: flexAPIPG,
			remove: func(t *testing.T, s *flexStack, name string) error {
				t.Helper()

				p, err := s.pgFactory(t).NewServersClient().BeginDelete(t.Context(), flexRG, name, nil)
				if err != nil {
					return err
				}

				_, err = p.PollUntilDone(t.Context(), pollFast())

				return err
			},
			promote: func(t *testing.T, s *flexStack, name string) error {
				t.Helper()

				p, err := s.pgFactory(t).NewServersClient().BeginUpdate(t.Context(), flexRG, name,
					armpostgresqlflexibleservers.ServerForUpdate{
						Properties: &armpostgresqlflexibleservers.ServerPropertiesForUpdate{
							ReplicationRole: new(armpostgresqlflexibleservers.ReplicationRoleNone),
						},
					}, nil)
				if err != nil {
					return err
				}

				_, err = p.PollUntilDone(t.Context(), pollFast())

				return err
			},
			create: func(t *testing.T, s *flexStack, name string) string {
				t.Helper()

				return *s.pgCreateGP(t, name).ID
			},
			restore: func(t *testing.T, s *flexStack, name, srcID string, at time.Time) error {
				t.Helper()

				return s.pgSeed(t, name, armpostgresqlflexibleservers.ServerProperties{
					CreateMode: new(armpostgresqlflexibleservers.CreateModePointInTimeRestore), PointInTimeUTC: &at,
					SourceServerResourceID: &srcID,
				})
			},
			replica: func(t *testing.T, s *flexStack, name, srcID string) error {
				t.Helper()

				return s.pgSeed(t, name, armpostgresqlflexibleservers.ServerProperties{
					CreateMode:             new(armpostgresqlflexibleservers.CreateModeReplica),
					SourceServerResourceID: &srcID,
				})
			},
		},
		{
			name: "mysql", ns: myNS, api: flexAPIMy,
			remove: func(t *testing.T, s *flexStack, name string) error {
				t.Helper()

				p, err := s.myFactory(t).NewServersClient().BeginDelete(t.Context(), flexRG, name, nil)
				if err != nil {
					return err
				}

				_, err = p.PollUntilDone(t.Context(), pollFast())

				return err
			},
			promote: func(t *testing.T, s *flexStack, name string) error {
				t.Helper()

				p, err := s.myFactory(t).NewServersClient().BeginUpdate(t.Context(), flexRG, name,
					armmysqlflexibleservers.ServerForUpdate{
						Properties: &armmysqlflexibleservers.ServerPropertiesForUpdate{
							ReplicationRole: new(armmysqlflexibleservers.ReplicationRoleNone),
						},
					}, nil)
				if err != nil {
					return err
				}

				_, err = p.PollUntilDone(t.Context(), pollFast())

				return err
			},
			create: func(t *testing.T, s *flexStack, name string) string {
				t.Helper()

				srv := mySrv()
				srv.SKU = &armmysqlflexibleservers.MySQLServerSKU{
					Name: new("Standard_D2ds_v4"), Tier: new(armmysqlflexibleservers.ServerSKUTierGeneralPurpose),
				}
				srv.Properties.Backup.BackupRetentionDays = new(int32(5))

				p, err := s.myFactory(t).NewServersClient().BeginCreate(t.Context(), flexRG, name, srv, nil)
				require.NoError(t, err)

				res, err := p.PollUntilDone(t.Context(), pollFast())
				require.NoError(t, err)

				return *res.ID
			},
			restore: func(t *testing.T, s *flexStack, name, srcID string, at time.Time) error {
				t.Helper()

				return s.mySeed(t, name, armmysqlflexibleservers.ServerProperties{
					CreateMode: new(armmysqlflexibleservers.CreateModePointInTimeRestore), RestorePointInTime: &at,
					SourceServerResourceID: &srcID,
				})
			},
			replica: func(t *testing.T, s *flexStack, name, srcID string) error {
				t.Helper()

				return s.mySeed(t, name, armmysqlflexibleservers.ServerProperties{
					CreateMode:             new(armmysqlflexibleservers.CreateModeReplica),
					SourceServerResourceID: &srcID,
				})
			},
		},
	}
}

func (s *flexStack) pgSeed(t *testing.T, name string, props armpostgresqlflexibleservers.ServerProperties) error {
	t.Helper()

	p, err := s.pgFactory(t).NewServersClient().BeginCreate(t.Context(), flexRG, name,
		armpostgresqlflexibleservers.Server{Location: new(flexLoc), Properties: &props}, nil)
	if err != nil {
		return err
	}

	_, err = p.PollUntilDone(t.Context(), pollFast())

	return err
}

func (s *flexStack) mySeed(t *testing.T, name string, props armmysqlflexibleservers.ServerProperties) error {
	t.Helper()

	p, err := s.myFactory(t).NewServersClient().BeginCreate(t.Context(), flexRG, name,
		armmysqlflexibleservers.Server{Location: new(flexLoc), Properties: &props}, nil)
	if err != nil {
		return err
	}

	_, err = p.PollUntilDone(t.Context(), pollFast())

	return err
}

func flexKey(ns, name string) string { return strings.ToLower(flexServerPath(ns, name)) }

type flexDataStack struct {
	*flexStack
	eng *fakeFlexEngine
	clk *flexClock
	dir string
}

func newFlexDataStack(t *testing.T) *flexDataStack {
	t.Helper()

	eng := newFakeFlexEngine()
	s := newFlexStack(t, eng)
	clk := newFlexClock()
	dir := t.TempDir()

	for _, p := range []interface {
		SetClock(func() time.Time)
		SetDumpDir(string, int64)
	}{s.pg, s.my} {
		p.SetClock(clk.Now)
		p.SetDumpDir(dir, 0)
	}

	return &flexDataStack{flexStack: s, eng: eng, clk: clk, dir: dir}
}

func (d *flexDataStack) cycle(t *testing.T, kind flexKindCase) {
	t.Helper()

	if kind.ns == pgNS {
		d.pg.RunBackupCycle(t.Context())

		return
	}

	d.my.RunBackupCycle(t.Context())
}

func (d *flexDataStack) files(t *testing.T) []string {
	t.Helper()

	entries, err := os.ReadDir(d.dir)
	require.NoError(t, err)

	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}

	return out
}

func TestFlexPointInTimeRestoreUsesNewestBackup(t *testing.T) {
	t.Parallel()

	for _, kind := range flexKindCases() {
		t.Run(kind.name, func(t *testing.T) {
			t.Parallel()

			d := newFlexDataStack(t)
			srcKey := flexKey(kind.ns, "src")
			d.eng.setData(srcKey, "v0")

			srcID := kind.create(t, d.flexStack, "src")
			t0 := d.clk.Now()

			d.eng.setData(srcKey, "v1")
			d.clk.Advance(25 * time.Hour)
			d.cycle(t, kind)

			d.eng.setData(srcKey, "v2")
			d.clk.Advance(25 * time.Hour)
			d.cycle(t, kind)
			d.eng.setData(srcKey, "v3")

			tests := []struct {
				name string
				want string
				at   time.Duration
			}{
				{"first backup", "|v0", time.Hour},
				{"between backups", "|v1", 30 * time.Hour},
				{"exactly a backup", "|v2", 50 * time.Hour},
				{"after the last backup", "|v2", 50*time.Hour + time.Minute},
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					t.Parallel()

					name := "r" + strings.ReplaceAll(tt.name, " ", "")
					require.NoError(t, kind.restore(t, d.flexStack, name, srcID, t0.Add(tt.at)))
					assert.Equal(t, tt.want, d.eng.loadedData(flexKey(kind.ns, name)))
					assert.False(t, d.eng.isReadOnly(flexKey(kind.ns, name)))
					assert.Contains(t, d.eng.log(), "copyadmin")
				})
			}
		})
	}
}

func (d *flexDataStack) backupCount(t *testing.T, kind flexKindCase, name string) int {
	t.Helper()

	status, body := d.raw(t, http.MethodGet, flexServerPath(kind.ns, name)+"/backups", "", kind.api)
	require.Equal(t, http.StatusOK, status)

	vals, _ := body["value"].([]any)

	return len(vals)
}

func TestFlexBackupFilesFollowRetention(t *testing.T) {
	t.Parallel()

	for _, kind := range flexKindCases() {
		t.Run(kind.name, func(t *testing.T) {
			t.Parallel()

			d := newFlexDataStack(t)
			kind.create(t, d.flexStack, "src")
			require.Len(t, d.files(t), 1, "the first backup holds a dump")

			for range 3 {
				d.clk.Advance(25 * time.Hour)
				d.cycle(t, kind)
			}

			assert.Equal(t, 4, d.backupCount(t, kind, "src"))
			assert.Len(t, d.files(t), 4)

			d.clk.Advance(8 * 24 * time.Hour)
			d.cycle(t, kind)

			assert.Equal(t, 1, d.backupCount(t, kind, "src"), "only the new backup is inside the retention window")
			assert.Len(t, d.files(t), 1, "expired dumps are deleted")

			require.NoError(t, kind.remove(t, d.flexStack, "src"))
			assert.Empty(t, d.files(t), "deleting the server deletes its dumps")
		})
	}
}

func TestFlexBackupSizeCapAndTamper(t *testing.T) {
	t.Parallel()

	for _, kind := range flexKindCases() {
		t.Run(kind.name+" too large", func(t *testing.T) {
			t.Parallel()

			d := newFlexDataStack(t)
			srcID := kind.create(t, d.flexStack, "src")
			d.pg.SetDumpDir(d.dir, 8)
			d.my.SetDumpDir(d.dir, 8)
			d.eng.setData(flexKey(kind.ns, "src"), "far more than eight bytes")

			before := d.files(t)

			d.clk.Advance(25 * time.Hour)
			d.cycle(t, kind)

			assert.Equal(t, 1, d.backupCount(t, kind, "src"), "an oversized dump records no backup")
			assert.Equal(t, before, d.files(t), "an oversized dump leaves no file behind")

			require.NoError(t, kind.restore(t, d.flexStack, "rst", srcID, d.clk.Now()),
				"the first backup was written before the cap and is restorable")
			assert.Equal(t, "|", d.eng.loadedData(flexKey(kind.ns, "rst")))
		})

		t.Run(kind.name+" tampered", func(t *testing.T) {
			t.Parallel()

			d := newFlexDataStack(t)
			srcID := kind.create(t, d.flexStack, "src")

			for _, f := range d.files(t) {
				require.NoError(t, os.WriteFile(filepath.Join(d.dir, f), []byte("|evil"), 0o600))
			}

			err := kind.restore(t, d.flexStack, "rst", srcID, d.clk.Now())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "checksum")
			assert.Empty(t, d.eng.loadedData(flexKey(kind.ns, "rst")), "a tampered dump is never loaded")

			_, ok := d.eng.Endpoint(flexKey(kind.ns, "rst"))
			assert.False(t, ok, "no half-loaded container is left")

			status, body := d.raw(t, http.MethodGet, flexServerPath(kind.ns, "rst"), "", kind.api)
			require.Equal(t, http.StatusOK, status)

			props, _ := body["properties"].(map[string]any)
			assert.Equal(t, "Disabled", props["state"])
		})
	}
}

func TestFlexRestoreWithoutDataIsRefused(t *testing.T) {
	t.Parallel()

	for _, kind := range flexKindCases() {
		t.Run(kind.name, func(t *testing.T) {
			t.Parallel()

			d := newFlexDataStack(t)
			d.eng.failDump = true
			srcID := kind.create(t, d.flexStack, "src")

			err := kind.restore(t, d.flexStack, "rst", srcID, d.clk.Now())
			require.Error(t, err)
			assert.Contains(t, err.Error(), "no backup with data")
		})
	}
}

func TestFlexReplicaIsSeededAndReadOnly(t *testing.T) {
	t.Parallel()

	for _, kind := range flexKindCases() {
		t.Run(kind.name, func(t *testing.T) {
			t.Parallel()

			d := newFlexDataStack(t)
			srcKey, repKey := flexKey(kind.ns, "src"), flexKey(kind.ns, "rep")
			srcID := kind.create(t, d.flexStack, "src")
			d.eng.setData(srcKey, "rows")

			require.NoError(t, kind.replica(t, d.flexStack, "rep", srcID))
			assert.Equal(t, "|rows", d.eng.loadedData(repKey), "the replica starts from a fresh dump of the source")
			assert.True(t, d.eng.isReadOnly(repKey), "a replica refuses writes")
			assert.False(t, d.eng.isReadOnly(srcKey))
			assert.Contains(t, d.eng.log(), "copyadmin")

			require.NoError(t, kind.promote(t, d.flexStack, "rep"))
			assert.False(t, d.eng.isReadOnly(repKey), "promotion makes the replica writable")

			status, body := d.raw(t, http.MethodGet, flexServerPath(kind.ns, "rep"), "", kind.api)
			require.Equal(t, http.StatusOK, status)

			props, _ := body["properties"].(map[string]any)
			assert.Equal(t, "None", props["replicationRole"])
		})

		t.Run(kind.name+" deleted source", func(t *testing.T) {
			t.Parallel()

			d := newFlexDataStack(t)
			srcID := kind.create(t, d.flexStack, "src")
			require.NoError(t, kind.replica(t, d.flexStack, "rep", srcID))
			require.True(t, d.eng.isReadOnly(flexKey(kind.ns, "rep")))

			require.NoError(t, kind.remove(t, d.flexStack, "src"))
			assert.False(t, d.eng.isReadOnly(flexKey(kind.ns, "rep")), "deleting the source frees its replica")
		})

		t.Run(kind.name+" failed seed", func(t *testing.T) {
			t.Parallel()

			d := newFlexDataStack(t)
			srcID := kind.create(t, d.flexStack, "src")
			d.eng.failRestore = true

			err := kind.replica(t, d.flexStack, "rep", srcID)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "could not be seeded")

			_, live := d.eng.Endpoint(flexKey(kind.ns, "rep"))
			assert.False(t, live, "the half-loaded container is removed")
			assert.Empty(t, d.eng.loadedData(flexKey(kind.ns, "rep")))
			assert.Len(t, d.files(t), 1, "the seed dump is removed; only the source backup remains")

			status, body := d.raw(t, http.MethodGet, flexServerPath(kind.ns, "rep"), "", kind.api)
			require.Equal(t, http.StatusOK, status)

			props, _ := body["properties"].(map[string]any)
			assert.Equal(t, "Disabled", props["state"])
		})
	}
}

func TestFlexReplicaLimitUnderConcurrency(t *testing.T) {
	t.Parallel()

	for _, kind := range flexKindCases() {
		t.Run(kind.name, func(t *testing.T) {
			t.Parallel()

			d := newFlexDataStack(t)
			srcID := kind.create(t, d.flexStack, "src")
			limit := map[string]int{pgNS: 5, myNS: 10}[kind.ns]
			total := limit + 3

			var (
				wg       sync.WaitGroup
				mu       sync.Mutex
				accepted int
				refused  int
			)

			for i := range total {
				wg.Go(func() {
					status, _ := d.raw(t, http.MethodPut, flexServerPath(kind.ns, "rep"+string(rune('a'+i))),
						`{"location":"x","properties":{"createMode":"Replica","sourceServerResourceId":"`+srcID+`"}}`,
						kind.api)

					mu.Lock()
					defer mu.Unlock()

					switch status {
					case http.StatusAccepted:
						accepted++
					case http.StatusBadRequest:
						refused++
					}
				})
			}

			wg.Wait()

			assert.Equal(t, limit, accepted)
			assert.Equal(t, total-limit, refused)

			d.eng.mu.Lock()
			defer d.eng.mu.Unlock()

			assert.Equal(t, 1, d.eng.maxDump, "dumps of one source never overlap")
		})
	}
}

func TestFlexGateSerialisesPerKey(t *testing.T) {
	t.Parallel()

	g := azurearm.NewTestGate()

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		inside = map[string]int{}
		worst  int
	)

	for i := range 24 {
		key := "a"
		if i%2 == 1 {
			key = "b"
		}

		wg.Go(func() {
			release, err := g.Acquire(t.Context(), key)
			if err != nil {
				t.Errorf("acquire: %v", err)

				return
			}

			mu.Lock()
			inside[key]++
			worst = max(worst, inside[key])
			mu.Unlock()

			runtime.Gosched()

			mu.Lock()
			inside[key]--
			mu.Unlock()

			release()
		})
	}

	wg.Wait()

	assert.Equal(t, 1, worst)
	assert.Zero(t, g.Held(), "released keys leave no entry behind")
}

func TestFlexGateStopsWaitingWithTheContext(t *testing.T) {
	t.Parallel()

	g := azurearm.NewTestGate()
	release, err := g.Acquire(t.Context(), "k")
	require.NoError(t, err)

	defer release()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err = g.Acquire(ctx, "k")
	require.ErrorIs(t, err, context.Canceled)
}

func TestFlexBackupsSurviveSnapshotRestore(t *testing.T) {
	t.Parallel()

	for _, kind := range flexKindCases() {
		t.Run(kind.name, func(t *testing.T) {
			t.Parallel()

			d := newFlexDataStack(t)
			srcKey := flexKey(kind.ns, "src")
			d.eng.setData(srcKey, "v0")
			srcID := kind.create(t, d.flexStack, "src")
			t0 := d.clk.Now()

			snap := d.handler.Snapshot(t.Context())
			assert.NotContains(t, string(snap), flexPassword)

			eng2 := newFakeFlexEngine()
			s2 := newFlexStack(t, eng2)

			for _, p := range []interface {
				SetClock(func() time.Time)
				SetDumpDir(string, int64)
			}{s2.pg, s2.my} {
				p.SetClock(func() time.Time { return t0.Add(2 * time.Hour) })
				p.SetDumpDir(d.dir, 0)
			}

			require.NoError(t, s2.handler.Restore(t.Context(), snap))
			assert.Len(t, d.files(t), 1, "restoring keeps the dumps the snapshot references")

			require.NoError(t, kind.restore(t, s2, "rst", srcID, t0.Add(time.Hour)))
			assert.Equal(t, "|v0", eng2.loadedData(flexKey(kind.ns, "rst")))
		})
	}
}

func TestFlexRestoreSweepsUnreferencedDumps(t *testing.T) {
	t.Parallel()

	const tail = "-0123456789abcdef01234567.dump"

	d := newFlexDataStack(t)
	own := filepath.Join(d.dir, d.pg.DumpPrefix()+tail)
	foreign := filepath.Join(d.dir, "postgres-0badc0de"+tail)
	otherKind := filepath.Join(d.dir, d.my.DumpPrefix()+tail)
	unrelated := filepath.Join(d.dir, "notes.txt")

	for _, f := range []string{own, foreign, otherKind, unrelated} {
		require.NoError(t, os.WriteFile(f, []byte("x"), 0o600))
	}

	require.NoError(t, d.handler.Restore(t.Context(), d.handler.Snapshot(t.Context())))

	assert.NoFileExists(t, own, "an unreferenced dump of this provider is removed")
	assert.NoFileExists(t, otherKind, "the same for the other provider's own dumps")
	assert.FileExists(t, foreign, "a dump of another emulator instance sharing the directory is never touched")
	assert.FileExists(t, unrelated)
}

func TestFlexRestoreRefusesHostileDumpReferences(t *testing.T) {
	t.Parallel()

	for _, kind := range flexKindCases() {
		for _, file := range []string{"../escape.dump", "sub/x.dump", `a\b.dump`, ".", "..", "notes.txt", "x.dump"} {
			t.Run(kind.name+" "+file, func(t *testing.T) {
				t.Parallel()

				d := newFlexDataStack(t)
				srcID := kind.create(t, d.flexStack, "src")
				stored := d.files(t)[0]
				planted := []byte("|planted")
				require.NoError(t, os.WriteFile(filepath.Join(d.dir, "notes.txt"), planted, 0o600))
				require.NoError(t, os.WriteFile(filepath.Join(d.dir, "x.dump"), planted, 0o600))

				sum := sha256.Sum256(planted)
				snap := strings.NewReplacer(
					`"file":"`+stored+`"`, `"file":"`+strings.ReplaceAll(file, `\`, `\\`)+`"`,
				).Replace(string(d.handler.Snapshot(t.Context())))

				snap = regexp.MustCompile(`"sha256":"[0-9a-f]{64}"`).
					ReplaceAllString(snap, `"sha256":"`+hex.EncodeToString(sum[:])+`"`)

				require.NoError(t, d.handler.Restore(t.Context(), []byte(snap)))

				err := kind.restore(t, d.flexStack, "rst", srcID, d.clk.Now())
				require.Error(t, err)
				assert.Empty(
					t,
					d.eng.loadedData(flexKey(kind.ns, "rst")),
					"a file the store did not create is never loaded",
				)
			})
		}
	}
}

func TestFlexTopologyGateHoldsBackSecondDump(t *testing.T) {
	t.Parallel()

	for _, kind := range flexKindCases() {
		t.Run(kind.name, func(t *testing.T) {
			t.Parallel()

			d := newFlexDataStack(t)
			srcID := kind.create(t, d.flexStack, "src")

			waited := make(chan string, 4)
			hold := make(chan struct{})
			entered := make(chan string, 4)
			overlap := make(chan struct{})

			d.pg.SetGateWaitHook(func(key string) { waited <- key })
			d.my.SetGateWaitHook(func(key string) { waited <- key })

			d.eng.mu.Lock()
			d.eng.dumpHold, d.eng.dumpEntered, d.eng.overlap = hold, entered, overlap
			d.eng.mu.Unlock()

			body := `{"location":"x","properties":{"createMode":"Replica","sourceServerResourceId":"` + srcID + `"}}`

			status, _ := d.raw(t, http.MethodPut, flexServerPath(kind.ns, "repa"), body, kind.api)
			require.Equal(t, http.StatusAccepted, status)
			require.Equal(t, flexKey(kind.ns, "src"), <-entered, "the first replica is dumping the source")

			status, _ = d.raw(t, http.MethodPut, flexServerPath(kind.ns, "repb"), body, kind.api)
			require.Equal(t, http.StatusAccepted, status)

			select {
			case key := <-waited:
				assert.Contains(
					t,
					key,
					flexKey(kind.ns, "src"),
					"the second replica waits on the source's topology gate",
				)
			case <-overlap:
				t.Fatal("two dumps of one source ran at once")
			}

			close(hold)
		})
	}
}

func TestFlexSnapshotCarriesTheDumpInstance(t *testing.T) {
	t.Parallel()

	a := newFlexDataStack(t)
	b := newFlexDataStack(t)

	require.NotEqual(t, a.pg.DumpPrefix(), b.pg.DumpPrefix(), "every emulator starts with its own dump namespace")
	require.NoError(t, b.handler.Restore(t.Context(), a.handler.Snapshot(t.Context())))

	assert.Equal(t, a.pg.DumpPrefix(), b.pg.DumpPrefix(), "a restored emulator owns the dumps of the snapshot")
	assert.Equal(t, a.my.DumpPrefix(), b.my.DumpPrefix())
}
