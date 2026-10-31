package dmsengine_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/dmsengine"
)

var errBoom = errors.New("boom")

type fakeSource struct {
	rows    map[string][]dmsengine.Row
	changes chan dmsengine.Change
	scanned map[string]int
	startAt dmsengine.StreamStart
	tables  []dmsengine.Table
	mu      sync.Mutex
}

func (s *fakeSource) Ping(context.Context) error                        { return nil }
func (s *fakeSource) Tables(context.Context) ([]dmsengine.Table, error) { return s.tables, nil }
func (s *fakeSource) Position(context.Context) (string, error)          { return "bin.000001:4", nil }
func (s *fakeSource) Release(context.Context) error                     { return nil }
func (s *fakeSource) Close() error                                      { return nil }

func (s *fakeSource) Scan(_ context.Context, t dmsengine.Table, fn func(dmsengine.Row) error) error {
	s.mu.Lock()
	if s.scanned == nil {
		s.scanned = map[string]int{}
	}

	s.scanned[t.Key()]++
	rows := s.rows[t.Key()]
	s.mu.Unlock()

	for _, r := range rows {
		if err := fn(r); err != nil {
			return err
		}
	}

	return nil
}

func (s *fakeSource) Stream(
	ctx context.Context, start dmsengine.StreamStart, tables func(string, string) (dmsengine.Table, bool),
	fn func(dmsengine.Change) error,
) error {
	s.mu.Lock()
	s.startAt = start
	s.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return nil
		case c := <-s.changes:
			if !c.Commit {
				if _, ok := tables(c.Schema, c.Table); !ok {
					continue
				}
			}

			if err := fn(c); err != nil {
				return err
			}
		}
	}
}

type fakeTarget struct {
	prepared map[string]dmsengine.PrepMode
	loaded   map[string][]dmsengine.Row
	failPrep map[string]error
	applied  []dmsengine.Change
	commits  int
	mu       sync.Mutex
}

func newFakeTarget() *fakeTarget {
	return &fakeTarget{
		prepared: map[string]dmsengine.PrepMode{}, loaded: map[string][]dmsengine.Row{}, failPrep: map[string]error{},
	}
}

func (f *fakeTarget) Ping(context.Context) error { return nil }
func (f *fakeTarget) Close() error               { return nil }

func (f *fakeTarget) Prepare(_ context.Context, t dmsengine.TargetTable, m dmsengine.PrepMode) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.failPrep[t.Key()]; err != nil {
		return err
	}

	f.prepared[t.Key()] = m
	f.loaded[t.Key()] = nil

	return nil
}

func (f *fakeTarget) Load(_ context.Context, t dmsengine.TargetTable, rows []dmsengine.Row) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, r := range rows {
		f.loaded[t.Key()] = append(f.loaded[t.Key()], append(dmsengine.Row(nil), r...))
	}

	return nil
}

func (f *fakeTarget) EndLoad(context.Context, dmsengine.TargetTable) error { return nil }

func (f *fakeTarget) Apply(_ context.Context, _ dmsengine.TargetTable, c dmsengine.Change) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.applied = append(f.applied, c)

	return nil
}

func (f *fakeTarget) Commit(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.commits++

	return nil
}

type fakeClock struct {
	t  time.Time
	mu sync.Mutex
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = c.t.Add(time.Second)

	return c.t
}

func twoTables() *fakeSource {
	items := dmsengine.Table{Schema: "app", Name: "items", PK: []string{"id"}, Columns: []dmsengine.Column{
		{Name: "id", Native: "int"}, {Name: "name", Native: "varchar(20)"},
	}}
	orders := dmsengine.Table{Schema: "app", Name: "orders", PK: []string{"id"}, Columns: []dmsengine.Column{
		{Name: "id", Native: "int"},
	}}

	return &fakeSource{
		tables: []dmsengine.Table{items, orders},
		rows: map[string][]dmsengine.Row{
			"app.items":  {{"1", "alpha"}, {"2", "beta"}},
			"app.orders": {{"10"}},
		},
		changes: make(chan dmsengine.Change, 16),
	}
}

const selectAll = `{"rules":[{"rule-type":"selection","rule-id":"1","rule-name":"1",
	"object-locator":{"schema-name":"app","table-name":"%"},"rule-action":"include"}]}`

type progressLog struct {
	ch chan dmsengine.Progress
}

func newProgressLog() *progressLog { return &progressLog{ch: make(chan dmsengine.Progress, 1024)} }

func (p *progressLog) push(pr dmsengine.Progress) {
	select {
	case p.ch <- pr:
	default:
	}
}

// waitFor drains progress until pred holds.
func (p *progressLog) waitFor(t *testing.T, pred func(dmsengine.Progress) bool) dmsengine.Progress {
	t.Helper()

	timeout := time.After(10 * time.Second)

	for {
		select {
		case pr := <-p.ch:
			if pred(pr) {
				return pr
			}
		case <-timeout:
			require.FailNow(t, "progress condition not reached")
		}
	}
}

func stat(pr dmsengine.Progress, key string) dmsengine.TableStat {
	for _, s := range pr.Tables {
		if s.Schema+"."+s.Name == key {
			return s
		}
	}

	return dmsengine.TableStat{}
}

func TestTaskFullLoadOnly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		settings string
		wantMode dmsengine.PrepMode
	}{
		{name: "default drops and creates", settings: "", wantMode: dmsengine.PrepDropAndCreate},
		{
			name:     "truncate before load",
			settings: `{"FullLoadSettings":{"TargetTablePrepMode":"TRUNCATE_BEFORE_LOAD"}}`,
			wantMode: dmsengine.PrepTruncateBefore,
		},
		{
			name:     "do nothing",
			settings: `{"FullLoadSettings":{"TargetTablePrepMode":"DO_NOTHING"}}`,
			wantMode: dmsengine.PrepDoNothing,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src, tgt, clk, log := twoTables(), newFakeTarget(), &fakeClock{
				t: time.Unix(1_700_000_000, 0),
			}, newProgressLog()
			mappings, err := dmsengine.ParseMappings(selectAll)
			require.NoError(t, err)

			settings, err := dmsengine.ParseTaskSettings(tt.settings)
			require.NoError(t, err)

			var last dmsengine.Progress

			task := dmsengine.NewTask(dmsengine.TaskConfig{
				Source: src, Target: tgt, Now: clk.Now, MigrationType: dmsengine.MigrationFullLoad,
				Mappings: mappings, Settings: settings, SourceFamily: dmsengine.FamilyMySQL,
				OnProgress: func(p dmsengine.Progress) { last = p; log.push(p) },
			})

			res := task.Run(t.Context())

			require.NoError(t, res.Err)
			assert.Equal(t, dmsengine.StopFullLoadOnly, res.Reason)
			assert.Equal(t, tt.wantMode, tgt.prepared["app.items"])
			assert.Equal(t, []dmsengine.Row{{"1", "alpha"}, {"2", "beta"}}, tgt.loaded["app.items"])

			items := stat(last, "app.items")
			assert.Equal(t, "Table completed", items.State)
			assert.EqualValues(t, 2, items.FullLoadRows)
			assert.True(t, items.FullLoadEnd.After(items.FullLoadStart))
			assert.EqualValues(t, 1, stat(last, "app.orders").FullLoadRows)
			assert.True(t, last.FullLoadFinish.After(last.FullLoadStart))
		})
	}
}

func TestTaskTransformsReachTarget(t *testing.T) {
	t.Parallel()

	src, tgt := twoTables(), newFakeTarget()
	mappings, err := dmsengine.ParseMappings(`{"rules":[
		{"rule-type":"selection","rule-id":"1","rule-name":"1",
			"object-locator":{"schema-name":"app","table-name":"items"},"rule-action":"include"},
		{"rule-type":"transformation","rule-id":"2","rule-name":"2","rule-target":"table","rule-action":"rename",
			"object-locator":{"schema-name":"app","table-name":"items"},"value":"items_copy"},
		{"rule-type":"transformation","rule-id":"3","rule-name":"3","rule-target":"column","rule-action":"remove-column",
			"object-locator":{"schema-name":"app","table-name":"items","column-name":"name"}}]}`)
	require.NoError(t, err)

	task := dmsengine.NewTask(dmsengine.TaskConfig{
		Source: src, Target: tgt, MigrationType: dmsengine.MigrationFullLoad, Mappings: mappings,
		Settings: defaultSettings(t), SourceFamily: dmsengine.FamilyMySQL,
	})

	require.NoError(t, task.Run(t.Context()).Err)
	assert.Equal(t, []dmsengine.Row{{"1"}, {"2"}}, tgt.loaded["app.items_copy"])
	assert.NotContains(t, tgt.prepared, "app.orders")
}

func defaultSettings(t *testing.T) dmsengine.TaskSettings {
	t.Helper()

	s, err := dmsengine.ParseTaskSettings("")
	require.NoError(t, err)

	return s
}

func TestTaskTableErrorDoesNotStopOthers(t *testing.T) {
	t.Parallel()

	src, tgt, log := twoTables(), newFakeTarget(), newProgressLog()
	tgt.failPrep["app.items"] = errBoom
	mappings, err := dmsengine.ParseMappings(selectAll)
	require.NoError(t, err)

	var last dmsengine.Progress

	task := dmsengine.NewTask(dmsengine.TaskConfig{
		Source: src, Target: tgt, MigrationType: dmsengine.MigrationFullLoad, Mappings: mappings,
		Settings: defaultSettings(t), SourceFamily: dmsengine.FamilyMySQL,
		OnProgress: func(p dmsengine.Progress) { last = p; log.push(p) },
	})

	res := task.Run(t.Context())

	require.NoError(t, res.Err)
	assert.Equal(t, "Table error", stat(last, "app.items").State)
	assert.Contains(t, stat(last, "app.items").Error, "boom")
	assert.Equal(t, "Table completed", stat(last, "app.orders").State)
}

func TestTaskFullLoadAndCDC(t *testing.T) {
	t.Parallel()

	src, tgt, log := twoTables(), newFakeTarget(), newProgressLog()
	mappings, err := dmsengine.ParseMappings(selectAll)
	require.NoError(t, err)

	task := dmsengine.NewTask(dmsengine.TaskConfig{
		Source: src, Target: tgt, MigrationType: dmsengine.MigrationFullLoadAndCDC, Mappings: mappings,
		Settings: defaultSettings(t), SourceFamily: dmsengine.FamilyMySQL, OnProgress: log.push,
	})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan dmsengine.Result, 1)

	go func() { done <- task.Run(ctx) }()

	log.waitFor(t, func(p dmsengine.Progress) bool { return stat(p, "app.orders").State == "Table completed" })

	src.changes <- dmsengine.Change{
		Schema: "app", Table: "items", Op: dmsengine.OpInsert, After: dmsengine.Row{"3", "gamma"},
	}
	src.changes <- dmsengine.Change{
		Schema: "app", Table: "items", Op: dmsengine.OpUpdate,
		Before: dmsengine.Row{"3", "gamma"}, After: dmsengine.Row{"3", "delta"},
	}
	src.changes <- dmsengine.Change{
		Schema: "app", Table: "items", Op: dmsengine.OpDelete, Before: dmsengine.Row{"1", "alpha"},
	}
	src.changes <- dmsengine.Change{Schema: "app", Table: "ignored", Op: dmsengine.OpInsert, After: dmsengine.Row{"1"}}
	src.changes <- dmsengine.Change{Commit: true, Position: "bin.000001:900"}

	final := log.waitFor(t, func(p dmsengine.Progress) bool { return p.Checkpoint == "bin.000001:900" })

	cancel()

	res := <-done

	require.NoError(t, res.Err)
	assert.Equal(t, dmsengine.StopNormal, res.Reason)
	assert.Equal(t, dmsengine.StreamStart{Position: "bin.000001:4"}, src.startAt)

	items := stat(final, "app.items")
	assert.EqualValues(t, 1, items.Inserts)
	assert.EqualValues(t, 1, items.Updates)
	assert.EqualValues(t, 1, items.Deletes)
	assert.EqualValues(t, 2, items.FullLoadRows)
	assert.Len(t, tgt.applied, 3)
	assert.Equal(t, 1, tgt.commits)
}

func TestTaskCDCOnlyDoesNotLoad(t *testing.T) {
	t.Parallel()

	src, tgt, log := twoTables(), newFakeTarget(), newProgressLog()
	mappings, err := dmsengine.ParseMappings(selectAll)
	require.NoError(t, err)

	task := dmsengine.NewTask(dmsengine.TaskConfig{
		Source: src, Target: tgt, MigrationType: dmsengine.MigrationCDC, Mappings: mappings,
		CDCStart: "2024-03-08T12:12:12", Settings: defaultSettings(t), SourceFamily: dmsengine.FamilyMySQL,
		OnProgress: log.push,
	})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan dmsengine.Result, 1)

	go func() { done <- task.Run(ctx) }()

	src.changes <- dmsengine.Change{Schema: "app", Table: "orders", Op: dmsengine.OpInsert, After: dmsengine.Row{"11"}}
	src.changes <- dmsengine.Change{Commit: true, Position: "p"}
	log.waitFor(t, func(p dmsengine.Progress) bool { return stat(p, "app.orders").Inserts == 1 })
	cancel()

	assert.Equal(t, dmsengine.StopNormal, (<-done).Reason)
	assert.Empty(t, src.scanned)
	assert.Empty(t, tgt.prepared)
	assert.Equal(t, time.Date(2024, 3, 8, 12, 12, 12, 0, time.UTC), src.startAt.Time)
}

func TestTaskStopsAtCommitTime(t *testing.T) {
	t.Parallel()

	src, tgt, log := twoTables(), newFakeTarget(), newProgressLog()
	mappings, err := dmsengine.ParseMappings(selectAll)
	require.NoError(t, err)

	task := dmsengine.NewTask(dmsengine.TaskConfig{
		Source: src, Target: tgt, MigrationType: dmsengine.MigrationCDC, Mappings: mappings,
		CDCStop: "commit_time:2024-01-01T00:00:10", Settings: defaultSettings(t),
		SourceFamily: dmsengine.FamilyMySQL, OnProgress: log.push,
	})

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	src.changes <- dmsengine.Change{
		Time: base, Schema: "app", Table: "orders", Op: dmsengine.OpInsert, After: dmsengine.Row{"1"},
	}
	src.changes <- dmsengine.Change{
		Time: base.Add(10 * time.Second), Schema: "app", Table: "orders", Op: dmsengine.OpInsert,
		After: dmsengine.Row{"2"},
	}

	res := task.Run(t.Context())

	require.NoError(t, res.Err)
	assert.Equal(t, dmsengine.StopAtCommitTime, res.Reason)
	assert.Len(t, tgt.applied, 1)
}

func TestTaskReload(t *testing.T) {
	t.Parallel()

	src, tgt, log := twoTables(), newFakeTarget(), newProgressLog()
	mappings, err := dmsengine.ParseMappings(selectAll)
	require.NoError(t, err)

	task := dmsengine.NewTask(dmsengine.TaskConfig{
		Source: src, Target: tgt, MigrationType: dmsengine.MigrationFullLoadAndCDC, Mappings: mappings,
		Settings: defaultSettings(t), SourceFamily: dmsengine.FamilyMySQL, OnProgress: log.push,
	})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan dmsengine.Result, 1)

	go func() { done <- task.Run(ctx) }()

	log.waitFor(t, func(p dmsengine.Progress) bool { return stat(p, "app.items").State == "Table completed" })
	require.ErrorIs(
		t,
		task.Reload(ctx, []dmsengine.TableRef{{Schema: "app", Name: "missing"}}),
		dmsengine.ErrTableNotInTask,
	)
	require.NoError(t, task.Reload(ctx, []dmsengine.TableRef{{Schema: "app", Name: "items"}}))

	got := log.waitFor(t, func(p dmsengine.Progress) bool {
		s := stat(p, "app.items")

		return s.FullLoadReloaded && s.State == "Table completed"
	})
	cancel()
	<-done

	assert.Equal(t, "Table completed", stat(got, "app.items").State)
	assert.Equal(t, 2, src.scanned["app.items"])
	assert.True(t, task.Has(dmsengine.TableRef{Schema: "app", Name: "items"}))
}

func TestTaskSourceFailure(t *testing.T) {
	t.Parallel()

	src := &failingSource{fakeSource: *twoTables()}
	task := dmsengine.NewTask(dmsengine.TaskConfig{
		Source: src, Target: newFakeTarget(), MigrationType: dmsengine.MigrationFullLoad,
		Settings: defaultSettings(t), SourceFamily: dmsengine.FamilyMySQL,
	})

	res := task.Run(t.Context())

	require.Error(t, res.Err)
	assert.Equal(t, dmsengine.StopFatalError, res.Reason)
	assert.Contains(t, res.Err.Error(), "boom")
}

type failingSource struct{ fakeSource }

func (f *failingSource) Ping(context.Context) error { return errBoom }
