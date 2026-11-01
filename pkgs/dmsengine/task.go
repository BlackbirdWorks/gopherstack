package dmsengine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Migration types.
const (
	MigrationFullLoad       = "full-load"
	MigrationCDC            = "cdc"
	MigrationFullLoadAndCDC = "full-load-and-cdc"
)

// Stop reasons as DescribeReplicationTasks reports them.
const (
	StopNormal          = "Stop Reason NORMAL"
	StopFatalError      = "Stop Reason FATAL_ERROR"
	StopFullLoadOnly    = "Stop Reason FULL_LOAD_ONLY_FINISHED"
	StopAtServerTime    = "Stop Reason STOPPED_AT_SERVER_TIME"
	StopAtCommitTime    = "Stop Reason STOPPED_AT_COMMIT_TIME"
	tableBeforeLoad     = "Before load"
	tableFullLoad       = "Full load"
	tableCompleted      = "Table completed"
	tableError          = "Table error"
	tableBeingReloaded  = "Table is being reloaded"
	loadBatchRows       = 1000
	cdcStopServerPrefix = "server_time:"
	cdcStopCommitPrefix = "commit_time:"
)

// ErrTableNotInTask is returned when a reload names a table the task does not replicate.
var ErrTableNotInTask = errors.New("table is not part of the replication task")

// TableStat is one row of DescribeTableStatistics; names are the source's.
type TableStat struct {
	FullLoadStart     time.Time
	FullLoadEnd       time.Time
	LastUpdate        time.Time
	Schema            string
	Name              string
	State             string
	Error             string
	FullLoadRows      int64
	FullLoadErrorRows int64
	Inserts           int64
	Updates           int64
	Deletes           int64
	FullLoadReloaded  bool
}

// Progress is a point-in-time view of a running task.
type Progress struct {
	FullLoadStart  time.Time
	FullLoadFinish time.Time
	Checkpoint     string
	Tables         []TableStat
	Running        bool
}

// Result is how a run ended: Reason is a StopReason string and Err is set when the task failed.
type Result struct {
	Err    error
	Reason string
}

// TableRef names a source table.
type TableRef struct{ Schema, Name string }

// TaskConfig wires a task to its endpoints.
type TaskConfig struct {
	Source        Source
	Target        Target
	Now           func() time.Time
	OnProgress    func(Progress)
	MigrationType string
	CDCStart      string
	CDCStop       string
	Resume        string
	Settings      TaskSettings
	Mappings      Mappings
	PingBackoff   time.Duration
	SourceFamily  Family
	// TargetFamily gates target-specific rules such as add-before-image-columns; FamilyOther skips the check.
	TargetFamily Family
}

type planned struct {
	filter *TableFilter
	src    Table
	target TargetTable
	stat   TableStat
	active bool
}

// Task runs one replication from start to finish.
type Task struct {
	plans   map[string]*planned
	order   []*planned
	prog    Progress
	cfg     TaskConfig
	applyMu sync.Mutex
	pmu     sync.Mutex
}

// NewTask builds a task; call Run once.
func NewTask(cfg TaskConfig) *Task {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	return &Task{cfg: cfg, plans: map[string]*planned{}}
}

func (t *Task) includesLoad() bool { return t.cfg.MigrationType != MigrationCDC && t.cfg.Resume == "" }

func (t *Task) includesCDC() bool { return t.cfg.MigrationType != MigrationFullLoad }

func (t *Task) notify() {
	if t.cfg.OnProgress == nil {
		return
	}

	t.pmu.Lock()
	p := t.prog
	p.Tables = make([]TableStat, len(t.order))

	for i, pl := range t.order {
		p.Tables[i] = pl.stat
	}
	t.pmu.Unlock()

	t.cfg.OnProgress(p)
}

func (t *Task) update(pl *planned, fn func(*TableStat)) {
	t.pmu.Lock()
	fn(&pl.stat)
	pl.stat.LastUpdate = t.cfg.Now()
	t.pmu.Unlock()
}

func (t *Task) fail(err error) Result { return Result{Reason: StopFatalError, Err: err} }

// Run executes the task until it finishes, fails or ctx is cancelled.
func (t *Task) Run(ctx context.Context) Result {
	if err := t.setup(ctx); err != nil {
		return t.fail(err)
	}

	t.pmu.Lock()
	t.prog.Running = true
	t.pmu.Unlock()
	t.notify()

	var position string

	if t.includesCDC() && t.cfg.Resume == "" && t.cfg.MigrationType == MigrationFullLoadAndCDC {
		p, err := t.cfg.Source.Position(ctx)
		if err != nil {
			return t.fail(fmt.Errorf("capture change position: %w", err))
		}

		position = p
	}

	if t.includesLoad() {
		if err := t.fullLoad(ctx); err != nil {
			return t.finish(ctx, err)
		}
	}

	if !t.includesCDC() {
		return Result{Reason: StopFullLoadOnly}
	}

	return t.finish(ctx, t.cdc(ctx, position))
}

func (t *Task) finish(ctx context.Context, err error) Result {
	var stop *stopError

	switch {
	case errors.As(err, &stop):
		return Result{Reason: stop.reason}
	case ctx.Err() != nil:
		return Result{Reason: StopNormal}
	case err != nil:
		return t.fail(err)
	default:
		return Result{Reason: StopNormal}
	}
}

func (t *Task) setup(ctx context.Context) error {
	if err := pingWithRetry(ctx, t.cfg.Source.Ping, t.cfg.PingBackoff); err != nil {
		return fmt.Errorf("source endpoint: %w", err)
	}

	if err := pingWithRetry(ctx, t.cfg.Target.Ping, t.cfg.PingBackoff); err != nil {
		return fmt.Errorf("target endpoint: %w", err)
	}

	all, err := t.cfg.Source.Tables(ctx)
	if err != nil {
		return fmt.Errorf("read source metadata: %w", err)
	}

	state := tableCompleted
	if t.includesLoad() {
		state = tableBeforeLoad
	}

	selected, err := t.cfg.Mappings.SelectFiltered(all)
	if err != nil {
		return err
	}

	for _, sel := range selected {
		st := sel.Table

		target, perr := t.cfg.Mappings.Plan(st, t.cfg.SourceFamily, t.cfg.Settings.TargetSchema)
		if perr != nil {
			return perr
		}

		if target.HasBeforeImage && !beforeImageTarget(t.cfg.TargetFamily) {
			return fmt.Errorf("%w: add-before-image-columns is supported for Kinesis and S3 targets only", ErrMapping)
		}

		pl := &planned{
			src: st, filter: sel.Filter, target: target, active: true,
			stat: TableStat{Schema: st.Schema, Name: st.Name, State: state},
		}
		t.plans[st.Key()] = pl
		t.order = append(t.order, pl)
	}

	return nil
}

func beforeImageTarget(f Family) bool {
	return f == FamilyOther || f == FamilyS3 || f == FamilyKinesis
}

func (t *Task) fullLoad(ctx context.Context) error {
	t.pmu.Lock()
	t.prog.FullLoadStart = t.cfg.Now()
	t.pmu.Unlock()

	for _, pl := range t.order {
		if err := ctx.Err(); err != nil {
			return err
		}

		t.loadTable(ctx, pl, t.cfg.Settings.PrepMode, false)
	}

	t.pmu.Lock()
	t.prog.FullLoadFinish = t.cfg.Now()
	t.pmu.Unlock()
	t.notify()

	return ctx.Err()
}

func (t *Task) loadTable(ctx context.Context, pl *planned, mode PrepMode, reload bool) {
	err := t.copyTable(ctx, pl, mode, reload)
	if err == nil {
		return
	}

	if ctx.Err() != nil {
		return
	}

	t.update(pl, func(s *TableStat) {
		s.State, s.Error = tableError, err.Error()
	})
	t.pmu.Lock()
	pl.active = false
	t.pmu.Unlock()
	t.notify()
}

func (t *Task) copyTable(ctx context.Context, pl *planned, mode PrepMode, reload bool) error {
	t.update(pl, func(s *TableStat) {
		s.State, s.FullLoadStart, s.FullLoadReloaded, s.FullLoadRows, s.Error = tableFullLoad, t.cfg.Now(), reload, 0, ""
	})
	t.notify()

	if err := t.cfg.Target.Prepare(ctx, pl.target, mode); err != nil {
		return fmt.Errorf("prepare target table %s: %w", pl.target.Key(), err)
	}

	batch := make([]Row, 0, min(t.cfg.Settings.CommitRate, loadBatchRows))
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		if err := t.cfg.Target.Load(ctx, pl.target, batch); err != nil {
			return err
		}

		n := int64(len(batch))
		batch = batch[:0]
		t.update(pl, func(s *TableStat) { s.FullLoadRows += n })
		t.notify()

		return nil
	}

	add := func(r Row) error {
		batch = append(batch, pl.target.ProjectWith(r, ProjectCtx{Full: true, Now: t.cfg.Now()}))
		if len(batch) >= cap(batch) {
			return flush()
		}

		return nil
	}

	err := t.scan(ctx, pl, add)
	if err == nil {
		err = flush()
	}

	if err == nil {
		err = t.cfg.Target.EndLoad(ctx, pl.target)
	}

	if err != nil {
		return err
	}

	t.update(pl, func(s *TableStat) { s.State, s.FullLoadEnd = tableCompleted, t.cfg.Now() })
	t.notify()

	return nil
}

// FilteredScanner is implemented by sources that push a selection filter into the full-load query.
type FilteredScanner interface {
	ScanWhere(ctx context.Context, t Table, f *TableFilter, fn func(Row) error) error
}

func (t *Task) scan(ctx context.Context, pl *planned, fn func(Row) error) error {
	if pl.filter == nil {
		return t.cfg.Source.Scan(ctx, pl.src, fn)
	}

	if fs, ok := t.cfg.Source.(FilteredScanner); ok {
		return fs.ScanWhere(ctx, pl.src, pl.filter, fn)
	}

	return t.cfg.Source.Scan(ctx, pl.src, func(r Row) error {
		if !pl.filter.Match(r) {
			return nil
		}

		return fn(r)
	})
}

// Reload re-copies tables while change capture keeps running.
func (t *Task) Reload(ctx context.Context, refs []TableRef) error {
	t.applyMu.Lock()
	defer t.applyMu.Unlock()

	plans := make([]*planned, 0, len(refs))

	for _, r := range refs {
		pl, ok := t.plans[r.Schema+"."+r.Name]
		if !ok {
			return fmt.Errorf("%w: %s.%s", ErrTableNotInTask, r.Schema, r.Name)
		}

		plans = append(plans, pl)
	}

	for _, pl := range plans {
		t.update(pl, func(s *TableStat) { s.State = tableBeingReloaded })
		t.pmu.Lock()
		pl.active = true
		t.pmu.Unlock()
		t.loadTable(ctx, pl, t.cfg.Settings.PrepMode, true)
	}

	return nil
}

type stopError struct{ reason string }

func (s *stopError) Error() string { return s.reason }

func parseStopTime(spec string) (time.Time, string, bool) {
	var reason string

	switch {
	case strings.HasPrefix(spec, cdcStopServerPrefix):
		spec, reason = strings.TrimPrefix(spec, cdcStopServerPrefix), StopAtServerTime
	case strings.HasPrefix(spec, cdcStopCommitPrefix):
		spec, reason = strings.TrimPrefix(spec, cdcStopCommitPrefix), StopAtCommitTime
	default:
		return time.Time{}, "", false
	}

	ts, ok := ParseCDCTime(spec)

	return ts, reason, ok
}

func cdcTimeLayouts() []string {
	return []string{time.RFC3339Nano, "2006-01-02T15:04:05", "2006-01-02 15:04:05", "2006-01-02T15:04:05Z0700"}
}

// ParseCDCTime reads the timestamp forms CdcStartPosition and CdcStopPosition accept.
func ParseCDCTime(s string) (time.Time, bool) {
	for _, layout := range cdcTimeLayouts() {
		if ts, err := time.ParseInLocation(layout, strings.TrimSpace(s), time.UTC); err == nil {
			return ts.UTC(), true
		}
	}

	return time.Time{}, false
}

func (t *Task) streamStart(captured string) StreamStart {
	switch {
	case t.cfg.Resume != "":
		return StreamStart{Position: t.cfg.Resume}
	case t.cfg.CDCStart != "" && !strings.EqualFold(t.cfg.CDCStart, "now"):
		if ts, ok := ParseCDCTime(t.cfg.CDCStart); ok {
			return StreamStart{Time: ts}
		}

		return StreamStart{Position: strings.TrimPrefix(t.cfg.CDCStart, "checkpoint:")}
	default:
		return StreamStart{Position: captured}
	}
}

func (t *Task) lookup(schema, table string) (Table, bool) {
	pl, ok := t.plans[schema+"."+table]
	if !ok {
		return Table{}, false
	}

	return pl.src, true
}

func (t *Task) cdc(ctx context.Context, captured string) error {
	stopAt, stopReason, hasStop := parseStopTime(t.cfg.CDCStop)

	return t.cfg.Source.Stream(ctx, t.streamStart(captured), t.lookup, func(c Change) error {
		if c.Commit {
			if err := t.cfg.Target.Commit(ctx); err != nil {
				return fmt.Errorf("commit target: %w", err)
			}

			t.pmu.Lock()
			t.prog.Checkpoint = c.Position
			t.pmu.Unlock()
			t.notify()

			return nil
		}

		if hasStop && stopReason == StopAtCommitTime && !c.Time.Before(stopAt) {
			return &stopError{reason: stopReason}
		}

		if hasStop && stopReason == StopAtServerTime && !t.cfg.Now().Before(stopAt) {
			return &stopError{reason: stopReason}
		}

		return t.apply(ctx, c)
	})
}

func (t *Task) apply(ctx context.Context, c Change) error {
	t.applyMu.Lock()
	defer t.applyMu.Unlock()

	t.pmu.Lock()
	pl := t.plans[c.Schema+"."+c.Table]
	active := pl != nil && pl.active
	t.pmu.Unlock()

	if !active {
		return nil
	}

	if bad := rowWidth(pl.src, c); bad != "" {
		t.suspend(pl, bad)

		return nil
	}

	c, ok := filterChange(pl.filter, c)
	if !ok {
		return nil
	}

	pc := ProjectCtx{Op: c.Op, Time: c.Time, Now: t.cfg.Now(), Position: c.Position, Before: c.Before}
	tc := c
	tc.Before, tc.After = pl.target.ProjectWith(c.Before, pc), pl.target.ProjectWith(c.After, pc)

	if err := t.cfg.Target.Apply(ctx, pl.target, tc); err != nil {
		if ctx.Err() != nil {
			return nil
		}

		t.suspend(pl, err.Error())

		return nil
	}

	t.update(pl, func(s *TableStat) {
		switch c.Op {
		case OpInsert:
			s.Inserts++
		case OpUpdate:
			s.Updates++
		case OpDelete:
			s.Deletes++
		}
	})

	return nil
}

// filterChange applies a selection filter to a change: rows entering the filter are inserted, rows leaving it are
// deleted, and rows that never matched are dropped.
func filterChange(f *TableFilter, c Change) (Change, bool) {
	if f == nil {
		return c, true
	}

	switch c.Op {
	case OpInsert:
		return c, f.Match(c.After)
	case OpDelete:
		return c, c.Before == nil || f.Match(c.Before)
	case OpUpdate:
		return filterUpdate(f, c)
	}

	return c, true
}

func filterUpdate(f *TableFilter, c Change) (Change, bool) {
	switch {
	case f.Match(c.After):
		if c.Before != nil && !f.Match(c.Before) {
			c.Op = OpInsert
		}

		return c, true
	case c.Before == nil || f.Match(c.Before):
		c.Op = OpDelete
		if c.Before == nil {
			c.Before = c.After
		}

		c.After = nil

		return c, true
	}

	return c, false
}

func rowWidth(src Table, c Change) string {
	for _, r := range []Row{c.Before, c.After} {
		if r != nil && len(r) != len(src.Columns) {
			return fmt.Sprintf(
				"source table definition changed: row has %d columns, expected %d",
				len(r),
				len(src.Columns),
			)
		}
	}

	return ""
}

func (t *Task) suspend(pl *planned, msg string) {
	t.update(pl, func(s *TableStat) {
		s.State, s.Error = tableError, msg
	})
	t.pmu.Lock()
	pl.active = false
	t.pmu.Unlock()
	t.notify()
}

// Has reports whether the task replicates the table.
func (t *Task) Has(r TableRef) bool {
	_, ok := t.plans[r.Schema+"."+r.Name]

	return ok
}

const (
	pingAttempts = 5
	pingBackoff  = 500 * time.Millisecond
)

// pingWithRetry rides out a database that is still restarting after it reported ready.
func pingWithRetry(ctx context.Context, ping func(context.Context) error, backoff time.Duration) error {
	var err error

	if backoff <= 0 {
		backoff = pingBackoff
	}

	for attempt := range pingAttempts {
		if err = ping(ctx); err == nil {
			return nil
		}

		if attempt == pingAttempts-1 {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}

	return err
}
