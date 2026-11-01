package dms

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/dmsengine"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	statusStopping = "stopping"
	statusFailed   = "failed"

	eventCategoryFailure = "failure"
	connectionTestLimit  = 15 * time.Second
	slotReleaseLimit     = 15 * time.Second
	startTypeResume      = "resume-processing"
)

// EngineConfig turns on real data movement for replication tasks. Without it tasks only change state.
type EngineConfig struct {
	Opener dmsengine.Opener
	// Now is the clock for task statistics; nil uses time.Now.
	Now func() time.Time
}

type engineState struct {
	opener dmsengine.Opener
	ctx    context.Context //nolint:containedctx // lifetime of every task goroutine
	cancel context.CancelFunc
	runs   map[string]*taskRun
	wg     sync.WaitGroup
	// ownOpener marks an opener EnableEngine built, which SetStores may replace.
	ownOpener bool
}

type taskRun struct {
	task   *dmsengine.Task
	cancel context.CancelFunc
	done   chan struct{}
}

type runParams struct {
	taskARN      string
	region       string
	slot         string
	migration    string
	mappings     string
	settings     string
	cdcStart     string
	cdcStop      string
	resume       string
	sourceEngine string
	source       dmsengine.EndpointConfig
	target       dmsengine.EndpointConfig
}

// EnableEngine switches the backend to real replication; call before serving requests.
func (b *InMemoryBackend) EnableEngine(cfg EngineConfig) {
	b.mu.Lock("EnableEngine")
	defer b.mu.Unlock()

	if cfg.Now != nil {
		b.now = cfg.Now
	}

	ctx, cancel := context.WithCancel(context.Background())
	b.eng = &engineState{opener: cfg.Opener, ctx: ctx, cancel: cancel, runs: map[string]*taskRun{}}

	if cfg.Opener == nil {
		b.eng.opener, b.eng.ownOpener = dmsengine.NewOpener(dmsengine.Stores{}), true
	}
}

// SetStores gives the default opener the in-process S3, Kinesis and DynamoDB targets can write to.
func (b *InMemoryBackend) SetStores(stores dmsengine.Stores) {
	b.mu.Lock("SetStores")
	defer b.mu.Unlock()

	if b.eng != nil && b.eng.ownOpener {
		b.eng.opener = dmsengine.NewOpener(stores)
	}
}

// Close stops every running replication task and waits for its goroutine.
func (b *InMemoryBackend) Close() {
	b.mu.Lock("Close")
	eng := b.eng
	b.mu.Unlock()

	if eng == nil {
		return
	}

	eng.cancel()
	eng.wg.Wait()
}

// Shutdown implements service.Shutdowner.
func (h *Handler) Shutdown(context.Context) { h.Backend.Close() }

func (b *InMemoryBackend) clock() time.Time {
	if b.now != nil {
		return b.now().UTC()
	}

	return time.Now().UTC()
}

// endpointConfigLocked is endpointConfig plus the PEM of the endpoint's certificate; the caller holds b.mu.
func (b *InMemoryBackend) endpointConfigLocked(region string, ep *Endpoint) dmsengine.EndpointConfig {
	cfg := endpointConfig(ep)

	if ep.CertificateArn != "" {
		if cert, ok := lookupUnique(b.certificatesByARN, regionKey(region, ep.CertificateArn)); ok {
			cfg.CertificatePEM = cert.CertificatePem
		}
	}

	return cfg
}

func endpointConfig(ep *Endpoint) dmsengine.EndpointConfig {
	cfg := dmsengine.EndpointConfig{
		Engine: ep.EngineName, Server: ep.ServerName, Port: int(ep.Port), Database: ep.DatabaseName,
		User: ep.Username, Password: ep.Password, SSLMode: ep.SslMode, S3Settings: ep.S3Settings,
		KinesisSettings:  ep.EngineSettings["KinesisSettings"],
		DynamoDBSettings: ep.EngineSettings["DynamoDbSettings"],
	}

	if cfg.Server != "" {
		return cfg
	}

	for _, key := range []string{"MySQLSettings", "PostgreSQLSettings"} {
		raw := ep.EngineSettings[key]
		if raw == "" {
			continue
		}

		var s struct {
			ServerName   string `json:"ServerName"`
			DatabaseName string `json:"DatabaseName"`
			Username     string `json:"Username"`
			Password     string `json:"Password"`
			Port         int    `json:"Port"`
		}

		if json.Unmarshal([]byte(raw), &s, jsonx.V1()) == nil && s.ServerName != "" {
			cfg.Server, cfg.Port, cfg.Database = s.ServerName, s.Port, s.DatabaseName
			cfg.User, cfg.Password = s.Username, s.Password

			break
		}
	}

	return cfg
}

func slotName(taskARN string) string {
	sum := sha256.Sum256([]byte(taskARN))

	return "dms_" + hex.EncodeToString(sum[:])[:16]
}

// engineEligible reports whether both endpoints are ones the engine can move data between.
func (b *InMemoryBackend) engineEligible(src, tgt *Endpoint) bool {
	return b.eng != nil && b.eng.opener != nil &&
		dmsengine.SupportedSource(src.EngineName) && dmsengine.SupportedTarget(tgt.EngineName)
}

func (b *InMemoryBackend) taskEndpoints(region string, rt *ReplicationTask) (*Endpoint, *Endpoint, bool) {
	src, ok1 := lookupUnique(b.endpointsByARN, regionKey(region, rt.SourceEndpointArn))
	tgt, ok2 := lookupUnique(b.endpointsByARN, regionKey(region, rt.TargetEndpointArn))

	return src, tgt, ok1 && ok2
}

// launchLocked starts the engine goroutine for rt; the caller holds b.mu and has set the status to starting.
func (b *InMemoryBackend) launchLocked(region string, rt *ReplicationTask, src, tgt *Endpoint, startType string) {
	p := runParams{
		taskARN:   rt.ReplicationTaskArn,
		region:    region,
		slot:      slotName(rt.ReplicationTaskArn),
		migration: rt.MigrationType,
		mappings:  rt.TableMappings,
		settings:  rt.ReplicationTaskSettings,
		cdcStart:  rt.CdcStartPosition,
		cdcStop:   rt.CdcStopPosition,
		source: b.endpointConfigLocked(
			region,
			src,
		),
		target:       b.endpointConfigLocked(region, tgt),
		sourceEngine: src.EngineName,
	}
	p.source.SlotName = p.slot

	if startType == startTypeResume {
		p.resume = rt.RecoveryCheckpoint
	}

	ctx, cancel := context.WithCancel(b.eng.ctx)
	run := &taskRun{cancel: cancel, done: make(chan struct{})}
	b.eng.runs[p.taskARN] = run
	b.eng.wg.Go(func() {
		defer close(run.done)
		defer cancel()

		b.execute(ctx, run, p)
	})
}

func (b *InMemoryBackend) execute(ctx context.Context, run *taskRun, p runParams) {
	res := b.runTask(ctx, run, p)
	b.finishRun(ctx, run, p, res)
}

func (b *InMemoryBackend) runTask(ctx context.Context, run *taskRun, p runParams) dmsengine.Result {
	fail := func(err error) dmsengine.Result {
		return dmsengine.Result{Reason: dmsengine.StopFatalError, Err: err}
	}

	mappings, err := dmsengine.ParseMappings(p.mappings)
	if err != nil {
		return fail(err)
	}

	settings, err := dmsengine.ParseTaskSettings(p.settings)
	if err != nil {
		return fail(err)
	}

	src, err := b.eng.opener.OpenSource(ctx, p.source)
	if err != nil {
		return fail(fmt.Errorf("source endpoint: %w", err))
	}
	defer src.Close()

	tgt, err := b.eng.opener.OpenTarget(ctx, p.target, dmsengine.FamilyOf(p.sourceEngine))
	if err != nil {
		return fail(fmt.Errorf("target endpoint: %w", err))
	}
	defer tgt.Close()

	task := dmsengine.NewTask(dmsengine.TaskConfig{
		Source: src, Target: tgt, Now: b.clock, MigrationType: p.migration,
		CDCStart: p.cdcStart, CDCStop: p.cdcStop, Resume: p.resume, Mappings: mappings, Settings: settings,
		SourceFamily: dmsengine.FamilyOf(p.sourceEngine),
		OnProgress:   func(pr dmsengine.Progress) { b.recordProgress(run, p, pr) },
	})

	b.mu.Lock("taskReady")
	run.task = task
	b.mu.Unlock()

	return task.Run(ctx)
}

func statsFrom(pr dmsengine.Progress) []TableStatistic {
	out := make([]TableStatistic, len(pr.Tables))
	for i, t := range pr.Tables {
		out[i] = TableStatistic{
			SchemaName: t.Schema, TableName: t.Name, TableState: t.State, FullLoadRows: t.FullLoadRows,
			FullLoadErrorRows: t.FullLoadErrorRows, Inserts: t.Inserts, Updates: t.Updates, Deletes: t.Deletes,
			FullLoadStartTime: t.FullLoadStart, FullLoadEndTime: t.FullLoadEnd, LastUpdateTime: t.LastUpdate,
			FullLoadReloaded: t.FullLoadReloaded,
		}
	}

	return out
}

func (b *InMemoryBackend) recordProgress(run *taskRun, p runParams, pr dmsengine.Progress) {
	b.mu.Lock("dmsProgress")
	defer b.mu.Unlock()

	if b.eng.runs[p.taskARN] != run {
		return
	}

	rt, ok := lookupUnique(b.replicationTasksByARN, regionKey(p.region, p.taskARN))
	if !ok {
		return
	}

	if pr.Running && rt.Status == statusStarting {
		rt.Status = statusRunning
	}

	rt.TableStats = statsFrom(pr)
	rt.FullLoadStartDate, rt.FullLoadFinishDate = pr.FullLoadStart, pr.FullLoadFinish

	if pr.Checkpoint != "" {
		rt.RecoveryCheckpoint = pr.Checkpoint
	}
}

func (b *InMemoryBackend) finishRun(ctx context.Context, run *taskRun, p runParams, res dmsengine.Result) {
	b.mu.Lock("dmsFinish")
	defer b.mu.Unlock()

	if b.eng.runs[p.taskARN] != run {
		return
	}

	delete(b.eng.runs, p.taskARN)

	rt, ok := lookupUnique(b.replicationTasksByARN, regionKey(p.region, p.taskARN))
	if !ok {
		return
	}

	rt.StopDate, rt.StopReason = b.clock(), res.Reason

	if res.Err != nil {
		rt.Status, rt.LastFailureMessage = statusFailed, res.Err.Error()
		logger.Load(ctx).WarnContext(ctx, "dms: replication task failed", "task", rt.ReplicationTaskIdentifier,
			"error", res.Err)
		b.appendEvent(p.region, rt.ReplicationTaskArn, "replication-task",
			"Replication task "+rt.ReplicationTaskIdentifier+" failed", []string{eventCategoryFailure})

		return
	}

	rt.Status = statusStopped
	b.appendEvent(p.region, rt.ReplicationTaskArn, "replication-task",
		"Replication task "+rt.ReplicationTaskIdentifier+" stopped", []string{eventCategoryStateChange})
}

// beginStartLocked resets run state for a fresh start.
func (b *InMemoryBackend) beginStartLocked(rt *ReplicationTask, startType string) {
	rt.Status = statusStarting
	rt.StartDate, rt.StopDate = b.clock(), time.Time{}
	rt.StopReason, rt.LastFailureMessage = "", ""

	if startType != startTypeResume {
		rt.TableStats, rt.RecoveryCheckpoint = nil, ""
		rt.FullLoadStartDate, rt.FullLoadFinishDate = time.Time{}, time.Time{}
	}
}

// stopRunLocked cancels the engine run behind rt, reporting whether one existed.
func (b *InMemoryBackend) stopRunLocked(rt *ReplicationTask) bool {
	if b.eng == nil {
		return false
	}

	run, ok := b.eng.runs[rt.ReplicationTaskArn]
	if !ok {
		return false
	}

	run.cancel()

	return true
}

func (b *InMemoryBackend) hasRunLocked(arn string) bool {
	if b.eng == nil {
		return false
	}

	_, ok := b.eng.runs[arn]

	return ok
}

// viewTask copies rt for a response and derives ElapsedTimeMillis.
func (b *InMemoryBackend) viewTask(rt *ReplicationTask) *ReplicationTask {
	cp := *rt

	switch {
	case rt.StartDate.IsZero():
	case rt.StopDate.IsZero():
		cp.ElapsedTimeMillis = b.clock().Sub(rt.StartDate).Milliseconds()
	default:
		cp.ElapsedTimeMillis = rt.StopDate.Sub(rt.StartDate).Milliseconds()
	}

	return &cp
}

// releaseSlotAsync drops a PostgreSQL slot the deleted task left on its source.
func (b *InMemoryBackend) releaseSlotAsync(src *Endpoint, taskARN string) {
	if b.eng == nil || b.eng.opener == nil || dmsengine.FamilyOf(src.EngineName) != dmsengine.FamilyPostgres {
		return
	}

	cfg := b.endpointConfigLocked(src.Region, src)
	cfg.SlotName = slotName(taskARN)

	b.eng.wg.Go(func() {
		ctx, cancel := context.WithTimeout(b.eng.ctx, slotReleaseLimit)
		defer cancel()

		_ = b.eng.opener.ReleaseSlot(ctx, cfg)
	})
}

// reloadRun re-copies tables on a live engine run. The caller must not hold b.mu.
func (b *InMemoryBackend) reloadRun(run *taskRun, task *dmsengine.Task, refs []dmsengine.TableRef) {
	b.eng.wg.Go(func() {
		ctx, cancel := context.WithCancel(b.eng.ctx)
		defer cancel()

		go func() {
			select {
			case <-run.done:
				cancel()
			case <-ctx.Done():
			}
		}()

		if err := task.Reload(ctx, refs); err != nil && !errors.Is(err, context.Canceled) {
			logger.Load(ctx).WarnContext(ctx, "dms: reload tables failed", "error", err)
		}
	})
}

func (e *engineState) runsFor(arn string) (*taskRun, bool) {
	if e == nil {
		return nil, false
	}

	r, ok := e.runs[arn]

	return r, ok
}
