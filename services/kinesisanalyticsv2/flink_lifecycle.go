package kinesisanalyticsv2

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const restoreSkip = "SKIP_RESTORE_FROM_SNAPSHOT"

// flinkEnabledFor reports whether app runs on the Flink engine.
func (b *InMemoryBackend) flinkEnabledFor(app *Application) bool {
	return b.flink != nil && isFlinkRuntime(app.RuntimeEnvironment)
}

func (b *InMemoryBackend) saveDirLocked(key string) (string, error) {
	e := b.flink
	if d, ok := e.dirs[key]; ok {
		return d, nil
	}

	if e.persistent() {
		d, err := e.ensureStateDir(key)
		if err != nil {
			return "", err
		}

		e.dirs[key] = d

		return d, nil
	}

	d, err := os.MkdirTemp("", "kda-flink-state-*")
	if err != nil {
		return "", fmt.Errorf("create savepoint dir: %w", err)
	}

	if err = os.Chmod(d, flinkDirMode); err != nil {
		_ = os.RemoveAll(d)

		return "", fmt.Errorf("chmod savepoint dir: %w", err)
	}

	e.dirs[key] = d

	return d, nil
}

func checkpointIntervalMS(app *Application) int64 {
	if app.FlinkConfig == nil || app.FlinkConfig.CheckpointConfigurationDescription == nil {
		return 0
	}

	c := app.FlinkConfig.CheckpointConfigurationDescription
	if c.CheckpointingEnabled != nil && !*c.CheckpointingEnabled {
		return 0
	}

	if c.CheckpointInterval == nil {
		return 0
	}

	return *c.CheckpointInterval
}

func parallelismOf(app *Application) int32 {
	if app.FlinkConfig != nil && app.FlinkConfig.ParallelismConfigurationDescription != nil &&
		app.FlinkConfig.ParallelismConfigurationDescription.Parallelism != nil {
		return max(*app.FlinkConfig.ParallelismConfigurationDescription.Parallelism, 1)
	}

	return 1
}

// prepareFlinkLaunchLocked snapshots everything a launch needs; callers hold b.mu.
func (b *InMemoryBackend) prepareFlinkLaunchLocked(
	region string, app *Application, rc *RunConfigInput,
) (flinkLaunch, error) {
	key := applicationKey(region, app.ApplicationName)

	dir, err := b.saveDirLocked(key)
	if err != nil {
		return flinkLaunch{}, err
	}

	in := flinkLaunch{
		key:          key,
		region:       region,
		name:         app.ApplicationName,
		image:        b.flink.cfg.Image,
		saveDir:      dir,
		roleARN:      app.ServiceExecutionRole,
		issuer:       b.roleIssuer,
		props:        slices.Clone(app.EnvironmentPropertyGroups),
		checkpointMS: checkpointIntervalMS(app),
		parallelism:  parallelismOf(app),
	}

	if in.image == "" {
		in.image = flinkImage(app.RuntimeEnvironment)
	}

	if app.CodeConfig != nil && app.CodeConfig.CodeContentDescription != nil {
		in.code = *app.CodeConfig.CodeContentDescription
	}

	restore, allow := effectiveRestore(app, rc)
	in.allowNonRest = allow

	if in.savepointPath, err = b.restorePathLocked(region, app, restore); err != nil {
		return flinkLaunch{}, err
	}

	return in, nil
}

func effectiveRestore(app *Application, rc *RunConfigInput) (*ApplicationRestoreConfig, bool) {
	var restore *ApplicationRestoreConfig

	allow := false

	if app.RunConfig != nil {
		restore = app.RunConfig.ApplicationRestoreConfigurationDescription

		if f := app.RunConfig.FlinkRunConfigurationDescription; f != nil && f.AllowNonRestoredState != nil {
			allow = *f.AllowNonRestoredState
		}
	}

	if rc != nil {
		if rc.ApplicationRestoreConfiguration != nil {
			restore = rc.ApplicationRestoreConfiguration
		}

		if rc.FlinkRunConfiguration != nil && rc.FlinkRunConfiguration.AllowNonRestoredState != nil {
			allow = *rc.FlinkRunConfiguration.AllowNonRestoredState
		}
	}

	return restore, allow
}

// restorePathLocked resolves the savepoint a start restores from; the default is the latest snapshot.
func (b *InMemoryBackend) restorePathLocked(
	region string, app *Application, restore *ApplicationRestoreConfig,
) (string, error) {
	kind := restoreLatest
	if restore != nil && restore.ApplicationRestoreType != "" {
		kind = restore.ApplicationRestoreType
	}

	switch kind {
	case restoreSkip:
		return "", nil
	case restoreCustom:
		return b.customSavepointLocked(region, app.ApplicationName, restore)
	}

	return b.latestSavepointLocked(region, app.ApplicationName), nil
}

// savepointUsableLocked reports whether a persisted savepoint still exists on disk; temp-dir mode trusts the path.
func (b *InMemoryBackend) savepointUsableLocked(region, name, loc string) bool {
	e := b.flink
	if !e.persistent() {
		return true
	}

	_, err := os.Stat(savepointHostPath(e.stateDir(applicationKey(region, name)), loc))

	return err == nil
}

func (b *InMemoryBackend) customSavepointLocked(
	region, name string, restore *ApplicationRestoreConfig,
) (string, error) {
	if restore == nil {
		return "", ErrValidation
	}

	s, ok := b.snapshots.Get(snapshotKey(region, name, restore.SnapshotName))
	if !ok {
		return "", ErrNotFound
	}

	if s.savepointPath == "" {
		return "", ErrValidation
	}

	return s.savepointPath, nil
}

func (b *InMemoryBackend) latestSavepointLocked(region, name string) string {
	var best *Snapshot

	for _, s := range b.snapshotsByApp.Get(appParentKey(region, name)) {
		if s.savepointPath != "" && s.SnapshotStatus == snapshotReady &&
			b.savepointUsableLocked(region, name, s.savepointPath) &&
			(best == nil || s.SnapshotCreation.After(best.SnapshotCreation)) {
			best = s
		}
	}

	stop, hasStop := b.flinkStopSaves[applicationKey(region, name)]

	hasStop = hasStop && b.savepointUsableLocked(region, name, stop.path)

	switch {
	case hasStop && (best == nil || stop.at.After(best.SnapshotCreation)):
		return stop.path
	case best != nil:
		return best.savepointPath
	}

	return ""
}

// stopFlinkLocked moves app to STOPPING and ends its job in the background; callers hold b.mu.
func (b *InMemoryBackend) stopFlinkLocked(lj *flinkJob, region string, app *Application, force bool) string {
	e := b.flink
	opID := b.recordOperation(region, app.ApplicationName, "StopApplication")
	b.setOperationStatusLocked(region, app.ApplicationName, opID, operationInProgress)

	app.ApplicationStatus = ApplicationStatusStopping
	app.transitionalStatus = ""

	withSavepoint := !force && lj.jobID != "" && (app.SnapshotsEnabled == nil || *app.SnapshotsEnabled)
	key := applicationKey(region, app.ApplicationName)
	name := app.ApplicationName

	e.wg.Go(func() {
		if withSavepoint {
			b.stopWithSavepoint(e, lj, key)
		} else {
			b.cancelFlinkJob(e, lj)
		}

		lj.cancel()
		<-lj.done
		b.finishStop(e, lj, key, region, name, opID)
	})

	return opID
}

func (b *InMemoryBackend) jobAddr(lj *flinkJob) (string, string) {
	b.mu.RLock("jobAddr")
	defer b.mu.RUnlock()

	return lj.base, lj.jobID
}

func (b *InMemoryBackend) cancelFlinkJob(e *flinkEngine, lj *flinkJob) {
	base, jobID := b.jobAddr(lj)
	if base == "" || jobID == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), e.cfg.StopTimeout)
	defer cancel()

	if err := (flinkREST{doer: e.cfg.HTTP, base: base}).cancelJob(ctx, jobID); err != nil {
		logger.Load(ctx).WarnContext(ctx, "kinesisanalyticsv2: flink job cancel failed", "error", err)
	}
}

// stopWithSavepoint ends the job with a savepoint that RESTORE_FROM_LATEST_SNAPSHOT later picks up.
// AWS does not document the auto-snapshot's name, so it is kept internal rather than listed.
func (b *InMemoryBackend) stopWithSavepoint(e *flinkEngine, lj *flinkJob, key string) {
	base, jobID := b.jobAddr(lj)
	ctx, cancel := context.WithTimeout(context.Background(), e.cfg.StopTimeout)
	defer cancel()

	loc, err := e.savepoint(ctx, flinkREST{doer: e.cfg.HTTP, base: base}, jobID, true)

	if err != nil {
		logger.Load(ctx).
			WarnContext(ctx, "kinesisanalyticsv2: stop-with-savepoint failed", "application", key, "error", err)

		return
	}

	b.mu.Lock("stopWithSavepoint")
	defer b.mu.Unlock()

	b.flinkStopSaves[key] = flinkStopSave{path: loc, at: time.Now().UTC()}
}

// savepoint triggers a savepoint (or stop-with-savepoint) and waits for its location.
func (e *flinkEngine) savepoint(ctx context.Context, rest flinkREST, jobID string, stop bool) (string, error) {
	trigger, err := rest.triggerSavepoint(ctx, jobID, "file://"+flinkSavepointsDir, stop)
	if err != nil {
		return "", err
	}

	var loc string

	err = pollUntil(ctx, e.cfg.PollInterval, func(c context.Context) (bool, error) {
		st, serr := rest.savepointStatus(c, jobID, trigger)
		if serr != nil {
			return false, serr
		}

		if st.Operation.FailureCause != nil {
			return false, fmt.Errorf("%w: %v", errFlinkSavepoint, st.Operation.FailureCause)
		}

		loc = st.Operation.Location

		return st.Status.ID == "COMPLETED", nil
	})

	return loc, err
}

func (b *InMemoryBackend) finishStop(e *flinkEngine, lj *flinkJob, key, region, name, opID string) {
	b.mu.Lock("finishStop")
	defer b.mu.Unlock()

	if cur, ok := e.jobs[key]; ok && cur == lj {
		delete(e.jobs, key)
	}

	if app, ok := b.findApplication(region, name); ok && app.ApplicationStatus == ApplicationStatusStopping {
		app.ApplicationStatus = ApplicationStatusReady
	}

	b.setOperationStatusLocked(region, name, opID, OperationStatusSuccessful)
}

// deleteFlinkLocked detaches the app's job and state; the cluster is torn down in the background.
func (b *InMemoryBackend) deleteFlinkLocked(key string) {
	e := b.flink
	if e == nil {
		return
	}

	lj, hasJob := e.jobs[key]
	dir := e.dirs[key]

	if e.persistent() {
		dir = e.stateDir(key)
		e.removeCode(key)
	}

	delete(e.jobs, key)
	delete(e.dirs, key)
	delete(b.flinkStopSaves, key)

	if !hasJob {
		if dir != "" {
			removeAllLogged(dir)
		}

		return
	}

	lj.cancel()

	e.wg.Go(func() {
		<-lj.done

		if dir != "" {
			removeAllLogged(dir)
		}
	})
}

// startFlinkSnapshotLocked begins a real savepoint for snap; callers hold b.mu.
func (b *InMemoryBackend) startFlinkSnapshotLocked(lj *flinkJob, snap *Snapshot) {
	e := b.flink
	snap.SnapshotStatus = snapshotCreating
	base, jobID := lj.base, lj.jobID
	key := snapshotKey(snap.Region, snap.AppName, snap.SnapshotName)

	e.wg.Go(func() {
		ctx, cancel := context.WithTimeout(lj.ctx, e.cfg.StopTimeout)
		defer cancel()

		loc, err := e.savepoint(ctx, flinkREST{doer: e.cfg.HTTP, base: base}, jobID, false)

		b.mu.Lock("completeSnapshot")
		defer b.mu.Unlock()

		cur, ok := b.snapshots.Get(key)
		if !ok {
			return
		}

		if err != nil {
			cur.SnapshotStatus = snapshotFailed

			return
		}

		cur.SnapshotStatus, cur.savepointPath = snapshotReady, loc
	})
}

// savepointHostPath maps a container savepoint location to its path under the host state dir.
func savepointHostPath(dir, loc string) string {
	rel, found := strings.CutPrefix(path.Clean(strings.TrimPrefix(loc, "file:")), flinkSavepointsDir+"/")
	if !found || rel == "" || strings.Contains(rel, "..") {
		return ""
	}

	return filepath.Join(dir, rel)
}

// removeSavepointLocked deletes a snapshot's savepoint files from the app's state dir.
func (b *InMemoryBackend) removeSavepointLocked(key string, s *Snapshot) {
	dir, ok := b.flink.dirs[key]
	if !ok && b.flink.persistent() {
		dir, ok = b.flink.stateDir(key), true
	}

	if !ok || s.savepointPath == "" {
		return
	}

	if host := savepointHostPath(dir, s.savepointPath); host != "" {
		removeAllLogged(host)
	}
}

// reconcileFlinkLocked aligns restored applications with the engine: containers do not survive a restart.
func (b *InMemoryBackend) reconcileFlinkLocked() {
	for _, s := range b.snapshots.All() {
		if s.SnapshotStatus == snapshotCreating {
			s.SnapshotStatus = snapshotFailed
		}
	}

	for _, app := range b.applications.All() {
		if !isFlinkRuntime(app.RuntimeEnvironment) {
			continue
		}

		switch app.ApplicationStatus {
		case ApplicationStatusStopping:
			app.ApplicationStatus = ApplicationStatusReady
		case ApplicationStatusStarting, ApplicationStatusRunning:
			b.relaunchFlinkLocked(app)
		}
	}
}

func (b *InMemoryBackend) relaunchFlinkLocked(app *Application) {
	if b.flink == nil {
		if app.ApplicationStatus == ApplicationStatusStarting {
			app.ApplicationStatus = ApplicationStatusReady
		}

		return
	}

	key := applicationKey(app.Region, app.ApplicationName)
	if _, ok := b.flink.jobs[key]; ok {
		return
	}

	latest := &RunConfigInput{ApplicationRestoreConfiguration: &ApplicationRestoreConfig{
		ApplicationRestoreType: restoreLatest,
	}}

	in, err := b.prepareFlinkLaunchLocked(app.Region, app, latest)
	if err != nil {
		app.ApplicationStatus = ApplicationStatusReady

		return
	}

	app.ApplicationStatus = ApplicationStatusStarting
	b.launchFlinkLocked(in, "")
}
