package codebuild

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	codeClientError     = "CLIENT_ERROR"
	buildStatusTimedOut = "TIMED_OUT"
	onFailureAbort      = "ABORT"
	logTimeLayout       = "2006/01/02 15:04:05.000000"
	srcDirModulus       = 1_000_000_000
)

// outcome is the result of a phase; an empty status means success.
type outcome struct {
	status string
	code   string
	msg    string
}

func (o outcome) ok() bool { return o.status == "" }

// buildRunner executes one build in a container.
type buildRunner struct {
	svc           EngineServices
	runCtx        context.Context
	ctx           context.Context
	rt            EngineRuntime
	spec          *specFile
	log           *buildLogger
	watcherDone   chan struct{}
	stopWatch     chan struct{}
	b             *InMemoryBackend
	workDir       string
	containerID   string
	region        string
	cacheStage    string
	srcDir        string
	cfg           EngineConfig
	masked        []string
	build         Build
	succeeding    bool
	reachedUpload bool
}

type buildLogger struct {
	sink    LogSink
	region  string
	group   string
	stream  string
	masked  []string
	buf     bytes.Buffer
	enabled bool
}

func (l *buildLogger) write(ctx context.Context, text string) {
	if text == "" {
		return
	}

	for _, m := range l.masked {
		if m != "" {
			text = strings.ReplaceAll(text, m, "****")
		}
	}

	l.buf.WriteString(text)

	if !l.enabled || l.sink == nil {
		return
	}

	lines := strings.Split(strings.TrimRight(text, "\n"), "\n")
	_ = l.sink.PutLogLines(context.WithoutCancel(ctx), l.region, l.group, l.stream, lines)
}

func marker(format string, args ...any) string {
	return "[Container] " + time.Now().UTC().Format(logTimeLayout) + " " + fmt.Sprintf(format, args...) + "\n"
}

// launchBuildLocked starts the build goroutine. The caller holds b.mu.
func (b *InMemoryBackend) launchBuildLocked(build *Build) {
	eng := b.engine
	runCtx, cancel := context.WithCancel(context.Background())
	b.runs[build.ID] = &buildRun{cancel: cancel}

	snap := *build

	eng.wg.Go(func() {
		defer cancel()

		b.executeBuild(runCtx, eng, snap)
	})
}

// WaitBuilds blocks until every executing build goroutine has exited.
func (b *InMemoryBackend) WaitBuilds() {
	b.mu.RLock("WaitBuilds")
	eng := b.engine
	b.mu.RUnlock()

	if eng != nil {
		eng.wg.Wait()
	}
}

func (b *InMemoryBackend) executeBuild(runCtx context.Context, eng *engineState, snap Build) {
	r := &buildRunner{
		b: b, rt: eng.cfg.Runtime, cfg: eng.cfg, svc: eng.svc(), build: snap, region: b.region,
		runCtx: runCtx, succeeding: true,
	}

	minutes := int64(snap.TimeoutInMinutes)
	if minutes <= 0 {
		minutes = defaultBuildTimeout
	}

	ctx, cancel := context.WithTimeout(runCtx, time.Duration(minutes*int64(eng.cfg.TimeoutUnit)))
	defer cancel()

	r.ctx = ctx

	status := r.run()
	b.completeBuild(snap.ID, status)
}

func (r *buildRunner) run() string {
	defer r.cleanup()

	r.initLogs()

	r.recordInstant("QUEUED")

	steps := []struct {
		fn   func() outcome
		name string
	}{
		{name: "PROVISIONING", fn: r.provision},
		{name: "DOWNLOAD_SOURCE", fn: r.downloadSource},
	}

	for _, s := range steps {
		if o := r.phase(s.name, s.fn); !o.ok() {
			r.phase("FINALIZING", r.finalizeLogs)

			return o.status
		}
	}

	final := r.runCommandPhases()

	if r.shouldUpload(final) {
		if o := r.phase("UPLOAD_ARTIFACTS", r.upload); !o.ok() && final.ok() {
			final = o
		}
	}

	r.phase("FINALIZING", r.finalizeLogs)

	if final.ok() {
		return buildStatusSucceeded
	}

	return final.status
}

// shouldUpload reports whether the build reached UPLOAD_ARTIFACTS.
func (r *buildRunner) shouldUpload(final outcome) bool {
	if final.status == buildStatusStopped || final.status == buildStatusTimedOut {
		return false
	}

	return r.spec != nil && r.reachedUpload
}

func (r *buildRunner) initLogs() {
	group := defaultLogGroupPfx + r.build.ProjectName
	stream := r.buildUUID()
	enabled := true

	if l := r.build.Logs; l != nil && l.CloudWatchLogs != nil {
		enabled = l.CloudWatchLogs.Status != "DISABLED"

		if l.CloudWatchLogs.GroupName != "" {
			group = l.CloudWatchLogs.GroupName
		}

		if l.CloudWatchLogs.StreamName != "" {
			stream = l.CloudWatchLogs.StreamName
		}
	}

	r.log = &buildLogger{sink: r.svc.Logs, region: r.region, group: group, stream: stream, enabled: enabled}

	status := "ENABLED"
	if !enabled {
		status = "DISABLED"
	}

	acct := r.b.accountID
	r.b.mutateBuild(r.build.ID, func(bd *Build) {
		if bd.Logs == nil {
			bd.Logs = &BuildLogs{}
		}

		bd.Logs.CloudWatchLogs = &CloudWatchLogsConfig{Status: status, GroupName: group, StreamName: stream}
		bd.Logs.GroupName = group
		bd.Logs.StreamName = stream

		if enabled {
			bd.Logs.CloudWatchLogsArn = arn.Build("logs", r.region, acct, "log-group:"+group+":log-stream:"+stream)
		}
	})

	if enabled && r.svc.Logs != nil {
		if err := r.svc.Logs.EnsureLogGroupAndStream(r.ctx, r.region, group, stream); err != nil {
			r.log.enabled = false
			logger.Load(r.ctx).WarnContext(r.ctx, "CodeBuild: cannot create log stream", "error", err)
		}
	}
}

func (r *buildRunner) now() float64 { return awstime.Epoch(time.Now()) }

func (r *buildRunner) recordInstant(name string) {
	t := r.now()
	r.b.mutateBuild(r.build.ID, func(bd *Build) {
		bd.CurrentPhase = name
		bd.Phases = append(bd.Phases, BuildPhase{
			PhaseType: name, PhaseStatus: buildStatusSucceeded, StartTime: t, EndTime: t,
		})
	})
}

// phase runs fn as a recorded build phase.
func (r *buildRunner) phase(name string, fn func() outcome) outcome {
	start := r.now()
	r.b.mutateBuild(r.build.ID, func(bd *Build) {
		bd.CurrentPhase = name
		bd.Phases = append(bd.Phases, BuildPhase{PhaseType: name, StartTime: start})
	})

	r.log.write(r.ctx, marker("Entering phase %s", name))

	o := fn()
	o = r.classify(o)

	status := o.status
	if status == "" {
		status = buildStatusSucceeded
	}

	end := r.now()
	r.b.mutateBuild(r.build.ID, func(bd *Build) {
		for i := len(bd.Phases) - 1; i >= 0; i-- {
			if bd.Phases[i].PhaseType != name {
				continue
			}

			p := &bd.Phases[i]
			p.PhaseStatus = status
			p.EndTime = end
			p.DurationInSeconds = float64(int64(end - start))

			if o.msg != "" {
				p.Contexts = []BuildPhaseContext{{StatusCode: o.code, Message: o.msg}}
			}

			break
		}
	})

	r.log.write(r.ctx, marker("Phase complete: %s State: %s", name, status))

	if o.msg != "" {
		r.log.write(r.ctx, marker("Phase context status code: %s Message: %s", o.code, o.msg))
	}

	return o
}

// classify rewrites a failure into STOPPED or TIMED_OUT when the build was interrupted.
func (r *buildRunner) classify(o outcome) outcome {
	if o.ok() {
		return o
	}

	switch {
	case r.runCtx.Err() != nil:
		return outcome{status: buildStatusStopped}
	case errors.Is(r.ctx.Err(), context.DeadlineExceeded):
		return outcome{status: buildStatusTimedOut, code: "BUILD_TIMED_OUT", msg: "Build timed out"}
	default:
		return o
	}
}

func clientError(format string, args ...any) outcome {
	return outcome{status: buildStatusFailed, code: codeClientError, msg: fmt.Sprintf(format, args...)}
}

func (r *buildRunner) provision() outcome {
	dir, err := os.MkdirTemp(r.cfg.WorkDir, "codebuild-")
	if err != nil {
		return clientError("Failed to create workspace: %v", err)
	}

	r.workDir = dir
	r.srcDir = fmt.Sprintf(
		"%s%d%s",
		containerSrcRoot,
		crc32.ChecksumIEEE([]byte(r.build.ID))%srcDirModulus,
		containerSrcSubdir,
	)

	image := ""
	if r.build.Environment != nil {
		image = r.build.Environment.Image
	}

	cacheMounts, err := r.cacheMounts()
	if err != nil {
		return clientError("Failed to prepare cache: %v", err)
	}

	spec := container.Spec{
		Image:      resolveImage(image, r.cfg.DefaultImage),
		Entrypoint: []string{"tail", "-f", "/dev/null"},
		Mounts:     append([]string{dir + ":" + r.srcDir}, cacheMounts...),
		Privileged: r.build.Environment != nil && r.build.Environment.PrivilegedMode,
	}

	r.log.write(r.ctx, marker("Pulling image %s", spec.Image))

	id, err := r.rt.CreateAndStart(r.ctx, spec)
	if err != nil {
		return clientError("Failed to provision build container: %v", err)
	}

	r.containerID = id
	r.stopWatch = make(chan struct{})
	r.watcherDone = make(chan struct{})

	go r.watch()

	return outcome{}
}

// watch kills the container when the build is stopped or times out.
func (r *buildRunner) watch() {
	defer close(r.watcherDone)

	select {
	case <-r.ctx.Done():
		r.destroyContainer()
	case <-r.stopWatch:
	}
}

func (r *buildRunner) destroyContainer() {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.ctx), engineCleanupTimeout)
	defer cancel()

	_, _ = r.rt.Exec(
		ctx,
		r.containerID,
		[]string{"sh", "-c", "rm -rf " + shQuote(r.srcDir) + "/* " + shQuote(r.srcDir) + "/.[!.]*"},
	)
	_ = r.rt.StopAndRemove(ctx, r.containerID)
}

func (r *buildRunner) cleanup() {
	if r.containerID != "" {
		close(r.stopWatch)
		<-r.watcherDone
		r.destroyContainer()
	}

	if r.workDir != "" {
		_ = os.RemoveAll(r.workDir)
	}

	if r.cacheStage != "" {
		_ = os.RemoveAll(r.cacheStage)
	}

	r.b.finishRun(r.build.ID)
}

func (r *buildRunner) downloadSource() outcome {
	if err := r.fetchSource(r.ctx, r.workDir); err != nil {
		return outcome{status: buildStatusFailed, code: codeClientError, msg: err.Error()}
	}

	text, err := r.loadBuildspec(r.ctx, r.workDir)
	if err != nil {
		return outcome{status: buildStatusFailed, code: "YAML_FILE_ERROR", msg: err.Error()}
	}

	spec, err := parseBuildspecFile(text)
	if err != nil {
		return outcome{status: buildStatusFailed, code: "YAML_FILE_ERROR", msg: err.Error()}
	}

	r.spec = spec
	r.restoreCache()

	return r.exportEnv()
}

// exportEnv resolves the build's variables and writes them to the container's shared env file.
func (r *buildRunner) exportEnv() outcome {
	var projVars []EnvironmentVariable
	if r.build.Environment != nil {
		projVars = r.build.Environment.EnvironmentVariables
	}

	merged := mergeEnvVarOverrides(append(r.spec.specVars(), r.spec.specRefs()...), projVars)

	resolved, masked, err := r.resolveEnv(r.ctx, merged)
	if err != nil {
		return outcome{status: buildStatusFailed, code: "PARAMETER_STORE_ACCESS_ERROR", msg: err.Error()}
	}

	r.masked = masked
	r.log.masked = masked

	all := append(r.builtinEnv(r.srcDir), resolved...)

	res, err := r.rt.Exec(r.ctx, r.containerID, shellInvocation(envFileScript(all)))
	if err != nil || res.ExitCode != 0 {
		return clientError("Failed to write build environment: %v %s", err, res.Stderr)
	}

	return outcome{}
}

func (r *buildRunner) upload() outcome {
	r.processReports()

	if err := r.uploadArtifacts(r.ctx, r.workDir); err != nil {
		return outcome{status: buildStatusFailed, code: codeClientError, msg: err.Error()}
	}

	if err := r.saveCache(); err != nil {
		r.warnCache("save", err)
	}

	return outcome{}
}

func (r *buildRunner) finalizeLogs() outcome {
	l := r.build.Logs
	if l == nil || l.S3Logs == nil || l.S3Logs.Status != "ENABLED" || r.svc.Store == nil {
		return outcome{}
	}

	bucket, prefix, _ := strings.Cut(strings.TrimPrefix(l.S3Logs.Location, s3ARNPrefix), "/")
	if bucket == "" {
		return outcome{}
	}

	var gz bytes.Buffer

	zw := gzip.NewWriter(&gz)
	_, _ = zw.Write(r.log.buf.Bytes())
	_ = zw.Close()

	key := strings.Trim(prefix+"/"+r.buildUUID()+".gz", "/")
	if err := r.svc.Store.PutObjectBytes(r.ctx, bucket, key, gz.Bytes()); err != nil {
		return clientError("Failed to upload logs: %v", err)
	}

	return outcome{}
}

// runCommandPhases runs install through post_build and returns the first failure.
func (r *buildRunner) runCommandPhases() outcome {
	var first outcome

	r.reachedUpload = true

	for _, name := range execPhaseNames() {
		ph := r.spec.Phases[name]

		o := r.phase(strings.ToUpper(name), func() outcome { return r.runPhaseCommands(ph) })
		if o.ok() {
			continue
		}

		r.succeeding = false

		if first.ok() {
			first = o
		}

		if o.status != buildStatusFailed {
			r.reachedUpload = false

			return first
		}

		if !r.continueAfterFailure(name, ph) {
			r.reachedUpload = false

			return first
		}
	}

	return first
}

// continueAfterFailure applies on-failure: CONTINUE always proceeds, ABORT never does, and
// unset proceeds only past a failed build phase so post_build still runs.
func (r *buildRunner) continueAfterFailure(name string, ph specPhase) bool {
	switch ph.OnFailure {
	case onFailureContinue:
		return true
	case onFailureAbort:
		return false
	default:
		return name == "build" || name == "post_build"
	}
}

func (r *buildRunner) runPhaseCommands(ph specPhase) outcome {
	o := r.runScripts(commandScripts(r.spec.Version, ph.Commands, r.srcDir, r.succeeding))

	if r.ctx.Err() != nil {
		return o
	}

	succ := r.succeeding && o.ok()

	fo := r.runScripts(commandScripts(r.spec.Version, ph.Finally, r.srcDir, succ))
	if o.ok() {
		return fo
	}

	return o
}

func (r *buildRunner) runScripts(scripts []string) outcome {
	for _, s := range scripts {
		out, code, err := r.execLogged(s)
		if err != nil {
			return outcome{status: buildStatusFailed, code: codeClientError, msg: err.Error()}
		}

		if code != 0 {
			return outcome{
				status: buildStatusFailed, code: "COMMAND_EXECUTION_ERROR",
				msg: failureMessage(out, code),
			}
		}
	}

	return outcome{}
}

// mutateBuild applies fn to a still-running build; it is a no-op once the build is complete.
func (b *InMemoryBackend) mutateBuild(id string, fn func(*Build)) {
	b.mu.Lock("mutateBuild")
	defer b.mu.Unlock()

	if bd, ok := b.builds.Get(id); ok && !bd.BuildComplete {
		fn(bd)
	}
}

// completeBuild records the terminal status unless StopBuild already did.
func (b *InMemoryBackend) completeBuild(id, status string) {
	defer b.reconcileBatchOf(id)

	b.mutateBuild(id, func(bd *Build) {
		now := awstime.Epoch(time.Now())

		bd.Phases = append(bd.Phases, BuildPhase{PhaseType: phaseCompleted, StartTime: now})
		bd.BuildStatus = status
		bd.EndTime = now
		bd.CurrentPhase = phaseCompleted
		bd.BuildComplete = true
	})
}

func (b *InMemoryBackend) finishRun(id string) {
	b.mu.Lock("finishRun")
	defer b.mu.Unlock()

	delete(b.runs, id)
}

// stopRunLocked cancels an executing build. The caller holds b.mu.
func (b *InMemoryBackend) stopRunLocked(build *Build) {
	run, ok := b.runs[build.ID]
	if !ok {
		return
	}

	run.cancel()

	for i := len(build.Phases) - 1; i >= 0; i-- {
		if build.Phases[i].EndTime == 0 && build.Phases[i].PhaseStatus == "" {
			build.Phases[i].PhaseStatus = buildStatusStopped
			build.Phases[i].EndTime = awstime.Epoch(time.Now())
		}
	}
}

// cancelRunsLocked cancels every executing build. The caller holds b.mu.
func (b *InMemoryBackend) cancelRunsLocked() {
	for _, run := range b.runs {
		run.cancel()
	}

	b.runs = make(map[string]*buildRun)
}

// buildExecuting reports whether the docker engine is running id. The caller holds b.mu (read).
func (b *InMemoryBackend) buildExecuting(id string) bool {
	_, ok := b.runs[id]

	return ok
}

// reconcileBatchOf advances the batch a finished child build belongs to.
func (b *InMemoryBackend) reconcileBatchOf(id string) {
	b.mu.Lock("reconcileBatchOf")
	defer b.mu.Unlock()

	bd, ok := b.builds.Get(id)
	if !ok || bd.BuildBatchArn == "" {
		return
	}

	for _, batch := range b.buildBatchesByARN.Get(bd.BuildBatchArn) {
		def, err := parseBatchDefinition(sourceBuildspec(batch.Source))
		if err != nil {
			continue
		}

		b.reconcileBatch(batch, def, float64(time.Now().Unix()))
	}
}
