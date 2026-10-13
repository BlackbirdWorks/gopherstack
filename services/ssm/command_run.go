package ssm

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	stdoutLimitChars  = 24000
	stderrLimitChars  = 8000
	pluginOutputChars = 2500

	defaultMaxConcurrency = 50

	detailsTerminated       = "Terminated"
	detailsExecutionTimeout = "ExecutionTimedOut"
	detailsIncomplete       = "Incomplete"

	responseCodeNotRun = -1
)

type directTarget struct {
	instanceID  string
	containerID string
}

type directJob struct {
	cw        *CloudWatchOutputConfig
	runner    *CommandRunner
	region    string
	cmdID     string
	docName   string
	bucket    string
	prefix    string
	steps     []scriptStep
	targets   []directTarget
	maxConc   int
	maxErrors int
}

type invResult struct {
	status    string
	details   string
	stdout    string
	stderr    string
	stdoutURL string
	stderrURL string
	plugins   []CommandPlugin
	started   float64
	finished  float64
	code      int32
}

// parseLimit resolves a MaxConcurrency/MaxErrors value ("N" or "P%") against total targets.
func parseLimit(v string, total, def int) int {
	if v == "" {
		return def
	}

	if p, ok := strings.CutSuffix(v, "%"); ok {
		n, err := strconv.Atoi(p)
		if err != nil {
			return def
		}

		return total * n / 100 //nolint:mnd // percent
	}

	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}

	return n
}

func execKey(region, cmdID, instanceID string) string {
	return region + "/" + cmdID + "/" + instanceID
}

func (b *InMemoryBackend) registerExec(key string, cancel context.CancelFunc) {
	b.execMu.Lock()
	defer b.execMu.Unlock()

	if b.execCancels == nil {
		b.execCancels = make(map[string]context.CancelFunc)
	}

	b.execCancels[key] = cancel
}

func (b *InMemoryBackend) unregisterExec(key string) {
	b.execMu.Lock()
	defer b.execMu.Unlock()

	delete(b.execCancels, key)
}

func (b *InMemoryBackend) cancelExec(key string) {
	b.execMu.Lock()
	cancel := b.execCancels[key]
	b.execMu.Unlock()

	if cancel != nil {
		cancel()
	}
}

// runDirectJob runs the job's invocations with MaxConcurrency/MaxErrors dispatch control.
func (b *InMemoryBackend) runDirectJob(job *directJob) {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs int
		next int
	)

	slots := make(chan struct{}, max(job.maxConc, 1))

	for next < len(job.targets) {
		slots <- struct{}{}

		mu.Lock()
		stop := errs > job.maxErrors
		mu.Unlock()

		if stop {
			<-slots

			break
		}

		t := job.targets[next]
		next++

		wg.Go(func() {
			failed := b.runInvocation(job, t)

			mu.Lock()
			if failed {
				errs++
			}
			mu.Unlock()

			<-slots
		})
	}

	wg.Wait()

	b.terminateUnstarted(job, job.targets[next:])
}

func (b *InMemoryBackend) terminateUnstarted(job *directJob, rest []directTarget) {
	if len(rest) == 0 {
		return
	}

	b.mu.Lock("terminateUnstarted")
	defer b.mu.Unlock()

	invs := b.commandInvocationsStore(job.region)[job.cmdID]
	now := UnixTimeFloat(timeNow())

	for _, t := range rest {
		for i := range invs {
			if invs[i].InstanceID == t.instanceID && invs[i].Status == commandStatusPending {
				invs[i].Status = commandStatusCancelled
				invs[i].StatusDetails = detailsTerminated
				invs[i].executionEndUnix = now
			}
		}
	}

	b.settleCommandLocked(job.region, job.cmdID)
}

// runInvocation executes the steps on one target and records the result; it reports whether the
// invocation counts against MaxErrors.
func (b *InMemoryBackend) runInvocation(job *directJob, t directTarget) bool {
	key := execKey(job.region, job.cmdID, t.instanceID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if !b.beginInvocation(job, t, key, cancel) {
		return false
	}

	defer b.unregisterExec(key)

	res := b.executeSteps(ctx, job, t)
	b.finishInvocation(job, t, res)

	return res.status == commandStatusFailed || res.status == commandStatusTimedOut
}

func (b *InMemoryBackend) beginInvocation(job *directJob, t directTarget, key string, cancel context.CancelFunc) bool {
	b.mu.Lock("beginInvocation")
	defer b.mu.Unlock()

	invs := b.commandInvocationsStore(job.region)[job.cmdID]
	for i := range invs {
		if invs[i].InstanceID != t.instanceID {
			continue
		}

		if invs[i].Status != commandStatusPending {
			return false
		}

		invs[i].Status = commandStatusInProgress
		invs[i].StatusDetails = commandStatusInProgress
		invs[i].startedUnix = UnixTimeFloat(timeNow())
		b.registerExec(key, cancel)

		return true
	}

	return false
}

func (b *InMemoryBackend) executeSteps(ctx context.Context, job *directJob, t directTarget) invResult {
	res := invResult{started: UnixTimeFloat(timeNow()), status: commandStatusSuccess, details: commandStatusSuccess}

	var out, errOut []string

	for i, step := range job.steps {
		p := b.runStep(ctx, job, t, step)
		b.storePluginOutput(job, t, i, step, &p)

		res.plugins = append(res.plugins, p)
		out = append(out, p.stdout)
		errOut = append(errOut, p.stderr)

		if res.status == commandStatusSuccess && p.Status != commandStatusSuccess {
			res.status, res.details, res.code = p.Status, p.StatusDetails, p.ResponseCode
		}

		if p.Status != commandStatusSuccess && (step.critical || p.Status == commandStatusCancelled) {
			break
		}
	}

	res.stdout = truncateChars(strings.Join(out, ""), stdoutLimitChars)
	res.stderr = truncateChars(strings.Join(errOut, ""), stderrLimitChars)

	if len(res.plugins) > 0 {
		res.stdoutURL, res.stderrURL = res.plugins[0].StandardOutputURL, res.plugins[0].StandardErrorURL
	}

	res.finished = UnixTimeFloat(timeNow())

	return res
}

func (b *InMemoryBackend) runStep(ctx context.Context, job *directJob, t directTarget, step scriptStep) CommandPlugin {
	p := CommandPlugin{Name: step.name, ResponseStartDateTime: UnixTimeFloat(timeNow())}

	tctx, cancel := context.WithTimeout(ctx, time.Duration(step.timeoutSecs)*time.Second)
	defer cancel()

	res, err := job.runner.Executor.RunScript(tctx, t.containerID, ScriptRequest{
		Script: step.script, WorkDir: step.workDir, Interpreter: step.interpreter(ScriptFilePlaceholder),
	})

	p.ResponseFinishDateTime = UnixTimeFloat(timeNow())
	p.stdout, p.stderr = string(res.Stdout), string(res.Stderr)

	switch {
	case err == nil && res.ExitCode == 0:
		p.Status, p.StatusDetails = commandStatusSuccess, commandStatusSuccess
	case err == nil:
		p.Status, p.StatusDetails, p.ResponseCode = commandStatusFailed, commandStatusFailed, int32(
			res.ExitCode,
		) // #nosec G115
	case ctx.Err() != nil:
		p.Status, p.StatusDetails, p.ResponseCode = commandStatusCancelled, commandStatusCancelled, responseCodeNotRun
	case errors.Is(tctx.Err(), context.DeadlineExceeded):
		p.Status, p.StatusDetails, p.ResponseCode = commandStatusTimedOut, detailsExecutionTimeout, responseCodeNotRun
	default:
		p.Status, p.StatusDetails, p.ResponseCode = commandStatusFailed, commandStatusFailed, responseCodeNotRun
		p.stderr += fmt.Sprintf("failed to run commands: %v\n", err)
	}

	return p
}

// storePluginOutput uploads the full plugin output and fills the plugin's wire fields.
func (b *InMemoryBackend) storePluginOutput(
	job *directJob,
	t directTarget,
	idx int,
	step scriptStep,
	p *CommandPlugin,
) {
	p.Output = truncateChars(p.stdout+p.stderr, pluginOutputChars)

	if job.runner.Sink == nil {
		return
	}

	if job.bucket != "" {
		base := outputKeyPrefix(job.prefix, job.cmdID, t.instanceID, step.pluginName(), idx)
		p.OutputS3BucketName, p.OutputS3KeyPrefix = job.bucket, job.prefix
		p.StandardOutputURL = uploadOutput(job, base+"/stdout", p.stdout)
		p.StandardErrorURL = uploadOutput(job, base+"/stderr", p.stderr)
	}

	if job.cw != nil && job.cw.CloudWatchOutputEnabled {
		sendLogOutput(job, t, step, p)
	}
}

// uploadOutput stores one stream in S3 and returns its URL, or "" when the upload failed.
func uploadOutput(job *directJob, key, body string) string {
	if err := job.runner.Sink.PutObject(job.bucket, key, []byte(body)); err != nil {
		logger.Load(context.Background()).Warn("ssm: command output upload failed", "bucket", job.bucket, "error", err)

		return ""
	}

	return s3ObjectURL(job.region, job.bucket, key)
}

func sendLogOutput(job *directJob, t directTarget, step scriptStep, p *CommandPlugin) {
	group := job.cw.CloudWatchLogGroupName
	if group == "" {
		group = "/aws/ssm/" + job.docName
	}

	stream := job.cmdID + "/" + t.instanceID + "/" + step.cloudWatchName()

	for name, text := range map[string]string{"stdout": p.stdout, "stderr": p.stderr} {
		lines := splitLines(text)
		if len(lines) == 0 {
			continue
		}

		if err := job.runner.Sink.PutLogLines(job.region, group, stream+"/"+name, lines); err != nil {
			logger.Load(context.Background()).Warn("ssm: command log delivery failed", "group", group, "error", err)
		}
	}
}

func outputKeyPrefix(prefix, cmdID, instanceID, plugin string, idx int) string {
	parts := []string{cmdID, instanceID, plugin, strconv.Itoa(idx) + "." + plugin}
	if prefix != "" {
		parts = append([]string{strings.Trim(prefix, "/")}, parts...)
	}

	return strings.Join(parts, "/")
}

func s3ObjectURL(region, bucket, key string) string {
	host := "s3." + region + ".amazonaws.com"
	if region == "" || region == "us-east-1" {
		host = "s3.amazonaws.com"
	}

	return "https://" + host + "/" + bucket + "/" + key
}

func splitLines(s string) []string {
	var out []string

	for l := range strings.SplitSeq(s, "\n") {
		if l != "" {
			out = append(out, l)
		}
	}

	return out
}

// truncateChars keeps the first n characters (runes) of s.
func truncateChars(s string, n int) string {
	if len(s) <= n {
		return s
	}

	r := []rune(s)
	if len(r) <= n {
		return s
	}

	return string(r[:n])
}

func (b *InMemoryBackend) finishInvocation(job *directJob, t directTarget, res invResult) {
	b.mu.Lock("finishInvocation")
	defer b.mu.Unlock()

	invs := b.commandInvocationsStore(job.region)[job.cmdID]
	for i := range invs {
		if invs[i].InstanceID != t.instanceID {
			continue
		}

		if invs[i].Status == commandStatusCancelled {
			break
		}

		invs[i].Status, invs[i].StatusDetails = res.status, res.details
		invs[i].StandardOutputContent, invs[i].StandardErrorContent = res.stdout, res.stderr
		invs[i].StandardOutputURL, invs[i].StandardErrorURL = res.stdoutURL, res.stderrURL
		invs[i].ResponseCode = res.code
		invs[i].CommandPlugins = res.plugins
		invs[i].executionEndUnix = res.finished

		break
	}

	b.settleCommandLocked(job.region, job.cmdID)
}

// overallCommandStatus derives a command's status from its invocations; ok is false until all are terminal.
func overallCommandStatus(invs []CommandInvocation, maxErrors int) (string, string, bool) {
	var failed, timedOut, cancelled int

	for _, inv := range invs {
		switch inv.Status {
		case commandStatusPending, commandStatusInProgress:
			return "", "", false
		case commandStatusFailed:
			failed++
		case commandStatusTimedOut:
			timedOut++
		case commandStatusCancelled:
			if inv.StatusDetails != detailsTerminated {
				cancelled++
			}
		}
	}

	switch {
	case cancelled > 0:
		return commandStatusCancelled, commandStatusCancelled, true
	case failed+timedOut > maxErrors && failed == 0:
		return commandStatusTimedOut, detailsExecutionTimeout, true
	case failed+timedOut > maxErrors:
		return commandStatusFailed, commandStatusFailed, true
	case failed+timedOut > 0:
		return commandStatusFailed, detailsIncomplete, true
	default:
		return commandStatusSuccess, commandStatusSuccess, true
	}
}

// settleCommandLocked sets the command's terminal status once every invocation is terminal.
// Must be called with b.mu held for writing.
func (b *InMemoryBackend) settleCommandLocked(region, cmdID string) {
	table := b.commandsStore(region)

	cmdPtr, ok := table.Get(cmdID)
	if !ok || cmdPtr.Status == commandStatusCancelled {
		return
	}

	invs := b.commandInvocationsStore(region)[cmdID]

	status, details, done := overallCommandStatus(invs, parseLimit(cmdPtr.MaxErrors, len(invs), 0))
	if !done {
		return
	}

	cmd := *cmdPtr
	cmd.Status, cmd.StatusDetails = status, details
	cmd.completeAfter = 0
	cmd.terminalAt = UnixTimeFloat(timeNow())
	table.Put(&cmd)
}

func (j *directJob) hasTarget(instanceID string) bool {
	for _, t := range j.targets {
		if t.instanceID == instanceID {
			return true
		}
	}

	return false
}

// planDirectJob selects the targets that run in containers; nil means the command is simulated.
// Must be called with b.mu held.
func (b *InMemoryBackend) planDirectJob(
	region string,
	runner *CommandRunner,
	containers []ManagedContainer,
	input *SendCommandInput,
	ids []string,
) *directJob {
	if runner == nil || runner.Executor == nil || len(containers) == 0 {
		return nil
	}

	docPtr, ok := b.documentsStore(region).Get(input.DocumentName)
	if !ok || docPtr.DocumentType != DocumentTypeCommand {
		return nil
	}

	content, format := docPtr.Content, docPtr.DocumentFormat

	versions := b.documentVersionsStore(region)[input.DocumentName]
	if target, err := resolveDocumentVersionTarget(*docPtr, versions, input.DocumentVersion, ""); err == nil {
		for _, v := range versions {
			if v.DocumentVersion == target {
				content, format = v.Content, v.DocumentFormat
			}
		}
	}

	steps, ok := parseScriptSteps(content, format, input.Parameters)
	if !ok {
		return nil
	}

	var targets []directTarget

	for _, id := range ids {
		for _, c := range containers {
			if c.InstanceID == id {
				targets = append(targets, directTarget{instanceID: id, containerID: c.ContainerID})
			}
		}
	}

	if len(targets) == 0 {
		return nil
	}

	return &directJob{
		region: region, docName: input.DocumentName, bucket: input.OutputS3BucketName,
		prefix: input.OutputS3KeyPrefix, cw: input.CloudWatchOutputConfig, steps: steps,
		targets: targets, runner: runner,
		maxConc:   min(parseLimit(input.MaxConcurrency, len(ids), defaultMaxConcurrency), len(targets)),
		maxErrors: parseLimit(input.MaxErrors, len(ids), 0),
	}
}
