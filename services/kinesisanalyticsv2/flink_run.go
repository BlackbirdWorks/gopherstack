package kinesisanalyticsv2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const flinkMaxPollFailures = 5

func flinkServerScript() string {
	return "/docker-entrypoint.sh jobmanager & " +
		"until (echo > /dev/tcp/localhost/" + flinkRESTPort + ") 2>/dev/null; do sleep 1; done; " +
		"/docker-entrypoint.sh taskmanager & wait -n"
}

func (e *flinkEngine) spec(in flinkLaunch, stageDir string, port int, creds []string) container.Spec {
	props := []string{
		"jobmanager.rpc.address: localhost",
		"rest.bind-address: 0.0.0.0",
		"taskmanager.numberOfTaskSlots: " + strconv.Itoa(int(max(in.parallelism, 1))),
		"state.savepoints.dir: file://" + flinkSavepointsDir,
	}

	if in.checkpointMS > 0 {
		props = append(props, "execution.checkpointing.interval: "+strconv.FormatInt(in.checkpointMS, 10)+"ms")
	}

	env := []string{
		"FLINK_PROPERTIES=" + strings.Join(props, "\n"),
		"JOB_MANAGER_RPC_ADDRESS=localhost",
		"AWS_REGION=" + in.region,
		"AWS_DEFAULT_REGION=" + in.region,
	}
	env = append(env, creds...)

	if e.cfg.EndpointURL != "" {
		env = append(env, "AWS_ENDPOINT_URL="+e.cfg.EndpointURL)
	}

	bind := ""
	if e.cfg.Host == flinkDefaultHost {
		bind = flinkDefaultHost + ":"
	}

	return container.Spec{
		Image:      in.image,
		Env:        env,
		Entrypoint: []string{"/bin/bash", "-c"},
		Cmd:        []string{flinkServerScript()},
		Ports:      []string{bind + strconv.Itoa(port) + ":" + flinkRESTPort},
		Mounts: []string{
			filepath.Join(stageDir, "application_properties.json") + ":" + flinkPropertiesFile + ":ro",
			in.saveDir + ":" + flinkSavepointsDir,
		},
	}
}

func (e *flinkEngine) stage(in flinkLaunch) (string, error) {
	dir, err := os.MkdirTemp("", "kda-flink-stage-*")
	if err != nil {
		return "", fmt.Errorf("create stage dir: %w", err)
	}

	props := in.props
	if props == nil {
		props = []PropertyGroup{}
	}

	raw, err := json.Marshal(props)
	if err == nil {
		err = os.WriteFile(filepath.Join(dir, "application_properties.json"), raw, flinkFileMode)
	}

	if err != nil {
		_ = os.RemoveAll(dir)

		return "", fmt.Errorf("write application properties: %w", err)
	}

	return dir, nil
}

func (e *flinkEngine) codeBytes(ctx context.Context, in flinkLaunch) ([]byte, error) {
	if len(in.code.zipContent) > 0 {
		return in.code.zipContent, nil
	}

	loc := in.code.S3ApplicationCodeLocationDescription
	if loc == nil {
		return nil, errFlinkNoCode
	}

	return e.cfg.Code(ctx, loc.BucketARN, loc.FileKey, loc.ObjectVersion)
}

// launchFlinkLocked registers a job for in and boots it in the background; callers hold b.mu.
func (b *InMemoryBackend) launchFlinkLocked(in flinkLaunch, opID string) {
	e := b.flink
	ctx, cancel := context.WithCancel(context.Background())
	lj := &flinkJob{ctx: ctx, cancel: cancel, done: make(chan struct{}), opID: opID}
	e.jobs[in.key] = lj

	e.wg.Go(func() { b.runFlink(e, lj, in) })
}

func (b *InMemoryBackend) runFlink(e *flinkEngine, lj *flinkJob, in flinkLaunch) {
	defer close(lj.done)
	defer e.teardown(lj)

	err := b.bootFlink(e, lj, in)
	if err == nil {
		err = b.watchFlink(e, lj)
	}

	if lj.ctx.Err() != nil {
		return
	}

	b.finishFlink(e, lj, in, err)
}

func (b *InMemoryBackend) bootFlink(e *flinkEngine, lj *flinkJob, in flinkLaunch) error {
	ctx, cancel := context.WithTimeout(lj.ctx, e.cfg.StartTimeout)
	defer cancel()

	code, err := e.codeBytes(ctx, in)
	if err != nil {
		return err
	}

	if lj.stageDir, err = e.stage(in); err != nil {
		return err
	}

	creds, err := credentialEnv(in)
	if err != nil {
		return err
	}

	lj.containerID, err = container.CreateAndStartRetry(ctx, e.cfg.Runtime,
		func() (container.Spec, error) {
			p, perr := e.acquirePort(in.name)
			if perr != nil {
				return container.Spec{}, perr
			}

			lj.port = p

			return e.spec(in, lj.stageDir, p, creds), nil
		},
		func() {
			portalloc.ReleaseTo(e.cfg.Ports, lj.port)
			lj.port = 0
		})
	if err != nil {
		return fmt.Errorf("start flink container: %w", err)
	}

	b.mu.Lock("bootFlink")
	lj.base = e.baseURL(lj.port)
	b.mu.Unlock()

	return b.submitFlink(ctx, e, lj, in, code)
}

func (b *InMemoryBackend) submitFlink(
	ctx context.Context, e *flinkEngine, lj *flinkJob, in flinkLaunch, code []byte,
) error {
	rest := e.rest(lj)

	err := pollUntil(ctx, e.cfg.PollInterval, func(c context.Context) (bool, error) {
		return rest.ready(c) == nil, nil
	})
	if err != nil {
		return err
	}

	jarID, err := rest.uploadJar(ctx, code)
	if err != nil {
		return err
	}

	jobID, err := rest.runJar(ctx, jarID, flinkRunRequest{
		Parallelism:           in.parallelism,
		SavepointPath:         in.savepointPath,
		AllowNonRestoredState: in.allowNonRest,
	})
	if err != nil {
		return err
	}

	b.mu.Lock("submitFlink")
	lj.jobID = jobID
	b.mu.Unlock()

	return pollUntil(ctx, e.cfg.PollInterval, func(c context.Context) (bool, error) {
		return b.checkStarting(c, rest, lj, in)
	})
}

func (b *InMemoryBackend) checkStarting(
	ctx context.Context,
	rest flinkREST,
	lj *flinkJob,
	in flinkLaunch,
) (bool, error) {
	state, tasksUp, err := rest.jobState(ctx, lj.jobID)
	if err != nil {
		return false, err
	}

	switch state {
	case "RUNNING":
		if !tasksUp {
			return false, nil
		}

		b.markFlinkRunning(lj, in)

		return true, nil
	case "FINISHED":
		return false, errFlinkFinished
	case "FAILED", "CANCELED", "SUSPENDED":
		return false, fmt.Errorf("%w: %s", errFlinkJobFailed, state)
	}

	return false, nil
}

func (b *InMemoryBackend) markFlinkRunning(lj *flinkJob, in flinkLaunch) {
	b.mu.Lock("markFlinkRunning")
	defer b.mu.Unlock()

	if cur, ok := b.flinkManagedLocked(in.key); !ok || cur != lj {
		return
	}

	if app, found := b.findApplication(in.region, in.name); found &&
		app.ApplicationStatus == ApplicationStatusStarting {
		app.ApplicationStatus = ApplicationStatusRunning
		b.setOperationStatusLocked(in.region, in.name, lj.opID, OperationStatusSuccessful)
	}
}

func (b *InMemoryBackend) watchFlink(e *flinkEngine, lj *flinkJob) error {
	rest := e.rest(lj)
	failures := 0

	return pollUntil(lj.ctx, e.cfg.PollInterval, func(c context.Context) (bool, error) {
		state, _, err := rest.jobState(c, lj.jobID)
		if err != nil {
			if failures++; failures > flinkMaxPollFailures {
				return false, err
			}

			return false, nil
		}

		failures = 0

		switch state {
		case "FINISHED":
			return false, errFlinkFinished
		case "FAILED", "CANCELED":
			return false, fmt.Errorf("%w: %s", errFlinkJobFailed, state)
		}

		return false, nil
	})
}

func (b *InMemoryBackend) finishFlink(e *flinkEngine, lj *flinkJob, in flinkLaunch, cause error) {
	b.mu.Lock("finishFlink")
	defer b.mu.Unlock()

	app, ok := b.findApplication(in.region, in.name)
	if cur, managed := e.jobs[in.key]; !managed || cur != lj || !ok ||
		app.ApplicationStatus == ApplicationStatusStopping {
		return
	}

	delete(e.jobs, in.key)

	app.ApplicationStatus = ApplicationStatusReady
	app.transitionalStatus = ""

	status := OperationStatusSuccessful
	if cause != nil && !errors.Is(cause, errFlinkFinished) {
		status = operationFailed

		logger.Load(lj.ctx).WarnContext(lj.ctx, "kinesisanalyticsv2: flink job ended",
			"application", in.name, "error", cause)
	}

	b.setOperationStatusLocked(in.region, in.name, lj.opID, status)
}

// setOperationStatusLocked updates a recorded operation; callers hold b.mu.
func (b *InMemoryBackend) setOperationStatusLocked(region, appName, opID, status string) {
	for _, op := range b.operations[region][appName] {
		if op.OperationID == opID {
			op.OperationStatus = status

			if status != operationInProgress {
				op.EndTimestamp = time.Now().UTC()
			}

			return
		}
	}
}
