package glue

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"
)

//go:embed session_repl.pyrepl
var sessionReplScript []byte

const (
	defaultStartTimeout = 10 * time.Minute
	replPollInterval    = 25 * time.Millisecond
	replLivenessEvery   = 5 * time.Second
	replHeartbeatStale  = time.Minute
	replReadyPoll       = 200 * time.Millisecond
	dirPerm             = 0o777
	filePerm            = 0o644
	containerSessionDir = "/session"
)

var (
	errReplStart   = errors.New("session interpreter failed to start")
	errReplDied    = errors.New("session interpreter is not running")
	errReplHung    = errors.New("session interpreter stopped responding")
	errSessionDone = errors.New("session interpreter stopped")
)

// glueImages maps GlueVersion to the public AWS Glue libs image.
var glueImages = map[string]string{ //nolint:gochecknoglobals // read-only lookup table
	"5.0": "public.ecr.aws/glue/aws-glue-libs:5",
	"4.0": "public.ecr.aws/glue/aws-glue-libs:glue_libs_4.0.0_image_01",
	"3.0": "public.ecr.aws/glue/aws-glue-libs:glue_libs_3.0.0_image_01",
}

// SessionContainerSpec is the container a session interpreter runs in.
type SessionContainerSpec struct {
	Image      string
	Entrypoint []string
	Env        []string
	Mounts     []string
}

// SessionRuntime is the slice of the container runtime a docker session needs.
type SessionRuntime interface {
	HasImage(ctx context.Context, imageRef string) (bool, error)
	PullImage(ctx context.Context, imageRef string) error
	CreateAndStart(ctx context.Context, spec SessionContainerSpec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
	Alive(ctx context.Context, containerID string) error
}

// DockerSessionConfig configures NewDockerSessionEngine.
type DockerSessionConfig struct {
	Runtime SessionRuntime
	// Image overrides the per-GlueVersion AWS Glue libs image.
	Image string
	// WorkDir is the host directory holding session exchange dirs; default is the OS temp dir.
	WorkDir string
	// StartTimeout bounds interpreter startup (image pull, Spark init); default 10m.
	StartTimeout time.Duration
}

type dockerSessionEngine struct{ cfg DockerSessionConfig }

// NewDockerSessionEngine runs each session as a long-lived container holding a Python REPL.
func NewDockerSessionEngine(cfg DockerSessionConfig) SessionEngine {
	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = defaultStartTimeout
	}

	return &dockerSessionEngine{cfg: cfg}
}

func (e *dockerSessionEngine) image(glueVersion string) string {
	if e.cfg.Image != "" {
		return e.cfg.Image
	}

	if img, ok := glueImages[glueVersion]; ok {
		return img
	}

	return glueImages["5.0"]
}

func sparkCommand(name string) bool { return name == "glueetl" || name == "gluestreaming" }

func sessionEnv(spec SessionSpec) ([]string, error) {
	args, err := json.Marshal(spec.DefaultArguments)
	if err != nil {
		return nil, fmt.Errorf("encode default arguments: %w", err)
	}

	conf, err := json.Marshal(parseSparkConf(spec.DefaultArguments["--conf"]))
	if err != nil {
		return nil, fmt.Errorf("encode spark conf: %w", err)
	}

	spark := "0"
	if sparkCommand(spec.CommandName) {
		spark = "1"
	}

	return []string{
		"GLUE_SESSION_ID=" + spec.SessionID,
		"GLUE_SESSION_SPARK=" + spark,
		"GLUE_VERSION=" + spec.GlueVersion,
		"GLUE_WORKER_TYPE=" + spec.WorkerType,
		"GLUE_NUMBER_OF_WORKERS=" + strconv.Itoa(int(spec.NumberOfWorkers)),
		"GLUE_DEFAULT_ARGUMENTS=" + string(args),
		"GLUE_SPARK_CONF=" + string(conf),
		"PYTHONUNBUFFERED=1",
	}, nil
}

func prepareExchangeDir(base string) (string, error) {
	dir, err := os.MkdirTemp(base, "glue-session-")
	if err != nil {
		return "", fmt.Errorf("create session dir: %w", err)
	}

	for _, d := range []string{dir, filepath.Join(dir, "req"), filepath.Join(dir, "resp"), filepath.Join(dir, "cancel")} {
		if err = os.MkdirAll(d, dirPerm); err != nil {
			return dir, fmt.Errorf("create %s: %w", d, err)
		}

		if err = os.Chmod(d, dirPerm); err != nil {
			return dir, fmt.Errorf("chmod %s: %w", d, err)
		}
	}

	if err = os.WriteFile(filepath.Join(dir, "repl.py"), sessionReplScript, filePerm); err != nil {
		return dir, fmt.Errorf("write repl: %w", err)
	}

	return dir, nil
}

func (e *dockerSessionEngine) StartSession(ctx context.Context, spec SessionSpec) (SessionProcess, error) {
	env, err := sessionEnv(spec)
	if err != nil {
		return nil, err
	}

	img := e.image(spec.GlueVersion)

	if err = e.ensureImage(ctx, img); err != nil {
		return nil, err
	}

	dir, err := prepareExchangeDir(e.cfg.WorkDir)
	if err != nil {
		_ = os.RemoveAll(dir)

		return nil, err
	}

	id, err := e.cfg.Runtime.CreateAndStart(ctx, SessionContainerSpec{
		Image:      img,
		Entrypoint: []string{"python3", "-u", containerSessionDir + "/repl.py"},
		Env:        env,
		Mounts:     []string{dir + ":" + containerSessionDir},
	})
	if err != nil {
		_ = os.RemoveAll(dir)

		return nil, fmt.Errorf("start session container: %w", err)
	}

	p := &dockerSession{rt: e.cfg.Runtime, containerID: id, dir: dir}

	if err = p.awaitReady(ctx, e.cfg.StartTimeout); err != nil {
		_ = p.Stop(context.WithoutCancel(ctx))

		return nil, err
	}

	return p, nil
}

func (e *dockerSessionEngine) ensureImage(ctx context.Context, img string) error {
	if ok, err := e.cfg.Runtime.HasImage(ctx, img); err == nil && ok {
		return nil
	}

	if err := e.cfg.Runtime.PullImage(ctx, img); err != nil {
		return fmt.Errorf("pull session image %q: %w", img, err)
	}

	return nil
}

type dockerSession struct {
	rt          SessionRuntime
	containerID string
	dir         string
	seq         int
	cur         atomic.Int64
}

func (p *dockerSession) awaitReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	tick := time.NewTicker(replReadyPoll)
	defer tick.Stop()

	lastCheck := time.Now()

	for {
		if data, err := os.ReadFile(filepath.Join(p.dir, "ready.json")); err == nil {
			var r struct {
				Status  string `json:"status"`
				Message string `json:"message"`
			}
			if json.Unmarshal(data, &r) == nil && r.Status == "ok" {
				return nil
			} else if r.Status == "error" {
				return fmt.Errorf("%w: %s", errReplStart, r.Message)
			}
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", errReplStart, ctx.Err())
		case <-deadline.C:
			return fmt.Errorf("%w: not ready after %s", errReplStart, timeout)
		case <-tick.C:
		}

		if time.Since(lastCheck) >= replLivenessEvery {
			lastCheck = time.Now()

			if err := p.rt.Alive(ctx, p.containerID); err != nil {
				return fmt.Errorf("%w: container exited: %w", errReplStart, err)
			}
		}
	}
}

type replResponse struct {
	Text      string   `json:"text"`
	ErrorName string   `json:"ename"`
	ErrorVal  string   `json:"evalue"`
	Traceback []string `json:"traceback"`
	Count     int32    `json:"count"`
	OK        bool     `json:"ok"`
}

func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, filePerm); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}

	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename %s: %w", tmp, err)
	}

	return nil
}

func (p *dockerSession) Run(ctx context.Context, code string) (StatementResult, error) {
	p.seq++
	n := p.seq

	body, err := json.Marshal(map[string]string{"code": code})
	if err != nil {
		return StatementResult{}, fmt.Errorf("encode statement: %w", err)
	}

	p.cur.Store(int64(n))
	defer p.cur.Store(0)

	if err = writeAtomic(filepath.Join(p.dir, "req", strconv.Itoa(n)+".json"), body); err != nil {
		return StatementResult{}, err
	}

	resp, err := p.awaitResponse(ctx, n)
	if err != nil {
		return StatementResult{}, err
	}

	return StatementResult{
		Text: resp.Text, ErrorName: resp.ErrorName, ErrorValue: resp.ErrorVal,
		Traceback: resp.Traceback, ExecutionCount: resp.Count, Failed: !resp.OK,
	}, nil
}

func (p *dockerSession) awaitResponse(ctx context.Context, n int) (replResponse, error) {
	path := filepath.Join(p.dir, "resp", strconv.Itoa(n)+".json")
	tick := time.NewTicker(replPollInterval)
	defer tick.Stop()

	lastCheck := time.Now()

	for {
		if data, err := os.ReadFile(path); err == nil {
			var r replResponse
			if err = json.Unmarshal(data, &r); err != nil {
				return r, fmt.Errorf("decode interpreter response: %w", err)
			}

			return r, nil
		}

		select {
		case <-ctx.Done():
			return replResponse{}, fmt.Errorf("%w: %w", errSessionDone, ctx.Err())
		case <-tick.C:
		}

		if time.Since(lastCheck) >= replLivenessEvery {
			lastCheck = time.Now()

			if err := p.checkAlive(ctx); err != nil {
				return replResponse{}, err
			}
		}
	}
}

func (p *dockerSession) checkAlive(ctx context.Context) error {
	if err := p.rt.Alive(ctx, p.containerID); err != nil {
		return fmt.Errorf("%w: %w", errReplDied, err)
	}

	fi, err := os.Stat(filepath.Join(p.dir, "heartbeat"))
	if err == nil && time.Since(fi.ModTime()) > replHeartbeatStale {
		return errReplHung
	}

	return nil
}

func (p *dockerSession) Interrupt(context.Context) error {
	n := p.cur.Load()
	if n == 0 {
		return nil
	}

	return writeAtomic(filepath.Join(p.dir, "cancel", strconv.FormatInt(n, 10)), []byte("1"))
}

func (p *dockerSession) Stop(ctx context.Context) error {
	err := p.rt.StopAndRemove(ctx, p.containerID)
	_ = os.RemoveAll(p.dir)

	if err != nil {
		return fmt.Errorf("stop session container: %w", err)
	}

	return nil
}
