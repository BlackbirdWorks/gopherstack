package kinesisanalyticsv2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

type flinkStopSave struct {
	at   time.Time
	path string
}

const (
	flinkRuntimePrefix  = "FLINK-"
	flinkImageRepo      = "flink:"
	flinkDefaultHost    = "127.0.0.1"
	flinkRESTPort       = "8081"
	flinkSavepointsDir  = "/opt/flink/savepoints"
	flinkPropertiesFile = "/etc/flink/application_properties.json"
	flinkStartTimeout   = 5 * time.Minute
	flinkPollInterval   = 2 * time.Second
	flinkStopTimeout    = 2 * time.Minute
	flinkHTTPTimeout    = 30 * time.Second
	flinkDirMode        = 0o777
	flinkFileMode       = 0o644
	flinkDefaultKey     = "test"
	operationInProgress = "IN_PROGRESS"
	operationFailed     = "FAILED"
	snapshotCreating    = "CREATING"
	snapshotReady       = "READY"
	snapshotFailed      = "FAILED"
	restoreLatest       = "RESTORE_FROM_LATEST_SNAPSHOT"
	restoreCustom       = "RESTORE_FROM_CUSTOM_SNAPSHOT"
)

var (
	errFlinkNoCode    = errors.New("application code is unavailable: only S3 and ZIPFILE Flink code can run")
	errFlinkFinished  = errors.New("flink job finished")
	errFlinkJobFailed = errors.New("flink job failed")
	errFlinkTimeout   = errors.New("flink cluster did not become ready")
	errFlinkSavepoint = errors.New("flink savepoint failed")
	errFlinkS3Missing = errors.New("emulated S3 is not wired")
	errFlinkBucketARN = errors.New("application code bucket ARN is malformed")
)

// FlinkRuntime is the slice of container.Runtime the Flink engine needs.
type FlinkRuntime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
}

// FlinkCodeFetcher returns application JAR bytes stored in emulated S3.
type FlinkCodeFetcher func(ctx context.Context, bucketARN, fileKey, version string) ([]byte, error)

// FlinkConfig configures docker-backed Flink jobs for StartApplication.
type FlinkConfig struct {
	Runtime      FlinkRuntime
	HTTP         FlinkHTTP
	Ports        *portalloc.Allocator
	Code         FlinkCodeFetcher
	Host         string
	EndpointURL  string
	Image        string
	DataDir      string
	StartTimeout time.Duration
	StopTimeout  time.Duration
	PollInterval time.Duration
}

type flinkJob struct {
	ctx         context.Context //nolint:containedctx // job-scoped context cancelled on stop
	cancel      context.CancelFunc
	done        chan struct{}
	containerID string
	base        string
	jobID       string
	stageDir    string
	opID        string
	port        int
}

type flinkEngine struct {
	jobs map[string]*flinkJob
	dirs map[string]string
	cfg  FlinkConfig
	wg   sync.WaitGroup
	owns bool
}

// flinkLaunch is the immutable input one job launch needs, copied under the backend lock.
type flinkLaunch struct {
	key           string
	region        string
	name          string
	image         string
	savepointPath string
	saveDir       string
	roleARN       string
	issuer        RoleCredentialIssuer
	props         []PropertyGroup
	code          CodeContentDescription
	checkpointMS  int64
	parallelism   int32
	allowNonRest  bool
}

// EnableFlink switches the backend to docker mode: running Flink applications execute real jobs.
func (b *InMemoryBackend) EnableFlink(cfg FlinkConfig) {
	if cfg.Host == "" {
		cfg.Host = flinkDefaultHost
	}

	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = flinkStartTimeout
	}

	if cfg.StopTimeout <= 0 {
		cfg.StopTimeout = flinkStopTimeout
	}

	if cfg.PollInterval <= 0 {
		cfg.PollInterval = flinkPollInterval
	}

	if cfg.HTTP == nil {
		cfg.HTTP = httpclient.New(flinkHTTPTimeout)
	}

	if cfg.Code == nil {
		cfg.Code = b.fetchFlinkCode
	}

	cfg.DataDir = absDataDir(cfg.DataDir)

	b.mu.Lock("EnableFlink")
	defer b.mu.Unlock()

	b.flink = &flinkEngine{
		cfg:  cfg,
		jobs: make(map[string]*flinkJob),
		dirs: make(map[string]string),
		owns: true,
	}
	b.reconcileFlinkLocked()
}

// Close stops every Flink cluster and waits for in-flight launches and stops.
func (b *InMemoryBackend) Close() {
	b.mu.Lock("Close")
	e := b.flink

	if e == nil {
		b.mu.Unlock()

		return
	}

	for key, lj := range e.jobs {
		lj.cancel()
		delete(e.jobs, key)
	}

	dirs := e.dirs
	e.dirs = make(map[string]string)
	b.mu.Unlock()

	e.wg.Wait()

	if !e.persistent() {
		for _, d := range dirs {
			_ = os.RemoveAll(d)
		}
	}

	if h, ok := e.cfg.HTTP.(interface{ CloseIdleConnections() }); ok {
		h.CloseIdleConnections()
	}

	if c, ok := e.cfg.Runtime.(io.Closer); ok && e.owns {
		_ = c.Close()
	}
}

func isFlinkRuntime(env string) bool { return strings.HasPrefix(env, flinkRuntimePrefix) }

// flinkImage maps FLINK-1_18 to flink:1.18.
func flinkImage(env string) string {
	return flinkImageRepo + strings.ReplaceAll(strings.TrimPrefix(env, flinkRuntimePrefix), "_", ".")
}

func (b *InMemoryBackend) flinkManagedLocked(key string) (*flinkJob, bool) {
	if b.flink == nil {
		return nil, false
	}

	lj, ok := b.flink.jobs[key]

	return lj, ok
}

func (e *flinkEngine) acquirePort(name string) (int, error) {
	return portalloc.AcquireFrom(e.cfg.Ports, "flink-"+name)
}

func (e *flinkEngine) rest(lj *flinkJob) flinkREST {
	return flinkREST{doer: e.cfg.HTTP, base: lj.base}
}

func (e *flinkEngine) teardown(lj *flinkJob) {
	if lj.containerID != "" {
		ctx, cancel := context.WithTimeout(context.Background(), e.cfg.StopTimeout)
		defer cancel()

		if err := e.cfg.Runtime.StopAndRemove(ctx, lj.containerID); err != nil {
			logger.Load(ctx).WarnContext(ctx, "kinesisanalyticsv2: flink container removal failed",
				"container", lj.containerID, "error", err)
		}
	}

	if lj.stageDir != "" {
		_ = os.RemoveAll(lj.stageDir)
	}

	portalloc.ReleaseTo(e.cfg.Ports, lj.port)
}

func (e *flinkEngine) baseURL(port int) string {
	return "http://" + net.JoinHostPort(e.cfg.Host, strconv.Itoa(port))
}

// pollUntil runs fn immediately and then every interval until it reports done, errors or ctx ends.
func pollUntil(ctx context.Context, interval time.Duration, fn func(context.Context) (bool, error)) error {
	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		done, err := fn(ctx)
		if err != nil {
			return err
		}

		if done {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %w", errFlinkTimeout, ctx.Err())
		case <-t.C:
		}
	}
}
