package codebuild

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// Engine modes for CodeBuild.
const (
	EngineStub   = "stub"
	EngineDocker = "docker"
)

const (
	defaultEngineImage   = "ubuntu:22.04"
	defaultBuildTimeout  = 60
	engineCleanupTimeout = 30 * time.Second
	awsStandardImagePfx  = "aws/codebuild/"
	containerSrcRoot     = "/codebuild/output/src"
	containerSrcSubdir   = "/src"
	defaultLogGroupPfx   = "/aws/codebuild/"
)

// EngineRuntime is the slice of the container runtime a docker-backed build needs.
type EngineRuntime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
	Exec(ctx context.Context, containerID string, cmd []string) (container.ExecResult, error)
}

// ObjectStore reads and writes S3 objects in the emulator.
type ObjectStore interface {
	GetObjectBytes(ctx context.Context, bucket, key string) ([]byte, error)
	PutObjectBytes(ctx context.Context, bucket, key string, data []byte) error
	// ListObjectKeys returns every object key under prefix.
	ListObjectKeys(ctx context.Context, bucket, prefix string) ([]string, error)
}

// LogSink delivers build output to CloudWatch Logs in the emulator.
type LogSink interface {
	EnsureLogGroupAndStream(ctx context.Context, region, group, stream string) error
	PutLogLines(ctx context.Context, region, group, stream string, lines []string) error
}

// RepoSource reads a CodeCommit repository tree at a branch, tag or commit ("" is the default branch).
type RepoSource interface {
	RepoFiles(ctx context.Context, region, repo, version string) (map[string][]byte, error)
}

// ValueResolver resolves PARAMETER_STORE and SECRETS_MANAGER environment values.
type ValueResolver interface {
	Parameter(ctx context.Context, region, name string) (string, error)
	SecretValue(ctx context.Context, region, secretID, versionStage, versionID string) (string, error)
}

// EngineServices are the emulator backends a docker build reads from and writes to.
type EngineServices struct {
	Store  ObjectStore
	Logs   LogSink
	Repos  RepoSource
	Values ValueResolver
}

// EngineConfig configures docker-backed builds.
type EngineConfig struct {
	Runtime EngineRuntime
	// DefaultImage replaces aws/codebuild/* curated images; default ubuntu:22.04.
	DefaultImage string
	// WorkDir is the host directory build workspaces are created under; default is the OS temp dir.
	WorkDir string
	// CacheDir holds LOCAL caches between builds of a project; default is under the OS temp dir.
	CacheDir string
	// AllowFileGitSources lets git sources use file:// locations (host paths); off by default.
	AllowFileGitSources bool
	// TimeoutUnit scales timeoutInMinutes; default one minute.
	TimeoutUnit time.Duration
}

// buildRun tracks one executing build.
type buildRun struct {
	cancel context.CancelFunc
}

// engineState is shared by a home backend and its regional siblings.
type engineState struct {
	mu       *lockmetrics.RWMutex
	services EngineServices
	cfg      EngineConfig
	wg       sync.WaitGroup
}

func newEngineState(cfg EngineConfig) *engineState {
	if cfg.DefaultImage == "" {
		cfg.DefaultImage = defaultEngineImage
	}

	if cfg.CacheDir == "" {
		cfg.CacheDir = filepath.Join(os.TempDir(), "gopherstack-codebuild-cache")
	}

	if cfg.TimeoutUnit <= 0 {
		cfg.TimeoutUnit = time.Minute
	}

	return &engineState{mu: lockmetrics.New("codebuild.engine"), cfg: cfg}
}

func (e *engineState) svc() EngineServices {
	e.mu.RLock("svc")
	defer e.mu.RUnlock()

	return e.services
}

// EnableEngine switches the backend to docker mode: StartBuild runs the buildspec in a container.
func (b *InMemoryBackend) EnableEngine(cfg EngineConfig) {
	b.mu.Lock("EnableEngine")
	defer b.mu.Unlock()

	b.engine = newEngineState(cfg)
}

// SetEngineServices wires the emulator backends used by docker builds; a no-op without an engine.
func (b *InMemoryBackend) SetEngineServices(s EngineServices) {
	b.mu.RLock("SetEngineServices")
	eng := b.engine
	b.mu.RUnlock()

	if eng == nil {
		return
	}

	eng.mu.Lock("SetEngineServices")
	eng.services = s
	eng.mu.Unlock()
}

// EngineEnabled reports whether builds run in docker.
func (b *InMemoryBackend) EngineEnabled() bool {
	b.mu.RLock("EngineEnabled")
	defer b.mu.RUnlock()

	return b.engine != nil
}

// SetEngineServices wires the services shared by the home backend and its regional siblings.
func (h *Handler) SetEngineServices(s EngineServices) {
	h.Backend.SetEngineServices(s)
}

// resolveImage maps CodeBuild curated images onto the configured local default.
func resolveImage(image, def string) string {
	if image == "" || strings.HasPrefix(image, awsStandardImagePfx) {
		return def
	}

	return image
}

// shareEngine returns the engine state a regional sibling shares with this backend.
func (b *InMemoryBackend) shareEngine() *engineState {
	b.mu.RLock("shareEngine")
	defer b.mu.RUnlock()

	return b.engine
}
