package app

import (
	"errors"
	"os"
	"time"
)

const (
	lifecycleDefaultName    = "gopherstack"
	podDefaultEndpoint      = "http://localhost:8000"
	lifecycleDefaultImage   = "ghcr.io/blackbirdworks/gopherstack"
	lifecycleContainerPort  = "8000"
	lifecycleContainerData  = "/data"
	lifecycleLoopback       = "127.0.0.1"
	lifecycleCmdLogs        = "logs"
	lifecycleCmdStatus      = "status"
	lifecycleReadyTimeout   = 90 * time.Second
	lifecycleReadyPoll      = 500 * time.Millisecond
	lifecycleStopTimeout    = 15 * time.Second
	lifecycleHTTPTimeout    = 30 * time.Second
	lifecycleLogTailOnError = "40"
	lifecycleModeContainer  = "container"
	lifecycleModeHost       = "host"
	lifecycleStateDirPerm   = 0o750
	lifecycleStateFilePerm  = 0o600
	lifecycleDefaultTail    = 100
	lifecycleLogTailMaxByte = 1 << 20
	lifecycleSocketInside   = "/var/run/docker.sock"
	lifecycleSchemeHTTPS    = "https"
	lifecycleStopGrace      = 2 * lifecycleStopTimeout
	localstackAvailable     = "available"
)

var (
	errLifecycleRequest    = errors.New("request failed")
	errLifecycleNotRunning = errors.New("gopherstack is not running")
	errLifecycleStartFail  = errors.New("gopherstack failed to become ready")
	errLifecycleBadEnv     = errors.New("invalid --env, want KEY=VALUE or KEY")
	errLifecycleNoRuntime  = errors.New("container runtime does not support this operation")
	errLifecycleHostOnly   = errors.New("--foreground and --docker-socket apply to container mode, not --host")
	errLifecycleSocketPath = errors.New("--docker-socket-path requires --docker-socket")
)

// LifecycleTarget identifies the managed instance on disk.
type LifecycleTarget struct {
	Name     string `name:"name"      env:"GOPHERSTACK_NAME"      default:"gopherstack" help:"Name of the managed instance (container name / state file)."`                   //nolint:lll // config struct tags are intentionally verbose
	StateDir string `name:"state-dir" env:"GOPHERSTACK_STATE_DIR" default:""            help:"Directory for pidfile, logs and state (default: $XDG_STATE_HOME/gopherstack)."` //nolint:lll // config struct tags are intentionally verbose
}

// StartCmd is `gopherstack start`.
type StartCmd struct {
	LifecycleTarget  `embed:""`
	Image            string        `         name:"image"              env:"GOPHERSTACK_IMAGE" default:"ghcr.io/blackbirdworks/gopherstack" help:"Container image repository."`                                                                                                                                                                                                //nolint:lll // config struct tags are intentionally verbose
	Tag              string        `         name:"tag"                env:"GOPHERSTACK_TAG"   default:"latest"                             help:"Image tag."`                                                                                                                                                                                                                 //nolint:lll // config struct tags are intentionally verbose
	Bind             string        `         name:"bind"                                       default:"127.0.0.1"                          help:"Host address to bind published ports to (default loopback)."`                                                                                                                                                                //nolint:lll // config struct tags are intentionally verbose
	Port             string        `         name:"port"               env:"PORT"              default:"8000"                               help:"Host port for the main HTTP endpoint."`                                                                                                                                                                                      //nolint:lll // config struct tags are intentionally verbose
	DataDir          string        `         name:"data-dir"                                                                                help:"Host directory mounted as the data dir (default with --persist: state dir)."`                                                                                                                                                //nolint:lll // config struct tags are intentionally verbose
	DockerSocketPath string        `         name:"docker-socket-path"                                                                      help:"Host Docker socket to mount with --docker-socket (default: unix DOCKER_HOST, else /var/run/docker.sock)."`                                                                                                                   //nolint:lll // config struct tags are intentionally verbose
	Publish          []string      `         name:"publish"                                                                                 help:"Extra container ports to publish, [HOSTPORT:]CONTAINERPORT (repeatable)."                                                                                                                                         short:"p"` //nolint:lll // config struct tags are intentionally verbose
	Env              []string      `         name:"env"                                                                                     help:"Environment for the server, KEY=VALUE or KEY (copied from this shell)."                                                                                                                                           short:"e"` //nolint:lll // config struct tags are intentionally verbose
	Timeout          time.Duration `         name:"timeout"                                    default:"90s"                                help:"How long to wait for the server to become healthy."`                                                                                                                                                                         //nolint:lll // config struct tags are intentionally verbose
	Host             bool          `         name:"host"                                                                                    help:"Run as a background host process instead of a container."`                                                                                                                                                                   //nolint:lll // config struct tags are intentionally verbose
	Foreground       bool          `         name:"foreground"                                                                              help:"Attach: stream container logs, stop on SIGINT/SIGTERM, exit with the container's exit code (container mode only)."`                                                                                                          //nolint:lll // config struct tags are intentionally verbose
	DockerSocket     bool          `         name:"docker-socket"                                                                           help:"SECURITY: mount the host Docker socket into the container for engine features (Lambda, ECS, RDS, ...). Grants the server root-equivalent control of the host Docker daemon; off by default. Container mode only."`           //nolint:lll // config struct tags are intentionally verbose
	Persist          bool          `         name:"persist"                                                                                 help:"Enable snapshot-based persistence across restarts."`                                                                                                                                                                         //nolint:lll // config struct tags are intentionally verbose
}

// StopCmd is `gopherstack stop`.
type StopCmd struct {
	LifecycleTarget `embed:""`
}

// StatusCmd is `gopherstack status`.
type StatusCmd struct {
	LifecycleTarget `     embed:""`
	PodTarget       `     embed:""`
	JSON            bool `         help:"Print machine-readable JSON." name:"json"`
}

// LogsCmd is `gopherstack logs`.
type LogsCmd struct {
	LifecycleTarget `     embed:""`
	Follow          bool `         help:"Stream new log output."        name:"follow" short:"f"`
	Tail            int  `         help:"Number of lines from the end." name:"tail"   short:"n" default:"100"`
}

// ResetCmd is `gopherstack reset`.
type ResetCmd struct {
	PodTarget `         embed:""`
	Services  []string `         help:"Only reset these services (default: all)." name:"service"`
}

// SnapshotCmd groups `snapshot save|load|list`, which reuse the pod machinery.
type SnapshotCmd struct {
	List PodListCmd `cmd:"" help:"List snapshots."`
	Save PodSaveCmd `cmd:"" help:"Save current state as a new snapshot version."`
	Load PodLoadCmd `cmd:"" help:"Load a snapshot version into the running instance."`
}

func (t LifecycleTarget) name() string {
	if t.Name == "" {
		return lifecycleDefaultName
	}

	return t.Name
}

func (t LifecycleTarget) stateDir(getenv func(string) string, home func() (string, error)) string {
	if t.StateDir != "" {
		return t.StateDir
	}

	if x := getenv("XDG_STATE_HOME"); x != "" {
		return x + "/gopherstack"
	}

	h, err := home()
	if err != nil || h == "" {
		return os.TempDir() + "/gopherstack"
	}

	return h + "/.local/state/gopherstack"
}

func endpointFor(bind, port string) string {
	if bind == "" || bind == "0.0.0.0" || bind == "::" {
		bind = lifecycleLoopback
	}

	return "http://" + bind + ":" + port
}
