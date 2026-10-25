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
)

var (
	errLifecycleRequest    = errors.New("request failed")
	errLifecycleNotRunning = errors.New("gopherstack is not running")
	errLifecycleStartFail  = errors.New("gopherstack failed to become ready")
	errLifecycleBadEnv     = errors.New("invalid --env, want KEY=VALUE or KEY")
	errLifecycleNoRuntime  = errors.New("container runtime does not support this operation")
)

// LifecycleTarget identifies the managed instance on disk.
type LifecycleTarget struct {
	Name     string `name:"name"      env:"GOPHERSTACK_NAME"      default:"gopherstack" help:"Name of the managed instance (container name / state file)."` //nolint:lll // config struct tags are intentionally verbose
	StateDir string `name:"state-dir" env:"GOPHERSTACK_STATE_DIR" default:""            help:"Directory for pidfile, logs and state (default: $XDG_STATE_HOME/gopherstack)."` //nolint:lll // config struct tags are intentionally verbose
}

// StartCmd is `gopherstack start`.
type StartCmd struct {
	LifecycleTarget `                embed:""`
	Image           string          `                help:"Container image repository."                                                  name:"image"   default:"ghcr.io/blackbirdworks/gopherstack" env:"GOPHERSTACK_IMAGE"` //nolint:lll // config struct tags are intentionally verbose
	Tag             string          `                help:"Image tag."                                                                  name:"tag"     default:"latest" env:"GOPHERSTACK_TAG"`                                  //nolint:lll // config struct tags are intentionally verbose
	Host            bool            `                help:"Run as a background host process instead of a container."                   name:"host"`                                                                             //nolint:lll // config struct tags are intentionally verbose
	Bind            string          `                help:"Host address to bind published ports to (default loopback)."                 name:"bind"    default:"127.0.0.1"`                                                     //nolint:lll // config struct tags are intentionally verbose
	Port            string          `                help:"Host port for the main HTTP endpoint."                                       name:"port"    default:"8000" env:"PORT"`                                               //nolint:lll // config struct tags are intentionally verbose
	Publish         []string        `                help:"Extra container ports to publish, [HOSTPORT:]CONTAINERPORT (repeatable)."   name:"publish" short:"p"`                                                              //nolint:lll // config struct tags are intentionally verbose
	DataDir         string          `                help:"Host directory mounted as the data dir (default with --persist: state dir)." name:"data-dir"`                                                          //nolint:lll // config struct tags are intentionally verbose
	Persist         bool            `                help:"Enable snapshot-based persistence across restarts."                          name:"persist"`                                                                         //nolint:lll // config struct tags are intentionally verbose
	Env             []string        `                help:"Environment for the server, KEY=VALUE or KEY (copied from this shell)."     name:"env"     short:"e"`                                                              //nolint:lll // config struct tags are intentionally verbose
	Timeout         time.Duration   `                help:"How long to wait for the server to become healthy."                          name:"timeout" default:"90s"`                                                          //nolint:lll // config struct tags are intentionally verbose
}

// StopCmd is `gopherstack stop`.
type StopCmd struct {
	LifecycleTarget `embed:""`
}

// StatusCmd is `gopherstack status`.
type StatusCmd struct {
	LifecycleTarget `embed:""`
	PodTarget       `embed:""`
	JSON            bool `help:"Print machine-readable JSON." name:"json"`
}

// LogsCmd is `gopherstack logs`.
type LogsCmd struct {
	LifecycleTarget `embed:""`
	Follow          bool `help:"Stream new log output."         name:"follow" short:"f"`
	Tail            int  `help:"Number of lines from the end." name:"tail"   short:"n" default:"100"`
}

// ResetCmd is `gopherstack reset`.
type ResetCmd struct {
	PodTarget `         embed:""`
	Services  []string `         help:"Only reset these services (default: all)." name:"service"`
}

// SnapshotCmd groups `snapshot save|load|list`, which reuse the pod machinery.
type SnapshotCmd struct {
	Save PodSaveCmd `cmd:"" help:"Save current state as a new snapshot version."`
	Load PodLoadCmd `cmd:"" help:"Load a snapshot version into the running instance."`
	List PodListCmd `cmd:"" help:"List snapshots."`
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
