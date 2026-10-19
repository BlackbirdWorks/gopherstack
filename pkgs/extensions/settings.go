// Package extensions runs out-of-process gopherstack extensions: executables or
// container images that register routes and hooks over a local control API.
package extensions

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// Restart policies.
const (
	RestartNever     = "never"
	RestartOnFailure = "on-failure"
	RestartAlways    = "always"
)

const (
	defaultMaxRestarts    = 5
	defaultBackoff        = time.Second
	defaultStartTimeout   = 30 * time.Second
	defaultHealthInterval = 10 * time.Second
	defaultHealthTimeout  = 2 * time.Second
	defaultHealthFailures = 3
	defaultHookTimeout    = 5 * time.Second
	imagePrefix           = "image:"
)

// Errors returned by config loading and validation.
var (
	ErrInvalidSpec   = errors.New("invalid extension spec")
	ErrInvalidConfig = errors.New("invalid extensions config")
)

// Settings is embedded in the root CLI.
type Settings struct {
	Config string   `name:"extensions-config" env:"GOPHERSTACK_EXTENSIONS_CONFIG" help:"YAML/JSON file declaring extensions (executables or container images)."`            //nolint:lll // config struct tags are intentionally verbose
	Inline []string `name:"extension"         env:"GOPHERSTACK_EXTENSIONS"        help:"Extension as name=command args, or name=image:REF (repeatable)."        sep:"none"` //nolint:lll // config struct tags are intentionally verbose
}

// Spec declares one extension: either Command (a host process) or Image (a container).
type Spec struct {
	Env            map[string]string `yaml:"env"`
	Name           string            `yaml:"name"`
	Image          string            `yaml:"image"`
	Restart        string            `yaml:"restart"`
	HealthPath     string            `yaml:"health_path"`
	ControlURL     string            `yaml:"control_url"`
	Command        []string          `yaml:"command"`
	Args           []string          `yaml:"args"`
	Port           int               `yaml:"port"`
	MaxRestarts    int               `yaml:"max_restarts"`
	RestartBackoff time.Duration     `yaml:"restart_backoff"`
	StartTimeout   time.Duration     `yaml:"start_timeout"`
	HealthInterval time.Duration     `yaml:"health_interval"`
	HealthTimeout  time.Duration     `yaml:"health_timeout"`
	HealthFailures int               `yaml:"health_failures"`
	AllowOverride  bool              `yaml:"allow_override"`
}

// Config is the extensions config file.
type Config struct {
	FailOpen    *bool         `yaml:"fail_open"`
	Extensions  []Spec        `yaml:"extensions"`
	HookTimeout time.Duration `yaml:"hook_timeout"`
}

// Enabled reports whether any extension is declared.
func (c Config) Enabled() bool { return len(c.Extensions) > 0 }

// FailOpenDefault is the fail-open default for request hooks (true unless configured).
func (c Config) FailOpenDefault() bool { return c.FailOpen == nil || *c.FailOpen }

// LoadConfig merges the config file and inline flags into one validated Config.
func LoadConfig(s Settings) (Config, error) {
	var cfg Config

	if s.Config != "" {
		raw, err := os.ReadFile(s.Config)
		if err != nil {
			return cfg, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
		}

		if err = yaml.Unmarshal(raw, &cfg); err != nil {
			return cfg, fmt.Errorf("%w: %s: %w", ErrInvalidConfig, s.Config, err)
		}
	}

	for _, in := range s.Inline {
		if strings.TrimSpace(in) == "" {
			continue
		}

		spec, err := ParseInline(in)
		if err != nil {
			return cfg, err
		}

		cfg.Extensions = append(cfg.Extensions, spec)
	}

	return cfg.normalize()
}

// ParseInline parses name=command args or name=image:REF.
func ParseInline(v string) (Spec, error) {
	name, rest, ok := strings.Cut(v, "=")
	name, rest = strings.TrimSpace(name), strings.TrimSpace(rest)

	if !ok || name == "" || rest == "" {
		return Spec{}, fmt.Errorf("%w: %q must be name=command or name=image:REF", ErrInvalidSpec, v)
	}

	if img, isImg := strings.CutPrefix(rest, imagePrefix); isImg {
		return Spec{Name: name, Image: strings.TrimSpace(img)}, nil
	}

	return Spec{Name: name, Command: strings.Fields(rest)}, nil
}

func (c Config) normalize() (Config, error) {
	if c.HookTimeout <= 0 {
		c.HookTimeout = defaultHookTimeout
	}

	seen := make(map[string]bool, len(c.Extensions))

	for i := range c.Extensions {
		s, err := c.Extensions[i].normalize()
		if err != nil {
			return c, err
		}

		key := strs.Fold(s.Name)
		if seen[key] {
			return c, fmt.Errorf("%w: duplicate name %q", ErrInvalidSpec, s.Name)
		}

		seen[key] = true
		c.Extensions[i] = s
	}

	return c, nil
}

func (s Spec) normalize() (Spec, error) {
	if !validName(s.Name) {
		return s, fmt.Errorf("%w: name %q must match [A-Za-z0-9._-]+", ErrInvalidSpec, s.Name)
	}

	if (len(s.Command) == 0) == (s.Image == "") {
		return s, fmt.Errorf("%w: %s needs exactly one of command or image", ErrInvalidSpec, s.Name)
	}

	switch s.Restart {
	case "":
		s.Restart = RestartOnFailure
	case RestartNever, RestartOnFailure, RestartAlways:
	default:
		return s, fmt.Errorf("%w: %s restart %q", ErrInvalidSpec, s.Name, s.Restart)
	}

	if s.Port < 0 || s.Port > 65535 {
		return s, fmt.Errorf("%w: %s port %d", ErrInvalidSpec, s.Name, s.Port)
	}

	if s.HealthPath != "" && !strings.HasPrefix(s.HealthPath, "/") {
		return s, fmt.Errorf("%w: %s health_path must start with /", ErrInvalidSpec, s.Name)
	}

	s.MaxRestarts = orInt(s.MaxRestarts, defaultMaxRestarts)
	s.HealthFailures = orInt(s.HealthFailures, defaultHealthFailures)
	s.RestartBackoff = orDur(s.RestartBackoff, defaultBackoff)
	s.StartTimeout = orDur(s.StartTimeout, defaultStartTimeout)
	s.HealthInterval = orDur(s.HealthInterval, defaultHealthInterval)
	s.HealthTimeout = orDur(s.HealthTimeout, defaultHealthTimeout)

	return s, nil
}

func validName(n string) bool {
	if n == "" {
		return false
	}

	for _, r := range n {
		ok := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-'
		if !ok {
			return false
		}
	}

	return true
}

func orInt(v, d int) int {
	if v <= 0 {
		return d
	}

	return v
}

func orDur(v, d time.Duration) time.Duration {
	if v <= 0 {
		return d
	}

	return v
}
