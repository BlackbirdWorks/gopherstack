package lambda

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	envDebugMode            = "LAMBDA_DEBUG_MODE"
	envDebugConfigPath      = "LAMBDA_DEBUG_MODE_CONFIG_PATH"
	envDebugFunctions       = "LAMBDA_DEBUG_MODE_FUNCTIONS"
	envDebugEnforceTimeouts = "LAMBDA_DEBUG_MODE_ENFORCE_TIMEOUTS"

	// debugInvokeTimeout replaces the function timeout while a debugger may hold the process at a breakpoint.
	debugInvokeTimeout = time.Hour

	arnResourceFunction = "function"
	maxTCPPort          = 65535
	arnPartsNoQual      = 7
	arnPartsWithQual    = 8
	debugPyMountDir     = "/opt/gopherstack-debug"
	debugPyBootstrap    = "import os\nimport debugpy\n" +
		"debugpy.listen((\"0.0.0.0\", int(os.environ[\"GOPHERSTACK_DEBUG_PORT\"])))\n"
	debugPyBootstrapFn = "sitecustomize.py"
)

// ErrInvalidDebugConfig is returned when the Lambda debug-mode configuration is malformed or inconsistent.
var ErrInvalidDebugConfig = errors.New("invalid lambda debug mode config")

type debugTarget struct {
	name            string
	qualifier       string
	port            int
	enforceTimeouts bool
}

// debugConfig maps function names to their debugger settings. An empty (or nil) config disables debug mode.
type debugConfig struct {
	targets map[string]debugTarget
}

type debugFileEntry struct {
	EnforceTimeouts *bool `yaml:"enforce-timeouts"`
	DebugPort       int   `yaml:"debug-port"`
}

type debugFile struct {
	Functions map[string]debugFileEntry `yaml:"functions"`
}

// loadDebugConfig builds the debug-mode configuration from LAMBDA_DEBUG_MODE, LAMBDA_DEBUG_MODE_CONFIG_PATH
// (YAML or JSON) and LAMBDA_DEBUG_MODE_FUNCTIONS ("name[:qualifier]=port,..."). Debug mode off yields an empty config.
func loadDebugConfig(getenv func(string) string, readFile func(string) ([]byte, error)) (*debugConfig, error) {
	if on, _ := strconv.ParseBool(getenv(envDebugMode)); !on {
		return &debugConfig{}, nil
	}

	cfg := &debugConfig{targets: map[string]debugTarget{}}
	ports := map[int]string{}

	if path := getenv(envDebugConfigPath); path != "" {
		data, err := readFile(path)
		if err != nil {
			return nil, fmt.Errorf("%w: read %s: %w", ErrInvalidDebugConfig, path, err)
		}

		if err = cfg.addFile(data, ports); err != nil {
			return nil, err
		}
	}

	if spec := getenv(envDebugFunctions); spec != "" {
		enforce := true
		if v, err := strconv.ParseBool(getenv(envDebugEnforceTimeouts)); err == nil {
			enforce = v
		}

		if err := cfg.addEnv(spec, enforce, ports); err != nil {
			return nil, err
		}
	}

	if len(cfg.targets) == 0 {
		return &debugConfig{}, fmt.Errorf(
			"%w: %s is set but no functions are configured",
			ErrInvalidDebugConfig,
			envDebugMode,
		)
	}

	return cfg, nil
}

func (c *debugConfig) addFile(data []byte, ports map[int]string) error {
	var f debugFile

	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)

	if err := dec.Decode(&f); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: %w", ErrInvalidDebugConfig, err)
	}

	keys := make([]string, 0, len(f.Functions))
	for k := range f.Functions {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for _, key := range keys {
		e := f.Functions[key]
		enforce := e.EnforceTimeouts == nil || *e.EnforceTimeouts

		if err := c.add(key, e.DebugPort, enforce, ports); err != nil {
			return err
		}
	}

	return nil
}

func (c *debugConfig) addEnv(spec string, enforce bool, ports map[int]string) error {
	for item := range strings.SplitSeq(spec, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		idx := strings.LastIndex(item, "=")
		if idx < 0 {
			return fmt.Errorf("%w: %s entry %q: want function=port", ErrInvalidDebugConfig, envDebugFunctions, item)
		}

		port, err := strconv.Atoi(strings.TrimSpace(item[idx+1:]))
		if err != nil {
			return fmt.Errorf("%w: %s entry %q: bad port", ErrInvalidDebugConfig, envDebugFunctions, item)
		}

		if err = c.add(strings.TrimSpace(item[:idx]), port, enforce, ports); err != nil {
			return err
		}
	}

	return nil
}

func (c *debugConfig) add(key string, port int, enforce bool, ports map[int]string) error {
	name, qualifier, err := parseDebugFunctionKey(key)
	if err != nil {
		return err
	}

	if port < 1 || port > maxTCPPort {
		return fmt.Errorf(
			"%w: function %q: debug-port %d out of range 1-%d",
			ErrInvalidDebugConfig,
			key,
			port,
			maxTCPPort,
		)
	}

	if _, dup := c.targets[name]; dup {
		return fmt.Errorf("%w: function %q is configured more than once (one debugger per function)",
			ErrInvalidDebugConfig, name)
	}

	if other, taken := ports[port]; taken {
		return fmt.Errorf("%w: debug-port %d is used by both %q and %q", ErrInvalidDebugConfig, port, other, name)
	}

	ports[port] = name
	c.targets[name] = debugTarget{name: name, qualifier: qualifier, port: port, enforceTimeouts: enforce}

	return nil
}

// parseDebugFunctionKey accepts "name", "name:qualifier" or a full function ARN with optional qualifier.
func parseDebugFunctionKey(key string) (string, string, error) {
	key = strings.TrimSpace(key)
	bad := func() (string, string, error) {
		return "", "", fmt.Errorf("%w: invalid function identifier %q", ErrInvalidDebugConfig, key)
	}

	if strings.HasPrefix(key, "arn:") {
		parts := strings.Split(key, ":")
		if (len(parts) != arnPartsNoQual && len(parts) != arnPartsWithQual) || parts[5] != arnResourceFunction ||
			parts[6] == "" {
			return bad()
		}

		if len(parts) == arnPartsWithQual {
			return parts[6], parts[7], nil
		}

		return parts[6], "", nil
	}

	name, qualifier, _ := strings.Cut(key, ":")
	if name == "" || strings.Contains(qualifier, ":") {
		return bad()
	}

	return name, qualifier, nil
}

func (c *debugConfig) forName(name string) (debugTarget, bool) {
	if c == nil {
		return debugTarget{}, false
	}

	t, ok := c.targets[name]

	return t, ok
}

// debugTargetFor returns the debug settings for fn when its name and version match a configured entry.
func (b *InMemoryBackend) debugTargetFor(fn *FunctionConfiguration) (debugTarget, bool) {
	t, ok := b.debug.forName(fn.FunctionName)
	if !ok {
		return debugTarget{}, false
	}

	if t.qualifier != "" {
		version := fn.Version
		if version == "" {
			version = latestVersion
		}

		if t.qualifier != version {
			return debugTarget{}, false
		}
	}

	return t, true
}

// debugInvokeTimeoutFor returns timeout, or debugInvokeTimeout when fn runs under debug with timeouts relaxed.
func (b *InMemoryBackend) debugInvokeTimeoutFor(fn *FunctionConfiguration, timeout time.Duration) time.Duration {
	if t, ok := b.debugTargetFor(fn); ok && !t.enforceTimeouts {
		return max(timeout, debugInvokeTimeout)
	}

	return timeout
}

// applyDebugEnv injects the runtime-specific debugger flags for fn into vars.
func (b *InMemoryBackend) applyDebugEnv(fn *FunctionConfiguration, vars map[string]string) {
	t, ok := b.debugTargetFor(fn)
	if !ok {
		return
	}

	port := strconv.Itoa(t.port)
	rt := fn.Runtime

	switch {
	case strings.HasPrefix(rt, "nodejs"):
		appendEnvFlag(vars, "NODE_OPTIONS", "--inspect=0.0.0.0:"+port)
	case strings.HasPrefix(rt, "java"):
		appendEnvFlag(vars, "JAVA_TOOL_OPTIONS",
			"-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:"+port)
	case strings.HasPrefix(rt, "python"):
		vars["GOPHERSTACK_DEBUG_PORT"] = port

		if prev := vars["PYTHONPATH"]; prev != "" {
			vars["PYTHONPATH"] = debugPyMountDir + ":" + prev
		} else {
			vars["PYTHONPATH"] = debugPyMountDir
		}
	}
}

func appendEnvFlag(vars map[string]string, key, flag string) {
	if prev := vars[key]; prev != "" {
		vars[key] = prev + " " + flag

		return
	}

	vars[key] = flag
}

type debugPlanKey struct{}

// debugPlan is the extra container wiring a debug-mode function needs.
type debugPlan struct {
	ports  []string
	mounts []string
}

// withDebugPlan prepares debug ports/mounts for fn and carries them on ctx; temp dirs are appended to layerDirs.
func (b *InMemoryBackend) withDebugPlan(
	ctx context.Context,
	fn *FunctionConfiguration,
	layerDirs []string,
) (context.Context, []string) {
	t, ok := b.debugTargetFor(fn)
	if !ok {
		return ctx, layerDirs
	}

	port := strconv.Itoa(t.port)
	plan := &debugPlan{ports: []string{container.PortSpec("127.0.0.1", port, port)}}

	if strings.HasPrefix(fn.Runtime, "python") {
		dir, err := os.MkdirTemp("", "gopherstack-lambda-debug-")
		if err == nil {
			err = os.WriteFile(filepath.Join(dir, debugPyBootstrapFn), []byte(debugPyBootstrap), 0o600)
		}

		if err != nil {
			logger.Load(ctx).WarnContext(ctx, "lambda: debugpy bootstrap unavailable",
				"function", fn.FunctionName, "error", err)
		} else {
			layerDirs = append(layerDirs, dir)
			plan.mounts = append(plan.mounts, dir+":"+debugPyMountDir+":ro")
		}
	}

	logger.Load(ctx).InfoContext(ctx, "lambda: debug mode enabled for function",
		"function", fn.FunctionName, "port", t.port, "enforceTimeouts", t.enforceTimeouts)

	return context.WithValue(ctx, debugPlanKey{}, plan), layerDirs
}

func applyDebugPlan(ctx context.Context, spec *container.Spec) {
	plan, ok := ctx.Value(debugPlanKey{}).(*debugPlan)
	if !ok {
		return
	}

	spec.Ports = append(spec.Ports, plan.ports...)
	spec.Mounts = append(spec.Mounts, plan.mounts...)
}
