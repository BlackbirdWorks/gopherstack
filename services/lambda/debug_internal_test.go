package lambda

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

var errBoom = errors.New("boom")

func debugFn(name, rt, version string) *FunctionConfiguration {
	return &FunctionConfiguration{FunctionName: name, Runtime: rt, Version: version}
}

func debugEnv(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func debugFileReader(content string) func(string) ([]byte, error) {
	return func(string) ([]byte, error) { return []byte(content), nil }
}

func TestLoadDebugConfig(t *testing.T) {
	t.Parallel()

	const arn = "arn:aws:lambda:us-east-1:000000000000:function:"

	tests := []struct {
		env     map[string]string
		want    map[string]debugTarget
		name    string
		file    string
		wantErr string
	}{
		{name: "off", env: map[string]string{}, want: map[string]debugTarget{}},
		{
			name: "yaml arn and name",
			env:  map[string]string{envDebugMode: "1", envDebugConfigPath: "/c.yaml"},
			file: "functions:\n  " + arn + "f1:\n    debug-port: 5678\n    enforce-timeouts: false\n" +
				"  f2:v1:\n    debug-port: 5679\n",
			want: map[string]debugTarget{
				"f1": {name: "f1", port: 5678},
				"f2": {name: "f2", qualifier: "v1", port: 5679, enforceTimeouts: true},
			},
		},
		{
			name: "json arn qualifier",
			env:  map[string]string{envDebugMode: "true", envDebugConfigPath: "/c.json"},
			file: `{"functions":{"` + arn + `f1:3":{"debug-port":9000}}}`,
			want: map[string]debugTarget{"f1": {name: "f1", qualifier: "3", port: 9000, enforceTimeouts: true}},
		},
		{
			name: "env form",
			env: map[string]string{
				envDebugMode: "1", envDebugFunctions: "a=1111, b:$LATEST=2222", envDebugEnforceTimeouts: "false",
			},
			want: map[string]debugTarget{
				"a": {name: "a", port: 1111},
				"b": {name: "b", qualifier: "$LATEST", port: 2222},
			},
		},
		{
			name:    "port collision",
			env:     map[string]string{envDebugMode: "1", envDebugFunctions: "a=1111,b=1111"},
			wantErr: "debug-port 1111 is used by both",
		},
		{
			name:    "duplicate function",
			env:     map[string]string{envDebugMode: "1", envDebugFunctions: "a=1111,a:2=2222"},
			wantErr: "more than once",
		},
		{
			name:    "bad port",
			env:     map[string]string{envDebugMode: "1", envDebugFunctions: "a=70000"},
			wantErr: "out of range",
		},
		{
			name:    "bad arn",
			env:     map[string]string{envDebugMode: "1", envDebugFunctions: "arn:aws:s3:::x=1"},
			wantErr: "invalid function",
		},
		{name: "nothing configured", env: map[string]string{envDebugMode: "1"}, wantErr: "no functions"},
		{
			name:    "unknown yaml key",
			env:     map[string]string{envDebugMode: "1", envDebugConfigPath: "/c"},
			file:    "functions:\n  f:\n    port: 1\n",
			wantErr: "field port not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := LoadDebugConfig(debugEnv(tt.env), debugFileReader(tt.file))
			if tt.wantErr != "" {
				require.Error(t, err)
				require.ErrorIs(t, err, ErrInvalidDebugConfig)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Len(t, cfg.targets, len(tt.want))

			for k, want := range tt.want {
				assert.Equal(t, want, cfg.targets[k])
			}
		})
	}
}

func TestLoadDebugConfigReadError(t *testing.T) {
	t.Parallel()

	_, err := LoadDebugConfig(
		debugEnv(map[string]string{envDebugMode: "1", envDebugConfigPath: "/x"}),
		func(string) ([]byte, error) { return nil, errBoom },
	)
	require.ErrorIs(t, err, ErrInvalidDebugConfig)
}

func TestDebugEnvAndTimeout(t *testing.T) {
	t.Parallel()

	cfg := &debugConfig{targets: map[string]debugTarget{
		"fn":  {name: "fn", port: 5678},
		"pin": {name: "pin", qualifier: "2", port: 5679, enforceTimeouts: true},
	}}
	b := &InMemoryBackend{debug: cfg}

	tests := []struct {
		name        string
		fn          *FunctionConfiguration
		vars        map[string]string
		wantKey     string
		wantVal     string
		wantTimeout time.Duration
	}{
		{
			name:        "node",
			fn:          debugFn("fn", "nodejs22.x", ""),
			vars:        map[string]string{},
			wantKey:     "NODE_OPTIONS",
			wantVal:     "--inspect=0.0.0.0:5678",
			wantTimeout: debugInvokeTimeout,
		},
		{
			name:        "node appends",
			fn:          debugFn("fn", "nodejs22.x", ""),
			vars:        map[string]string{"NODE_OPTIONS": "--max-old-space-size=64"},
			wantKey:     "NODE_OPTIONS",
			wantVal:     "--max-old-space-size=64 --inspect=0.0.0.0:5678",
			wantTimeout: debugInvokeTimeout,
		},
		{
			name: "java", fn: debugFn("fn", "java21", ""), vars: map[string]string{},
			wantKey:     "JAVA_TOOL_OPTIONS",
			wantVal:     "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5678",
			wantTimeout: debugInvokeTimeout,
		},
		{
			name:        "python",
			fn:          debugFn("fn", "python3.13", ""),
			vars:        map[string]string{},
			wantKey:     "PYTHONPATH",
			wantVal:     debugPyMountDir,
			wantTimeout: debugInvokeTimeout,
		},
		{
			name:        "qualifier mismatch",
			fn:          debugFn("pin", "nodejs22.x", "1"),
			vars:        map[string]string{},
			wantKey:     "NODE_OPTIONS",
			wantVal:     "",
			wantTimeout: time.Second,
		},
		{
			name:        "qualifier match keeps timeout",
			fn:          debugFn("pin", "nodejs22.x", "2"),
			vars:        map[string]string{},
			wantKey:     "NODE_OPTIONS",
			wantVal:     "--inspect=0.0.0.0:5679",
			wantTimeout: time.Second,
		},
		{
			name: "other function", fn: debugFn("x", "nodejs22.x", ""),
			vars: map[string]string{}, wantKey: "NODE_OPTIONS", wantVal: "", wantTimeout: time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b.applyDebugEnv(tt.fn, tt.vars)
			assert.Equal(t, tt.wantVal, tt.vars[tt.wantKey])
			assert.Equal(t, tt.wantTimeout, b.debugInvokeTimeoutFor(tt.fn, time.Second))
		})
	}
}

func TestDebugPlanPortsAndMounts(t *testing.T) {
	t.Parallel()

	b := &InMemoryBackend{debug: &debugConfig{targets: map[string]debugTarget{
		"py": {name: "py", port: 5680},
		"go": {name: "go", port: 5681},
	}}}

	tests := []struct {
		name      string
		fn        *FunctionConfiguration
		wantPort  string
		wantMount bool
	}{
		{
			name:      "python mounts bootstrap",
			fn:        debugFn("py", "python3.13", ""),
			wantPort:  "127.0.0.1:5680:5680",
			wantMount: true,
		},
		{
			name:     "go publishes port only",
			fn:       debugFn("go", "provided.al2023", ""),
			wantPort: "127.0.0.1:5681:5681",
		},
		{name: "unconfigured", fn: debugFn("none", "provided.al2023", "")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, dirs := b.withDebugPlan(context.Background(), tt.fn, nil)
			t.Cleanup(func() {
				for _, d := range dirs {
					_ = os.RemoveAll(d)
				}
			})

			spec := container.Spec{}
			applyDebugPlan(ctx, &spec)

			if tt.wantPort == "" {
				assert.Empty(t, spec.Ports)
				assert.Empty(t, dirs)

				return
			}

			assert.Equal(t, []string{tt.wantPort}, spec.Ports)
			assert.Equal(t, tt.wantMount, len(spec.Mounts) == 1)
			assert.Equal(t, tt.wantMount, len(dirs) == 1)
		})
	}
}
