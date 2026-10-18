package app

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

var errNoRuntime = errors.New("no runtime")

func TestResolveEngines(t *testing.T) {
	t.Parallel()

	tests := []struct {
		env       map[string]string
		name      string
		global    string
		ecs       string
		wantRDS   string
		wantEC    string
		wantCB    string
		wantECS   string
		wantRT    string
		probeRT   container.RuntimeName
		rdsSet    string
		args      []string
		probeFail bool
	}{
		{name: "stub_default", global: "stub", wantRDS: "stub", wantEC: "embedded", wantCB: "stub"},
		{name: "auto_no_runtime", global: "auto", probeFail: true, wantRDS: "stub", wantEC: "embedded", wantCB: "stub"},
		{
			name: "auto_docker", global: "auto", probeRT: container.RuntimeDocker,
			wantRDS: "docker", wantEC: "docker", wantCB: "docker", wantECS: "docker",
		},
		{
			name: "auto_podman_sets_runtime", global: "auto", probeRT: container.RuntimePodman,
			wantRDS: "docker", wantEC: "docker", wantCB: "docker", wantECS: "docker", wantRT: "podman",
		},
		{
			name: "forced_docker_skips_probe", global: "docker", probeFail: true,
			wantRDS: "docker", wantEC: "docker", wantCB: "docker", wantECS: "docker",
		},
		{
			name: "env_override_wins", global: "docker", env: map[string]string{"RDS_ENGINE": "stub"},
			wantRDS: "stub", wantEC: "docker", wantCB: "docker", wantECS: "docker",
		},
		{
			name: "flag_override_wins", global: "docker", args: []string{"--rds-engine=stub"},
			wantRDS: "stub", wantEC: "docker", wantCB: "docker", wantECS: "docker",
		},
		{
			name: "non_default_value_wins", global: "docker", rdsSet: "docker",
			wantRDS: "docker", wantEC: "docker", wantCB: "docker", wantECS: "docker",
		},
		{
			name: "ecs_flag_wins", global: "docker", ecs: "none",
			wantRDS: "docker", wantEC: "docker", wantCB: "docker", wantECS: "none",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := map[string]string{}
			maps.Copy(env, tt.env)

			cli := &CLI{
				RDSEngine:         "stub",
				ElastiCacheEngine: "embedded",
			}
			cli.EngineSwitch.Engines = tt.global
			cli.EngineSwitch.ECSRuntime = tt.ecs

			if tt.rdsSet != "" {
				cli.RDSEngine = tt.rdsSet
			}

			cli.CodeBuild.Engine = "stub"

			d := engineDeps{
				probe: func(context.Context) (container.RuntimeName, error) {
					if tt.probeFail {
						return "", errNoRuntime
					}

					return tt.probeRT, nil
				},
				getenv: func(k string) string { return env[k] },
				setenv: func(k, v string) { env[k] = v },
				args:   tt.args,
			}

			origRDS := cli.RDSEngine

			cli.resolveEngines(context.Background(), slog.Default(), d)

			assert.Equal(t, tt.wantRDS, cli.GetRDSEngine())
			assert.Equal(t, tt.wantEC, cli.GetElastiCacheEngine())
			assert.Equal(t, tt.wantCB, cli.GetCodeBuildSettings().Engine)
			assert.Equal(t, tt.wantECS, env["GOPHERSTACK_ECS_RUNTIME"])
			assert.Equal(t, tt.wantRT, env["CONTAINER_RUNTIME"])
			assert.Equal(t, origRDS, cli.RDSEngine, "persisted field untouched")
		})
	}
}
