package helm_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChart_Lint(t *testing.T) {
	t.Parallel()
	requireDocker(t)

	tests := []struct {
		name string
		args []string
	}{
		{"defaults", nil},
		{"everything", []string{
			"--set", "persistence.enabled=true,ingress.enabled=true,dind.enabled=true,service.type=NodePort",
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			args := append([]string{"lint", "--strict", "/chart"}, tt.args...)
			out, errOut, err := helm(t.Context(), t, nil, args...)
			require.NoError(t, err, out+errOut)
		})
	}
}

func TestChart_GoldenTemplate(t *testing.T) {
	t.Parallel()
	requireDocker(t)

	tests := []struct {
		name string
		args []string
	}{
		{"default", nil},
		{"persistence_ingress", []string{
			"--set", "persistence.enabled=true,persistence.size=5Gi,persistence.storageClass=fast",
			"--set", "ingress.enabled=true,ingress.className=nginx",
			"--set", "ingress.hosts[0].host=gs.example.com,ingress.hosts[0].paths[0].path=/",
		}},
		{"nodeport_extraports", []string{
			"--set", "service.type=NodePort,service.nodePort=30080",
			"--set", "service.extraPorts[0].name=azure-blob,service.extraPorts[0].port=10000",
			"--set", "service.extraPorts[1].name=cosmos,service.extraPorts[1].port=8081",
		}},
		{
			"config",
			[]string{
				"--set",
				"config.services={s3,dynamodb},config.disableServices={sqs},config.enforceIam=true,config.region=eu-west-1",
				"--set",
				"config.extra.PORT_RANGE_START=20000,resources.limits.memory=1Gi",
				"--set",
				"nodeSelector.disk=ssd",
			},
		},
		{"dind", []string{"--set", "dind.enabled=true,config.engines=docker"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			args := append([]string{"template", "rel", "/chart", "--namespace", "ns"}, tt.args...)
			out, errOut, err := helm(t.Context(), t, nil, args...)
			require.NoError(t, err, errOut)

			golden := filepath.Join("testdata", tt.name+".golden.yaml")
			if os.Getenv(updateGolden) == "1" {
				require.NoError(t, os.WriteFile(golden, []byte(out), 0o600))
			}

			want, err := os.ReadFile(golden)
			require.NoError(t, err, "run with %s=1 to create it", updateGolden)
			assert.Equal(t, string(want), out)
		})
	}
}

func TestChart_SchemaRejects(t *testing.T) {
	t.Parallel()
	requireDocker(t)

	tests := []struct {
		name string
		set  string
		want string
	}{
		{"bad engine", "config.engines=lxc", "engines"},
		{"bad service type", "service.type=ExternalName", "type"},
		{"bad port", "service.port=70000", "port"},
		{"bad account", "config.accountId=123", "accountId"},
		{"bad pull policy", "image.pullPolicy=Sometimes", "pullPolicy"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, errOut, err := helm(t.Context(), t, nil, "template", "rel", "/chart", "--set", tt.set)
			require.Error(t, err)
			assert.Contains(t, errOut, tt.want)
		})
	}
}
