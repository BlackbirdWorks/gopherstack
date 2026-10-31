package lambda

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

func TestSplitDockerFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    []string
		wantErr bool
	}{
		{name: "empty", in: "", want: nil},
		{name: "spaces", in: "  -e   A=1\t--dns 1.1.1.1 ", want: []string{"-e", "A=1", "--dns", "1.1.1.1"}},
		{name: "double_quotes", in: `-e "A=hello world"`, want: []string{"-e", "A=hello world"}},
		{name: "single_quotes_no_expansion", in: `-e 'A=$HOME \n'`, want: []string{"-e", `A=$HOME \n`}},
		{name: "no_shell_eval", in: `-e A=$(id) -e B=` + "`id`", want: []string{"-e", "A=$(id)", "-e", "B=`id`"}},
		{name: "escape", in: `-e A=a\ b`, want: []string{"-e", "A=a b"}},
		{name: "unterminated", in: `-e "A=1`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := splitDockerFlags(tt.in)
			if tt.wantErr {
				require.ErrorIs(t, err, errInvalidDockerFlags)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseDockerFlags(t *testing.T) {
	t.Setenv("LAMBDA_FLAGS_INHERIT", "from-host")

	tests := []struct {
		name     string
		in       string
		want     dockerFlags
		warnings int
	}{
		{name: "empty"},
		{name: "env_forms", in: "-e A=1 --env B=2 --env=C=3 -eD=4", want: dockerFlags{Env: []string{"A=1", "B=2", "C=3", "D=4"}}},
		{name: "env_inherit", in: "-e LAMBDA_FLAGS_INHERIT", want: dockerFlags{Env: []string{"LAMBDA_FLAGS_INHERIT=from-host"}}},
		{name: "env_inherit_unset", in: "-e LAMBDA_FLAGS_SURELY_UNSET"},
		{name: "env_reserved", in: "-e AWS_LAMBDA_RUNTIME_API=evil:1", warnings: 1},
		{name: "volume", in: "-v /host/data:/data:ro --volume=named:/n", want: dockerFlags{Mounts: []string{"/host/data:/data:ro", "named:/n"}}},
		{name: "volume_reserved_target", in: "-v /x:/var/task -v /y:/opt", warnings: 2},
		{name: "volume_docker_sock", in: "-v /var/run/docker.sock:/s.sock", warnings: 1},
		{name: "volume_relative", in: "-v ./rel:/data", warnings: 1},
		{name: "volume_propagation", in: "-v /a:/b:rshared", warnings: 1},
		{name: "volume_bad_shape", in: "-v /only", warnings: 1},
		{name: "add_host", in: "--add-host db.local:10.0.0.5", want: dockerFlags{ExtraHosts: []string{"db.local:10.0.0.5"}}},
		{name: "add_host_bad", in: "--add-host nocolon", warnings: 1},
		{name: "dns", in: "--dns 8.8.8.8 --dns=1.1.1.1", want: dockerFlags{DNS: []string{"8.8.8.8", "1.1.1.1"}}},
		{name: "dns_bad", in: "--dns not-an-ip", warnings: 1},
		{name: "network", in: "--network my-net", want: dockerFlags{Network: "my-net"}},
		{name: "network_host", in: "--network host --net=container:abc", warnings: 2},
		{name: "memory", in: "-m 512m", want: dockerFlags{MemoryBytes: 512 << 20}},
		{name: "memory_g", in: "--memory=1g", want: dockerFlags{MemoryBytes: 1 << 30}},
		{name: "memory_too_small", in: "-m 1k", warnings: 1},
		{name: "cpus", in: "--cpus 1.5", want: dockerFlags{NanoCPUs: 1_500_000_000}},
		{name: "cpus_bad", in: "--cpus -1", warnings: 1},
		{name: "platform", in: "--platform linux/arm64", want: dockerFlags{Platform: "linux/arm64"}},
		{name: "platform_bad", in: "--platform nonsense", warnings: 1},
		{name: "privileged", in: "--privileged", warnings: 1},
		{name: "dangerous_set", in: "--pid=host --ipc host --cap-add SYS_ADMIN --device /dev/sda --user root --security-opt seccomp=unconfined", warnings: 6},
		{name: "mixed", in: "--privileged -e KEEP=1", want: dockerFlags{Env: []string{"KEEP=1"}}, warnings: 1},
		{name: "missing_value", in: "-e", warnings: 1},
		{name: "stray_arg", in: "bash -c id", warnings: 2},
		{name: "unterminated_quote", in: `-e "A=1`, warnings: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, warnings := parseDockerFlags(tt.in)
			assert.Equal(t, tt.want, got)
			assert.Len(t, warnings, tt.warnings, "%v", warnings)
		})
	}
}

func TestDockerFlagsApply(t *testing.T) {
	t.Parallel()

	spec := container.Spec{Env: []string{"A=base"}, Mounts: []string{"/code:/var/task:ro"}}

	flags, warnings := parseDockerFlags(
		"-e A=override -v /h:/data --add-host x:1.2.3.4 --dns 9.9.9.9 --network n -m 256m --cpus 2 --platform linux/amd64",
	)
	require.Empty(t, warnings)
	flags.apply(&spec)

	assert.Equal(t, []string{"A=base", "A=override"}, spec.Env)
	assert.Equal(t, []string{"/code:/var/task:ro", "/h:/data"}, spec.Mounts)
	assert.Equal(t, []string{"x:1.2.3.4"}, spec.ExtraHosts)
	assert.Equal(t, []string{"9.9.9.9"}, spec.DNS)
	assert.Equal(t, "n", spec.Network)
	assert.EqualValues(t, 256<<20, spec.MemoryBytes)
	assert.EqualValues(t, 2_000_000_000, spec.NanoCPUs)
	assert.Equal(t, "linux/amd64", spec.Platform)
	assert.False(t, spec.Privileged)
}

func TestRuntimeIdentifiers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		filter  string
		has     string
		hasNot  string
		wantErr bool
	}{
		{name: "default", filter: "", has: "python3.12", hasNot: "python3.7"},
		{name: "supported", filter: "supported", has: "provided.al2023", hasNot: "nodejs12.x"},
		{name: "deprecated", filter: "deprecated", has: "python3.7", hasNot: "python3.12"},
		{name: "all", filter: "all", has: "python3.7"},
		{name: "invalid", filter: "bogus", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := RuntimeIdentifiers(tt.filter)
			if tt.wantErr {
				require.ErrorIs(t, err, ErrInvalidRuntimeFilter)

				return
			}

			require.NoError(t, err)
			assert.Contains(t, got, tt.has)

			if tt.hasNot != "" {
				assert.NotContains(t, got, tt.hasNot)
			}

			assert.IsIncreasing(t, got)
		})
	}

	all, _ := RuntimeIdentifiers("all")
	assert.Contains(t, all, "python3.12")
}
