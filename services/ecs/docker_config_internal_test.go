package ecs

import (
	"testing"
	"time"

	"github.com/moby/moby/api/types/mount"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dockertypes "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
)

func TestBuildDockerConfigs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, cfg *dockertypes.Config, host *dockertypes.HostConfig)
		name  string
		cd    ContainerDefinition
	}{
		{
			name: "command_entrypoint_user_workdir",
			cd: ContainerDefinition{
				Command: []string{"sh", "-c", "echo hi"}, EntryPoint: []string{"/bin/entry"},
				User: "1000:1000", WorkingDirectory: "/srv", Hostname: "web",
			},
			check: func(t *testing.T, cfg *dockertypes.Config, _ *dockertypes.HostConfig) {
				t.Helper()

				assert.Equal(t, []string{"sh", "-c", "echo hi"}, cfg.Cmd)
				assert.Equal(t, []string{"/bin/entry"}, cfg.Entrypoint)
				assert.Equal(t, "1000:1000", cfg.User)
				assert.Equal(t, "/srv", cfg.WorkingDir)
				assert.Equal(t, "web", cfg.Hostname)
			},
		},
		{
			name: "cpu_memory_limits",
			cd:   ContainerDefinition{CPU: 512, Memory: 256, MemoryReservation: 128},
			check: func(t *testing.T, _ *dockertypes.Config, host *dockertypes.HostConfig) {
				t.Helper()

				assert.EqualValues(t, 512, host.CPUShares)
				assert.EqualValues(t, 256<<20, host.Memory)
				assert.EqualValues(t, 128<<20, host.MemoryReservation)
			},
		},
		{
			name: "ulimits",
			cd:   ContainerDefinition{Ulimits: []Ulimit{{Name: "nofile", SoftLimit: 1024, HardLimit: 4096}}},
			check: func(t *testing.T, _ *dockertypes.Config, host *dockertypes.HostConfig) {
				t.Helper()

				require.Len(t, host.Ulimits, 1)
				assert.Equal(t, "nofile", host.Ulimits[0].Name)
				assert.EqualValues(t, 1024, host.Ulimits[0].Soft)
				assert.EqualValues(t, 4096, host.Ulimits[0].Hard)
			},
		},
		{
			name: "linux_parameters",
			cd: ContainerDefinition{
				Memory: 100,
				LinuxParameters: &LinuxParameters{
					Capabilities:       &KernelCapabilities{Add: []string{"SYS_PTRACE"}, Drop: []string{"NET_RAW"}},
					InitProcessEnabled: new(true),
					SharedMemorySize:   new(64),
					MaxSwap:            new(50),
					Swappiness:         new(10),
					Devices:            []Device{{HostPath: "/dev/fuse", Permissions: []string{"read", "mknod"}}},
					Tmpfs:              []Tmpfs{{ContainerPath: "/run", Size: 16, MountOptions: []string{"noexec"}}},
				},
			},
			check: func(t *testing.T, _ *dockertypes.Config, host *dockertypes.HostConfig) {
				t.Helper()

				assert.Equal(t, []string{"SYS_PTRACE"}, host.CapAdd)
				assert.Equal(t, []string{"NET_RAW"}, host.CapDrop)
				require.NotNil(t, host.Init)
				assert.True(t, *host.Init)
				assert.EqualValues(t, 64<<20, host.ShmSize)
				assert.EqualValues(t, 150<<20, host.MemorySwap)
				require.NotNil(t, host.MemorySwappiness)
				assert.EqualValues(t, 10, *host.MemorySwappiness)
				require.Len(t, host.Devices, 1)
				assert.Equal(t, "/dev/fuse", host.Devices[0].PathInContainer)
				assert.Equal(t, "rm", host.Devices[0].CgroupPermissions)
				assert.Equal(t, map[string]string{"/run": "size=16m,noexec"}, host.Tmpfs)
			},
		},
		{
			name: "security_and_network",
			cd: ContainerDefinition{
				Privileged: true, ReadonlyRootFilesystem: true, DisableNetworking: true,
				DNSServers: []string{"1.1.1.1"}, DNSSearchDomains: []string{"corp.local"},
				ExtraHosts:            []HostEntry{{Hostname: "db", IPAddress: "10.0.0.5"}},
				SystemControls:        []SystemControl{{Namespace: "net.core.somaxconn", Value: "1024"}},
				DockerSecurityOptions: []string{"no-new-privileges"},
			},
			check: func(t *testing.T, cfg *dockertypes.Config, host *dockertypes.HostConfig) {
				t.Helper()

				assert.True(t, host.Privileged)
				assert.True(t, host.ReadonlyRootfs)
				assert.True(t, cfg.NetworkDisabled)
				require.Len(t, host.DNS, 1)
				assert.Equal(t, "1.1.1.1", host.DNS[0].String())
				assert.Equal(t, []string{"corp.local"}, host.DNSSearch)
				assert.Equal(t, []string{"db:10.0.0.5"}, host.ExtraHosts)
				assert.Equal(t, map[string]string{"net.core.somaxconn": "1024"}, host.Sysctls)
				assert.Equal(t, []string{"no-new-privileges"}, host.SecurityOpt)
			},
		},
		{
			name: "stop_timeout_default",
			cd:   ContainerDefinition{},
			check: func(t *testing.T, cfg *dockertypes.Config, _ *dockertypes.HostConfig) {
				t.Helper()

				require.NotNil(t, cfg.StopTimeout)
				assert.Equal(t, 30, *cfg.StopTimeout)
			},
		},
		{
			name: "stop_timeout_explicit",
			cd:   ContainerDefinition{StopTimeout: new(5)},
			check: func(t *testing.T, cfg *dockertypes.Config, _ *dockertypes.HostConfig) {
				t.Helper()

				require.NotNil(t, cfg.StopTimeout)
				assert.Equal(t, 5, *cfg.StopTimeout)
			},
		},
		{
			name: "healthcheck_defaults",
			cd:   ContainerDefinition{HealthCheck: &HealthCheck{Command: []string{"CMD-SHELL", "true"}}},
			check: func(t *testing.T, cfg *dockertypes.Config, _ *dockertypes.HostConfig) {
				t.Helper()

				require.NotNil(t, cfg.Healthcheck)
				assert.Equal(t, []string{"CMD-SHELL", "true"}, cfg.Healthcheck.Test)
				assert.Equal(t, 30*time.Second, cfg.Healthcheck.Interval)
				assert.Equal(t, 5*time.Second, cfg.Healthcheck.Timeout)
				assert.Equal(t, 3, cfg.Healthcheck.Retries)
				assert.Zero(t, cfg.Healthcheck.StartPeriod)
			},
		},
		{
			name: "healthcheck_explicit",
			cd: ContainerDefinition{HealthCheck: &HealthCheck{
				Command: []string{"CMD", "ls"}, Interval: 6, Timeout: 3, Retries: 2, StartPeriod: 10,
			}},
			check: func(t *testing.T, cfg *dockertypes.Config, _ *dockertypes.HostConfig) {
				t.Helper()

				require.NotNil(t, cfg.Healthcheck)
				assert.Equal(t, 6*time.Second, cfg.Healthcheck.Interval)
				assert.Equal(t, 3*time.Second, cfg.Healthcheck.Timeout)
				assert.Equal(t, 2, cfg.Healthcheck.Retries)
				assert.Equal(t, 10*time.Second, cfg.Healthcheck.StartPeriod)
			},
		},
		{
			name: "labels_tty_stdin",
			cd: ContainerDefinition{
				DockerLabels:   map[string]string{"a": "b"},
				Interactive:    true,
				PseudoTerminal: true,
			},
			check: func(t *testing.T, cfg *dockertypes.Config, _ *dockertypes.HostConfig) {
				t.Helper()

				assert.Equal(t, "b", cfg.Labels["a"])
				assert.Equal(t, "arn:task", cfg.Labels["gopherstack.ecs.task"])
				assert.True(t, cfg.OpenStdin)
				assert.True(t, cfg.Tty)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, host, err := buildDockerConfigs(containerInput{
				task: &Task{TaskArn: "arn:task", ClusterArn: "arn:cluster"},
				cd:   tt.cd,
				env:  []string{"A=1"},
			})
			require.NoError(t, err)
			assert.Equal(t, []string{"A=1"}, cfg.Env)
			tt.check(t, cfg, host)
		})
	}
}

func TestBuildDockerConfigs_InvalidDNS(t *testing.T) {
	t.Parallel()

	_, _, err := buildDockerConfigs(containerInput{
		task: &Task{}, cd: ContainerDefinition{DNSServers: []string{"not-an-ip"}},
	})
	require.Error(t, err)
}

func TestContainerMounts(t *testing.T) {
	t.Parallel()

	plan := &volumePlan{specs: map[string]volumeSpec{
		"data": {mount: mount.Mount{Type: mount.TypeBind, Source: "/host/data"}},
		"tmp":  {mount: mount.Mount{Type: mount.TypeVolume, Source: "gopherstack-ecs-t-tmp"}},
	}}

	tests := []struct {
		name    string
		points  []MountPoint
		want    []mount.Mount
		wantErr bool
	}{
		{
			name: "bind_readonly_and_volume",
			points: []MountPoint{
				{SourceVolume: "data", ContainerPath: "/d", ReadOnly: true},
				{SourceVolume: "tmp", ContainerPath: "/t"},
			},
			want: []mount.Mount{
				{Type: mount.TypeBind, Source: "/host/data", Target: "/d", ReadOnly: true},
				{Type: mount.TypeVolume, Source: "gopherstack-ecs-t-tmp", Target: "/t"},
			},
		},
		{name: "undefined_volume", points: []MountPoint{{SourceVolume: "nope", ContainerPath: "/x"}}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := containerMounts(ContainerDefinition{Name: "c", MountPoints: tt.points}, plan)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseSecretRef(t *testing.T) {
	t.Parallel()

	const sm = "arn:aws:secretsmanager:eu-west-1:123456789012:secret:db-AbCdEf"

	tests := []struct {
		name string
		in   string
		want secretRef
		fail bool
	}{
		{name: "sm_arn", in: sm, want: secretRef{secretsManager: true, region: "eu-west-1", id: sm}},
		{
			name: "sm_arn_json_key",
			in:   sm + ":password::",
			want: secretRef{secretsManager: true, region: "eu-west-1", id: sm, jsonKey: "password"},
		},
		{
			name: "sm_arn_all_fields",
			in:   sm + ":password:AWSPREVIOUS:abc123",
			want: secretRef{
				secretsManager: true, region: "eu-west-1", id: sm,
				jsonKey: "password", versionStage: "AWSPREVIOUS", versionID: "abc123",
			},
		},
		{
			name: "ssm_arn", in: "arn:aws:ssm:eu-west-1:123456789012:parameter/app/db",
			want: secretRef{region: "eu-west-1", id: "/app/db"},
		},
		{name: "ssm_name", in: "/app/db", want: secretRef{region: "us-east-1", id: "/app/db"}},
		{name: "other_arn", in: "arn:aws:s3:::bucket", fail: true},
		{name: "short_arn", in: "arn:aws:secretsmanager", fail: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseSecretRef(tt.in, "us-east-1")
			if tt.fail {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
