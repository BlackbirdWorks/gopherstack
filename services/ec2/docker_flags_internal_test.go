package ec2

import (
	"testing"

	mobycontainer "github.com/moby/moby/api/types/container"
	mobynetwork "github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDockerFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check   func(t *testing.T, f dockerFlags)
		name    string
		in      string
		wantErr string
	}{
		{name: "empty", in: "", check: func(t *testing.T, f dockerFlags) {
			t.Helper()
			t.Helper()
			assert.Equal(t, dockerFlags{}, f)
		}},
		{
			name: "privileged_env_volume",
			in:   `--privileged -e A=1 --env=B=two -v /host:/ctr:ro --volume vol:/data`,
			check: func(t *testing.T, f dockerFlags) {
				t.Helper()
				assert.True(t, f.privileged)
				assert.Equal(t, []string{"A=1", "B=two"}, f.env)
				assert.Equal(t, []string{"/host:/ctr:ro", "vol:/data"}, f.binds)
			},
		},
		{
			name: "caps_resources_network",
			in:   `--cap-add net_admin --cap-drop=MKNOD -m 512m --cpus 1.5 --network=mynet`,
			check: func(t *testing.T, f dockerFlags) {
				t.Helper()
				assert.Equal(t, []string{"NET_ADMIN"}, f.capAdd)
				assert.Equal(t, []string{"MKNOD"}, f.capDrop)
				assert.Equal(t, int64(512*1024*1024), f.memory)
				assert.Equal(t, int64(1_500_000_000), f.nanoCPUs)
				assert.Equal(t, "mynet", f.network)
			},
		},
		{
			name: "publish_forms",
			in:   `-p 8080:80 -p 127.0.0.1:9000:9000/udp`,
			check: func(t *testing.T, f dockerFlags) {
				t.Helper()
				assert.Equal(t, []publishFlag{
					{hostPort: 8080, containerPort: 80, proto: "tcp"},
					{hostIP: "127.0.0.1", hostPort: 9000, containerPort: 9000, proto: "udp"},
				}, f.publish)
			},
		},
		{
			name: "quoted_value",
			in:   `-e "GREETING=hello world" -e 'X=y'`,
			check: func(t *testing.T, f dockerFlags) {
				t.Helper()
				assert.Equal(t, []string{"GREETING=hello world", "X=y"}, f.env)
			},
		},
		{name: "unknown_flag", in: "--pid=host", wantErr: "unsupported flag"},
		{name: "unknown_short", in: "-d", wantErr: "unsupported flag"},
		{name: "positional", in: "ubuntu", wantErr: "unsupported flag"},
		{name: "missing_value", in: "-e", wantErr: "needs a value"},
		{name: "env_without_equals", in: "-e FOO", wantErr: "KEY=VALUE"},
		{name: "relative_target", in: "-v /a:rel", wantErr: "absolute target"},
		{name: "bad_volume_mode", in: "-v /a:/b:zz", wantErr: "mode must be ro or rw"},
		{name: "bad_memory", in: "-m lots", wantErr: "invalid --memory"},
		{name: "negative_cpus", in: "--cpus -1", wantErr: "invalid --cpus"},
		{name: "bad_cap", in: "--cap-add 'a;b'", wantErr: "invalid capability"},
		{name: "bad_port", in: "-p 70000:80", wantErr: "invalid host port"},
		{name: "ssh_port_reserved", in: "-p 2222:22", wantErr: "reserved for sshd"},
		{name: "bad_proto", in: "-p 80:80/sctp", wantErr: "protocol must be tcp or udp"},
		{name: "bad_network", in: "--network 'a b;c'", wantErr: "invalid network name"},
		{name: "unterminated_quote", in: `-e "A=1`, wantErr: "unterminated"},
		{name: "privileged_with_value", in: "--privileged=true", wantErr: "unsupported flag"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f, err := parseDockerFlags(tt.in)
			if tt.wantErr != "" {
				require.ErrorIs(t, err, ErrDockerFlags)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)
			tt.check(t, f)
		})
	}
}

func TestDockerFlagsApply(t *testing.T) {
	t.Parallel()

	f, err := parseDockerFlags(`--privileged -e A=1 -v /h:/c --cap-add SYS_ADMIN -m 64m --cpus 2 -p 8080:80`)
	require.NoError(t, err)

	hc := &mobycontainer.HostConfig{PortBindings: mobynetwork.PortMap{}}
	exposed := mobynetwork.PortSet{}
	env := f.apply(hc, []string{"BASE=1"}, exposed)

	assert.Equal(t, []string{"BASE=1", "A=1"}, env)
	assert.True(t, hc.Privileged)
	assert.Equal(t, []string{"/h:/c"}, hc.Binds)
	assert.Equal(t, []string{"SYS_ADMIN"}, hc.CapAdd)
	assert.Equal(t, int64(64*1024*1024), hc.Memory)
	assert.Equal(t, int64(2_000_000_000), hc.NanoCPUs)

	port, perr := mobynetwork.ParsePort("80/tcp")
	require.NoError(t, perr)
	assert.Contains(t, exposed, port)
	require.Len(t, hc.PortBindings[port], 1)
	assert.Equal(t, "8080", hc.PortBindings[port][0].HostPort)
}

func TestNewDockerComputeRejectsBadFlags(t *testing.T) {
	t.Parallel()

	_, err := NewDockerCompute(DockerComputeConfig{Flags: "--pid=host"})
	require.ErrorIs(t, err, ErrDockerFlags)
}

func TestDockerComputeLaunchAppliesFlags(t *testing.T) {
	t.Parallel()

	api := &fakeDockerAPI{containerID: "c1", containerIP: "172.17.0.2"}
	dc := newDockerComputeWithAPI(api, DockerComputeConfig{
		Flags: `--privileged -e K=V -v /h:/c:ro -m 1g --network flagnet`,
	})

	_, err := dc.Launch(t.Context(), LaunchRequest{InstanceID: "i-1", ImageID: "ami-1"})
	require.NoError(t, err)
	require.Len(t, api.created, 1)

	got := api.created[0]
	assert.True(t, got.HostConfig.Privileged)
	assert.Contains(t, got.Config.Env, "K=V")
	assert.Equal(t, []string{"/h:/c:ro"}, got.HostConfig.Binds)
	assert.Equal(t, int64(1<<30), got.HostConfig.Memory)
	assert.Equal(t, mobycontainer.NetworkMode("flagnet"), got.HostConfig.NetworkMode)
}
