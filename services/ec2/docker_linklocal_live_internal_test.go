package ec2

import (
	"context"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	mobyclient "github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/linklocal"
)

const imdsTokenRequest = "printf 'PUT /latest/api/token HTTP/1.1\\r\\nHost: 169.254.169.254\\r\\n" +
	"X-aws-ec2-metadata-token-ttl-seconds: 60\\r\\nContent-Length: 0\\r\\nConnection: close\\r\\n\\r\\n'" +
	" | nc -w 5 169.254.169.254 80"

func dockerBridgeGateway(t *testing.T, cli *mobyclient.Client) string {
	t.Helper()

	res, err := cli.NetworkInspect(t.Context(), "bridge", mobyclient.NetworkInspectOptions{})
	require.NoError(t, err)

	for _, c := range res.Network.IPAM.Config {
		if c.Gateway.Is4() {
			return c.Gateway.String()
		}
	}

	t.Skip("docker bridge has no IPv4 gateway")

	return ""
}

func dockerExec(ctx context.Context, containerID, script string) (string, error) {
	out, err := exec.CommandContext(ctx, "docker", "exec", containerID, "sh", "-c", script).Output()

	return strings.TrimSpace(string(out)), err
}

func TestDockerComputeLiveDefaultIMDSAddress(t *testing.T) {
	t.Parallel()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not found")
	}

	dc, cli := newLiveCompute(t, nil)
	gateway := dockerBridgeGateway(t, cli)

	b := NewInMemoryBackend("000000000000", "us-east-1")
	h := NewHandler(b)
	h.AccountID, h.Region = "000000000000", "us-east-1"

	insts, err := b.RunInstances("ami-0abc", "t3.micro", "", 1)
	require.NoError(t, err)

	instanceID := insts[0].ID

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "0.0.0.0:0")
	require.NoError(t, err)

	e := echo.New()
	e.Any("/_gopherstack/ec2/imds/:instance/*", h.ServeIMDS)

	srv := &http.Server{Handler: e, ReadHeaderTimeout: time.Second}

	go func() { _ = srv.Serve(ln) }()

	t.Cleanup(func() { _ = srv.Close() })

	_, port, _ := net.SplitHostPort(ln.Addr().String())
	spec := linklocal.IMDSSpec("", gateway, port, dockerLabelPrefix+"instance")
	spec.Name = "imds-livetest"

	mgr, err := linklocal.NewFromEnv(spec)
	require.NoError(t, err)

	if !mgr.Active(t.Context()) {
		t.Skip("link-local relay unavailable (image missing or 169.254.169.0/24 in use)")
	}

	h.SetLinkLocal(mgr)
	dc.SetLinkLocal(mgr)

	liveCmds := &liveDockerAPI{Client: cli, cmds: map[string][]string{"gopherstack-" + instanceID: {"sleep", "120"}}}
	dc.api = liveCmds

	res, err := dc.Launch(t.Context(), LaunchRequest{InstanceID: instanceID, MetadataEndpoint: "http://unused.invalid"})
	require.NoError(t, err)

	t.Cleanup(func() {
		if tErr := dc.Terminate(context.Background(), instanceID, res.ProviderID); tErr != nil {
			t.Logf("terminate: %v", tErr)
		}
		_ = mgr.Remove(context.Background())
		_ = mgr.Close()
	})

	assert.False(
		t,
		strings.HasPrefix(res.PrivateIP, "169.254."),
		"primary IP must not be the link-local one: %s",
		res.PrivateIP,
	)

	var token string

	require.Eventually(t, func() bool {
		out, execErr := dockerExec(t.Context(), res.ProviderID, imdsTokenRequest)
		_, token, _ = strings.Cut(out, "\r\n\r\n")

		return execErr == nil && strings.HasPrefix(out, "HTTP/1.1 200") && token != ""
	}, liveTimeout, liveTick)

	got, err := dockerExec(t.Context(), res.ProviderID,
		"wget -qO- --header 'X-aws-ec2-metadata-token: "+token+"' http://169.254.169.254/latest/meta-data/instance-id")
	require.NoError(t, err)
	assert.Equal(t, instanceID, got)

	env, err := dockerExec(t.Context(), res.ProviderID, "env")
	require.NoError(t, err)
	assert.NotContains(t, env, "AWS_EC2_METADATA_SERVICE_ENDPOINT")
}
