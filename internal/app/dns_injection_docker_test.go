package app

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	gopherDNS "github.com/blackbirdworks/gopherstack/pkgs/dns"
	"github.com/blackbirdworks/gopherstack/pkgs/transparent"
)

const (
	dockerProbeImage = "public.ecr.aws/lambda/nodejs:20"
	dockerRunTimeout = 90 * time.Second
	nodeHTTPSProbe   = `require('https').get('https://s3.amazonaws.com/',r=>{` +
		`console.log('STATUS '+r.statusCode);r.resume()}).on('error',e=>{` +
		`console.log('ERR '+e.message);process.exit(1)})`
)

func dockerOut(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()

	return strings.TrimSpace(string(out)), err
}

// bridgeGateway skips the test unless Docker, the probe image and a bridge gateway are available.
func bridgeGateway(t *testing.T) string {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not found")
	}

	if _, err := dockerOut(t.Context(), "image", "inspect", dockerProbeImage); err != nil {
		t.Skipf("%s not present", dockerProbeImage)
	}

	gw, err := dockerOut(t.Context(), "network", "inspect", "bridge",
		"-f", "{{range .IPAM.Config}}{{.Gateway}} {{end}}")
	if err != nil || strings.TrimSpace(gw) == "" {
		t.Skip("docker bridge gateway unavailable")
	}

	return strings.Fields(gw)[0]
}

//nolint:paralleltest // fixed ports, process env
func TestTransparentInjectionRealDockerContainer(t *testing.T) {
	gw := bridgeGateway(t)

	ca, err := devtls.NewCA()
	require.NoError(t, err)

	dnsSrv, err := gopherDNS.New(gopherDNS.Config{
		ListenAddr:  gw + ":53",
		ResolveIP:   gw,
		Transparent: gopherDNS.TransparentConfig{Suffixes: gopherDNS.DefaultTransparentSuffixes()},
	})
	require.NoError(t, err)

	if startErr := dnsSrv.Start(t.Context()); startErr != nil {
		t.Skipf("cannot bind %s:53: %v", gw, startErr)
	}

	t.Cleanup(func() { _ = dnsSrv.Stop() })

	srv, err := transparent.Start(t.Context(), gw+":443", http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) }), ca)
	if err != nil {
		t.Skipf("cannot bind %s:443: %v", gw, err)
	}

	t.Cleanup(func() { _ = srv.Close() })

	caFile := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caFile, ca.CertPEM(), 0o644))

	ctx, cancel := context.WithTimeout(t.Context(), dockerRunTimeout)
	defer cancel()

	out, err := dockerOut(ctx, "run", "--rm", "--dns", gw,
		"-v", caFile+":"+container.InjectedCAPath+":ro",
		"-e", "NODE_EXTRA_CA_CERTS="+container.InjectedCAPath,
		"--entrypoint", "/bin/sh", dockerProbeImage, "-c",
		"getent hosts s3.amazonaws.com && node -e \""+nodeHTTPSProbe+"\"")
	require.NoError(t, err, out)

	assert.Contains(t, out, gw)
	assert.Contains(t, out, "s3.amazonaws.com")
	assert.Contains(t, out, "STATUS 418")
}
