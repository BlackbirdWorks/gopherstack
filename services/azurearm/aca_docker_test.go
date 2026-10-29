package azurearm_test

import (
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appcontainers/armappcontainers/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
)

func newACADockerStack(t *testing.T) *acaStack {
	t.Helper()

	if os.Getenv(aciDockerOptIn) == "" {
		t.Skipf("set %s=1 to run against a real container runtime", aciDockerOptIn)
	}

	lr := gcpserve.NewLazyRuntime(nil, "azurearm.aca.test.runtime")
	t.Cleanup(func() { _ = lr.Close() })

	if _, err := lr.Get(); err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	s := newACAStackWith(t, nil)
	s.engine.SetTiming(func(int) time.Duration { return 50 * time.Millisecond }, 5*time.Minute)

	return s
}

func TestACADocker_AppServesThroughFQDN(t *testing.T) {
	t.Parallel()

	s := newACADockerStack(t)
	env := s.mustEnv(t, "rg", "env1")
	domain := *env.Properties.DefaultDomain

	script := `mkdir -p /www && echo "served-by-aca:$APP_SECRET:$CONTAINER_APP_NAME" > /www/index.html ` +
		`&& httpd -f -p 8080 -h /www`

	app, err := s.putApp(t, "rg", "web", *env.ID, acaApp{
		image: "busybox:1.36", extIng: true,
		command: []*string{new("sh"), new("-c")}, args: []*string{&script},
		secrets: []*armappcontainers.Secret{{Name: new("app-secret"), Value: new("s3cret")}},
		env:     []*armappcontainers.EnvironmentVar{{Name: new("APP_SECRET"), SecretRef: new("app-secret")}},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		poller, delErr := s.factory.NewContainerAppsClient().BeginDelete(t.Context(), "rg", "web", nil)
		if delErr == nil {
			_, _ = poller.PollUntilDone(t.Context(), pollFast())
		}
	})

	assert.Equal(t, "web."+domain, *app.Properties.Configuration.Ingress.Fqdn)

	var body string

	require.Eventually(t, func() bool {
		status, got := s.rawNoFail("web."+domain, "/")
		body = got

		return status == http.StatusOK && strings.Contains(got, "served-by-aca")
	}, 90*time.Second, 200*time.Millisecond)

	assert.Contains(t, body, "served-by-aca:s3cret:web")

	replicas, err := s.factory.NewContainerAppsRevisionReplicasClient().ListReplicas(
		t.Context(), "rg", "web", *app.Properties.LatestRevisionName, nil)
	require.NoError(t, err)
	require.Len(t, replicas.Value, 1)
	assert.Equal(t, armappcontainers.ContainerAppReplicaRunningStateRunning, *replicas.Value[0].Properties.RunningState)
}
