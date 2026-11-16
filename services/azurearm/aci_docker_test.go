package azurearm_test

import (
	"crypto/tls"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerinstance/armcontainerinstance/v2"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

const aciDockerOptIn = "GOPHERSTACK_ACI_DOCKER_TEST"

func newACIDockerStack(t *testing.T) *aciStack {
	t.Helper()

	if os.Getenv(aciDockerOptIn) == "" {
		t.Skipf("set %s=1 to run against a real container runtime", aciDockerOptIn)
	}

	lr := gcpserve.NewLazyRuntime(nil, "azurearm.aci.test.runtime")
	t.Cleanup(func() { _ = lr.Close() })

	if _, err := lr.Get(); err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	s := newACIStackWith(t, nil)
	s.engine.SetTiming(func(int) time.Duration { return 50 * time.Millisecond }, 5*time.Minute)

	return s
}

func TestACIDocker_RunToCompletion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		script    string
		wantState string
		wantLogs  []string
		wantExit  int32
	}{
		{
			name: "exit-zero", wantState: "Succeeded", wantExit: 0,
			script:   "echo hello-aci; echo greeting=$GREETING; echo oops >&2",
			wantLogs: []string{"hello-aci", "greeting=world", "oops"},
		},
		{
			name: "exit-nonzero", wantState: "Failed", wantExit: 3,
			script: "echo failing; exit 3", wantLogs: []string{"failing"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newACIDockerStack(t)

			_, err := s.create(t, "rg", "job", aciOpts{
				image:   "alpine:3.21",
				command: []string{"sh", "-c", tc.script},
				env:     []*armcontainerinstance.EnvironmentVariable{{Name: new("GREETING"), Value: new("world")}},
			})
			require.NoError(t, err)

			g := s.waitState(t, "rg", "job", tc.wantState)
			cs := contState(g)
			assert.Equal(t, "Terminated", *cs.State)
			assert.Equal(t, tc.wantExit, *cs.ExitCode)
			assert.Equal(t, int32(0), *g.Properties.Containers[0].Properties.InstanceView.RestartCount)

			logs, err := s.factory.NewContainersClient().ListLogs(t.Context(), "rg", "job", "main", nil)
			require.NoError(t, err)

			for _, want := range tc.wantLogs {
				assert.Contains(t, *logs.Content, want)
			}
		})
	}
}

func TestACIDocker_OnFailureRestartsAndStopRemoves(t *testing.T) {
	t.Parallel()

	s := newACIDockerStack(t)

	_, err := s.create(t, "rg", "crash", aciOpts{
		image: "alpine:3.21", policy: armcontainerinstance.ContainerGroupRestartPolicyOnFailure,
		command: []string{"sh", "-c", "exit 1"},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		g := s.get(t, "rg", "crash")

		return *g.Properties.Containers[0].Properties.InstanceView.RestartCount >= 2
	}, 30*time.Second, 20*time.Millisecond)

	_, err = s.factory.NewContainerGroupsClient().Stop(t.Context(), "rg", "crash", nil)
	require.NoError(t, err)
	assert.Equal(t, "Stopped", *s.get(t, "rg", "crash").Properties.InstanceView.State)
}

var publishedRe = regexp.MustCompile(`127\.0\.0\.1:(\d+)`)

func TestACIDocker_PublishedPortServes(t *testing.T) {
	t.Parallel()

	s := newACIDockerStack(t)

	_, err := s.create(t, "rg", "web", aciOpts{
		image:  "alpine:3.21",
		policy: armcontainerinstance.ContainerGroupRestartPolicyAlways,
		command: []string{
			"sh",
			"-c",
			`while true; do printf "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nserved-by-aci\n" | nc -l -p 8080; done`,
		},
		ip: &armcontainerinstance.IPAddress{
			Type: new(armcontainerinstance.ContainerGroupIPAddressTypePublic),
			Ports: []*armcontainerinstance.Port{
				{Port: new(int32(8080)), Protocol: new(armcontainerinstance.ContainerGroupNetworkProtocolTCP)},
			},
		},
		ports: []*armcontainerinstance.ContainerPort{{Port: new(int32(8080))}},
	})
	require.NoError(t, err)

	var addr string

	for _, e := range s.get(t, "rg", "web").Properties.InstanceView.Events {
		if m := publishedRe.FindStringSubmatch(*e.Message); *e.Name == "PortPublished" && m != nil {
			addr = m[0]
		}
	}

	require.NotEmpty(t, addr)

	require.Eventually(t, func() bool {
		req, reqErr := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/", http.NoBody)
		if reqErr != nil {
			return false
		}

		resp, doErr := testclient.New(t).Do(req)
		if doErr != nil {
			return false
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)

		return strings.Contains(string(body), "served-by-aci")
	}, 30*time.Second, 100*time.Millisecond)
}

func TestACIDocker_ExecStreamsCommandOutput(t *testing.T) {
	t.Parallel()

	s := newACIDockerStack(t)

	_, err := s.create(t, "rg", "execd", aciOpts{
		image: "alpine:3.21", command: []string{"sleep", "300"},
		policy: armcontainerinstance.ContainerGroupRestartPolicyAlways,
	})
	require.NoError(t, err)
	s.waitState(t, "rg", "execd", "Running")

	exec, err := s.factory.NewContainersClient().ExecuteCommand(t.Context(), "rg", "execd", "main",
		armcontainerinstance.ContainerExecRequest{Command: new("echo from-exec; echo err-line >&2")}, nil)
	require.NoError(t, err)

	dialer := websocket.Dialer{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	conn, _, err := dialer.DialContext(t.Context(), *exec.WebSocketURI, nil)
	require.NoError(t, err)

	defer conn.Close()

	require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(*exec.Password)))

	var out strings.Builder

	for {
		_, msg, rerr := conn.ReadMessage()
		if rerr != nil {
			break
		}

		out.Write(msg)
	}

	assert.Contains(t, out.String(), "from-exec")
	assert.Contains(t, out.String(), "err-line")
}

func TestACIDocker_ExecRelaysStdin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		term *armcontainerinstance.ContainerExecRequestTerminalSize
		name string
	}{
		{name: "pipe"},
		{
			name: "terminal",
			term: &armcontainerinstance.ContainerExecRequestTerminalSize{Rows: new(int32(24)), Cols: new(int32(80))},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newACIDockerStack(t)

			_, err := s.create(t, "rg", "stdin", aciOpts{
				image: "alpine:3.21", command: []string{"sleep", "300"},
				policy: armcontainerinstance.ContainerGroupRestartPolicyAlways,
			})
			require.NoError(t, err)
			s.waitState(t, "rg", "stdin", "Running")

			exec, err := s.factory.NewContainersClient().ExecuteCommand(t.Context(), "rg", "stdin", "main",
				armcontainerinstance.ContainerExecRequest{
					Command: new("read line; echo got-$line"), TerminalSize: tt.term,
				}, nil)
			require.NoError(t, err)

			dialer := websocket.Dialer{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
			conn, _, err := dialer.DialContext(t.Context(), *exec.WebSocketURI, nil)
			require.NoError(t, err)

			defer conn.Close()

			require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte(*exec.Password)))
			require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte("ping\n")))

			var out strings.Builder

			for {
				_, msg, rerr := conn.ReadMessage()
				if rerr != nil {
					break
				}

				out.Write(msg)
			}

			assert.Contains(t, out.String(), "got-ping")
		})
	}
}
