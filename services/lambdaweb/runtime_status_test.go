package lambdaweb_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/lambdaweb"
)

var errDaemonDown = errors.New("cannot connect to the docker daemon")

// garbageEnv is an environment whose app answers with bytes that are not HTTP.
type garbageEnv struct{ ln net.Listener }

func (g *garbageEnv) Addr() (string, int) {
	host, port, _ := net.SplitHostPort(g.ln.Addr().String())
	p, _ := strconv.Atoi(port)

	return host, p
}

func (g *garbageEnv) Close(context.Context) { _ = g.ln.Close() }

type garbageLauncher struct{ t *testing.T }

func (g garbageLauncher) Launch(ctx context.Context, _ lambdaweb.LaunchSpec) (lambdaweb.Environment, error) {
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}

			_, _ = c.Write([]byte("not http\r\n\r\n"))
			_ = c.Close()
		}
	}()

	return &garbageEnv{ln: ln}, nil
}

func TestDataPlane_EnvironmentFailureStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*fakeRuntime)
		name       string
		errType    string
		wantStatus int
		garbage    bool
	}{
		{
			name: "runtime absent", setup: func(r *fakeRuntime) { r.pingErr = errDaemonDown },
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name: "image cannot be pulled", setup: func(r *fakeRuntime) { r.noImage, r.pullErr = true, errDaemonDown },
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name: "runtime present start fails", setup: func(r *fakeRuntime) { r.createErr = errDaemonDown },
			wantStatus: http.StatusBadGateway, errType: "Application.TimeoutListenError",
		},
		{
			name: "function returns invalid response", garbage: true,
			wantStatus: http.StatusServiceUnavailable, errType: "Runtime.Unreachable",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env, rt, l := newLauncherEnv(t, map[string]string{"index.js": "x"})
			if tc.setup != nil {
				tc.setup(rt)
			}

			if tc.garbage {
				l = garbageLauncher{t: t}
			}

			env.backend.SetLauncher(l)

			created := env.createFunction(t, "app")
			env.settle()

			d := &dataEnv{
				testEnv: env, domain: aws.ToString(created.Endpoint.DomainName),
				http: &http.Client{Transport: testclient.New(t).Transport, Timeout: 30 * time.Second},
			}

			resp := d.get(t)
			assert.Equal(t, tc.wantStatus, resp.StatusCode)
			assert.Equal(t, tc.errType, resp.Header.Get("Lambda-Web-Error-Type"))
			require.NotEmpty(t, resp.Header.Get("Lambda-Web-Request-Id"))
		})
	}
}
