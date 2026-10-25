package ocifunctions_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/functions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocifunctions"
)

var (
	errNoSuchImage = errors.New("no such image")
	errDaemonDown  = errors.New("docker daemon not running")
)

const testUnit = 10 * time.Millisecond

type invokeEnv struct {
	*env
	rt  *fakeRuntime
	app functions.Application
	fn  functions.Function
}

func newInvokeEnv(
	t *testing.T,
	modApp func(*functions.CreateApplicationDetails),
	modFn func(*functions.CreateFunctionDetails),
) *invokeEnv {
	t.Helper()

	rt := newFakeRuntime()
	e := newEnv(t, envOpts{rt: rt})
	e.backend.SetTimeoutUnit(testUnit)

	app := e.mkApp(t, "app", modApp)

	return &invokeEnv{env: e, rt: rt, app: app, fn: e.mkFn(t, app, "fn", modFn)}
}

func (ie *invokeEnv) invoke(
	t *testing.T,
	body string,
	mod func(*functions.InvokeFunctionRequest),
) (functions.InvokeFunctionResponse, string, error) {
	t.Helper()

	return ie.invokeFn(t, ie.fn, body, mod)
}

func (ie *invokeEnv) invokeFn(
	t *testing.T,
	fn functions.Function,
	body string,
	mod func(*functions.InvokeFunctionRequest),
) (functions.InvokeFunctionResponse, string, error) {
	t.Helper()

	c, err := functions.NewFunctionsInvokeClientWithConfigurationProvider(ie.prov, *fn.InvokeEndpoint)
	require.NoError(t, err)

	req := functions.InvokeFunctionRequest{
		FunctionId: fn.Id, InvokeFunctionBody: io.NopCloser(strings.NewReader(body)),
	}
	if mod != nil {
		mod(&req)
	}

	resp, err := c.InvokeFunction(t.Context(), req)
	if err != nil {
		return resp, "", err
	}

	defer resp.Content.Close()

	out, rerr := io.ReadAll(resp.Content)
	require.NoError(t, rerr)

	return resp, string(out), nil
}

type echo struct {
	Env  map[string]string `json:"env"`
	Body string            `json:"body"`
	Path string            `json:"path"`
}

func TestInvoke_FDKContract(t *testing.T) {
	t.Parallel()

	ie := newInvokeEnv(t,
		func(d *functions.CreateApplicationDetails) {
			d.Config = map[string]string{"A": "app", "B": "app", "FN_FORMAT": "hijack"}
		},
		func(d *functions.CreateFunctionDetails) {
			d.Config = map[string]string{"B": "fn"}
			d.MemoryInMBs = new(int64(512))
			d.SourceDetails = functions.CreateContainerImageFunctionSourceDetails{
				Image: new("reg.example/fn:3"), ImageDigest: new("sha256:abc"),
			}
		})

	resp, out, err := ie.invoke(t, "hello", func(r *functions.InvokeFunctionRequest) {
		r.FnIntent = functions.InvokeFunctionFnIntentHttprequest
		r.FnInvokeType = functions.InvokeFunctionFnInvokeTypeSync
	})
	require.NoError(t, err)

	var got echo
	require.NoError(t, json.Unmarshal([]byte(out), &got))
	assert.Equal(t, "hello", got.Body)
	assert.Equal(t, "/call", got.Path)
	assert.Equal(t, "1", resp.RawResponse.Header.Get("Fn-Http-H-X-Echo"))
	assert.Equal(t, "200", resp.RawResponse.Header.Get("Fn-Http-Status"))
	assert.NotEmpty(t, resp.RawResponse.Header.Get("Fn-Call-Id"))

	want := map[string]string{
		"FN_APP_NAME": "app", "FN_FN_NAME": "fn", "FN_FORMAT": "http-stream", "FN_MEMORY": "512",
		"FN_LISTENER": "unix:/tmp/iofs/lsnr.sock", "FN_TMPSIZE": "0", "A": "app", "B": "fn",
		"FN_APP_ID": *ie.app.Id, "FN_FN_ID": *ie.fn.Id,
	}
	for k, v := range want {
		assert.Equal(t, v, got.Env[k], k)
	}

	specs := ie.rt.startedSpecs()
	require.Len(t, specs, 1)
	assert.Equal(t, "reg.example/fn:3@sha256:abc", specs[0].Image)
	assert.Contains(t, specs[0].Mounts[0], ":/tmp/iofs")

	calls := ie.rt.recorded()
	require.Len(t, calls, 1)

	h := calls[0].Header
	assert.Equal(t, ie.base, *ie.fn.InvokeEndpoint)
	assert.NotEmpty(t, h.Get("Fn-Call-Id"))
	assert.Equal(t, resp.RawResponse.Header.Get("Fn-Call-Id"), h.Get("Fn-Call-Id"))
	assert.Equal(t, http.MethodPost, h.Get("Fn-Http-Method"))
	assert.Contains(t, h.Get("Fn-Http-Request-Url"), "/functions/"+*ie.fn.Id+"/actions/invoke")
	assert.Empty(t, h.Get("Authorization"))
	assert.Empty(t, h.Get("Fn-Http-H-Authorization"))

	deadline, perr := time.Parse(time.RFC3339Nano, h.Get("Fn-Deadline"))
	require.NoError(t, perr)
	assert.WithinDuration(t, ie.clock.Now().Add(ie.timeout()), deadline, time.Second)
}

func (ie *invokeEnv) timeout() time.Duration {
	return time.Duration(*ie.fn.TimeoutInSeconds) * testUnit
}

func TestInvoke_Pool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *invokeEnv)
		name string
	}{
		{name: "container reused across invokes", run: func(t *testing.T, ie *invokeEnv) {
			t.Helper()

			for range 3 {
				_, _, err := ie.invoke(t, "x", nil)
				require.NoError(t, err)
			}

			assert.Len(t, ie.rt.startedSpecs(), 1)
			assert.Equal(t, 1, ie.backend.LiveContainers())
			assert.Len(t, ie.rt.recorded(), 3)
		}},
		{name: "update replaces the warm container", run: func(t *testing.T, ie *invokeEnv) {
			t.Helper()

			_, _, err := ie.invoke(t, "x", nil)
			require.NoError(t, err)

			_, err = ie.mgmt.UpdateFunction(t.Context(), functions.UpdateFunctionRequest{
				FunctionId: ie.fn.Id,
				Config:     map[string]string{"V": "2"},
			})
			require.NoError(t, err)
			assert.Len(t, ie.rt.removedIDs(), 1)

			_, out, err := ie.invoke(t, "x", nil)
			require.NoError(t, err)
			assert.Contains(t, out, `"V":"2"`)
			assert.Len(t, ie.rt.startedSpecs(), 2)
		}},
		{name: "application config change starts a fresh container", run: func(t *testing.T, ie *invokeEnv) {
			t.Helper()

			_, _, err := ie.invoke(t, "x", nil)
			require.NoError(t, err)

			_, err = ie.mgmt.UpdateApplication(t.Context(), functions.UpdateApplicationRequest{
				ApplicationId: ie.app.Id,
				Config:        map[string]string{"NEW": "1"},
			})
			require.NoError(t, err)

			_, out, err := ie.invoke(t, "x", nil)
			require.NoError(t, err)
			assert.Contains(t, out, `"NEW":"1"`)
			assert.Len(t, ie.rt.startedSpecs(), 2)
			assert.Len(t, ie.rt.removedIDs(), 1)
			assert.Equal(t, 1, ie.backend.LiveContainers())
		}},
		{name: "delete removes the container", run: func(t *testing.T, ie *invokeEnv) {
			t.Helper()

			_, _, err := ie.invoke(t, "x", nil)
			require.NoError(t, err)

			_, err = ie.mgmt.DeleteFunction(t.Context(), functions.DeleteFunctionRequest{FunctionId: ie.fn.Id})
			require.NoError(t, err)
			assert.Equal(t, 0, ie.rt.running())
			assert.Equal(t, 0, ie.backend.LiveContainers())
		}},
		{name: "idle containers are reaped", run: func(t *testing.T, ie *invokeEnv) {
			t.Helper()

			_, _, err := ie.invoke(t, "x", nil)
			require.NoError(t, err)

			ie.clock.Advance(time.Minute)
			ie.backend.ReapIdle()
			assert.Equal(t, 1, ie.rt.running())

			ie.clock.Advance(oci.DefaultIdleTimeout)
			ie.backend.ReapIdle()
			assert.Equal(t, 0, ie.rt.running())
			assert.Equal(t, 0, ie.backend.LiveContainers())
		}},
		{name: "shutdown removes containers", run: func(t *testing.T, ie *invokeEnv) {
			t.Helper()

			_, _, err := ie.invoke(t, "x", nil)
			require.NoError(t, err)

			ie.handler.Shutdown(t.Context())
			assert.Equal(t, 0, ie.rt.running())
		}},
		{name: "reset removes containers", run: func(t *testing.T, ie *invokeEnv) {
			t.Helper()

			_, _, err := ie.invoke(t, "x", nil)
			require.NoError(t, err)

			ie.handler.Reset()
			assert.Equal(t, 0, ie.rt.running())
		}},
		{name: "cap evicts the least recently used idle container", run: func(t *testing.T, ie *invokeEnv) {
			t.Helper()
			ie.backend.Configure(oci.Settings{MaxContainers: 1})

			second := ie.mkFn(t, ie.app, "second", nil)

			_, _, err := ie.invoke(t, "x", nil)
			require.NoError(t, err)

			_, _, err = ie.invokeFn(t, second, "x", nil)
			require.NoError(t, err)
			assert.Len(t, ie.rt.startedSpecs(), 2)
			assert.Len(t, ie.rt.removedIDs(), 1)
			assert.Equal(t, 1, ie.backend.LiveContainers())
		}},
		{name: "cap throttles when every container is busy", run: func(t *testing.T, ie *invokeEnv) {
			t.Helper()
			ie.backend.Configure(oci.Settings{MaxContainers: 1})

			second := ie.mkFn(t, ie.app, "second", nil)
			done := make(chan error, 1)

			go func() {
				c, cerr := functions.NewFunctionsInvokeClientWithConfigurationProvider(ie.prov, *ie.fn.InvokeEndpoint)
				if cerr != nil {
					done <- cerr

					return
				}

				resp, ierr := c.InvokeFunction(t.Context(), functions.InvokeFunctionRequest{
					FunctionId: ie.fn.Id, InvokeFunctionBody: io.NopCloser(strings.NewReader("block")),
				})
				if ierr == nil {
					_ = resp.Content.Close()
				}

				done <- ierr
			}()

			require.Eventually(t, func() bool { return len(ie.rt.recorded()) == 1 }, 5*time.Second, 5*time.Millisecond)

			_, _, err := ie.invokeFn(t, second, "x", nil)
			se := svcErr(t, err)
			assert.Equal(t, http.StatusTooManyRequests, se.GetHTTPStatusCode())

			close(ie.rt.release)
			require.NoError(t, <-done)
		}},
		{name: "missing image is pulled by digest reference", run: func(t *testing.T, ie *invokeEnv) {
			t.Helper()
			ie.rt.hasImage = false

			_, _, err := ie.invoke(t, "x", nil)
			require.NoError(t, err)
			assert.Equal(t, []string{"example/fn:1"}, ie.rt.pulls)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newInvokeEnv(t, nil, nil))
		})
	}
}

func TestInvoke_Outcomes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *invokeEnv)
		name string
	}{
		{name: "function failure is 502 and the container stays warm", run: func(t *testing.T, ie *invokeEnv) {
			t.Helper()

			_, _, err := ie.invoke(t, "fail", nil)
			se := svcErr(t, err)
			assert.Equal(t, http.StatusBadGateway, se.GetHTTPStatusCode())
			assert.Equal(t, "FunctionInvokeExecutionFailed", se.GetCode())

			_, _, err = ie.invoke(t, "ok", nil)
			require.NoError(t, err)
			assert.Len(t, ie.rt.startedSpecs(), 1)
		}},
		{name: "timeout is 504 and the container is removed", run: func(t *testing.T, ie *invokeEnv) {
			t.Helper()

			_, _, err := ie.invoke(t, "hang", nil)
			se := svcErr(t, err)
			assert.Equal(t, http.StatusGatewayTimeout, se.GetHTTPStatusCode())
			assert.Equal(t, "FunctionInvokeTimeout", se.GetCode())
			assert.Equal(t, 0, ie.rt.running())
			assert.Equal(t, 0, ie.backend.LiveContainers())
		}},
		{name: "detached returns 202 and still runs the function", run: func(t *testing.T, ie *invokeEnv) {
			t.Helper()

			resp, out, err := ie.invoke(t, "later", func(r *functions.InvokeFunctionRequest) {
				r.FnInvokeType = functions.InvokeFunctionFnInvokeTypeDetached
			})
			require.NoError(t, err)
			assert.Equal(t, http.StatusAccepted, resp.RawResponse.StatusCode)
			assert.Empty(t, out)

			require.Eventually(t, func() bool { return len(ie.rt.recorded()) == 1 }, 5*time.Second, 5*time.Millisecond)
			assert.Equal(t, "later", ie.rt.recorded()[0].Body)
		}},
		{name: "dry run does not execute", run: func(t *testing.T, ie *invokeEnv) {
			t.Helper()

			resp, _, err := ie.invoke(t, "x", func(r *functions.InvokeFunctionRequest) { r.IsDryRun = new(true) })
			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, resp.RawResponse.StatusCode)
			assert.Empty(t, ie.rt.startedSpecs())
		}},
		{name: "container start failure is 502", run: func(t *testing.T, ie *invokeEnv) {
			t.Helper()
			ie.rt.createErr = errNoSuchImage

			_, _, err := ie.invoke(t, "x", nil)
			se := svcErr(t, err)
			assert.Equal(t, http.StatusBadGateway, se.GetHTTPStatusCode())
			assert.Contains(t, se.GetMessage(), "no such image")
			assert.Equal(t, 0, ie.backend.LiveContainers())
		}},
		{name: "engine none is 503", run: func(t *testing.T, ie *invokeEnv) {
			t.Helper()
			ie.backend.Configure(oci.Settings{DisableEngine: true})

			_, _, err := ie.invoke(t, "x", nil)
			se := svcErr(t, err)
			assert.Equal(t, http.StatusServiceUnavailable, se.GetHTTPStatusCode())
			assert.Contains(t, se.GetMessage(), "disabled")
			assert.Empty(t, ie.rt.startedSpecs())
		}},
		{name: "engine none rejects detached too", run: func(t *testing.T, ie *invokeEnv) {
			t.Helper()
			ie.backend.Configure(oci.Settings{DisableEngine: true})

			_, _, err := ie.invoke(t, "x", func(r *functions.InvokeFunctionRequest) {
				r.FnInvokeType = functions.InvokeFunctionFnInvokeTypeDetached
			})
			assert.Equal(t, http.StatusServiceUnavailable, svcErr(t, err).GetHTTPStatusCode())
		}},
		{name: "unreachable runtime is 503", run: func(t *testing.T, ie *invokeEnv) {
			t.Helper()
			ie.backend.SetRuntime(func() (oci.Runner, error) { return nil, errDaemonDown })

			_, _, err := ie.invoke(t, "x", nil)
			se := svcErr(t, err)
			assert.Equal(t, http.StatusServiceUnavailable, se.GetHTTPStatusCode())
			assert.Contains(t, se.GetMessage(), "docker daemon not running")
		}},
		{name: "unknown function is 404", run: func(t *testing.T, ie *invokeEnv) {
			t.Helper()

			ghost := ie.fn
			ghost.Id = new("ocid1.fnfunc.oc1..ghost")

			_, _, err := ie.invokeFn(t, ghost, "x", nil)
			assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
		}},
		{name: "function still creating is 409", run: func(t *testing.T, ie *invokeEnv) {
			t.Helper()

			created, err := ie.mgmt.CreateFunction(t.Context(), functions.CreateFunctionRequest{
				DisplayName: new("fresh"), ApplicationId: ie.app.Id, MemoryInMBs: new(int64(128)),
				SourceDetails: functions.CreateContainerImageFunctionSourceDetails{Image: new("i")},
			})
			require.NoError(t, err)

			created.InvokeEndpoint = &ie.base

			_, _, err = ie.invokeFn(t, created.Function, "x", nil)
			assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newInvokeEnv(t, nil, func(d *functions.CreateFunctionDetails) { d.TimeoutInSeconds = new(5) }))
		})
	}
}

func TestInvoke_RawRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		hdr        map[string]string
		body       string
		wantStatus int
	}{
		{"bad invoke type", map[string]string{"fn-invoke-type": "later"}, "x", http.StatusBadRequest},
		{"bad intent", map[string]string{"fn-intent": "telepathy"}, "x", http.StatusBadRequest},
		{"cloudevent intent", map[string]string{"fn-intent": "cloudevent"}, "x", http.StatusOK},
		{"oversize payload", nil, string(bytes.Repeat([]byte("a"), 8<<20)), http.StatusRequestEntityTooLarge},
		{"empty body", nil, "", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ie := newInvokeEnv(t, nil, nil)
			status, _, _ := rawDo(t, http.MethodPost,
				ie.base+"/20181201/functions/"+*ie.fn.Id+"/actions/invoke", tt.body, tt.hdr)
			assert.Equal(t, tt.wantStatus, status)
		})
	}
}
