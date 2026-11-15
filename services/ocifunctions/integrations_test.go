package ocifunctions_test

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/functions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocifunctions"
)

func createApp(t *testing.T, e *env, mod func(*functions.CreateApplicationDetails)) error {
	t.Helper()

	d := functions.CreateApplicationDetails{
		CompartmentId: new(testCompartment), DisplayName: new("app-" + t.Name()), SubnetIds: []string{testSubnet},
	}
	if mod != nil {
		mod(&d)
	}

	_, err := e.mgmt.CreateApplication(t.Context(), functions.CreateApplicationRequest{CreateApplicationDetails: d})

	return err
}

func TestApplication_ConfigAndSyslogValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod  func(*functions.CreateApplicationDetails)
		name string
		ok   bool
	}{
		{
			name: "syslog tcp",
			mod:  func(d *functions.CreateApplicationDetails) { d.SyslogUrl = new("tcp://logs.example:514") },
			ok:   true,
		},
		{
			name: "syslog udp",
			mod:  func(d *functions.CreateApplicationDetails) { d.SyslogUrl = new("udp://logs.example:514") },
			ok:   true,
		},
		{
			name: "syslog tcp tls",
			mod:  func(d *functions.CreateApplicationDetails) { d.SyslogUrl = new("tcp+tls://logs.example:6514") },
			ok:   true,
		},
		{
			name: "syslog https",
			mod:  func(d *functions.CreateApplicationDetails) { d.SyslogUrl = new("https://logs.example") },
		},
		{
			name: "config key leading digit",
			mod:  func(d *functions.CreateApplicationDetails) { d.Config = map[string]string{"1A": "x"} },
		},
		{
			name: "config key dash",
			mod:  func(d *functions.CreateApplicationDetails) { d.Config = map[string]string{"A-B": "x"} },
		},
		{
			name: "config key ok",
			mod:  func(d *functions.CreateApplicationDetails) { d.Config = map[string]string{"_A1": "x"} },
			ok:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			err := createApp(t, e, tt.mod)

			if tt.ok {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestLimits(t *testing.T) {
	t.Parallel()

	t.Run("applications", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		e.backend.Configure(oci.Settings{MaxApplications: 1})
		e.mkApp(t, "one", nil)

		se := svcErr(t, createApp(t, e, nil))
		assert.Equal(t, "LimitExceeded", se.GetCode())
		assert.Equal(t, http.StatusBadRequest, se.GetHTTPStatusCode())
	})

	t.Run("functions", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		e.backend.Configure(oci.Settings{MaxFunctions: 1})
		app := e.mkApp(t, "app", nil)
		e.mkFn(t, app, "one", nil)

		_, err := e.mgmt.CreateFunction(t.Context(), functions.CreateFunctionRequest{
			DisplayName: new("two"), ApplicationId: app.Id, MemoryInMBs: new(int64(256)),
			SourceDetails: functions.CreateContainerImageFunctionSourceDetails{Image: new("example/fn:1")},
		})
		assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode())
	})

	t.Run("provisioned memory", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		e.backend.Configure(oci.Settings{ProvisionedMB: 1024})
		app := e.mkApp(t, "app", nil)
		mk := func(count int) error {
			_, err := e.mgmt.CreateFunction(t.Context(), functions.CreateFunctionRequest{
				DisplayName:                  new("fn"),
				ApplicationId:                app.Id,
				MemoryInMBs:                  new(int64(512)),
				ProvisionedConcurrencyConfig: functions.ConstantProvisionedConcurrencyConfig{Count: new(count)},
				SourceDetails: functions.CreateContainerImageFunctionSourceDetails{
					Image: new("example/fn:1"),
				},
			})

			return err
		}

		assert.Equal(t, "LimitExceeded", svcErr(t, mk(3)).GetCode())
		require.NoError(t, mk(2))
	})
}

func TestApplication_NetworkLinks(t *testing.T) {
	t.Parallel()

	net := oci.Network{
		Subnet: func(id string) (string, bool) { return "vcn-a", id == testSubnet },
		NSG: func(id string) (string, bool) {
			switch id {
			case "nsg-a":
				return "vcn-a", true
			case "nsg-b":
				return "vcn-b", true
			}

			return "", false
		},
	}

	tests := []struct {
		mod      func(*functions.CreateApplicationDetails)
		name     string
		wantCode string
	}{
		{
			name: "known",
			mod:  func(d *functions.CreateApplicationDetails) { d.NetworkSecurityGroupIds = []string{"nsg-a"} },
		},
		{
			name: "unknown subnet", wantCode: "NotAuthorizedOrNotFound",
			mod: func(d *functions.CreateApplicationDetails) { d.SubnetIds = []string{"ocid1.subnet.oc1..nope"} },
		},
		{
			name: "unknown nsg", wantCode: "NotAuthorizedOrNotFound",
			mod: func(d *functions.CreateApplicationDetails) { d.NetworkSecurityGroupIds = []string{"nsg-x"} },
		},
		{
			name: "nsg in other vcn", wantCode: "InvalidParameter",
			mod: func(d *functions.CreateApplicationDetails) { d.NetworkSecurityGroupIds = []string{"nsg-b"} },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.backend.SetNetwork(net)

			err := createApp(t, e, tt.mod)
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.wantCode, svcErr(t, err).GetCode())
		})
	}

	t.Run("update nsg", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		e.backend.SetNetwork(net)
		app := e.mkApp(t, "app", nil)

		_, err := e.mgmt.UpdateApplication(t.Context(), functions.UpdateApplicationRequest{
			ApplicationId:           app.Id,
			NetworkSecurityGroupIds: []string{"nsg-b"},
		})
		assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())
	})
}

const (
	testKey    = "ocid1.key.oc1..k"
	signedImg  = "iad.ocir.io/ns/signed:1"
	plainImg   = "iad.ocir.io/ns/plain:1"
	foreignImg = "docker.io/library/alpine:3"
)

func testImages() oci.Images {
	return oci.Images{
		Resolve: func(ref string) (oci.ImageInfo, bool, bool) {
			switch ref {
			case signedImg:
				return oci.ImageInfo{ID: "img-signed", Digest: "sha256:aa"}, true, true
			case plainImg:
				return oci.ImageInfo{ID: "img-plain", Digest: "sha256:bb"}, true, true
			case "iad.ocir.io/ns/missing:1":
				return oci.ImageInfo{}, false, true
			}

			return oci.ImageInfo{}, false, false
		},
		Verified: func(img oci.ImageInfo, keys []string) bool {
			return img.ID == "img-signed" && len(keys) == 1 && keys[0] == testKey
		},
	}
}

func TestFunction_ImageLinks(t *testing.T) {
	t.Parallel()

	policy := func(d *functions.CreateApplicationDetails) {
		d.ImagePolicyConfig = &functions.ImagePolicyConfig{
			IsPolicyEnabled: new(true), KeyDetails: []functions.KeyDetails{{KmsKeyId: new(testKey)}},
		}
	}

	tests := []struct {
		appMod   func(*functions.CreateApplicationDetails)
		name     string
		image    string
		digest   string
		wantCode string
	}{
		{name: "stored image", image: plainImg},
		{name: "foreign image", image: foreignImg},
		{name: "missing owned image", image: "iad.ocir.io/ns/missing:1", wantCode: "InvalidParameter"},
		{name: "digest mismatch", image: plainImg, digest: "sha256:cc", wantCode: "InvalidParameter"},
		{name: "digest match", image: plainImg, digest: "sha256:bb"},
		{name: "policy signed", appMod: policy, image: signedImg},
		{name: "policy unsigned", appMod: policy, image: plainImg, wantCode: "InvalidParameter"},
		{name: "policy foreign", appMod: policy, image: foreignImg, wantCode: "InvalidParameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.backend.SetImages(testImages())
			app := e.mkApp(t, "app", tt.appMod)

			src := functions.CreateContainerImageFunctionSourceDetails{Image: new(tt.image)}
			if tt.digest != "" {
				src.ImageDigest = new(tt.digest)
			}

			_, err := e.mgmt.CreateFunction(t.Context(), functions.CreateFunctionRequest{
				DisplayName: new("fn"), ApplicationId: app.Id, MemoryInMBs: new(int64(256)), SourceDetails: src,
			})
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.wantCode, svcErr(t, err).GetCode())
		})
	}

	t.Run("invoke rechecks policy", func(t *testing.T) {
		t.Parallel()

		rt := newFakeRuntime()
		e := newEnv(t, envOpts{rt: rt})
		app := e.mkApp(t, "app", nil)
		fn := e.mkFn(t, app, "fn", func(d *functions.CreateFunctionDetails) {
			d.SourceDetails = functions.CreateContainerImageFunctionSourceDetails{Image: new(plainImg)}
		})

		e.backend.SetImages(testImages())

		_, err := e.mgmt.UpdateApplication(t.Context(), functions.UpdateApplicationRequest{
			ApplicationId: app.Id,
			ImagePolicyConfig: &functions.ImagePolicyConfig{
				IsPolicyEnabled: new(true), KeyDetails: []functions.KeyDetails{{KmsKeyId: new(testKey)}},
			},
		})
		require.NoError(t, err)

		ie := &invokeEnv{env: e, rt: rt, app: app, fn: fn}
		_, _, err = ie.invoke(t, "x", nil)
		assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
		assert.Empty(t, rt.startedSpecs())
	})
}

func TestFunction_Destinations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dest     functions.SuccessDestinationDetails
		name     string
		wantCode string
	}{
		{name: "stream", dest: functions.StreamSuccessDestinationDetails{StreamId: new("s1")}},
		{name: "queue", dest: functions.QueueSuccessDestinationDetails{QueueId: new("q1"), ChannelId: new("c")}},
		{name: "topic", dest: functions.NotificationSuccessDestinationDetails{TopicId: new("t1")}},
		{name: "none", dest: functions.NoneSuccessDestinationDetails{}},
		{
			name: "unknown stream", wantCode: "NotAuthorizedOrNotFound",
			dest: functions.StreamSuccessDestinationDetails{StreamId: new("gone")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.backend.SetDestinations(oci.Destinations{
				Exists: func(_, id string) bool { return id != "gone" },
			})
			app := e.mkApp(t, "app", nil)

			_, err := e.mgmt.CreateFunction(t.Context(), functions.CreateFunctionRequest{
				DisplayName: new("fn"), ApplicationId: app.Id, MemoryInMBs: new(int64(256)),
				SuccessDestination: tt.dest,
				SourceDetails:      functions.CreateContainerImageFunctionSourceDetails{Image: new("example/fn:1")},
			})
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.wantCode, svcErr(t, err).GetCode())
		})
	}
}

type delivered struct {
	kind    string
	target  string
	channel string
	body    string
}

func TestInvoke_DetachedDestinations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantBody func(t *testing.T, got string)
		name     string
		body     string
		wantKind string
	}{
		{
			name: "success to stream", body: "hello", wantKind: "STREAM",
			wantBody: func(t *testing.T, got string) { t.Helper(); assert.Contains(t, got, `"body":"hello"`) },
		},
		{
			name: "failure to queue", body: "fail", wantKind: "QUEUE",
			wantBody: func(t *testing.T, got string) {
				t.Helper()
				assert.Contains(t, got, "FunctionInvokeExecutionFailed")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := newFakeRuntime()
			e := newEnv(t, envOpts{rt: rt})
			e.backend.SetTimeoutUnit(testUnit)

			ch := make(chan delivered, 1)
			e.backend.SetDestinations(oci.Destinations{
				Stream: func(_ context.Context, id string, v []byte) error {
					ch <- delivered{kind: "STREAM", target: id, body: string(v)}

					return nil
				},
				Queue: func(_ context.Context, id, channel, content string) error {
					ch <- delivered{kind: "QUEUE", target: id, channel: channel, body: content}

					return nil
				},
			})

			app := e.mkApp(t, "app", nil)
			fn := e.mkFn(t, app, "fn", func(d *functions.CreateFunctionDetails) {
				d.SuccessDestination = functions.StreamSuccessDestinationDetails{StreamId: new("stream-1")}
				d.FailureDestination = functions.QueueFailureDestinationDetails{
					QueueId:   new("queue-1"),
					ChannelId: new("ch"),
				}
			})

			ie := &invokeEnv{env: e, rt: rt, app: app, fn: fn}
			_, _, err := ie.invoke(t, tt.body, func(r *functions.InvokeFunctionRequest) {
				r.FnInvokeType = functions.InvokeFunctionFnInvokeTypeDetached
			})
			require.NoError(t, err)

			got := <-ch
			assert.Equal(t, tt.wantKind, got.kind)
			tt.wantBody(t, got.body)

			if tt.wantKind == "QUEUE" {
				assert.Equal(t, "queue-1", got.target)
				assert.Equal(t, "ch", got.channel)
			} else {
				assert.Equal(t, "stream-1", got.target)
			}
		})
	}
}

func TestInvoke_ContainerLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		shape        functions.CreateApplicationDetailsShapeEnum
		wantPlatform string
	}{
		{shape: functions.CreateApplicationDetailsShapeX86, wantPlatform: "linux/amd64"},
		{shape: functions.CreateApplicationDetailsShapeArm, wantPlatform: "linux/arm64"},
		{shape: functions.CreateApplicationDetailsShapeX86Arm, wantPlatform: ""},
	}

	for _, tt := range tests {
		t.Run(string(tt.shape), func(t *testing.T) {
			t.Parallel()

			ie := newInvokeEnv(t, func(d *functions.CreateApplicationDetails) { d.Shape = tt.shape }, nil)

			_, _, err := ie.invoke(t, "x", nil)
			require.NoError(t, err)

			specs := ie.rt.startedSpecs()
			require.Len(t, specs, 1)
			assert.Equal(t, int64(256)<<20, specs[0].MemoryBytes)
			assert.Equal(t, tt.wantPlatform, specs[0].Platform)
		})
	}
}

func TestInvoke_ConcurrencyMemoryLimit(t *testing.T) {
	t.Parallel()

	rt := newFakeRuntime()
	e := newEnv(t, envOpts{rt: rt})
	e.backend.SetTimeoutUnit(time.Second)
	e.backend.Configure(oci.Settings{ConcurrencyMB: 256})

	app := e.mkApp(t, "app", nil)
	one := e.mkFn(t, app, "one", nil)
	two := e.mkFn(t, app, "two", nil)
	ie := &invokeEnv{env: e, rt: rt, app: app, fn: one}

	done := make(chan error, 1)

	c, err := functions.NewFunctionsInvokeClientWithConfigurationProvider(ie.prov, *one.InvokeEndpoint)
	require.NoError(t, err)

	go func() {
		resp, ierr := c.InvokeFunction(t.Context(), functions.InvokeFunctionRequest{
			FunctionId: one.Id, InvokeFunctionBody: io.NopCloser(strings.NewReader("block")),
		})
		if ierr == nil {
			_ = resp.Content.Close()
		}

		done <- ierr
	}()

	require.Eventually(t, func() bool { return e.backend.LiveContainers() == 1 && len(rt.recorded()) == 1 },
		5*time.Second, 5*time.Millisecond)

	_, _, err = ie.invokeFn(t, two, "x", nil)
	assert.Equal(t, http.StatusTooManyRequests, svcErr(t, err).GetHTTPStatusCode())

	close(rt.release)
	require.NoError(t, <-done)

	_, _, err = ie.invokeFn(t, two, "x", nil)
	require.NoError(t, err)
}

func TestInvoke_OutputCapture(t *testing.T) {
	t.Parallel()

	t.Run("sink receives only new lines", func(t *testing.T) {
		t.Parallel()

		ie := newInvokeEnv(t, func(d *functions.CreateApplicationDetails) {
			d.Logging = &functions.ApplicationLoggingConfig{
				LineFormat: functions.ApplicationLoggingConfigLineFormatJson,
			}
		}, nil)

		var got []oci.LogLine

		ie.backend.SetLogSink(func(l oci.LogLine) int {
			got = append(got, l)

			return 1
		})

		for _, b := range []string{"a", "b"} {
			_, _, err := ie.invoke(t, b, nil)
			require.NoError(t, err)
		}

		require.Len(t, got, 4)
		assert.Equal(t, "out:a", got[0].Text)
		assert.Equal(t, "stdout", got[0].Stream)
		assert.Equal(t, "err:a", got[1].Text)
		assert.Equal(t, "stderr", got[1].Stream)
		assert.Equal(t, "out:b", got[2].Text)
		assert.Equal(t, "JSON", got[2].LineFormat)
		assert.NotEmpty(t, got[2].CallID)
	})

	t.Run("syslog when no log takes the line", func(t *testing.T) {
		t.Parallel()

		l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
		require.NoError(t, err)

		t.Cleanup(func() { _ = l.Close() })

		lines := make(chan string, 4)

		go func() {
			for {
				c, aerr := l.Accept()
				if aerr != nil {
					return
				}

				sc := bufio.NewScanner(c)
				for sc.Scan() {
					lines <- sc.Text()
				}

				_ = c.Close()
			}
		}()

		ie := newInvokeEnv(t, func(d *functions.CreateApplicationDetails) {
			d.SyslogUrl = new("tcp://" + l.Addr().String())
		}, nil)

		_, _, err = ie.invoke(t, "z", nil)
		require.NoError(t, err)

		out, errLine := <-lines, <-lines
		assert.True(t, strings.HasPrefix(out, "<14>1 "), out)
		assert.True(t, strings.HasSuffix(out, " - out:z"), out)
		assert.True(t, strings.HasPrefix(errLine, "<11>1 "), errLine)
		assert.Contains(t, out, " gopherstack app ")
	})

	t.Run("sink wins over syslog", func(t *testing.T) {
		t.Parallel()

		l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
		require.NoError(t, err)

		accepted := make(chan struct{}, 1)

		go func() {
			if c, aerr := l.Accept(); aerr == nil {
				accepted <- struct{}{}
				_ = c.Close()
			}
		}()

		ie := newInvokeEnv(t, func(d *functions.CreateApplicationDetails) {
			d.SyslogUrl = new("tcp://" + l.Addr().String())
		}, nil)
		ie.backend.SetLogSink(func(oci.LogLine) int { return 1 })

		_, _, err = ie.invoke(t, "z", nil)
		require.NoError(t, err)
		require.NoError(t, l.Close())

		select {
		case <-accepted:
			t.Fatal("syslog connection opened although OCI Logging took the lines")
		default:
		}
	})
}

func TestInvoke_ProvisionedConcurrency(t *testing.T) {
	t.Parallel()

	rt := newFakeRuntime()
	e := newEnv(t, envOpts{rt: rt})
	e.backend.SetTimeoutUnit(testUnit)

	app := e.mkApp(t, "app", nil)
	fn := e.mkFn(t, app, "fn", func(d *functions.CreateFunctionDetails) {
		d.ProvisionedConcurrencyConfig = functions.ConstantProvisionedConcurrencyConfig{Count: new(2)}
	})

	e.backend.Sweep()
	require.Eventually(t, func() bool { return e.backend.LiveContainers() == 2 }, 5*time.Second, 5*time.Millisecond)

	e.clock.Advance(time.Hour)
	e.backend.ReapIdle()
	assert.Equal(t, 2, e.backend.LiveContainers())

	ie := &invokeEnv{env: e, rt: rt, app: app, fn: fn}
	_, _, err := ie.invoke(t, "x", nil)
	require.NoError(t, err)
	assert.Len(t, rt.startedSpecs(), 2)
}
