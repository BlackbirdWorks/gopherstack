package azurearm_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerinstance/armcontainerinstance/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const aciEventWait = 10 * time.Second

func (s *aciStack) createWith(
	t *testing.T, name string, o aciOpts, mutate func(g *armcontainerinstance.ContainerGroup),
) (*armcontainerinstance.ContainerGroup, error) {
	t.Helper()

	g := o.group()
	if mutate != nil {
		mutate(&g)
	}

	poller, err := s.factory.NewContainerGroupsClient().BeginCreateOrUpdate(t.Context(), "rg", name, g, nil)
	if err != nil {
		return nil, err
	}

	res, err := poller.PollUntilDone(t.Context(), pollFast())
	if err != nil {
		return nil, err
	}

	return &res.ContainerGroup, nil
}

func (s *aciStack) putRaw(t *testing.T, path, body string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, s.url+path, strings.NewReader(body))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	out, _ := io.ReadAll(resp.Body)
	require.Less(t, resp.StatusCode, 300, string(out))
}

// tickPause makes probe waits advance only when the test sends a tick.
func tickPause() (func(context.Context, time.Duration) bool, func(*testing.T)) {
	ch := make(chan struct{})

	pause := func(ctx context.Context, _ time.Duration) bool {
		select {
		case <-ch:
			return true
		case <-ctx.Done():
			return false
		}
	}

	tick := func(t *testing.T) {
		t.Helper()

		select {
		case ch <- struct{}{}:
		case <-time.After(aciEventWait):
			t.Fatal("no probe was waiting for a tick")
		}
	}

	return pause, tick
}

func execCmd(cmd ...string) *armcontainerinstance.ContainerExec {
	return &armcontainerinstance.ContainerExec{Command: toPtrs(cmd)}
}

func setProbe(live, ready *armcontainerinstance.ContainerProbe) func(*armcontainerinstance.ContainerGroup) {
	return func(g *armcontainerinstance.ContainerGroup) {
		p := g.Properties.Containers[0].Properties
		p.LivenessProbe, p.ReadinessProbe = live, ready
	}
}

func TestACISDK_ExtValidation(t *testing.T) {
	t.Parallel()

	initWith := func(name, image string) *armcontainerinstance.InitContainerDefinition {
		return &armcontainerinstance.InitContainerDefinition{
			Name:       new(name),
			Properties: &armcontainerinstance.InitContainerPropertiesDefinition{Image: new(image)},
		}
	}
	probe := func(mut func(p *armcontainerinstance.ContainerProbe)) func(*armcontainerinstance.ContainerGroup) {
		p := &armcontainerinstance.ContainerProbe{Exec: execCmd("true")}
		mut(p)

		return setProbe(p, nil)
	}

	tests := []struct {
		mutate  func(g *armcontainerinstance.ContainerGroup)
		name    string
		wantMsg string
	}{
		{probe(func(p *armcontainerinstance.ContainerProbe) { p.Exec = nil }), "probe-empty", "exec or httpGet"},
		{probe(func(p *armcontainerinstance.ContainerProbe) {
			p.HTTPGet = &armcontainerinstance.ContainerHTTPGet{Port: new(int32(80))}
		}), "probe-both", "only one"},
		{probe(func(p *armcontainerinstance.ContainerProbe) {
			p.Exec = nil
			p.HTTPGet = &armcontainerinstance.ContainerHTTPGet{Port: new(int32(70000))}
		}), "probe-port", "out of range"},
		{probe(func(p *armcontainerinstance.ContainerProbe) {
			p.Exec = nil
			p.HTTPGet = &armcontainerinstance.ContainerHTTPGet{
				Port: new(int32(80)), Scheme: new(armcontainerinstance.Scheme("ftp")),
			}
		}), "probe-scheme", "scheme"},
		{
			probe(func(p *armcontainerinstance.ContainerProbe) { p.PeriodSeconds = new(int32(-1)) }),
			"probe-negative",
			"negative",
		},
		{
			probe(func(p *armcontainerinstance.ContainerProbe) { p.Exec = execCmd() }),
			"probe-no-command",
			"needs a command",
		},
		{func(g *armcontainerinstance.ContainerGroup) {
			g.Properties.InitContainers = []*armcontainerinstance.InitContainerDefinition{initWith("main", "busybox:1")}
		}, "init-dup-name", "duplicate"},
		{func(g *armcontainerinstance.ContainerGroup) {
			g.Properties.InitContainers = []*armcontainerinstance.InitContainerDefinition{initWith("i", "")}
		}, "init-no-image", "needs an image"},
		{func(g *armcontainerinstance.ContainerGroup) {
			ic := initWith("i", "busybox:1")
			ic.Properties.VolumeMounts = []*armcontainerinstance.VolumeMount{
				{Name: new("nope"), MountPath: new("/x")},
			}
			g.Properties.InitContainers = []*armcontainerinstance.InitContainerDefinition{ic}
		}, "init-bad-mount", "unknown volume"},
		{
			func(g *armcontainerinstance.ContainerGroup) { g.Zones = toPtrs([]string{"1", "2"}) },
			"zones-two",
			"only one",
		},
		{
			func(g *armcontainerinstance.ContainerGroup) { g.Zones = toPtrs([]string{"4"}) },
			"zone-value",
			"must be 1, 2 or 3",
		},
		{func(g *armcontainerinstance.ContainerGroup) {
			g.Properties.DNSConfig = &armcontainerinstance.DNSConfiguration{NameServers: toPtrs([]string{"not-an-ip"})}
		}, "dns-bad-ip", "not an IP"},
		{func(g *armcontainerinstance.ContainerGroup) {
			g.Properties.DNSConfig = &armcontainerinstance.DNSConfiguration{}
		}, "dns-empty", "nameServers"},
		{func(g *armcontainerinstance.ContainerGroup) {
			g.Properties.Diagnostics = &armcontainerinstance.ContainerGroupDiagnostics{
				LogAnalytics: &armcontainerinstance.LogAnalytics{WorkspaceID: new("w")},
			}
		}, "diag-no-key", "workspaceKey"},
		{func(g *armcontainerinstance.ContainerGroup) {
			g.Properties.Diagnostics = &armcontainerinstance.ContainerGroupDiagnostics{
				LogAnalytics: &armcontainerinstance.LogAnalytics{
					WorkspaceID: new("w"), WorkspaceKey: new("k"),
					LogType: new(armcontainerinstance.LogAnalyticsLogType("Other")),
				},
			}
		}, "diag-log-type", "logType"},
		{func(g *armcontainerinstance.ContainerGroup) {
			g.Properties.Volumes = []*armcontainerinstance.Volume{
				{Name: new("g"), GitRepo: &armcontainerinstance.GitRepoVolume{}},
			}
		}, "git-no-repo", "needs a repository"},
		{func(g *armcontainerinstance.ContainerGroup) {
			g.Properties.Volumes = []*armcontainerinstance.Volume{{
				Name: new("g"), GitRepo: &armcontainerinstance.GitRepoVolume{
					Repository: new("https://x/y.git"), Directory: new("../escape"),
				},
			}}
		}, "git-dir", "directory"},
		{func(g *armcontainerinstance.ContainerGroup) {
			g.Properties.SubnetIDs = []*armcontainerinstance.ContainerGroupSubnetID{
				{ID: new("/subscriptions/" + aciSub +
					"/resourceGroups/rg/providers/Microsoft.Network/virtualNetworks/none/subnets/s")},
			}
			g.Properties.IPAddress = &armcontainerinstance.IPAddress{
				Type:  new(armcontainerinstance.ContainerGroupIPAddressTypePrivate),
				Ports: []*armcontainerinstance.Port{{Port: new(int32(80))}},
			}
		}, "subnet-missing", "was not found"},
		{func(g *armcontainerinstance.ContainerGroup) {
			g.Properties.SubnetIDs = []*armcontainerinstance.ContainerGroupSubnetID{{ID: new("x")}}
			g.Properties.IPAddress = &armcontainerinstance.IPAddress{
				Type:  new(armcontainerinstance.ContainerGroupIPAddressTypePublic),
				Ports: []*armcontainerinstance.Port{{Port: new(int32(80))}},
			}
		}, "subnet-public", "Private"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newACIStack(t)

			_, err := s.createWith(
				t,
				"v",
				aciOpts{ports: []*armcontainerinstance.ContainerPort{{Port: new(int32(80))}}},
				tc.mutate,
			)

			var re *azcore.ResponseError

			require.ErrorAs(t, err, &re)
			assert.Equal(t, http.StatusBadRequest, re.StatusCode)
			assert.Equal(t, "InvalidRequestContent", re.ErrorCode)
			assert.Contains(t, re.Error(), tc.wantMsg)
			assert.Zero(t, s.rt.live())
		})
	}
}

func TestACISDK_SubnetDelegation(t *testing.T) {
	t.Parallel()

	s := newACIStack(t)
	vnet := "/subscriptions/" + aciSub + "/resourceGroups/rg/providers/Microsoft.Network/virtualNetworks/vn"
	s.putRaw(t, "/subscriptions/"+aciSub+"/resourcegroups/rg?api-version=2021-04-01", `{"location":"local"}`)
	s.putRaw(
		t,
		vnet+"?api-version=2024-05-01",
		`{"location":"local","properties":{"addressSpace":{"addressPrefixes":["10.0.0.0/16"]}}}`,
	)
	s.putRaw(t, vnet+"/subnets/plain?api-version=2024-05-01", `{"properties":{"addressPrefix":"10.0.1.0/24"}}`)
	s.putRaw(
		t,
		vnet+"/subnets/aci?api-version=2024-05-01",
		`{"properties":{"addressPrefix":"10.0.2.0/24","delegations":[{"name":"d","properties":{"serviceName":"Microsoft.ContainerInstance/containerGroups"}}]}}`,
	)

	private := &armcontainerinstance.IPAddress{
		Type:  new(armcontainerinstance.ContainerGroupIPAddressTypePrivate),
		Ports: []*armcontainerinstance.Port{{Port: new(int32(80))}},
	}
	withSubnet := func(sub string) func(*armcontainerinstance.ContainerGroup) {
		return func(g *armcontainerinstance.ContainerGroup) {
			g.Properties.IPAddress = private
			g.Properties.SubnetIDs = []*armcontainerinstance.ContainerGroupSubnetID{
				{ID: new(vnet + "/subnets/" + sub), Name: new(sub)},
			}
		}
	}
	opts := aciOpts{ports: []*armcontainerinstance.ContainerPort{{Port: new(int32(80))}}}

	_, err := s.createWith(t, "plain", opts, withSubnet("plain"))

	var re *azcore.ResponseError

	require.ErrorAs(t, err, &re)
	assert.Contains(t, re.Error(), "must be delegated to Microsoft.ContainerInstance/containerGroups")

	g, err := s.createWith(t, "ok", opts, withSubnet("aci"))
	require.NoError(t, err)
	require.Len(t, g.Properties.SubnetIDs, 1)
	assert.Equal(t, "Private", string(*g.Properties.IPAddress.Type))
	assert.True(t, strings.HasSuffix(*g.Properties.SubnetIDs[0].ID, "/subnets/aci"))
}

func TestACISDK_ExtRoundTripKeepsSecretsWriteOnly(t *testing.T) {
	t.Parallel()

	s := newACIStack(t)
	s.rt.scripts["alpine/git"] = []int{0}
	s.rt.scripts["busybox:1"] = []int{0}

	_, err := s.createWith(t, "rt", aciOpts{policy: armcontainerinstance.ContainerGroupRestartPolicyAlways},
		func(g *armcontainerinstance.ContainerGroup) {
			g.Zones = toPtrs([]string{"2"})
			g.Properties.DNSConfig = &armcontainerinstance.DNSConfiguration{
				NameServers: toPtrs(
					[]string{"10.0.0.10", "10.0.0.11"},
				),
				Options:       new("ndots:2"),
				SearchDomains: new("corp.local svc.local"),
			}
			g.Properties.Diagnostics = &armcontainerinstance.ContainerGroupDiagnostics{
				LogAnalytics: &armcontainerinstance.LogAnalytics{
					WorkspaceID: new("ws-id"), WorkspaceKey: new("ws-secret-key"),
					LogType:  new(armcontainerinstance.LogAnalyticsLogTypeContainerInsights),
					Metadata: map[string]*string{"k": new("v")},
				},
			}
			g.Properties.Volumes = []*armcontainerinstance.Volume{{
				Name: new("src"), GitRepo: &armcontainerinstance.GitRepoVolume{
					Repository: new("https://example.invalid/r.git"), Directory: new("."), Revision: new("main"),
				},
			}}
			g.Properties.InitContainers = []*armcontainerinstance.InitContainerDefinition{{
				Name: new("setup"), Properties: &armcontainerinstance.InitContainerPropertiesDefinition{
					Image: new("busybox:1"), Command: toPtrs([]string{"true"}),
					EnvironmentVariables: []*armcontainerinstance.EnvironmentVariable{
						{Name: new("TOKEN"), SecureValue: new("init-secret")}, {Name: new("PLAIN"), Value: new("p")},
					},
				},
			}}
			setProbe(
				&armcontainerinstance.ContainerProbe{
					Exec:                execCmd("cat", "/tmp/ok"),
					InitialDelaySeconds: new(int32(3)),
				},
				&armcontainerinstance.ContainerProbe{HTTPGet: &armcontainerinstance.ContainerHTTPGet{
					Port: new(int32(8080)), Path: new("/ready"),
					HTTPHeaders: []*armcontainerinstance.HTTPHeader{{Name: new("X-A"), Value: new("b")}},
				}},
			)(
				g,
			)
		})
	require.NoError(t, err)

	got := s.get(t, "rg", "rt")
	assert.Equal(t, []*string{new("2")}, got.Zones)

	dns := got.Properties.DNSConfig
	require.NotNil(t, dns)
	assert.Equal(t, "ndots:2", *dns.Options)
	assert.Equal(t, "corp.local svc.local", *dns.SearchDomains)
	assert.Len(t, dns.NameServers, 2)

	la := got.Properties.Diagnostics.LogAnalytics
	assert.Equal(t, "ws-id", *la.WorkspaceID)
	assert.Nil(t, la.WorkspaceKey, "workspaceKey is write-only")
	assert.Equal(t, "ContainerInsights", string(*la.LogType))

	assert.Equal(t, "https://example.invalid/r.git", *got.Properties.Volumes[0].GitRepo.Repository)
	assert.Equal(t, "main", *got.Properties.Volumes[0].GitRepo.Revision)

	ic := got.Properties.InitContainers[0]
	assert.Equal(t, "setup", *ic.Name)
	require.Len(t, ic.Properties.EnvironmentVariables, 2)

	for _, e := range ic.Properties.EnvironmentVariables {
		assert.Nil(t, e.SecureValue)

		if *e.Name == "TOKEN" {
			assert.Nil(t, e.Value)
		}
	}

	live := got.Properties.Containers[0].Properties.LivenessProbe
	assert.Equal(t, int32(10), *live.PeriodSeconds)
	assert.Equal(t, int32(3), *live.FailureThreshold)
	assert.Equal(t, int32(1), *live.SuccessThreshold)
	assert.Equal(t, int32(1), *live.TimeoutSeconds)
	assert.Equal(t, int32(3), *live.InitialDelaySeconds)

	ready := got.Properties.Containers[0].Properties.ReadinessProbe.HTTPGet
	assert.Equal(t, "http", string(*ready.Scheme))
	assert.Equal(t, "/ready", *ready.Path)
	assert.Equal(t, "X-A", *ready.HTTPHeaders[0].Name)

	snap := string(s.handler.Snapshot(t.Context()))
	assert.Contains(t, snap, "ws-secret-key", "stored like the other azurearm credentials")
}

func TestACISDK_InitContainersRunInOrderBeforeApps(t *testing.T) {
	t.Parallel()

	s := newACIStack(t)
	s.rt.scripts["init-a"] = []int{0}
	s.rt.scripts["init-b"] = []int{0}

	g, err := s.createWith(
		t,
		"init",
		aciOpts{image: "app-img", policy: armcontainerinstance.ContainerGroupRestartPolicyAlways},
		func(g *armcontainerinstance.ContainerGroup) {
			for _, n := range []string{"init-a", "init-b"} {
				g.Properties.InitContainers = append(
					g.Properties.InitContainers,
					&armcontainerinstance.InitContainerDefinition{
						Name:       new(n),
						Properties: &armcontainerinstance.InitContainerPropertiesDefinition{Image: new(n)},
					},
				)
			}
		},
	)
	require.NoError(t, err)

	images := make([]string, 0, 3)
	for _, sp := range s.rt.started() {
		images = append(images, sp.Image)
	}

	assert.Equal(t, []string{"init-a", "init-b", "app-img"}, images)
	assert.Equal(t, "Running", *g.Properties.InstanceView.State)
	require.Len(t, g.Properties.InitContainers, 2)

	for _, ic := range g.Properties.InitContainers {
		cs := ic.Properties.InstanceView.CurrentState
		assert.Equal(t, "Terminated", *cs.State)
		assert.Equal(t, int32(0), *cs.ExitCode)
		assert.Equal(t, "Completed", *cs.DetailStatus)
	}
}

func TestACISDK_InitContainerFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policy      armcontainerinstance.ContainerGroupRestartPolicy
		name        string
		scripts     []int
		wantFailed  bool
		wantRestart int32
	}{
		{armcontainerinstance.ContainerGroupRestartPolicyNever, "never-fails-group", []int{3}, true, 0},
		{armcontainerinstance.ContainerGroupRestartPolicyOnFailure, "on-failure-retries", []int{1, 0}, false, 1},
		{armcontainerinstance.ContainerGroupRestartPolicyAlways, "always-retries", []int{2, 2, 0}, false, 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newACIStack(t)
			s.rt.scripts["init-x"] = tc.scripts

			_, err := s.createWith(t, "initf", aciOpts{image: "app-img", policy: tc.policy},
				func(g *armcontainerinstance.ContainerGroup) {
					g.Properties.InitContainers = []*armcontainerinstance.InitContainerDefinition{{
						Name:       new("x"),
						Properties: &armcontainerinstance.InitContainerPropertiesDefinition{Image: new("init-x")},
					}}
				})

			if tc.wantFailed {
				require.Error(t, err)

				got := s.get(t, "rg", "initf")
				assert.Equal(t, "Failed", *got.Properties.ProvisioningState)
				assert.Equal(t, "Failed", *got.Properties.InstanceView.State)
				assert.Contains(t, *got.Properties.InstanceView.Events[0].Message, "init container x")
				assert.Contains(
					t,
					*got.Properties.InstanceView.Events[0].Message,
					"exited unsuccessfully with exit code 3",
				)

				cs := got.Properties.InitContainers[0].Properties.InstanceView.CurrentState
				assert.Equal(t, int32(3), *cs.ExitCode)
				assert.Equal(t, "Error", *cs.DetailStatus)

				for _, sp := range s.rt.started() {
					assert.NotEqual(t, "app-img", sp.Image, "app containers must not start after a failed init")
				}

				return
			}

			require.NoError(t, err)

			require.Eventually(t, func() bool {
				got := s.get(t, "rg", "initf")

				return *got.Properties.InitContainers[0].Properties.InstanceView.RestartCount == tc.wantRestart &&
					*got.Properties.InstanceView.State == "Running"
			}, aciEventWait, 5*time.Millisecond)
		})
	}
}

func TestACISDK_DNSConfigMountsResolvConf(t *testing.T) {
	t.Parallel()

	s := newACIStack(t)

	var (
		mu      sync.Mutex
		content string
	)

	s.rt.onStart = func(spec container.Spec) {
		for _, m := range spec.Mounts {
			if src, ok := strings.CutSuffix(m, ":/etc/resolv.conf:ro"); ok {
				b, err := os.ReadFile(filepath.Clean(src))
				if err == nil {
					mu.Lock()
					content = string(b)
					mu.Unlock()
				}
			}
		}
	}

	_, err := s.createWith(t, "dns", aciOpts{policy: armcontainerinstance.ContainerGroupRestartPolicyAlways},
		func(g *armcontainerinstance.ContainerGroup) {
			g.Properties.DNSConfig = &armcontainerinstance.DNSConfiguration{
				NameServers: toPtrs(
					[]string{"10.1.1.1"},
				),
				Options:       new("ndots:3"),
				SearchDomains: new("a.local b.local"),
			}
		})
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()

	assert.Equal(t, "nameserver 10.1.1.1\nsearch a.local b.local\noptions ndots:3\n", content)
}

func TestACISDK_LivenessProbe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policy      armcontainerinstance.ContainerGroupRestartPolicy
		name        string
		wantRestart bool
	}{
		{armcontainerinstance.ContainerGroupRestartPolicyAlways, "always-restarts", true},
		{armcontainerinstance.ContainerGroupRestartPolicyOnFailure, "on-failure-restarts", true},
		{armcontainerinstance.ContainerGroupRestartPolicyNever, "never-only-reports", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newACIStack(t)
			pause, tick := tickPause()
			s.engine.SetProbeHooks(pause, nil)

			var healthy atomic.Bool

			s.rt.execFn = func(container.Spec, []string) int {
				if healthy.Load() {
					return 0
				}

				return 1
			}

			_, err := s.createWith(
				t,
				"live",
				aciOpts{policy: tc.policy},
				setProbe(
					&armcontainerinstance.ContainerProbe{Exec: execCmd("check"), FailureThreshold: new(int32(3))},
					nil,
				),
			)
			require.NoError(t, err)

			tick(t)
			tick(t)

			restarts := func() int32 {
				return *s.get(t, "rg", "live").Properties.Containers[0].Properties.InstanceView.RestartCount
			}

			if tc.wantRestart {
				require.Eventually(t, func() bool { return restarts() == 1 }, aciEventWait, 5*time.Millisecond)
			} else {
				require.Eventually(t, func() bool {
					for _, e := range s.get(t, "rg", "live").Properties.Containers[0].Properties.InstanceView.Events {
						if *e.Name == "Unhealthy" && *e.Count >= 3 {
							return true
						}
					}

					return false
				}, aciEventWait, 5*time.Millisecond)
				assert.Zero(t, restarts(), "restartPolicy supersedes liveness restarts")
			}

			names := make([]string, 0)

			for _, e := range s.get(t, "rg", "live").Properties.Containers[0].Properties.InstanceView.Events {
				names = append(names, *e.Name)
			}

			assert.Contains(t, names, "Unhealthy")

			if tc.wantRestart {
				assert.Contains(t, names, "Killing")
			} else {
				assert.NotContains(t, names, "Killing")
			}
		})
	}
}

func TestACISDK_ReadinessProbeGatesTraffic(t *testing.T) {
	t.Parallel()

	s := newACIStack(t)
	pause, tick := tickPause()

	var (
		ready   atomic.Bool
		lastReq atomic.Value
	)

	s.engine.SetProbeHooks(pause, func(_ context.Context, p azurearm.ACIHTTPProbe) (int, error) {
		lastReq.Store(p)

		if ready.Load() {
			return http.StatusNoContent, nil
		}

		return http.StatusServiceUnavailable, nil
	})

	s.rt.onStart = func(spec container.Spec) {
		for _, p := range spec.Ports {
			parts := strings.Split(p, ":")
			if len(parts) != 3 || parts[2] != "8080" {
				continue
			}

			var lc net.ListenConfig

			ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:"+parts[1])
			if err != nil {
				return
			}

			go func() {
				defer ln.Close()

				for {
					c, aerr := ln.Accept()
					if aerr != nil {
						return
					}

					_, _ = c.Write([]byte("served"))
					_ = c.Close()
				}
			}()

			t.Cleanup(func() { _ = ln.Close() })
		}
	}

	g, err := s.createWith(t, "ready", aciOpts{
		policy: armcontainerinstance.ContainerGroupRestartPolicyAlways,
		ip: &armcontainerinstance.IPAddress{
			Type:  new(armcontainerinstance.ContainerGroupIPAddressTypePublic),
			Ports: []*armcontainerinstance.Port{{Port: new(int32(8080))}},
		},
		ports: []*armcontainerinstance.ContainerPort{{Port: new(int32(8080))}},
	}, setProbe(nil, &armcontainerinstance.ContainerProbe{
		HTTPGet: &armcontainerinstance.ContainerHTTPGet{
			Port: new(int32(8080)), Path: new("ready"), Scheme: new(armcontainerinstance.SchemeHTTPS),
			HTTPHeaders: []*armcontainerinstance.HTTPHeader{{Name: new("X-Probe"), Value: new("yes")}},
		},
		SuccessThreshold: new(int32(1)),
	}))
	require.NoError(t, err)

	assert.Equal(t, "Running", *g.Properties.InstanceView.State, "an unready container keeps running")

	var front string

	for _, e := range g.Properties.InstanceView.Events {
		if *e.Name == "PortPublished" {
			front = (*e.Message)[strings.Index(*e.Message, "127.0.0.1:"):]
		}
	}

	require.NotEmpty(t, front)

	dial := func() string {
		c, derr := net.DialTimeout("tcp", front, time.Second)
		if derr != nil {
			return "refused"
		}
		defer c.Close()

		_ = c.SetReadDeadline(time.Now().Add(time.Second))
		b, _ := io.ReadAll(c)

		return string(b)
	}

	require.Eventually(t, func() bool {
		_, ok := lastReq.Load().(azurearm.ACIHTTPProbe)

		return ok
	}, aciEventWait, 5*time.Millisecond)
	assert.Empty(t, dial(), "traffic is dropped while the readiness probe fails")

	req, _ := lastReq.Load().(azurearm.ACIHTTPProbe)
	assert.True(t, strings.HasPrefix(req.URL, "https://127.0.0.1:"), req.URL)
	assert.True(t, strings.HasSuffix(req.URL, "/ready"), req.URL)
	assert.Equal(t, "yes", req.Headers.Get("X-Probe"))

	ready.Store(true)
	tick(t)

	require.Eventually(t, func() bool { return dial() == "served" }, aciEventWait, 10*time.Millisecond)
}

func TestACISDK_GitRepoVolume(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantFail  string
		directory string
		exit      []int
	}{
		{name: "clones-into-volume", exit: []int{0}, directory: "code"},
		{name: "clone-failure-scrubs-credentials", exit: []int{128}, wantFail: "git exited with code 128"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newACIStack(t)
			s.rt.scripts["alpine/git"] = tc.exit
			s.rt.logs["alpine/git"] = "fatal: unable to access 'https://user:tok3n@example.invalid/r.git/'"

			var (
				mu   sync.Mutex
				git  container.Spec
				mnts []string
			)

			s.rt.onStart = func(spec container.Spec) {
				mu.Lock()
				defer mu.Unlock()

				if spec.Image == "alpine/git" {
					git = spec
				} else {
					mnts = spec.Mounts
				}
			}

			repo := "https://user:tok3n@example.invalid/r.git"
			g, err := s.createWith(t, "git", aciOpts{
				policy: armcontainerinstance.ContainerGroupRestartPolicyAlways,
				mounts: []*armcontainerinstance.VolumeMount{{Name: new("src"), MountPath: new("/src")}},
				volumes: []*armcontainerinstance.Volume{{
					Name: new("src"),
					GitRepo: &armcontainerinstance.GitRepoVolume{
						Repository: new(repo), Directory: new(tc.directory), Revision: new("v1"),
					},
				}},
			}, nil)

			mu.Lock()
			defer mu.Unlock()

			assert.Equal(t, "alpine/git", git.Image)
			assert.Contains(t, git.Env, "ACI_REPO="+repo)
			assert.Contains(t, git.Env, "ACI_DIR="+tc.directory)
			assert.Contains(t, git.Env, "ACI_REV=v1")

			if tc.wantFail != "" {
				require.Error(t, err)

				got := s.get(t, "rg", "git")
				msg := *got.Properties.InstanceView.Events[0].Message
				assert.Contains(t, msg, tc.wantFail)
				assert.NotContains(t, msg, "tok3n", "credentials in the repository URL are never surfaced")
				assert.Empty(t, mnts, "app containers must not start")

				return
			}

			require.NoError(t, err)
			require.Len(t, mnts, 1)
			assert.True(t, strings.HasSuffix(mnts[0], ":/src"))
			assert.Equal(t, "Running", *g.Properties.InstanceView.State)
			assert.NotContains(t, strconv.Quote(*g.Properties.Volumes[0].GitRepo.Directory), "..")
		})
	}
}
