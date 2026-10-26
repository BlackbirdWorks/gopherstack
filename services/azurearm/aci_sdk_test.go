package azurearm_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerinstance/armcontainerinstance/v2"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const aciSub = "00000000-0000-0000-0000-000000000000"

type aciCred struct{}

func (aciCred) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "emulator"}, nil
}

type aciStack struct {
	factory *armcontainerinstance.ClientFactory
	rt      *fakeACIRuntime
	shares  *fakeShares
	engine  *azurearm.ACIEngine
	url     string
	client  *http.Client
	handler *azurearm.Handler
}

func newACIStackWith(t *testing.T, rt *fakeACIRuntime) *aciStack {
	t.Helper()

	armSvc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	armH, ok := armSvc.(*azurearm.Handler)
	require.True(t, ok)

	p, ok := armH.Registry.ProviderNamed("Microsoft.ContainerInstance")
	require.True(t, ok)

	cp, ok := p.(*azurearm.ContainerInstanceProvider)
	require.True(t, ok)

	shares := &fakeShares{trees: map[string]azurearm.ACIShareTree{}}
	engine := cp.Engine()
	engine.SetFileShares(shares)
	engine.SetTiming(func(int) time.Duration { return 0 }, time.Minute)

	if rt != nil {
		engine.SetRuntime(func() (azurearm.ACIRuntime, error) { return rt, nil })
	}

	t.Cleanup(func() { armH.Shutdown(context.Background()) })

	e := echo.New()
	e.Any("/*", armH.Handler())

	srv := httptest.NewTLSServer(e)
	t.Cleanup(srv.Close)

	f, err := armcontainerinstance.NewClientFactory(aciSub, aciCred{}, &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: srv.URL, Audience: "https://management.core.windows.net/"},
		}},
		Transport: srv.Client(),
		Retry:     policy.RetryOptions{MaxRetries: -1},
	})
	require.NoError(t, err)

	return &aciStack{factory: f, rt: rt, shares: shares, engine: engine, url: srv.URL, client: srv.Client(), handler: armH}
}

func newACIStack(t *testing.T) *aciStack {
	t.Helper()

	return newACIStackWith(t, newFakeACIRuntime())
}

func pollFast() *runtime.PollUntilDoneOptions {
	return &runtime.PollUntilDoneOptions{Frequency: time.Millisecond}
}

type aciOpts struct {
	policy  armcontainerinstance.ContainerGroupRestartPolicy
	image   string
	command []string
	env     []*armcontainerinstance.EnvironmentVariable
	mounts  []*armcontainerinstance.VolumeMount
	volumes []*armcontainerinstance.Volume
	creds   []*armcontainerinstance.ImageRegistryCredential
	ip      *armcontainerinstance.IPAddress
	ports   []*armcontainerinstance.ContainerPort
	limits  *armcontainerinstance.ResourceLimits
}

func (o aciOpts) group() armcontainerinstance.ContainerGroup {
	image := o.image
	if image == "" {
		image = "busybox:1"
	}

	pol := o.policy
	if pol == "" {
		pol = armcontainerinstance.ContainerGroupRestartPolicyNever
	}

	return armcontainerinstance.ContainerGroup{
		Location: new("local"),
		Tags:     map[string]*string{"env": new("test")},
		Properties: &armcontainerinstance.ContainerGroupPropertiesProperties{
			OSType:                   new(armcontainerinstance.OperatingSystemTypesLinux),
			RestartPolicy:            &pol,
			IPAddress:                o.ip,
			Volumes:                  o.volumes,
			ImageRegistryCredentials: o.creds,
			Containers: []*armcontainerinstance.Container{{
				Name: new("main"),
				Properties: &armcontainerinstance.ContainerProperties{
					Image: &image, Command: toPtrs(o.command), EnvironmentVariables: o.env, VolumeMounts: o.mounts,
					Ports: o.ports,
					Resources: &armcontainerinstance.ResourceRequirements{
						Requests: &armcontainerinstance.ResourceRequests{CPU: new(1.0), MemoryInGB: new(1.5)},
						Limits:   o.limits,
					},
				},
			}},
		},
	}
}

func toPtrs(in []string) []*string {
	out := make([]*string, 0, len(in))
	for i := range in {
		out = append(out, &in[i])
	}

	return out
}

func (s *aciStack) create(t *testing.T, rg, name string, o aciOpts) (*armcontainerinstance.ContainerGroup, error) {
	t.Helper()

	poller, err := s.factory.NewContainerGroupsClient().BeginCreateOrUpdate(t.Context(), rg, name, o.group(), nil)
	if err != nil {
		return nil, err
	}

	res, err := poller.PollUntilDone(t.Context(), pollFast())
	if err != nil {
		return nil, err
	}

	return &res.ContainerGroup, nil
}

func (s *aciStack) get(t *testing.T, rg, name string) *armcontainerinstance.ContainerGroup {
	t.Helper()

	res, err := s.factory.NewContainerGroupsClient().Get(t.Context(), rg, name, nil)
	require.NoError(t, err)

	return &res.ContainerGroup
}

func (s *aciStack) waitState(t *testing.T, rg, name, want string) *armcontainerinstance.ContainerGroup {
	t.Helper()

	var g *armcontainerinstance.ContainerGroup

	require.Eventually(t, func() bool {
		g = s.get(t, rg, name)
		iv := g.Properties.InstanceView

		return iv != nil && iv.State != nil && *iv.State == want
	}, 10*time.Second, 5*time.Millisecond, "group never reached %s", want)

	return g
}

func contState(g *armcontainerinstance.ContainerGroup) *armcontainerinstance.ContainerState {
	return g.Properties.Containers[0].Properties.InstanceView.CurrentState
}

func TestACISDK_Lifecycle(t *testing.T) {
	t.Parallel()

	s := newACIStack(t)
	ctx := t.Context()
	groups := s.factory.NewContainerGroupsClient()

	g, err := s.create(t, "rg1", "grp1", aciOpts{
		policy: armcontainerinstance.ContainerGroupRestartPolicyAlways, command: []string{"sleep", "1000"},
		env: []*armcontainerinstance.EnvironmentVariable{
			{Name: new("PLAIN"), Value: new("v")}, {Name: new("SECRET"), SecureValue: new("hunter2")},
		},
		limits: &armcontainerinstance.ResourceLimits{CPU: new(2.0), MemoryInGB: new(2.0)},
	})
	require.NoError(t, err)
	assert.Equal(t, "Succeeded", *g.Properties.ProvisioningState)
	assert.Equal(t, "Running", *g.Properties.InstanceView.State)
	assert.Equal(t, "test", *g.Tags["env"])

	cs := contState(g)
	require.NotNil(t, cs)
	assert.Equal(t, "Running", *cs.State)
	assert.NotNil(t, cs.StartTime)
	assert.Equal(t, int32(0), *g.Properties.Containers[0].Properties.InstanceView.RestartCount)

	for _, e := range g.Properties.Containers[0].Properties.EnvironmentVariables {
		if *e.Name == "SECRET" {
			assert.Nil(t, e.SecureValue)
			assert.Nil(t, e.Value)
		}
	}

	specs := s.rt.started()
	require.Len(t, specs, 1)
	assert.Equal(t, []string{"sleep", "1000"}, specs[0].Entrypoint)
	assert.Contains(t, specs[0].Env, "SECRET=hunter2")
	assert.Equal(t, int64(2_000_000_000), specs[0].NanoCPUs)
	assert.Equal(t, int64(2<<30), specs[0].MemoryBytes)

	_, err = s.create(t, "rg2", "grp2", aciOpts{policy: armcontainerinstance.ContainerGroupRestartPolicyAlways})
	require.NoError(t, err)

	pager := groups.NewListByResourceGroupPager("rg1", nil)
	page, err := pager.NextPage(ctx)
	require.NoError(t, err)
	require.Len(t, page.Value, 1)
	assert.Equal(t, "grp1", *page.Value[0].Name)

	all := 0
	subPager := groups.NewListPager(nil)

	for subPager.More() {
		pg, pgErr := subPager.NextPage(ctx)
		require.NoError(t, pgErr)

		all += len(pg.Value)
	}

	assert.Equal(t, 2, all)

	logs, err := s.factory.NewContainersClient().ListLogs(ctx, "rg1", "grp1", "main", &armcontainerinstance.ContainersClientListLogsOptions{Tail: new(int32(1))})
	require.NoError(t, err)
	assert.Equal(t, "line three\n", *logs.Content)

	_, err = groups.Stop(ctx, "rg1", "grp1", nil)
	require.NoError(t, err)

	g = s.get(t, "rg1", "grp1")
	assert.Equal(t, "Stopped", *g.Properties.InstanceView.State)
	assert.Equal(t, "Terminated", *contState(g).State)
	assert.Equal(t, 1, s.rt.live(), "only the other group's container is left")

	_, err = s.factory.NewContainersClient().ListLogs(ctx, "rg1", "grp1", "main", nil)
	require.Error(t, err)

	startPoller, err := groups.BeginStart(ctx, "rg1", "grp1", nil)
	require.NoError(t, err)
	_, err = startPoller.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
	s.waitState(t, "rg1", "grp1", "Running")

	restartPoller, err := groups.BeginRestart(ctx, "rg1", "grp1", nil)
	require.NoError(t, err)
	_, err = restartPoller.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)
	s.waitState(t, "rg1", "grp1", "Running")
	assert.Len(t, s.rt.started(), 4)

	patched, err := groups.Update(ctx, "rg1", "grp1", armcontainerinstance.Resource{Tags: map[string]*string{"k": new("v")}}, nil)
	require.NoError(t, err)
	assert.Equal(t, "v", *patched.Tags["k"])

	delPoller, err := groups.BeginDelete(ctx, "rg1", "grp1", nil)
	require.NoError(t, err)
	_, err = delPoller.PollUntilDone(ctx, pollFast())
	require.NoError(t, err)

	_, err = groups.Get(ctx, "rg1", "grp1", nil)
	require.Error(t, err)
	assert.Equal(t, 1, s.rt.live())

	_, err = groups.Stop(ctx, "rg1", "grp1", nil)
	require.Error(t, err)
}

func TestACISDK_RestartPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		policy    armcontainerinstance.ContainerGroupRestartPolicy
		script    []int
		wantState string
		wantCont  string
		wantExit  int32
		wantRest  int32
	}{
		{"never-success", armcontainerinstance.ContainerGroupRestartPolicyNever, []int{0}, "Succeeded", "Terminated", 0, 0},
		{"never-failure", armcontainerinstance.ContainerGroupRestartPolicyNever, []int{3}, "Failed", "Terminated", 3, 0},
		{"onfailure-recovers", armcontainerinstance.ContainerGroupRestartPolicyOnFailure, []int{1, 2, 0}, "Succeeded", "Terminated", 0, 2},
		{"onfailure-success-stays", armcontainerinstance.ContainerGroupRestartPolicyOnFailure, []int{0}, "Succeeded", "Terminated", 0, 0},
		{"always-restarts-success", armcontainerinstance.ContainerGroupRestartPolicyAlways, []int{0, 0}, "Running", "Running", -1, 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newACIStack(t)
			s.rt.scripts["job:1"] = tc.script

			_, err := s.create(t, "rg", "job", aciOpts{policy: tc.policy, image: "job:1"})
			require.NoError(t, err)

			var g *armcontainerinstance.ContainerGroup

			require.Eventually(t, func() bool {
				g = s.get(t, "rg", "job")
				iv := g.Properties.Containers[0].Properties.InstanceView

				return *g.Properties.InstanceView.State == tc.wantState && *iv.RestartCount == tc.wantRest &&
					*iv.CurrentState.State == tc.wantCont
			}, 10*time.Second, 5*time.Millisecond)

			iv := g.Properties.Containers[0].Properties.InstanceView
			if tc.wantExit >= 0 {
				assert.Equal(t, tc.wantExit, *iv.CurrentState.ExitCode)
				assert.NotNil(t, iv.CurrentState.FinishTime)
			}

			if tc.wantRest > 0 {
				require.NotNil(t, iv.PreviousState)
				assert.Equal(t, "Terminated", *iv.PreviousState.State)
			}

			logs, err := s.factory.NewContainersClient().ListLogs(t.Context(), "rg", "job", "main", nil)
			require.NoError(t, err)
			assert.Contains(t, *logs.Content, "starting job:1")
		})
	}
}

func TestACISDK_ProvisioningFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		stack   func(t *testing.T) *aciStack
		image   string
		wantMsg string
	}{
		{
			name: "engine-disabled", image: "busybox:1", wantMsg: "disabled",
			stack: func(t *testing.T) *aciStack {
				t.Helper()

				s := newACIStackWith(t, nil)
				s.engine.SetRuntime(nil)

				return s
			},
		},
		{
			name: "pull-fails", image: "registry.example.com/missing:1", wantMsg: "pull access denied",
			stack: newACIStack,
		},
		{
			name: "runtime-unavailable", image: "busybox:1", wantMsg: "unavailable",
			stack: func(t *testing.T) *aciStack {
				t.Helper()

				s := newACIStackWith(t, nil)
				s.engine.SetRuntime(func() (azurearm.ACIRuntime, error) { return nil, context.DeadlineExceeded })

				return s
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := tc.stack(t)

			_, err := s.create(t, "rg", "bad", aciOpts{image: tc.image})
			require.Error(t, err, "a failed provisioning must surface through the poller")

			g := s.get(t, "rg", "bad")
			assert.Equal(t, "Failed", *g.Properties.ProvisioningState)
			assert.Equal(t, "Failed", *g.Properties.InstanceView.State)
			require.NotEmpty(t, g.Properties.InstanceView.Events)
			assert.Contains(t, *g.Properties.InstanceView.Events[0].Message, tc.wantMsg)
			assert.Equal(t, "Warning", *g.Properties.InstanceView.Events[0].Type)
		})
	}
}

func TestACISDK_ImageRegistryCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		image    string
		wantUser string
	}{
		{"matching-server", "myreg.azurecr.localhost:5000/app:1", "puller"},
		{"other-server", "docker.io/library/busybox:1", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newACIStack(t)
			g, err := s.create(t, "rg", "cred", aciOpts{
				image: tc.image,
				creds: []*armcontainerinstance.ImageRegistryCredential{
					{Server: new("myreg.azurecr.localhost:5000"), Username: new("puller"), Password: new("s3cret")},
				},
			})
			require.NoError(t, err)

			cred := s.rt.credFor(tc.image)
			if tc.wantUser == "" {
				assert.Nil(t, cred)
			} else {
				require.NotNil(t, cred)
				assert.Equal(t, tc.wantUser, cred.Username)
				assert.Equal(t, "s3cret", cred.Password)
			}

			require.Len(t, g.Properties.ImageRegistryCredentials, 1)
			assert.Equal(t, "puller", *g.Properties.ImageRegistryCredentials[0].Username)
			assert.Nil(t, g.Properties.ImageRegistryCredentials[0].Password, "passwords are never echoed")
		})
	}
}

func TestACISDK_IPAddressAndPorts(t *testing.T) {
	t.Parallel()

	s := newACIStack(t)
	ip := &armcontainerinstance.IPAddress{
		Type:         new(armcontainerinstance.ContainerGroupIPAddressTypePublic),
		DNSNameLabel: new("my-app"),
		Ports:        []*armcontainerinstance.Port{{Port: new(int32(8080)), Protocol: new(armcontainerinstance.ContainerGroupNetworkProtocolTCP)}},
	}

	g, err := s.create(t, "rg", "web", aciOpts{
		policy: armcontainerinstance.ContainerGroupRestartPolicyAlways, ip: ip,
		ports: []*armcontainerinstance.ContainerPort{{Port: new(int32(8080))}},
	})
	require.NoError(t, err)

	assert.Equal(t, "127.0.0.1", *g.Properties.IPAddress.IP)
	assert.Equal(t, "my-app.local.azurecontainer.io", *g.Properties.IPAddress.Fqdn)
	assert.Equal(t, "Public", string(*g.Properties.IPAddress.Type))

	specs := s.rt.started()
	require.Len(t, specs, 1)
	require.Len(t, specs[0].Ports, 1)
	assert.Contains(t, specs[0].Ports[0], ":8080")

	var published bool

	for _, e := range g.Properties.InstanceView.Events {
		published = published || *e.Name == "PortPublished"
	}

	assert.True(t, published, "the published host port is reported as an event")

	_, err = s.factory.NewContainerGroupsClient().Stop(t.Context(), "rg", "web", nil)
	require.NoError(t, err)
	assert.Nil(t, s.get(t, "rg", "web").Properties.IPAddress.IP, "a stopped group has no address")
}

func TestACISDK_Volumes(t *testing.T) {
	t.Parallel()

	s := newACIStack(t)
	s.shares.trees["acct/share1"] = azurearm.ACIShareTree{Files: map[string][]byte{"seed.txt": []byte("seeded")}}

	seen := map[string]string{}
	s.rt.onStart = func(spec container.Spec) {
		for _, m := range spec.Mounts {
			dir, _, _ := splitMount(m)

			if b, err := os.ReadFile(filepath.Join(dir, "seed.txt")); err == nil {
				seen["seed"] = string(b)
			}

			if b, err := os.ReadFile(filepath.Join(dir, "token")); err == nil {
				seen["token"] = string(b)
			}

			if _, err := os.Stat(dir); err == nil && filepath.Base(dir) != "" {
				_ = os.WriteFile(filepath.Join(dir, "out.txt"), []byte("written by container"), 0o600)
			}
		}
	}

	_, err := s.create(t, "rg", "vols", aciOpts{
		policy: armcontainerinstance.ContainerGroupRestartPolicyAlways,
		volumes: []*armcontainerinstance.Volume{
			{Name: new("scratch"), EmptyDir: map[string]any{}},
			{Name: new("sec"), Secret: map[string]*string{"token": new("c2VjcmV0")}},
			{Name: new("files"), AzureFile: &armcontainerinstance.AzureFileVolume{
				ShareName: new("share1"), StorageAccountName: new("acct"), StorageAccountKey: new("a2V5"),
			}},
		},
		mounts: []*armcontainerinstance.VolumeMount{
			{Name: new("scratch"), MountPath: new("/scratch")},
			{Name: new("sec"), MountPath: new("/secrets")},
			{Name: new("files"), MountPath: new("/data")},
		},
	})
	require.NoError(t, err)

	assert.Equal(t, "seeded", seen["seed"])
	assert.Equal(t, "secret", seen["token"])

	specs := s.rt.started()
	require.Len(t, specs, 1)
	assert.Len(t, specs[0].Mounts, 3)

	for _, m := range specs[0].Mounts {
		if _, path, ro := splitMount(m); path == "/secrets" {
			assert.True(t, ro, "secret volumes are read-only")
		}
	}

	_, err = s.factory.NewContainerGroupsClient().Stop(t.Context(), "rg", "vols", nil)
	require.NoError(t, err)

	tree := s.shares.tree("acct", "share1")
	assert.Equal(t, "written by container", string(tree.Files["out.txt"]))
	assert.Equal(t, "seeded", string(tree.Files["seed.txt"]))
}

func splitMount(m string) (string, string, bool) {
	parts := splitN(m, ":")
	ro := len(parts) == 3 && parts[2] == "ro"

	return parts[0], parts[1], ro
}

func splitN(s, sep string) []string {
	var out []string

	for {
		i := indexOf(s, sep)
		if i < 0 {
			return append(out, s)
		}

		out = append(out, s[:i])
		s = s[i+len(sep):]
	}
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}

	return -1
}

func TestACISDK_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate  func(g *armcontainerinstance.ContainerGroup)
		name    string
		wantMsg string
	}{
		{func(g *armcontainerinstance.ContainerGroup) {
			g.Properties.OSType = new(armcontainerinstance.OperatingSystemTypesWindows)
		}, "windows", "osType"},
		{func(g *armcontainerinstance.ContainerGroup) { g.Properties.Containers = nil }, "no-containers", "at least one container"},
		{func(g *armcontainerinstance.ContainerGroup) { g.Properties.Containers[0].Properties.Resources = nil }, "no-resources", "requests"},
		{func(g *armcontainerinstance.ContainerGroup) {
			g.Properties.Volumes = []*armcontainerinstance.Volume{{Name: new("g"), GitRepo: &armcontainerinstance.GitRepoVolume{Repository: new("https://x/y.git")}}}
		}, "gitrepo", "gitRepo"},
		{func(g *armcontainerinstance.ContainerGroup) {
			g.Properties.Containers[0].Properties.LivenessProbe = &armcontainerinstance.ContainerProbe{}
		}, "probe", "livenessProbe"},
		{func(g *armcontainerinstance.ContainerGroup) {
			g.Properties.IPAddress = &armcontainerinstance.IPAddress{
				Type:  new(armcontainerinstance.ContainerGroupIPAddressTypePublic),
				Ports: []*armcontainerinstance.Port{{Port: new(int32(80))}},
			}
		}, "port-not-exposed", "not exposed"},
		{func(g *armcontainerinstance.ContainerGroup) {
			g.Properties.Containers[0].Properties.EnvironmentVariables = []*armcontainerinstance.EnvironmentVariable{
				{Name: new("A"), Value: new("1"), SecureValue: new("2")},
			}
		}, "env-both", "environment"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newACIStack(t)
			g := aciOpts{}.group()
			tc.mutate(&g)

			_, err := s.factory.NewContainerGroupsClient().BeginCreateOrUpdate(t.Context(), "rg", "v", g, nil)
			require.Error(t, err)

			var re *azcore.ResponseError

			require.ErrorAs(t, err, &re)
			assert.Equal(t, http.StatusBadRequest, re.StatusCode)
			assert.Equal(t, "InvalidRequestContent", re.ErrorCode)
			assert.Contains(t, re.Error(), tc.wantMsg)
			assert.Zero(t, s.rt.live())
		})
	}
}

func TestACISDK_ExecIsNotSupported(t *testing.T) {
	t.Parallel()

	s := newACIStack(t)
	_, err := s.create(t, "rg", "x", aciOpts{policy: armcontainerinstance.ContainerGroupRestartPolicyAlways})
	require.NoError(t, err)

	_, err = s.factory.NewContainersClient().ExecuteCommand(t.Context(), "rg", "x", "main",
		armcontainerinstance.ContainerExecRequest{Command: new("sh")}, nil)

	var re *azcore.ResponseError

	require.ErrorAs(t, err, &re)
	assert.Equal(t, http.StatusNotImplemented, re.StatusCode)
}

func TestACISDK_DeleteResourceGroupStopsContainers(t *testing.T) {
	t.Parallel()

	s := newACIStack(t)
	_, err := s.create(t, "rg", "x", aciOpts{policy: armcontainerinstance.ContainerGroupRestartPolicyAlways})
	require.NoError(t, err)
	assert.Equal(t, 1, s.rt.live())

	rgURL := s.url + "/subscriptions/" + aciSub + "/resourcegroups/rg?api-version=2021-04-01"
	putReq, err := http.NewRequestWithContext(t.Context(), http.MethodPut, rgURL, strings.NewReader(`{"location":"local"}`))
	require.NoError(t, err)

	putReq.Header.Set("Content-Type", "application/json")

	putResp, err := s.client.Do(putReq)
	require.NoError(t, err)
	_ = putResp.Body.Close()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete,
		s.url+"/subscriptions/"+aciSub+"/resourcegroups/rg?api-version=2021-04-01", http.NoBody)
	require.NoError(t, err)

	resp, err := s.client.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()

	assert.Zero(t, s.rt.live())

	_, err = s.factory.NewContainerGroupsClient().Get(t.Context(), "rg", "x", nil)
	require.Error(t, err)
}

func TestACISDK_SnapshotRestore(t *testing.T) {
	t.Parallel()

	src := newACIStack(t)
	_, err := src.create(t, "rg", "persisted", aciOpts{
		policy: armcontainerinstance.ContainerGroupRestartPolicyAlways,
		env:    []*armcontainerinstance.EnvironmentVariable{{Name: new("SECRET"), SecureValue: new("keep-me")}},
	})
	require.NoError(t, err)

	snap := src.handler.Snapshot(t.Context())

	dst := newACIStack(t)
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	g := dst.get(t, "rg", "persisted")
	assert.Equal(t, "Stopped", *g.Properties.InstanceView.State, "restored groups have no running containers")
	assert.Zero(t, dst.rt.live())

	_, err = dst.factory.NewContainerGroupsClient().BeginStart(t.Context(), "rg", "persisted", nil)
	require.NoError(t, err)
	dst.waitState(t, "rg", "persisted", "Running")

	specs := dst.rt.started()
	require.Len(t, specs, 1)
	assert.Contains(t, specs[0].Env, "SECRET=keep-me", "secure values survive a snapshot")
}
