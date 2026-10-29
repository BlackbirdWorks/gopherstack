package azurearm_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appcontainers/armappcontainers/v4"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

type acaStack struct {
	factory *armappcontainers.ClientFactory
	rt      *fakeACIRuntime
	handler *azurearm.Handler
	client  *http.Client
	apps    *azurearm.ContainerAppsProvider
	engine  *azurearm.ACIEngine
	url     string
}

func newACAStack(t *testing.T) *acaStack {
	t.Helper()

	return newACAStackWith(t, newFakeACIRuntime())
}

// newACAStackWith runs against rt; a nil rt leaves the engine on its real container runtime.
func newACAStackWith(t *testing.T, rt *fakeACIRuntime) *acaStack {
	t.Helper()

	armSvc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	armH, ok := armSvc.(*azurearm.Handler)
	require.True(t, ok)

	ap, ok := armH.Registry.ProviderNamed("Microsoft.App")
	require.True(t, ok)

	apps, ok := ap.(*azurearm.ContainerAppsProvider)
	require.True(t, ok)

	ip, ok := armH.Registry.ProviderNamed("Microsoft.ContainerInstance")
	require.True(t, ok)

	aci, ok := ip.(*azurearm.ContainerInstanceProvider)
	require.True(t, ok)

	engine := aci.Engine()
	engine.SetTiming(func(int) time.Duration { return 0 }, time.Minute)

	if rt != nil {
		engine.SetRuntime(func() (azurearm.ACIRuntime, error) { return rt, nil })
	}

	t.Cleanup(func() { armH.Shutdown(context.Background()) })

	e := echo.New()
	e.Any("/*", armH.Handler())

	srv := httptest.NewTLSServer(e)
	t.Cleanup(srv.Close)

	f, err := armappcontainers.NewClientFactory(aciSub, aciCred{}, &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: srv.URL, Audience: "https://management.core.windows.net/"},
		}},
		Transport: srv.Client(),
		Retry:     policy.RetryOptions{MaxRetries: -1},
	})
	require.NoError(t, err)

	return &acaStack{
		factory: f, rt: rt, handler: armH, client: srv.Client(), apps: apps, engine: engine, url: srv.URL,
	}
}

func (s *acaStack) envID(rg, name string) string {
	return "/subscriptions/" + aciSub + "/resourceGroups/" + rg + "/providers/Microsoft.App/managedEnvironments/" + name
}

func (s *acaStack) createEnv(t *testing.T, rg, name string, props *armappcontainers.ManagedEnvironmentProperties) (
	*armappcontainers.ManagedEnvironment, error,
) {
	t.Helper()

	poller, err := s.factory.NewManagedEnvironmentsClient().BeginCreateOrUpdate(t.Context(), rg, name,
		armappcontainers.ManagedEnvironment{
			Location:   new("local"),
			Tags:       map[string]*string{"env": new("test")},
			Properties: props,
		}, nil)
	if err != nil {
		return nil, err
	}

	res, err := poller.PollUntilDone(t.Context(), pollFast())
	if err != nil {
		return nil, err
	}

	return &res.ManagedEnvironment, nil
}

func (s *acaStack) mustEnv(t *testing.T, rg, name string) *armappcontainers.ManagedEnvironment {
	t.Helper()

	env, err := s.createEnv(t, rg, name, &armappcontainers.ManagedEnvironmentProperties{})
	require.NoError(t, err)

	return env
}

type acaApp struct {
	ident   *armappcontainers.ManagedServiceIdentity
	scale   *armappcontainers.Scale
	image   string
	suffix  string
	profile string
	mode    armappcontainers.ActiveRevisionsMode
	mem     string
	secrets []*armappcontainers.Secret
	traffic []*armappcontainers.TrafficWeight
	env     []*armappcontainers.EnvironmentVar
	command []*string
	args    []*string
	cpu     float64
	port    int32
	noIng   bool
	extIng  bool
}

func (o acaApp) model(envID string) armappcontainers.ContainerApp {
	image := o.image
	if image == "" {
		image = "busybox:1"
	}

	cpu, mem := o.cpu, o.mem
	if cpu == 0 {
		cpu, mem = 0.5, "1Gi"
	}

	port := o.port
	if port == 0 {
		port = 8080
	}

	mode := o.mode
	if mode == "" {
		mode = armappcontainers.ActiveRevisionsModeSingle
	}

	cfg := &armappcontainers.Configuration{ActiveRevisionsMode: &mode, Secrets: o.secrets}
	if !o.noIng {
		cfg.Ingress = &armappcontainers.Ingress{External: new(o.extIng), TargetPort: &port, Traffic: o.traffic}
	}

	tmpl := &armappcontainers.Template{
		Containers: []*armappcontainers.Container{{
			Name: new("main"), Image: &image, Env: o.env, Command: o.command, Args: o.args,
			Resources: &armappcontainers.ContainerResources{CPU: &cpu, Memory: &mem},
		}},
		Scale: o.scale,
	}
	if o.suffix != "" {
		tmpl.RevisionSuffix = &o.suffix
	}

	props := &armappcontainers.ContainerAppProperties{EnvironmentID: &envID, Configuration: cfg, Template: tmpl}
	if o.profile != "" {
		props.WorkloadProfileName = &o.profile
	}

	return armappcontainers.ContainerApp{
		Location: new("local"), Tags: map[string]*string{"env": new("test")}, Identity: o.ident, Properties: props,
	}
}

func (s *acaStack) putApp(t *testing.T, rg, name, envID string, o acaApp) (*armappcontainers.ContainerApp, error) {
	t.Helper()

	poller, err := s.factory.NewContainerAppsClient().BeginCreateOrUpdate(t.Context(), rg, name, o.model(envID), nil)
	if err != nil {
		return nil, err
	}

	res, err := poller.PollUntilDone(t.Context(), pollFast())
	if err != nil {
		return nil, err
	}

	return &res.ContainerApp, nil
}

func (s *acaStack) mustApp(t *testing.T, rg, name, envID string, o acaApp) *armappcontainers.ContainerApp {
	t.Helper()

	app, err := s.putApp(t, rg, name, envID, o)
	require.NoError(t, err)

	return app
}

func (s *acaStack) revisions(t *testing.T, app string) []*armappcontainers.Revision {
	t.Helper()

	var out []*armappcontainers.Revision

	pager := s.factory.NewContainerAppsRevisionsClient().NewListRevisionsPager("rg", app, nil)
	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		out = append(out, page.Value...)
	}

	return out
}

func respCode(t *testing.T, err error) (string, int) {
	t.Helper()

	var re *azcore.ResponseError

	require.ErrorAs(t, err, &re, "not an ARM response error: %v", err)

	return re.ErrorCode, re.StatusCode
}

// raw sends a request to the ARM listener with an explicit Host header.
func (s *acaStack) raw(t *testing.T, method, host, path string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, s.url+path, http.NoBody)
	require.NoError(t, err)

	if host != "" {
		req.Host = host
	}

	resp, err := s.client.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(body)
}

// rawNoFail is raw without test assertions, for use inside polling conditions.
func (s *acaStack) rawNoFail(host, path string) (int, string) {
	req, err := http.NewRequest(http.MethodGet, s.url+path, http.NoBody)
	if err != nil {
		return 0, ""
	}

	req.Host = host

	resp, err := s.client.Do(req)
	if err != nil {
		return 0, ""
	}

	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	return resp.StatusCode, string(body)
}

func TestACA_EnvironmentLifecycle(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	client := s.factory.NewManagedEnvironmentsClient()

	env, err := s.createEnv(t, "rg", "my-env", &armappcontainers.ManagedEnvironmentProperties{
		AppLogsConfiguration: &armappcontainers.AppLogsConfiguration{
			Destination: new("log-analytics"),
			LogAnalyticsConfiguration: &armappcontainers.LogAnalyticsConfiguration{
				CustomerID: new("cust"), SharedKey: new("secret-key"),
			},
		},
		WorkloadProfiles: []*armappcontainers.WorkloadProfile{
			{Name: new("Consumption"), WorkloadProfileType: new("Consumption")},
			{Name: new("d4"), WorkloadProfileType: new("D4"), MinimumCount: new(int32(1)), MaximumCount: new(int32(3))},
		},
	})
	require.NoError(t, err)

	assert.Equal(t, s.envID("rg", "my-env"), *env.ID)
	assert.Equal(t, "Microsoft.App/managedEnvironments", *env.Type)
	assert.Equal(t, armappcontainers.EnvironmentProvisioningStateSucceeded, *env.Properties.ProvisioningState)
	assert.Equal(t, "127.0.0.1", *env.Properties.StaticIP)
	assert.Regexp(t, `^my-env-[0-9a-f]{6}\.local\.azurecontainerapps\.io$`, *env.Properties.DefaultDomain)
	assert.Equal(t, "cust", *env.Properties.AppLogsConfiguration.LogAnalyticsConfiguration.CustomerID)
	assert.Nil(t, env.Properties.AppLogsConfiguration.LogAnalyticsConfiguration.SharedKey, "shared key is write-only")
	assert.Len(t, env.Properties.WorkloadProfiles, 2)
	assert.Equal(t, "ME_my-env_rg_local", *env.Properties.InfrastructureResourceGroup)

	got, err := client.Get(t.Context(), "rg", "my-env", nil)
	require.NoError(t, err)
	assert.Equal(t, *env.Properties.DefaultDomain, *got.Properties.DefaultDomain)

	s.mustEnv(t, "other-rg", "second")

	for _, tc := range []struct {
		name string
		rg   string
		want int
	}{{"by-group", "rg", 1}, {"by-subscription", "", 2}} {
		var n int

		if tc.rg == "" {
			pager := client.NewListBySubscriptionPager(nil)
			for pager.More() {
				page, pageErr := pager.NextPage(t.Context())
				require.NoError(t, pageErr)

				n += len(page.Value)
			}
		} else {
			pager := client.NewListByResourceGroupPager(tc.rg, nil)
			for pager.More() {
				page, pageErr := pager.NextPage(t.Context())
				require.NoError(t, pageErr)

				n += len(page.Value)
			}
		}

		assert.Equal(t, tc.want, n, tc.name)
	}

	updater, err := client.BeginUpdate(t.Context(), "rg", "my-env", armappcontainers.ManagedEnvironment{
		Tags: map[string]*string{"env": new("patched")},
	}, nil)
	require.NoError(t, err)

	patched, err := updater.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.Equal(t, "patched", *patched.Tags["env"])
	assert.Equal(t, *env.Properties.DefaultDomain, *patched.Properties.DefaultDomain)

	del, err := client.BeginDelete(t.Context(), "rg", "my-env", nil)
	require.NoError(t, err)

	_, err = del.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	_, err = client.Get(t.Context(), "rg", "my-env", nil)
	code, status := respCode(t, err)
	assert.Equal(t, http.StatusNotFound, status)
	assert.Equal(t, "ResourceNotFound", code)
}

func TestACA_EnvironmentValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props *armappcontainers.ManagedEnvironmentProperties
		name  string
		env   string
	}{
		{name: "bad-name", env: "Bad_Name", props: &armappcontainers.ManagedEnvironmentProperties{}},
		{name: "double-dash", env: "a--b", props: &armappcontainers.ManagedEnvironmentProperties{}},
		{
			name: "bad-destination", env: "e1",
			props: &armappcontainers.ManagedEnvironmentProperties{
				AppLogsConfiguration: &armappcontainers.AppLogsConfiguration{Destination: new("syslog")},
			},
		},
		{
			name: "log-analytics-without-key", env: "e2",
			props: &armappcontainers.ManagedEnvironmentProperties{
				AppLogsConfiguration: &armappcontainers.AppLogsConfiguration{Destination: new("log-analytics")},
			},
		},
		{
			name: "dedicated-profile-without-counts", env: "e3",
			props: &armappcontainers.ManagedEnvironmentProperties{
				WorkloadProfiles: []*armappcontainers.WorkloadProfile{
					{Name: new("d4"), WorkloadProfileType: new("D4")},
				},
			},
		},
		{
			name: "consumption-with-counts", env: "e4",
			props: &armappcontainers.ManagedEnvironmentProperties{
				WorkloadProfiles: []*armappcontainers.WorkloadProfile{
					{
						Name:                new("c"),
						WorkloadProfileType: new("Consumption"),
						MinimumCount:        new(int32(1)),
						MaximumCount:        new(int32(2)),
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newACAStack(t)

			_, err := s.createEnv(t, "rg", tc.env, tc.props)
			code, status := respCode(t, err)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidRequestParameterWithDetails", code)
		})
	}
}

func TestACA_EnvironmentImmutableAndInUse(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	s.mustEnv(t, "rg", "env1")
	s.mustApp(t, "rg", "app1", s.envID("rg", "env1"), acaApp{})

	_, err := s.factory.NewManagedEnvironmentsClient().BeginDelete(t.Context(), "rg", "env1", nil)
	code, status := respCode(t, err)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ManagedEnvironmentHasContainerApps", code)

	_, err = s.createEnv(t, "rg", "env1", &armappcontainers.ManagedEnvironmentProperties{ZoneRedundant: new(true)})
	_, status = respCode(t, err)
	assert.Equal(t, http.StatusBadRequest, status)
}

func TestACA_Storages(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	s.mustEnv(t, "rg", "env1")

	client := s.factory.NewManagedEnvironmentsStoragesClient()
	good := armappcontainers.ManagedEnvironmentStorage{
		Properties: &armappcontainers.ManagedEnvironmentStorageProperties{
			AzureFile: &armappcontainers.AzureFileProperties{
				AccountName: new("acct"), AccountKey: new("key"), ShareName: new("share"),
				AccessMode: new(armappcontainers.AccessModeReadWrite),
			},
		},
	}

	st, err := client.CreateOrUpdate(t.Context(), "rg", "env1", "files", good, nil)
	require.NoError(t, err)
	assert.Equal(t, "files", *st.Name)
	assert.Nil(t, st.Properties.AzureFile.AccountKey, "account key is write-only")
	assert.Equal(t, "share", *st.Properties.AzureFile.ShareName)

	list, err := client.List(t.Context(), "rg", "env1", nil)
	require.NoError(t, err)
	require.Len(t, list.Value, 1)

	_, err = client.CreateOrUpdate(t.Context(), "rg", "env1", "bad", armappcontainers.ManagedEnvironmentStorage{
		Properties: &armappcontainers.ManagedEnvironmentStorageProperties{},
	}, nil)
	_, status := respCode(t, err)
	assert.Equal(t, http.StatusBadRequest, status)

	_, err = client.CreateOrUpdate(t.Context(), "rg", "missing-env", "files", good, nil)
	_, status = respCode(t, err)
	assert.Equal(t, http.StatusNotFound, status)

	_, err = client.Delete(t.Context(), "rg", "env1", "files", nil)
	require.NoError(t, err)

	_, err = client.Get(t.Context(), "rg", "env1", "files", nil)
	_, status = respCode(t, err)
	assert.Equal(t, http.StatusNotFound, status)
}

func TestACA_AppLifecycleAndRevisions(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	env := s.mustEnv(t, "rg", "env1")
	envID := *env.ID
	domain := *env.Properties.DefaultDomain

	app := s.mustApp(t, "rg", "web", envID, acaApp{extIng: true})
	props := app.Properties

	require.NotNil(t, props.LatestRevisionName)
	assert.Regexp(t, `^web--[0-9a-f]{7}$`, *props.LatestRevisionName)
	assert.Equal(t, *props.LatestRevisionName+"."+domain, *props.LatestRevisionFqdn)
	assert.Equal(t, "web."+domain, *props.Configuration.Ingress.Fqdn)
	assert.Equal(t, armappcontainers.ContainerAppProvisioningStateSucceeded, *props.ProvisioningState)
	assert.Equal(t, armappcontainers.ContainerAppRunningStatusRunning, *props.RunningStatus)
	assert.Equal(t, *props.LatestRevisionName, *props.LatestReadyRevisionName)
	assert.Equal(t, armappcontainers.ActiveRevisionsModeSingle, *props.Configuration.ActiveRevisionsMode)
	require.Len(t, props.Configuration.Ingress.Traffic, 1)
	assert.True(t, *props.Configuration.Ingress.Traffic[0].LatestRevision)

	first := *props.LatestRevisionName

	again := s.mustApp(t, "rg", "web", envID, acaApp{extIng: true})
	assert.Equal(t, first, *again.Properties.LatestRevisionName, "unchanged template keeps the revision")
	assert.Len(t, s.revisions(t, "web"), 1)

	changed := s.mustApp(t, "rg", "web", envID, acaApp{extIng: true, image: "busybox:2", suffix: "v2"})
	assert.Equal(t, "web--v2", *changed.Properties.LatestRevisionName)

	revs := s.revisions(t, "web")
	require.Len(t, revs, 2)

	byName := map[string]*armappcontainers.Revision{}
	for _, r := range revs {
		byName[*r.Name] = r
	}

	assert.False(t, *byName[first].Properties.Active, "single mode retires the previous revision")
	assert.True(t, *byName["web--v2"].Properties.Active)
	assert.Equal(t, int32(100), *byName["web--v2"].Properties.TrafficWeight)
	assert.Equal(
		t,
		armappcontainers.RevisionProvisioningStateDeprovisioned,
		*byName[first].Properties.ProvisioningState,
	)
	assert.Equal(t, "busybox:2", *byName["web--v2"].Properties.Template.Containers[0].Image)

	_, err := s.putApp(t, "rg", "web", envID, acaApp{extIng: true, image: "busybox:3", suffix: "v2"})
	code, status := respCode(t, err)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ContainerAppRevisionNameConflict", code)

	rc := s.factory.NewContainerAppsRevisionsClient()
	_, err = rc.DeactivateRevision(t.Context(), "rg", "web", "web--v2", nil)
	code, _ = respCode(t, err)
	assert.Equal(t, "ContainerAppInvalidRevisionMode", code, "single mode cannot toggle revisions")
}

func TestACA_MultipleRevisionActions(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	envID := *s.mustEnv(t, "rg", "env1").ID
	multi := armappcontainers.ActiveRevisionsModeMultiple

	s.mustApp(t, "rg", "api", envID, acaApp{mode: multi, suffix: "one", extIng: true})
	s.mustApp(t, "rg", "api", envID, acaApp{mode: multi, suffix: "two", image: "busybox:2", extIng: true})

	revs := s.revisions(t, "api")
	require.Len(t, revs, 2)

	for _, r := range revs {
		assert.True(t, *r.Properties.Active, *r.Name)
	}

	rc := s.factory.NewContainerAppsRevisionsClient()
	_, err := rc.DeactivateRevision(t.Context(), "rg", "api", "api--one", nil)
	require.NoError(t, err)

	got, err := rc.GetRevision(t.Context(), "rg", "api", "api--one", nil)
	require.NoError(t, err)
	assert.False(t, *got.Properties.Active)
	assert.Equal(t, armappcontainers.RevisionRunningStateStopped, *got.Properties.RunningState)

	_, err = rc.RestartRevision(t.Context(), "rg", "api", "api--one", nil)
	code, _ := respCode(t, err)
	assert.Equal(t, "ContainerAppRevisionInactive", code)

	_, err = rc.ActivateRevision(t.Context(), "rg", "api", "api--one", nil)
	require.NoError(t, err)

	got, err = rc.GetRevision(t.Context(), "rg", "api", "api--one", nil)
	require.NoError(t, err)
	assert.True(t, *got.Properties.Active)

	before := len(s.rt.started())
	_, err = rc.RestartRevision(t.Context(), "rg", "api", "api--two", nil)
	require.NoError(t, err)
	assert.Greater(t, len(s.rt.started()), before, "restart recreates the replicas")

	_, err = rc.GetRevision(t.Context(), "rg", "api", "api--missing", nil)
	_, status := respCode(t, err)
	assert.Equal(t, http.StatusNotFound, status)

	replicas, err := s.factory.NewContainerAppsRevisionReplicasClient().
		ListReplicas(t.Context(), "rg", "api", "api--two", nil)
	require.NoError(t, err)
	require.Len(t, replicas.Value, 1)
	assert.Contains(t, *replicas.Value[0].Name, "api--two-")
	assert.Equal(t, armappcontainers.ContainerAppReplicaRunningStateRunning, *replicas.Value[0].Properties.RunningState)
	assert.Equal(t, "main", *replicas.Value[0].Properties.Containers[0].Name)

	one, err := s.factory.NewContainerAppsRevisionReplicasClient().GetReplica(
		t.Context(), "rg", "api", "api--two", *replicas.Value[0].Name, nil)
	require.NoError(t, err)
	assert.Equal(t, *replicas.Value[0].Name, *one.Name)
}

func TestACA_InactiveRevisionLimit(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	envID := *s.mustEnv(t, "rg", "env1").ID

	model := func(suffix string) armappcontainers.ContainerApp {
		m := acaApp{suffix: suffix, image: "busybox:" + suffix}.model(envID)
		m.Properties.Configuration.MaxInactiveRevisions = new(int32(1))

		return m
	}

	for _, suffix := range []string{"a", "b", "c"} {
		poller, err := s.factory.NewContainerAppsClient().
			BeginCreateOrUpdate(t.Context(), "rg", "pruned", model(suffix), nil)
		require.NoError(t, err)

		_, err = poller.PollUntilDone(t.Context(), pollFast())
		require.NoError(t, err)
	}

	revs := s.revisions(t, "pruned")
	names := make([]string, 0, len(revs))

	for _, r := range revs {
		names = append(names, *r.Name)
	}

	assert.Equal(t, []string{"pruned--b", "pruned--c"}, names, "only one inactive revision is kept")
}

func TestACA_AppValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(m *armappcontainers.ContainerApp)
		name   string
		app    string
		opts   acaApp
	}{
		{name: "bad-name", app: "Bad_App", opts: acaApp{}},
		{name: "double-dash-name", app: "a--b", opts: acaApp{}},
		{name: "cpu-memory-mismatch", app: "a1", opts: acaApp{cpu: 0.5, mem: "2Gi"}},
		{name: "cpu-not-quarter-step", app: "a2", opts: acaApp{cpu: 0.3, mem: "0.6Gi"}},
		{name: "consumption-only-over-two-cores", app: "a3", opts: acaApp{cpu: 2.5, mem: "5Gi"}},
		{name: "bad-suffix", app: "a4", opts: acaApp{suffix: "Bad--Suffix"}},
		{name: "unknown-profile", app: "a5", opts: acaApp{profile: "nope"}},
		{name: "missing-secret-ref", app: "a6", opts: acaApp{env: []*armappcontainers.EnvironmentVar{
			{Name: new("A"), SecretRef: new("absent")},
		}}},
		{name: "min-above-max", app: "a7", opts: acaApp{scale: &armappcontainers.Scale{
			MinReplicas: new(int32(5)), MaxReplicas: new(int32(2)),
		}}},
		{name: "scale-rule-without-kind", app: "a8", opts: acaApp{scale: &armappcontainers.Scale{
			Rules: []*armappcontainers.ScaleRule{{Name: new("empty")}},
		}}},
		{name: "no-containers", app: "a9", opts: acaApp{}, mutate: func(m *armappcontainers.ContainerApp) {
			m.Properties.Template.Containers = nil
		}},
		{name: "unknown-environment", app: "a10", opts: acaApp{}, mutate: func(m *armappcontainers.ContainerApp) {
			m.Properties.EnvironmentID = new(
				"/subscriptions/" + aciSub + "/resourceGroups/rg/providers/Microsoft.App/managedEnvironments/ghost",
			)
		}},
		{name: "ingress-without-port", app: "a11", opts: acaApp{}, mutate: func(m *armappcontainers.ContainerApp) {
			m.Properties.Configuration.Ingress.TargetPort = nil
		}},
		{name: "bad-revision-mode", app: "a12", opts: acaApp{mode: "Sideways"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newACAStack(t)
			envID := *s.mustEnv(t, "rg", "env1").ID
			m := tc.opts.model(envID)

			if tc.mutate != nil {
				tc.mutate(&m)
			}

			poller, err := s.factory.NewContainerAppsClient().BeginCreateOrUpdate(t.Context(), "rg", tc.app, m, nil)
			if err == nil {
				_, err = poller.PollUntilDone(t.Context(), pollFast())
			}

			code, status := respCode(t, err)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Contains(t, []string{"InvalidRequestParameterWithDetails", "ManagedEnvironmentNotFound"}, code)
		})
	}
}

func TestACA_TrafficValidation(t *testing.T) {
	t.Parallel()

	multi := armappcontainers.ActiveRevisionsModeMultiple

	tests := []struct {
		name    string
		opts    acaApp
		wantErr bool
	}{
		{
			name: "sum-not-100", wantErr: true,
			opts: acaApp{mode: multi, traffic: []*armappcontainers.TrafficWeight{
				{RevisionName: new("tv--one"), Weight: new(int32(60))},
				{RevisionName: new("tv--two"), Weight: new(int32(30))},
			}},
		},
		{
			name: "unknown-revision", wantErr: true,
			opts: acaApp{mode: multi, traffic: []*armappcontainers.TrafficWeight{
				{RevisionName: new("tv--ghost"), Weight: new(int32(100))},
			}},
		},
		{
			name: "both-revision-and-latest", wantErr: true,
			opts: acaApp{mode: multi, traffic: []*armappcontainers.TrafficWeight{
				{RevisionName: new("tv--one"), LatestRevision: new(true), Weight: new(int32(100))},
			}},
		},
		{
			name: "named-revision-in-single-mode", wantErr: true,
			opts: acaApp{traffic: []*armappcontainers.TrafficWeight{
				{RevisionName: new("tv--one"), Weight: new(int32(100))},
			}},
		},
		{
			name: "bad-label", wantErr: true,
			opts: acaApp{mode: multi, traffic: []*armappcontainers.TrafficWeight{
				{RevisionName: new("tv--one"), Weight: new(int32(100)), Label: new("Bad--Label")},
			}},
		},
		{
			name: "weight-over-100", wantErr: true,
			opts: acaApp{mode: multi, traffic: []*armappcontainers.TrafficWeight{
				{RevisionName: new("tv--one"), Weight: new(int32(150))},
			}},
		},
		{
			name: "split-80-20",
			opts: acaApp{mode: multi, traffic: []*armappcontainers.TrafficWeight{
				{RevisionName: new("tv--one"), Weight: new(int32(80))},
				{RevisionName: new("tv--two"), Weight: new(int32(20)), Label: new("canary")},
			}},
		},
		{
			name: "latest-plus-label",
			opts: acaApp{mode: multi, traffic: []*armappcontainers.TrafficWeight{
				{LatestRevision: new(true), Weight: new(int32(100))},
				{RevisionName: new("tv--one"), Weight: new(int32(0)), Label: new("stable")},
			}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newACAStack(t)
			envID := *s.mustEnv(t, "rg", "env1").ID

			s.mustApp(t, "rg", "tv", envID, acaApp{mode: multi, suffix: "one", extIng: true})
			s.mustApp(t, "rg", "tv", envID, acaApp{mode: multi, suffix: "two", image: "busybox:2", extIng: true})

			tc.opts.suffix, tc.opts.image, tc.opts.extIng = "two", "busybox:2", true

			app, err := s.putApp(t, "rg", "tv", envID, tc.opts)
			if tc.wantErr {
				code, status := respCode(t, err)
				assert.Equal(t, http.StatusBadRequest, status)
				assert.Equal(t, "InvalidRequestParameterWithDetails", code)

				return
			}

			require.NoError(t, err)
			assert.Len(t, app.Properties.Configuration.Ingress.Traffic, len(tc.opts.traffic))
		})
	}
}

func TestACA_SecretsAndRegistries(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	envID := *s.mustEnv(t, "rg", "env1").ID

	t.Run("inline-secret-hidden-and-injected", func(t *testing.T) {
		t.Parallel()

		app := s.mustApp(t, "rg", "sec", envID, acaApp{
			secrets: []*armappcontainers.Secret{{Name: new("db-pass"), Value: new("hunter2")}},
			env: []*armappcontainers.EnvironmentVar{
				{Name: new("DB_PASS"), SecretRef: new("db-pass")}, {Name: new("PLAIN"), Value: new("v")},
			},
		})
		require.Len(t, app.Properties.Configuration.Secrets, 1)
		assert.Equal(t, "db-pass", *app.Properties.Configuration.Secrets[0].Name)
		assert.Nil(t, app.Properties.Configuration.Secrets[0].Value, "GET never returns secret values")

		list, err := s.factory.NewContainerAppsClient().ListSecrets(t.Context(), "rg", "sec", nil)
		require.NoError(t, err)
		require.Len(t, list.Value, 1)
		assert.Equal(t, "hunter2", *list.Value[0].Value)

		var env []string

		for _, sp := range s.rt.started() {
			if strings.Contains(sp.Name, "-sec-") {
				env = sp.Env
			}
		}

		assert.Contains(t, env, "DB_PASS=hunter2")
		assert.Contains(t, env, "PLAIN=v")
		assert.Contains(t, env, "CONTAINER_APP_NAME=sec")
		assert.Contains(t, env, "CONTAINER_APP_PORT=8080")
	})

	tests := []struct {
		ident   *armappcontainers.ManagedServiceIdentity
		name    string
		secrets []*armappcontainers.Secret
	}{
		{name: "duplicate", secrets: []*armappcontainers.Secret{
			{Name: new("a"), Value: new("1")}, {Name: new("a"), Value: new("2")},
		}},
		{name: "bad-name", secrets: []*armappcontainers.Secret{{Name: new("Bad_Name"), Value: new("1")}}},
		{name: "neither-value-nor-url", secrets: []*armappcontainers.Secret{{Name: new("a")}}},
		{name: "both-value-and-url", secrets: []*armappcontainers.Secret{
			{
				Name:        new("a"),
				Value:       new("1"),
				KeyVaultURL: new("https://v.vault.azure.net/secrets/x"),
				Identity:    new("system"),
			},
		}},
		{name: "keyvault-url-malformed", ident: &armappcontainers.ManagedServiceIdentity{
			Type: new(armappcontainers.ManagedServiceIdentityTypeSystemAssigned),
		}, secrets: []*armappcontainers.Secret{
			{Name: new("a"), KeyVaultURL: new("https://v.vault.azure.net/keys/x"), Identity: new("system")},
		}},
		{name: "keyvault-without-identity", secrets: []*armappcontainers.Secret{
			{Name: new("a"), KeyVaultURL: new("https://v.vault.azure.net/secrets/x"), Identity: new("system")},
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := s.putApp(t, "rg", "bad-"+strings.ReplaceAll(tc.name, "_", "-"), envID,
				acaApp{secrets: tc.secrets, ident: tc.ident})
			code, status := respCode(t, err)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "InvalidRequestParameterWithDetails", code)
		})
	}
}

type fakeVault struct{ values map[string]string }

var errFakeVaultMissing = errors.New("secret not found")

func (f fakeVault) SecretValue(vault, name, version string) (string, error) {
	if v, ok := f.values[vault+"/"+name+"/"+version]; ok {
		return v, nil
	}

	return "", errFakeVaultMissing
}

func TestACA_KeyVaultSecrets(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	s.apps.SetKeyVaultSecrets(fakeVault{values: map[string]string{"myvault/api-key/": "from-vault"}})

	envID := *s.mustEnv(t, "rg", "env1").ID
	sys := &armappcontainers.ManagedServiceIdentity{
		Type: new(armappcontainers.ManagedServiceIdentityTypeSystemAssigned),
	}

	app := s.mustApp(t, "rg", "kv", envID, acaApp{
		ident: sys,
		secrets: []*armappcontainers.Secret{
			{
				Name: new(
					"api-key",
				),
				KeyVaultURL: new("https://myvault.vault.azure.net/secrets/api-key"),
				Identity:    new("system"),
			},
		},
		env: []*armappcontainers.EnvironmentVar{{Name: new("API_KEY"), SecretRef: new("api-key")}},
	})

	sec := app.Properties.Configuration.Secrets[0]
	assert.Equal(t, "https://myvault.vault.azure.net/secrets/api-key", *sec.KeyVaultURL)
	assert.Equal(t, "system", *sec.Identity)
	assert.Nil(t, sec.Value)

	var found bool

	for _, sp := range s.rt.started() {
		if strings.Contains(sp.Name, "-kv-") {
			found = assert.Contains(t, sp.Env, "API_KEY=from-vault")
		}
	}

	assert.True(t, found, "the Key Vault value reaches the container")

	_, err := s.factory.NewContainerAppsClient().ListSecrets(t.Context(), "rg", "kv", nil)
	require.NoError(t, err)

	missing, err := s.putApp(t, "rg", "kv-missing", envID, acaApp{
		ident: sys,
		secrets: []*armappcontainers.Secret{
			{
				Name: new(
					"nope",
				),
				KeyVaultURL: new("https://myvault.vault.azure.net/secrets/absent"),
				Identity:    new("system"),
			},
		},
	})
	require.NoError(t, err, "an unreadable Key Vault secret fails the revision, not the request")

	revs := s.revisions(t, "kv-missing")
	require.Len(t, revs, 1)
	assert.Equal(t, armappcontainers.RevisionRunningStateFailed, *revs[0].Properties.RunningState)
	assert.Contains(t, *revs[0].Properties.ProvisioningError, "absent")
	assert.Nil(t, missing.Properties.LatestReadyRevisionName)
}

func TestACA_Identity(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	envID := *s.mustEnv(t, "rg", "env1").ID

	app := s.mustApp(t, "rg", "ident", envID, acaApp{ident: &armappcontainers.ManagedServiceIdentity{
		Type: new(armappcontainers.ManagedServiceIdentityTypeSystemAssigned),
	}})
	require.NotNil(t, app.Identity)
	assert.NotEmpty(t, *app.Identity.PrincipalID)
	assert.NotEmpty(t, *app.Identity.TenantID)

	principal := *app.Identity.PrincipalID

	again := s.mustApp(t, "rg", "ident", envID, acaApp{ident: &armappcontainers.ManagedServiceIdentity{
		Type: new(armappcontainers.ManagedServiceIdentityTypeSystemAssigned),
	}})
	assert.Equal(t, principal, *again.Identity.PrincipalID, "the system principal survives re-application")

	_, err := s.putApp(t, "rg", "ghost-uai", envID, acaApp{ident: &armappcontainers.ManagedServiceIdentity{
		Type: new(armappcontainers.ManagedServiceIdentityTypeUserAssigned),
		UserAssignedIdentities: map[string]*armappcontainers.UserAssignedIdentity{
			"/subscriptions/" + aciSub + "/resourceGroups/rg/providers/Microsoft.ManagedIdentity/" +
				"userAssignedIdentities/none": {},
		},
	}})
	code, status := respCode(t, err)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "FailedIdentityOperation", code)

	cleared := s.mustApp(t, "rg", "ident", envID, acaApp{ident: &armappcontainers.ManagedServiceIdentity{
		Type: new(armappcontainers.ManagedServiceIdentityTypeNone),
	}})
	assert.Nil(t, cleared.Identity)
}

func TestACA_StartStopAndDelete(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	envID := *s.mustEnv(t, "rg", "env1").ID
	client := s.factory.NewContainerAppsClient()

	s.mustApp(t, "rg", "ctl", envID, acaApp{})
	assert.Equal(t, 1, s.rt.live())

	stop, err := client.BeginStop(t.Context(), "rg", "ctl", nil)
	require.NoError(t, err)

	stopped, err := stop.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.Equal(t, armappcontainers.ContainerAppRunningStatusStopped, *stopped.Properties.RunningStatus)
	assert.Equal(t, 0, s.rt.live(), "stopping removes the replicas")

	start, err := client.BeginStart(t.Context(), "rg", "ctl", nil)
	require.NoError(t, err)

	started, err := start.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.Equal(t, armappcontainers.ContainerAppRunningStatusRunning, *started.Properties.RunningStatus)
	assert.Equal(t, 1, s.rt.live())

	del, err := client.BeginDelete(t.Context(), "rg", "ctl", nil)
	require.NoError(t, err)

	_, err = del.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.Equal(t, 0, s.rt.live())

	_, err = client.Get(t.Context(), "rg", "ctl", nil)
	_, status := respCode(t, err)
	assert.Equal(t, http.StatusNotFound, status)

	again, err := client.BeginDelete(t.Context(), "rg", "ctl", nil)
	require.NoError(t, err)

	_, err = again.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err, "deleting a missing app is idempotent")
}

func TestACA_PatchAndListing(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	envID := *s.mustEnv(t, "rg", "env1").ID
	client := s.factory.NewContainerAppsClient()

	first := s.mustApp(t, "rg", "one", envID, acaApp{})
	s.mustApp(t, "rg2", "two", envID, acaApp{})

	patch, err := client.BeginUpdate(t.Context(), "rg", "one", armappcontainers.ContainerApp{
		Tags: map[string]*string{"patched": new("yes")},
	}, nil)
	require.NoError(t, err)

	patched, err := patch.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
	assert.Equal(t, "yes", *patched.Tags["patched"])
	assert.Equal(t, *first.Properties.LatestRevisionName, *patched.Properties.LatestRevisionName,
		"a tag patch keeps the revision")

	count := func(pager func() (int, bool)) int {
		n := 0

		for {
			c, more := pager()
			n += c

			if !more {
				return n
			}
		}
	}

	bySub := client.NewListBySubscriptionPager(nil)
	assert.Equal(t, 2, count(func() (int, bool) {
		page, pageErr := bySub.NextPage(t.Context())
		require.NoError(t, pageErr)

		return len(page.Value), bySub.More()
	}))

	byRG := client.NewListByResourceGroupPager("rg", nil)
	assert.Equal(t, 1, count(func() (int, bool) {
		page, pageErr := byRG.NextPage(t.Context())
		require.NoError(t, pageErr)

		return len(page.Value), byRG.More()
	}))
}

func TestACA_ResourceGroupCascade(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	envID := *s.mustEnv(t, "doomed", "env1").ID

	s.mustApp(t, "doomed", "app1", envID, acaApp{})
	s.mustApp(t, "elsewhere", "app2", envID, acaApp{})
	assert.Equal(t, 2, s.rt.live())

	status, _ := s.raw(t, http.MethodPut, "", "/subscriptions/"+aciSub+"/resourcegroups/doomed?api-version=2021-04-01")
	require.Less(t, status, 300)

	status, _ = s.raw(
		t,
		http.MethodDelete,
		"",
		"/subscriptions/"+aciSub+"/resourcegroups/doomed?api-version=2021-04-01",
	)
	require.Less(t, status, 300)

	_, err := s.factory.NewContainerAppsClient().Get(t.Context(), "doomed", "app1", nil)
	_, code := respCode(t, err)
	assert.Equal(t, http.StatusNotFound, code)

	_, err = s.factory.NewManagedEnvironmentsClient().Get(t.Context(), "doomed", "env1", nil)
	_, code = respCode(t, err)
	assert.Equal(t, http.StatusNotFound, code)

	_, err = s.factory.NewContainerAppsClient().Get(t.Context(), "elsewhere", "app2", nil)
	_, code = respCode(t, err)
	assert.Equal(t, http.StatusNotFound, code, "apps in a deleted environment go with it")

	assert.Equal(t, 0, s.rt.live())
}

func TestACA_PreconditionsAndOperations(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	envID := *s.mustEnv(t, "rg", "env1").ID
	path := "/subscriptions/" + aciSub +
		"/resourceGroups/rg/providers/Microsoft.App/containerApps/etag?api-version=2025-07-01"

	s.mustApp(t, "rg", "etag", envID, acaApp{})

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, s.url+path,
		strings.NewReader(`{"location":"local","properties":{}}`))
	require.NoError(t, err)

	req.Header.Set("If-None-Match", "*")
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	require.NoError(t, err)

	_ = resp.Body.Close()

	assert.Equal(t, http.StatusPreconditionFailed, resp.StatusCode, "If-None-Match:* on an existing app")

	status, _ := s.raw(t, http.MethodGet, "", path)
	assert.Equal(t, http.StatusOK, status)

	getReq, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, s.url+path, http.NoBody)
	require.NoError(t, err)

	getReq.Header.Set("If-Match", `"stale"`)

	delResp, err := s.client.Do(getReq)
	require.NoError(t, err)

	_ = delResp.Body.Close()

	assert.Equal(t, http.StatusPreconditionFailed, delResp.StatusCode, "stale etag blocks delete")
}

func TestACA_SnapshotRestore(t *testing.T) {
	t.Parallel()

	s := newACAStack(t)
	envID := *s.mustEnv(t, "rg", "env1").ID
	multi := armappcontainers.ActiveRevisionsModeMultiple

	s.mustApp(t, "rg", "persist", envID, acaApp{mode: multi, suffix: "one", extIng: true})
	app := s.mustApp(t, "rg", "persist", envID, acaApp{
		mode: multi, suffix: "two", image: "busybox:2", extIng: true,
		secrets: []*armappcontainers.Secret{{Name: new("s"), Value: new("v")}},
	})

	snap := s.handler.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	other := newACAStack(t)
	require.NoError(t, other.handler.Restore(t.Context(), snap))

	got, err := other.factory.NewContainerAppsClient().Get(t.Context(), "rg", "persist", nil)
	require.NoError(t, err)
	assert.Equal(t, *app.Properties.LatestRevisionName, *got.Properties.LatestRevisionName)
	assert.Equal(t, *app.Properties.Configuration.Ingress.Fqdn, *got.Properties.Configuration.Ingress.Fqdn)
	assert.Len(t, other.revisions(t, "persist"), 2)

	list, err := other.factory.NewContainerAppsClient().ListSecrets(t.Context(), "rg", "persist", nil)
	require.NoError(t, err)
	assert.Equal(t, "v", *list.Value[0].Value)

	env, err := other.factory.NewManagedEnvironmentsClient().Get(t.Context(), "rg", "env1", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, *env.Properties.DefaultDomain)
	assert.Equal(t, 0, other.rt.live(), "restored revisions start on demand")
}
