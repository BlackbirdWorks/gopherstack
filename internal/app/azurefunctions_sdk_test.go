package app

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appservice/armappservice/v6"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

const (
	fnSDKSub    = "00000000-0000-0000-0000-000000000000"
	fnSDKRG     = "fn-rg"
	fnSDKDomain = "azurewebsites.localhost"
)

type fnSDKCredential struct{}

func (fnSDKCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "emulator"}, nil
}

// fnFakeRuntime serves every "container" from an in-process HTTP server on its published port and answers
// the host admin API with a fixed function list.
type fnFakeRuntime struct {
	boxes map[string]*http.Server
	specs []container.Spec
	mu    sync.Mutex
	seq   int
}

func (f *fnFakeRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	_, host, _, err := container.ParsePortSpec(spec.Ports[0])
	if err != nil {
		return "", err
	}

	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", net.JoinHostPort("127.0.0.1", host))
	if err != nil {
		return "", err
	}

	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.UserAgent() == gcpserve.ProbeUserAgent {
			return
		}

		if name, ok := strings.CutPrefix(r.URL.Path, "/env/"); ok {
			for _, e := range spec.Env {
				if k, v, _ := strings.Cut(e, "="); k == name {
					_, _ = w.Write([]byte(v))
				}
			}

			return
		}

		if r.URL.Path == "/marker" {
			for _, m := range spec.Mounts {
				if dir, _, found := strings.Cut(m, ":/home/site/wwwroot"); found {
					raw, _ := os.ReadFile(filepath.Join(dir, "marker.txt"))
					_, _ = w.Write(raw)
				}
			}

			return
		}

		if r.URL.Path == "/admin/functions" {
			_, _ = w.Write([]byte(`[{"name":"hello","language":"node","isDisabled":false,"config":{"bindings":` +
				`[{"type":"httpTrigger","authLevel":"function","direction":"in","name":"req","route":"greet"}]}},` +
				`{"name":"tick","language":"node","isDisabled":false,"config":{"bindings":[{"type":"timerTrigger"}]}}]`))

			return
		}

		_, _ = fmt.Fprintf(w, "host saw %s", r.URL.Path)
	})}

	go func() { _ = srv.Serve(l) }()

	f.mu.Lock()
	defer f.mu.Unlock()

	f.seq++
	id := fmt.Sprintf("fn-%d", f.seq)
	f.boxes[id] = srv
	f.specs = append(f.specs, spec)

	return id, nil
}

func (f *fnFakeRuntime) StopAndRemove(_ context.Context, id string) error {
	f.mu.Lock()
	srv := f.boxes[id]
	delete(f.boxes, id)
	f.mu.Unlock()

	if srv != nil {
		_ = srv.Close()
	}

	return nil
}

func (f *fnFakeRuntime) lastEnv(site string) map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := map[string]string{}

	for _, spec := range f.specs {
		env := map[string]string{}

		for _, e := range spec.Env {
			k, v, _ := strings.Cut(e, "=")
			env[k] = v
		}

		if env["WEBSITE_SITE_NAME"] == site {
			out = env
		}
	}

	return out
}

type fnSDKStack struct {
	arm     *azurearm.Handler
	factory *armappservice.ClientFactory
	backend *azurefunctions.InMemoryBackend
	rt      *fnFakeRuntime
	fnSrv   *httptest.Server
}

func newFnSDKStack(t *testing.T) *fnSDKStack {
	t.Helper()

	armSvc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	armH, ok := armSvc.(*azurearm.Handler)
	require.True(t, ok)

	backend := azurefunctions.NewInMemoryBackend()
	backend.Configure(azurefunctions.Settings{Domain: fnSDKDomain, Port: azurefunctions.DefaultPort})

	rt := &fnFakeRuntime{boxes: map[string]*http.Server{}}
	backend.SetRuntime(func() (azurefunctions.Runner, error) { return rt, nil })
	t.Cleanup(backend.Close)

	armH.Registry.SetFunctionApps(&azureFunctionAppsAdapter{backend: backend})

	e := echo.New()
	e.Any("/*", armH.Handler())

	armSrv := httptest.NewTLSServer(e)
	t.Cleanup(armSrv.Close)

	fnSrv := httptest.NewServer(azurefunctions.NewHandler(backend, azurefunctions.DefaultSettings()))
	t.Cleanup(fnSrv.Close)

	f, err := armappservice.NewClientFactory(fnSDKSub, fnSDKCredential{}, &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: armSrv.URL, Audience: "https://management.core.windows.net/"},
		}},
		Transport: armSrv.Client(),
		Retry:     policy.RetryOptions{MaxRetries: -1},
	})
	require.NoError(t, err)

	return &fnSDKStack{arm: armH, factory: f, backend: backend, rt: rt, fnSrv: fnSrv}
}

// call sends a request to the Functions listener addressed to host.
func (s *fnSDKStack) call(t *testing.T, method, host, path string, body []byte, user, pass string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, s.fnSrv.URL+path, bytes.NewReader(body))
	require.NoError(t, err)

	req.Host = host

	if user != "" {
		req.SetBasicAuth(user, pass)
	}

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(raw)
}

func fnZip(t *testing.T) []byte {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	for name, content := range map[string]string{
		"host.json":           `{"version":"2.0"}`,
		"hello/function.json": `{"bindings":[{"authLevel":"function","type":"httpTrigger","direction":"in","name":"req"}]}`,
		"hello/index.js":      "module.exports = async () => {}",
	} {
		w, err := zw.Create(name)
		require.NoError(t, err)

		_, err = w.Write([]byte(content))
		require.NoError(t, err)
	}

	require.NoError(t, zw.Close())

	return buf.Bytes()
}

func (s *fnSDKStack) createPlan(t *testing.T, name string) *armappservice.Plan {
	t.Helper()

	poller, err := s.factory.NewPlansClient().BeginCreateOrUpdate(t.Context(), fnSDKRG, name, armappservice.Plan{
		Location: new("local"), Kind: new("functionapp"),
		SKU:        &armappservice.SKUDescription{Name: new("Y1"), Tier: new("Dynamic")},
		Properties: &armappservice.PlanProperties{Reserved: new(true)},
	}, nil)
	require.NoError(t, err)

	res, err := poller.PollUntilDone(t.Context(), pollOpts())
	require.NoError(t, err)

	return &res.Plan
}

func (s *fnSDKStack) createSite(t *testing.T, name, planID string, settings map[string]string) *armappservice.Site {
	t.Helper()

	appSettings := make([]*armappservice.NameValuePair, 0, len(settings))
	for k, v := range settings {
		appSettings = append(appSettings, &armappservice.NameValuePair{Name: new(k), Value: new(v)})
	}

	poller, err := s.factory.NewWebAppsClient().BeginCreateOrUpdate(t.Context(), fnSDKRG, name, armappservice.Site{
		Location: new("local"), Kind: new("functionapp,linux"), Tags: map[string]*string{"env": new("test")},
		Properties: &armappservice.SiteProperties{
			ServerFarmID: new(planID),
			SiteConfig: &armappservice.SiteConfig{
				LinuxFxVersion: new("NODE|20"), AppSettings: appSettings,
			},
		},
	}, nil)
	require.NoError(t, err)

	res, err := poller.PollUntilDone(t.Context(), pollOpts())
	require.NoError(t, err)

	return &res.Site
}

// freshApp creates an independent linux node function app so subtests can run in parallel.
func (s *fnSDKStack) freshApp(t *testing.T, name, planID string) string {
	t.Helper()

	s.createSite(t, name, planID, map[string]string{"FUNCTIONS_WORKER_RUNTIME": "node", "MY": "v1"})

	return name
}

func TestAzureFunctionsARMSDK_Lifecycle(t *testing.T) {
	t.Parallel()

	s := newFnSDKStack(t)
	ctx := t.Context()
	apps := s.factory.NewWebAppsClient()

	plan := s.createPlan(t, "plan1")
	require.NotNil(t, plan.Properties)
	assert.Equal(t, "Ready", string(*plan.Properties.Status))
	assert.Equal(t, int32(0), *plan.Properties.NumberOfSites)

	site := s.createSite(
		t,
		"lifecycleapp",
		*plan.ID,
		map[string]string{"FUNCTIONS_WORKER_RUNTIME": "node", "MY": "v1"},
	)
	assert.Equal(t, "functionapp,linux", *site.Kind)
	assert.Equal(t, "lifecycleapp."+fnSDKDomain+":10012", *site.Properties.DefaultHostName)
	assert.Equal(t, "Running", *site.Properties.State)
	assert.True(t, *site.Properties.Reserved)
	assert.Equal(t, "test", *site.Tags["env"])

	got, err := apps.Get(ctx, fnSDKRG, "lifecycleapp", nil)
	require.NoError(t, err)
	assert.Equal(t, *plan.ID, *got.Properties.ServerFarmID)

	gotPlan, err := s.factory.NewPlansClient().Get(ctx, fnSDKRG, "plan1", nil)
	require.NoError(t, err)
	assert.Equal(t, int32(1), *gotPlan.Properties.NumberOfSites)

	t.Run("app settings", func(t *testing.T) {
		t.Parallel()

		app := s.freshApp(t, "lc-settings", *plan.ID)

		list, listErr := apps.ListApplicationSettings(ctx, fnSDKRG, app, nil)
		require.NoError(t, listErr)
		assert.Equal(t, "v1", *list.Properties["MY"])

		_, updErr := apps.UpdateApplicationSettings(ctx, fnSDKRG, app, armappservice.StringDictionary{
			Properties: map[string]*string{"FUNCTIONS_WORKER_RUNTIME": new("node"), "MY": new("v2")},
		}, nil)
		require.NoError(t, updErr)

		list, listErr = apps.ListApplicationSettings(ctx, fnSDKRG, app, nil)
		require.NoError(t, listErr)
		assert.Equal(t, "v2", *list.Properties["MY"])
	})

	t.Run("site configuration", func(t *testing.T) {
		t.Parallel()

		app := s.freshApp(t, "lc-config", *plan.ID)

		cfg, cfgErr := apps.GetConfiguration(ctx, fnSDKRG, app, nil)
		require.NoError(t, cfgErr)
		assert.Equal(t, "NODE|20", *cfg.Properties.LinuxFxVersion)
		assert.Equal(t, "$"+app, *cfg.Properties.PublishingUsername)

		_, putErr := apps.CreateOrUpdateConfiguration(ctx, fnSDKRG, app, armappservice.SiteConfigResource{
			Properties: &armappservice.SiteConfig{LinuxFxVersion: new("NODE|22"), Http20Enabled: new(false)},
		}, nil)
		require.NoError(t, putErr)

		cfg, cfgErr = apps.GetConfiguration(ctx, fnSDKRG, app, nil)
		require.NoError(t, cfgErr)
		assert.Equal(t, "NODE|22", *cfg.Properties.LinuxFxVersion)
		assert.False(t, *cfg.Properties.Http20Enabled)
	})

	t.Run("deploy invoke and keys", func(t *testing.T) {
		t.Parallel()

		app := s.freshApp(t, "lc-deploy", *plan.ID)

		poller, credErr := apps.BeginListPublishingCredentials(ctx, fnSDKRG, app, nil)
		require.NoError(t, credErr)

		cred, credErr := poller.PollUntilDone(ctx, pollOpts())
		require.NoError(t, credErr)
		user, pass := *cred.Properties.PublishingUserName, *cred.Properties.PublishingPassword
		assert.Equal(t, "$"+app, user)
		assert.Contains(t, *cred.Properties.ScmURI, app+".scm."+fnSDKDomain)

		code, _ := s.call(
			t,
			http.MethodPost,
			app+".scm."+fnSDKDomain,
			"/api/zipdeploy",
			fnZip(t),
			user,
			"wrong",
		)
		assert.Equal(t, http.StatusUnauthorized, code)

		code, _ = s.call(t, http.MethodPost, app+".scm."+fnSDKDomain, "/api/zipdeploy", fnZip(t), user, pass)
		require.Equal(t, http.StatusOK, code)

		code, body := s.call(t, http.MethodGet, app+"."+fnSDKDomain, "/api/greet", nil, "", "")
		require.Equal(t, http.StatusOK, code)
		assert.Equal(t, "host saw /api/greet", body)
		assert.Equal(t, "v1", s.rt.lastEnv(app)["MY"], "settings reach the host environment")
		assert.Equal(t, app, s.rt.lastEnv(app)["WEBSITE_SITE_NAME"])

		var names []string

		pager := apps.NewListFunctionsPager(fnSDKRG, app, nil)
		for pager.More() {
			page, pageErr := pager.NextPage(ctx)
			require.NoError(t, pageErr)

			for _, f := range page.Value {
				names = append(names, *f.Name)
			}
		}

		assert.Equal(t, []string{app + "/hello", app + "/tick"}, names)

		fn, fnErr := apps.GetFunction(ctx, fnSDKRG, app, "hello", nil)
		require.NoError(t, fnErr)
		assert.Equal(t, "node", *fn.Properties.Language)
		assert.Equal(t, "http://"+app+"."+fnSDKDomain+":10012/api/greet", *fn.Properties.InvokeURLTemplate)

		_, missErr := apps.GetFunction(ctx, fnSDKRG, app, "nope", nil)
		require.Error(t, missErr)

		keys, keysErr := apps.ListFunctionKeys(ctx, fnSDKRG, app, "hello", nil)
		require.NoError(t, keysErr)
		require.Contains(t, keys.Properties, "default")

		secrets, secErr := apps.ListFunctionSecrets(ctx, fnSDKRG, app, "hello", nil)
		require.NoError(t, secErr)
		assert.Equal(t, *keys.Properties["default"], *secrets.Key)
		assert.Equal(t, "http://"+app+"."+fnSDKDomain+":10012/api/greet?code="+*secrets.Key, *secrets.TriggerURL)

		made, mkErr := apps.CreateOrUpdateFunctionSecret(ctx, fnSDKRG, app, "hello", "extra",
			armappservice.KeyInfo{Name: new("extra"), Value: new("extra-key-value==")}, nil)
		require.NoError(t, mkErr)
		assert.Equal(t, "extra-key-value==", *made.Value)

		keys, keysErr = apps.ListFunctionKeys(ctx, fnSDKRG, app, "hello", nil)
		require.NoError(t, keysErr)
		assert.Equal(t, "extra-key-value==", *keys.Properties["extra"])

		_, delErr := apps.DeleteFunctionSecret(ctx, fnSDKRG, app, "hello", "extra", nil)
		require.NoError(t, delErr)

		_, delErr = apps.DeleteFunctionSecret(ctx, fnSDKRG, app, "hello", "extra", nil)
		require.Error(t, delErr)
	})

	t.Run("host keys", func(t *testing.T) {
		t.Parallel()

		app := s.freshApp(t, "lc-hostkeys", *plan.ID)

		hk, hkErr := apps.ListHostKeys(ctx, fnSDKRG, app, nil)
		require.NoError(t, hkErr)
		assert.NotEmpty(t, *hk.MasterKey)
		assert.Contains(t, hk.FunctionKeys, "default")

		made, mkErr := apps.CreateOrUpdateHostSecret(ctx, fnSDKRG, app, "functionKeys", "ci",
			armappservice.KeyInfo{Name: new("ci"), Value: new("ci-host-key==")}, nil)
		require.NoError(t, mkErr)
		assert.Equal(t, "ci-host-key==", *made.Value)

		hk, hkErr = apps.ListHostKeys(ctx, fnSDKRG, app, nil)
		require.NoError(t, hkErr)
		assert.Equal(t, "ci-host-key==", *hk.FunctionKeys["ci"])

		_, badErr := apps.CreateOrUpdateHostSecret(ctx, fnSDKRG, app, "masterKey", "x",
			armappservice.KeyInfo{Name: new("x")}, nil)
		require.Error(t, badErr)

		_, delErr := apps.DeleteHostSecret(ctx, fnSDKRG, app, "functionKeys", "ci", nil)
		require.NoError(t, delErr)

		hk, hkErr = apps.ListHostKeys(ctx, fnSDKRG, app, nil)
		require.NoError(t, hkErr)
		assert.NotContains(t, hk.FunctionKeys, "ci")
	})

	t.Run("stop start restart", func(t *testing.T) {
		t.Parallel()

		app := s.freshApp(t, "lc-state", *plan.ID)

		_, stopErr := apps.Stop(ctx, fnSDKRG, app, nil)
		require.NoError(t, stopErr)

		_, deployErr := s.backend.Deploy(app, fnZip(t), "test", "m")
		require.NoError(t, deployErr)

		stopped, getErr := apps.Get(ctx, fnSDKRG, app, nil)
		require.NoError(t, getErr)
		assert.Equal(t, "Stopped", *stopped.Properties.State)

		code, _ := s.call(t, http.MethodGet, app+"."+fnSDKDomain, "/api/greet", nil, "", "")
		assert.Equal(t, http.StatusForbidden, code)

		_, startErr := apps.Start(ctx, fnSDKRG, app, nil)
		require.NoError(t, startErr)

		_, restartErr := apps.Restart(ctx, fnSDKRG, app, nil)
		require.NoError(t, restartErr)

		code, _ = s.call(t, http.MethodGet, app+"."+fnSDKDomain, "/api/greet", nil, "", "")
		assert.Equal(t, http.StatusOK, code)
	})

	t.Run("update tags", func(t *testing.T) {
		t.Parallel()

		app := s.freshApp(t, "lc-update", *plan.ID)

		upd, updErr := apps.Update(ctx, fnSDKRG, app, armappservice.SitePatchResource{
			Properties: &armappservice.SitePatchResourceProperties{HTTPSOnly: new(true)},
		}, nil)
		require.NoError(t, updErr)
		assert.True(t, *upd.Properties.HTTPSOnly)
		assert.Equal(t, "test", *upd.Tags["env"], "patch keeps what it does not set")
		assert.Equal(t, "NODE|20", *upd.Properties.SiteConfig.LinuxFxVersion)
	})

	t.Run("list", func(t *testing.T) {
		t.Parallel()

		app := s.freshApp(t, "lc-list", *plan.ID)

		pager := apps.NewListByResourceGroupPager(fnSDKRG, nil)

		var names []string

		for pager.More() {
			page, pageErr := pager.NextPage(ctx)
			require.NoError(t, pageErr)

			for _, st := range page.Value {
				names = append(names, *st.Name)
			}
		}

		assert.Contains(t, names, app)
	})

	t.Run("delete", func(t *testing.T) {
		t.Parallel()

		delPlan := s.createPlan(t, "lc-delplan")
		app := "lc-delete"
		s.createSite(t, app, *delPlan.ID, nil)

		_, planErr := s.factory.NewPlansClient().Delete(ctx, fnSDKRG, "lc-delplan", nil)
		require.Error(t, planErr, "a plan with sites cannot be deleted")

		_, delErr := apps.Delete(ctx, fnSDKRG, app, nil)
		require.NoError(t, delErr)

		_, getErr := apps.Get(ctx, fnSDKRG, app, nil)
		require.Error(t, getErr)

		code, _ := s.call(t, http.MethodGet, app+"."+fnSDKDomain, "/api/greet", nil, "", "")
		assert.Equal(t, http.StatusNotFound, code)

		_, planErr = s.factory.NewPlansClient().Delete(ctx, fnSDKRG, "lc-delplan", nil)
		require.NoError(t, planErr)
	})
}

func TestAzureFunctionsARMSDK_Validation(t *testing.T) {
	t.Parallel()

	s := newFnSDKStack(t)
	plan := s.createPlan(t, "valplan")
	s.createSite(t, "taken-name", *plan.ID, nil)

	tests := []struct {
		name string
		rg   string
		site string
		kind string
		farm string
	}{
		{"windows web app kind is unsupported", fnSDKRG, "webapp1", "app", *plan.ID},
		{"missing plan", fnSDKRG, "noplan", "functionapp,linux", strings.Replace(*plan.ID, "valplan", "ghost", 1)},
		{"name taken in another group", "other-rg", "taken-name", "functionapp,linux", *plan.ID},
		{"invalid name", fnSDKRG, "bad_name", "functionapp,linux", *plan.ID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			poller, err := s.factory.NewWebAppsClient().
				BeginCreateOrUpdate(t.Context(), tt.rg, tt.site, armappservice.Site{
					Location: new("local"), Kind: new(tt.kind),
					Properties: &armappservice.SiteProperties{ServerFarmID: new(tt.farm)},
				}, nil)
			if err == nil {
				_, err = poller.PollUntilDone(t.Context(), pollOpts())
			}

			var respErr *azcore.ResponseError

			require.ErrorAs(t, err, &respErr)
			assert.Contains(t, []int{http.StatusBadRequest, http.StatusConflict}, respErr.StatusCode)
		})
	}
}

func TestAzureFunctionsARMSDK_CheckNameAndMissing(t *testing.T) {
	t.Parallel()

	s := newFnSDKStack(t)
	ctx := t.Context()
	plan := s.createPlan(t, "cnplan")
	s.createSite(t, "usedname", *plan.ID, nil)

	mgmt := s.factory.NewWebSiteManagementClient()

	tests := []struct {
		name      string
		reason    armappservice.InAvailabilityReasonType
		check     string
		available bool
	}{
		{"free", "", "freshname", true},
		{"taken", armappservice.InAvailabilityReasonTypeAlreadyExists, "usedname", false},
		{"invalid", armappservice.InAvailabilityReasonTypeInvalid, "-bad-", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := mgmt.CheckNameAvailability(ctx, armappservice.ResourceNameAvailabilityRequest{
				Name: new(tt.check), Type: new(armappservice.CheckNameResourceTypesMicrosoftWebSites),
			}, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.available, *res.NameAvailable)

			if !tt.available {
				assert.Equal(t, tt.reason, *res.Reason)
			}
		})
	}

	apps := s.factory.NewWebAppsClient()
	_, err := apps.ListApplicationSettings(ctx, fnSDKRG, "ghost", nil)
	require.Error(t, err)

	_, err = apps.ListHostKeys(ctx, fnSDKRG, "ghost", nil)
	require.Error(t, err)

	pager := apps.NewListFunctionsPager(fnSDKRG, "ghost", nil)
	_, err = pager.NextPage(ctx)
	require.Error(t, err)
}
