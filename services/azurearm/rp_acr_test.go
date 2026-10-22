package azurearm_test

import (
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

type fakeRegistries struct {
	regs map[string]*fakeReg
	mu   sync.Mutex
}

type fakeReg struct {
	pw1, pw2 string
	admin    bool
}

func newFakeRegistries() *fakeRegistries { return &fakeRegistries{regs: map[string]*fakeReg{}} }

func (f *fakeRegistries) PutRegistry(name string, admin bool) (azurearm.ContainerRegistryInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(name) < 5 {
		return azurearm.ContainerRegistryInfo{}, azurearm.ErrInvalidRequestBody
	}

	r, ok := f.regs[name]
	if !ok {
		r = &fakeReg{pw1: "pw1-" + name, pw2: "pw2-" + name}
		f.regs[name] = r
	}

	r.admin = admin

	return azurearm.ContainerRegistryInfo{CreatedAt: time.Unix(1700000000, 0), AdminEnabled: admin}, nil
}

func (f *fakeRegistries) RegistryInfo(name string) (azurearm.ContainerRegistryInfo, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	r, ok := f.regs[name]
	if !ok {
		return azurearm.ContainerRegistryInfo{}, false
	}

	return azurearm.ContainerRegistryInfo{CreatedAt: time.Unix(1700000000, 0), AdminEnabled: r.admin}, true
}

func (f *fakeRegistries) DeleteRegistry(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.regs, name)

	return nil
}

func (f *fakeRegistries) Credentials(name string) (azurearm.ContainerRegistryCredentials, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	r, ok := f.regs[name]
	if !ok {
		return azurearm.ContainerRegistryCredentials{}, azurearm.ErrACRRegistryNotFound
	}

	if !r.admin {
		return azurearm.ContainerRegistryCredentials{}, azurearm.ErrACRAdminDisabled
	}

	return azurearm.ContainerRegistryCredentials{Username: name, Password: r.pw1, Password2: r.pw2}, nil
}

func (f *fakeRegistries) RegenerateCredential(name, which string) (azurearm.ContainerRegistryCredentials, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	r := f.regs[name]
	if which == "password" {
		r.pw1 += "-new"
	} else {
		r.pw2 += "-new"
	}

	return azurearm.ContainerRegistryCredentials{Username: name, Password: r.pw1, Password2: r.pw2}, nil
}

const (
	acrBase    = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.ContainerRegistry/registries/gopherreg"
	acrVersion = "?api-version=2023-07-01"
)

func newACRHandler(t *testing.T, premium bool) *azurearm.Handler {
	t.Helper()

	h := azurearm.NewTestHandlerWithContainerRegistry(newFakeRegistries(), azurearm.DefaultSettings())

	st, _ := doRequest(t, h, http.MethodPut, "/subscriptions/sub/resourceGroups/rg", []byte(`{"location":"local"}`))
	require.Equal(t, http.StatusCreated, st)

	sku := "Basic"
	if premium {
		sku = "Premium"
	}

	body := `{"location":"local","sku":{"name":"` + sku + `"},"tags":{"env":"test"},"properties":{"adminUserEnabled":true}}`
	st, out := doRequest(t, h, http.MethodPut, acrBase+acrVersion, []byte(body))
	require.Equal(t, http.StatusOK, st, "%v", out)

	return h
}

func TestContainerRegistryRegistryCRUD(t *testing.T) {
	t.Parallel()

	h := newACRHandler(t, false)

	st, got := doRequest(t, h, http.MethodGet, acrBase+acrVersion, nil)
	require.Equal(t, http.StatusOK, st)
	assert.Equal(t, "Microsoft.ContainerRegistry/registries", got["type"])
	assert.Equal(t, map[string]any{"name": "Basic", "tier": "Basic"}, got["sku"])
	assert.Equal(t, map[string]any{"env": "test"}, got["tags"])

	pr := props(got)
	assert.Equal(t, true, pr["adminUserEnabled"])
	assert.Equal(t, "gopherreg.azurecr.host:10011", pr["loginServer"])
	assert.Equal(t, "Succeeded", pr["provisioningState"])
	assert.Equal(t, "Enabled", pr["publicNetworkAccess"])

	st, list := doRequest(t, h, http.MethodGet,
		"/subscriptions/sub/providers/Microsoft.ContainerRegistry/registries"+acrVersion, nil)
	require.Equal(t, http.StatusOK, st)
	assert.Len(t, list["value"], 1)

	st, list = doRequest(t, h, http.MethodGet,
		"/subscriptions/sub/resourceGroups/rg/providers/Microsoft.ContainerRegistry/registries"+acrVersion, nil)
	require.Equal(t, http.StatusOK, st)
	assert.Len(t, list["value"], 1)

	st, _ = doRequest(t, h, http.MethodDelete, acrBase+acrVersion, nil)
	assert.Equal(t, http.StatusOK, st)

	st, _ = doRequest(t, h, http.MethodGet, acrBase+acrVersion, nil)
	assert.Equal(t, http.StatusNotFound, st)
}

func TestContainerRegistryPut(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		body   string
		status int
	}{
		{"missing sku", acrBase + "new", `{"location":"local"}`, http.StatusBadRequest},
		{"bad sku", acrBase, `{"sku":{"name":"Gold"}}`, http.StatusBadRequest},
		{"short name rejected by data plane", "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.ContainerRegistry/registries/abc",
			`{"sku":{"name":"Basic"}}`, http.StatusBadRequest},
		{"other group name taken", "/subscriptions/sub/resourceGroups/rg2/providers/Microsoft.ContainerRegistry/registries/GopherReg",
			`{"sku":{"name":"Basic"}}`, http.StatusConflict},
		{"ok", acrBase, `{"sku":{"name":"Standard"}}`, http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACRHandler(t, false)
			st, _ := doRequest(t, h, http.MethodPut, tt.path+acrVersion, []byte(tt.body))
			assert.Equal(t, tt.status, st)
		})
	}
}

func TestContainerRegistryCredentials(t *testing.T) {
	t.Parallel()

	h := newACRHandler(t, false)

	st, creds := doRequest(t, h, http.MethodPost, acrBase+"/listCredentials"+acrVersion, nil)
	require.Equal(t, http.StatusOK, st)
	assert.Equal(t, "gopherreg", creds["username"])

	pws, _ := creds["passwords"].([]any)
	require.Len(t, pws, 2)
	assert.Equal(t, map[string]any{"name": "password", "value": "pw1-gopherreg"}, pws[0])

	st, creds = doRequest(t, h, http.MethodPost, acrBase+"/regenerateCredential"+acrVersion, []byte(`{"name":"password2"}`))
	require.Equal(t, http.StatusOK, st)
	assert.Equal(t, "pw2-gopherreg-new", creds["passwords"].([]any)[1].(map[string]any)["value"])

	st, _ = doRequest(t, h, http.MethodPost, acrBase+"/regenerateCredential"+acrVersion, []byte(`{"name":"bogus"}`))
	assert.Equal(t, http.StatusBadRequest, st)

	st, _ = doRequest(t, h, http.MethodPost, acrBase+"x/listCredentials"+acrVersion, nil)
	assert.Equal(t, http.StatusNotFound, st)

	// Turning the admin user off hides the credentials.
	st, _ = doRequest(t, h, http.MethodPatch, acrBase+acrVersion, []byte(`{"properties":{"adminUserEnabled":false}}`))
	require.Equal(t, http.StatusOK, st)

	st, out := doRequest(t, h, http.MethodPost, acrBase+"/listCredentials"+acrVersion, nil)
	assert.Equal(t, http.StatusBadRequest, st)
	assert.Contains(t, out["error"], "code")
}

func TestContainerRegistryPatch(t *testing.T) {
	t.Parallel()

	h := newACRHandler(t, false)

	st, got := doRequest(t, h, http.MethodPatch, acrBase+acrVersion,
		[]byte(`{"tags":{"a":"b"},"sku":{"name":"Standard"},"properties":{"anonymousPullEnabled":true}}`))
	require.Equal(t, http.StatusOK, st, "%v", got)
	assert.Equal(t, map[string]any{"a": "b"}, got["tags"])
	assert.Equal(t, "Standard", got["sku"].(map[string]any)["name"])
	assert.Equal(t, true, props(got)["anonymousPullEnabled"])
	assert.Equal(t, true, props(got)["adminUserEnabled"], "untouched properties are kept")

	st, _ = doRequest(t, h, http.MethodPatch, acrBase+acrVersion, []byte(`{"sku":{"name":"Gold"}}`))
	assert.Equal(t, http.StatusBadRequest, st)

	st, _ = doRequest(t, h, http.MethodPatch, acrBase+"x"+acrVersion, []byte(`{}`))
	assert.Equal(t, http.StatusNotFound, st)
}

func TestContainerRegistryCheckName(t *testing.T) {
	t.Parallel()

	h := newACRHandler(t, false)
	path := "/subscriptions/sub/providers/Microsoft.ContainerRegistry/checkNameAvailability" + acrVersion

	tests := []struct {
		name   string
		body   string
		reason string
		want   bool
	}{
		{"free", `{"name":"freename1","type":"Microsoft.ContainerRegistry/registries"}`, "", true},
		{"taken", `{"name":"GopherReg","type":"Microsoft.ContainerRegistry/registries"}`, "AlreadyExists", false},
		{"invalid", `{"name":"no-dash","type":"Microsoft.ContainerRegistry/registries"}`, "Invalid", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			st, out := doRequest(t, h, http.MethodPost, path, []byte(tt.body))
			require.Equal(t, http.StatusOK, st)
			assert.Equal(t, tt.want, out["nameAvailable"])

			if tt.reason != "" {
				assert.Equal(t, tt.reason, out["reason"])
			}
		})
	}
}

func TestContainerRegistryReplications(t *testing.T) {
	t.Parallel()

	repl := acrBase + "/replications/westus"

	t.Run("premium only", func(t *testing.T) {
		t.Parallel()

		h := newACRHandler(t, false)
		st, _ := doRequest(t, h, http.MethodPut, repl+acrVersion, []byte(`{"location":"westus"}`))
		assert.Equal(t, http.StatusBadRequest, st)
	})

	t.Run("lifecycle", func(t *testing.T) {
		t.Parallel()

		h := newACRHandler(t, true)

		st, got := doRequest(t, h, http.MethodPut, repl+acrVersion,
			[]byte(`{"location":"westus","tags":{"x":"y"},"properties":{"regionEndpointEnabled":false}}`))
		require.Equal(t, http.StatusOK, st, "%v", got)
		assert.Equal(t, "Microsoft.ContainerRegistry/registries/replications", got["type"])
		assert.Equal(t, false, props(got)["regionEndpointEnabled"])

		st, list := doRequest(t, h, http.MethodGet, acrBase+"/replications"+acrVersion, nil)
		require.Equal(t, http.StatusOK, st)
		assert.Len(t, list["value"], 1)

		st, _ = doRequest(t, h, http.MethodGet, repl+acrVersion, nil)
		assert.Equal(t, http.StatusOK, st)

		st, _ = doRequest(t, h, http.MethodDelete, repl+acrVersion, nil)
		assert.Equal(t, http.StatusOK, st)

		st, _ = doRequest(t, h, http.MethodGet, repl+acrVersion, nil)
		assert.Equal(t, http.StatusNotFound, st)
	})

	t.Run("missing parent", func(t *testing.T) {
		t.Parallel()

		h := newACRHandler(t, true)
		st, _ := doRequest(t, h, http.MethodGet,
			"/subscriptions/sub/resourceGroups/rg/providers/Microsoft.ContainerRegistry/registries/nope/replications"+acrVersion, nil)
		assert.Equal(t, http.StatusNotFound, st)
	})
}

func TestContainerRegistryWebhooks(t *testing.T) {
	t.Parallel()

	hook := acrBase + "/webhooks/deployhook"
	good := `{"location":"local","properties":{"serviceUri":"https://example.com/h","actions":["push"],` +
		`"customHeaders":{"Authorization":"secret"},"scope":"app:*"}}`

	h := newACRHandler(t, false)

	tests := []struct {
		name   string
		path   string
		body   string
		status int
	}{
		{"missing uri", hook, `{"location":"local","properties":{"actions":["push"]}}`, http.StatusBadRequest},
		{"missing actions", hook, `{"location":"local","properties":{"serviceUri":"https://e"}}`, http.StatusBadRequest},
		{"bad name", acrBase + "/webhooks/x", good, http.StatusBadRequest},
		{"ok", hook, good, http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st, _ := doRequest(t, h, http.MethodPut, tt.path+acrVersion, []byte(tt.body))
			assert.Equal(t, tt.status, st)
		})
	}

	st, got := doRequest(t, h, http.MethodGet, hook+acrVersion, nil)
	require.Equal(t, http.StatusOK, st)

	pr := props(got)
	assert.Equal(t, "enabled", pr["status"])
	assert.Equal(t, "app:*", pr["scope"])
	assert.NotContains(t, pr, "customHeaders", "headers are only returned by getCallbackConfig")

	st, cfg := doRequest(t, h, http.MethodPost, hook+"/getCallbackConfig"+acrVersion, nil)
	require.Equal(t, http.StatusOK, st)
	assert.Equal(t, "https://example.com/h", cfg["serviceUri"])
	assert.Equal(t, map[string]any{"Authorization": "secret"}, cfg["customHeaders"])

	st, list := doRequest(t, h, http.MethodGet, acrBase+"/webhooks"+acrVersion, nil)
	require.Equal(t, http.StatusOK, st)
	assert.Len(t, list["value"], 1)

	// Deleting the registry removes its children.
	st, _ = doRequest(t, h, http.MethodDelete, acrBase+acrVersion, nil)
	require.Equal(t, http.StatusOK, st)

	st, _ = doRequest(t, h, http.MethodGet, hook+acrVersion, nil)
	assert.Equal(t, http.StatusNotFound, st)
}

func TestContainerRegistrySnapshot(t *testing.T) {
	t.Parallel()

	dp := newFakeRegistries()
	p := azurearm.NewContainerRegistryProvider(azurearm.ContainerRegistryEndpointConfig{}, dp)
	id := azurearm.ResourceID{
		SubscriptionID: "sub", ResourceGroup: "rg", Namespace: "Microsoft.ContainerRegistry",
		Types: []string{"registries"}, Names: []string{"snapreg"},
	}

	_, err := p.Put(t.Context(), id, map[string]any{
		"location": "westus", "sku": map[string]any{"name": "Premium"}, "tags": map[string]any{"a": "b"},
		"properties": map[string]any{"adminUserEnabled": true, "zoneRedundancy": "Enabled"},
	})
	require.NoError(t, err)

	snap := azurearm.ContainerRegistrySnapshotJSON(p)

	restored := azurearm.NewContainerRegistryProvider(azurearm.ContainerRegistryEndpointConfig{}, dp)
	azurearm.ContainerRegistryRestoreJSON(t, restored, snap)

	got, err := restored.Get(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, "westus", got["location"])
	assert.Equal(t, "Enabled", props(got)["zoneRedundancy"])
	assert.Equal(t, "Premium", got["sku"].(map[string]any)["name"])

	restored.Reset()
	_, err = restored.Get(t.Context(), id)
	require.ErrorIs(t, err, azurearm.ErrACRRegistryNotFound)
}

func TestContainerRegistryNoDataPlane(t *testing.T) {
	t.Parallel()

	p := azurearm.NewContainerRegistryProvider(azurearm.ContainerRegistryEndpointConfig{}, nil)
	id := azurearm.ResourceID{
		SubscriptionID: "sub", ResourceGroup: "rg", Namespace: "Microsoft.ContainerRegistry",
		Types: []string{"registries"}, Names: []string{"nodataplane"},
	}

	_, err := p.Put(t.Context(), id, map[string]any{"sku": map[string]any{"name": "Basic"}})
	require.ErrorIs(t, err, azurearm.ErrACRUnavailable)
}
