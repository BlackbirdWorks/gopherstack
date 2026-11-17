package azurefunctions_test

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

var errExchangeDenied = errors.New("denied")

type exchangeCall struct{ host, tenant, token string }

type fakeExchanger struct {
	err   error
	calls []exchangeCall
	mu    sync.Mutex
}

func (f *fakeExchanger) exchange(_ context.Context, host, tenant, token string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls = append(f.calls, exchangeCall{host, tenant, token})

	if f.err != nil {
		return "", f.err
	}

	return "refresh-for-" + host, nil
}

func (f *fakeExchanger) seen() []exchangeCall {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]exchangeCall(nil), f.calls...)
}

func TestManagedIdentityImagePull(t *testing.T) {
	t.Parallel()

	identity := azurefunctions.Identity{
		TenantID: "tenant-1", SystemPrincipalID: "sys-oid",
		User: []azurefunctions.UserIdentity{{ClientID: "client-u1", PrincipalID: "u1-oid", ResourceID: "/rid/u1"}},
	}
	static := map[string]string{
		"DOCKER_REGISTRY_SERVER_URL": "reg.azurecr.io", "DOCKER_REGISTRY_SERVER_USERNAME": "u",
		"DOCKER_REGISTRY_SERVER_PASSWORD": "p",
	}

	tests := []struct {
		settings  map[string]string
		exchErr   error
		name      string
		image     string
		identity  string
		wantToken string
		wantPull  []string
		wantCalls int
		wantCode  int
		noID      bool
		useMI     bool
	}{
		{name: "system identity", image: "reg.azurecr.io/app:1", useMI: true, wantCalls: 1, wantToken: "sys-oid",
			wantPull: []string{"reg.azurecr.io/app:1|00000000-0000-0000-0000-000000000000|refresh-for-reg.azurecr.io"},
			wantCode: http.StatusOK},
		{name: "user identity", image: "reg.azurecr.io/app:1", useMI: true, identity: "client-u1", wantCalls: 1,
			wantToken: "u1-oid",
			wantPull:  []string{"reg.azurecr.io/app:1|00000000-0000-0000-0000-000000000000|refresh-for-reg.azurecr.io"},
			wantCode:  http.StatusOK},
		{name: "unknown user identity", image: "reg.azurecr.io/app:1", useMI: true, identity: "nope",
			wantCode: http.StatusBadGateway},
		{name: "no identity on the site", image: "reg.azurecr.io/app:1", useMI: true, noID: true,
			wantCode: http.StatusBadGateway},
		{name: "exchange fails", image: "reg.azurecr.io/app:1", useMI: true, exchErr: errExchangeDenied, wantCalls: 1,
			wantToken: "sys-oid", wantCode: http.StatusBadGateway},
		{name: "static login wins", image: "reg.azurecr.io/app:1", useMI: true, settings: static,
			wantPull: []string{"reg.azurecr.io/app:1|u|p"}, wantCode: http.StatusOK},
		{name: "flag off", image: "reg.azurecr.io/app:1", wantCode: http.StatusOK},
		{name: "docker hub", image: "busybox", useMI: true, wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			ex := &fakeExchanger{err: tt.exchErr}

			var (
				issued   []string
				issuedMu sync.Mutex
			)

			e.backend.SetRegistryExchanger(ex.exchange)
			e.backend.SetIdentity(
				func(string) (azurefunctions.Identity, bool) { return identity, !tt.noID },
				func(_, _, objectID, resource string) (string, time.Time, error) {
					issuedMu.Lock()
					defer issuedMu.Unlock()

					issued = append(issued, resource)

					return "tok-" + objectID, time.Now().Add(time.Hour), nil
				})

			e.backend.PutApp("w", azurefunctions.AppConfig{
				Kind: "app,linux,container", LinuxFx: "DOCKER|" + tt.image,
				ACRManagedID: tt.useMI, ACRIdentityID: tt.identity,
			})
			require.NoError(t, e.backend.SetSettings("w", tt.settings))

			resp, body := e.do(t, http.MethodGet, "w."+testDomain, "/", nil, nil)
			assert.Equal(t, tt.wantCode, resp.StatusCode, body)
			assert.Equal(t, tt.wantPull, e.rt.pulled())
			assert.NotContains(t, body, "tok-", "tokens never reach the response")

			calls := ex.seen()
			require.Len(t, calls, tt.wantCalls)

			if tt.wantCalls > 0 {
				assert.Equal(t, exchangeCall{"reg.azurecr.io", "tenant-1", "tok-" + tt.wantToken}, calls[0])
				assert.Equal(t, []string{"https://management.azure.com/"}, issued)
			}
		})
	}
}

func TestManagedIdentityPullPersists(t *testing.T) {
	t.Parallel()

	src := newEnv(t, false)
	src.backend.PutApp("w", azurefunctions.AppConfig{
		Kind: "app,linux,container", LinuxFx: "DOCKER|reg.azurecr.io/app:1",
		ACRManagedID: true, ACRIdentityID: "client-u1",
	})

	dst := newEnv(t, true)
	ex := &fakeExchanger{}
	dst.backend.SetRegistryExchanger(ex.exchange)
	dst.backend.SetIdentity(
		func(string) (azurefunctions.Identity, bool) {
			return azurefunctions.Identity{
				TenantID: "t", User: []azurefunctions.UserIdentity{{ClientID: "client-u1", PrincipalID: "oid"}},
			}, true
		},
		func(_, _, objectID, _ string) (string, time.Time, error) { return "tok-" + objectID, time.Now(), nil })
	require.NoError(t, dst.handler.Restore(t.Context(), src.handler.Snapshot(t.Context())))

	resp, _ := dst.do(t, http.MethodGet, "w."+testDomain, "/", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Len(t, ex.seen(), 1, "the flag and the identity id survive a snapshot")
}
