package azurearm_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const kvBase = "/subscriptions/" + azurearm.DefaultSubscriptionID

func newKVHandler() (*azurearm.Handler, *fakeKeyVaults) {
	fake := &fakeKeyVaults{vaults: map[string]int{}}

	return azurearm.NewTestHandlerWithKeyVault(fake, azurearm.DefaultSettings()), fake
}

func putVault(t *testing.T, h *azurearm.Handler, name string, props map[string]any) int {
	t.Helper()

	raw, err := json.Marshal(map[string]any{"location": "westus", "properties": props})
	require.NoError(t, err)

	status, _ := doRequest(t, h, "PUT",
		kvBase+"/resourceGroups/rg/providers/Microsoft.KeyVault/vaults/"+name, raw)

	return status
}

func deleteVault(t *testing.T, h *azurearm.Handler, name string) {
	t.Helper()

	status, _ := doRequest(t, h, "DELETE",
		kvBase+"/resourceGroups/rg/providers/Microsoft.KeyVault/vaults/"+name, nil)
	require.Less(t, status, 300)
}

func TestKeyVault_DeletedVaultLifecycle(t *testing.T) {
	t.Parallel()

	const deletedPath = kvBase + "/providers/Microsoft.KeyVault/locations/westus/deletedVaults/"

	tests := []struct {
		props map[string]any
		steps func(t *testing.T, h *azurearm.Handler, fake *fakeKeyVaults)
		name  string
	}{
		{
			name:  "delete_get_list_recover",
			props: map[string]any{"enableSoftDelete": true},
			steps: func(t *testing.T, h *azurearm.Handler, fake *fakeKeyVaults) {
				t.Helper()
				deleteVault(t, h, "kv1")

				st, body := doRequest(t, h, "GET", deletedPath+"kv1", nil)
				require.Equal(t, 200, st)
				assert.Equal(t, "Microsoft.KeyVault/deletedVaults", body["type"])
				props, _ := body["properties"].(map[string]any)
				assert.Contains(t, props["vaultId"], "/vaults/kv1")
				assert.NotEmpty(t, props["scheduledPurgeDate"])

				st, body = doRequest(t, h, "GET", kvBase+"/providers/Microsoft.KeyVault/deletedVaults", nil)
				require.Equal(t, 200, st)
				assert.Len(t, body["value"], 1)

				st, _ = doRequest(t, h, "GET",
					kvBase+"/resourceGroups/rg/providers/Microsoft.KeyVault/vaults/kv1", nil)
				assert.Equal(t, 404, st)

				assert.Equal(t, 409, putVault(t, h, "kv1", map[string]any{}))
				assert.Less(t, putVault(t, h, "kv1", map[string]any{"createMode": "recover"}), 300)
				assert.Contains(t, fake.calls, "RecoverVault:kv1")

				st, _ = doRequest(t, h, "GET", deletedPath+"kv1", nil)
				assert.Equal(t, 404, st)
			},
		},
		{
			name:  "purge",
			props: map[string]any{},
			steps: func(t *testing.T, h *azurearm.Handler, fake *fakeKeyVaults) {
				t.Helper()
				deleteVault(t, h, "kv1")

				st, _ := doRequest(t, h, "POST", deletedPath+"kv1/purge", nil)
				assert.Equal(t, 202, st)
				assert.Contains(t, fake.calls, "PurgeVault:kv1")

				st, _ = doRequest(t, h, "GET", deletedPath+"kv1", nil)
				assert.Equal(t, 404, st)
				assert.Less(t, putVault(t, h, "kv1", map[string]any{}), 300)
			},
		},
		{
			name:  "purge_protection_rejects",
			props: map[string]any{"enablePurgeProtection": true},
			steps: func(t *testing.T, h *azurearm.Handler, fake *fakeKeyVaults) {
				t.Helper()
				deleteVault(t, h, "kv1")

				st, body := doRequest(t, h, "POST", deletedPath+"kv1/purge", nil)
				assert.Equal(t, 403, st)
				errBody, _ := body["error"].(map[string]any)
				assert.Equal(t, "ActionNotAllowed", errBody["code"])
				assert.NotContains(t, fake.calls, "PurgeVault:kv1")
			},
		},
		{
			name:  "soft_delete_disabled_hard_deletes",
			props: map[string]any{"enableSoftDelete": false},
			steps: func(t *testing.T, h *azurearm.Handler, fake *fakeKeyVaults) {
				t.Helper()
				deleteVault(t, h, "kv1")

				assert.Contains(t, fake.calls, "DeleteVault:kv1")

				st, _ := doRequest(t, h, "GET", deletedPath+"kv1", nil)
				assert.Equal(t, 404, st)
			},
		},
		{
			name:  "recover_without_deleted_is_404",
			props: nil,
			steps: func(t *testing.T, h *azurearm.Handler, _ *fakeKeyVaults) {
				t.Helper()
				assert.Equal(t, 404, putVault(t, h, "ghost", map[string]any{"createMode": "recover"}))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, fake := newKVHandler()
			if tt.props != nil {
				require.Less(t, putVault(t, h, "kv1", tt.props), 300)
			}

			tt.steps(t, h, fake)
		})
	}
}

func TestKeyVault_CheckNameAvailability(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		check     string
		reason    string
		available bool
	}{
		{name: "free", check: "my-vault", available: true},
		{name: "too_short", check: "ab", reason: "AccountNameInvalid"},
		{name: "too_long", check: "abcdefghijklmnopqrstuvwxy", reason: "AccountNameInvalid"},
		{name: "starts_digit", check: "1vault", reason: "AccountNameInvalid"},
		{name: "double_hyphen", check: "my--vault", reason: "AccountNameInvalid"},
		{name: "trailing_hyphen", check: "myvault-", reason: "AccountNameInvalid"},
		{name: "underscore", check: "my_vault", reason: "AccountNameInvalid"},
		{name: "live", check: "Live1", reason: "AlreadyExists"},
		{name: "soft_deleted", check: "gone1", reason: "AlreadyExists"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newKVHandler()
			require.Less(t, putVault(t, h, "live1", map[string]any{}), 300)
			require.Less(t, putVault(t, h, "gone1", map[string]any{}), 300)
			deleteVault(t, h, "gone1")

			raw, err := json.Marshal(map[string]any{"name": tt.check, "type": "Microsoft.KeyVault/vaults"})
			require.NoError(t, err)

			st, body := doRequest(t, h, "POST",
				kvBase+"/providers/Microsoft.KeyVault/checkNameAvailability", raw)
			require.Equal(t, 200, st)
			assert.Equal(t, tt.available, body["nameAvailable"])
			assert.Equal(t, orNil(tt.reason), body["reason"])
		})
	}
}

func orNil(s string) any {
	if s == "" {
		return nil
	}

	return s
}

func TestKeyVault_SnapshotRestoreKeepsVaultsAndDeleted(t *testing.T) {
	t.Parallel()

	h, _ := newKVHandler()
	require.Less(t, putVault(t, h, "live1", map[string]any{"enablePurgeProtection": true}), 300)
	require.Less(t, putVault(t, h, "gone1", map[string]any{}), 300)
	deleteVault(t, h, "gone1")

	data := h.Snapshot(t.Context())

	h2, fake2 := newKVHandler()
	require.NoError(t, h2.Restore(t.Context(), data))
	assert.Empty(t, fake2.calls, "restore must not touch the data plane")

	st, _ := doRequest(t, h2, "GET", kvBase+"/resourceGroups/rg/providers/Microsoft.KeyVault/vaults/live1", nil)
	assert.Equal(t, 200, st)

	st, body := doRequest(t, h2, "GET",
		kvBase+"/providers/Microsoft.KeyVault/locations/westus/deletedVaults/gone1", nil)
	require.Equal(t, 200, st)
	assert.Equal(t, "gone1", body["name"])
}

func TestKeyVaultPurge_LocationPollCompletes(t *testing.T) {
	t.Parallel()

	h, _ := newKVHandler()
	require.Less(t, putVault(t, h, "kv1", map[string]any{}), 300)
	deleteVault(t, h, "kv1")

	req := httptest.NewRequest(
		http.MethodPost,
		kvBase+"/providers/Microsoft.KeyVault/locations/westus/deletedVaults/kv1/purge",
		http.NoBody,
	)
	req.Host = "localhost:18006"
	rec := httptest.NewRecorder()
	newEchoServer(h).ServeHTTP(rec, req)

	require.Equal(t, http.StatusAccepted, rec.Code)

	loc, err := url.Parse(rec.Header().Get("Location"))
	require.NoError(t, err)
	assert.Equal(t, "localhost:18006", loc.Host)
	assert.Contains(t, loc.Path, "/operationResults/")

	st, body := doRequest(t, h, http.MethodGet, loc.Path, nil)
	assert.Equal(t, http.StatusOK, st)
	assert.Equal(t, "Succeeded", body["status"])
}
