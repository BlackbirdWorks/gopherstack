package azurearm_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ListSubscriptionResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		filter string
		want   int
	}{
		{name: "matching type", filter: "resourceType eq 'Microsoft.Storage/storageAccounts'", want: 1},
		{name: "case insensitive", filter: "ResourceType eq 'microsoft.storage/storageaccounts'", want: 1},
		{name: "other type", filter: "resourceType eq 'Microsoft.KeyVault/vaults'", want: 0},
		{name: "no filter", filter: "", want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			base := "/subscriptions/" + h.Settings.SubscriptionID

			_, _ = doRequest(t, h, http.MethodPut, base+"/resourcegroups/rg1", []byte(`{"location":"westus"}`))
			status, _ := doRequest(t, h, http.MethodPut,
				base+"/resourceGroups/rg1/providers/Microsoft.Storage/storageAccounts/acct1",
				[]byte(`{"location":"westus","sku":{"name":"Standard_LRS"}}`))
			require.Equal(t, http.StatusOK, status)

			path := base + "/resources?api-version=2022-12-01"
			if tt.filter != "" {
				path += "&$filter=" + url.QueryEscape(tt.filter)
			}

			status, body := doRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, status)

			values, ok := body["value"].([]any)
			require.True(t, ok)
			assert.Len(t, values, tt.want)
		})
	}
}
