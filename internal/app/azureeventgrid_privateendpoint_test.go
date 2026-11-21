package app

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func (s *egDomainStack) arm(t *testing.T, method, path, body string) (int, map[string]any) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, s.armSrv.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.armSrv.Client().Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	var out map[string]any

	_ = json.UnmarshalDecode(jsontext.NewDecoder(resp.Body), &out, jsonx.V1())

	return resp.StatusCode, out
}

func TestAzureEventGridPrivateEndpoint(t *testing.T) {
	t.Parallel()

	const (
		sub = redisSDKSub
		rg  = "pe-eg-rg"
		eg  = "/subscriptions/" + sub + "/resourceGroups/" + rg + "/providers/Microsoft.EventGrid/"
	)

	tests := []struct {
		name     string
		typ      string
		body     string
		group    string
		wantFQDN string
		wantCode int
	}{
		{name: "topic", typ: "topics", body: `{"location":"westus"}`, group: "topic",
			wantFQDN: "res1.westus-1.eventgrid.azure.net", wantCode: http.StatusCreated},
		{name: "domain", typ: "domains", body: `{"location":"westus"}`, group: "domain",
			wantFQDN: "res1.westus-1.eventgrid.azure.net", wantCode: http.StatusCreated},
		{name: "namespace topic space", typ: "namespaces",
			body: `{"location":"westus","sku":{"name":"Standard","capacity":1}}`, group: "topicSpace",
			wantFQDN: "res1.westus-1.ts.eventgrid.azure.net", wantCode: http.StatusCreated},
		{name: "wrong group", typ: "topics", body: `{"location":"westus"}`, group: "domain",
			wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newEGDomainStack(t)

			code, body := s.arm(
				t,
				http.MethodPut,
				"/subscriptions/"+sub+"/resourcegroups/"+rg+"?api-version=2021-04-01",
				`{"location":"westus"}`,
			)
			require.Less(t, code, 300, "%v", body)

			code, body = s.arm(t, http.MethodPut, "/subscriptions/"+sub+"/resourceGroups/"+rg+
				"/providers/Microsoft.Network/virtualNetworks/vnet?api-version=2024-05-01",
				`{"location":"westus","properties":{"addressSpace":{"addressPrefixes":["10.0.0.0/16"]},`+
					`"subnets":[{"name":"pe","properties":{"addressPrefix":"10.0.1.0/24"}}]}}`)
			require.Less(t, code, 300, "%v", body)

			target := eg + tt.typ + "/res1"
			code, body = s.arm(t, http.MethodPut, target+"?api-version=2025-02-15", tt.body)
			require.Less(t, code, 300, "%v", body)

			code, body = s.arm(t, http.MethodPut, "/subscriptions/"+sub+"/resourceGroups/"+rg+
				"/providers/Microsoft.Network/privateEndpoints/pe?api-version=2024-05-01",
				`{"location":"westus","properties":{"subnet":{"id":"/subscriptions/`+sub+`/resourceGroups/`+rg+
					`/providers/Microsoft.Network/virtualNetworks/vnet/subnets/pe"},`+
					`"privateLinkServiceConnections":[{"name":"c","properties":{"privateLinkServiceId":"`+target+
					`","groupIds":["`+tt.group+`"]}}]}}`)
			require.Equal(t, tt.wantCode, code, "%v", body)

			if tt.wantCode != http.StatusCreated {
				return
			}

			dns := body["properties"].(map[string]any)["customDnsConfigs"].([]any)
			require.Len(t, dns, 1)
			assert.Equal(t, tt.wantFQDN, dns[0].(map[string]any)["fqdn"])

			code, body = s.arm(t, http.MethodGet, target+"/privateEndpointConnections?api-version=2025-02-15", "")
			require.Equal(t, http.StatusOK, code)
			assert.Len(t, body["value"], 1)
		})
	}
}
