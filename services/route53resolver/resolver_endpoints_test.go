package route53resolver_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

// TestResolverEndpoint_IPv6IPAddress verifies IP addresses supplied to
// CreateResolverEndpoint are stored and retrievable. Per the real SDK's
// types.ResolverEndpoint (verified against aws-sdk-go-v2/service/
// route53resolver/types/types.go), the CreateResolverEndpoint *response* does
// NOT carry an IpAddresses list -- that field does not exist on the type, only
// IpAddressCount does. IPs are only readable via the separate
// ListResolverEndpointIpAddresses call, which this test now uses instead of
// (incorrectly) reading them back off the Create response.
func TestResolverEndpoint_IPv6IPAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		checkFn  func(t *testing.T, ips []any)
		name     string
		wantCode int
	}{
		{
			name: "ipv4_address_stored",
			body: map[string]any{
				"Name":      "ep-ipv4",
				"Direction": "INBOUND",
				"IpAddresses": []map[string]any{
					{"SubnetId": "subnet-1", "Ip": "10.0.0.1"},
				},
			},
			wantCode: http.StatusOK,
			checkFn: func(t *testing.T, ips []any) {
				t.Helper()
				require.NotEmpty(t, ips)
				ip := ips[0].(map[string]any)
				assert.Equal(t, "10.0.0.1", ip["Ip"])
			},
		},
		{
			name: "ipv6_address_stored",
			body: map[string]any{
				"Name":                 "ep-ipv6",
				"Direction":            "INBOUND",
				"ResolverEndpointType": "IPV6",
				"IpAddresses": []map[string]any{
					{"SubnetId": "subnet-1", "Ipv6": "2001:db8::1"},
				},
			},
			wantCode: http.StatusOK,
			checkFn: func(t *testing.T, ips []any) {
				t.Helper()
				require.NotEmpty(t, ips)
				ip := ips[0].(map[string]any)
				assert.Equal(t, "2001:db8::1", ip["Ipv6"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateResolverEndpoint", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK && tt.checkFn != nil {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				ep := resp["ResolverEndpoint"].(map[string]any)
				_, hasIPs := ep["IpAddresses"]
				assert.False(t, hasIPs, "ResolverEndpoint response must not carry an IpAddresses field")
				epID := ep["Id"].(string)

				listRec := doRequest(t, h, "ListResolverEndpointIpAddresses", map[string]any{
					"ResolverEndpointId": epID,
				})
				require.Equal(t, http.StatusOK, listRec.Code)
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
				ips, _ := listResp["IpAddresses"].([]any)
				tt.checkFn(t, ips)
			}
		})
	}
}

// --- Issue 2: Protocols on endpoints ---

func TestResolverEndpoint_Protocols(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body          map[string]any
		name          string
		wantProtocols []any
		wantCode      int
	}{
		{
			name: "default_protocol_do53",
			body: map[string]any{
				"Name":      "ep-default-proto",
				"Direction": "INBOUND",
			},
			wantCode:      http.StatusOK,
			wantProtocols: []any{"Do53"},
		},
		{
			name: "explicit_doh_protocol",
			body: map[string]any{
				"Name":      "ep-doh",
				"Direction": "INBOUND",
				"Protocols": []string{"DoH"},
			},
			wantCode:      http.StatusOK,
			wantProtocols: []any{"DoH"},
		},
		{
			name: "multiple_protocols",
			body: map[string]any{
				"Name":      "ep-multi-proto",
				"Direction": "OUTBOUND",
				"Protocols": []string{"Do53", "DoH"},
			},
			wantCode:      http.StatusOK,
			wantProtocols: []any{"Do53", "DoH"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateResolverEndpoint", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				ep := resp["ResolverEndpoint"].(map[string]any)
				assert.Equal(t, tt.wantProtocols, ep["Protocols"])
			}
		})
	}
}

// --- Issue 3: OutpostArn / PreferredInstanceType on endpoints ---

func TestResolverEndpoint_OutpostFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "with_outpost_arn",
			body: map[string]any{
				"Name":                  "ep-outpost",
				"Direction":             "INBOUND",
				"OutpostArn":            "arn:aws:outposts:us-east-1:000000000000:outpost/op-12345",
				"PreferredInstanceType": "m5.large",
			},
			wantCode: http.StatusOK,
		},
		{
			name: "without_outpost_arn",
			body: map[string]any{
				"Name":      "ep-no-outpost",
				"Direction": "OUTBOUND",
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateResolverEndpoint", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				ep := resp["ResolverEndpoint"].(map[string]any)

				if outpostArn, ok := tt.body["OutpostArn"]; ok {
					assert.Equal(t, outpostArn, ep["OutpostArn"])
					assert.Equal(t, tt.body["PreferredInstanceType"], ep["PreferredInstanceType"])
				}
			}
		})
	}
}

// --- Issue 4: CreatorRequestId + timestamps on endpoints ---

func TestResolverEndpoint_CreatorRequestIdAndTimestamps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":             "ep-with-creator",
		"Direction":        "INBOUND",
		"CreatorRequestId": "req-abc-123",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ep := resp["ResolverEndpoint"].(map[string]any)

	assert.Equal(t, "req-abc-123", ep["CreatorRequestId"])
	assert.NotEmpty(t, ep["CreationTime"])
	assert.NotEmpty(t, ep["ModificationTime"])
}

// --- Issue 8: ResolverEndpointType validation ---

func TestResolverEndpoint_TypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		epType   string
		wantCode int
	}{
		{name: "ipv4_valid", epType: "IPV4", wantCode: http.StatusOK},
		{name: "ipv6_valid", epType: "IPV6", wantCode: http.StatusOK},
		{name: "dualstack_valid", epType: "DUALSTACK", wantCode: http.StatusOK},
		{name: "invalid_type", epType: "INVALID", wantCode: http.StatusBadRequest},
		{name: "empty_defaults_to_ipv4", epType: "", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"Name":      "ep-type-test",
				"Direction": "INBOUND",
			}
			if tt.epType != "" {
				body["ResolverEndpointType"] = tt.epType
			}

			rec := doRequest(t, h, "CreateResolverEndpoint", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				ep := resp["ResolverEndpoint"].(map[string]any)
				if tt.epType == "" {
					assert.Equal(t, "IPV4", ep["ResolverEndpointType"])
				} else {
					assert.Equal(t, tt.epType, ep["ResolverEndpointType"])
				}
			}
		})
	}
}

// --- Issue 6: UpdateResolverEndpoint with Protocols and Type ---

func TestUpdateResolverEndpoint_Extended(t *testing.T) {
	t.Parallel()

	tests := []struct {
		updateBody map[string]any
		checkFn    func(t *testing.T, ep map[string]any)
		name       string
		wantCode   int
	}{
		{
			name:       "update_name",
			updateBody: map[string]any{"Name": "updated-name"},
			wantCode:   http.StatusOK,
			checkFn: func(t *testing.T, ep map[string]any) {
				t.Helper()
				assert.Equal(t, "updated-name", ep["Name"])
			},
		},
		{
			name:       "update_protocols",
			updateBody: map[string]any{"Protocols": []string{"DoH"}},
			wantCode:   http.StatusOK,
			checkFn: func(t *testing.T, ep map[string]any) {
				t.Helper()
				assert.Equal(t, []any{"DoH"}, ep["Protocols"])
			},
		},
		{
			name:       "update_endpoint_type",
			updateBody: map[string]any{"ResolverEndpointType": "IPV6"},
			wantCode:   http.StatusOK,
			checkFn: func(t *testing.T, ep map[string]any) {
				t.Helper()
				assert.Equal(t, "IPV6", ep["ResolverEndpointType"])
			},
		},
		{
			name:       "invalid_type_rejected",
			updateBody: map[string]any{"ResolverEndpointType": "BOGUS"},
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
				"Name":      "orig-ep",
				"Direction": "INBOUND",
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			epID := createResp["ResolverEndpoint"].(map[string]any)["Id"].(string)

			tt.updateBody["ResolverEndpointId"] = epID
			rec := doRequest(t, h, "UpdateResolverEndpoint", tt.updateBody)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK && tt.checkFn != nil {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				ep := resp["ResolverEndpoint"].(map[string]any)
				tt.checkFn(t, ep)
			}
		})
	}
}

// TestUpdateResolverEndpoint_RejectedTypeLeavesNameUnchanged asserts that
// when ResolverEndpointType fails validation, a Name change bundled into
// the same request is not partially applied. UpdateResolverEndpoint
// (resolver_endpoints.go) mutates ep.Name on the live stored pointer
// before it validates ResolverEndpointType, so a request that fails
// validation was still leaving the name change committed.
func TestUpdateResolverEndpoint_RejectedTypeLeavesNameUnchanged(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":      "orig-ep-partial",
		"Direction": "INBOUND",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	epID := createResp["ResolverEndpoint"].(map[string]any)["Id"].(string)

	updateRec := doRequest(t, h, "UpdateResolverEndpoint", map[string]any{
		"ResolverEndpointId":   epID,
		"Name":                 "changed-ep-name",
		"ResolverEndpointType": "BOGUS",
	})
	require.Equal(t, http.StatusBadRequest, updateRec.Code)

	getRec := doRequest(t, h, "GetResolverEndpoint", map[string]any{"ResolverEndpointId": epID})
	require.Equal(t, http.StatusOK, getRec.Code)
	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	ep := getResp["ResolverEndpoint"].(map[string]any)
	assert.Equal(t, "orig-ep-partial", ep["Name"])
}

// TestResolverEndpoint_MetricsFlags verifies RniEnhancedMetricsEnabled and
// TargetNameServerMetricsEnabled -- settable on both CreateResolverEndpoint
// and UpdateResolverEndpoint per the real SDK's CreateResolverEndpointInput /
// UpdateResolverEndpointInput (verified against
// aws-sdk-go-v2/service/route53resolver/api_op_CreateResolverEndpoint.go and
// api_op_UpdateResolverEndpoint.go) -- round-trip through Create, default to
// false when omitted, and partial-update correctly on UpdateResolverEndpoint.
func TestResolverEndpoint_MetricsFlags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Defaults to false when omitted on Create.
	createRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":      "metrics-ep",
		"Direction": "OUTBOUND",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	createResp := decodeJSON(t, createRec)
	ep := createResp["ResolverEndpoint"].(map[string]any)
	assert.Equal(t, false, ep["RniEnhancedMetricsEnabled"])
	assert.Equal(t, false, ep["TargetNameServerMetricsEnabled"])
	epID := ep["Id"].(string)

	// Explicit true on Create.
	createRec2 := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":                           "metrics-ep-2",
		"Direction":                      "OUTBOUND",
		"RniEnhancedMetricsEnabled":      true,
		"TargetNameServerMetricsEnabled": true,
	})
	require.Equal(t, http.StatusOK, createRec2.Code)
	createResp2 := decodeJSON(t, createRec2)
	ep2 := createResp2["ResolverEndpoint"].(map[string]any)
	assert.Equal(t, true, ep2["RniEnhancedMetricsEnabled"])
	assert.Equal(t, true, ep2["TargetNameServerMetricsEnabled"])

	// Partial update: only flip RniEnhancedMetricsEnabled, leave the other alone.
	updRec := doRequest(t, h, "UpdateResolverEndpoint", map[string]any{
		"ResolverEndpointId":        epID,
		"RniEnhancedMetricsEnabled": true,
	})
	require.Equal(t, http.StatusOK, updRec.Code)
	updResp := decodeJSON(t, updRec)
	updEP := updResp["ResolverEndpoint"].(map[string]any)
	assert.Equal(t, true, updEP["RniEnhancedMetricsEnabled"])
	assert.Equal(t, false, updEP["TargetNameServerMetricsEnabled"])

	// Update the other flag too.
	updRec2 := doRequest(t, h, "UpdateResolverEndpoint", map[string]any{
		"ResolverEndpointId":             epID,
		"TargetNameServerMetricsEnabled": true,
	})
	require.Equal(t, http.StatusOK, updRec2.Code)
	updResp2 := decodeJSON(t, updRec2)
	updEP2 := updResp2["ResolverEndpoint"].(map[string]any)
	assert.Equal(t, true, updEP2["RniEnhancedMetricsEnabled"], "prior update must not be clobbered")
	assert.Equal(t, true, updEP2["TargetNameServerMetricsEnabled"])
}

// TestResolverEndpoint_Dns64AndIpv6InternetAccess verifies Dns64Enabled and
// Ipv6InternetAccessEnabled -- settable on both CreateResolverEndpoint and
// UpdateResolverEndpoint (verified against api_op_CreateResolverEndpoint.go
// / api_op_UpdateResolverEndpoint.go) -- round-trip through Create, default
// to false when omitted, and partial-update correctly on
// UpdateResolverEndpoint, mirroring TestResolverEndpoint_MetricsFlags.
func TestResolverEndpoint_Dns64AndIpv6InternetAccess(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":      "dns64-ep",
		"Direction": "INBOUND",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	ep := decodeJSON(t, createRec)["ResolverEndpoint"].(map[string]any)
	assert.Equal(t, false, ep["Dns64Enabled"])
	assert.Equal(t, false, ep["Ipv6InternetAccessEnabled"])
	epID := ep["Id"].(string)

	createRec2 := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":                      "dns64-ep-2",
		"Direction":                 "INBOUND",
		"Dns64Enabled":              true,
		"Ipv6InternetAccessEnabled": true,
	})
	require.Equal(t, http.StatusOK, createRec2.Code)
	ep2 := decodeJSON(t, createRec2)["ResolverEndpoint"].(map[string]any)
	assert.Equal(t, true, ep2["Dns64Enabled"])
	assert.Equal(t, true, ep2["Ipv6InternetAccessEnabled"])

	updRec := doRequest(t, h, "UpdateResolverEndpoint", map[string]any{
		"ResolverEndpointId": epID,
		"Dns64Enabled":       true,
	})
	require.Equal(t, http.StatusOK, updRec.Code)
	updEP := decodeJSON(t, updRec)["ResolverEndpoint"].(map[string]any)
	assert.Equal(t, true, updEP["Dns64Enabled"])
	assert.Equal(t, false, updEP["Ipv6InternetAccessEnabled"])

	updRec2 := doRequest(t, h, "UpdateResolverEndpoint", map[string]any{
		"ResolverEndpointId":        epID,
		"Ipv6InternetAccessEnabled": true,
	})
	require.Equal(t, http.StatusOK, updRec2.Code)
	updEP2 := decodeJSON(t, updRec2)["ResolverEndpoint"].(map[string]any)
	assert.Equal(t, true, updEP2["Dns64Enabled"], "prior update must not be clobbered")
	assert.Equal(t, true, updEP2["Ipv6InternetAccessEnabled"])
}

// TestUpdateResolverEndpoint_UpdateIpAddresses verifies UpdateIpAddresses
// (verified against api_op_UpdateResolverEndpoint.go: "Specifies the IPv6
// address when you update the Resolver endpoint from IPv4 to dual-stack")
// assigns an Ipv6 address to an existing IP identified by IpId, visible via
// a follow-up ListResolverEndpointIpAddresses, and that an unknown IpId is
// rejected rather than silently ignored.
func TestUpdateResolverEndpoint_UpdateIpAddresses(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":      "dualstack-ep",
		"Direction": "INBOUND",
		"IpAddresses": []map[string]any{
			{"SubnetId": "subnet-1", "Ip": "10.0.0.1"},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	epID := decodeJSON(t, createRec)["ResolverEndpoint"].(map[string]any)["Id"].(string)

	ipsRec := doRequest(t, h, "ListResolverEndpointIpAddresses", map[string]any{"ResolverEndpointId": epID})
	require.Equal(t, http.StatusOK, ipsRec.Code)
	ips, _ := decodeJSON(t, ipsRec)["IpAddresses"].([]any)
	require.Len(t, ips, 1)
	ipID := ips[0].(map[string]any)["IpId"].(string)

	updRec := doRequest(t, h, "UpdateResolverEndpoint", map[string]any{
		"ResolverEndpointId":   epID,
		"ResolverEndpointType": "DUALSTACK",
		"UpdateIpAddresses": []map[string]any{
			{"IpId": ipID, "Ipv6": "2001:db8::1"},
		},
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	ipsRec2 := doRequest(t, h, "ListResolverEndpointIpAddresses", map[string]any{"ResolverEndpointId": epID})
	require.Equal(t, http.StatusOK, ipsRec2.Code)
	ips2, _ := decodeJSON(t, ipsRec2)["IpAddresses"].([]any)
	require.Len(t, ips2, 1)
	assert.Equal(t, "2001:db8::1", ips2[0].(map[string]any)["Ipv6"])

	badRec := doRequest(t, h, "UpdateResolverEndpoint", map[string]any{
		"ResolverEndpointId": epID,
		"UpdateIpAddresses": []map[string]any{
			{"IpId": "rni-does-not-exist", "Ipv6": "2001:db8::2"},
		},
	})
	assert.Equal(t, http.StatusNotFound, badRec.Code)
}

// --- Issue 7: AssociateResolverEndpointIpAddress with IPv6 ---

// TestAssociateResolverEndpointIpAddress_IPv6 verifies the associated IP is
// stored and retrievable via ListResolverEndpointIpAddresses. The
// AssociateResolverEndpointIpAddress response wraps a types.ResolverEndpoint,
// same shape as CreateResolverEndpoint's -- it has no IpAddresses field
// either (see TestResolverEndpoint_IPv6IPAddress doc comment).
func TestAssociateResolverEndpointIpAddress_IPv6(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ipBody   map[string]any
		checkFn  func(t *testing.T, ips []any)
		name     string
		wantCode int
	}{
		{
			name: "associate_ipv4",
			ipBody: map[string]any{
				"SubnetId": "subnet-abc",
				"Ip":       "10.0.1.5",
			},
			wantCode: http.StatusOK,
			checkFn: func(t *testing.T, ips []any) {
				t.Helper()
				found := false
				for _, raw := range ips {
					ip := raw.(map[string]any)
					if ip["Ip"] == "10.0.1.5" {
						found = true
					}
				}
				assert.True(t, found)
			},
		},
		{
			name: "associate_ipv6",
			ipBody: map[string]any{
				"SubnetId": "subnet-abc",
				"Ipv6":     "2001:db8::42",
			},
			wantCode: http.StatusOK,
			checkFn: func(t *testing.T, ips []any) {
				t.Helper()
				found := false
				for _, raw := range ips {
					ip := raw.(map[string]any)
					if ip["Ipv6"] == "2001:db8::42" {
						found = true
					}
				}
				assert.True(t, found)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
				"Name":      "ep-for-assoc",
				"Direction": "INBOUND",
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			epID := createResp["ResolverEndpoint"].(map[string]any)["Id"].(string)

			assocRec := doRequest(t, h, "AssociateResolverEndpointIpAddress", map[string]any{
				"ResolverEndpointId": epID,
				"IpAddress":          tt.ipBody,
			})
			assert.Equal(t, tt.wantCode, assocRec.Code)

			if tt.wantCode == http.StatusOK && tt.checkFn != nil {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &resp))
				ep := resp["ResolverEndpoint"].(map[string]any)
				_, hasIPs := ep["IpAddresses"]
				assert.False(t, hasIPs, "ResolverEndpoint response must not carry an IpAddresses field")

				listRec := doRequest(t, h, "ListResolverEndpointIpAddresses", map[string]any{
					"ResolverEndpointId": epID,
				})
				require.Equal(t, http.StatusOK, listRec.Code)
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
				ips, _ := listResp["IpAddresses"].([]any)
				tt.checkFn(t, ips)
			}
		})
	}
}

// --- Issue 9: TargetIP with IPv6 and Protocol ---

func TestBackend_CreateEndpointTypeEnum(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		epType  string
		wantErr bool
	}{
		{name: "ipv4", epType: "IPV4", wantErr: false},
		{name: "ipv6", epType: "IPV6", wantErr: false},
		{name: "dualstack", epType: "DUALSTACK", wantErr: false},
		{name: "empty_defaults_ok", epType: "", wantErr: false},
		{name: "bogus_rejected", epType: "BOGUS", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateResolverEndpoint(
				context.Background(),
				"ep",
				"INBOUND",
				"vpc-1",
				nil,
				nil,
				tt.epType,
				nil,
				"",
				"",
				"",
				false,
				false,
				false,
				false,
			)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// --- Backend direct: CreateResolverRule SYSTEM/RECURSIVE enforcement ---

func TestBackend_EndpointTimestampsRoundTrip(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	ep, err := b.CreateResolverEndpoint(
		context.Background(),
		"ep-ts",
		"INBOUND",
		"vpc-1",
		nil,
		nil,
		"IPV4",
		nil,
		"",
		"",
		"req-ts-1",
		false,
		false,
		false,
		false,
	)
	require.NoError(t, err)
	require.NotEmpty(t, ep.CreationTime)
	require.NotEmpty(t, ep.ModificationTime)

	snap := b.Snapshot(t.Context())
	b2 := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	ep2, err := b2.GetResolverEndpoint(context.Background(), ep.ID)
	require.NoError(t, err)
	assert.Equal(t, ep.CreationTime, ep2.CreationTime)
	assert.Equal(t, ep.ModificationTime, ep2.ModificationTime)
	assert.Equal(t, "req-ts-1", ep2.CreatorRequestID)
}

// --- Firewall rule round trip preserves BlockOverride fields ---

func TestDisassociateResolverEndpointIPAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "success", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create endpoint with one IP.
			rec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
				"Name":        "ep-disassoc",
				"Direction":   "INBOUND",
				"IpAddresses": []map[string]string{{"SubnetId": "subnet-1", "Ip": "10.0.0.1"}},
			})
			require.Equal(t, http.StatusOK, rec.Code)
			createResp := decodeJSON(t, rec)
			epID := createResp["ResolverEndpoint"].(map[string]any)["Id"].(string)

			// Add a second IP via AssociateResolverEndpointIpAddress.
			rec = doRequest(t, h, "AssociateResolverEndpointIpAddress", map[string]any{
				"ResolverEndpointId": epID,
				"IpAddress":          map[string]string{"SubnetId": "subnet-2", "Ip": "10.0.0.2"},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// List IPs to get the IPID of the second one.
			rec = doRequest(t, h, "ListResolverEndpointIpAddresses", map[string]any{
				"ResolverEndpointId": epID,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			listResp := decodeJSON(t, rec)
			ips, _ := listResp["IpAddresses"].([]any)
			require.Len(t, ips, 2)

			// Get the IPID of the second IP (10.0.0.2).
			var ipID string
			for _, ipAny := range ips {
				ip, _ := ipAny.(map[string]any)
				if ip["Ip"] == "10.0.0.2" {
					ipID = ip["IpId"].(string)
				}
			}
			require.NotEmpty(t, ipID)

			// Disassociate.
			rec = doRequest(t, h, "DisassociateResolverEndpointIpAddress", map[string]any{
				"ResolverEndpointId": epID,
				"IpAddress":          map[string]string{"IpId": ipID},
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			// Verify one IP remains.
			rec = doRequest(t, h, "ListResolverEndpointIpAddresses", map[string]any{
				"ResolverEndpointId": epID,
			})
			afterResp := decodeJSON(t, rec)
			afterIPs, _ := afterResp["IpAddresses"].([]any)
			assert.Len(t, afterIPs, 1)
		})
	}
}

func TestDisassociateResolverEndpointIPAddressErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		wantCode int
	}{
		{
			name:     "missing_endpoint_id",
			body:     map[string]any{"IpAddress": map[string]string{"IpId": "ip-123"}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_ip_id",
			body:     map[string]any{"ResolverEndpointId": "rslvr-in-abc"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "endpoint_not_found",
			body: map[string]any{
				"ResolverEndpointId": "rslvr-in-notexist",
				"IpAddress":          map[string]string{"IpId": "ip-abc"},
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "DisassociateResolverEndpointIpAddress", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// --- UpdateResolverRule ---

func TestUpdateResolverEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupEP  func(t *testing.T, h *route53resolver.Handler) string
		body     func(epID string) map[string]any
		name     string
		wantName string
		wantCode int
	}{
		{
			name: "update_name",
			setupEP: func(t *testing.T, h *route53resolver.Handler) string {
				t.Helper()
				rec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
					"Name":      "ep-before-update",
					"Direction": "INBOUND",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := decodeJSON(t, rec)

				return resp["ResolverEndpoint"].(map[string]any)["Id"].(string)
			},
			body: func(epID string) map[string]any {
				return map[string]any{
					"ResolverEndpointId": epID,
					"Name":               "ep-after-update",
				}
			},
			wantCode: http.StatusOK,
			wantName: "ep-after-update",
		},
		{
			name:    "missing_id",
			setupEP: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body: func(_ string) map[string]any {
				return map[string]any{"Name": "new-name"}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:    "not_found",
			setupEP: func(_ *testing.T, _ *route53resolver.Handler) string { return "" },
			body: func(_ string) map[string]any {
				return map[string]any{"ResolverEndpointId": "rslvr-in-notexist", "Name": "x"}
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			epID := tt.setupEP(t, h)
			rec := doRequest(t, h, "UpdateResolverEndpoint", tt.body(epID))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				resp := decodeJSON(t, rec)
				ep, ok := resp["ResolverEndpoint"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, ep["Name"])
			}
		})
	}
}

func TestCreateResolverEndpoint_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "missing_name_returns_bad_request",
			body:     map[string]any{"Direction": "INBOUND"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid_direction_returns_bad_request",
			body:     map[string]any{"Name": "ep", "Direction": "INVALID"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_direction_returns_bad_request",
			body:     map[string]any{"Name": "ep"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "inbound_direction_ok",
			body:     map[string]any{"Name": "ep", "Direction": "INBOUND"},
			wantCode: http.StatusOK,
		},
		{
			name:     "outbound_direction_ok",
			body:     map[string]any{"Name": "ep", "Direction": "OUTBOUND"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateResolverEndpoint", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// --- Endpoint VpcId + SecurityGroupIds in output ---

func TestCreateResolverEndpoint_VpcIdAndSecurityGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":             "ep-with-vpc",
		"Direction":        "INBOUND",
		"VpcId":            "vpc-abc123",
		"SecurityGroupIds": []string{"sg-111", "sg-222"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ep, ok := resp["ResolverEndpoint"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "vpc-abc123", ep["VpcId"])
	sgs, ok := ep["SecurityGroupIds"].([]any)
	require.True(t, ok)
	assert.Len(t, sgs, 2)
}

// --- Resolver rule validation ---

func TestListResolverEndpoints_SortedByName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateResolverEndpoint", map[string]any{"Name": "zebra-ep", "Direction": "INBOUND"})
	doRequest(t, h, "CreateResolverEndpoint", map[string]any{"Name": "alpha-ep", "Direction": "OUTBOUND"})
	doRequest(t, h, "CreateResolverEndpoint", map[string]any{"Name": "mango-ep", "Direction": "INBOUND"})

	rec := doRequest(t, h, "ListResolverEndpoints", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	endpoints, ok := resp["ResolverEndpoints"].([]any)
	require.True(t, ok)
	require.Len(t, endpoints, 3)

	names := make([]string, 3)
	for i, e := range endpoints {
		names[i] = e.(map[string]any)["Name"].(string)
	}
	assert.Equal(t, []string{"alpha-ep", "mango-ep", "zebra-ep"}, names)
}

func TestAddEndpointInternal(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	ep := b.AddEndpointInternal("seeded-ep", "OUTBOUND")
	require.NotNil(t, ep)
	assert.Equal(t, "seeded-ep", ep.Name)
	assert.Contains(t, ep.ARN, "arn:aws:route53resolver:")
	assert.Equal(t, 1, route53resolver.EndpointCount(b))
}

func TestCreateResolverEndpoint_WithTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":      "tagged-ep",
		"Direction": "INBOUND",
		"Tags":      []map[string]string{{"Key": "env", "Value": "prod"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ep := resp["ResolverEndpoint"].(map[string]any)
	epARN := ep["Arn"].(string)

	listRec := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": epARN})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags, ok := listResp["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tags, 1)
}

// --- AssociateResolverQueryLogConfig not-found ---

func TestListResolverEndpointIPAddresses_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListResolverEndpointIpAddresses", map[string]any{
		"ResolverEndpointId": "rslvr-in-nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- AssociateFirewallRuleGroup not-found ---

func TestAssociateResolverEndpointIPAddress_Success(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":      "ip-assoc-ep",
		"Direction": "INBOUND",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	ep := createResp["ResolverEndpoint"].(map[string]any)
	epID := ep["Id"].(string)

	assocRec := doRequest(t, h, "AssociateResolverEndpointIpAddress", map[string]any{
		"ResolverEndpointId": epID,
		"IpAddress": map[string]string{
			"SubnetId": "subnet-new",
			"Ip":       "10.0.1.5",
		},
	})
	require.Equal(t, http.StatusOK, assocRec.Code)

	var assocResp map[string]any
	require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &assocResp))
	updatedEP := assocResp["ResolverEndpoint"].(map[string]any)
	_, hasIPs := updatedEP["IpAddresses"]
	assert.False(t, hasIPs, "ResolverEndpoint response must not carry an IpAddresses field")

	listRec := doRequest(t, h, "ListResolverEndpointIpAddresses", map[string]any{
		"ResolverEndpointId": epID,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	ips, ok := listResp["IpAddresses"].([]any)
	require.True(t, ok)
	assert.Len(t, ips, 1)
}

// --- Pagination: MaxResults + NextToken -----------------------------------------

func TestListResolverEndpoints_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults int
		wantLen    int
		wantToken  bool
	}{
		{"MaxResults=1 limits to 1", 1, 1, true},
		{"MaxResults=2 limits to 2", 2, 2, true},
		{"MaxResults=100 returns all 3", 100, 3, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			for _, name := range []string{"ep-a", "ep-b", "ep-c"} {
				doRequest(t, h, "CreateResolverEndpoint", map[string]any{"Name": name, "Direction": "INBOUND"})
			}

			rec := doRequest(t, h, "ListResolverEndpoints", map[string]any{"MaxResults": tt.maxResults})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			eps, _ := resp["ResolverEndpoints"].([]any)
			assert.Len(t, eps, tt.wantLen)
			nextToken, _ := resp["NextToken"].(string)
			if tt.wantToken {
				assert.NotEmpty(t, nextToken)
			} else {
				assert.Empty(t, nextToken)
			}
		})
	}
}

func TestListResolverEndpoints_NextTokenContinuation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, name := range []string{"ep-x", "ep-y", "ep-z"} {
		doRequest(t, h, "CreateResolverEndpoint", map[string]any{"Name": name, "Direction": "INBOUND"})
	}

	rec1 := doRequest(t, h, "ListResolverEndpoints", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec1.Code)
	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))
	eps1, _ := resp1["ResolverEndpoints"].([]any)
	require.Len(t, eps1, 2)
	nextToken, _ := resp1["NextToken"].(string)
	require.NotEmpty(t, nextToken)

	rec2 := doRequest(t, h, "ListResolverEndpoints", map[string]any{
		"MaxResults": 2,
		"NextToken":  nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	eps2, _ := resp2["ResolverEndpoints"].([]any)
	assert.NotEmpty(t, eps2)

	names1 := make(map[string]bool)
	for _, e := range eps1 {
		em, _ := e.(map[string]any)
		names1[em["Name"].(string)] = true
	}
	for _, e := range eps2 {
		em, _ := e.(map[string]any)
		assert.False(t, names1[em["Name"].(string)], "page 2 should not repeat page 1 items")
	}
}

func TestDeleteResolverEndpoint_CascadeDeletesRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantRulesAfter int
		createRule     bool
	}{
		{
			name:           "cascade_deletes_associated_rule",
			createRule:     true,
			wantRulesAfter: 0,
		},
		{
			name:           "no_rules_to_cascade",
			createRule:     false,
			wantRulesAfter: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create endpoint.
			epRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
				"Name":      "cascade-ep",
				"Direction": "OUTBOUND",
			})
			require.Equal(t, http.StatusOK, epRec.Code)

			var epResp map[string]any
			require.NoError(t, json.Unmarshal(epRec.Body.Bytes(), &epResp))
			ep := epResp["ResolverEndpoint"].(map[string]any)
			epID := ep["Id"].(string)

			if tt.createRule {
				// Create a rule referencing the endpoint.
				ruleRec := doRequest(t, h, "CreateResolverRule", map[string]any{
					"Name":               "test-rule",
					"DomainName":         "example.com.",
					"RuleType":           "FORWARD",
					"ResolverEndpointId": epID,
					"TargetIps":          []map[string]any{{"Ip": "10.0.0.1", "Port": 53}},
				})
				require.Equal(t, http.StatusOK, ruleRec.Code)
			}

			// Delete the endpoint.
			delRec := doRequest(t, h, "DeleteResolverEndpoint", map[string]any{
				"ResolverEndpointId": epID,
			})
			assert.Equal(t, http.StatusOK, delRec.Code)

			// List rules — should be empty.
			listRec := doRequest(t, h, "ListResolverRules", map[string]any{})
			require.Equal(t, http.StatusOK, listRec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
			rules := listResp["ResolverRules"].([]any)
			assert.Len(t, rules, tt.wantRulesAfter)
		})
	}
}

func TestAssociateResolverEndpointIpAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		endpointID  string
		wantCode    int
		wantIPCount int
		useRealEp   bool
	}{
		{
			name:        "success_adds_ip",
			useRealEp:   true,
			wantCode:    http.StatusOK,
			wantIPCount: 2, // existing 1 + new 1
		},
		{
			name:       "missing_endpoint_id",
			useRealEp:  false,
			endpointID: "",
			wantCode:   http.StatusBadRequest,
		},
		{
			name:       "endpoint_not_found",
			useRealEp:  false,
			endpointID: "rslvr-in-notexist",
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			endpointID := tt.endpointID
			if tt.useRealEp {
				rec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
					"Name":        "assoc-ep",
					"Direction":   "INBOUND",
					"IpAddresses": []map[string]string{{"SubnetId": "subnet-1", "Ip": "10.0.0.1"}},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				endpointID = resp["ResolverEndpoint"].(map[string]any)["Id"].(string)
			}

			rec := doRequest(t, h, "AssociateResolverEndpointIpAddress", map[string]any{
				"ResolverEndpointId": endpointID,
				"IpAddress":          map[string]string{"SubnetId": "subnet-2", "Ip": "10.0.0.2"},
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				ep, ok := resp["ResolverEndpoint"].(map[string]any)
				require.True(t, ok)
				_, hasIPs := ep["IpAddresses"]
				assert.False(t, hasIPs, "ResolverEndpoint response must not carry an IpAddresses field")

				listRec := doRequest(t, h, "ListResolverEndpointIpAddresses", map[string]any{
					"ResolverEndpointId": endpointID,
				})
				require.Equal(t, http.StatusOK, listRec.Code)
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
				ips, _ := listResp["IpAddresses"].([]any)
				assert.Len(t, ips, tt.wantIPCount)
			}
		})
	}
}

// --- CreateResolverQueryLogConfig ---

// TestParity_ListResolverEndpointIpAddresses_Pagination verifies NextToken/MaxResults
// on ListResolverEndpointIpAddresses. Real AWS paginates IP addresses per endpoint.
func TestListResolverEndpointIpAddresses_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":        "ep-paginate",
		"Direction":   "INBOUND",
		"IpAddresses": []map[string]string{{"SubnetId": "subnet-1", "Ip": "10.0.0.1"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	epID := createOut["ResolverEndpoint"].(map[string]any)["Id"].(string)

	for i := 2; i <= 4; i++ {
		ipRec := doRequest(t, h, "AssociateResolverEndpointIpAddress", map[string]any{
			"ResolverEndpointId": epID,
			"IpAddress": map[string]string{
				"SubnetId": fmt.Sprintf("subnet-%d", i),
				"Ip":       fmt.Sprintf("10.0.0.%d", i),
			},
		})
		require.Equal(t, http.StatusOK, ipRec.Code)
	}

	tests := []struct {
		body          map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			body:          map[string]any{"ResolverEndpointId": epID},
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			body:          map[string]any{"ResolverEndpointId": epID, "MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, "ListResolverEndpointIpAddresses", tt.body)
			require.Equal(t, http.StatusOK, listRec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			ips, _ := out["IpAddresses"].([]any)
			assert.Len(t, ips, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

func TestCreateResolverEndpoint(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":      "my-endpoint",
		"Direction": "INBOUND",
		"IpAddresses": []map[string]string{
			{"SubnetId": "subnet-abc", "Ip": "10.0.0.1"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ep, ok := resp["ResolverEndpoint"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, ep["Arn"], "arn:aws:route53resolver:")
	assert.Equal(t, "my-endpoint", ep["Name"])
	assert.Equal(t, "INBOUND", ep["Direction"])
	assert.Equal(t, "OPERATIONAL", ep["Status"])
}

func TestGetResolverEndpoint(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":      "ep1",
		"Direction": "OUTBOUND",
	})
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	ep := createResp["ResolverEndpoint"].(map[string]any)
	id := ep["Id"].(string)

	rec := doRequest(t, h, "GetResolverEndpoint", map[string]any{
		"ResolverEndpointId": id,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	got := resp["ResolverEndpoint"].(map[string]any)
	assert.Equal(t, id, got["Id"])
}

func TestListResolverEndpoints(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateResolverEndpoint", map[string]any{"Name": "ep1", "Direction": "INBOUND"})
	doRequest(
		t,
		h,
		"CreateResolverEndpoint",
		map[string]any{"Name": "ep2", "Direction": "OUTBOUND"},
	)

	rec := doRequest(t, h, "ListResolverEndpoints", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	endpoints, ok := resp["ResolverEndpoints"].([]any)
	require.True(t, ok)
	assert.Len(t, endpoints, 2)
}

func TestListResolverEndpoints_Filters(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) *route53resolver.Handler {
		t.Helper()
		h := newTestHandler(t)
		doRequest(t, h, "CreateResolverEndpoint", map[string]any{
			"Name": "in-ep", "Direction": "INBOUND", "VpcId": "vpc-1",
			"CreatorRequestId": "req-in", "SecurityGroupIds": []string{"sg-1", "sg-2"},
		})
		doRequest(t, h, "CreateResolverEndpoint", map[string]any{
			"Name": "out-ep", "Direction": "OUTBOUND", "VpcId": "vpc-2",
			"CreatorRequestId": "req-out", "SecurityGroupIds": []string{"sg-3"},
		})

		return h
	}

	tests := []struct {
		filter    map[string]any
		name      string
		wantNames []string
	}{
		{
			name:      "direction canonical name",
			filter:    map[string]any{"Name": "Direction", "Values": []string{"INBOUND"}},
			wantNames: []string{"in-ep"},
		},
		{
			name:      "direction legacy uppercase name",
			filter:    map[string]any{"Name": "DIRECTION", "Values": []string{"OUTBOUND"}},
			wantNames: []string{"out-ep"},
		},
		{
			name:      "host vpc id",
			filter:    map[string]any{"Name": "HostVPCId", "Values": []string{"vpc-2"}},
			wantNames: []string{"out-ep"},
		},
		{
			name:      "creator request id",
			filter:    map[string]any{"Name": "CreatorRequestId", "Values": []string{"req-in"}},
			wantNames: []string{"in-ep"},
		},
		{
			name:      "security group ids matches any member",
			filter:    map[string]any{"Name": "SecurityGroupIds", "Values": []string{"sg-3"}},
			wantNames: []string{"out-ep"},
		},
		{
			name:      "values are OR-combined",
			filter:    map[string]any{"Name": "Direction", "Values": []string{"INBOUND", "OUTBOUND"}},
			wantNames: []string{"in-ep", "out-ep"},
		},
		{
			name:      "empty values matches nothing",
			filter:    map[string]any{"Name": "Direction", "Values": []string{}},
			wantNames: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := setup(t)

			rec := doRequest(t, h, "ListResolverEndpoints", map[string]any{
				"Filters": []map[string]any{tt.filter},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			eps, _ := resp["ResolverEndpoints"].([]any)
			gotNames := make([]string, len(eps))
			for i, e := range eps {
				gotNames[i] = e.(map[string]any)["Name"].(string)
			}
			assert.ElementsMatch(t, tt.wantNames, gotNames)
		})
	}
}

func TestListResolverEndpoints_FiltersANDedTogether(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name": "in-vpc1", "Direction": "INBOUND", "VpcId": "vpc-1",
	})
	doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name": "in-vpc2", "Direction": "INBOUND", "VpcId": "vpc-2",
	})

	rec := doRequest(t, h, "ListResolverEndpoints", map[string]any{
		"Filters": []map[string]any{
			{"Name": "Direction", "Values": []string{"INBOUND"}},
			{"Name": "HostVPCId", "Values": []string{"vpc-2"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	eps, _ := resp["ResolverEndpoints"].([]any)
	require.Len(t, eps, 1)
	assert.Equal(t, "in-vpc2", eps[0].(map[string]any)["Name"])
}

func TestListResolverEndpoints_UnknownFilterNameRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateResolverEndpoint", map[string]any{"Name": "ep1", "Direction": "INBOUND"})

	rec := doRequest(t, h, "ListResolverEndpoints", map[string]any{
		"Filters": []map[string]any{
			{"Name": "NotARealFilter", "Values": []string{"x"}},
		},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InvalidParameterException", resp["__type"])
}

func TestDeleteResolverEndpoint(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":      "ep-to-delete",
		"Direction": "INBOUND",
	})
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	ep := createResp["ResolverEndpoint"].(map[string]any)
	id := ep["Id"].(string)

	rec := doRequest(t, h, "DeleteResolverEndpoint", map[string]any{
		"ResolverEndpointId": id,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify deleted
	getRec := doRequest(t, h, "GetResolverEndpoint", map[string]any{"ResolverEndpointId": id})
	assert.Equal(t, http.StatusNotFound, getRec.Code)
}
