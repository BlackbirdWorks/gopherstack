package route53resolver_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

// --- Issue 1: IPv6 on IP addresses ---

func TestAudit_ResolverEndpoint_IPv6IPAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		checkFn  func(t *testing.T, ep map[string]any)
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
			checkFn: func(t *testing.T, ep map[string]any) {
				t.Helper()
				ips, _ := ep["IpAddresses"].([]any)
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
			checkFn: func(t *testing.T, ep map[string]any) {
				t.Helper()
				ips, _ := ep["IpAddresses"].([]any)
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
				tt.checkFn(t, ep)
			}
		})
	}
}

// --- Issue 2: Protocols on endpoints ---

func TestAudit_ResolverEndpoint_Protocols(t *testing.T) {
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

func TestAudit_ResolverEndpoint_OutpostFields(t *testing.T) {
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

func TestAudit_ResolverEndpoint_CreatorRequestIdAndTimestamps(t *testing.T) {
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

func TestAudit_ResolverEndpoint_TypeValidation(t *testing.T) {
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

func TestAudit_UpdateResolverEndpoint_Extended(t *testing.T) {
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

// --- Issue 7: AssociateResolverEndpointIpAddress with IPv6 ---

func TestAudit_AssociateResolverEndpointIpAddress_IPv6(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ipBody   map[string]any
		checkFn  func(t *testing.T, ep map[string]any)
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
			checkFn: func(t *testing.T, ep map[string]any) {
				t.Helper()
				ips := ep["IpAddresses"].([]any)
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
			checkFn: func(t *testing.T, ep map[string]any) {
				t.Helper()
				ips := ep["IpAddresses"].([]any)
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
				tt.checkFn(t, ep)
			}
		})
	}
}

// --- Issue 9: TargetIP with IPv6 and Protocol ---

func TestAudit_ResolverRule_TargetIpWithIPv6AndProtocol(t *testing.T) {
	t.Parallel()

	tests := []struct {
		targetIP map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "ipv4_target",
			targetIP: map[string]any{"Ip": "10.0.0.1", "Port": 53},
			wantCode: http.StatusOK,
		},
		{
			name:     "ipv6_target",
			targetIP: map[string]any{"Ipv6": "2001:db8::1", "Port": 853},
			wantCode: http.StatusOK,
		},
		{
			name:     "target_with_protocol_doh",
			targetIP: map[string]any{"Ip": "10.0.0.1", "Port": 443, "Protocol": "DoH"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateResolverRule", map[string]any{
				"Name":       "rule-target-test",
				"DomainName": "example.com",
				"RuleType":   "FORWARD",
				"TargetIps":  []map[string]any{tt.targetIP},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// --- Issue 10: ResolverRule CreatorRequestId, OwnerID, timestamps ---

func TestAudit_ResolverRule_CreatorAndTimestamps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateResolverRule", map[string]any{
		"Name":             "rule-with-creator",
		"DomainName":       "example.com",
		"RuleType":         "FORWARD",
		"CreatorRequestId": "req-rule-001",
		"TargetIps":        []map[string]any{{"Ip": "10.0.0.1", "Port": 53}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	rule := resp["ResolverRule"].(map[string]any)

	assert.Equal(t, "req-rule-001", rule["CreatorRequestId"])
	assert.NotEmpty(t, rule["OwnerID"])
	assert.NotEmpty(t, rule["CreationTime"])
	assert.NotEmpty(t, rule["ModificationTime"])
}

// --- Issue 12: SYSTEM/RECURSIVE rule enforcement ---

func TestAudit_ResolverRule_TypeEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "system_no_endpoint_no_targets_ok",
			body: map[string]any{
				"Name":       "sys-rule",
				"DomainName": "sys.example.com",
				"RuleType":   "SYSTEM",
			},
			wantCode: http.StatusOK,
		},
		{
			name: "recursive_no_endpoint_no_targets_ok",
			body: map[string]any{
				"Name":       "rec-rule",
				"DomainName": "rec.example.com",
				"RuleType":   "RECURSIVE",
			},
			wantCode: http.StatusOK,
		},
		{
			name: "system_with_endpoint_rejected",
			body: map[string]any{
				"Name":               "sys-rule-ep",
				"DomainName":         "sys.example.com",
				"RuleType":           "SYSTEM",
				"ResolverEndpointId": "rslvr-in-12345678",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "system_with_target_ips_rejected",
			body: map[string]any{
				"Name":       "sys-rule-tips",
				"DomainName": "sys.example.com",
				"RuleType":   "SYSTEM",
				"TargetIps":  []map[string]any{{"Ip": "10.0.0.1", "Port": 53}},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "recursive_with_target_ips_rejected",
			body: map[string]any{
				"Name":       "rec-rule-tips",
				"DomainName": "rec.example.com",
				"RuleType":   "RECURSIVE",
				"TargetIps":  []map[string]any{{"Ip": "10.0.0.1", "Port": 53}},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "forward_with_targets_ok",
			body: map[string]any{
				"Name":       "fwd-rule",
				"DomainName": "fwd.example.com",
				"RuleType":   "FORWARD",
				"TargetIps":  []map[string]any{{"Ip": "10.0.0.1", "Port": 53}},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateResolverRule", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// --- Issue 14: ResolverQueryLogConfig AssociationCount, ShareStatus ---

func TestAudit_ResolverQueryLogConfig_AssociationCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
		"Name":           "qlc-assoc-count",
		"DestinationArn": "arn:aws:s3:::my-query-logs",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	cfg := createResp["ResolverQueryLogConfig"].(map[string]any)
	cfgID := cfg["Id"].(string)
	assert.EqualValues(t, 0, cfg["AssociationCount"])
	assert.Equal(t, "NOT_SHARED", cfg["ShareStatus"])
	assert.NotEmpty(t, cfg["CreationTime"])

	// Associate once.
	doRequest(t, h, "AssociateResolverQueryLogConfig", map[string]any{
		"ResolverQueryLogConfigId": cfgID,
		"ResourceId":               "vpc-aaa",
	})

	// Associate again.
	assocRec := doRequest(t, h, "AssociateResolverQueryLogConfig", map[string]any{
		"ResolverQueryLogConfigId": cfgID,
		"ResourceId":               "vpc-bbb",
	})
	require.Equal(t, http.StatusOK, assocRec.Code)

	// Get config and verify count.
	getRec := doRequest(
		t,
		h,
		"GetResolverQueryLogConfig",
		map[string]any{"ResolverQueryLogConfigId": cfgID},
	)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	cfg = getResp["ResolverQueryLogConfig"].(map[string]any)
	assert.EqualValues(t, 2, cfg["AssociationCount"])
}

// --- Issue 15: DestinationArn validation ---

func TestAudit_ResolverQueryLogConfig_DestinationArnValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		destinationArn string
		wantCode       int
	}{
		{
			name:           "s3_arn_valid",
			destinationArn: "arn:aws:s3:::my-bucket",
			wantCode:       http.StatusOK,
		},
		{
			name:           "cloudwatch_logs_arn_valid",
			destinationArn: "arn:aws:logs:us-east-1:000000000000:log-group:/aws/route53/query",
			wantCode:       http.StatusOK,
		},
		{
			name:           "firehose_arn_valid",
			destinationArn: "arn:aws:firehose:us-east-1:000000000000:deliverystream/my-stream",
			wantCode:       http.StatusOK,
		},
		{
			name:           "invalid_arn_rejected",
			destinationArn: "arn:aws:ec2:us-east-1:000000000000:instance/i-12345",
			wantCode:       http.StatusBadRequest,
		},
		{
			name:           "empty_arn_rejected",
			destinationArn: "",
			wantCode:       http.StatusBadRequest,
		},
		{
			name:           "plain_string_rejected",
			destinationArn: "not-an-arn",
			wantCode:       http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"Name": "qlc-dest-test",
			}
			if tt.destinationArn != "" {
				body["DestinationArn"] = tt.destinationArn
			}
			rec := doRequest(t, h, "CreateResolverQueryLogConfig", body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// --- Issue 17: ResolverDnssecConfig ValidationStatus ---

func TestAudit_ResolverDnssecConfig_StatusValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		action     string
		validation string
		wantStatus string
		wantCode   int
	}{
		{
			name:       "get_default_disabled",
			action:     "get",
			wantCode:   http.StatusOK,
			wantStatus: "DISABLED",
		},
		{
			name:       "enable_transitions_to_enabling",
			action:     "enable",
			validation: "ENABLE",
			wantCode:   http.StatusOK,
			wantStatus: "ENABLING",
		},
		{
			name:       "disable_transitions_to_disabling",
			action:     "disable",
			validation: "DISABLE",
			wantCode:   http.StatusOK,
			wantStatus: "DISABLING",
		},
		{
			name:       "invalid_validation_rejected",
			action:     "invalid",
			validation: "UNKNOWN",
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vpcID := "vpc-dnssec-test"

			var rec *httptest.ResponseRecorder
			switch tt.action {
			case "get":
				rec = doRequest(
					t,
					h,
					"GetResolverDnssecConfig",
					map[string]any{"ResourceId": vpcID},
				)
			case "enable", "disable", "invalid":
				rec = doRequest(t, h, "UpdateResolverDnssecConfig", map[string]any{
					"ResourceId": vpcID,
					"Validation": tt.validation,
				})
			}

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				cfg := resp["ResolverDNSSECConfig"].(map[string]any)
				assert.Equal(t, tt.wantStatus, cfg["ValidationStatus"])
			}
		})
	}
}

// --- Issue 18: FirewallConfig no ARN ---

func TestAudit_FirewallConfig_NoArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetFirewallConfig", map[string]any{"ResourceId": "vpc-fwc-test"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	cfg := resp["FirewallConfig"].(map[string]any)

	// AWS does not return Arn for FirewallConfig.
	_, hasArn := cfg["Arn"]
	assert.False(t, hasArn, "FirewallConfig should not have an Arn field")
	assert.NotEmpty(t, cfg["Id"])
	assert.NotEmpty(t, cfg["OwnerID"])
	assert.NotEmpty(t, cfg["ResourceId"])
}

// --- Issue 19: FirewallDomainList ManagedOwnerName ---

func TestAudit_FirewallDomainList_ManagedOwnerName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateFirewallDomainList", map[string]any{
		"Name":             "my-domain-list",
		"CreatorRequestId": "req-dl-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	dl := resp["FirewallDomainList"].(map[string]any)

	// ManagedOwnerName should be empty for user-created domain lists.
	assert.Empty(t, dl["ManagedOwnerName"])
}

// --- Issue 20 + 21: FirewallRule BlockOverride* fields ---

func TestAudit_FirewallRule_BlockOverrideFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     func(groupID, dlID string) map[string]any
		checkFn  func(t *testing.T, rule map[string]any)
		name     string
		wantCode int
	}{
		{
			name: "block_nodata",
			body: func(groupID, dlID string) map[string]any {
				return map[string]any{
					"FirewallRuleGroupId":  groupID,
					"FirewallDomainListId": dlID,
					"Priority":             100,
					"Action":               "BLOCK",
					"BlockResponse":        "NODATA",
					"Name":                 "block-nodata",
				}
			},
			wantCode: http.StatusOK,
			checkFn: func(t *testing.T, rule map[string]any) {
				t.Helper()
				assert.Equal(t, "BLOCK", rule["Action"])
				assert.Equal(t, "NODATA", rule["BlockResponse"])
			},
		},
		{
			name: "block_override_with_domain",
			body: func(groupID, dlID string) map[string]any {
				return map[string]any{
					"FirewallRuleGroupId":  groupID,
					"FirewallDomainListId": dlID,
					"Priority":             200,
					"Action":               "BLOCK",
					"BlockResponse":        "OVERRIDE",
					"BlockOverrideDomain":  "safe.example.com",
					"BlockOverrideDNSType": "CNAME",
					"BlockOverrideTTL":     300,
					"Name":                 "block-override",
				}
			},
			wantCode: http.StatusOK,
			checkFn: func(t *testing.T, rule map[string]any) {
				t.Helper()
				assert.Equal(t, "OVERRIDE", rule["BlockResponse"])
				assert.Equal(t, "safe.example.com", rule["BlockOverrideDomain"])
				assert.Equal(t, "CNAME", rule["BlockOverrideDNSType"])
				assert.EqualValues(t, 300, rule["BlockOverrideTTL"])
			},
		},
		{
			name: "block_override_missing_domain_rejected",
			body: func(groupID, dlID string) map[string]any {
				return map[string]any{
					"FirewallRuleGroupId":  groupID,
					"FirewallDomainListId": dlID,
					"Priority":             300,
					"Action":               "BLOCK",
					"BlockResponse":        "OVERRIDE",
					"Name":                 "block-override-missing",
				}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "allow_action",
			body: func(groupID, dlID string) map[string]any {
				return map[string]any{
					"FirewallRuleGroupId":  groupID,
					"FirewallDomainListId": dlID,
					"Priority":             400,
					"Action":               "ALLOW",
					"Name":                 "allow-rule",
				}
			},
			wantCode: http.StatusOK,
			checkFn: func(t *testing.T, rule map[string]any) {
				t.Helper()
				assert.Equal(t, "ALLOW", rule["Action"])
			},
		},
		{
			name: "with_qtype",
			body: func(groupID, dlID string) map[string]any {
				return map[string]any{
					"FirewallRuleGroupId":  groupID,
					"FirewallDomainListId": dlID,
					"Priority":             500,
					"Action":               "BLOCK",
					"BlockResponse":        "NODATA",
					"Qtype":                "A",
					"Name":                 "qtype-rule",
				}
			},
			wantCode: http.StatusOK,
			checkFn: func(t *testing.T, rule map[string]any) {
				t.Helper()
				assert.Equal(t, "A", rule["Qtype"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			grpRec := doRequest(
				t,
				h,
				"CreateFirewallRuleGroup",
				map[string]any{"Name": "override-test-grp"},
			)
			require.Equal(t, http.StatusOK, grpRec.Code)
			var grpResp map[string]any
			require.NoError(t, json.Unmarshal(grpRec.Body.Bytes(), &grpResp))
			groupID := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

			dlRec := doRequest(
				t,
				h,
				"CreateFirewallDomainList",
				map[string]any{"Name": "override-test-dl"},
			)
			require.Equal(t, http.StatusOK, dlRec.Code)
			var dlResp map[string]any
			require.NoError(t, json.Unmarshal(dlRec.Body.Bytes(), &dlResp))
			dlID := dlResp["FirewallDomainList"].(map[string]any)["Id"].(string)

			rec := doRequest(t, h, "CreateFirewallRule", tt.body(groupID, dlID))
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK && tt.checkFn != nil {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				rule := resp["FirewallRule"].(map[string]any)
				tt.checkFn(t, rule)
			}
		})
	}
}

// --- Issue 22: UpdateFirewallRule with extended fields ---

func TestAudit_UpdateFirewallRule_ExtendedFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	grpRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "update-rule-grp"})
	require.Equal(t, http.StatusOK, grpRec.Code)
	var grpResp map[string]any
	require.NoError(t, json.Unmarshal(grpRec.Body.Bytes(), &grpResp))
	groupID := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

	dlRec := doRequest(t, h, "CreateFirewallDomainList", map[string]any{"Name": "update-rule-dl"})
	require.Equal(t, http.StatusOK, dlRec.Code)
	var dlResp map[string]any
	require.NoError(t, json.Unmarshal(dlRec.Body.Bytes(), &dlResp))
	dlID := dlResp["FirewallDomainList"].(map[string]any)["Id"].(string)

	createRec := doRequest(t, h, "CreateFirewallRule", map[string]any{
		"FirewallRuleGroupId":  groupID,
		"FirewallDomainListId": dlID,
		"Priority":             100,
		"Action":               "ALLOW",
		"Name":                 "rule-to-update",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	ruleID := createResp["FirewallRule"].(map[string]any)["Id"].(string)

	tests := []struct {
		update  map[string]any
		checkFn func(t *testing.T, rule map[string]any)
		name    string
	}{
		{
			name: "update_action_and_block_response",
			update: map[string]any{
				"Action":        "BLOCK",
				"BlockResponse": "NXDOMAIN",
			},
			checkFn: func(t *testing.T, rule map[string]any) {
				t.Helper()
				assert.Equal(t, "BLOCK", rule["Action"])
				assert.Equal(t, "NXDOMAIN", rule["BlockResponse"])
			},
		},
		{
			name: "update_qtype",
			update: map[string]any{
				"Qtype": "AAAA",
			},
			checkFn: func(t *testing.T, rule map[string]any) {
				t.Helper()
				assert.Equal(t, "AAAA", rule["Qtype"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h2 := newTestHandler(t)
			// Re-setup: need same group/dl/rule in fresh handler.
			grpRec2 := doRequest(
				t,
				h2,
				"CreateFirewallRuleGroup",
				map[string]any{"Name": "upd-grp"},
			)
			require.Equal(t, http.StatusOK, grpRec2.Code)
			var grpR2 map[string]any
			require.NoError(t, json.Unmarshal(grpRec2.Body.Bytes(), &grpR2))
			gID := grpR2["FirewallRuleGroup"].(map[string]any)["Id"].(string)

			dlRec2 := doRequest(t, h2, "CreateFirewallDomainList", map[string]any{"Name": "upd-dl"})
			require.Equal(t, http.StatusOK, dlRec2.Code)
			var dlR2 map[string]any
			require.NoError(t, json.Unmarshal(dlRec2.Body.Bytes(), &dlR2))
			dID := dlR2["FirewallDomainList"].(map[string]any)["Id"].(string)

			createRec2 := doRequest(t, h2, "CreateFirewallRule", map[string]any{
				"FirewallRuleGroupId":  gID,
				"FirewallDomainListId": dID,
				"Priority":             100,
				"Action":               "ALLOW",
				"Name":                 "rule-to-upd",
			})
			require.Equal(t, http.StatusOK, createRec2.Code)
			var createR2 map[string]any
			require.NoError(t, json.Unmarshal(createRec2.Body.Bytes(), &createR2))
			rID := createR2["FirewallRule"].(map[string]any)["Id"].(string)

			upd := tt.update
			upd["FirewallRuleId"] = rID
			rec := doRequest(t, h2, "UpdateFirewallRule", upd)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			rule := resp["FirewallRule"].(map[string]any)
			tt.checkFn(t, rule)
		})
	}

	_ = ruleID // used in setup only
}

// --- Issue 23: FirewallRuleGroup ShareStatus + timestamps ---

func TestAudit_FirewallRuleGroup_ShareStatusAndTimestamps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{
		"Name":             "rg-share-test",
		"CreatorRequestId": "req-rg-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	rg := resp["FirewallRuleGroup"].(map[string]any)

	assert.Equal(t, "NOT_SHARED", rg["ShareStatus"])
	assert.NotEmpty(t, rg["CreationTime"])
	assert.NotEmpty(t, rg["ModificationTime"])
}

// --- Issue 24: FirewallRuleGroupAssociation MutationProtection ---

func TestAudit_FirewallRuleGroupAssociation_MutationProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		mutationProtection string
		tryDisassociate    bool
		wantAssocCode      int
		wantDisassocCode   int
	}{
		{
			name:               "disabled_allows_disassociate",
			mutationProtection: "DISABLED",
			tryDisassociate:    true,
			wantAssocCode:      http.StatusOK,
			wantDisassocCode:   http.StatusOK,
		},
		{
			name:               "enabled_blocks_disassociate",
			mutationProtection: "ENABLED",
			tryDisassociate:    true,
			wantAssocCode:      http.StatusOK,
			wantDisassocCode:   http.StatusBadRequest,
		},
		{
			name:               "default_disabled",
			mutationProtection: "",
			tryDisassociate:    false,
			wantAssocCode:      http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			grpRec := doRequest(
				t,
				h,
				"CreateFirewallRuleGroup",
				map[string]any{"Name": "mut-prot-grp"},
			)
			require.Equal(t, http.StatusOK, grpRec.Code)
			var grpResp map[string]any
			require.NoError(t, json.Unmarshal(grpRec.Body.Bytes(), &grpResp))
			groupID := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

			assocBody := map[string]any{
				"FirewallRuleGroupId": groupID,
				"VpcId":               "vpc-mp-test",
				"Priority":            100,
				"Name":                "mp-assoc",
			}
			if tt.mutationProtection != "" {
				assocBody["MutationProtection"] = tt.mutationProtection
			}

			assocRec := doRequest(t, h, "AssociateFirewallRuleGroup", assocBody)
			assert.Equal(t, tt.wantAssocCode, assocRec.Code)

			if tt.wantAssocCode == http.StatusOK && tt.tryDisassociate {
				var assocResp map[string]any
				require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &assocResp))
				assocID := assocResp["FirewallRuleGroupAssociation"].(map[string]any)["Id"].(string)

				disassocRec := doRequest(t, h, "DisassociateFirewallRuleGroup", map[string]any{
					"FirewallRuleGroupAssociationId": assocID,
				})
				assert.Equal(t, tt.wantDisassocCode, disassocRec.Code)
			}
		})
	}
}

// --- MutationProtection update ---

func TestAudit_UpdateFirewallRuleGroupAssociation_MutationProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		mutationProtection string
		wantCode           int
	}{
		{name: "set_enabled", mutationProtection: "ENABLED", wantCode: http.StatusOK},
		{name: "set_disabled", mutationProtection: "DISABLED", wantCode: http.StatusOK},
		{name: "invalid_value", mutationProtection: "MAYBE", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			grpRec := doRequest(
				t,
				h,
				"CreateFirewallRuleGroup",
				map[string]any{"Name": "upd-mp-grp"},
			)
			require.Equal(t, http.StatusOK, grpRec.Code)
			var grpResp map[string]any
			require.NoError(t, json.Unmarshal(grpRec.Body.Bytes(), &grpResp))
			groupID := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

			assocRec := doRequest(t, h, "AssociateFirewallRuleGroup", map[string]any{
				"FirewallRuleGroupId": groupID,
				"VpcId":               "vpc-upd-mp",
				"Priority":            100,
				"Name":                "upd-mp-assoc",
			})
			require.Equal(t, http.StatusOK, assocRec.Code)
			var assocResp map[string]any
			require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &assocResp))
			assocID := assocResp["FirewallRuleGroupAssociation"].(map[string]any)["Id"].(string)

			rec := doRequest(t, h, "UpdateFirewallRuleGroupAssociation", map[string]any{
				"FirewallRuleGroupAssociationId": assocID,
				"MutationProtection":             tt.mutationProtection,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assoc := resp["FirewallRuleGroupAssociation"].(map[string]any)
				assert.Equal(t, tt.mutationProtection, assoc["MutationProtection"])
			}
		})
	}
}

// --- AssociationCount decrements on disassociate ---

func TestAudit_ResolverQueryLogConfig_AssociationCountDecrement(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
		"Name":           "qlc-decrement",
		"DestinationArn": "arn:aws:s3:::my-logs",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	cfgID := createResp["ResolverQueryLogConfig"].(map[string]any)["Id"].(string)

	// Associate.
	assocRec := doRequest(t, h, "AssociateResolverQueryLogConfig", map[string]any{
		"ResolverQueryLogConfigId": cfgID,
		"ResourceId":               "vpc-dec-test",
	})
	require.Equal(t, http.StatusOK, assocRec.Code)
	var assocResp map[string]any
	require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &assocResp))
	assocID := assocResp["ResolverQueryLogConfigAssociation"].(map[string]any)["Id"].(string)

	// Disassociate.
	doRequest(t, h, "DisassociateResolverQueryLogConfig", map[string]any{
		"ResolverQueryLogConfigAssociationId": assocID,
	})

	// Count should be 0.
	getRec := doRequest(
		t,
		h,
		"GetResolverQueryLogConfig",
		map[string]any{"ResolverQueryLogConfigId": cfgID},
	)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	cfg := getResp["ResolverQueryLogConfig"].(map[string]any)
	assert.EqualValues(t, 0, cfg["AssociationCount"])
}

// --- QueryLogConfigAssociation Error/ErrorMessage fields ---

func TestAudit_ResolverQueryLogConfigAssociation_Fields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
		"Name":           "qlc-assoc-fields",
		"DestinationArn": "arn:aws:logs:us-east-1:000000000000:log-group:/test",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	cfgID := createResp["ResolverQueryLogConfig"].(map[string]any)["Id"].(string)

	assocRec := doRequest(t, h, "AssociateResolverQueryLogConfig", map[string]any{
		"ResolverQueryLogConfigId": cfgID,
		"ResourceId":               "vpc-fields-test",
	})
	require.Equal(t, http.StatusOK, assocRec.Code)

	var assocResp map[string]any
	require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &assocResp))
	assoc := assocResp["ResolverQueryLogConfigAssociation"].(map[string]any)

	assert.NotEmpty(t, assoc["Id"])
	assert.NotEmpty(t, assoc["CreationTime"])
	assert.Equal(t, "ACTIVE", assoc["Status"])
}

// --- CreateFirewallRule stores CreatorRequestId and timestamps ---

func TestAudit_FirewallRule_MetadataFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	grpRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "meta-grp"})
	require.Equal(t, http.StatusOK, grpRec.Code)
	var grpResp map[string]any
	require.NoError(t, json.Unmarshal(grpRec.Body.Bytes(), &grpResp))
	groupID := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

	dlRec := doRequest(t, h, "CreateFirewallDomainList", map[string]any{"Name": "meta-dl"})
	require.Equal(t, http.StatusOK, dlRec.Code)
	var dlResp map[string]any
	require.NoError(t, json.Unmarshal(dlRec.Body.Bytes(), &dlResp))
	dlID := dlResp["FirewallDomainList"].(map[string]any)["Id"].(string)

	createRec := doRequest(t, h, "CreateFirewallRule", map[string]any{
		"FirewallRuleGroupId":  groupID,
		"FirewallDomainListId": dlID,
		"Priority":             100,
		"Action":               "ALLOW",
		"Name":                 "meta-rule",
		"CreatorRequestId":     "req-fw-rule-1",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &resp))
	rule := resp["FirewallRule"].(map[string]any)

	assert.Equal(t, "req-fw-rule-1", rule["CreatorRequestId"])
	assert.NotEmpty(t, rule["CreationTime"])
	assert.NotEmpty(t, rule["ModificationTime"])
}

// --- Backend: AssociateFirewallRuleGroup stores CreatorRequestId and timestamps ---

func TestAudit_FirewallRuleGroupAssociation_CreatorAndTimestamps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	grpRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{"Name": "assoc-ts-grp"})
	require.Equal(t, http.StatusOK, grpRec.Code)
	var grpResp map[string]any
	require.NoError(t, json.Unmarshal(grpRec.Body.Bytes(), &grpResp))
	groupID := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

	assocRec := doRequest(t, h, "AssociateFirewallRuleGroup", map[string]any{
		"FirewallRuleGroupId": groupID,
		"VpcId":               "vpc-assoc-ts",
		"Priority":            100,
		"Name":                "assoc-ts",
		"CreatorRequestId":    "req-assoc-ts-1",
	})
	require.Equal(t, http.StatusOK, assocRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &resp))
	assoc := resp["FirewallRuleGroupAssociation"].(map[string]any)

	assert.Equal(t, "req-assoc-ts-1", assoc["CreatorRequestId"])
	assert.NotEmpty(t, assoc["CreationTime"])
	assert.NotEmpty(t, assoc["ModificationTime"])
	assert.Equal(t, "DISABLED", assoc["MutationProtection"])
}

// --- Backend direct: CreateResolverEndpoint type enum validation ---

func TestAudit_Backend_CreateEndpointTypeEnum(t *testing.T) {
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

func TestAudit_Backend_RuleTypeEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		ruleType  string
		epID      string
		targetIps []route53resolver.TargetIP
		wantErr   bool
	}{
		{
			name:     "system_no_ep_no_tips_ok",
			ruleType: "SYSTEM",
			wantErr:  false,
		},
		{
			name:     "system_with_ep_error",
			ruleType: "SYSTEM",
			epID:     "rslvr-in-fake1234",
			wantErr:  true,
		},
		{
			name:      "system_with_tips_error",
			ruleType:  "SYSTEM",
			targetIps: []route53resolver.TargetIP{{IP: "1.2.3.4", Port: 53}},
			wantErr:   true,
		},
		{
			name:      "forward_with_tips_ok",
			ruleType:  "FORWARD",
			targetIps: []route53resolver.TargetIP{{IP: "1.2.3.4", Port: 53}},
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateResolverRule(
				context.Background(),
				"r1",
				"example.com",
				tt.ruleType,
				tt.epID,
				"",
				tt.targetIps,
			)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// --- Backend direct: isValidQueryLogDestination ---

func TestAudit_Backend_QueryLogDestinationValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		destinationArn string
		wantErr        bool
	}{
		{name: "s3_valid", destinationArn: "arn:aws:s3:::my-bucket", wantErr: false},
		{
			name:           "cloudwatch_valid",
			destinationArn: "arn:aws:logs:us-east-1:000000000000:log-group:/test",
			wantErr:        false,
		},
		{
			name:           "firehose_valid",
			destinationArn: "arn:aws:firehose:us-east-1:000000000000:deliverystream/s",
			wantErr:        false,
		},
		{
			name:           "ec2_invalid",
			destinationArn: "arn:aws:ec2:us-east-1:000000000000:instance/i-abc",
			wantErr:        true,
		},
		{name: "empty_invalid", destinationArn: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateResolverQueryLogConfig(context.Background(), "qlc", "req-1", tt.destinationArn)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// --- Backend direct: DNSSEC default DISABLED ---

func TestAudit_Backend_DnssecDefaultDisabled(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	cfg := b.GetResolverDnssecConfig(context.Background(), "vpc-dnssec-new")

	assert.Equal(t, "DISABLED", cfg.ValidationStatus)
}

// --- Backend direct: DNSSEC ENABLING/DISABLING transitions ---

func TestAudit_Backend_DnssecTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		validation string
		wantStatus string
		wantErr    bool
	}{
		{name: "enable_to_enabling", validation: "ENABLE", wantStatus: "ENABLING"},
		{name: "disable_to_disabling", validation: "DISABLE", wantStatus: "DISABLING"},
		{name: "invalid_rejected", validation: "UNKNOWN", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
			cfg, err := b.UpdateResolverDnssecConfig(context.Background(), "vpc-test", tt.validation)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantStatus, cfg.ValidationStatus)
			}
		})
	}
}

// --- Backend direct: FirewallConfig no ARN ---

func TestAudit_Backend_FirewallConfigNoArn(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	cfg := b.GetFirewallConfig(context.Background(), "vpc-fwc")

	assert.NotEmpty(t, cfg.ID)
	assert.NotEmpty(t, cfg.OwnerID)
	assert.Equal(t, "vpc-fwc", cfg.ResourceID)
	assert.Equal(t, "DISABLED", cfg.FirewallFailOpen)
}

// --- Backend direct: AssociateFirewallRuleGroup MutationProtection defaults DISABLED ---

func TestAudit_Backend_MutationProtectionDefault(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	grp, _ := b.CreateFirewallRuleGroup(context.Background(), "grp", "req-1")

	assoc, err := b.AssociateFirewallRuleGroup(context.Background(), grp.ID, "vpc-test", "assoc", "req-2", "", 100)
	require.NoError(t, err)
	assert.Equal(t, "DISABLED", assoc.MutationProtection)
}

// --- Endpoint timestamps survive round trip ---

func TestAudit_Backend_EndpointTimestampsRoundTrip(t *testing.T) {
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
	)
	require.NoError(t, err)
	require.NotEmpty(t, ep.CreationTime)
	require.NotEmpty(t, ep.ModificationTime)

	snap := b.Snapshot()
	b2 := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	ep2, err := b2.GetResolverEndpoint(context.Background(), ep.ID)
	require.NoError(t, err)
	assert.Equal(t, ep.CreationTime, ep2.CreationTime)
	assert.Equal(t, ep.ModificationTime, ep2.ModificationTime)
	assert.Equal(t, "req-ts-1", ep2.CreatorRequestID)
}

// --- Firewall rule round trip preserves BlockOverride fields ---

func TestAudit_Backend_FirewallRuleBlockOverrideRoundTrip(t *testing.T) {
	t.Parallel()

	b := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	grp, _ := b.CreateFirewallRuleGroup(context.Background(), "grp-bor", "req-bor")
	dl, _ := b.CreateFirewallDomainList(context.Background(), "dl-bor", "req-bor")

	rule, err := b.CreateFirewallRule(context.Background(), route53resolver.CreateFirewallRuleParams{
		FirewallRuleGroupID:  grp.ID,
		FirewallDomainListID: dl.ID,
		Name:                 "rule-bor",
		Action:               "BLOCK",
		BlockResponse:        "OVERRIDE",
		BlockOverrideDomain:  "safe.example.com",
		BlockOverrideDNSType: "CNAME",
		BlockOverrideTTL:     600,
		Priority:             100,
	})
	require.NoError(t, err)

	snap := b.Snapshot()
	b2 := route53resolver.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	rules := b2.ListFirewallRules(context.Background(), grp.ID)
	require.Len(t, rules, 1)
	assert.Equal(t, rule.BlockOverrideDomain, rules[0].BlockOverrideDomain)
	assert.Equal(t, rule.BlockOverrideDNSType, rules[0].BlockOverrideDNSType)
	assert.Equal(t, rule.BlockOverrideTTL, rules[0].BlockOverrideTTL)
}

// TestAudit_FullCRUDLifecycle verifies complete lifecycle across all major resources.
func TestAudit_FullCRUDLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// 1. Endpoint lifecycle.
	epRec := doRequest(t, h, "CreateResolverEndpoint", map[string]any{
		"Name":                 "lifecycle-ep",
		"Direction":            "OUTBOUND",
		"ResolverEndpointType": "IPV4",
		"Protocols":            []string{"Do53"},
		"CreatorRequestId":     "req-ep-lifecycle",
	})
	require.Equal(t, http.StatusOK, epRec.Code)
	var epResp map[string]any
	require.NoError(t, json.Unmarshal(epRec.Body.Bytes(), &epResp))
	ep := epResp["ResolverEndpoint"].(map[string]any)
	epID := ep["Id"].(string)
	assert.NotEmpty(t, ep["CreationTime"])

	// 2. Rule with TargetIps.
	ruleRec := doRequest(t, h, "CreateResolverRule", map[string]any{
		"Name":               "lifecycle-rule",
		"DomainName":         "example.internal",
		"RuleType":           "FORWARD",
		"ResolverEndpointId": epID,
		"CreatorRequestId":   "req-rule-lifecycle",
		"TargetIps": []map[string]any{
			{"Ip": "10.0.0.1", "Port": 53},
			{"Ip": "10.0.0.2", "Port": 53},
		},
	})
	require.Equal(t, http.StatusOK, ruleRec.Code)
	var ruleResp map[string]any
	require.NoError(t, json.Unmarshal(ruleRec.Body.Bytes(), &ruleResp))
	rule := ruleResp["ResolverRule"].(map[string]any)
	ruleID := rule["Id"].(string)
	assert.Equal(t, "req-rule-lifecycle", rule["CreatorRequestId"])
	assert.NotEmpty(t, rule["OwnerID"])

	// 3. Query log config.
	qlcRec := doRequest(t, h, "CreateResolverQueryLogConfig", map[string]any{
		"Name":           "lifecycle-qlc",
		"DestinationArn": "arn:aws:s3:::lifecycle-logs",
	})
	require.Equal(t, http.StatusOK, qlcRec.Code)
	var qlcResp map[string]any
	require.NoError(t, json.Unmarshal(qlcRec.Body.Bytes(), &qlcResp))
	qlc := qlcResp["ResolverQueryLogConfig"].(map[string]any)
	qlcID := qlc["Id"].(string)
	assert.EqualValues(t, 0, qlc["AssociationCount"])

	// 4. Query log association increments count.
	doRequest(t, h, "AssociateResolverQueryLogConfig", map[string]any{
		"ResolverQueryLogConfigId": qlcID,
		"ResourceId":               "vpc-lifecycle",
	})
	getRec := doRequest(
		t,
		h,
		"GetResolverQueryLogConfig",
		map[string]any{"ResolverQueryLogConfigId": qlcID},
	)
	require.Equal(t, http.StatusOK, getRec.Code)
	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	assert.EqualValues(t, 1, getResp["ResolverQueryLogConfig"].(map[string]any)["AssociationCount"])

	// 5. Firewall rule group with timestamps.
	frgRec := doRequest(t, h, "CreateFirewallRuleGroup", map[string]any{
		"Name":             "lifecycle-frg",
		"CreatorRequestId": "req-frg-lifecycle",
	})
	require.Equal(t, http.StatusOK, frgRec.Code)
	var frgResp map[string]any
	require.NoError(t, json.Unmarshal(frgRec.Body.Bytes(), &frgResp))
	frg := frgResp["FirewallRuleGroup"].(map[string]any)
	frgID := frg["Id"].(string)
	assert.Equal(t, "NOT_SHARED", frg["ShareStatus"])
	assert.NotEmpty(t, frg["CreationTime"])

	// 6. Delete rule + endpoint.
	doRequest(t, h, "DeleteResolverRule", map[string]any{"ResolverRuleId": ruleID})
	doRequest(t, h, "DeleteResolverEndpoint", map[string]any{"ResolverEndpointId": epID})
	doRequest(t, h, "DeleteFirewallRuleGroup", map[string]any{"FirewallRuleGroupId": frgID})

	// 7. Verify deletion.
	getRuleRec := doRequest(t, h, "GetResolverRule", map[string]any{"ResolverRuleId": ruleID})
	assert.Equal(t, http.StatusNotFound, getRuleRec.Code)
}

// TestAudit_FirewallRule_BlockResponseStoredOnCreate ensures blockResponse is actually
// stored when creating a firewall rule (was previously dropped due to `_` param).
func TestAudit_FirewallRule_BlockResponseStoredOnCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		blockResponse string
		wantCode      int
	}{
		{name: "nodata", blockResponse: "NODATA", wantCode: http.StatusOK},
		{name: "nxdomain", blockResponse: "NXDOMAIN", wantCode: http.StatusOK},
		{name: "empty_allow", blockResponse: "", wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			grpRec := doRequest(
				t,
				h,
				"CreateFirewallRuleGroup",
				map[string]any{"Name": "br-test-grp"},
			)
			require.Equal(t, http.StatusOK, grpRec.Code)
			var grpResp map[string]any
			require.NoError(t, json.Unmarshal(grpRec.Body.Bytes(), &grpResp))
			groupID := grpResp["FirewallRuleGroup"].(map[string]any)["Id"].(string)

			dlRec := doRequest(
				t,
				h,
				"CreateFirewallDomainList",
				map[string]any{"Name": "br-test-dl"},
			)
			require.Equal(t, http.StatusOK, dlRec.Code)
			var dlResp map[string]any
			require.NoError(t, json.Unmarshal(dlRec.Body.Bytes(), &dlResp))
			dlID := dlResp["FirewallDomainList"].(map[string]any)["Id"].(string)

			body := map[string]any{
				"FirewallRuleGroupId":  groupID,
				"FirewallDomainListId": dlID,
				"Priority":             100,
				"Action":               "BLOCK",
				"Name":                 "br-rule",
			}
			if tt.blockResponse != "" {
				body["BlockResponse"] = tt.blockResponse
			}

			rec := doRequest(t, h, "CreateFirewallRule", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK && tt.blockResponse != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				rule := resp["FirewallRule"].(map[string]any)
				assert.Equal(t, tt.blockResponse, rule["BlockResponse"])
			}
		})
	}
}

// Avoid unused import.
var _ = strings.HasPrefix
