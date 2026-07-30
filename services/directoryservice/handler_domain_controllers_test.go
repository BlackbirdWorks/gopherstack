package directoryservice_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDomainControllers_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vpcSettings map[string]any
		name        string
		networkType string
		wantVpcID   string
		wantSubnets []string
		wantLen     int
		desired     int32
		wantIPv6DNS bool
	}{
		{name: "scale up to 3", desired: 3, wantLen: 3},
		{name: "scale up to 1", desired: 1, wantLen: 1},
		{name: "desired 0 removes all", desired: 0, wantLen: 0},
		{
			name:    "VpcId/SubnetId echo the directory's real VpcSettings",
			desired: 2,
			wantLen: 2,
			vpcSettings: map[string]any{
				"VpcId":     "vpc-domainctrl",
				"SubnetIds": []string{"subnet-dc1", "subnet-dc2"},
			},
			wantVpcID:   "vpc-domainctrl",
			wantSubnets: []string{"subnet-dc1", "subnet-dc2"},
		},
		{
			name:        "Dual-stack directory reports DnsIpv6Addr",
			desired:     1,
			wantLen:     1,
			networkType: "Dual-stack",
			wantIPv6DNS: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			createBody := map[string]any{
				"Name":     "corp.example.com",
				"Password": "Admin1234!",
				"Edition":  "Enterprise",
			}
			if tt.vpcSettings != nil {
				createBody["VpcSettings"] = tt.vpcSettings
			}
			if tt.networkType != "" {
				createBody["NetworkType"] = tt.networkType
			}
			createRec := doRequest(t, h, "CreateMicrosoftAD", createBody)
			require.Equal(t, http.StatusOK, createRec.Code)
			dirID, ok := respBody(t, createRec)["DirectoryId"].(string)
			require.True(t, ok)

			rec := doRequest(t, h, "UpdateNumberOfDomainControllers", map[string]any{
				"DirectoryId":   dirID,
				"DesiredNumber": tt.desired,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			listRec := doRequest(
				t,
				h,
				"DescribeDomainControllers",
				map[string]any{"DirectoryId": dirID},
			)
			require.Equal(t, http.StatusOK, listRec.Code)
			body := respBody(t, listRec)
			controllers, _ := body["DomainControllers"].([]any)
			assert.Len(t, controllers, tt.wantLen)

			for _, raw := range controllers {
				dc := raw.(map[string]any)
				assert.NotEmpty(t, dc["DnsIpAddr"], "DnsIpAddr must always be populated")
				assert.NotEmpty(
					t, dc["StatusLastUpdatedDateTime"], "StatusLastUpdatedDateTime must always be populated",
				)
				if tt.wantVpcID != "" {
					assert.Equal(t, tt.wantVpcID, dc["VpcId"], "VpcId must echo the directory's real VpcSettings")
					wantSubnets := make([]any, len(tt.wantSubnets))
					for i, s := range tt.wantSubnets {
						wantSubnets[i] = s
					}
					assert.Contains(t, wantSubnets, dc["SubnetId"])
				}
				if tt.wantIPv6DNS {
					assert.NotEmpty(t, dc["DnsIpv6Addr"], "Dual-stack directories must report DnsIpv6Addr")
				} else {
					assert.Empty(t, dc["DnsIpv6Addr"], "non-IPv6-capable directories must not report DnsIpv6Addr")
				}
			}
		})
	}
}

// --- CreateDirectory/ConnectDirectory returns DirectoryId shape ---

func TestDomainControllers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "update to 2 then describe"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			dirID := mustCreateSimpleAD(t, h, "corp.example.com")

			// Describe empty
			rec1 := doRequest(t, h, "DescribeDomainControllers", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec1.Code)
			var r1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &r1))
			dcs, _ := r1["DomainControllers"].([]any)
			assert.Empty(t, dcs)

			// Update to 2
			rec2 := doRequest(t, h, "UpdateNumberOfDomainControllers", map[string]any{
				"DirectoryId":   dirID,
				"DesiredNumber": 2,
			})
			assert.Equal(t, http.StatusOK, rec2.Code)

			// Describe again
			rec3 := doRequest(t, h, "DescribeDomainControllers", map[string]any{"DirectoryId": dirID})
			assert.Equal(t, http.StatusOK, rec3.Code)
			var r3 map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &r3))
			dcs2, _ := r3["DomainControllers"].([]any)
			assert.Len(t, dcs2, 2)

			_ = tc
		})
	}
}
