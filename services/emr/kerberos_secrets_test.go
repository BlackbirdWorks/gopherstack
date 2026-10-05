package emr_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDescribeCluster_KerberosAttributesOmitPasswords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		attr map[string]any
		name string
	}{
		{
			name: "kdc_admin_password",
			attr: map[string]any{"Realm": "EC2.INTERNAL", "KdcAdminPassword": "kdc-secret-1"},
		},
		{
			name: "all_passwords",
			attr: map[string]any{
				"Realm": "EC2.INTERNAL", "KdcAdminPassword": "kdc-secret-2",
				"ADDomainJoinUser": "joiner", "ADDomainJoinPassword": "join-secret",
				"CrossRealmTrustPrincipalPassword": "trust-secret",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "krb", "KerberosAttributes": tt.attr})
			require.Equal(t, http.StatusOK, rec.Code)

			var create struct {
				JobFlowID string `json:"JobFlowId"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &create))

			desc := doEMRRequest(t, h, "DescribeCluster", map[string]any{"ClusterId": create.JobFlowID})
			require.Equal(t, http.StatusOK, desc.Code)

			var out struct {
				Cluster struct {
					KerberosAttributes map[string]any `json:"KerberosAttributes"`
				} `json:"Cluster"`
			}
			require.NoError(t, json.Unmarshal(desc.Body.Bytes(), &out))

			ka := out.Cluster.KerberosAttributes
			assert.Equal(t, "EC2.INTERNAL", ka["Realm"])
			assert.NotContains(t, ka, "KdcAdminPassword")
			assert.NotContains(t, ka, "ADDomainJoinPassword")
			assert.NotContains(t, ka, "CrossRealmTrustPrincipalPassword")
			assert.NotContains(t, desc.Body.String(), "-secret")
		})
	}
}
