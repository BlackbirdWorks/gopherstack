package transfer_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Patterns are the SDK-documented ID shapes; terraform-provider-aws validates workflow and server IDs against them.
func TestResourceIDShapes(t *testing.T) {
	t.Parallel()

	const role = "arn:aws:iam::000000000000:role/transfer-role"

	tests := []struct {
		body    func(serverID, profileID string) map[string]any
		name    string
		target  string
		idField string
		pattern string
	}{
		{
			name: "server", target: "CreateServer", idField: "ServerId", pattern: `^s-[0-9a-f]{17}$`,
			body: func(string, string) map[string]any { return map[string]any{} },
		},
		{
			name: "workflow", target: "CreateWorkflow", idField: "WorkflowId", pattern: `^w-[a-z0-9]{17}$`,
			body: func(string, string) map[string]any {
				return map[string]any{
					"Steps": []map[string]any{{"Type": "DELETE", "DeleteStepDetails": map[string]any{"Name": "d"}}},
				}
			},
		},
		{
			name: "connector", target: "CreateConnector", idField: "ConnectorId", pattern: `^c-[0-9a-f]{17}$`,
			body: func(string, string) map[string]any {
				return map[string]any{
					"Url":        "https://partner.example.com",
					"AccessRole": role,
					"As2Config":  map[string]any{},
				}
			},
		},
		{
			name: "profile", target: "CreateProfile", idField: "ProfileId", pattern: `^p-[0-9a-f]{17}$`,
			body: func(string, string) map[string]any {
				return map[string]any{"As2Id": "AS2-ID", "ProfileType": "LOCAL"}
			},
		},
		{
			name: "agreement", target: "CreateAgreement", idField: "AgreementId", pattern: `^a-[0-9a-f]{17}$`,
			body: func(serverID, profileID string) map[string]any {
				return map[string]any{
					"ServerId": serverID, "LocalProfileId": profileID, "PartnerProfileId": profileID,
					"BaseDirectory": "/b", "AccessRole": role,
				}
			},
		},
		{
			name: "host_key", target: "ImportHostKey", idField: "HostKeyId", pattern: `^hostkey-[0-9a-f]{17}$`,
			body: func(serverID, _ string) map[string]any {
				return map[string]any{"ServerId": serverID, "HostKeyBody": testHostKeyEd25519}
			},
		},
		{
			name: "certificate", target: "ImportCertificate", idField: "CertificateId", pattern: `^cert-[0-9a-f]{17}$`,
			body: func(string, string) map[string]any {
				return map[string]any{"Usage": "SIGNING", "Certificate": testCertPEM}
			},
		},
		{
			name: "web_app", target: "CreateWebApp", idField: "WebAppId", pattern: `^webapp-[0-9a-f]{17}$`,
			body: func(string, string) map[string]any { return webAppCreateBody() },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			srv := doTransferRequest(t, h, "CreateServer", map[string]any{})
			require.Equal(t, http.StatusOK, srv.Code)

			var srvOut, profOut map[string]any
			require.NoError(t, json.Unmarshal(srv.Body.Bytes(), &srvOut))

			prof := doTransferRequest(t, h, "CreateProfile", map[string]any{"As2Id": "AS2", "ProfileType": "LOCAL"})
			require.Equal(t, http.StatusOK, prof.Code)
			require.NoError(t, json.Unmarshal(prof.Body.Bytes(), &profOut))

			rec := doTransferRequest(
				t,
				h,
				tt.target,
				tt.body(srvOut["ServerId"].(string), profOut["ProfileId"].(string)),
			)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Regexp(t, tt.pattern, out[tt.idField])
		})
	}
}

func TestSSHKeyAndExecutionIDShapes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	srv := doTransferRequest(t, h, "CreateServer", map[string]any{})

	var srvOut map[string]any
	require.NoError(t, json.Unmarshal(srv.Body.Bytes(), &srvOut))

	doTransferRequest(t, h, "CreateUser", map[string]any{
		"ServerId": srvOut["ServerId"], "UserName": "alice", "Role": "arn:aws:iam::000000000000:role/r",
	})

	rec := doTransferRequest(t, h, "ImportSshPublicKey", map[string]any{
		"ServerId": srvOut["ServerId"], "UserName": "alice", "SshPublicKeyBody": testHostKeyEd25519,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Regexp(t, `^key-[0-9a-f]{17}$`, out["SshPublicKeyId"])
}
