package transfer_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_DescribeUser_SshKeyOmitsKeyType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{name: "rsa", body: "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAAgQC5 rsa-key"},
		{name: "ed25519", body: "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMq ed-key"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			s, err := h.Backend.CreateServer(nil, nil)
			require.NoError(t, err)

			doTransferRequest(t, h, "CreateUser", map[string]any{
				"ServerId": s.ServerID, "UserName": "u",
				"Role": "arn:aws:iam::123456789012:role/TransferUserRole",
			})
			imp := doTransferRequest(t, h, "ImportSshPublicKey", map[string]any{
				"ServerId": s.ServerID, "UserName": "u", "SshPublicKeyBody": tc.body,
			})
			require.Equal(t, http.StatusOK, imp.Code, imp.Body.String())

			rec := doTransferRequest(t, h, "DescribeUser", map[string]any{"ServerId": s.ServerID, "UserName": "u"})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				User struct {
					SSHPublicKeys []map[string]any `json:"SshPublicKeys"`
				} `json:"User"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			require.Len(t, out.User.SSHPublicKeys, 1)
			assert.NotContains(t, out.User.SSHPublicKeys[0], "KeyType")
			assert.Equal(t, tc.body, out.User.SSHPublicKeys[0]["SshPublicKeyBody"])
		})
	}
}
