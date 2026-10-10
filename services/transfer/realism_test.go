package transfer_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const realismRole = "arn:aws:iam::123456789012:role/TransferUserRole"

func transferErr(t *testing.T, body []byte) (string, string) {
	t.Helper()

	var out struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(body, &out))

	return out.Type, out.Message
}

func TestUserNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		userName string
		wantOK   bool
	}{
		{name: "simple", userName: "user1", wantOK: true},
		{name: "email like", userName: "a.b@example.com", wantOK: true},
		{name: "too short", userName: "ab"},
		{name: "space", userName: "bad name"},
		{name: "bang", userName: "bad!name"},
		{name: "leading hyphen", userName: "-user"},
		{name: "leading at", userName: "@user"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			s, err := h.Backend.CreateServer(nil, nil)
			require.NoError(t, err)

			rec := doTransferRequest(t, h, "CreateUser", map[string]any{
				"ServerId": s.ServerID, "UserName": tt.userName, "Role": realismRole,
			})

			if tt.wantOK {
				assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				return
			}

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			code, _ := transferErr(t, rec.Body.Bytes())
			assert.Equal(t, "InvalidRequestException", code)
		})
	}
}

func TestImportSSHPublicKeyFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		body   string
		wantOK bool
	}{
		{name: "rsa", body: "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAAgQC5 comment", wantOK: true},
		{
			name: "ed25519", wantOK: true,
			body: "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl",
		},
		{name: "ecdsa", body: "ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTY=", wantOK: true},
		{name: "garbage", body: "notakey"},
		{name: "unknown type", body: "ssh-dss AAAAB3NzaC1kc3M="},
		{name: "no body", body: "ssh-rsa"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			s, err := h.Backend.CreateServer(nil, nil)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, doTransferRequest(t, h, "CreateUser", map[string]any{
				"ServerId": s.ServerID, "UserName": "user1", "Role": realismRole,
			}).Code)

			rec := doTransferRequest(t, h, "ImportSshPublicKey", map[string]any{
				"ServerId": s.ServerID, "UserName": "user1", "SshPublicKeyBody": tt.body,
			})
			if tt.wantOK {
				assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				return
			}

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestFTPServerRequirements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req    map[string]any
		name   string
		wantOK bool
	}{
		{name: "sftp public", req: map[string]any{"Protocols": []string{"SFTP"}}, wantOK: true},
		{name: "ftp public", req: map[string]any{"Protocols": []string{"FTP"}}},
		{
			name: "ftps vpc service managed",
			req:  map[string]any{"Protocols": []string{"FTPS"}, "EndpointType": "VPC"},
		},
		{
			name: "ftp vpc lambda",
			req: map[string]any{
				"Protocols": []string{"FTP"}, "EndpointType": "VPC", "IdentityProviderType": "AWS_LAMBDA",
			},
			wantOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doTransferRequest(t, newTestHandler(t), "CreateServer", tt.req)
			if tt.wantOK {
				assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				return
			}

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestListPagingValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req      map[string]any
		name     string
		wantType string
	}{
		{name: "bad token", req: map[string]any{"NextToken": "garbage"}, wantType: "InvalidNextTokenException"},
		{name: "zero max", req: map[string]any{"MaxResults": 0}, wantType: "InvalidRequestException"},
		{name: "huge max", req: map[string]any{"MaxResults": 1001}, wantType: "InvalidRequestException"},
		{name: "valid", req: map[string]any{"MaxResults": 10, "NextToken": "MA=="}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doTransferRequest(t, newTestHandler(t), "ListServers", tt.req)
			if tt.wantType == "" {
				assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				return
			}

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			code, _ := transferErr(t, rec.Body.Bytes())
			assert.Equal(t, tt.wantType, code)
		})
	}
}

func TestTagResourceUnknownARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	tests := []struct {
		name   string
		arn    string
		wantOK bool
	}{
		{name: "existing server", arn: "arn:aws:transfer:us-east-1:123456789012:server/" + s.ServerID, wantOK: true},
		{name: "missing server", arn: "arn:aws:transfer:us-east-1:123456789012:server/s-00000000000000000"},
		{name: "missing user", arn: "arn:aws:transfer:us-east-1:123456789012:user/" + s.ServerID + "/nobody"},
		{name: "not a transfer arn", arn: "arn:aws:s3:::bucket"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doTransferRequest(t, h, "TagResource", map[string]any{
				"Arn": tt.arn, "Tags": []map[string]string{{"Key": "k", "Value": "v"}},
			})
			if tt.wantOK {
				assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				return
			}

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			code, msg := transferErr(t, rec.Body.Bytes())
			assert.Equal(t, "ResourceNotFoundException", code)
			assert.NotContains(t, msg, "ResourceNotFoundException")
		})
	}
}
