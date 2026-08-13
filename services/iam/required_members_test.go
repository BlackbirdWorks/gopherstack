package iam_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// getSSHPublicKeyResponseXML mirrors the unexported getSSHPublicKeyResponse
// wire shape (models_ssh_signing_certs.go) so this external test package can
// unmarshal SSHPublicKeyBody out of a real GetSSHPublicKey response.
type getSSHPublicKeyResponseXML struct {
	GetSSHPublicKeyResult struct {
		SSHPublicKey struct {
			SSHPublicKeyBody string `xml:"SSHPublicKeyBody"`
		} `xml:"SSHPublicKey"`
	} `xml:"GetSSHPublicKeyResult"`
}

// validPrivateKeyPEM is a syntactically valid (real base64, real PEM framing)
// but not-a-real-key PrivateKey body, matching the pattern already used by
// handler_create_tags_test.go's UploadServerCertificate coverage.
const validPrivateKeyPEM = "-----BEGIN PRIVATE KEY-----\nMA==\n-----END PRIVATE KEY-----"

// TestUploadServerCertificate_PrivateKeyRequired covers
// UploadServerCertificate.PrivateKey (required, api_op_UploadServerCertificate.go:95),
// dropped entirely before this fix -- handler_server_certificates.go never
// read it, so a request missing PrivateKey (or carrying garbage) succeeded
// with 200 and stored nothing to validate the key was well-formed.
func TestUploadServerCertificate_PrivateKeyRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params   map[string]string
		name     string
		wantCode string
		wantHTTP int
	}{
		{
			name: "missing private key rejected",
			params: map[string]string{
				"ServerCertificateName": "cert-a",
				"CertificateBody":       "-----BEGIN CERTIFICATE-----\nMA==\n-----END CERTIFICATE-----",
			},
			wantHTTP: http.StatusBadRequest,
			wantCode: "InvalidInput",
		},
		{
			name: "malformed private key rejected",
			params: map[string]string{
				"ServerCertificateName": "cert-b",
				"CertificateBody":       "-----BEGIN CERTIFICATE-----\nMA==\n-----END CERTIFICATE-----",
				"PrivateKey":            "not-a-pem-key-at-all",
			},
			wantHTTP: http.StatusBadRequest,
			wantCode: "MalformedCertificate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := callIAM(t, h, "UploadServerCertificate", tt.params)

			require.Equal(t, tt.wantHTTP, rec.Code)

			var errResp iam.ErrorResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.wantCode, errResp.Error.Code)
		})
	}

	t.Run("valid private key accepted and never echoed", func(t *testing.T) {
		t.Parallel()

		h, _ := newTestHandler(t)
		rec := callIAM(t, h, "UploadServerCertificate", map[string]string{
			"ServerCertificateName": "cert-c",
			"CertificateBody":       "-----BEGIN CERTIFICATE-----\nMA==\n-----END CERTIFICATE-----",
			"PrivateKey":            validPrivateKeyPEM,
		})

		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "cert-c")
		assert.NotContains(
			t,
			rec.Body.String(),
			"PRIVATE KEY",
			"real AWS never returns a private key; gopherstack must not either",
		)
	})
}

// TestGetSSHPublicKey_EncodingRequired covers GetSSHPublicKey.Encoding
// (required, api_op_GetSSHPublicKey.go:42), ignored entirely before this fix
// -- handler_ssh_keys.go always returned the stored body verbatim regardless
// of the requested encoding.
func TestGetSSHPublicKey_EncodingRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		encoding string
		name     string
	}{
		{name: "missing encoding rejected", encoding: ""},
		{name: "unrecognized encoding rejected", encoding: "XML"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			_, err := b.CreateUser("ssh-enc-user", "/", "")
			require.NoError(t, err)

			key, err := b.UploadSSHPublicKey("ssh-enc-user", "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAAgQC fixture")
			require.NoError(t, err)

			params := map[string]string{
				"UserName":       "ssh-enc-user",
				"SSHPublicKeyId": key.SSHPublicKeyID,
			}
			if tt.encoding != "" {
				params["Encoding"] = tt.encoding
			}

			rec := callIAM(t, h, "GetSSHPublicKey", params)
			require.Equal(t, http.StatusBadRequest, rec.Code)

			var errResp iam.ErrorResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "UnrecognizedPublicKeyEncoding", errResp.Error.Code)
		})
	}

	t.Run("ssh encoding returns stored body verbatim", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler(t)
		_, err := b.CreateUser("ssh-verbatim-user", "/", "")
		require.NoError(t, err)

		rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		sshPub, err := ssh.NewPublicKey(&rsaKey.PublicKey)
		require.NoError(t, err)

		body := string(ssh.MarshalAuthorizedKey(sshPub))

		key, err := b.UploadSSHPublicKey("ssh-verbatim-user", body)
		require.NoError(t, err)

		rec := callIAM(t, h, "GetSSHPublicKey", map[string]string{
			"UserName":       "ssh-verbatim-user",
			"SSHPublicKeyId": key.SSHPublicKeyID,
			"Encoding":       "SSH",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp getSSHPublicKeyResponseXML
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, body, resp.GetSSHPublicKeyResult.SSHPublicKey.SSHPublicKeyBody)
	})

	t.Run("pem encoding converts a real ssh key", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler(t)
		_, err := b.CreateUser("ssh-pem-user", "/", "")
		require.NoError(t, err)

		rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)

		sshPub, err := ssh.NewPublicKey(&rsaKey.PublicKey)
		require.NoError(t, err)

		authorizedKeysLine := string(ssh.MarshalAuthorizedKey(sshPub))

		key, err := b.UploadSSHPublicKey("ssh-pem-user", authorizedKeysLine)
		require.NoError(t, err)

		var resp getSSHPublicKeyResponseXML

		rec := callIAM(t, h, "GetSSHPublicKey", map[string]string{
			"UserName":       "ssh-pem-user",
			"SSHPublicKeyId": key.SSHPublicKeyID,
			"Encoding":       "PEM",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

		block, _ := pem.Decode([]byte(resp.GetSSHPublicKeyResult.SSHPublicKey.SSHPublicKeyBody))
		require.NotNil(t, block, "converted body must be valid PEM")

		pub, err := x509.ParsePKIXPublicKey(block.Bytes)
		require.NoError(t, err)

		gotRSA, ok := pub.(*rsa.PublicKey)
		require.True(t, ok)
		assert.Equal(t, rsaKey.PublicKey.N, gotRSA.N, "converted key must be the same key, not fabricated")
		assert.Equal(t, rsaKey.PublicKey.E, gotRSA.E)
	})
}

// TestSetSecurityTokenServicePreferences_GlobalEndpointTokenVersionRequired
// covers SetSecurityTokenServicePreferences.GlobalEndpointTokenVersion
// (required, :63), ignored entirely before this fix. Real IAM exposes no
// dedicated getter, but does surface it via GetAccountSummary's
// GlobalEndpointTokenVersion SummaryMap entry, so that is this test's
// observability path.
func TestSetSecurityTokenServicePreferences_GlobalEndpointTokenVersionRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version string
	}{
		{name: "missing version rejected", version: ""},
		{name: "unrecognized version rejected", version: "v3Token"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)

			params := map[string]string{}
			if tt.version != "" {
				params["GlobalEndpointTokenVersion"] = tt.version
			}

			rec := callIAM(t, h, "SetSecurityTokenServicePreferences", params)
			require.Equal(t, http.StatusBadRequest, rec.Code)

			var errResp iam.ErrorResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "ValidationError", errResp.Error.Code)
		})
	}

	t.Run("v2token observable via account summary", func(t *testing.T) {
		t.Parallel()

		h, _ := newTestHandler(t)

		rec := callIAM(t, h, "SetSecurityTokenServicePreferences", map[string]string{
			"GlobalEndpointTokenVersion": "v2Token",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		rec = callIAM(t, h, "GetAccountSummary", map[string]string{})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "<key>GlobalEndpointTokenVersion</key><value>2</value>")
	})
}

// TestCreateDelegationRequest_RequiredMembers covers CreateDelegationRequest
// (handler_account.go), which read only OwnerAccountId and silently dropped
// required Description (:49), NotificationChannel (:49), RequestorWorkflowId
// (:65), SessionDuration (:74), and Permissions -- and returned a fabricated
// nested <DelegationRequest> wire shape that does not match the real SDK's
// flat ConsoleDeepLink/DelegationRequestId output.
func TestCreateDelegationRequest_RequiredMembers(t *testing.T) {
	t.Parallel()

	validParams := map[string]string{
		"OwnerAccountId":                "111122223333",
		"Description":                   "test delegation",
		"NotificationChannel":           "arn:aws:sns:us-east-1:000000000000:topic",
		"RequestorWorkflowId":           "workflow-1",
		"SessionDuration":               "3600",
		"Permissions.PolicyTemplateArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
	}

	withoutKey := func(key string) map[string]string {
		clone := map[string]string{}
		for k, v := range validParams {
			if k != key {
				clone[k] = v
			}
		}

		return clone
	}

	tests := []struct {
		params map[string]string
		name   string
	}{
		{name: "missing description rejected", params: withoutKey("Description")},
		{name: "missing notification channel rejected", params: withoutKey("NotificationChannel")},
		{name: "missing requestor workflow id rejected", params: withoutKey("RequestorWorkflowId")},
		{name: "missing session duration rejected", params: withoutKey("SessionDuration")},
		{name: "missing permissions rejected", params: withoutKey("Permissions.PolicyTemplateArn")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			rec := callIAM(t, h, "CreateDelegationRequest", tt.params)
			require.Equal(t, http.StatusBadRequest, rec.Code)

			var errResp iam.ErrorResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "InvalidInput", errResp.Error.Code)
		})
	}

	t.Run("valid request returns real wire shape", func(t *testing.T) {
		t.Parallel()

		h, _ := newTestHandler(t)
		rec := callIAM(t, h, "CreateDelegationRequest", validParams)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp iam.CreateDelegationRequestResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

		assert.NotEmpty(t, resp.CreateDelegationRequestResult.DelegationRequestID)
		assert.NotEmpty(t, resp.CreateDelegationRequestResult.ConsoleDeepLink)
		assert.NotContains(t, rec.Body.String(), "<DelegationRequest>",
			"the real CreateDelegationRequestOutput has no nested DelegationRequest element")
	})
}

// TestGetHumanReadableSummary covers GetHumanReadableSummary
// (handler_account.go), which ignored vals entirely, dropped required
// EntityArn (:49), and returned the generic empty iamSimpleTagResponse
// instead of GetHumanReadableSummaryResult{Locale,SummaryContent,SummaryState}.
// gopherstack does not fabricate the LLM-generated summary (see PARITY.md);
// a known entity honestly gets SummaryState=NOT_SUPPORTED.
func TestGetHumanReadableSummary(t *testing.T) {
	t.Parallel()

	t.Run("missing entity arn rejected", func(t *testing.T) {
		t.Parallel()

		h, _ := newTestHandler(t)
		rec := callIAM(t, h, "GetHumanReadableSummary", map[string]string{})
		require.Equal(t, http.StatusBadRequest, rec.Code)

		var errResp iam.ErrorResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
		assert.Equal(t, "InvalidInput", errResp.Error.Code)
	})

	t.Run("unknown entity arn is not found", func(t *testing.T) {
		t.Parallel()

		h, _ := newTestHandler(t)
		rec := callIAM(t, h, "GetHumanReadableSummary", map[string]string{
			"EntityArn": "arn:aws:iam::" + iam.IAMAccountID + ":delegation-request/does-not-exist",
		})
		require.Equal(t, http.StatusNotFound, rec.Code)

		var errResp iam.ErrorResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
		assert.Equal(t, "NoSuchEntity", errResp.Error.Code)
	})

	t.Run("known entity honestly reports not supported", func(t *testing.T) {
		t.Parallel()

		h, b := newTestHandler(t)

		req, err := b.CreateDelegationRequest(iam.CreateDelegationRequestInput{
			OwnerAccountID:      "111122223333",
			Description:         "test delegation",
			NotificationChannel: "arn:aws:sns:us-east-1:000000000000:topic",
			RequestorWorkflowID: "workflow-1",
			SessionDuration:     3600,
			PolicyTemplateArn:   "arn:aws:iam::aws:policy/ReadOnlyAccess",
		})
		require.NoError(t, err)

		entityArn := "arn:aws:iam::" + iam.IAMAccountID + ":delegation-request/" + req.DelegationID

		rec := callIAM(t, h, "GetHumanReadableSummary", map[string]string{
			"EntityArn": entityArn,
			"Locale":    "en",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp iam.GetHumanReadableSummaryResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

		assert.Equal(t, "NOT_SUPPORTED", resp.GetHumanReadableSummaryResult.SummaryState)
		assert.Empty(
			t,
			resp.GetHumanReadableSummaryResult.SummaryContent,
			"gopherstack must never fabricate LLM summary prose",
		)
		assert.Equal(t, "en", resp.GetHumanReadableSummaryResult.Locale)
	})
}
