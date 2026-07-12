package transfer_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// testCertPEM is a self-signed RSA 2048-bit certificate used by certificate parity tests.
// Generated with: openssl req -x509 -newkey rsa:2048 -keyout /dev/null -out - -days 3650 -nodes -subj "/CN=test".
const testCertPEM = `-----BEGIN CERTIFICATE-----
MIIC/zCCAeegAwIBAgIURQA1ea7ssWq2hJxY5habS2n67x8wDQYJKoZIhvcNAQEL
BQAwDzENMAsGA1UEAwwEdGVzdDAeFw0yNjA2MjYxNTU3MzFaFw0zNjA2MjMxNTU3
MzFaMA8xDTALBgNVBAMMBHRlc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEK
AoIBAQCTZgjXmrJtpbR3QMaMPF/TAp5dTr0T92wCw0sU0TUrYEEOy+sNpSrHHL2G
bA3LCrces9M6SsKK9jlBUpd9THLccwDDZu9qadUgTYvufaMXrJRlaBVuDf7ek1Um
SogeUz+J8mhdQvW2lHblDf14H6IF4ZZhWEWcBDGHNPvjrUgyArjEVgrBAnnmBRUJ
j4Sd+ZU/56Xj9kMjXLcz/X+Xxx4enhQZaJ5RamyY2N05yMB5V9AdZhQNstttLHLa
hcnWnQN6hGY592k/QESSd3iF7SKSYi9ibJHYdmL8ER8sDCfrMGA6p5kcfBlNs03d
ZyloCovDxS8Ut67QPNRzoHVVlFuzAgMBAAGjUzBRMB0GA1UdDgQWBBRsmCv3SxDr
aYhA7NougOn/HZtnGDAfBgNVHSMEGDAWgBRsmCv3SxDraYhA7NougOn/HZtnGDAP
BgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBun1ThOPLQ5uokRaNg
L0lr39TK3vMZPD4FwUPbtLJ7DIiOhs2bs0VUIsawfeBW3Hy1BMuYPcNiVIn8YM9o
F+KosTDHt9mUN56dNQdqHWoXYXGyu47m0642K0hs7AZaqbHmlHdqdfnd3Ej7Dd18
5eWN4A/OsiWPZxCXN/UNOPQYY+iGo7Zzw5qhg4tmhzUJiA06IR1aXx6VvQpLy3Us
sc+cWqCMXDtucv4DJ4+cvp8dnMo78XSEpCV6qyJcWjUjkLmqYKpxiwDtslVz5ktd
CSPNOxS7HMW5q6nQ5NaTo2FivH0VfliOA3BspWypU02jPWghQkJTjRlzOeCK3PAu
t/Kr
-----END CERTIFICATE-----
`

// TestParity_CreateWorkflow_InvalidStepTypeRejected verifies that CreateWorkflow returns 400
// when a step Type is not one of the real AWS-valid values (COPY, CUSTOM, DELETE, TAG, DECRYPT).
func TestParity_CreateWorkflow_InvalidStepTypeRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "CreateWorkflow", map[string]any{
		"Steps": []map[string]any{
			{"Type": "INVALID_STEP"},
		},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"CreateWorkflow with invalid step Type must return 400; body: %s", rec.Body.String())
}

// TestParity_CreateWorkflow_ValidStepTypesAccepted verifies that all five real AWS step types
// are accepted by CreateWorkflow.
func TestParity_CreateWorkflow_ValidStepTypesAccepted(t *testing.T) {
	t.Parallel()

	validTypes := []string{"COPY", "CUSTOM", "DELETE", "TAG", "DECRYPT"}

	for _, stepType := range validTypes {
		t.Run(stepType, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTransferRequest(t, h, "CreateWorkflow", map[string]any{
				"Steps": []map[string]any{
					{"Type": stepType},
				},
			})

			assert.Equal(t, http.StatusOK, rec.Code,
				"CreateWorkflow with step Type %q must return 200; body: %s", stepType, rec.Body.String())
		})
	}
}

// TestParity_ImportCertificate_TLSUsageAccepted verifies that Usage=TLS is accepted by
// ImportCertificate. Real AWS supports SIGNING, ENCRYPTION, and TLS as valid Usage values.
func TestParity_ImportCertificate_TLSUsageAccepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "ImportCertificate", map[string]any{
		"Usage": "TLS",
	})

	assert.Equal(t, http.StatusOK, rec.Code,
		"ImportCertificate with Usage=TLS must return 200; body: %s", rec.Body.String())
}

// TestParity_ImportCertificate_InvalidUsageRejected verifies that ImportCertificate returns 400
// for an unknown Usage value, matching real AWS behaviour.
func TestParity_ImportCertificate_InvalidUsageRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "ImportCertificate", map[string]any{
		"Usage": "UNKNOWN_USAGE",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"ImportCertificate with unknown Usage must return 400; body: %s", rec.Body.String())
}

// TestParity_DescribeCertificate_BodyReturnedWhenPresent verifies that DescribeCertificate
// returns the Certificate body when one was stored at import time. Real AWS returns the
// certificate PEM body in the Certificate field of the response.
func TestParity_DescribeCertificate_BodyReturnedWhenPresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	importRec := doTransferRequest(t, h, "ImportCertificate", map[string]any{
		"Certificate": testCertPEM,
		"Usage":       "SIGNING",
	})
	require.Equal(t, http.StatusOK, importRec.Code,
		"ImportCertificate failed: %s", importRec.Body.String())

	var importOut struct {
		CertificateID string `json:"CertificateId"`
	}
	require.NoError(t, json.Unmarshal(importRec.Body.Bytes(), &importOut))

	descRec := doTransferRequest(t, h, "DescribeCertificate", map[string]any{
		"CertificateId": importOut.CertificateID,
	})
	require.Equal(t, http.StatusOK, descRec.Code, "DescribeCertificate failed: %s", descRec.Body.String())

	var descOut struct {
		Certificate struct {
			Certificate string `json:"Certificate"`
		} `json:"Certificate"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

	assert.Equal(t, testCertPEM, descOut.Certificate.Certificate,
		"DescribeCertificate must return the certificate body when present")
}

// TestParity_ListUsers_SshPublicKeyCountAndHomeDirectoryType verifies that ListUsers returns
// the correct SshPublicKeyCount and HomeDirectoryType for each user. Real AWS includes both
// fields in the list response.
func TestParity_ListUsers_SshPublicKeyCountAndHomeDirectoryType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createSrvRec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, createSrvRec.Code)

	var srvOut struct {
		ServerID string `json:"ServerId"`
	}
	require.NoError(t, json.Unmarshal(createSrvRec.Body.Bytes(), &srvOut))

	createUserRec := doTransferRequest(t, h, "CreateUser", map[string]any{
		"ServerId":          srvOut.ServerID,
		"UserName":          "alice",
		"Role":              "arn:aws:iam::123456789012:role/TransferRole",
		"HomeDirectoryType": "LOGICAL",
	})
	require.Equal(t, http.StatusOK, createUserRec.Code, "CreateUser failed: %s", createUserRec.Body.String())

	importRec := doTransferRequest(t, h, "ImportSshPublicKey", map[string]any{
		"ServerId":         srvOut.ServerID,
		"UserName":         "alice",
		"SshPublicKeyBody": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQC test@test",
	})
	require.Equal(t, http.StatusOK, importRec.Code, "ImportSshPublicKey failed: %s", importRec.Body.String())

	listRec := doTransferRequest(t, h, "ListUsers", map[string]any{
		"ServerId": srvOut.ServerID,
	})
	require.Equal(t, http.StatusOK, listRec.Code, "ListUsers failed: %s", listRec.Body.String())

	var listOut struct {
		Users []struct {
			UserName          string `json:"UserName"`
			HomeDirectoryType string `json:"HomeDirectoryType"`
			SSHPublicKeyCount int    `json:"SshPublicKeyCount"`
		} `json:"Users"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	require.Len(t, listOut.Users, 1)

	assert.Equal(t, "LOGICAL", listOut.Users[0].HomeDirectoryType)
	assert.Equal(t, 1, listOut.Users[0].SSHPublicKeyCount)
}

// TestParity_UpdateAccess_FullFieldsRoundtrip verifies that UpdateAccess persists
// PosixProfile, HomeDirectoryType, and Policy fields. Real AWS supports all mutable
// Access fields in UpdateAccess.
func TestParity_UpdateAccess_FullFieldsRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createSrvRec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, createSrvRec.Code)

	var srvOut struct {
		ServerID string `json:"ServerId"`
	}
	require.NoError(t, json.Unmarshal(createSrvRec.Body.Bytes(), &srvOut))

	createAccessRec := doTransferRequest(t, h, "CreateAccess", map[string]any{
		"ServerId":          srvOut.ServerID,
		"ExternalId":        "S-1-5-21-9999",
		"Role":              "arn:aws:iam::123456789012:role/TransferRole",
		"HomeDirectoryType": "PATH",
		"HomeDirectory":     "/home/alice",
	})
	require.Equal(t, http.StatusOK, createAccessRec.Code, "CreateAccess failed: %s", createAccessRec.Body.String())

	updateRec := doTransferRequest(t, h, "UpdateAccess", map[string]any{
		"ServerId":          srvOut.ServerID,
		"ExternalId":        "S-1-5-21-9999",
		"HomeDirectoryType": "LOGICAL",
		"Policy":            `{"Version":"2012-10-17","Statement":[]}`,
		"PosixProfile": map[string]any{
			"Uid": 1001,
			"Gid": 1001,
		},
	})
	require.Equal(t, http.StatusOK, updateRec.Code, "UpdateAccess failed: %s", updateRec.Body.String())

	descRec := doTransferRequest(t, h, "DescribeAccess", map[string]any{
		"ServerId":   srvOut.ServerID,
		"ExternalId": "S-1-5-21-9999",
	})
	require.Equal(t, http.StatusOK, descRec.Code, "DescribeAccess failed: %s", descRec.Body.String())

	var descOut struct {
		Access struct {
			PosixProfile *struct {
				UID int `json:"Uid"`
				GID int `json:"Gid"`
			} `json:"PosixProfile"`
			HomeDirectoryType string `json:"HomeDirectoryType"`
			Policy            string `json:"Policy"`
		} `json:"Access"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

	assert.Equal(t, "LOGICAL", descOut.Access.HomeDirectoryType)
	assert.JSONEq(t, `{"Version":"2012-10-17","Statement":[]}`, descOut.Access.Policy)
	require.NotNil(t, descOut.Access.PosixProfile)
	assert.Equal(t, 1001, descOut.Access.PosixProfile.UID)
}

// TestParity_UpdateConnector_LoggingRoleAndSecurityPolicyName verifies that UpdateConnector
// persists LoggingRole and SecurityPolicyName. Real AWS returns both in DescribeConnector.
func TestParity_UpdateConnector_LoggingRoleAndSecurityPolicyName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateConnector", map[string]any{
		"Url": "https://partner.example.com",
		"As2Config": map[string]any{
			"LocalProfileId":      "local-profile",
			"PartnerProfileId":    "partner-profile",
			"MessageSubject":      "test",
			"Compression":         "DISABLED",
			"EncryptionAlgorithm": "AES128_CBC",
			"SigningAlgorithm":    "SHA256",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code, "CreateConnector failed: %s", createRec.Body.String())

	var createOut struct {
		ConnectorID string `json:"ConnectorId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	updateRec := doTransferRequest(t, h, "UpdateConnector", map[string]any{
		"ConnectorId":        createOut.ConnectorID,
		"LoggingRole":        "arn:aws:iam::123456789012:role/TransferLogging",
		"SecurityPolicyName": "TransferSecurityPolicy-2024-01",
	})
	require.Equal(t, http.StatusOK, updateRec.Code, "UpdateConnector failed: %s", updateRec.Body.String())

	descRec := doTransferRequest(t, h, "DescribeConnector", map[string]any{
		"ConnectorId": createOut.ConnectorID,
	})
	require.Equal(t, http.StatusOK, descRec.Code, "DescribeConnector failed: %s", descRec.Body.String())

	var descOut struct {
		Connector struct {
			LoggingRole        string `json:"LoggingRole"`
			SecurityPolicyName string `json:"SecurityPolicyName"`
		} `json:"Connector"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

	assert.Equal(t, "arn:aws:iam::123456789012:role/TransferLogging", descOut.Connector.LoggingRole)
	assert.Equal(t, "TransferSecurityPolicy-2024-01", descOut.Connector.SecurityPolicyName)
}

// TestParity_Profile_CertificateIDsRoundtrip verifies that CertificateIds set via UpdateProfile
// are returned by DescribeProfile. Real AWS stores CertificateIds on AS2 profiles.
func TestParity_Profile_CertificateIDsRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateProfile", map[string]any{
		"As2Id":       "MY-AS2-ID",
		"ProfileType": "LOCAL",
	})
	require.Equal(t, http.StatusOK, createRec.Code, "CreateProfile failed: %s", createRec.Body.String())

	var createOut struct {
		ProfileID string `json:"ProfileId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	certIDs := []string{"cert-aaa111", "cert-bbb222"}
	updateRec := doTransferRequest(t, h, "UpdateProfile", map[string]any{
		"ProfileId":      createOut.ProfileID,
		"CertificateIds": certIDs,
	})
	require.Equal(t, http.StatusOK, updateRec.Code, "UpdateProfile failed: %s", updateRec.Body.String())

	descRec := doTransferRequest(t, h, "DescribeProfile", map[string]any{
		"ProfileId": createOut.ProfileID,
	})
	require.Equal(t, http.StatusOK, descRec.Code, "DescribeProfile failed: %s", descRec.Body.String())

	var descOut struct {
		Profile struct {
			CertificateIDs []string `json:"CertificateIds"`
		} `json:"Profile"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

	assert.Equal(t, certIDs, descOut.Profile.CertificateIDs)
}

// TestParity_ListTagsForResource_CreationTagsVisible verifies that tags specified at
// resource creation time are visible via ListTagsForResource. Real AWS returns creation-time
// tags from ListTagsForResource without a separate TagResource call.
func TestParity_ListTagsForResource_CreationTagsVisible(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateServer", map[string]any{
		"Tags": []map[string]any{
			{"Key": "Environment", "Value": "production"},
			{"Key": "Owner", "Value": "platform"},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code, "CreateServer failed: %s", createRec.Body.String())

	var createOut struct {
		ServerID string `json:"ServerId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	serverARN := "arn:aws:transfer:us-east-1:123456789012:server/" + createOut.ServerID
	listRec := doTransferRequest(t, h, "ListTagsForResource", map[string]any{
		"Arn": serverARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code, "ListTagsForResource failed: %s", listRec.Body.String())

	var listOut struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))

	tagMap := make(map[string]string, len(listOut.Tags))

	for _, tag := range listOut.Tags {
		tagMap[tag.Key] = tag.Value
	}

	assert.Equal(t, "production", tagMap["Environment"],
		"creation-time tags must be visible via ListTagsForResource")
	assert.Equal(t, "platform", tagMap["Owner"])
}

// TestParity_ListTagsForResource_CreationTagsVisibleAcrossResources extends the
// Server-only coverage above to every other taggable Transfer resource type.
// Real AWS returns creation-time tags from ListTagsForResource for Agreement,
// Profile, User, WebApp, Certificate, and HostKey without a separate TagResource
// call, exactly as it does for Server.
func TestParity_ListTagsForResource_CreationTagsVisibleAcrossResources(t *testing.T) {
	t.Parallel()

	creationTags := []map[string]any{{"Key": "Env", "Value": "test"}}

	tests := []struct {
		setup func(t *testing.T, h *transfer.Handler) string // returns resource ARN
		name  string
	}{
		{
			name: "Agreement",
			setup: func(t *testing.T, h *transfer.Handler) string {
				t.Helper()

				serverID := mustCreateServer(t, h)

				rec := doTransferRequest(t, h, "CreateAgreement", map[string]any{
					"ServerId": serverID,
					"Tags":     creationTags,
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var out struct {
					AgreementID string `json:"AgreementId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return "arn:aws:transfer:us-east-1:123456789012:server/" + serverID + "/agreement/" + out.AgreementID
			},
		},
		{
			name: "Profile",
			setup: func(t *testing.T, h *transfer.Handler) string {
				t.Helper()

				rec := doTransferRequest(t, h, "CreateProfile", map[string]any{
					"ProfileType": "LOCAL",
					"Tags":        creationTags,
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var out struct {
					ProfileID string `json:"ProfileId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return "arn:aws:transfer:us-east-1:123456789012:profile/" + out.ProfileID
			},
		},
		{
			name: "User",
			setup: func(t *testing.T, h *transfer.Handler) string {
				t.Helper()

				serverID := mustCreateServer(t, h)

				rec := doTransferRequest(t, h, "CreateUser", map[string]any{
					"ServerId":      serverID,
					"UserName":      "alice",
					"Role":          "arn:aws:iam::123456789012:role/test",
					"HomeDirectory": "/alice",
					"Tags":          creationTags,
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				return "arn:aws:transfer:us-east-1:123456789012:user/" + serverID + "/alice"
			},
		},
		{
			name: "WebApp",
			setup: func(t *testing.T, h *transfer.Handler) string {
				t.Helper()

				rec := doTransferRequest(t, h, "CreateWebApp", map[string]any{"Tags": creationTags})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var out struct {
					WebAppID string `json:"WebAppId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return "arn:aws:transfer:us-east-1:123456789012:webapp/" + out.WebAppID
			},
		},
		{
			name: "Certificate",
			setup: func(t *testing.T, h *transfer.Handler) string {
				t.Helper()

				rec := doTransferRequest(t, h, "ImportCertificate", map[string]any{
					"Usage": "SIGNING",
					"Tags":  creationTags,
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var out struct {
					CertificateID string `json:"CertificateId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return "arn:aws:transfer:us-east-1:123456789012:certificate/" + out.CertificateID
			},
		},
		{
			name: "HostKey",
			setup: func(t *testing.T, h *transfer.Handler) string {
				t.Helper()

				serverID := mustCreateServer(t, h)

				rec := doTransferRequest(t, h, "ImportHostKey", map[string]any{
					"ServerId":    serverID,
					"HostKeyBody": testSSHHostKeyBody,
					"Tags":        creationTags,
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var out struct {
					HostKeyID string `json:"HostKeyId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return "arn:aws:transfer:us-east-1:123456789012:host-key/" + serverID + "/" + out.HostKeyID
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(t, h)

			listRec := doTransferRequest(t, h, "ListTagsForResource", map[string]any{"Arn": resourceARN})
			require.Equal(t, http.StatusOK, listRec.Code, listRec.Body.String())

			var listOut struct {
				Tags []struct {
					Key   string `json:"Key"`
					Value string `json:"Value"`
				} `json:"Tags"`
			}
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))

			tagMap := make(map[string]string, len(listOut.Tags))
			for _, tag := range listOut.Tags {
				tagMap[tag.Key] = tag.Value
			}

			assert.Equal(t, "test", tagMap["Env"],
				"creation-time tags must be visible via ListTagsForResource for "+tt.name)
		})
	}
}

// mustCreateServer creates a server and returns its ServerId.
func mustCreateServer(t *testing.T, h *transfer.Handler) string {
	t.Helper()

	rec := doTransferRequest(t, h, "CreateServer", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out struct {
		ServerID string `json:"ServerId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out.ServerID
}

// testSSHHostKeyBody is a syntactically valid (but not cryptographically
// meaningful) ed25519 SSH host key body, sufficient for ImportHostKey which
// does not validate key authenticity.
const testSSHHostKeyBody = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl " +
	"test@example"
