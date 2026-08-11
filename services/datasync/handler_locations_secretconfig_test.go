package datasync_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/datasync"
)

// secretConfigLocationCase describes the minimal valid create body for one
// location type that accepts CmkSecretConfig/CustomSecretConfig (botocore
// datasync 2018-11-09 model: CreateLocationAzureBlobRequest,
// CreateLocationFsxWindowsRequest, CreateLocationHdfsRequest,
// CreateLocationObjectStorageRequest, CreateLocationSmbRequest).
type secretConfigLocationCase struct {
	base           map[string]any
	name           string
	createAction   string
	describeAction string
	needsAgent     bool
}

func secretConfigLocationCases() []secretConfigLocationCase {
	return []secretConfigLocationCase{
		{
			name:           "azureblob",
			createAction:   "CreateLocationAzureBlob",
			describeAction: "DescribeLocationAzureBlob",
			needsAgent:     true,
			base: map[string]any{
				"ContainerUrl":       "https://myaccount.blob.core.windows.net/mycontainer",
				"AuthenticationType": "NONE",
			},
		},
		{
			name:           "objectstorage",
			createAction:   "CreateLocationObjectStorage",
			describeAction: "DescribeLocationObjectStorage",
			base: map[string]any{
				"ServerHostname": "objstore.example.com",
				"BucketName":     "my-bucket",
			},
		},
		{
			name:           "fsxwindows",
			createAction:   "CreateLocationFsxWindows",
			describeAction: "DescribeLocationFsxWindows",
			base: map[string]any{
				"FsxFilesystemArn": "arn:aws:fsx:us-east-1:000000000000:file-system/fs-0123456789",
				"User":             "admin",
				"SecurityGroupArns": []string{
					"arn:aws:ec2:us-east-1:000000000000:security-group/sg-0123456789",
				},
			},
		},
		{
			name:           "hdfs",
			createAction:   "CreateLocationHdfs",
			describeAction: "DescribeLocationHdfs",
			needsAgent:     true,
			base: map[string]any{
				"NameNodes": []any{
					map[string]any{"Hostname": "namenode1.example.com", "Port": 8020},
				},
				"AuthenticationType": "SIMPLE",
				"SimpleUser":         "hadoop",
			},
		},
		{
			name:           "smb",
			createAction:   "CreateLocationSmb",
			describeAction: "DescribeLocationSmb",
			needsAgent:     true,
			base: map[string]any{
				"ServerHostname": "smb.example.com",
				"Subdirectory":   "/share",
				"User":           "smbuser",
				"Password":       "smbpass",
			},
		},
	}
}

// secretConfigCreateBody builds the create body for a case, injecting a
// freshly created real agent ARN when the location type requires one --
// AgentArns must reference an agent this backend actually knows about (see
// validateAgentArns), so a hardcoded placeholder ARN would 400.
func secretConfigCreateBody(t *testing.T, h *datasync.Handler, tc secretConfigLocationCase) map[string]any {
	t.Helper()

	body := make(map[string]any, len(tc.base)+1)
	maps.Copy(body, tc.base)

	if tc.needsAgent {
		body["AgentArns"] = []string{createTestAgent(t, h)}
	}

	return body
}

// TestDataSync_SecretConfig_CmkRoundTrip verifies CmkSecretConfig is stored
// and echoed back on Describe for every location type that accepts it on
// input (previously accepted then silently dropped: none of the Create*
// input structs had a CmkSecretConfig field).
func TestDataSync_SecretConfig_CmkRoundTrip(t *testing.T) {
	t.Parallel()

	for _, tc := range secretConfigLocationCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := secretConfigCreateBody(t, h, tc)

			body["CmkSecretConfig"] = map[string]any{
				"SecretArn": "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
				"KmsKeyArn": "arn:aws:kms:us-east-1:000000000000:key/1234abcd",
			}

			rec := doRequest(t, h, tc.createAction, body)
			require.Equal(t, http.StatusOK, rec.Code, "create: %s", rec.Body.String())

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			locArn, ok := createResp["LocationArn"].(string)
			require.True(t, ok)

			rec = doRequest(t, h, tc.describeAction, map[string]any{"LocationArn": locArn})
			require.Equal(t, http.StatusOK, rec.Code, "describe: %s", rec.Body.String())

			var descResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))

			cmk, ok := descResp["CmkSecretConfig"].(map[string]any)
			require.True(t, ok, "CmkSecretConfig missing from describe response: %v", descResp)
			assert.Equal(t, "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret", cmk["SecretArn"])
			assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/1234abcd", cmk["KmsKeyArn"])
			assert.Nil(t, descResp["CustomSecretConfig"])
		})
	}
}

// TestDataSync_SecretConfig_CustomRoundTrip mirrors the Cmk test for
// CustomSecretConfig.
func TestDataSync_SecretConfig_CustomRoundTrip(t *testing.T) {
	t.Parallel()

	for _, tc := range secretConfigLocationCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := secretConfigCreateBody(t, h, tc)

			body["CustomSecretConfig"] = map[string]any{
				"SecretArn":           "arn:aws:secretsmanager:us-east-1:000000000000:secret:custom-secret",
				"SecretAccessRoleArn": "arn:aws:iam::000000000000:role/DataSyncSecretRole",
			}

			rec := doRequest(t, h, tc.createAction, body)
			require.Equal(t, http.StatusOK, rec.Code, "create: %s", rec.Body.String())

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			locArn, ok := createResp["LocationArn"].(string)
			require.True(t, ok)

			rec = doRequest(t, h, tc.describeAction, map[string]any{"LocationArn": locArn})
			require.Equal(t, http.StatusOK, rec.Code, "describe: %s", rec.Body.String())

			var descResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))

			custom, ok := descResp["CustomSecretConfig"].(map[string]any)
			require.True(t, ok, "CustomSecretConfig missing from describe response: %v", descResp)
			assert.Equal(t, "arn:aws:secretsmanager:us-east-1:000000000000:secret:custom-secret", custom["SecretArn"])
			assert.Equal(t, "arn:aws:iam::000000000000:role/DataSyncSecretRole", custom["SecretAccessRoleArn"])
			assert.Nil(t, descResp["CmkSecretConfig"])
		})
	}
}

// TestDataSync_SecretConfig_MutuallyExclusive verifies both CmkSecretConfig
// and CustomSecretConfig on the same request is rejected: "You can use
// either CmkSecretConfig ... or CustomSecretConfig ... Do not provide both
// parameters for the same request" (botocore datasync 2018-11-09 model,
// CmkSecretConfig documentation, repeated on every location type below).
func TestDataSync_SecretConfig_MutuallyExclusive(t *testing.T) {
	t.Parallel()

	for _, tc := range secretConfigLocationCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := secretConfigCreateBody(t, h, tc)

			body["CmkSecretConfig"] = map[string]any{
				"SecretArn": "arn:aws:secretsmanager:us-east-1:000000000000:secret:a",
			}
			body["CustomSecretConfig"] = map[string]any{
				"SecretArn": "arn:aws:secretsmanager:us-east-1:000000000000:secret:b",
			}

			rec := doRequest(t, h, tc.createAction, body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestDataSync_Smb_KerberosAndDNS verifies DnsIpAddresses, KerberosPrincipal,
// KerberosKeytab, and KerberosKrb5Conf are settable on CreateLocationSmb and
// UpdateLocationSmb (botocore datasync 2018-11-09 model,
// CreateLocationSmbRequest/UpdateLocationSmbRequest). KerberosPrincipal and
// DnsIpAddresses round-trip through Describe; KerberosKeytab/KerberosKrb5Conf
// are write-only credential blobs (absent from DescribeLocationSmbResponse
// too, matching AWS).
func TestDataSync_Smb_KerberosAndDNS(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	agentArn := createTestAgent(t, h)

	rec := doRequest(t, h, "CreateLocationSmb", map[string]any{
		"ServerHostname":     "smb.example.com",
		"Subdirectory":       "/share",
		"AuthenticationType": "KERBEROS",
		"AgentArns":          []string{agentArn},
		"DnsIpAddresses":     []string{"10.0.0.1", "10.0.0.2"},
		"KerberosPrincipal":  "HOST/kerberosuser@MYDOMAIN.ORG",
		"KerberosKeytab":     "base64keytabbytes",
		"KerberosKrb5Conf":   "base64krb5conf",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	locArn, ok := createResp["LocationArn"].(string)
	require.True(t, ok)

	rec = doRequest(t, h, "DescribeLocationSmb", map[string]any{"LocationArn": locArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))

	assert.Equal(t, "KERBEROS", descResp["AuthenticationType"])
	assert.Equal(t, "HOST/kerberosuser@MYDOMAIN.ORG", descResp["KerberosPrincipal"])

	dns, ok := descResp["DnsIpAddresses"].([]any)
	require.True(t, ok)
	assert.ElementsMatch(t, []any{"10.0.0.1", "10.0.0.2"}, dns)

	// Write-only credential blobs never come back on Describe.
	assert.Nil(t, descResp["KerberosKeytab"])
	assert.Nil(t, descResp["KerberosKrb5Conf"])

	// Update: DnsIpAddresses/KerberosPrincipal change, credential blobs stay write-only.
	rec = doRequest(t, h, "UpdateLocationSmb", map[string]any{
		"LocationArn":       locArn,
		"DnsIpAddresses":    []string{"10.0.0.9"},
		"KerberosPrincipal": "HOST/other@MYDOMAIN.ORG",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec = doRequest(t, h, "DescribeLocationSmb", map[string]any{"LocationArn": locArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "HOST/other@MYDOMAIN.ORG", descResp["KerberosPrincipal"])

	dns, ok = descResp["DnsIpAddresses"].([]any)
	require.True(t, ok)
	assert.ElementsMatch(t, []any{"10.0.0.9"}, dns)
}

// TestDataSync_Smb_AuthenticationTypeEnum verifies AuthenticationType is
// restricted to AWS's SmbAuthenticationType enum (NTLM, KERBEROS; botocore
// datasync 2018-11-09 model) instead of accepting any string.
func TestDataSync_Smb_AuthenticationTypeEnum(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		authType       string
		expectEchoedAs string
		wantStatusOK   bool
	}{
		{name: "unset defaults to ntlm", authType: "", wantStatusOK: true, expectEchoedAs: "NTLM"},
		{name: "ntlm", authType: "NTLM", wantStatusOK: true, expectEchoedAs: "NTLM"},
		{name: "kerberos", authType: "KERBEROS", wantStatusOK: true, expectEchoedAs: "KERBEROS"},
		{name: "bogus value rejected", authType: "BOGUS", wantStatusOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			agentArn := createTestAgent(t, h)

			body := map[string]any{
				"ServerHostname": "smb.example.com",
				"Subdirectory":   "/share",
				"AgentArns":      []string{agentArn},
			}
			if tt.authType != "" {
				body["AuthenticationType"] = tt.authType
			}

			rec := doRequest(t, h, "CreateLocationSmb", body)

			if !tt.wantStatusOK {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			locArn := createResp["LocationArn"].(string)

			rec = doRequest(t, h, "DescribeLocationSmb", map[string]any{"LocationArn": locArn})
			var descResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
			assert.Equal(t, tt.expectEchoedAs, descResp["AuthenticationType"])
		})
	}
}
