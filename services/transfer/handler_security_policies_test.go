package transfer_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ListSecurityPolicies(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "ListSecurityPolicies", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DescribeSecurityPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "DescribeSecurityPolicy", map[string]any{
		"SecurityPolicyName": "TransferSecurityPolicy-2020-06",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandler_ListSecurityPoliciesIncludesCurrentNames verifies the catalog contains
// the real, currently-documented AWS Transfer security policy names (server + SFTP
// connector), and does NOT contain gopherstack-invented names that never existed in
// real AWS (e.g. "TransferSecurityPolicy-Connector-*", which used the wrong naming
// pattern -- the real prefix for connector policies is "TransferSFTPConnectorSecurityPolicy-").
func TestHandler_ListSecurityPoliciesIncludesCurrentNames(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "ListSecurityPolicies", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		SecurityPolicyNames []string `json:"SecurityPolicyNames"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	names := make(map[string]bool, len(resp.SecurityPolicyNames))
	for _, n := range resp.SecurityPolicyNames {
		names[n] = true
	}

	for _, want := range []string{
		"TransferSecurityPolicy-2025-03",
		"TransferSecurityPolicy-FIPS-2025-03",
		"TransferSecurityPolicy-2024-01",
		"TransferSecurityPolicy-FIPS-2024-01",
		"TransferSecurityPolicy-2018-11",
		"TransferSFTPConnectorSecurityPolicy-2024-03",
		"TransferSFTPConnectorSecurityPolicy-FIPS-2024-10",
		"TransferSFTPConnectorSecurityPolicy-2023-07",
	} {
		assert.True(t, names[want], "expected real AWS security policy %q in catalog", want)
	}

	for _, unwanted := range []string{
		"TransferSecurityPolicy-Connector-2023-05",
		"TransferSecurityPolicy-FIPS-Connector-2023-05",
		"TransferSecurityPolicy-PQ-SSH-2023-04",
		"TransferSecurityPolicy-PQ-SSH-FIPS-2023-04",
	} {
		assert.False(t, names[unwanted], "gopherstack-invented policy name %q must not appear in the catalog", unwanted)
	}
}

// TestHandler_DescribeSecurityPolicy_ServerShape verifies a SERVER policy's response
// shape: TlsCiphers present, SshHostKeyAlgorithms absent (real DescribedSecurityPolicy
// only populates SshHostKeyAlgorithms for CONNECTOR policies).
func TestHandler_DescribeSecurityPolicy_ServerShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "DescribeSecurityPolicy", map[string]any{
		"SecurityPolicyName": "TransferSecurityPolicy-2025-03",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	pol := resp["SecurityPolicy"].(map[string]any)

	assert.Equal(t, "SERVER", pol["Type"])
	assert.Equal(t, false, pol["Fips"])
	assert.ElementsMatch(t, []any{"SFTP", "FTPS"}, pol["Protocols"])
	assert.NotEmpty(t, pol["TlsCiphers"], "SERVER policies must include TlsCiphers")
	_, hasHostKeyAlgos := pol["SshHostKeyAlgorithms"]
	assert.False(t, hasHostKeyAlgos, "SshHostKeyAlgorithms only applies to CONNECTOR policies")
}

// TestHandler_DescribeSecurityPolicy_ConnectorShape verifies a CONNECTOR policy's
// response shape: SshHostKeyAlgorithms present, TlsCiphers absent (TlsCiphers only
// applies to SERVER policies in real AWS).
func TestHandler_DescribeSecurityPolicy_ConnectorShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "DescribeSecurityPolicy", map[string]any{
		"SecurityPolicyName": "TransferSFTPConnectorSecurityPolicy-2024-03",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	pol := resp["SecurityPolicy"].(map[string]any)

	assert.Equal(t, "CONNECTOR", pol["Type"])
	assert.NotEmpty(t, pol["SshHostKeyAlgorithms"], "CONNECTOR policies must include SshHostKeyAlgorithms")
	_, hasTLS := pol["TlsCiphers"]
	assert.False(t, hasTLS, "TlsCiphers only applies to SERVER policies")
}

// TestHandler_DescribeSecurityPolicy_UnknownName verifies ResourceNotFoundException.
func TestHandler_DescribeSecurityPolicy_UnknownName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doTransferRequest(t, h, "DescribeSecurityPolicy", map[string]any{
		"SecurityPolicyName": "TransferSecurityPolicy-does-not-exist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "ResourceNotFoundException", errResp["__type"])
}
