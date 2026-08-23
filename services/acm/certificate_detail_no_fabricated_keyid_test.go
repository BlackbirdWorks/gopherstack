package acm_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestACMHandler_DescribeCertificate_NoFabricatedKeyId locks in the removal
// of a "KeyId" member that DescribeCertificate's response previously
// fabricated on the Certificate object. Real AWS's CertificateDetail
// deserializer (acm@v1.43.4 deserializers.go:6456-6768) has no "KeyId" case
// at all -- that key belongs exclusively to
// GetAcmeExternalAccountBindingCredentialsOutput (deserializers.go:10053).
// Since types.CertificateDetail has no KeyId field, the real SDK client
// can't observe the leak either way (it just silently ignores unknown
// keys), so a raw-body absence assertion is the only instrument that can
// catch this -- a typed-client round trip would pass whether or not the
// key were present.
func TestACMHandler_DescribeCertificate_NoFabricatedKeyId(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	reqRec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"no-fabricated-keyid.example.com"}`)
	require.Equal(t, http.StatusOK, reqRec.Code)

	var reqOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

	descBody, err := json.Marshal(map[string]string{"CertificateArn": reqOut.CertificateArn})
	require.NoError(t, err)

	descRec := postACMJSON(t, h, "DescribeCertificate", string(descBody))
	require.Equal(t, http.StatusOK, descRec.Code)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &raw))

	var certRaw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw["Certificate"], &certRaw))

	_, hasKeyID := certRaw["KeyId"]
	require.False(t, hasKeyID, "DescribeCertificate must not emit a fabricated KeyId member on Certificate")
}
