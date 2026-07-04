package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_CognitoIDPAudit_GetSigningCertificate verifies that
// GetSigningCertificate returns a non-empty, PEM-encoded certificate for a real user
// pool and that the certificate is stable across repeated calls.
func TestIntegration_CognitoIDPAudit_GetSigningCertificate(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createCognitoIDPClient(t)

	poolOut, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("audit-cert-pool-" + t.Name()),
	})
	require.NoError(t, err, "CreateUserPool failed")
	require.NotNil(t, poolOut.UserPool)

	poolID := aws.ToString(poolOut.UserPool.Id)
	require.NotEmpty(t, poolID, "pool ID should not be empty")

	certOut, err := client.GetSigningCertificate(ctx, &cognitoidpsdk.GetSigningCertificateInput{
		UserPoolId: aws.String(poolID),
	})
	require.NoError(t, err, "GetSigningCertificate failed")

	cert := aws.ToString(certOut.Certificate)
	require.NotEmpty(t, cert, "signing certificate should not be empty")
	assert.Contains(t, cert, "BEGIN CERTIFICATE", "certificate should be PEM-encoded")
	assert.Contains(t, cert, "END CERTIFICATE", "certificate should be PEM-encoded")

	// The certificate must be stable across repeated calls for the same pool.
	certOut2, err := client.GetSigningCertificate(ctx, &cognitoidpsdk.GetSigningCertificateInput{
		UserPoolId: aws.String(poolID),
	})
	require.NoError(t, err, "second GetSigningCertificate failed")
	assert.Equal(t, cert, aws.ToString(certOut2.Certificate),
		"signing certificate should be stable across calls")
}

// TestIntegration_CognitoIDPAudit_GetSigningCertificateMissingPool verifies that
// GetSigningCertificate returns an error for an unknown user pool.
func TestIntegration_CognitoIDPAudit_GetSigningCertificateMissingPool(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createCognitoIDPClient(t)

	_, err := client.GetSigningCertificate(ctx, &cognitoidpsdk.GetSigningCertificateInput{
		UserPoolId: aws.String("us-east-1_doesnotexist"),
	})
	require.Error(t, err, "GetSigningCertificate should fail for unknown pool")
}
