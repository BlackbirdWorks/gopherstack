package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	acmsdk "github.com/aws/aws-sdk-go-v2/service/acm"
	acmtypes "github.com/aws/aws-sdk-go-v2/service/acm/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_ACM_CertificateValidatedWaiter verifies that CertificateValidatedWaiter
// succeeds immediately after RequestCertificate because the status is ISSUED.
func TestIntegration_ACM_CertificateValidatedWaiter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createACMClient(t)
	ctx := t.Context()

	reqOut, err := client.RequestCertificate(ctx, &acmsdk.RequestCertificateInput{
		DomainName:       aws.String("waiter-test.example.com"),
		ValidationMethod: acmtypes.ValidationMethodDns,
	})
	require.NoError(t, err)
	require.NotEmpty(t, reqOut.CertificateArn)

	certARN := aws.ToString(reqOut.CertificateArn)

	t.Cleanup(func() {
		_, _ = client.DeleteCertificate(ctx, &acmsdk.DeleteCertificateInput{CertificateArn: aws.String(certARN)})
	})

	// Verify the certificate status is ISSUED immediately
	descOut, err := client.DescribeCertificate(ctx, &acmsdk.DescribeCertificateInput{
		CertificateArn: aws.String(certARN),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Certificate)
	assert.Equal(t, acmtypes.CertificateStatusIssued, descOut.Certificate.Status)

	waiter := acmsdk.NewCertificateValidatedWaiter(client)
	start := time.Now()
	err = waiter.Wait(ctx, &acmsdk.DescribeCertificateInput{
		CertificateArn: aws.String(certARN),
	}, 5*time.Second)
	elapsed := time.Since(start)

	require.NoError(t, err, "CertificateValidatedWaiter should succeed because certificate is already ISSUED")
	assert.Less(t, elapsed, 2*time.Second, "CertificateValidatedWaiter should complete quickly, took %v", elapsed)
}
