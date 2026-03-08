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
// succeeds after the certificate auto-validates from PENDING_VALIDATION to ISSUED.
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

	// Certificate starts in PENDING_VALIDATION with DNS validation
	descOut, err := client.DescribeCertificate(ctx, &acmsdk.DescribeCertificateInput{
		CertificateArn: aws.String(certARN),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Certificate)

	// Status is either PENDING_VALIDATION (immediately after request) or ISSUED (after auto-validate)
	status := descOut.Certificate.Status
	assert.True(t,
		status == acmtypes.CertificateStatusPendingValidation || status == acmtypes.CertificateStatusIssued,
		"expected PENDING_VALIDATION or ISSUED, got %s", status)

	// Waiter polls until ISSUED; the mock auto-validates within ~100ms.
	// Use a lower MinDelay so the waiter can retry quickly if the cert is still
	// PENDING_VALIDATION on the first poll (race between goroutine and test).
	waiter := acmsdk.NewCertificateValidatedWaiter(client, func(o *acmsdk.CertificateValidatedWaiterOptions) {
		o.MinDelay = 500 * time.Millisecond
		o.MaxDelay = 2 * time.Second
	})
	start := time.Now()
	err = waiter.Wait(ctx, &acmsdk.DescribeCertificateInput{
		CertificateArn: aws.String(certARN),
	}, 10*time.Second)
	elapsed := time.Since(start)

	require.NoError(t, err, "CertificateValidatedWaiter should succeed after auto-validation")
	assert.Less(t, elapsed, 5*time.Second, "CertificateValidatedWaiter should complete quickly, took %v", elapsed)
}
