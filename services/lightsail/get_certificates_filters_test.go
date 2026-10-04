package lightsail_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lightsailsdk "github.com/aws/aws-sdk-go-v2/service/lightsail"
	lightsailtypes "github.com/aws/aws-sdk-go-v2/service/lightsail/types"
	"github.com/stretchr/testify/require"
)

// TestGetCertificates_StatusFilter pins api_op_GetCertificates.go: CertificateStatuses
// narrows to the listed statuses, omitted means every status.
func TestGetCertificates_StatusFilter(t *testing.T) {
	t.Parallel()

	issued := lightsailtypes.CertificateStatusIssued
	failed := lightsailtypes.CertificateStatusFailed

	tests := []struct {
		name      string
		statuses  []lightsailtypes.CertificateStatus
		wantCount int
	}{
		{name: "issued", statuses: []lightsailtypes.CertificateStatus{issued}, wantCount: 1},
		{name: "unmatched_status", statuses: []lightsailtypes.CertificateStatus{failed}, wantCount: 0},
		{name: "multiple_statuses", statuses: []lightsailtypes.CertificateStatus{failed, issued}, wantCount: 1},
		{name: "omitted_all", wantCount: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t)
			ctx := t.Context()

			_, err := client.CreateCertificate(ctx, &lightsailsdk.CreateCertificateInput{
				CertificateName: aws.String("c1"), DomainName: aws.String("example.com"),
			})
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				out, getErr := client.GetCertificates(ctx, &lightsailsdk.GetCertificatesInput{})

				return getErr == nil && len(out.Certificates) == 1 &&
					out.Certificates[0].CertificateDetail.Status == issued
			}, defaultAsyncWait, defaultAsyncPoll, "certificate never issued")

			out, err := client.GetCertificates(ctx, &lightsailsdk.GetCertificatesInput{
				CertificateStatuses: tc.statuses,
			})
			require.NoError(t, err)
			require.Len(t, out.Certificates, tc.wantCount)
		})
	}
}
