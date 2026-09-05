package directoryservice_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	directoryservicesdk "github.com/aws/aws-sdk-go-v2/service/directoryservice"
	directoryservicetypes "github.com/aws/aws-sdk-go-v2/service/directoryservice/types"
	"github.com/stretchr/testify/require"
)

// TestListCertificates_LimitTruncates verifies the real client's Limit field
// (directoryservice@v1.41.4 api_op_ListCertificates.go) truncates the
// returned certificates. gopherstack read a wire key "PageSize" that the SDK
// never sends -- the real field is "Limit" -- so Limit was silently ignored.
func TestListCertificates_LimitTruncates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestDirectoryServiceClient(t, h)

	dirID := mustCreateMicrosoftAD(t, h, "lfp-certs.example.com")

	for range 4 {
		_, err := client.RegisterCertificate(t.Context(), &directoryservicesdk.RegisterCertificateInput{
			DirectoryId:     aws.String(dirID),
			CertificateData: aws.String(testCertPEM),
			Type:            "ClientLDAPS",
		})
		require.NoError(t, err)
	}

	out, err := client.ListCertificates(t.Context(), &directoryservicesdk.ListCertificatesInput{
		DirectoryId: aws.String(dirID),
		Limit:       aws.Int32(2),
	})
	require.NoError(t, err)

	require.Len(t, out.CertificatesInfo, 2)
	require.NotNil(t, out.NextToken)
}

// TestListADAssessments_LimitTruncates verifies Limit
// (directoryservice@v1.41.4 api_op_ListADAssessments.go) truncates the
// returned assessments -- same wrong-key bug as ListCertificates.
func TestListADAssessments_LimitTruncates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestDirectoryServiceClient(t, h)

	dirID := mustCreateSimpleAD(t, h, "lfp-assess.example.com")

	for range 3 {
		_, err := client.StartADAssessment(t.Context(), &directoryservicesdk.StartADAssessmentInput{
			DirectoryId: aws.String(dirID),
		})
		require.NoError(t, err)
	}

	out, err := client.ListADAssessments(t.Context(), &directoryservicesdk.ListADAssessmentsInput{
		DirectoryId: aws.String(dirID),
		Limit:       aws.Int32(2),
	})
	require.NoError(t, err)

	require.Len(t, out.Assessments, 2)
	require.NotNil(t, out.NextToken)
}

// TestDescribeClientAuthenticationSettings_LimitTruncates verifies Limit
// (directoryservice@v1.41.4 api_op_DescribeClientAuthenticationSettings.go)
// truncates the returned settings -- same wrong-key bug.
func TestDescribeClientAuthenticationSettings_LimitTruncates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestDirectoryServiceClient(t, h)

	dirID := mustCreateMicrosoftAD(t, h, "lfp-clientauth.example.com")

	for _, authType := range []directoryservicetypes.ClientAuthenticationType{
		directoryservicetypes.ClientAuthenticationTypeSmartCard,
		directoryservicetypes.ClientAuthenticationTypeSmartCardOrPassword,
	} {
		_, err := client.EnableClientAuthentication(t.Context(), &directoryservicesdk.EnableClientAuthenticationInput{
			DirectoryId: aws.String(dirID),
			Type:        authType,
		})
		require.NoError(t, err)
	}

	out, err := client.DescribeClientAuthenticationSettings(
		t.Context(),
		&directoryservicesdk.DescribeClientAuthenticationSettingsInput{
			DirectoryId: aws.String(dirID),
			Limit:       aws.Int32(1),
		},
	)
	require.NoError(t, err)

	require.Len(t, out.ClientAuthenticationSettingsInfo, 1)
}
