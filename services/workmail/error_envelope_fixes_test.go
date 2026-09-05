package workmail_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	workmailsdk "github.com/aws/aws-sdk-go-v2/service/workmail"
	"github.com/aws/aws-sdk-go-v2/service/workmail/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workmail"
)

// TestDescribeOrganization_OrganizationNotFound_RealClient drives
// DescribeOrganization for a nonexistent org through the real client.
// DescribeOrganization's own error model (workmail@v1.39.4 deserializers.go
// awsAwsjson11_deserializeOpErrorDescribeOrganization) declares
// OrganizationNotFoundException, not the shared EntityNotFoundException
// sentinel most of this package's not-found checks previously used
// unconditionally (gopherstack-6flj/uox6 error-envelope sweep).
func TestDescribeOrganization_OrganizationNotFound_RealClient(t *testing.T) {
	t.Parallel()

	client := newWorkMailSDKClient(t, workmail.NewHandler(workmail.NewInMemoryBackend("000000000000", "us-east-1")))

	_, err := client.DescribeOrganization(t.Context(), &workmailsdk.DescribeOrganizationInput{
		OrganizationId: aws.String("m-doesnotexist00000000000000000000"),
	})
	require.Error(t, err)

	var apiErr *types.OrganizationNotFoundException
	require.ErrorAs(t, err, &apiErr, "expected a real OrganizationNotFoundException from the SDK deserializer")
}

// TestGetImpersonationRole_ResourceNotFound_RealClient drives
// GetImpersonationRole for a nonexistent role in a real org. GetImpersonationRole's
// own error model declares ResourceNotFoundException for this, not
// EntityNotFoundException.
func TestGetImpersonationRole_ResourceNotFound_RealClient(t *testing.T) {
	t.Parallel()

	client := newWorkMailSDKClient(t, workmail.NewHandler(workmail.NewInMemoryBackend("000000000000", "us-east-1")))
	orgID := newWorkMailOrg(t, client)

	_, err := client.GetImpersonationRole(t.Context(), &workmailsdk.GetImpersonationRoleInput{
		OrganizationId:      orgID,
		ImpersonationRoleId: aws.String("ir-doesnotexist"),
	})
	require.Error(t, err)

	var apiErr *types.ResourceNotFoundException
	require.ErrorAs(t, err, &apiErr, "expected a real ResourceNotFoundException from the SDK deserializer")
}

// TestGetMailDomain_MailDomainNotFound_RealClient drives GetMailDomain for a
// nonexistent domain in a real org. GetMailDomain's own error model declares
// MailDomainNotFoundException for this, not EntityNotFoundException.
func TestGetMailDomain_MailDomainNotFound_RealClient(t *testing.T) {
	t.Parallel()

	client := newWorkMailSDKClient(t, workmail.NewHandler(workmail.NewInMemoryBackend("000000000000", "us-east-1")))
	orgID := newWorkMailOrg(t, client)

	_, err := client.GetMailDomain(t.Context(), &workmailsdk.GetMailDomainInput{
		OrganizationId: orgID,
		DomainName:     aws.String("nope.example"),
	})
	require.Error(t, err)

	var apiErr *types.MailDomainNotFoundException
	require.ErrorAs(t, err, &apiErr, "expected a real MailDomainNotFoundException from the SDK deserializer")
}
