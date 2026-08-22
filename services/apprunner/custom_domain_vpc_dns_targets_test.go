package apprunner_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apprunnersdk "github.com/aws/aws-sdk-go-v2/service/apprunner"
	"github.com/aws/aws-sdk-go-v2/service/apprunner/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apprunner"
)

// TestAssociateDisassociateCustomDomain_VpcDNSTargetsPresent verifies that
// AssociateCustomDomainOutput and DisassociateCustomDomainOutput carry the
// VpcDNSTargets key the real deserializers.go:7705/8462 document
// deserializers switch on. Omitting the key entirely (as opposed to sending
// an empty array) leaves the real client's out.VpcDNSTargets nil instead of
// a non-nil empty slice, since the real deserializeDocumentVpcDNSTargetList
// is only invoked when the "VpcDNSTargets" case matches.
func TestAssociateDisassociateCustomDomain_VpcDNSTargetsPresent(t *testing.T) {
	t.Parallel()

	backend := apprunner.NewInMemoryBackend("000000000000", apprunnerTagsRTRegion)
	client := newTestAppRunnerClient(t, apprunner.NewHandler(backend))
	ctx := t.Context()

	svc, err := client.CreateService(ctx, &apprunnersdk.CreateServiceInput{
		ServiceName: aws.String("vpc-dns-targets-svc"),
		SourceConfiguration: &types.SourceConfiguration{
			ImageRepository: &types.ImageRepository{
				ImageIdentifier:     aws.String("public.ecr.aws/nginx/nginx:latest"),
				ImageRepositoryType: types.ImageRepositoryTypeEcrPublic,
			},
		},
	})
	require.NoError(t, err)

	assocOut, err := client.AssociateCustomDomain(ctx, &apprunnersdk.AssociateCustomDomainInput{
		ServiceArn: svc.Service.ServiceArn,
		DomainName: aws.String("example.com"),
	})
	require.NoError(t, err)
	assert.NotNil(t, assocOut.VpcDNSTargets, "Associate must emit VpcDNSTargets for a non-nil client-side slice")
	assert.Empty(t, assocOut.VpcDNSTargets)

	disassocOut, err := client.DisassociateCustomDomain(ctx, &apprunnersdk.DisassociateCustomDomainInput{
		ServiceArn: svc.Service.ServiceArn,
		DomainName: aws.String("example.com"),
	})
	require.NoError(t, err)
	assert.NotNil(t, disassocOut.VpcDNSTargets, "Disassociate must emit VpcDNSTargets for a non-nil client-side slice")
	assert.Empty(t, disassocOut.VpcDNSTargets)
}
