package appconfig_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appconfigsdk "github.com/aws/aws-sdk-go-v2/service/appconfig"
	"github.com/aws/aws-sdk-go-v2/service/appconfig/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListExtensionAssociationsFilter_ByNameAndID proves ExtensionIdentifier
// (api_op_ListExtensionAssociations.go: "The name, the ID, or the Amazon
// Resource Name (ARN) of the extension") narrows the result when given the
// extension's name or ID, not only its ARN.
func TestListExtensionAssociationsFilter_ByNameAndID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestAppConfigClient(t, h)

	extOut, err := client.CreateExtension(t.Context(), &appconfigsdk.CreateExtensionInput{
		Name: aws.String("filter-ext"),
		Actions: map[string][]types.Action{
			"ON_DEPLOYMENT_START": {{Name: aws.String("act"), Uri: aws.String("arn:aws:sns:us-east-1:123456789012:t")}},
		},
	})
	require.NoError(t, err)

	appOut, err := client.CreateApplication(t.Context(), &appconfigsdk.CreateApplicationInput{
		Name: aws.String("filter-ext-app"),
	})
	require.NoError(t, err)

	_, err = client.CreateExtensionAssociation(t.Context(), &appconfigsdk.CreateExtensionAssociationInput{
		ExtensionIdentifier: extOut.Arn,
		ResourceIdentifier:  appOut.Id,
	})
	require.NoError(t, err)

	byARN, err := client.ListExtensionAssociations(t.Context(), &appconfigsdk.ListExtensionAssociationsInput{
		ExtensionIdentifier: extOut.Arn,
	})
	require.NoError(t, err)
	require.Len(t, byARN.Items, 1, "filtering by ARN must already work")

	byName, err := client.ListExtensionAssociations(t.Context(), &appconfigsdk.ListExtensionAssociationsInput{
		ExtensionIdentifier: extOut.Name,
	})
	require.NoError(t, err)
	assert.Len(t, byName.Items, 1, "filtering by extension name must narrow to the association")

	byID, err := client.ListExtensionAssociations(t.Context(), &appconfigsdk.ListExtensionAssociationsInput{
		ExtensionIdentifier: extOut.Id,
	})
	require.NoError(t, err)
	assert.Len(t, byID.Items, 1, "filtering by extension ID must narrow to the association")
}
