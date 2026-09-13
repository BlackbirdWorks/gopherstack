package apigateway_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewaysdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	agwtypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

// TestSlice7_GetDomainNameAccessAssociations_Pagination covers
// gopherstack-xhu2t slice 7: GetDomainNameAccessAssociationsInput.Limit
// (httpQuery, apigateway@v1.42.4 serializers.go:5224-5226) was undeclared,
// so a real client's Limit was silently ignored and every call returned the
// full unpaged set with no Position token.
func TestSlice7_GetDomainNameAccessAssociations_Pagination(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	client := newTestAPIGatewayClient(t, apigateway.NewHandler(b))

	for i := range 3 {
		suffix := string(rune('a' + i))
		_, err := client.CreateDomainNameAccessAssociation(
			t.Context(),
			&apigatewaysdk.CreateDomainNameAccessAssociationInput{
				DomainNameArn:               aws.String("arn:aws:apigateway:us-east-1::/domainnames/d" + suffix),
				AccessAssociationSource:     aws.String("vpce-" + suffix),
				AccessAssociationSourceType: agwtypes.AccessAssociationSourceTypeVpce,
			},
		)
		require.NoError(t, err)
	}

	first, err := client.GetDomainNameAccessAssociations(
		t.Context(),
		&apigatewaysdk.GetDomainNameAccessAssociationsInput{Limit: aws.Int32(2)},
	)
	require.NoError(t, err)
	assert.Len(t, first.Items, 2, "Limit must truncate the page")
	require.NotNil(t, first.Position)
	assert.NotEmpty(t, *first.Position)

	second, err := client.GetDomainNameAccessAssociations(
		t.Context(),
		&apigatewaysdk.GetDomainNameAccessAssociationsInput{
			Limit:    aws.Int32(2),
			Position: first.Position,
		},
	)
	require.NoError(t, err)
	assert.Len(t, second.Items, 1, "remaining item must be on the second page")
}

// TestSlice7_GetSdkTypes_Limit covers gopherstack-xhu2t slice 7:
// GetSdkTypesInput.Limit (httpQuery, apigateway@v1.42.4
// serializers.go:6891-6893) was undeclared, so it never truncated the fixed
// SDK type catalog.
func TestSlice7_GetSdkTypes_Limit(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	client := newTestAPIGatewayClient(t, apigateway.NewHandler(b))

	full, err := client.GetSdkTypes(t.Context(), &apigatewaysdk.GetSdkTypesInput{})
	require.NoError(t, err)
	require.Greater(t, len(full.Items), 2, "catalog must have more than 2 entries for this test to be meaningful")

	limited, err := client.GetSdkTypes(t.Context(), &apigatewaysdk.GetSdkTypesInput{Limit: aws.Int32(2)})
	require.NoError(t, err)
	assert.Len(t, limited.Items, 2)
}
