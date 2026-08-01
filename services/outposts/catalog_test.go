package outposts_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/aws/aws-sdk-go-v2/service/outposts/types"
	"github.com/stretchr/testify/require"
)

func TestGetCatalogItem(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	out, err := client.GetCatalogItem(t.Context(), &outpostssdk.GetCatalogItemInput{
		CatalogItemId: aws.String("cat-rack-m5"),
	})
	require.NoError(t, err)
	require.Equal(t, "cat-rack-m5", aws.ToString(out.CatalogItem.CatalogItemId))
	require.NotEmpty(t, out.CatalogItem.EC2Capacities)
	// Quantity/MaxSize are strings on the real wire type, not numbers.
	require.NotEmpty(t, aws.ToString(out.CatalogItem.EC2Capacities[0].Quantity))
}

func TestGetCatalogItem_NotFound(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	_, err := client.GetCatalogItem(t.Context(), &outpostssdk.GetCatalogItemInput{
		CatalogItemId: aws.String("does-not-exist"),
	})
	require.Error(t, err)

	var nfe *types.NotFoundException
	require.ErrorAs(t, err, &nfe)
}

func TestListCatalogItems_FiltersByItemClass(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	rackOnly, err := client.ListCatalogItems(t.Context(), &outpostssdk.ListCatalogItemsInput{
		ItemClassFilter: []types.CatalogItemClass{types.CatalogItemClassRack},
	})
	require.NoError(t, err)
	require.NotEmpty(t, rackOnly.CatalogItems)

	serverOnly, err := client.ListCatalogItems(t.Context(), &outpostssdk.ListCatalogItemsInput{
		ItemClassFilter: []types.CatalogItemClass{types.CatalogItemClassServer},
	})
	require.NoError(t, err)
	require.NotEmpty(t, serverOnly.CatalogItems)
}

func TestListOrderableInstanceTypes(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	out, err := client.ListOrderableInstanceTypes(t.Context(), &outpostssdk.ListOrderableInstanceTypesInput{})
	require.NoError(t, err)
	require.NotEmpty(t, out.InstanceTypes)

	gen1, err := client.ListOrderableInstanceTypes(t.Context(), &outpostssdk.ListOrderableInstanceTypesInput{
		OutpostGenerationFilter: types.OutpostGenerationGeneration1,
	})
	require.NoError(t, err)
	require.NotEmpty(t, gen1.InstanceTypes)

	for _, it := range gen1.InstanceTypes {
		require.NotEmpty(t, it.FormFactorConfigs)
		require.Equal(t, types.OutpostGenerationGeneration1, it.FormFactorConfigs[0].OutpostGeneration)
	}
}
