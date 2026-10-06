package outposts_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	outpostssdk "github.com/aws/aws-sdk-go-v2/service/outposts"
	"github.com/aws/aws-sdk-go-v2/service/outposts/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListCatalogItems_FilterMembers proves ItemClass/EC2Family/SupportedStorage filters narrow the result.
func TestListCatalogItems_FilterMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   *outpostssdk.ListCatalogItemsInput
		name string
		want []string
	}{
		{
			name: "item_class_server",
			in: &outpostssdk.ListCatalogItemsInput{
				ItemClassFilter: []types.CatalogItemClass{types.CatalogItemClassServer},
			},
			want: []string{"OR-SRVC6ID"},
		},
		{
			name: "ec2_family_c5",
			in:   &outpostssdk.ListCatalogItemsInput{EC2FamilyFilter: []string{"c5"}},
			want: []string{"OR-RACKC05"},
		},
		{
			name: "supported_storage_s3",
			in: &outpostssdk.ListCatalogItemsInput{
				SupportedStorageFilter: []types.SupportedStorageEnum{types.SupportedStorageEnumS3},
			},
			want: []string{"OR-RACKM05"},
		},
		{
			name: "ec2_family_miss",
			in:   &outpostssdk.ListCatalogItemsInput{EC2FamilyFilter: []string{"nope"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)

			out, err := client.ListCatalogItems(t.Context(), tt.in)
			require.NoError(t, err)

			got := make([]string, 0, len(out.CatalogItems))
			for _, c := range out.CatalogItems {
				got = append(got, aws.ToString(c.CatalogItemId))
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
