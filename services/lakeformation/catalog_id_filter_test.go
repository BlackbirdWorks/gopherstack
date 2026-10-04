package lakeformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lakeformationsdk "github.com/aws/aws-sdk-go-v2/service/lakeformation"
	"github.com/aws/aws-sdk-go-v2/service/lakeformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const lfOtherCatalog = "111111111111"

// ListPermissionsInput.CatalogId / GetEffectivePermissionsForPathInput.CatalogId scope results to one catalog.
func TestPermissions_CatalogIDScoping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		catalogID *string
		name      string
		wantDBs   []string
	}{
		{name: "unset-lists-all", wantDBs: []string{"own", "other"}},
		{name: "account-catalog", catalogID: aws.String(testAccountID), wantDBs: []string{"own"}},
		{name: "other-catalog", catalogID: aws.String(lfOtherCatalog), wantDBs: []string{"other"}},
		{name: "unknown-catalog", catalogID: aws.String("999999999999"), wantDBs: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestLakeFormationClient(t, newTestHandler())
			ctx := t.Context()
			principal := &types.DataLakePrincipal{
				DataLakePrincipalIdentifier: aws.String("arn:aws:iam::123456789012:role/r"),
			}

			for name, cat := range map[string]*string{"own": nil, "other": aws.String(lfOtherCatalog)} {
				_, err := client.GrantPermissions(ctx, &lakeformationsdk.GrantPermissionsInput{
					Principal:   principal,
					Permissions: []types.Permission{types.PermissionDescribe},
					Resource: &types.Resource{
						Database: &types.DatabaseResource{Name: aws.String(name), CatalogId: cat},
					},
				})
				require.NoError(t, err)
			}

			out, err := client.ListPermissions(ctx, &lakeformationsdk.ListPermissionsInput{CatalogId: tt.catalogID})
			require.NoError(t, err)

			got := []string{}
			for _, p := range out.PrincipalResourcePermissions {
				got = append(got, aws.ToString(p.Resource.Database.Name))
			}

			assert.ElementsMatch(t, tt.wantDBs, got)
		})
	}

	t.Run("effective-permissions-for-path", func(t *testing.T) {
		t.Parallel()

		client := newTestLakeFormationClient(t, newTestHandler())
		ctx := t.Context()
		const loc = "arn:aws:s3:::lf-bucket"

		_, err := client.GrantPermissions(ctx, &lakeformationsdk.GrantPermissionsInput{
			Principal: &types.DataLakePrincipal{
				DataLakePrincipalIdentifier: aws.String("arn:aws:iam::123456789012:role/r"),
			},
			Permissions: []types.Permission{types.PermissionDataLocationAccess},
			Resource: &types.Resource{
				DataLocation: &types.DataLocationResource{
					ResourceArn: aws.String(loc),
					CatalogId:   aws.String(lfOtherCatalog),
				},
			},
		})
		require.NoError(t, err)

		hit, err := client.GetEffectivePermissionsForPath(ctx, &lakeformationsdk.GetEffectivePermissionsForPathInput{
			ResourceArn: aws.String(loc), CatalogId: aws.String(lfOtherCatalog),
		})
		require.NoError(t, err)
		assert.Len(t, hit.Permissions, 1)

		miss, err := client.GetEffectivePermissionsForPath(ctx, &lakeformationsdk.GetEffectivePermissionsForPathInput{
			ResourceArn: aws.String(loc), CatalogId: aws.String(testAccountID),
		})
		require.NoError(t, err)
		assert.Empty(t, miss.Permissions)
	})
}
