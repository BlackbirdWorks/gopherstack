package lakeformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lakeformationsdk "github.com/aws/aws-sdk-go-v2/service/lakeformation"
	"github.com/aws/aws-sdk-go-v2/service/lakeformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

func TestGrantRevoke_CatalogIDDefaultsResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		batch bool
	}{
		{name: "single"},
		{name: "batch", batch: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestLakeFormationClient(t, newTestHandler())
			ctx := t.Context()
			principal := &types.DataLakePrincipal{
				DataLakePrincipalIdentifier: aws.String("arn:aws:iam::123456789012:role/r"),
			}
			resource := &types.Resource{Database: &types.DatabaseResource{Name: aws.String("db")}}

			if tt.batch {
				out, err := client.BatchGrantPermissions(ctx, &lakeformationsdk.BatchGrantPermissionsInput{
					CatalogId: aws.String(lfOtherCatalog),
					Entries: []types.BatchPermissionsRequestEntry{{
						Id: aws.String("1"), Principal: principal, Resource: resource,
						Permissions: []types.Permission{types.PermissionDescribe},
					}},
				})
				require.NoError(t, err)
				require.Empty(t, out.Failures)
			} else {
				_, err := client.GrantPermissions(ctx, &lakeformationsdk.GrantPermissionsInput{
					CatalogId: aws.String(lfOtherCatalog), Principal: principal, Resource: resource,
					Permissions: []types.Permission{types.PermissionDescribe},
				})
				require.NoError(t, err)
			}

			listIn := func(catalog string) int {
				out, err := client.ListPermissions(
					ctx,
					&lakeformationsdk.ListPermissionsInput{CatalogId: aws.String(catalog)},
				)
				require.NoError(t, err)

				return len(out.PrincipalResourcePermissions)
			}

			assert.Equal(t, 1, listIn(lfOtherCatalog))
			assert.Equal(t, 0, listIn(testAccountID))

			if tt.batch {
				_, err := client.BatchRevokePermissions(ctx, &lakeformationsdk.BatchRevokePermissionsInput{
					CatalogId: aws.String(lfOtherCatalog),
					Entries: []types.BatchPermissionsRequestEntry{{
						Id: aws.String("1"), Principal: principal, Resource: resource,
						Permissions: []types.Permission{types.PermissionDescribe},
					}},
				})
				require.NoError(t, err)
			} else {
				_, err := client.RevokePermissions(ctx, &lakeformationsdk.RevokePermissionsInput{
					CatalogId: aws.String(lfOtherCatalog), Principal: principal, Resource: resource,
					Permissions: []types.Permission{types.PermissionDescribe},
				})
				require.NoError(t, err)
			}

			assert.Equal(t, 0, listIn(lfOtherCatalog))
		})
	}
}

func TestDataLakeSettings_PerCatalog(t *testing.T) {
	t.Parallel()

	admin := func(id string) *types.DataLakeSettings {
		return &types.DataLakeSettings{DataLakeAdmins: []types.DataLakePrincipal{
			{DataLakePrincipalIdentifier: aws.String(id)},
		}}
	}

	tests := []struct {
		putCatalog *string
		getCatalog *string
		name       string
		wantAdmins int
	}{
		{name: "default_catalog", wantAdmins: 1},
		{name: "own_account_is_default", putCatalog: aws.String(testAccountID), wantAdmins: 1},
		{name: "other_catalog_isolated", putCatalog: aws.String(lfOtherCatalog), wantAdmins: 0},
		{
			name: "other_catalog_readable", putCatalog: aws.String(lfOtherCatalog),
			getCatalog: aws.String(lfOtherCatalog), wantAdmins: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := lakeformation.NewInMemoryBackend()
			client := newTestLakeFormationClient(t, handlerFor(backend))
			ctx := t.Context()

			_, err := client.PutDataLakeSettings(ctx, &lakeformationsdk.PutDataLakeSettingsInput{
				CatalogId: tt.putCatalog, DataLakeSettings: admin("arn:aws:iam::123456789012:role/a"),
			})
			require.NoError(t, err)

			snap, err := backend.Snapshot()
			require.NoError(t, err)

			restored := lakeformation.NewInMemoryBackend()
			require.NoError(t, restored.Restore(ctx, snap))
			client = newTestLakeFormationClient(t, handlerFor(restored))

			out, err := client.GetDataLakeSettings(
				ctx,
				&lakeformationsdk.GetDataLakeSettingsInput{CatalogId: tt.getCatalog},
			)
			require.NoError(t, err)
			assert.Len(t, out.DataLakeSettings.DataLakeAdmins, tt.wantAdmins)
		})
	}
}

func handlerFor(b *lakeformation.InMemoryBackend) *lakeformation.Handler {
	h := lakeformation.NewHandler(b)
	h.AccountID = testAccountID

	return h
}
