package lakeformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lakeformationsdk "github.com/aws/aws-sdk-go-v2/service/lakeformation"
	"github.com/aws/aws-sdk-go-v2/service/lakeformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListPermissions_IncludeRelated(t *testing.T) {
	t.Parallel()

	const (
		tableOwner = "arn:aws:iam::123456789012:role/owner"
		filterUser = "arn:aws:iam::123456789012:role/reader"
	)

	tests := []struct {
		name           string
		includeRelated string
		wantResources  int
		withPrincipal  bool
		wantErr        bool
	}{
		{name: "default_excludes_cell_filters", wantResources: 1},
		{name: "false_excludes_cell_filters", includeRelated: "FALSE", wantResources: 1},
		{name: "true_includes_cell_filters", includeRelated: "TRUE", wantResources: 2},
		{name: "true_with_principal_rejected", includeRelated: "TRUE", withPrincipal: true, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			ctx := t.Context()

			table := &types.Resource{Table: &types.TableResource{
				DatabaseName: aws.String("db"), Name: aws.String("tbl"),
			}}
			filter := &types.Resource{DataCellsFilter: &types.DataCellsFilterResource{
				DatabaseName: aws.String("db"), TableName: aws.String("tbl"), Name: aws.String("f"),
				TableCatalogId: aws.String("123456789012"),
			}}

			for res, principal := range map[*types.Resource]string{table: tableOwner, filter: filterUser} {
				_, err := client.GrantPermissions(ctx, &lakeformationsdk.GrantPermissionsInput{
					Principal:   &types.DataLakePrincipal{DataLakePrincipalIdentifier: aws.String(principal)},
					Resource:    res,
					Permissions: []types.Permission{types.PermissionSelect},
				})
				require.NoError(t, err)
			}

			in := &lakeformationsdk.ListPermissionsInput{Resource: table}
			if tt.includeRelated != "" {
				in.IncludeRelated = aws.String(tt.includeRelated)
			}

			if tt.withPrincipal {
				in.Principal = &types.DataLakePrincipal{DataLakePrincipalIdentifier: aws.String(tableOwner)}
			}

			out, err := client.ListPermissions(ctx, in)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.PrincipalResourcePermissions, tt.wantResources)
		})
	}
}
