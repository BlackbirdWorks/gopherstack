package organizations_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	orgsdk "github.com/aws/aws-sdk-go-v2/service/organizations"
	"github.com/aws/aws-sdk-go-v2/service/organizations/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateOrganizationalUnit_OmittedNameKeepsName(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		newName  *string
		wantName string
	}{
		{name: "omitted name", wantName: "eng"},
		{name: "supplied name", newName: aws.String("platform"), wantName: "platform"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestOrganizationsClient(t, newTestHandler(t))
			ctx := t.Context()

			_, err := c.CreateOrganization(ctx, &orgsdk.CreateOrganizationInput{})
			require.NoError(t, err)

			roots, err := c.ListRoots(ctx, &orgsdk.ListRootsInput{})
			require.NoError(t, err)

			ou, err := c.CreateOrganizationalUnit(ctx, &orgsdk.CreateOrganizationalUnitInput{
				ParentId: roots.Roots[0].Id, Name: aws.String("eng"),
			})
			require.NoError(t, err)

			upd, err := c.UpdateOrganizationalUnit(ctx, &orgsdk.UpdateOrganizationalUnitInput{
				OrganizationalUnitId: ou.OrganizationalUnit.Id, Name: tc.newName,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.wantName, aws.ToString(upd.OrganizationalUnit.Name))

			got, err := c.DescribeOrganizationalUnit(ctx, &orgsdk.DescribeOrganizationalUnitInput{
				OrganizationalUnitId: ou.OrganizationalUnit.Id,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.wantName, aws.ToString(got.OrganizationalUnit.Name))
		})
	}
}

func TestListAccounts_ReportsAccountState(t *testing.T) {
	t.Parallel()

	c := newTestOrganizationsClient(t, newTestHandler(t))
	ctx := t.Context()

	_, err := c.CreateOrganization(ctx, &orgsdk.CreateOrganizationInput{})
	require.NoError(t, err)

	out, err := c.ListAccounts(ctx, &orgsdk.ListAccountsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, out.Accounts)

	for _, a := range out.Accounts {
		assert.Equal(t, types.AccountStateActive, a.State)
	}
}
