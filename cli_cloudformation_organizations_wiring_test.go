package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/aws/aws-sdk-go-v2/service/organizations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudFormationImportStacksStampsOrganizationalUnit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		moveIn  bool
		wantErr bool
	}{
		{name: "account_in_ou", moveIn: true},
		{name: "account_outside_ou", moveIn: false, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			orgs := organizations.NewFromConfig(fx.cfg)
			cfn := cloudformation.NewFromConfig(fx.cfg)

			_, err := orgs.CreateOrganization(t.Context(), &organizations.CreateOrganizationInput{})
			require.NoError(t, err)

			roots, err := orgs.ListRoots(t.Context(), &organizations.ListRootsInput{})
			require.NoError(t, err)
			require.NotEmpty(t, roots.Roots)

			rootID := aws.ToString(roots.Roots[0].Id)

			ou, err := orgs.CreateOrganizationalUnit(t.Context(), &organizations.CreateOrganizationalUnitInput{
				ParentId: aws.String(rootID), Name: aws.String("Workloads"),
			})
			require.NoError(t, err)

			ouID := aws.ToString(ou.OrganizationalUnit.Id)

			acct, err := orgs.CreateAccount(t.Context(), &organizations.CreateAccountInput{
				AccountName: aws.String("member"), Email: aws.String("member@example.com"),
			})
			require.NoError(t, err)

			accountID := aws.ToString(acct.CreateAccountStatus.AccountId)

			if tt.moveIn {
				_, err = orgs.MoveAccount(t.Context(), &organizations.MoveAccountInput{
					AccountId: aws.String(accountID), SourceParentId: aws.String(rootID),
					DestinationParentId: aws.String(ouID),
				})
				require.NoError(t, err)
			}

			_, err = cfn.ActivateOrganizationsAccess(t.Context(), &cloudformation.ActivateOrganizationsAccessInput{})
			require.NoError(t, err)

			_, err = cfn.CreateStackSet(t.Context(), &cloudformation.CreateStackSetInput{
				StackSetName: aws.String("imp"), PermissionModel: cfntypes.PermissionModelsServiceManaged,
				TemplateBody:   aws.String(`{"Resources":{"Q":{"Type":"AWS::SQS::Queue"}}}`),
				AutoDeployment: &cfntypes.AutoDeployment{Enabled: aws.Bool(true)},
			})
			require.NoError(t, err)

			_, err = cfn.ImportStacksToStackSet(t.Context(), &cloudformation.ImportStacksToStackSetInput{
				StackSetName:          aws.String("imp"),
				StackIds:              []string{"arn:aws:cloudformation:us-east-1:" + accountID + ":stack/s/abc"},
				OrganizationalUnitIds: []string{ouID},
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			list, err := cfn.ListStackInstances(t.Context(), &cloudformation.ListStackInstancesInput{
				StackSetName: aws.String("imp"),
			})
			require.NoError(t, err)
			require.Len(t, list.Summaries, 1)
			assert.Equal(t, ouID, aws.ToString(list.Summaries[0].OrganizationalUnitId))
		})
	}
}
