package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	"github.com/blackbirdworks/gopherstack/services/organizations"
)

// accountFilterFixture wires a SERVICE_MANAGED StackSet to a real
// Organizations backend with three accounts under one OU plus a fourth
// account outside it, so DeploymentTargets.AccountFilterType's four modes
// (NONE/INTERSECTION/DIFFERENCE/UNION,
// docs.aws.amazon.com/AWSCloudFormation/latest/APIReference/API_DeploymentTargets.html)
// each produce a distinguishable target set.
type accountFilterFixture struct {
	client         *cfnsdk.Client
	stackSetName   string
	ouID           string
	outsideAccount string
	ouAccounts     []string
}

func newAccountFilterFixture(t *testing.T, stackSetName string) accountFilterFixture {
	t.Helper()

	orgBackend := organizations.NewInMemoryBackend("000000000000", "us-east-1")
	_, root, err := orgBackend.CreateOrganization("ALL")
	require.NoError(t, err)

	ou, err := orgBackend.CreateOrganizationalUnit(root.ID, "Workloads", nil)
	require.NoError(t, err)

	ouAccounts := make([]string, 0, 3)
	for _, email := range []string{"a1@example.com", "a2@example.com", "a3@example.com"} {
		status, acctErr := orgBackend.CreateAccount(email, email, "OrganizationAccountAccessRole", "ALLOW", nil)
		require.NoError(t, acctErr)
		require.NoError(t, orgBackend.MoveAccount(status.AccountID, root.ID, ou.ID))
		ouAccounts = append(ouAccounts, status.AccountID)
	}

	outsideStatus, err := orgBackend.CreateAccount(
		"outside@example.com", "outside@example.com", "OrganizationAccountAccessRole", "ALLOW", nil,
	)
	require.NoError(t, err)

	cfnBackend := cloudformation.NewInMemoryBackendWithConfig(
		"000000000000", "us-east-1", cloudformation.NewResourceCreator(nil),
	)
	cfnBackend.SetOrganizationsDirectory(orgBackend)

	client := newTestClientForBackend(t, cfnBackend)
	ctx := t.Context()

	_, err = client.ActivateOrganizationsAccess(ctx, &cfnsdk.ActivateOrganizationsAccessInput{})
	require.NoError(t, err)

	_, err = client.CreateStackSet(ctx, &cfnsdk.CreateStackSetInput{
		StackSetName:    aws.String(stackSetName),
		TemplateBody:    aws.String(simpleTemplate),
		PermissionModel: types.PermissionModelsServiceManaged,
	})
	require.NoError(t, err)

	return accountFilterFixture{
		client:         client,
		stackSetName:   stackSetName,
		ouID:           ou.ID,
		ouAccounts:     ouAccounts,
		outsideAccount: outsideStatus.AccountID,
	}
}

func (f accountFilterFixture) listInstanceAccounts(t *testing.T) []string {
	t.Helper()

	out, err := f.client.ListStackInstances(t.Context(), &cfnsdk.ListStackInstancesInput{
		StackSetName: aws.String(f.stackSetName),
	})
	require.NoError(t, err)

	accounts := make([]string, 0, len(out.Summaries))
	for _, s := range out.Summaries {
		accounts = append(accounts, aws.ToString(s.Account))
	}

	return accounts
}

// TestStackInstances_AccountFilterType_Create drives CreateStackInstances
// through the real aws-sdk-go-v2 client with each AccountFilterType value
// CreateStackInstances supports and asserts ListStackInstances returns
// exactly the expected accounts.
func TestStackInstances_AccountFilterType_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filterType types.AccountFilterType
		wantIdx    []int // indices into ouAccounts expected present
	}{
		{name: "none_ignores_explicit_accounts", filterType: types.AccountFilterTypeNone, wantIdx: []int{0, 1, 2}},
		{name: "intersection", filterType: types.AccountFilterTypeIntersection, wantIdx: []int{0, 1}},
		{name: "difference", filterType: types.AccountFilterTypeDifference, wantIdx: []int{2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newAccountFilterFixture(t, "filter-create-"+tt.name)
			ctx := t.Context()

			// Explicit Accounts overlaps ouAccounts[0:2] and adds an outside
			// account, so each filter type produces a distinguishable set.
			explicit := []string{f.ouAccounts[0], f.ouAccounts[1], f.outsideAccount}

			_, err := f.client.CreateStackInstances(ctx, &cfnsdk.CreateStackInstancesInput{
				StackSetName: aws.String(f.stackSetName),
				Regions:      []string{"us-east-1"},
				DeploymentTargets: &types.DeploymentTargets{
					AccountFilterType:     tt.filterType,
					Accounts:              explicit,
					OrganizationalUnitIds: []string{f.ouID},
				},
			})
			require.NoError(t, err)

			want := make([]string, 0, len(tt.wantIdx))
			for _, idx := range tt.wantIdx {
				want = append(want, f.ouAccounts[idx])
			}

			assert.ElementsMatch(t, want, f.listInstanceAccounts(t))
		})
	}
}

// TestStackInstances_AccountFilterType_UnionRejectedAtCreate verifies the
// documented restriction that UNION is not supported for CreateStackInstances
// operations.
func TestStackInstances_AccountFilterType_UnionRejectedAtCreate(t *testing.T) {
	t.Parallel()

	f := newAccountFilterFixture(t, "filter-create-union-rejected")

	_, err := f.client.CreateStackInstances(t.Context(), &cfnsdk.CreateStackInstancesInput{
		StackSetName: aws.String(f.stackSetName),
		Regions:      []string{"us-east-1"},
		DeploymentTargets: &types.DeploymentTargets{
			AccountFilterType:     types.AccountFilterTypeUnion,
			Accounts:              []string{f.outsideAccount},
			OrganizationalUnitIds: []string{f.ouID},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "UNION")
}

// TestStackInstances_AccountFilterType_RequiredWhenBothGivenAtCreate verifies
// the documented rule that create operations specifying both
// OrganizationalUnitIds and Accounts must also specify AccountFilterType.
func TestStackInstances_AccountFilterType_RequiredWhenBothGivenAtCreate(t *testing.T) {
	t.Parallel()

	f := newAccountFilterFixture(t, "filter-create-required")

	_, err := f.client.CreateStackInstances(t.Context(), &cfnsdk.CreateStackInstancesInput{
		StackSetName: aws.String(f.stackSetName),
		Regions:      []string{"us-east-1"},
		DeploymentTargets: &types.DeploymentTargets{
			Accounts:              []string{f.outsideAccount},
			OrganizationalUnitIds: []string{f.ouID},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AccountFilterType")
}

// TestStackInstances_AccountFilterType_InvalidValue verifies an unsupported
// AccountFilterType enum value is rejected with ValidationError.
func TestStackInstances_AccountFilterType_InvalidValue(t *testing.T) {
	t.Parallel()

	f := newAccountFilterFixture(t, "filter-create-invalid")

	_, err := f.client.CreateStackInstances(t.Context(), &cfnsdk.CreateStackInstancesInput{
		StackSetName: aws.String(f.stackSetName),
		Regions:      []string{"us-east-1"},
		DeploymentTargets: &types.DeploymentTargets{
			AccountFilterType:     "BOGUS",
			OrganizationalUnitIds: []string{f.ouID},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AccountFilterType")
}

// TestStackInstances_AccountFilterType_UpdateUnion verifies UNION (allowed on
// UpdateStackInstances, unlike Create): the touched accounts are the OU
// accounts plus the listed ones. UpdateStackInstances updates existing
// instances rather than provisioning new ones (gopherstack, like real AWS,
// requires an instance to already exist for an account/region pair to be
// updated), so the resolved target set is asserted via
// ListStackSetOperationResults rather than ListStackInstances.
func TestStackInstances_AccountFilterType_UpdateUnion(t *testing.T) {
	t.Parallel()

	f := newAccountFilterFixture(t, "filter-update-union")
	ctx := t.Context()

	_, err := f.client.CreateStackInstances(ctx, &cfnsdk.CreateStackInstancesInput{
		StackSetName: aws.String(f.stackSetName),
		Regions:      []string{"us-east-1"},
		DeploymentTargets: &types.DeploymentTargets{
			AccountFilterType:     types.AccountFilterTypeNone,
			OrganizationalUnitIds: []string{f.ouID},
		},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, f.ouAccounts, f.listInstanceAccounts(t))

	updOut, err := f.client.UpdateStackInstances(ctx, &cfnsdk.UpdateStackInstancesInput{
		StackSetName: aws.String(f.stackSetName),
		Regions:      []string{"us-east-1"},
		DeploymentTargets: &types.DeploymentTargets{
			AccountFilterType:     types.AccountFilterTypeUnion,
			Accounts:              []string{f.outsideAccount},
			OrganizationalUnitIds: []string{f.ouID},
		},
	})
	require.NoError(t, err)

	resultsOut, err := f.client.ListStackSetOperationResults(ctx, &cfnsdk.ListStackSetOperationResultsInput{
		StackSetName: aws.String(f.stackSetName),
		OperationId:  updOut.OperationId,
	})
	require.NoError(t, err)

	touched := make([]string, 0, len(resultsOut.Summaries))
	for _, s := range resultsOut.Summaries {
		touched = append(touched, aws.ToString(s.Account))
	}

	want := append(append([]string{}, f.ouAccounts...), f.outsideAccount)
	assert.ElementsMatch(t, want, touched)
}

// TestStackInstances_AccountFilterType_DeleteDifference verifies DIFFERENCE
// on DeleteStackInstances: the accounts targeted for deletion are the OU
// accounts minus the listed ones, so the listed account's instance survives.
func TestStackInstances_AccountFilterType_DeleteDifference(t *testing.T) {
	t.Parallel()

	f := newAccountFilterFixture(t, "filter-delete-difference")
	ctx := t.Context()

	_, err := f.client.CreateStackInstances(ctx, &cfnsdk.CreateStackInstancesInput{
		StackSetName: aws.String(f.stackSetName),
		Regions:      []string{"us-east-1"},
		DeploymentTargets: &types.DeploymentTargets{
			AccountFilterType:     types.AccountFilterTypeNone,
			OrganizationalUnitIds: []string{f.ouID},
		},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, f.ouAccounts, f.listInstanceAccounts(t))

	_, err = f.client.DeleteStackInstances(ctx, &cfnsdk.DeleteStackInstancesInput{
		StackSetName: aws.String(f.stackSetName),
		Regions:      []string{"us-east-1"},
		RetainStacks: aws.Bool(false),
		DeploymentTargets: &types.DeploymentTargets{
			AccountFilterType:     types.AccountFilterTypeDifference,
			Accounts:              []string{f.ouAccounts[0]},
			OrganizationalUnitIds: []string{f.ouID},
		},
	})
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{f.ouAccounts[0]}, f.listInstanceAccounts(t))
}
