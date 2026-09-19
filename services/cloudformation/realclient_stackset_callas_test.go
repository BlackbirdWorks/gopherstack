package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_StackSetCallAs proves the CallAs request parameter -- the
// reqfielddiff tier-1 sweep found it silently dropped on all twenty
// StackSet-family operations (gopherstack-xhu2t) -- is now enforced: an
// out-of-enum value is rejected on every operation that declares it, and
// DELEGATED_ADMIN is rejected against a StackSet whose PermissionModel isn't
// SERVICE_MANAGED (the one real, observable constraint this backend can
// honor without simulating an AWS Organizations management/delegated-
// administrator identity) but accepted against one that is.
func TestRealClient_StackSetCallAs(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testCallAsInvalidEnumRejectedAcrossOps, "invalid_enum_rejected_across_ops"},
		{testCallAsDelegatedAdminRequiresServiceManaged, "delegated_admin_requires_service_managed"},
		{testCallAsDelegatedAdminAcceptedForServiceManaged, "delegated_admin_accepted_for_service_managed"},
		{testCallAsSelfAlwaysAccepted, "self_always_accepted"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// testCallAsInvalidEnumRejectedAcrossOps proves the shared validateCallAs
// enum check reaches a representative spread of the twenty CallAs-declaring
// operations -- not just one, since each op's handler wires the check in
// independently.
func testCallAsInvalidEnumRejectedAcrossOps(t *testing.T) {
	t.Helper()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()
	bogus := types.CallAs("BOGUS")

	opCases := []struct {
		call func() error
		name string
	}{
		{name: "CreateStackSet", call: func() error {
			_, err := client.CreateStackSet(ctx, &cfnsdk.CreateStackSetInput{
				StackSetName: aws.String("callas-bogus-css"), TemplateBody: aws.String(simpleTemplate), CallAs: bogus,
			})

			return err
		}},
		{name: "ListStackSets", call: func() error {
			_, err := client.ListStackSets(ctx, &cfnsdk.ListStackSetsInput{CallAs: bogus})

			return err
		}},
		{name: "DescribeOrganizationsAccess", call: func() error {
			_, err := client.DescribeOrganizationsAccess(ctx, &cfnsdk.DescribeOrganizationsAccessInput{CallAs: bogus})

			return err
		}},
		{name: "DescribeStackSet", call: func() error {
			_, err := client.DescribeStackSet(ctx, &cfnsdk.DescribeStackSetInput{
				StackSetName: aws.String("callas-nonexistent"), CallAs: bogus,
			})

			return err
		}},
		{name: "ListStackInstances", call: func() error {
			_, err := client.ListStackInstances(ctx, &cfnsdk.ListStackInstancesInput{
				StackSetName: aws.String("callas-nonexistent"), CallAs: bogus,
			})

			return err
		}},
		{name: "GetTemplateSummary", call: func() error {
			_, err := client.GetTemplateSummary(ctx, &cfnsdk.GetTemplateSummaryInput{
				StackSetName: aws.String("callas-nonexistent"), CallAs: bogus,
			})

			return err
		}},
	}

	for _, oc := range opCases {
		t.Run(oc.name, func(t *testing.T) {
			t.Parallel()

			err := oc.call()
			require.Error(t, err)
		})
	}
}

// testCallAsDelegatedAdminRequiresServiceManaged proves DELEGATED_ADMIN is
// rejected against a self-managed StackSet, across the ops that resolve a
// target StackSet to check its PermissionModel against.
func testCallAsDelegatedAdminRequiresServiceManaged(t *testing.T) {
	t.Helper()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.CreateStackSet(ctx, &cfnsdk.CreateStackSetInput{
		StackSetName: aws.String("callas-self-managed-ss"),
		TemplateBody: aws.String(simpleTemplate),
	})
	require.NoError(t, err)

	_, err = client.DescribeStackSet(ctx, &cfnsdk.DescribeStackSetInput{
		StackSetName: aws.String("callas-self-managed-ss"),
		CallAs:       types.CallAsDelegatedAdmin,
	})
	require.Error(t, err)

	_, err = client.DeleteStackSet(ctx, &cfnsdk.DeleteStackSetInput{
		StackSetName: aws.String("callas-self-managed-ss"),
		CallAs:       types.CallAsDelegatedAdmin,
	})
	require.Error(t, err)

	_, err = client.DetectStackSetDrift(ctx, &cfnsdk.DetectStackSetDriftInput{
		StackSetName: aws.String("callas-self-managed-ss"),
		CallAs:       types.CallAsDelegatedAdmin,
	})
	require.Error(t, err)
}

// testCallAsDelegatedAdminAcceptedForServiceManaged proves the mirror image:
// DELEGATED_ADMIN succeeds against a StackSet whose PermissionModel is
// SERVICE_MANAGED.
func testCallAsDelegatedAdminAcceptedForServiceManaged(t *testing.T) {
	t.Helper()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.CreateStackSet(ctx, &cfnsdk.CreateStackSetInput{
		StackSetName:    aws.String("callas-service-managed-ss"),
		TemplateBody:    aws.String(simpleTemplate),
		PermissionModel: types.PermissionModelsServiceManaged,
	})
	require.NoError(t, err)

	descOut, err := client.DescribeStackSet(ctx, &cfnsdk.DescribeStackSetInput{
		StackSetName: aws.String("callas-service-managed-ss"),
		CallAs:       types.CallAsDelegatedAdmin,
	})
	require.NoError(t, err)
	assert.Equal(t, "callas-service-managed-ss", aws.ToString(descOut.StackSet.StackSetName))
}

// testCallAsSelfAlwaysAccepted proves SELF (and omitting CallAs, which
// defaults to SELF) never triggers the PermissionModel check regardless of
// the target StackSet's own permission model.
func testCallAsSelfAlwaysAccepted(t *testing.T) {
	t.Helper()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.CreateStackSet(ctx, &cfnsdk.CreateStackSetInput{
		StackSetName: aws.String("callas-self-ss"),
		TemplateBody: aws.String(simpleTemplate),
		CallAs:       types.CallAsSelf,
	})
	require.NoError(t, err)

	_, err = client.DescribeStackSet(ctx, &cfnsdk.DescribeStackSetInput{
		StackSetName: aws.String("callas-self-ss"),
	})
	require.NoError(t, err)
}
