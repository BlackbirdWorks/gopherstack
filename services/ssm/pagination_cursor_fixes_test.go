package ssm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestDescribeAssociationExecutions_Pagination drives StartAssociationsOnce
// (real api_op_StartAssociationsOnce.go) five times against one association
// to accumulate five execution records, then verifies DescribeAssociationExecutions
// (api_op_DescribeAssociationExecutions.go: MaxResults/NextToken) returns a
// full first page, a non-empty NextToken, and the exact remainder on the
// second page with no duplication -- the shape the primary elbv2 bug this
// sweep is modeled on always failed.
func TestDescribeAssociationExecutions_Pagination(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))

	_, err := client.CreateDocument(t.Context(), &ssmsdk.CreateDocumentInput{
		Name:    aws.String("exec-pagination-doc"),
		Content: aws.String(`{"schemaVersion":"2.2","mainSteps":[]}`),
	})
	require.NoError(t, err)

	assoc, err := client.CreateAssociation(t.Context(), &ssmsdk.CreateAssociationInput{
		Name: aws.String("exec-pagination-doc"),
	})
	require.NoError(t, err)

	assocID := assoc.AssociationDescription.AssociationId

	// CreateAssociation already recorded one execution; four more runs bring
	// the total to five, one more than the page size requested below.
	for range 4 {
		_, startErr := client.StartAssociationsOnce(t.Context(), &ssmsdk.StartAssociationsOnceInput{
			AssociationIds: []string{aws.ToString(assocID)},
		})
		require.NoError(t, startErr)
	}

	first, err := client.DescribeAssociationExecutions(
		t.Context(),
		&ssmsdk.DescribeAssociationExecutionsInput{
			AssociationId: assocID,
			MaxResults:    aws.Int32(3),
		},
	)
	require.NoError(t, err)
	require.Len(t, first.AssociationExecutions, 3)
	require.NotEmpty(
		t,
		aws.ToString(first.NextToken),
		"response declares NextToken but never sets it",
	)

	second, err := client.DescribeAssociationExecutions(
		t.Context(),
		&ssmsdk.DescribeAssociationExecutionsInput{
			AssociationId: assocID,
			MaxResults:    aws.Int32(3),
			NextToken:     first.NextToken,
		},
	)
	require.NoError(t, err)
	require.Len(t, second.AssociationExecutions, 2)
	require.Empty(t, aws.ToString(second.NextToken))

	seen := make(map[string]bool, 5)
	for _, e := range first.AssociationExecutions {
		seen[aws.ToString(e.ExecutionId)] = true
	}

	for _, e := range second.AssociationExecutions {
		id := aws.ToString(e.ExecutionId)
		require.False(t, seen[id], "execution %s returned on both pages", id)
		seen[id] = true
	}

	require.Len(t, seen, 5)
}

// TestDescribeInstanceProperties_Pagination registers five managed-instance
// activations (each becomes one InstanceProperty entry) and verifies
// DescribeInstanceProperties (api_op_DescribeInstanceProperties.go:
// MaxResults/NextToken) actually paginates instead of returning every
// property in one page while advertising a cursor it never sets.
func TestDescribeInstanceProperties_Pagination(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))

	for range 5 {
		_, err := client.CreateActivation(t.Context(), &ssmsdk.CreateActivationInput{
			IamRole: aws.String("arn:aws:iam::000000000000:role/SSMRole"),
		})
		require.NoError(t, err)
	}

	first, err := client.DescribeInstanceProperties(
		t.Context(),
		&ssmsdk.DescribeInstancePropertiesInput{
			MaxResults: aws.Int32(3),
		},
	)
	require.NoError(t, err)
	require.Len(t, first.InstanceProperties, 3)
	require.NotEmpty(t, aws.ToString(first.NextToken))

	second, err := client.DescribeInstanceProperties(
		t.Context(),
		&ssmsdk.DescribeInstancePropertiesInput{
			MaxResults: aws.Int32(3),
			NextToken:  first.NextToken,
		},
	)
	require.NoError(t, err)
	require.Len(t, second.InstanceProperties, 2)
	require.Empty(t, aws.ToString(second.NextToken))

	seen := make(map[string]bool, 5)
	for _, p := range first.InstanceProperties {
		seen[aws.ToString(p.ActivationId)] = true
	}

	for _, p := range second.InstanceProperties {
		id := aws.ToString(p.ActivationId)
		require.False(t, seen[id], "instance property %s returned on both pages", id)
		seen[id] = true
	}

	require.Len(t, seen, 5)
}

// TestDescribeMaintenanceWindowTargets_Pagination registers five targets on
// one maintenance window and confirms DescribeMaintenanceWindowTargets
// (api_op_DescribeMaintenanceWindowTargets.go) actually models and honours
// MaxResults/NextToken -- before this fix the gopherstack request/response
// structs for this op had no such fields at all, so the real SDK client
// could never even ask for a second page.
func TestDescribeMaintenanceWindowTargets_Pagination(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))

	mw, err := client.CreateMaintenanceWindow(t.Context(), &ssmsdk.CreateMaintenanceWindowInput{
		Name:                     aws.String("target-pagination-mw"),
		Schedule:                 aws.String("rate(1 day)"),
		Duration:                 aws.Int32(2),
		Cutoff:                   0,
		AllowUnassociatedTargets: true,
	})
	require.NoError(t, err)

	for i := range 5 {
		_, registerErr := client.RegisterTargetWithMaintenanceWindow(
			t.Context(),
			&ssmsdk.RegisterTargetWithMaintenanceWindowInput{
				WindowId:     mw.WindowId,
				ResourceType: ssmtypes.MaintenanceWindowResourceTypeInstance,
				Targets: []ssmtypes.Target{
					{Key: aws.String("InstanceIds"), Values: []string{"i-" + string(rune('a'+i))}},
				},
			},
		)
		require.NoError(t, registerErr)
	}

	first, err := client.DescribeMaintenanceWindowTargets(
		t.Context(),
		&ssmsdk.DescribeMaintenanceWindowTargetsInput{
			WindowId:   mw.WindowId,
			MaxResults: aws.Int32(3),
		},
	)
	require.NoError(t, err)
	require.Len(t, first.Targets, 3)
	require.NotEmpty(t, aws.ToString(first.NextToken))

	second, err := client.DescribeMaintenanceWindowTargets(
		t.Context(),
		&ssmsdk.DescribeMaintenanceWindowTargetsInput{
			WindowId:   mw.WindowId,
			MaxResults: aws.Int32(3),
			NextToken:  first.NextToken,
		},
	)
	require.NoError(t, err)
	require.Len(t, second.Targets, 2)
	require.Empty(t, aws.ToString(second.NextToken))

	seen := make(map[string]bool, 5)
	for _, tg := range first.Targets {
		seen[aws.ToString(tg.WindowTargetId)] = true
	}

	for _, tg := range second.Targets {
		id := aws.ToString(tg.WindowTargetId)
		require.False(t, seen[id], "target %s returned on both pages", id)
		seen[id] = true
	}

	require.Len(t, seen, 5)
}
