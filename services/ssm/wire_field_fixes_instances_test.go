package ssm_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestDescribeEffectiveInstanceAssociations_InstanceId_RealClient covers a
// wrong-per-item-shape bug: gopherstack's InstanceAssociationInfo used to
// emit "Name"/"DocumentVersion" -- neither a real member of
// types.InstanceAssociation (ssm@v1.73.4, api_op_DescribeEffectiveInstanceAssociations.go's
// only response element type: AssociationId/AssociationVersion/Content/InstanceId)
// -- while never emitting InstanceId, even though it is the exact value the
// backend just filtered by, and never resolving Content at all even though
// the backend genuinely tracks the association's document body via
// Association.Name/DocumentVersion. A real client always saw a nil
// InstanceId and a nil Content here.
func TestDescribeEffectiveInstanceAssociations_InstanceId_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
		Name:    aws.String("EffectiveAssocDoc"),
		Content: aws.String(`{"schemaVersion":"2.2"}`),
	})
	require.NoError(t, err)

	_, err = client.CreateAssociation(ctx, &ssmsdk.CreateAssociationInput{
		Name:       aws.String("EffectiveAssocDoc"),
		InstanceId: aws.String("i-effective-assoc"),
	})
	require.NoError(t, err)

	out, err := client.DescribeEffectiveInstanceAssociations(ctx, &ssmsdk.DescribeEffectiveInstanceAssociationsInput{
		InstanceId: aws.String("i-effective-assoc"),
	})
	require.NoError(t, err)
	require.Len(t, out.Associations, 1)
	require.NotNil(t, out.Associations[0].InstanceId,
		"InstanceAssociation.InstanceId must round-trip; pre-fix it was never emitted at all")
	assert.Equal(t, "i-effective-assoc", aws.ToString(out.Associations[0].InstanceId))
	require.NotNil(t, out.Associations[0].AssociationVersion)
	assert.Equal(t, "1", aws.ToString(out.Associations[0].AssociationVersion))
	require.NotNil(t, out.Associations[0].Content,
		"InstanceAssociation.Content must resolve from the stored document version; pre-fix it was never emitted")
	assert.JSONEq(t, `{"schemaVersion":"2.2"}`, aws.ToString(out.Associations[0].Content))
}

// TestDescribeInstanceAssociationsStatus_Fields_RealClient covers the same
// bug class one op over: AssociationName/AssociationVersion/DocumentVersion/
// InstanceId are all real members of types.InstanceAssociationStatusInfo
// (ssm@v1.73.4) that this backend already tracks on the underlying
// Association record but never echoed onto this narrower response type.
func TestDescribeInstanceAssociationsStatus_Fields_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateDocument(ctx, &ssmsdk.CreateDocumentInput{
		Name:    aws.String("AssocStatusDoc"),
		Content: aws.String(`{"schemaVersion":"2.2"}`),
	})
	require.NoError(t, err)

	_, err = client.CreateAssociation(ctx, &ssmsdk.CreateAssociationInput{
		Name:            aws.String("AssocStatusDoc"),
		InstanceId:      aws.String("i-assoc-status"),
		AssociationName: aws.String("my-assoc-name"),
		DocumentVersion: aws.String("1"),
	})
	require.NoError(t, err)

	out, err := client.DescribeInstanceAssociationsStatus(ctx, &ssmsdk.DescribeInstanceAssociationsStatusInput{
		InstanceId: aws.String("i-assoc-status"),
	})
	require.NoError(t, err)
	require.Len(t, out.InstanceAssociationStatusInfos, 1)

	got := out.InstanceAssociationStatusInfos[0]
	require.NotNil(t, got.InstanceId, "InstanceId must round-trip; pre-fix it was never emitted")
	assert.Equal(t, "i-assoc-status", aws.ToString(got.InstanceId))
	assert.Equal(t, "my-assoc-name", aws.ToString(got.AssociationName))
	assert.Equal(t, "1", aws.ToString(got.AssociationVersion))
	assert.Equal(t, "1", aws.ToString(got.DocumentVersion))
}

// TestDescribeInstancePatchStates_OperationEndTime_RealClient covers a
// missing-required-field bug: types.InstancePatchState.OperationEndTime is
// documented as a required response member (api_op_DescribeInstancePatchStates.go)
// but had no Go field at all, so it was always omitted even though every
// patch operation this backend runs completes synchronously in the same
// call that sets OperationStartTime.
func TestDescribeInstancePatchStates_OperationEndTime_RealClient(t *testing.T) {
	t.Parallel()

	backend := ssm.NewInMemoryBackend()
	client := newTestSSMClient(t, ssm.NewHandler(backend))
	ctx := t.Context()

	createOut, err := client.CreatePatchBaseline(ctx, &ssmsdk.CreatePatchBaselineInput{
		Name:            aws.String("op-end-time-baseline"),
		OperatingSystem: "AMAZON_LINUX_2",
	})
	require.NoError(t, err)

	_, err = client.RegisterPatchBaselineForPatchGroup(ctx, &ssmsdk.RegisterPatchBaselineForPatchGroupInput{
		BaselineId: createOut.BaselineId,
		PatchGroup: aws.String("op-end-time-group"),
	})
	require.NoError(t, err)

	_, err = client.SendCommand(ctx, &ssmsdk.SendCommandInput{
		DocumentName: aws.String("AWS-RunPatchBaseline"),
		InstanceIds:  []string{"i-op-end-time"},
		Parameters: map[string][]string{
			"PatchGroup": {"op-end-time-group"},
			"Operation":  {"Scan"},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeInstancePatchStates(ctx, &ssmsdk.DescribeInstancePatchStatesInput{
		InstanceIds: []string{"i-op-end-time"},
	})
	require.NoError(t, err)
	require.Len(t, out.InstancePatchStates, 1)
	require.NotNil(t, out.InstancePatchStates[0].OperationEndTime,
		"OperationEndTime is a required member on the real type; pre-fix it was never emitted")
	assert.False(t, aws.ToTime(out.InstancePatchStates[0].OperationEndTime).IsZero())
}
