package ecs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListTasks_StartedByMustBeExclusive is a regression test for
// gopherstack-uox6: ListTasksInput.StartedBy's own doc ("When you specify
// startedBy as the filter, it must be the only filter that you use",
// ecs@v1.96.0 api_op_ListTasks.go) was never enforced -- combining it with
// another filter was silently ANDed instead of rejected.
func TestListTasks_StartedByMustBeExclusive(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)

	_, err := client.CreateCluster(
		t.Context(),
		&ecssdk.CreateClusterInput{ClusterName: aws.String("startedby-cluster")},
	)
	require.NoError(t, err)

	_, err = client.ListTasks(t.Context(), &ecssdk.ListTasksInput{
		Cluster:     aws.String("startedby-cluster"),
		StartedBy:   aws.String("my-starter"),
		ServiceName: aws.String("some-service"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidParameterException")

	_, err = client.ListTasks(t.Context(), &ecssdk.ListTasksInput{
		Cluster:   aws.String("startedby-cluster"),
		StartedBy: aws.String("my-starter"),
	})
	assert.NoError(t, err, "startedBy alone must be accepted")
}

// TestListAccountSettings_ValueRequiresName is a regression test for
// gopherstack-uox6: ListAccountSettingsInput.Value's own doc ("You must also
// specify an account setting name to use this parameter", ecs@v1.96.0
// api_op_ListAccountSettings.go) was never enforced, and Value itself wasn't
// even read as a result filter.
func TestListAccountSettings_ValueRequiresName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)

	_, err := client.ListAccountSettings(t.Context(), &ecssdk.ListAccountSettingsInput{
		Value: aws.String("enabled"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidParameterException")

	_, err = client.PutAccountSetting(t.Context(), &ecssdk.PutAccountSettingInput{
		Name:  types.SettingNameContainerInsights,
		Value: aws.String("enabled"),
	})
	require.NoError(t, err)

	out, err := client.ListAccountSettings(t.Context(), &ecssdk.ListAccountSettingsInput{
		Name:  types.SettingNameContainerInsights,
		Value: aws.String("enabled"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Settings)

	out, err = client.ListAccountSettings(t.Context(), &ecssdk.ListAccountSettingsInput{
		Name:  types.SettingNameContainerInsights,
		Value: aws.String("disabled"),
	})
	require.NoError(t, err)
	assert.Empty(t, out.Settings, "value filter must exclude non-matching settings")
}

// TestListAttributes_AttributeValueRequiresName is a regression test for
// gopherstack-uox6: ListAttributesInput.AttributeValue's own doc ("You must
// also specify an attribute name to use this parameter", ecs@v1.96.0
// api_op_ListAttributes.go) was never enforced, and AttributeValue itself
// wasn't even read as a result filter.
func TestListAttributes_AttributeValueRequiresName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)

	_, err := client.ListAttributes(t.Context(), &ecssdk.ListAttributesInput{
		TargetType:     types.TargetTypeContainerInstance,
		AttributeValue: aws.String("gpu"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidParameterException")

	_, err = client.PutAttributes(t.Context(), &ecssdk.PutAttributesInput{
		Attributes: []types.Attribute{
			{Name: aws.String("device"), Value: aws.String("gpu"), TargetType: types.TargetTypeContainerInstance},
		},
	})
	require.NoError(t, err)

	out, err := client.ListAttributes(t.Context(), &ecssdk.ListAttributesInput{
		TargetType:     types.TargetTypeContainerInstance,
		AttributeName:  aws.String("device"),
		AttributeValue: aws.String("gpu"),
	})
	require.NoError(t, err)
	require.Len(t, out.Attributes, 1)

	out, err = client.ListAttributes(t.Context(), &ecssdk.ListAttributesInput{
		TargetType:     types.TargetTypeContainerInstance,
		AttributeName:  aws.String("device"),
		AttributeValue: aws.String("cpu"),
	})
	require.NoError(t, err)
	assert.Empty(t, out.Attributes, "attributeValue filter must exclude non-matching attributes")
}
