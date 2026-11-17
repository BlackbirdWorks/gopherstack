package cloudformation_test

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

func awaitOperationTerminal(t *testing.T, b *cloudformation.InMemoryBackend, opID string) string {
	t.Helper()

	for range 5_000_000 {
		op, err := b.DescribeStackSetOperation(updSetName, opID)
		require.NoError(t, err)

		switch op.Status {
		case "QUEUED", "RUNNING", "STOPPING":
			runtime.Gosched()
		default:
			return op.Status
		}
	}

	require.FailNow(t, "operation never reached a terminal status")

	return ""
}

func TestStackSetOperations_SequentialCallerNeverSeesOperationInProgress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		next func(b *cloudformation.InMemoryBackend, i int) (string, error)
		name string
	}{
		{name: "update_stack_set", next: func(b *cloudformation.InMemoryBackend, i int) (string, error) {
			tmpl := updTemplateV1
			if i%2 == 0 {
				tmpl = updTemplateV2
			}

			_, opID, err := b.UpdateStackSet(updSetName, "", tmpl, cloudformation.StackSetOptions{})

			return opID, err
		}},
		{name: "update_stack_instances", next: func(b *cloudformation.InMemoryBackend, _ int) (string, error) {
			return b.UpdateStackInstances(
				updSetName, []string{updAcct1}, nil, []string{updRegion1}, "",
				cloudformation.WithParameterOverrides(envParam("y")),
			)
		}},
		{name: "detect_drift", next: func(b *cloudformation.InMemoryBackend, _ int) (string, error) {
			return b.DetectStackSetDrift(updSetName)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			_, err := b.CreateStackSet(updSetName, "", updTemplateV1, cloudformation.StackSetOptions{
				Parameters: envParam("x"),
			})
			require.NoError(t, err)

			for i := range 200 {
				opID, cerr := b.CreateStackInstances(
					t.Context(), updSetName, []string{updAcct1, updAcct2}, nil, []string{updRegion1}, "",
				)
				require.NoError(t, cerr, "iteration %d create", i)
				require.Equal(t, "SUCCEEDED", awaitOperationTerminal(t, b, opID))

				opID, cerr = tt.next(b, i)
				require.NoError(t, cerr, "iteration %d follow-up", i)
				require.Equal(t, "SUCCEEDED", awaitOperationTerminal(t, b, opID))

				opID, cerr = b.DeleteStackInstances(
					t.Context(), updSetName, []string{updAcct1, updAcct2}, nil, []string{updRegion1}, false, "",
				)
				require.NoError(t, cerr, "iteration %d delete", i)
				require.Equal(t, "SUCCEEDED", awaitOperationTerminal(t, b, opID))
			}
		})
	}
}

func TestUpdateStackSet_SkipsInoperableInstances(t *testing.T) {
	t.Parallel()

	b := newUpdateFixture(t, []string{updAcct1, updAcct2}, []string{updRegion1})
	b.SetStackInstanceStatusForTest(updSetName, updAcct2, updRegion1, "INOPERABLE")

	_, opID, err := b.UpdateStackSet(updSetName, "", updTemplateV2, cloudformation.StackSetOptions{})
	require.NoError(t, err)
	b.WaitForStackSetOperations()

	res, err := b.ListStackSetOperationResults(updSetName, opID, 0, "", nil)
	require.NoError(t, err)
	require.Len(t, res.Data, 1)
	assert.Equal(t, updAcct1, res.Data[0].Account)

	inst, err := b.DescribeStackInstance(updSetName, updAcct2, updRegion1)
	require.NoError(t, err)
	assert.Equal(t, "INOPERABLE", inst.Status)
	assert.JSONEq(t, updTemplateV1, instanceStack(t, b, updAcct2).TemplateBody)
}

func TestUpdateStackSet_NestedStackTemplate(t *testing.T) {
	t.Parallel()

	nested := func(topic string) string {
		return `{"Resources":{"Child":{"Type":"AWS::CloudFormation::Stack","Properties":{"TemplateBody":` +
			`"{\"Resources\":{\"Q\":{\"Type\":\"AWS::SQS::Queue\",\"Properties\":{\"QueueName\":\"` + topic + `\"}}}}"}}}}`
	}

	b := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1",
		cloudformation.NewResourceCreator(newServiceBackends(t)))
	_, err := b.CreateStackSet(updSetName, "", nested("nq1"), cloudformation.StackSetOptions{})
	require.NoError(t, err)
	_, err = b.CreateStackInstances(t.Context(), updSetName, []string{updAcct1}, nil, []string{updRegion1}, "")
	require.NoError(t, err)
	b.WaitForStackSetOperations()

	_, opID, err := b.UpdateStackSet(updSetName, "", nested("nq2"), cloudformation.StackSetOptions{})
	require.NoError(t, err)
	b.WaitForStackSetOperations()

	op, err := b.DescribeStackSetOperation(updSetName, opID)
	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", op.Status, op.StatusReason)
}

func TestPreviewStackSetChanges_ReplacementUsesSDKEnum(t *testing.T) {
	t.Parallel()

	table := func(attr string) string {
		return `{"Resources":{"T":{"Type":"AWS::DynamoDB::Table","Properties":{"TableName":"t",` +
			`"BillingMode":"PAY_PER_REQUEST","AttributeDefinitions":[{"AttributeName":"` + attr +
			`","AttributeType":"S"}],"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}}}}`
	}

	b := newBackend()
	_, err := b.CreateStackSet(updSetName, "", table("id"), cloudformation.StackSetOptions{})
	require.NoError(t, err)

	changes, err := b.PreviewStackSetChanges(updSetName, table("other"), nil)
	require.NoError(t, err)
	require.Len(t, changes, 1)
	assert.Equal(t, "Conditional", changes[0].ResourceChange.Replacement)
}

func TestUpdateStackSet_PercentagesRoundDown(t *testing.T) {
	t.Parallel()

	accounts := []string{"111111111111", "222222222222", "333333333333"}

	tests := []struct {
		prefs *cloudformation.OperationPreferences
		name  string
		want  []string
	}{
		{
			name:  "failure_tolerance_50_of_3_is_1",
			prefs: &cloudformation.OperationPreferences{FailureTolerancePercentage: int32p(50)},
			want:  []string{"FAILED", "FAILED", "CANCELLED"},
		},
		{
			name: "max_concurrent_70_of_3_is_2",
			prefs: &cloudformation.OperationPreferences{
				ConcurrencyMode: "SOFT_FAILURE_TOLERANCE", MaxConcurrentPercentage: int32p(70),
			},
			want: []string{"FAILED", "FAILED", "CANCELLED"},
		},
		{
			name: "max_concurrent_10_of_3_is_at_least_1",
			prefs: &cloudformation.OperationPreferences{
				ConcurrencyMode: "SOFT_FAILURE_TOLERANCE", MaxConcurrentPercentage: int32p(10),
			},
			want: []string{"FAILED", "CANCELLED", "CANCELLED"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newUpdateFixture(t, accounts, []string{updRegion1})
			_, err := b.UpdateStackInstances(updSetName, accounts, nil, []string{updRegion1}, "",
				cloudformation.WithParameterOverrides(envParam("y")))
			require.NoError(t, err)
			b.WaitForStackSetOperations()

			_, opID, err := b.UpdateStackSet(updSetName, "", updTemplateStrict, cloudformation.StackSetOptions{},
				cloudformation.WithOperationPreferences(tt.prefs))
			require.NoError(t, err)
			b.WaitForStackSetOperations()

			assert.Equal(t, tt.want, opResultStatuses(t, b, updSetName, opID))
		})
	}
}

func TestUpdateStackSet_UsePreviousValueKeepsSetValue(t *testing.T) {
	t.Parallel()

	b := newUpdateFixture(t, []string{updAcct1}, []string{updRegion1})

	_, _, err := b.UpdateStackSet(
		updSetName, "", updTemplateV1, cloudformation.StackSetOptions{Parameters: envParam("z")},
	)
	require.NoError(t, err)
	b.WaitForStackSetOperations()

	_, _, err = b.UpdateStackSet(updSetName, "", updTemplateV2, cloudformation.StackSetOptions{
		Parameters: []cloudformation.Parameter{{ParameterKey: "Env", UsePreviousValue: true}},
	})
	require.NoError(t, err)
	b.WaitForStackSetOperations()

	assert.Equal(t, "z", stackEnv(instanceStack(t, b, updAcct1)))

	ss, err := b.DescribeStackSet(updSetName)
	require.NoError(t, err)
	assert.Equal(t, "z", ss.Parameters[0].ParameterValue)
}
