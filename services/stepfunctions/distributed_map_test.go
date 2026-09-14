package stepfunctions_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// distributedMapDef is a three-item Map state whose ItemProcessor declares
// ProcessorConfig.Mode=DISTRIBUTED (AWS Step Functions ASL reference, Map
// state -> ItemProcessor -> ProcessorConfig:
// docs.aws.amazon.com/step-functions/latest/dg/state-map-distributed.html).
// Its ItemProcessor just echoes each item back via a Pass state, so the
// parent's own output is the three items in their original order.
const distributedMapDef = `{
	"StartAt": "DistMap",
	"States": {
		"DistMap": {
			"Type": "Map",
			"ItemsPath": "$.items",
			"ItemProcessor": {
				"ProcessorConfig": {"Mode": "DISTRIBUTED", "ExecutionType": "STANDARD"},
				"StartAt": "Echo",
				"States": {"Echo": {"Type": "Pass", "End": true}}
			},
			"End": true
		}
	}
}`

// inlineMapDef is distributedMapDef's INLINE sibling: identical shape, but
// ItemProcessor carries no ProcessorConfig at all, which defaults Mode to
// INLINE per the same ASL reference.
const inlineMapDef = `{
	"StartAt": "InlineMap",
	"States": {
		"InlineMap": {
			"Type": "Map",
			"ItemsPath": "$.items",
			"ItemProcessor": {
				"StartAt": "Echo",
				"States": {"Echo": {"Type": "Pass", "End": true}}
			},
			"End": true
		}
	}
}`

// runMapExecution creates a state machine from def, starts it against
// {"items":[1,2,3]}, and waits for it to leave RUNNING.
func runMapExecution(
	ctx context.Context, t *testing.T, client *sfnsdk.Client, def, namePrefix string,
) (string, string) {
	t.Helper()

	smName := namePrefix + "-" + uuid.NewString()[:8]
	createSM, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
		Name:       aws.String(smName),
		Definition: aws.String(def),
		RoleArn:    aws.String(validRoleARN),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
		StateMachineArn: createSM.StateMachineArn,
		Input:           aws.String(`{"items":[1,2,3]}`),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		desc, descErr := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
			ExecutionArn: startOut.ExecutionArn,
		})

		return descErr == nil && desc.Status != sfntypes.ExecutionStatusRunning
	}, 5*time.Second, 20*time.Millisecond, "execution should leave RUNNING")

	return *startOut.ExecutionArn, *createSM.StateMachineArn
}

func TestListExecutions_MapRunArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		definition   string
		wantChildren int
	}{
		{name: "distributed spawns three children", definition: distributedMapDef, wantChildren: 3},
		{name: "inline spawns no children", definition: inlineMapDef, wantChildren: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := stepfunctions.NewInMemoryBackend()
			h := stepfunctions.NewHandler(backend)
			client := newSFNSDKClient(t, h)
			ctx := t.Context()

			execArn, _ := runMapExecution(ctx, t, client, tt.definition, "map")

			desc, err := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
				ExecutionArn: aws.String(execArn),
			})
			require.NoError(t, err)
			require.Equal(t, sfntypes.ExecutionStatusSucceeded, desc.Status)
			assert.JSONEq(t, `[1,2,3]`, aws.ToString(desc.Output), "parent output preserves item order")

			listMR, err := client.ListMapRuns(ctx, &sfnsdk.ListMapRunsInput{
				ExecutionArn: aws.String(execArn),
			})
			require.NoError(t, err)
			require.Len(t, listMR.MapRuns, 1)
			mapRunArn := aws.ToString(listMR.MapRuns[0].MapRunArn)
			require.NotEmpty(t, mapRunArn)

			listExec, err := client.ListExecutions(ctx, &sfnsdk.ListExecutionsInput{
				MapRunArn: aws.String(mapRunArn),
			})
			require.NoError(t, err)
			require.Len(t, listExec.Executions, tt.wantChildren)

			for _, child := range listExec.Executions {
				assert.Equal(t, mapRunArn, aws.ToString(child.MapRunArn))
				require.NotNil(t, child.ItemCount, "itemCount must be set for a mapRunArn-scoped result")
				assert.Equal(t, int32(1), *child.ItemCount)
			}

			descMR, err := client.DescribeMapRun(ctx, &sfnsdk.DescribeMapRunInput{
				MapRunArn: aws.String(mapRunArn),
			})
			require.NoError(t, err)
			assert.EqualValues(t, tt.wantChildren, descMR.ExecutionCounts.Total)

			if tt.wantChildren == 0 {
				return
			}

			assert.EqualValues(t, tt.wantChildren, descMR.ExecutionCounts.Succeeded)
			assert.EqualValues(t, tt.wantChildren, descMR.ItemCounts.Succeeded)

			childDesc, err := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
				ExecutionArn: listExec.Executions[0].ExecutionArn,
			})
			require.NoError(t, err, "DescribeExecution on a Distributed Map child")
			assert.Equal(t, mapRunArn, aws.ToString(childDesc.MapRunArn))
			assert.Equal(t, sfntypes.ExecutionStatusSucceeded, childDesc.Status)
		})
	}
}

func TestListExecutions_BothArnsRejected(t *testing.T) {
	t.Parallel()

	backend := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(backend)
	client := newSFNSDKClient(t, h)
	ctx := t.Context()

	execArn, smArn := runMapExecution(ctx, t, client, distributedMapDef, "map")

	listMR, err := client.ListMapRuns(ctx, &sfnsdk.ListMapRunsInput{ExecutionArn: aws.String(execArn)})
	require.NoError(t, err)
	require.Len(t, listMR.MapRuns, 1)

	_, err = client.ListExecutions(ctx, &sfnsdk.ListExecutionsInput{
		StateMachineArn: aws.String(smArn),
		MapRunArn:       listMR.MapRuns[0].MapRunArn,
	}, func(o *sfnsdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var validationErr *sfntypes.ValidationException
	require.ErrorAs(t, err, &validationErr,
		"expected a real ValidationException from the SDK deserializer, got: %v", err)
}
