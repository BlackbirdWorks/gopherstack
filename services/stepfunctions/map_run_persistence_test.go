package stepfunctions_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

func TestMapRun_SurvivesSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		definition string
		wantTotal  int32
	}{
		{name: "distributed", definition: distributedMapDef, wantTotal: 3},
		{name: "inline", definition: inlineMapDef, wantTotal: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := stepfunctions.NewInMemoryBackend()
			ctx := t.Context()
			execArn, _ := runMapExecution(
				ctx,
				t,
				newSFNSDKClient(t, stepfunctions.NewHandler(original)),
				tt.definition,
				"mr",
			)

			restored := stepfunctions.NewInMemoryBackend()
			require.NoError(t, restored.Restore(ctx, original.Snapshot(ctx)))
			client := newSFNSDKClient(t, stepfunctions.NewHandler(restored))

			list, err := client.ListMapRuns(
				ctx,
				&sfnsdk.ListMapRunsInput{ExecutionArn: aws.String(execArn)},
			)
			require.NoError(t, err)
			require.Len(t, list.MapRuns, 1)

			desc, err := client.DescribeMapRun(
				ctx,
				&sfnsdk.DescribeMapRunInput{MapRunArn: list.MapRuns[0].MapRunArn},
			)
			require.NoError(t, err)
			assert.Equal(t, sfntypes.MapRunStatusSucceeded, desc.Status)
			assert.Equal(t, tt.wantTotal, int32(desc.ExecutionCounts.Total))
			assert.EqualValues(t, 3, desc.ItemCounts.Succeeded)
		})
	}
}
