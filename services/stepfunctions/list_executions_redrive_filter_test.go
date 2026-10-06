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

func TestListExecutions_RedriveFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filter      sfntypes.ExecutionRedriveFilter
		name        string
		wantCode    string
		useMapRun   bool
		wantMatches int
	}{
		{
			name:        "not redriven children",
			filter:      sfntypes.ExecutionRedriveFilterNotRedriven,
			useMapRun:   true,
			wantMatches: 3,
		},
		{name: "redriven children", filter: sfntypes.ExecutionRedriveFilterRedriven, useMapRun: true, wantMatches: 0},
		{name: "no filter", useMapRun: true, wantMatches: 3},
		{
			name:     "state machine arn rejected",
			filter:   sfntypes.ExecutionRedriveFilterRedriven,
			wantCode: "ValidationException",
		},
		{name: "unknown value rejected", filter: "BOGUS", useMapRun: true, wantCode: "ValidationException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newSFNSDKClient(t, stepfunctions.NewHandler(stepfunctions.NewInMemoryBackend()))
			ctx := t.Context()

			execArn, smArn := runMapExecution(ctx, t, client, distributedMapDef, "redrive")

			in := &sfnsdk.ListExecutionsInput{RedriveFilter: tt.filter}
			if tt.useMapRun {
				mr, err := client.ListMapRuns(ctx, &sfnsdk.ListMapRunsInput{ExecutionArn: aws.String(execArn)})
				require.NoError(t, err)
				require.Len(t, mr.MapRuns, 1)

				in.MapRunArn = mr.MapRuns[0].MapRunArn
			} else {
				in.StateMachineArn = aws.String(smArn)
			}

			out, err := client.ListExecutions(ctx, in)
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantCode)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.Executions, tt.wantMatches)
		})
	}
}
