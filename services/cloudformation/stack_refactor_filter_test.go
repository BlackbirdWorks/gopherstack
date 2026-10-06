package cloudformation_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfnsdktypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListStackRefactors_ExecutionStatusFilter checks the filter and the Status/ExecutionStatus split.
func TestListStackRefactors_ExecutionStatusFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		filter []cfnsdktypes.StackRefactorExecutionStatus
		want   int
	}{
		{name: "no_filter", want: 2},
		{name: "available", filter: []cfnsdktypes.StackRefactorExecutionStatus{"AVAILABLE"}, want: 1},
		{name: "execute_complete", filter: []cfnsdktypes.StackRefactorExecutionStatus{"EXECUTE_COMPLETE"}, want: 1},
		{name: "both", filter: []cfnsdktypes.StackRefactorExecutionStatus{"AVAILABLE", "EXECUTE_COMPLETE"}, want: 2},
		{name: "none_match", filter: []cfnsdktypes.StackRefactorExecutionStatus{"ROLLBACK_FAILED"}, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend, client := newTestHandlerAndClientWithBackend(t)
			ctx := t.Context()

			_, err := backend.CreateStackRefactor("pending", nil, nil, false)
			require.NoError(t, err)

			doneID, err := backend.CreateStackRefactor("done", nil, nil, false)
			require.NoError(t, err)
			require.NoError(t, backend.ExecuteStackRefactor(ctx, doneID))

			in := &cfnsdk.ListStackRefactorsInput{ExecutionStatusFilter: tt.filter}

			out, err := client.ListStackRefactors(ctx, in)
			require.NoError(t, err)
			assert.Len(t, out.StackRefactorSummaries, tt.want)

			for _, s := range out.StackRefactorSummaries {
				assert.Equal(t, cfnsdktypes.StackRefactorStatusCreateComplete, s.Status)

				want := cfnsdktypes.StackRefactorExecutionStatusAvailable
				if aws.ToString(s.Description) == "done" {
					want = cfnsdktypes.StackRefactorExecutionStatusExecuteComplete
				}

				assert.Equal(t, want, s.ExecutionStatus)
			}
		})
	}
}
