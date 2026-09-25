package swf_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	swfsdk "github.com/aws/aws-sdk-go-v2/service/swf"
	swftypes "github.com/aws/aws-sdk-go-v2/service/swf/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

// TestPollForActivityTask_TaskTokenRequiredMember proves
// PollForActivityTaskOutput.TaskToken (required, api_op_PollForActivityTask.go)
// reaches a real SDK client as a non-nil pointer in both the idle and
// task-available cases. Before this fix, pollForActivityTaskOutput.TaskToken
// carried a `json:"taskToken,omitempty"` tag, so an idle poll (TaskToken ==
// "") dropped the key entirely and the typed client decoded TaskToken as
// nil -- exactly the required-member-absent shape that crashes a caller
// following the real API's own documented contract ("an empty result...
// means that the ActivityTask object has a taskToken with the value of an
// empty string").
func TestPollForActivityTask_TaskTokenRequiredMember(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		seed bool
	}{
		{name: "no_task_available", seed: false},
		{name: "task_available", seed: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			if tt.seed {
				b.EnqueueActivityTaskInternal("dom", "list", "act-1", "MyActivity", "1.0", "payload", "wf-1", "run-1")
			}

			client := newTestSWFSDKClient(t, swf.NewHandler(b))

			out, err := client.PollForActivityTask(t.Context(), &swfsdk.PollForActivityTaskInput{
				Domain:   aws.String("dom"),
				TaskList: &swftypes.TaskList{Name: aws.String("list")},
			})
			require.NoError(t, err)
			require.NotNil(t, out.TaskToken, "TaskToken is a required member; must never decode nil")

			if tt.seed {
				require.NotEmpty(t, *out.TaskToken)
			} else {
				require.Empty(t, *out.TaskToken)
			}
		})
	}
}
