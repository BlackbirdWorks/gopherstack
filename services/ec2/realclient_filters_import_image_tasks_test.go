package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestRealClient_DescribeImportImageTasksFilters covers
// DescribeImportImageTasks, which previously ignored Filters entirely. This
// backend runs ImportImage synchronously, so every task's Status is the
// constant "completed" -- the task-state filter still exercises real
// filtering behavior on that real, populated field (matching "completed"
// keeps both tasks; matching "active" drops them).
func TestRealClient_DescribeImportImageTasksFilters(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(backend)
	client := newTestEC2Client(t, h)

	task1, err := client.ImportImage(t.Context(), &ec2sdk.ImportImageInput{Description: aws.String("task one")})
	require.NoError(t, err)
	task2, err := client.ImportImage(t.Context(), &ec2sdk.ImportImageInput{Description: aws.String("task two")})
	require.NoError(t, err)

	tests := []struct {
		name    string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "task-state completed",
			filters: []types.Filter{{Name: aws.String("task-state"), Values: []string{"completed"}}},
			want:    []string{aws.ToString(task1.ImportTaskId), aws.ToString(task2.ImportTaskId)},
		},
		{
			name:    "task-state active",
			filters: []types.Filter{{Name: aws.String("task-state"), Values: []string{"active"}}},
			want:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, reqErr := client.DescribeImportImageTasks(
				t.Context(), &ec2sdk.DescribeImportImageTasksInput{Filters: tt.filters},
			)
			require.NoError(t, reqErr)

			got := make([]string, 0, len(out.ImportImageTasks))
			for _, task := range out.ImportImageTasks {
				got = append(got, aws.ToString(task.ImportTaskId))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
