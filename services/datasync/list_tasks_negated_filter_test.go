package datasync_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	datasyncsdk "github.com/aws/aws-sdk-go-v2/service/datasync"
	"github.com/aws/aws-sdk-go-v2/service/datasync/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/datasync"
)

func TestListTasks_LocationIDOperators_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   types.Operator
		loc  string
		want []string
	}{
		{name: "equals source", op: types.OperatorEq, loc: "a", want: []string{"task-a"}},
		{name: "equals destination", op: types.OperatorEq, loc: "dst", want: []string{"task-a", "task-b"}},
		{name: "not equals source", op: types.OperatorNe, loc: "a", want: []string{"task-b"}},
		{name: "not equals destination", op: types.OperatorNe, loc: "dst", want: []string{}},
		{name: "not contains all", op: types.OperatorNotContains, loc: "dst", want: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestDataSyncClient(
				t,
				datasync.NewHandler(datasync.NewInMemoryBackend("000000000000", "us-east-1")),
			)
			ctx := t.Context()
			arns := map[string]string{}

			for _, n := range []string{"a", "b", "dst"} {
				out, err := client.CreateLocationObjectStorage(ctx, &datasyncsdk.CreateLocationObjectStorageInput{
					ServerHostname: aws.String(n + ".example.com"),
					BucketName:     aws.String(n + "-bucket"),
				})
				require.NoError(t, err)

				arns[n] = aws.ToString(out.LocationArn)
			}

			for _, n := range []string{"a", "b"} {
				_, err := client.CreateTask(ctx, &datasyncsdk.CreateTaskInput{
					SourceLocationArn:      aws.String(arns[n]),
					DestinationLocationArn: aws.String(arns["dst"]),
					Name:                   aws.String("task-" + n),
				})
				require.NoError(t, err)
			}

			val := arns[tt.loc]

			out, err := client.ListTasks(ctx, &datasyncsdk.ListTasksInput{
				Filters: []types.TaskFilter{
					{Name: types.TaskFilterNameLocationId, Operator: tt.op, Values: []string{val}},
				},
			})
			require.NoError(t, err)

			got := make([]string, 0)
			for _, task := range out.Tasks {
				got = append(got, aws.ToString(task.Name))
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
