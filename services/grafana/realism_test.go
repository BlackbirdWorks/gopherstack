package grafana_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	grafanasdk "github.com/aws/aws-sdk-go-v2/service/grafana"
	"github.com/aws/aws-sdk-go-v2/service/grafana/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealism_CreateWorkspaceValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(in *grafanasdk.CreateWorkspaceInput)
		name   string
	}{
		{
			name:   "name chars",
			mutate: func(in *grafanasdk.CreateWorkspaceInput) { in.WorkspaceName = aws.String("bad name!") },
		},
		{name: "name too long", mutate: func(in *grafanasdk.CreateWorkspaceInput) {
			in.WorkspaceName = aws.String(string(make([]byte, 256)))
		}},
		{name: "data source", mutate: func(in *grafanasdk.CreateWorkspaceInput) {
			in.WorkspaceDataSources = []types.DataSourceType{"MYSQL"}
		}},
		{name: "notification destination", mutate: func(in *grafanasdk.CreateWorkspaceInput) {
			in.WorkspaceNotificationDestinations = []types.NotificationDestinationType{"EMAIL"}
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)
			in := minimalCreateWorkspaceInput()
			tc.mutate(in)

			_, err := client.CreateWorkspace(t.Context(), in)

			var ve *types.ValidationException
			require.ErrorAs(t, err, &ve)
		})
	}
}

func TestRealism_ListPaginationParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   *grafanasdk.ListWorkspacesInput
		name string
	}{
		{name: "bad token", in: &grafanasdk.ListWorkspacesInput{NextToken: aws.String("%%%")}},
		{name: "max results high", in: &grafanasdk.ListWorkspacesInput{MaxResults: aws.Int32(101)}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)

			_, err := client.ListWorkspaces(t.Context(), tc.in)

			var ae smithy.APIError
			require.ErrorAs(t, err, &ae)
			assert.Equal(t, "ValidationException", ae.ErrorCode())
		})
	}
}

func TestRealism_LifecycleDelayHoldsCreating(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	h.Backend.SetLifecycleDelay(time.Hour)

	out, err := client.CreateWorkspace(t.Context(), minimalCreateWorkspaceInput())
	require.NoError(t, err)

	desc, err := client.DescribeWorkspace(
		t.Context(),
		&grafanasdk.DescribeWorkspaceInput{WorkspaceId: out.Workspace.Id},
	)
	require.NoError(t, err)
	assert.Equal(t, types.WorkspaceStatusCreating, desc.Workspace.Status)
}
