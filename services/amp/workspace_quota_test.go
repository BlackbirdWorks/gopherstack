package amp_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ampsdk "github.com/aws/aws-sdk-go-v2/service/amp"
	"github.com/aws/aws-sdk-go-v2/service/amp/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const workspaceQuota = 25

func fillWorkspaces(t *testing.T, f *fixture, n int) []string {
	t.Helper()

	ids := make([]string, 0, n)

	for range n {
		out, err := f.client.CreateWorkspace(t.Context(), &ampsdk.CreateWorkspaceInput{})
		require.NoError(t, err)

		ids = append(ids, aws.ToString(out.WorkspaceId))
	}

	return ids
}

func TestCreateWorkspaceQuota(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{name: "twenty sixth rejected", run: func(t *testing.T, f *fixture) {
			t.Helper()

			fillWorkspaces(t, f, workspaceQuota)

			_, err := f.client.CreateWorkspace(t.Context(), &ampsdk.CreateWorkspaceInput{})

			var qe *types.ServiceQuotaExceededException

			require.ErrorAs(t, err, &qe)
			assert.Equal(t, "workspace", aws.ToString(qe.ResourceType))
			assert.Equal(t, "aps", aws.ToString(qe.ServiceCode))
		}},
		{name: "idempotent retry at limit", run: func(t *testing.T, f *fixture) {
			t.Helper()

			first, err := f.client.CreateWorkspace(t.Context(), &ampsdk.CreateWorkspaceInput{
				ClientToken: aws.String("tok-1"),
			})
			require.NoError(t, err)

			fillWorkspaces(t, f, workspaceQuota-1)

			again, err := f.client.CreateWorkspace(t.Context(), &ampsdk.CreateWorkspaceInput{
				ClientToken: aws.String("tok-1"),
			})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(first.WorkspaceId), aws.ToString(again.WorkspaceId))
		}},
		{name: "delete frees a slot", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ids := fillWorkspaces(t, f, workspaceQuota)
			f.settle()

			_, err := f.client.DeleteWorkspace(
				t.Context(), &ampsdk.DeleteWorkspaceInput{WorkspaceId: aws.String(ids[0])})
			require.NoError(t, err)

			_, err = f.client.CreateWorkspace(t.Context(), &ampsdk.CreateWorkspaceInput{})
			require.Error(t, err, "a DELETING workspace still counts")

			f.settle()

			_, err = f.client.CreateWorkspace(t.Context(), &ampsdk.CreateWorkspaceInput{})
			require.NoError(t, err)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newFixture(t))
		})
	}
}
