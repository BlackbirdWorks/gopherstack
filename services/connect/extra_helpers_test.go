package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/require"
)

// newFlow creates a published contact flow and returns its ID.
func (f *fixture) newFlow(t *testing.T, inst, name string) string {
	t.Helper()

	out, err := f.client.CreateContactFlow(t.Context(), &sdk.CreateContactFlowInput{
		InstanceId: aws.String(inst), Name: aws.String(name),
		Type: types.ContactFlowTypeContactFlow, Content: aws.String(validFlow),
	})
	require.NoError(t, err)

	return aws.ToString(out.ContactFlowId)
}

// restored snapshots f and returns a fresh fixture holding the restored state.
func (f *fixture) restored(t *testing.T) *fixture {
	t.Helper()

	snap := f.handler.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	dst := newFixture(t)
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	return dst
}
