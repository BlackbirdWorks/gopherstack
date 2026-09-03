package fis_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	fissdk "github.com/aws/aws-sdk-go-v2/service/fis"
	"github.com/aws/aws-sdk-go-v2/service/fis/types"
	"github.com/stretchr/testify/require"
)

// TestUpdateSafetyLeverState_RealEnvelope proves gopherstack-101r's fix for
// models.go's updateSafetyLeverStateRequest: the real body is {"state": {...}}
// (aws-sdk-go-v2/service/fis@v1.40.4 serializers.go:2100-2105), not an
// "updateSafetyLeverStateInput" envelope. A real SDK client can only ever send
// the real envelope, so this drives the real typed client and asserts the
// change round-trips through GetSafetyLever. Before the fix, the handler read
// an empty updateSafetyLeverStateInputDTO and rejected the request as an
// invalid status.
func TestUpdateSafetyLeverState_RealEnvelope(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client, _ := newTestFISClient(t, h)
	ctx := t.Context()

	out, err := client.UpdateSafetyLeverState(ctx, &fissdk.UpdateSafetyLeverStateInput{
		Id: aws.String("000000000000"),
		State: &types.UpdateSafetyLeverStateInput{
			Status: types.SafetyLeverStatusInputEngaged,
			Reason: aws.String("wire fix round trip"),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.SafetyLever)
	require.NotNil(t, out.SafetyLever.State)
	require.Equal(t, types.SafetyLeverStatusEngaged, out.SafetyLever.State.Status)
	require.Equal(t, "wire fix round trip", aws.ToString(out.SafetyLever.State.Reason))

	got, err := client.GetSafetyLever(ctx, &fissdk.GetSafetyLeverInput{Id: aws.String("000000000000")})
	require.NoError(t, err)
	require.NotNil(t, got.SafetyLever)
	require.NotNil(t, got.SafetyLever.State)
	require.Equal(t, types.SafetyLeverStatusEngaged, got.SafetyLever.State.Status)
}
