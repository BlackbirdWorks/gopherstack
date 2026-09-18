package glue_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_SessionLifecycle proves Session.Status actually transitions:
// previously it was set to PROVISIONING on CreateSession and nothing ever
// advanced it (items_still_open, "NEW gap FOUND... parity-4"). A real client
// polling GetSession must now observe PROVISIONING->READY and, after
// StopSession, STOPPING->STOPPED.
func TestRealClient_SessionLifecycle(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateSession(ctx, &gluesdk.CreateSessionInput{
		Id:      aws.String("lifecycle1"),
		Role:    aws.String("r"),
		Command: &types.SessionCommand{Name: aws.String("glueetl")},
	})
	require.NoError(t, err)

	got, err := client.GetSession(ctx, &gluesdk.GetSessionInput{Id: aws.String("lifecycle1")})
	require.NoError(t, err)
	assert.Equal(t, types.SessionStatusProvisioning, got.Session.Status)

	require.Eventually(t, func() bool {
		s, gErr := client.GetSession(ctx, &gluesdk.GetSessionInput{Id: aws.String("lifecycle1")})
		require.NoError(t, gErr)

		return s.Session.Status == types.SessionStatusReady
	}, 2*time.Second, 10*time.Millisecond, "session never reached READY")

	_, err = client.StopSession(ctx, &gluesdk.StopSessionInput{Id: aws.String("lifecycle1")})
	require.NoError(t, err)

	stopped, err := client.GetSession(ctx, &gluesdk.GetSessionInput{Id: aws.String("lifecycle1")})
	require.NoError(t, err)
	assert.Equal(t, types.SessionStatusStopping, stopped.Session.Status)

	require.Eventually(t, func() bool {
		s, gErr := client.GetSession(ctx, &gluesdk.GetSessionInput{Id: aws.String("lifecycle1")})
		require.NoError(t, gErr)

		return s.Session.Status == types.SessionStatusStopped
	}, 2*time.Second, 10*time.Millisecond, "session stuck in STOPPING")
}
