package bedrockagentcore_test

import (
	"testing"

	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcore"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcore/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersistence_RoundTrip(t *testing.T) {
	t.Parallel()

	src := newFixture(t, defaultControl())
	base := src.clock.now()

	putEvent(t, src, "a", "s", "persist me", base, nil)
	ids := seedRecords(t, src)
	session := startCI(t, src)
	tok := workloadToken(t, src)

	snap := src.backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	dst := newFixture(t, defaultControl())
	require.NoError(t, dst.backend.Restore(t.Context(), snap))

	events, err := dst.client.ListEvents(t.Context(), &sdk.ListEventsInput{
		MemoryId: new(testMemory), ActorId: new("a"), SessionId: new("s"),
	})
	require.NoError(t, err)
	assert.Len(t, events.Events, 1)

	_, err = dst.client.GetMemoryRecord(t.Context(), &sdk.GetMemoryRecordInput{
		MemoryId: new(testMemory), MemoryRecordId: new(ids["r1"]),
	})
	require.NoError(t, err)

	got, err := dst.client.GetCodeInterpreterSession(t.Context(), &sdk.GetCodeInterpreterSessionInput{
		CodeInterpreterIdentifier: new(ciID), SessionId: &session,
	})
	require.NoError(t, err)
	assert.Equal(t, types.CodeInterpreterSessionStatusTerminated, got.Status, "sandboxes do not survive a restart")

	_, err = dst.client.GetResourceApiKey(t.Context(), &sdk.GetResourceApiKeyInput{
		ResourceCredentialProviderName: new("keyp"), WorkloadIdentityToken: &tok,
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err), "the restored token is still valid")

	dst.backend.Reset()

	events, err = dst.client.ListEvents(t.Context(), &sdk.ListEventsInput{
		MemoryId: new(testMemory), ActorId: new("a"), SessionId: new("s"),
	})
	require.NoError(t, err)
	assert.Empty(t, events.Events)
}
