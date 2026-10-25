package gcpeventarc_test

import (
	"testing"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	ctx := t.Context()

	trig := src.createTrigger(t, "kept-trigger", storageTrigger("finalized", "b", runDestination("receiver", "/p"),
		&eventarcpb.EventFilter{Attribute: "subject", Value: "objects/a/**", Operator: "match-path-pattern"}))
	ch := src.createChannel(t, "kept-channel")

	_, err := src.client.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{
		Resource: trig.GetName(),
		Policy: &iampb.Policy{Bindings: []*iampb.Binding{{
			Role: "roles/eventarc.viewer", Members: []string{"user:a@example.com"},
		}}},
	})
	require.NoError(t, err)

	key := "projects/" + project + "/locations/" + location + "/keyRings/r/cryptoKeys/k"
	_, err = src.client.UpdateGoogleChannelConfig(ctx, &eventarcpb.UpdateGoogleChannelConfigRequest{
		GoogleChannelConfig: &eventarcpb.GoogleChannelConfig{Name: parent + "/googleChannelConfig", CryptoKeyName: key},
		UpdateMask:          &fieldmaskpb.FieldMask{Paths: []string{"crypto_key_name"}},
	})
	require.NoError(t, err)

	connOp, err := src.client.CreateChannelConnection(ctx, &eventarcpb.CreateChannelConnectionRequest{
		Parent: parent, ChannelConnectionId: "kept-link",
		ChannelConnection: &eventarcpb.ChannelConnection{Channel: ch.GetName(), ActivationToken: ch.GetActivationToken()},
	})
	require.NoError(t, err)
	_, err = connOp.Wait(ctx)
	require.NoError(t, err)

	snap := src.handler.Backend.Snapshot(ctx)
	require.NotEmpty(t, snap)

	dst := newEnv(t)
	require.NoError(t, dst.handler.Backend.Restore(ctx, snap))

	got, err := dst.client.GetTrigger(ctx, &eventarcpb.GetTriggerRequest{Name: trig.GetName()})
	require.NoError(t, err)
	assert.Equal(t, trig.GetUid(), got.GetUid())
	assert.Equal(t, trig.GetEtag(), got.GetEtag())
	assert.Equal(t, "/p", got.GetDestination().GetCloudRun().GetPath())
	assert.Equal(t, "match-path-pattern", got.GetEventFilters()[2].GetOperator())
	assert.Equal(t, trig.GetTransport().GetPubsub().GetTopic(), got.GetTransport().GetPubsub().GetTopic())

	pol, err := dst.client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: trig.GetName()})
	require.NoError(t, err)
	require.Len(t, pol.GetBindings(), 1)

	gotCh, err := dst.client.GetChannel(ctx, &eventarcpb.GetChannelRequest{Name: ch.GetName()})
	require.NoError(t, err)
	assert.Equal(t, eventarcpb.Channel_ACTIVE, gotCh.GetState())

	_, err = dst.client.GetChannelConnection(ctx, &eventarcpb.GetChannelConnectionRequest{
		Name: parent + "/channelConnections/kept-link",
	})
	require.NoError(t, err)

	cfg, err := dst.client.GetGoogleChannelConfig(ctx, &eventarcpb.GetGoogleChannelConfigRequest{Name: parent + "/googleChannelConfig"})
	require.NoError(t, err)
	assert.Equal(t, key, cfg.GetCryptoKeyName())

	op, err := dst.client.GetOperation(ctx, opRequest(connOp.Name()))
	require.NoError(t, err)
	assert.True(t, op.GetDone(), "operations survive a restore")
}

func TestRestoreDiscardsIncompatibleSnapshot(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createTrigger(t, "doomed", pubsubTrigger("", runDestination("receiver", "")))

	require.NoError(t, e.handler.Backend.Restore(t.Context(), []byte(`{"version":999,"triggers":{}}`)))

	_, err := e.client.GetTrigger(t.Context(), &eventarcpb.GetTriggerRequest{Name: parent + "/triggers/doomed"})
	assert.Equal(t, codes.NotFound, code(err))
}

func TestReset(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.createTrigger(t, "gone-trigger", pubsubTrigger("", runDestination("receiver", "")))
	e.createChannel(t, "gone-channel")
	e.handler.Reset()

	_, err := e.client.GetTrigger(t.Context(), &eventarcpb.GetTriggerRequest{Name: parent + "/triggers/gone-trigger"})
	assert.Equal(t, codes.NotFound, code(err))

	_, err = e.client.GetChannel(t.Context(), &eventarcpb.GetChannelRequest{Name: parent + "/channels/gone-channel"})
	assert.Equal(t, codes.NotFound, code(err))
}
