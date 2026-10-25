package gcpeventarc_test

import (
	"testing"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	locpb "google.golang.org/genproto/googleapis/cloud/location"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const providerName = parent + "/providers/pubsub.googleapis.com"

func (e *env) createChannel(t *testing.T, id string) *eventarcpb.Channel {
	t.Helper()

	op, err := e.client.CreateChannel(t.Context(), &eventarcpb.CreateChannelRequest{
		Parent: parent, ChannelId: id,
		Channel: &eventarcpb.Channel{Provider: parent + "/providers/partner.example.com"},
	})
	require.NoError(t, err)

	ch, err := op.Wait(t.Context())
	require.NoError(t, err)

	return ch
}

func TestChannelLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ch := e.createChannel(t, "partner-channel")

	assert.Equal(t, parent+"/channels/partner-channel", ch.GetName())
	assert.Equal(t, eventarcpb.Channel_PENDING, ch.GetState())
	assert.NotEmpty(t, ch.GetActivationToken())
	assert.NotEmpty(t, ch.GetUid())

	topic := ch.GetPubsubTopic()
	assert.Contains(t, topic, "projects/"+project+"/topics/eventarc-channel-"+location+"-partner-channel-")

	_, err := e.psClient.TopicAdminClient.GetTopic(t.Context(), &pubsubpb.GetTopicRequest{Topic: topic})
	require.NoError(t, err)

	_, err = e.client.CreateChannel(t.Context(), &eventarcpb.CreateChannelRequest{
		Parent: parent, ChannelId: "partner-channel", Channel: &eventarcpb.Channel{},
	})
	assert.Equal(t, codes.AlreadyExists, code(err))

	upOp, err := e.client.UpdateChannel(t.Context(), &eventarcpb.UpdateChannelRequest{
		Channel:    &eventarcpb.Channel{Name: ch.GetName(), Labels: map[string]string{"env": "dev"}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels"}},
	})
	require.NoError(t, err)

	updated, err := upOp.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "dev"}, updated.GetLabels())
	assert.Equal(t, ch.GetActivationToken(), updated.GetActivationToken())

	it := e.client.ListChannels(t.Context(), &eventarcpb.ListChannelsRequest{Parent: parent})
	listed, err := it.Next()
	require.NoError(t, err)
	assert.Equal(t, ch.GetName(), listed.GetName())

	_, err = it.Next()
	assert.Equal(t, iterator.Done, err)

	delOp, err := e.client.DeleteChannel(t.Context(), &eventarcpb.DeleteChannelRequest{Name: ch.GetName()})
	require.NoError(t, err)
	_, err = delOp.Wait(t.Context())
	require.NoError(t, err)

	_, err = e.client.GetChannel(t.Context(), &eventarcpb.GetChannelRequest{Name: ch.GetName()})
	assert.Equal(t, codes.NotFound, code(err))

	_, err = e.psClient.TopicAdminClient.GetTopic(t.Context(), &pubsubpb.GetTopicRequest{Topic: topic})
	assert.Equal(t, codes.NotFound, code(err))
}

func TestChannelConnectionActivation(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ch := e.createChannel(t, "partner-channel")
	connID := "partner-link"

	tests := []struct {
		name  string
		conn  *eventarcpb.ChannelConnection
		want  codes.Code
		state eventarcpb.Channel_State
	}{
		{
			name: "wrong token", want: codes.InvalidArgument, state: eventarcpb.Channel_PENDING,
			conn: &eventarcpb.ChannelConnection{Channel: ch.GetName(), ActivationToken: "wrong"},
		},
		{
			name: "unknown channel", want: codes.NotFound, state: eventarcpb.Channel_PENDING,
			conn: &eventarcpb.ChannelConnection{Channel: parent + "/channels/ghost", ActivationToken: "x"},
		},
		{
			name: "token activates the channel", want: codes.OK, state: eventarcpb.Channel_ACTIVE,
			conn: &eventarcpb.ChannelConnection{Channel: ch.GetName(), ActivationToken: ch.GetActivationToken()},
		},
	}

	for _, tt := range tests {
		op, err := e.client.CreateChannelConnection(t.Context(), &eventarcpb.CreateChannelConnectionRequest{
			Parent: parent, ChannelConnectionId: connID, ChannelConnection: tt.conn,
		})
		if tt.want != codes.OK {
			assert.Equal(t, tt.want, code(err), tt.name)
		} else {
			require.NoError(t, err, tt.name)

			conn, werr := op.Wait(t.Context())
			require.NoError(t, werr)
			assert.Equal(t, parent+"/channelConnections/"+connID, conn.GetName())
			assert.NotEmpty(t, conn.GetUid())
		}

		got, err := e.client.GetChannel(t.Context(), &eventarcpb.GetChannelRequest{Name: ch.GetName()})
		require.NoError(t, err)
		assert.Equal(t, tt.state, got.GetState(), tt.name)
	}

	activated, err := e.client.GetChannel(t.Context(), &eventarcpb.GetChannelRequest{Name: ch.GetName()})
	require.NoError(t, err)
	assert.Empty(t, activated.GetActivationToken(), "an activation token is single use")

	_, err = e.client.CreateChannelConnection(t.Context(), &eventarcpb.CreateChannelConnectionRequest{
		Parent: parent, ChannelConnectionId: connID,
		ChannelConnection: &eventarcpb.ChannelConnection{Channel: ch.GetName(), ActivationToken: "again"},
	})
	assert.Equal(t, codes.AlreadyExists, code(err))

	read, err := e.client.GetChannelConnection(t.Context(), &eventarcpb.GetChannelConnectionRequest{
		Name: parent + "/channelConnections/" + connID,
	})
	require.NoError(t, err)
	assert.Equal(t, ch.GetName(), read.GetChannel())

	list, err := e.client.ListChannelConnections(t.Context(), &eventarcpb.ListChannelConnectionsRequest{Parent: parent}).Next()
	require.NoError(t, err)
	assert.Equal(t, read.GetName(), list.GetName())

	delOp, err := e.client.DeleteChannelConnection(t.Context(), &eventarcpb.DeleteChannelConnectionRequest{Name: read.GetName()})
	require.NoError(t, err)
	_, err = delOp.Wait(t.Context())
	require.NoError(t, err)

	after, err := e.client.GetChannel(t.Context(), &eventarcpb.GetChannelRequest{Name: ch.GetName()})
	require.NoError(t, err)
	assert.Equal(t, eventarcpb.Channel_INACTIVE, after.GetState())
}

func TestChannelUsedByTrigger(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ch := e.createChannel(t, "partner-channel")

	trig := pubsubTrigger("", runDestination("receiver", ""))
	trig.Channel = ch.GetName()
	created := e.createTrigger(t, "channel-trigger", trig)
	assert.Equal(t, ch.GetName(), created.GetChannel())

	_, err := e.client.DeleteChannel(t.Context(), &eventarcpb.DeleteChannelRequest{Name: ch.GetName()})
	assert.Equal(t, codes.FailedPrecondition, code(err))
}

func TestProviders(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	p, err := e.client.GetProvider(t.Context(), &eventarcpb.GetProviderRequest{Name: parent + "/providers/storage.googleapis.com"})
	require.NoError(t, err)
	assert.Equal(t, "Cloud Storage", p.GetDisplayName())

	types := make([]string, 0, len(p.GetEventTypes()))
	for _, et := range p.GetEventTypes() {
		types = append(types, et.GetType())
	}

	assert.ElementsMatch(t, []string{
		"google.cloud.storage.object.v1.finalized", "google.cloud.storage.object.v1.deleted",
		"google.cloud.storage.object.v1.archived", "google.cloud.storage.object.v1.metadataUpdated",
	}, types)

	attrs := p.GetEventTypes()[0].GetFilteringAttributes()
	require.Len(t, attrs, 2)
	assert.Equal(t, "bucket", attrs[0].GetAttribute())
	assert.True(t, attrs[0].GetRequired())
	assert.True(t, attrs[1].GetPathPatternSupported())

	_, err = e.client.GetProvider(t.Context(), &eventarcpb.GetProviderRequest{Name: parent + "/providers/nope.googleapis.com"})
	assert.Equal(t, codes.NotFound, code(err))

	var names []string

	it := e.client.ListProviders(t.Context(), &eventarcpb.ListProvidersRequest{Parent: parent, OrderBy: "name desc"})
	for {
		got, nerr := it.Next()
		if nerr == iterator.Done {
			break
		}

		require.NoError(t, nerr)

		names = append(names, got.GetName())
	}

	assert.Equal(t, []string{parent + "/providers/storage.googleapis.com", providerName}, names)

	filtered, err := e.client.ListProviders(t.Context(), &eventarcpb.ListProvidersRequest{
		Parent: parent, Filter: `displayName:"pub/sub"`,
	}).Next()
	require.NoError(t, err)
	assert.Equal(t, providerName, filtered.GetName())

	_, err = e.client.ListProviders(t.Context(), &eventarcpb.ListProvidersRequest{Parent: parent, Filter: "bogus"}).Next()
	assert.Equal(t, codes.InvalidArgument, code(err))
}

func TestGoogleChannelConfig(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	name := parent + "/googleChannelConfig"

	cfg, err := e.client.GetGoogleChannelConfig(t.Context(), &eventarcpb.GetGoogleChannelConfigRequest{Name: name})
	require.NoError(t, err)
	assert.Equal(t, name, cfg.GetName())
	assert.Empty(t, cfg.GetCryptoKeyName())

	key := "projects/" + project + "/locations/" + location + "/keyRings/ring/cryptoKeys/key"
	updated, err := e.client.UpdateGoogleChannelConfig(t.Context(), &eventarcpb.UpdateGoogleChannelConfigRequest{
		GoogleChannelConfig: &eventarcpb.GoogleChannelConfig{Name: name, CryptoKeyName: key},
		UpdateMask:          &fieldmaskpb.FieldMask{Paths: []string{"crypto_key_name"}},
	})
	require.NoError(t, err)
	assert.Equal(t, key, updated.GetCryptoKeyName())
	assert.True(t, updated.GetUpdateTime().IsValid())

	again, err := e.client.GetGoogleChannelConfig(t.Context(), &eventarcpb.GetGoogleChannelConfigRequest{Name: name})
	require.NoError(t, err)
	assert.Equal(t, key, again.GetCryptoKeyName())

	_, err = e.client.UpdateGoogleChannelConfig(t.Context(), &eventarcpb.UpdateGoogleChannelConfigRequest{
		GoogleChannelConfig: &eventarcpb.GoogleChannelConfig{Name: name, CryptoKeyName: "not-a-key"},
	})
	assert.Equal(t, codes.InvalidArgument, code(err))

	_, err = e.client.GetGoogleChannelConfig(t.Context(), &eventarcpb.GetGoogleChannelConfigRequest{Name: parent + "/googleChannelConfigs"})
	assert.Equal(t, codes.InvalidArgument, code(err))
}

func TestLocations(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	loc, err := e.client.GetLocation(t.Context(), &locpb.GetLocationRequest{Name: parent})
	require.NoError(t, err)
	assert.Equal(t, location, loc.GetLocationId())

	_, err = e.client.GetLocation(t.Context(), &locpb.GetLocationRequest{Name: "projects/p/locations/mars-1"})
	assert.Equal(t, codes.NotFound, code(err))

	it := e.client.ListLocations(t.Context(), &locpb.ListLocationsRequest{Name: "projects/" + project, PageSize: 5})

	var ids []string

	for {
		l, nerr := it.Next()
		if nerr == iterator.Done {
			break
		}

		require.NoError(t, nerr)

		ids = append(ids, l.GetLocationId())
	}

	assert.Contains(t, ids, "global")
	assert.Contains(t, ids, "us-central1")
	assert.Greater(t, len(ids), 5, "later pages are followed")
}

func TestTriggerIAMPolicy(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	created := e.createTrigger(t, "iam-trigger", pubsubTrigger("", runDestination("receiver", "")))

	pol, err := e.client.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: created.GetName()})
	require.NoError(t, err)
	assert.Empty(t, pol.GetBindings())

	set, err := e.client.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
		Resource: created.GetName(),
		Policy: &iampb.Policy{Bindings: []*iampb.Binding{{
			Role: "roles/eventarc.viewer", Members: []string{"user:a@example.com"},
		}}, Etag: pol.GetEtag()},
	})
	require.NoError(t, err)
	assert.NotEqual(t, pol.GetEtag(), set.GetEtag())

	_, err = e.client.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
		Resource: created.GetName(), Policy: &iampb.Policy{Etag: pol.GetEtag()},
	})
	assert.Equal(t, codes.Aborted, code(err), "a stale etag is rejected")

	_, err = e.client.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
		Resource: created.GetName(),
		Policy:   &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/x"}}},
	})
	assert.Equal(t, codes.InvalidArgument, code(err))

	perms, err := e.client.TestIamPermissions(t.Context(), &iampb.TestIamPermissionsRequest{
		Resource: created.GetName(), Permissions: []string{"eventarc.triggers.get"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"eventarc.triggers.get"}, perms.GetPermissions())

	_, err = e.client.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: parent + "/triggers/ghost-trigger"})
	assert.Equal(t, codes.NotFound, code(err))

	_, err = e.client.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: parent})
	assert.Equal(t, codes.InvalidArgument, code(err))
}
