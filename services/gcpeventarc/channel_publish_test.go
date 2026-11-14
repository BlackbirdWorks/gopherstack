package gcpeventarc_test

import (
	"encoding/json/v2"
	"testing"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/eventarc/publishing/apiv1/publishingpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const customType = "com.example.order.created"

func channelEvent(t *testing.T, typ string) string {
	t.Helper()

	raw, err := json.Marshal(map[string]any{
		"id": "evt-1", "source": "//example.com/orders", "specversion": "1.0", "type": typ,
		"subject": "orders/7", "tenant": "acme", "data": map[string]any{"total": 12},
	}, jsonx.V1())
	require.NoError(t, err)

	return string(raw)
}

func channelTrigger(
	ch *eventarcpb.Channel,
	dest *eventarcpb.Destination,
	extra ...*eventarcpb.EventFilter,
) *eventarcpb.Trigger {
	return &eventarcpb.Trigger{
		Channel:      ch.GetName(),
		EventFilters: append([]*eventarcpb.EventFilter{typeFilter(customType)}, extra...),
		Destination:  dest,
	}
}

func TestPublishEventsDeliversToChannelTriggers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		typ     string
		extra   []*eventarcpb.EventFilter
		deliver bool
	}{
		{name: "matching type", typ: customType, deliver: true},
		{name: "extension filter", typ: customType, deliver: true,
			extra: []*eventarcpb.EventFilter{{Attribute: "tenant", Value: "acme"}}},
		{name: "extension mismatch", typ: customType,
			extra: []*eventarcpb.EventFilter{{Attribute: "tenant", Value: "other"}}},
		{name: "other type", typ: "com.example.order.deleted"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			ch := e.createChannel(t, "partner-channel")
			e.createTrigger(t, "orders", channelTrigger(ch, httpDestination(e.dest.srv.URL+"/hook"), tt.extra...))

			_, err := publishingpb.NewPublisherClient(e.conn).PublishEvents(t.Context(),
				&publishingpb.PublishEventsRequest{
					Channel:    ch.GetName(),
					TextEvents: []string{channelEvent(t, tt.typ)},
				})
			require.NoError(t, err)

			if !tt.deliver {
				e.dest.none(t)

				return
			}

			got := e.dest.next(t)
			assert.Equal(t, "/hook", got.path)
			assert.Equal(t, customType, got.header.Get("Ce-Type"))
			assert.Equal(t, "evt-1", got.header.Get("Ce-Id"))
			assert.Equal(t, "//example.com/orders", got.header.Get("Ce-Source"))
			assert.Equal(t, "orders/7", got.header.Get("Ce-Subject"))
			assert.Equal(t, "acme", got.header.Get("Ce-Tenant"))
			assert.JSONEq(t, `{"total":12}`, string(got.body))
		})
	}
}

func TestPublishChannelConnectionEvents(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ch := e.createChannel(t, "partner-channel")
	e.createTrigger(t, "orders", channelTrigger(ch, httpDestination(e.dest.srv.URL)))

	op, err := e.client.CreateChannelConnection(t.Context(), &eventarcpb.CreateChannelConnectionRequest{
		Parent:              parent,
		ChannelConnectionId: "link",
		ChannelConnection: &eventarcpb.ChannelConnection{
			Channel:         ch.GetName(),
			ActivationToken: ch.GetActivationToken(),
		},
	})
	require.NoError(t, err)

	conn, err := op.Wait(t.Context())
	require.NoError(t, err)

	ce := &publishingpb.CloudEvent{
		Id: "evt-2", Source: "//example.com/orders", SpecVersion: "1.0", Type: customType,
		Data: &publishingpb.CloudEvent_TextData{TextData: `{"total":3}`},
	}
	packed, err := anypb.New(ce)
	require.NoError(t, err)

	_, err = publishingpb.NewPublisherClient(e.conn).PublishChannelConnectionEvents(t.Context(),
		&publishingpb.PublishChannelConnectionEventsRequest{
			ChannelConnection: conn.GetName(), Events: []*anypb.Any{packed},
		})
	require.NoError(t, err)

	got := e.dest.next(t)
	assert.Equal(t, "evt-2", got.header.Get("Ce-Id"))
	assert.JSONEq(t, `{"total":3}`, string(got.body))
}

func TestPublishEventsErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	ch := e.createChannel(t, "partner-channel")
	pub := publishingpb.NewPublisherClient(e.conn)

	tests := []struct {
		req  *publishingpb.PublishEventsRequest
		name string
		want codes.Code
	}{
		{name: "missing channel", want: codes.NotFound, req: &publishingpb.PublishEventsRequest{
			Channel: parent + "/channels/ghost", TextEvents: []string{channelEvent(t, customType)},
		}},
		{name: "malformed channel", want: codes.InvalidArgument, req: &publishingpb.PublishEventsRequest{
			Channel: "ghost", TextEvents: []string{channelEvent(t, customType)},
		}},
		{
			name: "no events",
			want: codes.InvalidArgument,
			req:  &publishingpb.PublishEventsRequest{Channel: ch.GetName()},
		},
		{name: "not json", want: codes.InvalidArgument, req: &publishingpb.PublishEventsRequest{
			Channel: ch.GetName(), TextEvents: []string{"nope"},
		}},
		{name: "missing type", want: codes.InvalidArgument, req: &publishingpb.PublishEventsRequest{
			Channel: ch.GetName(), TextEvents: []string{`{"id":"1","source":"s","specversion":"1.0"}`},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := pub.PublishEvents(t.Context(), tt.req)
			assert.Equal(t, tt.want, code(err))
		})
	}
}

func TestChannelTriggersIgnoreDirectEvents(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	require.NoError(t, e.gcs.Bucket("watched").Create(t.Context(), project, nil))

	ch := e.createChannel(t, "partner-channel")
	trig := storageTrigger("finalized", "watched", httpDestination(e.dest.srv.URL))
	trig.Channel = ch.GetName()
	e.createTrigger(t, "on-channel", trig)
	e.createTrigger(t, "direct", storageTrigger("finalized", "watched", httpDestination(e.dest.srv.URL+"/direct")))

	w := e.gcs.Bucket("watched").Object("w.txt").NewWriter(t.Context())
	_, err := w.Write([]byte("x"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	assert.Equal(t, "/direct", e.dest.next(t).path)
	e.dest.none(t)
}
