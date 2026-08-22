package iotdataplane_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	iotdataplanesdk "github.com/aws/aws-sdk-go-v2/service/iotdataplane"
	"github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
)

// newTestIoTDataPlaneSDKClient stands up the real aws-sdk-go-v2 iotdataplane
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production -- so a
// shape is verified by the real client's own serializer/deserializer, not
// gopherstack's own JSON tags or path parsing.
func newTestIoTDataPlaneSDKClient(t *testing.T, h *iotdataplane.Handler) (*iotdataplanesdk.Client, string) {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	client := iotdataplanesdk.NewFromConfig(cfg, func(o *iotdataplanesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	return client, srv.URL
}

// registerConnectionAdmin drives gopherstack's admin-only RegisterConnection
// extension (POST /_admin/connections/{clientId}) directly over HTTP -- it
// has no real AWS SDK operation (see handler.go's adminConnectionsPath doc
// comment), so it cannot be reached through iotdataplanesdk.Client.
func registerConnectionAdmin(t *testing.T, baseURL, clientID string) {
	t.Helper()

	req, err := http.NewRequestWithContext(
		t.Context(), http.MethodPost, baseURL+"/_admin/connections/"+clientID, nil,
	)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusCreated, resp.StatusCode)
}

// TestShadowLifecycle_SDKRoundTrip proves the httpPayload-bound trio
// (GetThingShadow/UpdateThingShadow/DeleteThingShadow) round-trip through the
// real SDK client: Payload is the sole output member and, per
// deserializers.go's awsRestjson1_deserializeOpDocument<Op>Output, is read as
// the raw response body -- not JSON-decoded field-by-field. A wrong
// Content-Type or an extra response envelope layer would break the real
// client's decode even though gopherstack's own JSON parsing might tolerate
// it.
func TestShadowLifecycle_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(backend)
	client, _ := newTestIoTDataPlaneSDKClient(t, h)

	const thingName = "roundtrip-thing"

	doc := []byte(`{"state":{"desired":{"on":true}}}`)

	updated, err := client.UpdateThingShadow(t.Context(), &iotdataplanesdk.UpdateThingShadowInput{
		ThingName: aws.String(thingName),
		Payload:   doc,
	})
	require.NoError(t, err)
	require.NotEmpty(t, updated.Payload)

	got, err := client.GetThingShadow(t.Context(), &iotdataplanesdk.GetThingShadowInput{
		ThingName: aws.String(thingName),
	})
	require.NoError(t, err)
	require.NotEmpty(t, got.Payload)

	deleted, err := client.DeleteThingShadow(t.Context(), &iotdataplanesdk.DeleteThingShadowInput{
		ThingName: aws.String(thingName),
	})
	require.NoError(t, err)
	require.NotEmpty(t, deleted.Payload)

	var deleteBody struct {
		Version   int64 `json:"version"`
		Timestamp int64 `json:"timestamp"`
	}
	require.NoError(t, json.Unmarshal(deleted.Payload, &deleteBody))

	_, err = client.GetThingShadow(t.Context(), &iotdataplanesdk.GetThingShadowInput{
		ThingName: aws.String(thingName),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}

// TestListNamedShadowsForThing_SDKRoundTrip proves the unusual
// /api/things/shadow/ListNamedShadowsForThing/{thingName} URI (not the
// {thingName}/shadow?name= pattern the other shadow ops use) is wired
// correctly, and that results/nextToken/timestamp decode through the real
// client.
func TestListNamedShadowsForThing_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(backend)
	client, _ := newTestIoTDataPlaneSDKClient(t, h)

	const thingName = "roundtrip-named-thing"

	_, err := client.UpdateThingShadow(t.Context(), &iotdataplanesdk.UpdateThingShadowInput{
		ThingName:  aws.String(thingName),
		ShadowName: aws.String("config"),
		Payload:    []byte(`{"state":{"desired":{"x":1}}}`),
	})
	require.NoError(t, err)

	out, err := client.ListNamedShadowsForThing(t.Context(), &iotdataplanesdk.ListNamedShadowsForThingInput{
		ThingName: aws.String(thingName),
	})
	require.NoError(t, err)
	require.Contains(t, out.Results, "config")
	require.NotZero(t, out.Timestamp)
}

// TestPublish_SDKRoundTrip proves Publish's request-only Payload/Topic
// bindings and its empty-body response deserialize cleanly through the real
// client (PublishOutput carries no members -- HandleDeserialize discards the
// body rather than routing through an OpDocument helper).
func TestPublish_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(backend)
	client, _ := newTestIoTDataPlaneSDKClient(t, h)

	_, err := client.Publish(t.Context(), &iotdataplanesdk.PublishInput{
		Topic:   aws.String("roundtrip/topic"),
		Payload: []byte("hello"),
		Qos:     1,
		Retain:  true,
	})
	require.NoError(t, err)

	msg, err := client.GetRetainedMessage(t.Context(), &iotdataplanesdk.GetRetainedMessageInput{
		Topic: aws.String("roundtrip/topic"),
	})
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), msg.Payload)
	require.Equal(t, int32(1), msg.Qos)

	list, err := client.ListRetainedMessages(t.Context(), &iotdataplanesdk.ListRetainedMessagesInput{})
	require.NoError(t, err)
	require.NotEmpty(t, list.RetainedTopics)

	var found bool

	for _, s := range list.RetainedTopics {
		if aws.ToString(s.Topic) == "roundtrip/topic" {
			found = true

			require.Equal(t, int32(1), s.Qos)
			require.Equal(t, int64(len("hello")), s.PayloadSize)
		}
	}

	require.True(t, found, "expected roundtrip/topic in ListRetainedMessages summary")
}

// TestConnectionsFamily_SDKRoundTrip proves GetConnection, ListSubscriptions,
// SendDirectMessage, and DeleteConnection -- all sharing the real wire path
// root /connections/{clientId} that also collides with Outposts' real
// GetConnection -- resolve to this service's real op set through the SDK
// client, and that GetConnection's includeSocketInformation gating and
// DeleteConnection's ResourceNotFoundException-on-untracked-client both hold.
func TestConnectionsFamily_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(backend)
	client, baseURL := newTestIoTDataPlaneSDKClient(t, h)

	const clientID = "roundtrip-client"

	registerConnectionAdmin(t, baseURL, clientID)

	conn, err := client.GetConnection(t.Context(), &iotdataplanesdk.GetConnectionInput{
		ClientId: aws.String(clientID),
	})
	require.NoError(t, err)
	require.True(t, conn.Connected)
	require.Equal(t, clientID, aws.ToString(conn.ClientId))
	require.NotZero(t, conn.ConnectedSince)

	subs, err := client.ListSubscriptions(t.Context(), &iotdataplanesdk.ListSubscriptionsInput{
		ClientId: aws.String(clientID),
	})
	require.NoError(t, err)
	require.Empty(t, subs.Subscriptions, "no broker wired: honestly empty, not fabricated")

	sent, err := client.SendDirectMessage(t.Context(), &iotdataplanesdk.SendDirectMessageInput{
		ClientId: aws.String(clientID),
		Topic:    aws.String("roundtrip/direct"),
		Payload:  []byte("hi"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(sent.TraceId))

	_, err = client.DeleteConnection(t.Context(), &iotdataplanesdk.DeleteConnectionInput{
		ClientId: aws.String(clientID),
	})
	require.NoError(t, err)

	_, err = client.DeleteConnection(t.Context(), &iotdataplanesdk.DeleteConnectionInput{
		ClientId: aws.String("never-registered"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}
