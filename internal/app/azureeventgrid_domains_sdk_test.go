package app

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/eventgrid/azeventgrid"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/eventgrid/armeventgrid/v2"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azureeventgrid"
)

const egDomainRG = "sdk-rg"

type egReceiver struct {
	srv      *httptest.Server
	requests []*http.Request
	bodies   [][]byte
	mu       sync.Mutex
}

func newEGReceiver(t *testing.T) *egReceiver {
	t.Helper()

	r := &egReceiver{}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)

		if req.Header.Get("Aeg-Event-Type") == "SubscriptionValidation" {
			var evs []map[string]any

			_ = json.Unmarshal(body, &evs)
			code, _ := evs[0]["data"].(map[string]any)["validationCode"].(string)
			_ = json.NewEncoder(w).Encode(map[string]string{"validationResponse": code})

			return
		}

		r.mu.Lock()
		r.requests, r.bodies = append(r.requests, req), append(r.bodies, body)
		r.mu.Unlock()
	}))
	t.Cleanup(r.srv.Close)

	return r
}

// events returns the events of every notification received, and the request count.
func (r *egReceiver) events(t *testing.T) ([]map[string]any, int) {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]map[string]any, 0, len(r.bodies))

	for _, b := range r.bodies {
		var arr []map[string]any

		require.NoError(t, json.Unmarshal(b, &arr))

		out = append(out, arr...)
	}

	return out, len(r.bodies)
}

type egDomainStack struct {
	factory *armeventgrid.ClientFactory
	backend *azureeventgrid.InMemoryBackend
	client  *http.Client
	dataURL string
}

func newEGDomainStack(t *testing.T) *egDomainStack {
	t.Helper()

	armSvc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	armH, ok := armSvc.(*azurearm.Handler)
	require.True(t, ok)

	backend := azureeventgrid.NewInMemoryBackend()
	armH.Registry.SetEventGrid(&azureEventGridAdapter{backend: backend})

	e := echo.New()
	e.Any("/*", armH.Handler())

	armSrv := httptest.NewTLSServer(e)
	t.Cleanup(armSrv.Close)

	dataEcho := echo.New()
	dataEcho.Any(
		"/*",
		azureeventgrid.NewHandler(backend, azureeventgrid.Settings{Port: azureeventgrid.DefaultPort}).Handler(),
	)

	dataSrv := httptest.NewTLSServer(dataEcho)
	t.Cleanup(dataSrv.Close)

	opts := &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: armSrv.URL, Audience: "https://management.core.windows.net/"},
		}},
		Transport: armSrv.Client(),
		Retry:     policy.RetryOptions{MaxRetries: -1},
	}

	f, err := armeventgrid.NewClientFactory(redisSDKSub, redisSDKCredential{}, opts)
	require.NoError(t, err)

	return &egDomainStack{factory: f, backend: backend, dataURL: dataSrv.URL, client: dataSrv.Client()}
}

func (s *egDomainStack) publisher(t *testing.T, key string) *azeventgrid.Client {
	t.Helper()

	co := azcore.ClientOptions{Transport: s.client, Retry: policy.RetryOptions{MaxRetries: -1}}

	c, err := azeventgrid.NewClientWithSharedKeyCredential(
		s.dataURL+"/api/events", azcore.NewKeyCredential(key), &azeventgrid.ClientOptions{ClientOptions: co})
	require.NoError(t, err)

	return c
}

func webhookSubscription(
	url string,
	batch int32,
	mappings ...armeventgrid.DeliveryAttributeMappingClassification,
) *armeventgrid.EventSubscription {
	return &armeventgrid.EventSubscription{Properties: &armeventgrid.EventSubscriptionProperties{
		Destination: &armeventgrid.WebHookEventSubscriptionDestination{
			EndpointType: new(armeventgrid.EndpointTypeWebHook),
			Properties: &armeventgrid.WebHookEventSubscriptionDestinationProperties{
				EndpointURL: &url, MaxEventsPerBatch: &batch, DeliveryAttributeMappings: mappings,
			},
		},
	}}
}

func egCustomEvent(topic, id string) map[string]any {
	return map[string]any{
		"topic": topic, "id": id, "subject": "/" + topic + "/" + id, "eventType": "App.Thing",
		"eventTime": "2026-10-20T12:00:00Z", "dataVersion": "1", "data": map[string]any{"n": 1},
	}
}

func TestAzureEventGridDomainsSDK(t *testing.T) {
	t.Parallel()

	s := newEGDomainStack(t)
	ctx := t.Context()
	domains := s.factory.NewDomainsClient()

	poller, err := domains.BeginCreateOrUpdate(ctx, egDomainRG, "dom1", armeventgrid.Domain{
		Location: new("local"),
		Properties: &armeventgrid.DomainProperties{
			InputSchema: new(armeventgrid.InputSchemaEventGridSchema), AutoDeleteTopicWithLastSubscription: new(false),
		},
	}, nil)
	require.NoError(t, err)

	created, err := poller.PollUntilDone(ctx, pollOpts())
	require.NoError(t, err)
	require.NotNil(t, created.Properties)
	assert.NotEmpty(t, *created.Properties.Endpoint)
	assert.True(t, *created.Properties.AutoCreateTopicWithFirstSubscription)
	assert.False(t, *created.Properties.AutoDeleteTopicWithLastSubscription)

	keys, err := domains.ListSharedAccessKeys(ctx, egDomainRG, "dom1", nil)
	require.NoError(t, err)
	require.NotEmpty(t, *keys.Key1)

	rot, err := domains.RegenerateKey(ctx, egDomainRG, "dom1",
		armeventgrid.DomainRegenerateKeyRequest{KeyName: new("key1")}, nil)
	require.NoError(t, err)
	assert.NotEqual(t, *keys.Key1, *rot.Key1)
	assert.Equal(t, *keys.Key2, *rot.Key2)

	pager := domains.NewListByResourceGroupPager(egDomainRG, nil)
	page, err := pager.NextPage(ctx)
	require.NoError(t, err)
	require.Len(t, page.Value, 1)

	topics := s.factory.NewDomainTopicsClient()
	tp, err := topics.BeginCreateOrUpdate(ctx, egDomainRG, "dom1", "orders", nil)
	require.NoError(t, err)
	_, err = tp.PollUntilDone(ctx, pollOpts())
	require.NoError(t, err)

	all, orders, users := newEGReceiver(t), newEGReceiver(t), newEGReceiver(t)
	static := &armeventgrid.StaticDeliveryAttributeMapping{
		Name: new("X-Env"), Type: new(armeventgrid.DeliveryAttributeMappingTypeStatic),
		Properties: &armeventgrid.StaticDeliveryAttributeMappingProperties{Value: new("prod"), IsSecret: new(true)},
	}
	dynamic := &armeventgrid.DynamicDeliveryAttributeMapping{
		Name: new("X-Subject"), Type: new(armeventgrid.DeliveryAttributeMappingTypeDynamic),
		Properties: &armeventgrid.DynamicDeliveryAttributeMappingProperties{SourceField: new("subject")},
	}

	// domain scope, domain topic scope (topic exists) and the generic scope API (topic auto-created)
	ds, err := s.factory.NewDomainEventSubscriptionsClient().BeginCreateOrUpdate(ctx, egDomainRG, "dom1", "all",
		*webhookSubscription(all.srv.URL, 1), nil)
	require.NoError(t, err)
	_, err = ds.PollUntilDone(ctx, pollOpts())
	require.NoError(t, err)

	dts, err := s.factory.NewDomainTopicEventSubscriptionsClient().
		BeginCreateOrUpdate(ctx, egDomainRG, "dom1", "orders",
			"orders-sub", *webhookSubscription(orders.srv.URL, 10, static, dynamic), nil)
	require.NoError(t, err)
	_, err = dts.PollUntilDone(ctx, pollOpts())
	require.NoError(t, err)

	usersScope := "/subscriptions/" + redisSDKSub + "/resourceGroups/" + egDomainRG +
		"/providers/Microsoft.EventGrid/domains/dom1/topics/users"
	gs, err := s.factory.NewEventSubscriptionsClient().BeginCreateOrUpdate(ctx, usersScope, "users-sub",
		*webhookSubscription(users.srv.URL, 1), nil)
	require.NoError(t, err)
	_, err = gs.PollUntilDone(ctx, pollOpts())
	require.NoError(t, err)

	autoTopic, err := topics.Get(ctx, egDomainRG, "dom1", "users", nil)
	require.NoError(t, err, "the first subscription creates the domain topic")
	assert.Equal(t, "users", *autoTopic.Name)

	listed := topics.NewListByDomainPager(egDomainRG, "dom1", nil)
	topicPage, err := listed.NextPage(ctx)
	require.NoError(t, err)
	assert.Len(t, topicPage.Value, 2)

	got, err := s.factory.NewDomainTopicEventSubscriptionsClient().
		Get(ctx, egDomainRG, "dom1", "orders", "orders-sub", nil)
	require.NoError(t, err)
	assert.Equal(t, usersScope[:len(usersScope)-len("users")]+"orders", *got.Properties.Topic)

	wh, ok := got.Properties.Destination.(*armeventgrid.WebHookEventSubscriptionDestination)
	require.True(t, ok)
	assert.Equal(t, int32(10), *wh.Properties.MaxEventsPerBatch)
	require.Len(t, wh.Properties.DeliveryAttributeMappings, 2)

	secret, ok := wh.Properties.DeliveryAttributeMappings[0].(*armeventgrid.StaticDeliveryAttributeMapping)
	require.True(t, ok)
	assert.Nil(t, secret.Properties.Value, "secret values are not returned by GET")

	attrs, err := s.factory.NewDomainTopicEventSubscriptionsClient().GetDeliveryAttributes(
		ctx, egDomainRG, "dom1", "orders", "orders-sub", nil)
	require.NoError(t, err)
	require.Len(t, attrs.Value, 2)
	assert.Equal(t, "prod", *attrs.Value[0].(*armeventgrid.StaticDeliveryAttributeMapping).Properties.Value)

	full, err := s.factory.NewDomainEventSubscriptionsClient().GetFullURL(ctx, egDomainRG, "dom1", "all", nil)
	require.NoError(t, err)
	assert.Equal(t, all.srv.URL, *full.EndpointURL)

	pub := s.publisher(t, *rot.Key1)
	_, err = pub.PublishCustomEventEvents(ctx, []any{
		egCustomEvent("orders", "o1"), egCustomEvent("orders", "o2"), egCustomEvent("users", "u1"),
	}, nil)
	require.NoError(t, err)
	s.backend.RunDue(ctx)

	allEvents, _ := all.events(t)
	assert.Len(t, allEvents, 3)

	orderEvents, orderReqs := orders.events(t)
	assert.Len(t, orderEvents, 2)
	assert.Equal(t, 1, orderReqs, "both order events travel in one batch")
	assert.Equal(t, "prod", orders.requests[0].Header.Get("X-Env"))
	assert.Equal(t, "/orders/o1", orders.requests[0].Header.Get("X-Subject"))

	userEvents, _ := users.events(t)
	require.Len(t, userEvents, 1)
	assert.Equal(t, "/subscriptions/"+redisSDKSub+"/resourceGroups/"+egDomainRG+
		"/providers/Microsoft.EventGrid/domains/dom1/topics/users", userEvents[0]["topic"])

	// autoDeleteTopicWithLastSubscription is false: the topic outlives its subscription
	del, err := s.factory.NewEventSubscriptionsClient().BeginDelete(ctx, usersScope, "users-sub", nil)
	require.NoError(t, err)
	_, err = del.PollUntilDone(ctx, pollOpts())
	require.NoError(t, err)

	_, err = topics.Get(ctx, egDomainRG, "dom1", "users", nil)
	require.NoError(t, err)

	td, err := topics.BeginDelete(ctx, egDomainRG, "dom1", "orders", nil)
	require.NoError(t, err)
	_, err = td.PollUntilDone(ctx, pollOpts())
	require.NoError(t, err)

	_, err = s.factory.NewDomainTopicEventSubscriptionsClient().
		Get(ctx, egDomainRG, "dom1", "orders", "orders-sub", nil)
	require.Error(t, err, "deleting a domain topic removes its subscriptions")

	dd, err := domains.BeginDelete(ctx, egDomainRG, "dom1", nil)
	require.NoError(t, err)
	_, err = dd.PollUntilDone(ctx, pollOpts())
	require.NoError(t, err)

	_, err = domains.Get(ctx, egDomainRG, "dom1", nil)
	require.Error(t, err)
	_, err = s.backend.Domain("dom1")
	require.ErrorIs(t, err, azureeventgrid.ErrDomainNotFound)
}

func TestAzureEventGridDomainAutoManagedTopics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		autoCreate *bool
		autoDelete *bool
		name       string
		createFail bool
		topicLeft  bool
	}{
		{name: "auto create and delete", topicLeft: false},
		{name: "self managed delete", autoDelete: new(false), topicLeft: true},
		{name: "self managed create", autoCreate: new(false), createFail: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newEGDomainStack(t)
			ctx := t.Context()
			rcv := newEGReceiver(t)

			poller, err := s.factory.NewDomainsClient().
				BeginCreateOrUpdate(ctx, egDomainRG, "dom1", armeventgrid.Domain{
					Location: new("local"),
					Properties: &armeventgrid.DomainProperties{
						AutoCreateTopicWithFirstSubscription: tt.autoCreate,
						AutoDeleteTopicWithLastSubscription:  tt.autoDelete,
					},
				}, nil)
			require.NoError(t, err)
			_, err = poller.PollUntilDone(ctx, pollOpts())
			require.NoError(t, err)

			subs := s.factory.NewDomainTopicEventSubscriptionsClient()
			topics := s.factory.NewDomainTopicsClient()

			create, err := subs.BeginCreateOrUpdate(ctx, egDomainRG, "dom1", "orders", "s",
				*webhookSubscription(rcv.srv.URL, 1), nil)
			if tt.createFail {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			_, err = create.PollUntilDone(ctx, pollOpts())
			require.NoError(t, err)

			_, err = topics.Get(ctx, egDomainRG, "dom1", "orders", nil)
			require.NoError(t, err)

			del, err := subs.BeginDelete(ctx, egDomainRG, "dom1", "orders", "s", nil)
			require.NoError(t, err)
			_, err = del.PollUntilDone(ctx, pollOpts())
			require.NoError(t, err)

			_, err = topics.Get(ctx, egDomainRG, "dom1", "orders", nil)
			assert.Equal(t, tt.topicLeft, err == nil)
		})
	}
}
