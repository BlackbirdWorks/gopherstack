package app

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerregistry/armcontainerregistry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
)

type hookSink struct {
	srv  *httptest.Server
	got  []map[string]any
	mu   sync.Mutex
	hdrs []http.Header
}

func newHookSink(t *testing.T) *hookSink {
	t.Helper()

	s := &hookSink{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)

		var m map[string]any
		_ = json.Unmarshal(raw, &m)

		s.mu.Lock()
		s.got = append(s.got, m)
		s.hdrs = append(s.hdrs, r.Header.Clone())
		s.mu.Unlock()

		_, _ = io.WriteString(w, "received")
	}))
	t.Cleanup(s.srv.Close)

	return s
}

func (s *hookSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.got)
}

func TestAzureContainerRegistryWebhooksSDK(t *testing.T) {
	t.Parallel()

	fx := newACRFixture(t)
	ctx := t.Context()
	sink := newHookSink(t)
	hooks := fx.f.NewWebhooksClient()

	poller, err := hooks.BeginCreate(ctx, "sdk-rg", "sdkregistry", "deployhook", armcontainerregistry.WebhookCreateParameters{
		Location: new("local"),
		Properties: &armcontainerregistry.WebhookPropertiesCreateParameters{
			ServiceURI:    new(sink.srv.URL),
			CustomHeaders: map[string]*string{"X-Deploy-Key": new("k1")},
			Scope:         new("app:*"),
			Status:        new(armcontainerregistry.WebhookStatusEnabled),
			Actions: []*armcontainerregistry.WebhookAction{
				new(armcontainerregistry.WebhookActionPush), new(armcontainerregistry.WebhookActionDelete),
			},
		},
	}, nil)
	require.NoError(t, err)

	_, err = poller.PollUntilDone(ctx, nil)
	require.NoError(t, err)

	creds, err := fx.regs.ListCredentials(ctx, "sdk-rg", "sdkregistry", nil)
	require.NoError(t, err)

	c := ocitest.Client{
		T: t, Base: fx.acr.URL, Host: "sdkregistry.azurecr.localhost",
		Prepare: func(r *http.Request) { r.SetBasicAuth(*creds.Username, *creds.Passwords[0].Value) },
	}
	img := c.PushImage("app", "v1")
	c.PushImage("unrelated", "v1")

	require.Eventually(t, func() bool { return sink.count() >= 1 }, 10*time.Second, 5*time.Millisecond)

	ping, err := hooks.Ping(ctx, "sdk-rg", "sdkregistry", "deployhook", nil)
	require.NoError(t, err)
	require.NotEmpty(t, *ping.ID)

	var events []*armcontainerregistry.Event

	require.Eventually(t, func() bool {
		events = nil
		pager := hooks.NewListEventsPager("sdk-rg", "sdkregistry", "deployhook", nil)

		for pager.More() {
			page, pageErr := pager.NextPage(ctx)
			if pageErr != nil {
				return false
			}

			events = append(events, page.Value...)
		}

		return len(events) == 2
	}, 10*time.Second, 5*time.Millisecond)

	byAction := map[string]*armcontainerregistry.Event{}
	for _, ev := range events {
		byAction[*ev.EventRequestMessage.Content.Action] = ev
	}

	push := byAction["push"]
	require.NotNil(t, push)
	assert.Equal(t, img.Digest, *push.EventRequestMessage.Content.Target.Digest)
	assert.Equal(t, "app", *push.EventRequestMessage.Content.Target.Repository)
	assert.Equal(t, "v1", *push.EventRequestMessage.Content.Target.Tag)
	assert.Equal(t, int64(len(img.Body)), *push.EventRequestMessage.Content.Target.Size)
	assert.Equal(t, "PUT", *push.EventRequestMessage.Content.Request.Method)
	assert.Equal(t, "k1", *push.EventRequestMessage.Headers["X-Deploy-Key"])
	assert.Equal(t, sink.srv.URL, *push.EventRequestMessage.RequestURI)
	assert.Equal(t, "200", *push.EventResponseMessage.StatusCode)
	assert.Equal(t, "received", *push.EventResponseMessage.Content)

	pinged := byAction["ping"]
	require.NotNil(t, pinged)
	assert.Equal(t, *ping.ID, *pinged.ID)

	_, err = hooks.Ping(ctx, "sdk-rg", "sdkregistry", "missinghook", nil)
	require.Error(t, err)
}
