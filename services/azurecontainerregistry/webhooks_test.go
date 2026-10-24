package azurecontainerregistry_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry/ocitest"
	acr "github.com/blackbirdworks/gopherstack/services/azurecontainerregistry"
)

const (
	pollTimeout = 10 * time.Second
	pollTick    = 5 * time.Millisecond
	helmConfig  = "application/vnd.cncf.helm.config.v1+json"
)

type delivery struct {
	header  http.Header
	content acr.EventContent
}

type receiver struct {
	srv   *httptest.Server
	reply string
	got   []delivery
	mu    sync.Mutex
}

func newReceiver(t *testing.T) *receiver {
	t.Helper()

	r := &receiver{reply: "ack"}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		raw, _ := io.ReadAll(req.Body)

		var d delivery

		d.header = req.Header.Clone()
		_ = json.Unmarshal(raw, &d.content)

		r.mu.Lock()
		r.got = append(r.got, d)
		reply := r.reply
		r.mu.Unlock()

		_, _ = io.WriteString(w, reply)
	}))
	t.Cleanup(r.srv.Close)

	return r
}

func (r *receiver) deliveries() []delivery {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]delivery(nil), r.got...)
}

func (r *receiver) waitFor(t *testing.T, n int) []delivery {
	t.Helper()

	require.Eventually(t, func() bool { return len(r.deliveries()) >= n }, pollTimeout, pollTick)

	return r.deliveries()
}

func pushHelmChart(t *testing.T, c ocitest.Client, repo, tag string) ocitest.Image {
	t.Helper()

	cfg := []byte(`{"name":"chart"}`)
	cfgDigest := c.PushBlob(repo, cfg, 0)
	layer := []byte("chart layer")
	layerDigest := c.PushBlob(repo, layer, 0)

	body, err := json.Marshal(map[string]any{
		"schemaVersion": 2, "mediaType": ocitest.ManifestMediaType,
		"config": map[string]any{"mediaType": helmConfig, "size": len(cfg), "digest": cfgDigest},
		"layers": []any{map[string]any{
			"mediaType": "application/vnd.cncf.helm.chart.content.v1.tar+gzip", "size": len(layer),
			"digest": layerDigest,
		}},
	})
	require.NoError(t, err)

	put := c.Do(http.MethodPut, "/v2/"+repo+"/manifests/"+tag, body,
		map[string]string{"Content-Type": ocitest.ManifestMediaType})
	require.Equal(t, http.StatusCreated, put.Status, string(put.Body))

	return ocitest.Image{Digest: ocitest.Digest(body), Body: body}
}

func TestWebhookDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		hook    func(uri string) acr.Webhook
		name    string
		wantAct string
		chart   bool
		del     bool
		want    bool
	}{
		{
			name: "push", wantAct: "push", want: true,
			hook: func(uri string) acr.Webhook {
				return acr.Webhook{Name: "h", ServiceURI: uri, Status: "enabled", Actions: []string{"push"}}
			},
		},
		{
			name: "delete", wantAct: "delete", del: true, want: true,
			hook: func(uri string) acr.Webhook {
				return acr.Webhook{Name: "h", ServiceURI: uri, Status: "enabled", Actions: []string{"delete"}}
			},
		},
		{
			name: "chart push", wantAct: "chart_push", chart: true, want: true,
			hook: func(uri string) acr.Webhook {
				return acr.Webhook{Name: "h", ServiceURI: uri, Status: "enabled", Actions: []string{"chart_push"}}
			},
		},
		{
			name: "chart delete", wantAct: "chart_delete", chart: true, del: true, want: true,
			hook: func(uri string) acr.Webhook {
				return acr.Webhook{Name: "h", ServiceURI: uri, Status: "enabled", Actions: []string{"chart_delete"}}
			},
		},
		{
			name: "image push skips chart action", want: false, chart: true,
			hook: func(uri string) acr.Webhook {
				return acr.Webhook{Name: "h", ServiceURI: uri, Status: "enabled", Actions: []string{"push"}}
			},
		},
		{
			name: "delete action skips push", want: false,
			hook: func(uri string) acr.Webhook {
				return acr.Webhook{Name: "h", ServiceURI: uri, Status: "enabled", Actions: []string{"delete"}}
			},
		},
		{
			name: "disabled", want: false,
			hook: func(uri string) acr.Webhook {
				return acr.Webhook{Name: "h", ServiceURI: uri, Status: "disabled", Actions: []string{"push"}}
			},
		},
		{
			name: "scope repo implies latest", wantAct: "push", want: true,
			hook: func(uri string) acr.Webhook {
				return acr.Webhook{
					Name: "h", ServiceURI: uri, Status: "enabled", Actions: []string{"push"}, Scope: "app",
				}
			},
		},
		{
			name: "scope other tag", want: false,
			hook: func(uri string) acr.Webhook {
				return acr.Webhook{
					Name: "h", ServiceURI: uri, Status: "enabled", Actions: []string{"push"}, Scope: "app:v9",
				}
			},
		},
		{
			name: "scope other repo", want: false,
			hook: func(uri string) acr.Webhook {
				return acr.Webhook{
					Name: "h", ServiceURI: uri, Status: "enabled", Actions: []string{"push"}, Scope: "other:*",
				}
			},
		},
		{
			name: "scope all tags", wantAct: "push", want: true,
			hook: func(uri string) acr.Webhook {
				return acr.Webhook{
					Name: "h", ServiceURI: uri, Status: "enabled", Actions: []string{"push"}, Scope: "ap*:*",
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, "")
			creds := e.registry(t, "hookreg", true)
			c := e.client(t, "hookreg", basic(creds.Username, creds.Password1))
			rcv := newReceiver(t)

			require.NoError(t, e.h.Backend.SetWebhook("hookreg", tt.hook(rcv.srv.URL)))

			var img ocitest.Image
			if tt.chart {
				img = pushHelmChart(t, c, "app", "latest")
			} else {
				img = c.PushImage("app", "latest")
			}

			if tt.del {
				got := c.Do(http.MethodDelete, "/v2/app/manifests/"+img.Digest, nil, nil)
				require.Equal(t, http.StatusAccepted, got.Status)
			}

			if !tt.want {
				require.NoError(t, e.h.Backend.SetWebhook("hookreg", acr.Webhook{
					Name: "sentinel", ServiceURI: rcv.srv.URL, Status: "enabled", Actions: []string{"push"},
					Headers: map[string]string{"X-Sentinel": "1"},
				}))
				c.PushImage("sentinel", "latest")

				require.Eventually(t, func() bool {
					for _, d := range rcv.deliveries() {
						if d.header.Get("X-Sentinel") == "1" {
							return true
						}
					}

					return false
				}, pollTimeout, pollTick)

				for _, d := range rcv.deliveries() {
					assert.NotEqual(t, "app", d.content.Target.Repository, "filtered event must not be delivered")
				}

				return
			}

			got := rcv.waitFor(t, 1)
			require.Len(t, got, 1)
			assert.Equal(t, tt.wantAct, got[0].content.Action)
			assert.Equal(t, "app", got[0].content.Target.Repository)
			assert.Equal(t, img.Digest, got[0].content.Target.Digest)
		})
	}
}

func TestWebhookPayloadAndEvents(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	creds := e.registry(t, "payloadreg", true)
	c := e.client(t, "payloadreg", basic(creds.Username, creds.Password1))
	rcv := newReceiver(t)
	rcv.reply = "thanks"

	require.NoError(t, e.h.Backend.SetWebhook("payloadreg", acr.Webhook{
		Name: "deploy", ServiceURI: rcv.srv.URL + "/hook", Status: "enabled", Actions: []string{"push"},
		Headers: map[string]string{"X-Token": "s3cret"},
	}))

	img := c.PushImage("team/app", "v1")
	got := rcv.waitFor(t, 1)[0]

	t.Run("body", func(t *testing.T) {
		t.Parallel()

		ev := got.content
		assert.NotEmpty(t, ev.ID)
		assert.Equal(t, "push", ev.Action)

		_, err := time.Parse(time.RFC3339Nano, ev.Timestamp)
		require.NoError(t, err)

		require.NotNil(t, ev.Target)
		assert.Equal(t, ocitest.ManifestMediaType, ev.Target.MediaType)
		assert.Equal(t, int64(len(img.Body)), ev.Target.Size)
		assert.Equal(t, ev.Target.Size, ev.Target.Length)
		assert.Equal(t, img.Digest, ev.Target.Digest)
		assert.Equal(t, "team/app", ev.Target.Repository)
		assert.Equal(t, "v1", ev.Target.Tag)

		require.NotNil(t, ev.Request)
		assert.NotEmpty(t, ev.Request.ID)
		assert.Equal(t, host("payloadreg"), ev.Request.Host)
		assert.Equal(t, http.MethodPut, ev.Request.Method)
		assert.NotEmpty(t, ev.Request.Useragent)
	})

	t.Run("headers", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "s3cret", got.header.Get("X-Token"))
		assert.Equal(t, "application/json", got.header.Get("Content-Type"))
	})

	t.Run("events", func(t *testing.T) {
		t.Parallel()

		var events []acr.WebhookEvent

		require.Eventually(t, func() bool {
			var err error

			events, err = e.h.Backend.WebhookEvents("payloadreg", "deploy")

			return err == nil && len(events) == 1
		}, pollTimeout, pollTick)

		ev := events[0]
		assert.Equal(t, got.content.ID, ev.ID)
		assert.Equal(t, got.content.ID, ev.EventRequestMessage.Content.ID)
		assert.Equal(t, http.MethodPost, ev.EventRequestMessage.Method)
		assert.Equal(t, rcv.srv.URL+"/hook", ev.EventRequestMessage.RequestURI)
		assert.Equal(t, "s3cret", ev.EventRequestMessage.Headers["X-Token"])

		require.NotNil(t, ev.EventResponseMessage)
		assert.Equal(t, "200", ev.EventResponseMessage.StatusCode)
		assert.Equal(t, "OK", ev.EventResponseMessage.ReasonPhrase)
		assert.Equal(t, "thanks", ev.EventResponseMessage.Content)
	})

	t.Run("unknown webhook", func(t *testing.T) {
		t.Parallel()

		_, err := e.h.Backend.WebhookEvents("payloadreg", "missing")
		require.ErrorIs(t, err, acr.ErrWebhookNotFound)
	})
}

func TestWebhookUnreachableRecorded(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	creds := e.registry(t, "deadreg", true)
	c := e.client(t, "deadreg", basic(creds.Username, creds.Password1))

	gone := httptest.NewServer(http.NotFoundHandler())
	uri := gone.URL
	gone.Close()

	require.NoError(t, e.h.Backend.SetWebhook("deadreg", acr.Webhook{
		Name: "dead", ServiceURI: uri, Status: "enabled", Actions: []string{"push"},
	}))
	c.PushImage("app", "v1")

	require.Eventually(t, func() bool {
		events, err := e.h.Backend.WebhookEvents("deadreg", "dead")

		return err == nil && len(events) == 1
	}, pollTimeout, pollTick)

	events, err := e.h.Backend.WebhookEvents("deadreg", "dead")
	require.NoError(t, err)
	require.NotNil(t, events[0].EventResponseMessage)
	assert.Empty(t, events[0].EventResponseMessage.StatusCode)
	assert.NotEmpty(t, events[0].EventResponseMessage.ReasonPhrase)
}

func TestWebhookPing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		reg     string
		hook    string
	}{
		{name: "ok", reg: "pingreg", hook: "pinger"},
		{name: "missing webhook", reg: "pingreg", hook: "nope", wantErr: acr.ErrWebhookNotFound},
		{name: "missing registry", reg: "nopereg", hook: "pinger", wantErr: acr.ErrRegistryNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, "")
			e.registry(t, "pingreg", true)
			rcv := newReceiver(t)

			require.NoError(t, e.h.Backend.SetWebhook("pingreg", acr.Webhook{
				Name: "pinger", ServiceURI: rcv.srv.URL, Status: "disabled", Actions: []string{"push"},
			}))

			id, err := e.h.Backend.PingWebhook(tt.reg, tt.hook)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Empty(t, rcv.deliveries())

				return
			}

			require.NoError(t, err)
			require.NotEmpty(t, id)

			got := rcv.waitFor(t, 1)[0].content
			assert.Equal(t, "ping", got.Action)
			assert.Equal(t, id, got.ID)

			require.Eventually(t, func() bool {
				events, evErr := e.h.Backend.WebhookEvents("pingreg", "pinger")

				return evErr == nil && len(events) == 1 && events[0].ID == id
			}, pollTimeout, pollTick)
		})
	}
}

func TestWebhookEventsPersisted(t *testing.T) {
	t.Parallel()

	e := newEnv(t, "")
	creds := e.registry(t, "persistreg", true)
	c := e.client(t, "persistreg", basic(creds.Username, creds.Password1))
	rcv := newReceiver(t)

	require.NoError(t, e.h.Backend.SetWebhook("persistreg", acr.Webhook{
		Name: "keeper", ServiceURI: rcv.srv.URL, Status: "enabled", Actions: []string{"push"},
		Headers: map[string]string{"X-K": "v"},
	}))
	c.PushImage("app", "v1")

	require.Eventually(t, func() bool {
		events, err := e.h.Backend.WebhookEvents("persistreg", "keeper")

		return err == nil && len(events) == 1
	}, pollTimeout, pollTick)

	snap := e.h.Backend.Snapshot(t.Context())

	restored := acr.NewInMemoryBackend()
	restored.SetContext(t.Context())
	t.Cleanup(restored.Close)
	require.NoError(t, restored.Restore(t.Context(), snap))

	events, err := restored.WebhookEvents("persistreg", "keeper")
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "200", events[0].EventResponseMessage.StatusCode)
	assert.True(t, restored.HasWebhooks("persistreg"))

	require.NoError(t, restored.DeleteWebhook("persistreg", "keeper"))
	assert.False(t, restored.HasWebhooks("persistreg"))
}
