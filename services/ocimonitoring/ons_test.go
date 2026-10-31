package ocimonitoring_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	ons "github.com/blackbirdworks/gopherstack/services/ocinotifications"
)

type received struct {
	header http.Header
	body   string
}

// sink is an HTTP endpoint that records every request it receives.
type sink struct {
	requests chan received
	srv      *httptest.Server
}

func newSink(t *testing.T) *sink {
	t.Helper()

	s := &sink{requests: make(chan received, 64)}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		s.requests <- received{header: r.Header.Clone(), body: string(b)}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(s.srv.Close)

	return s
}

func (s *sink) next(t *testing.T) received {
	t.Helper()

	select {
	case r := <-s.requests:
		return r
	case <-t.Context().Done():
		t.Fatal("no request received")

		return received{}
	}
}

func (s *sink) none(t *testing.T) bool {
	t.Helper()

	select {
	case <-s.requests:
		return false
	default:
		return true
	}
}

// onsEnv is a Notifications backend with one confirmed HTTPS subscription feeding a sink.
type onsEnv struct {
	backend *ons.InMemoryBackend
	sink    *sink
	topicID string
}

// newONS starts Notifications, subscribes a sink to a fresh topic and wires the monitoring backend to it.
func newONS(t *testing.T, e *env) *onsEnv {
	t.Helper()

	nb := ons.NewInMemoryBackend()
	nb.SetTransition(0)

	h := ons.NewHandler(nb)
	h.Port = freePort(t)
	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	topic, err := nb.CreateTopic(ons.CreateTopicIn{Name: "alarms", Compartment: testCompartment}, "http://x")
	require.NoError(t, err)

	sk := newSink(t)
	sub, err := nb.CreateSubscription(ons.CreateSubscriptionIn{
		TopicID: topic.TopicID, Compartment: testCompartment, Protocol: "HTTPS", Endpoint: sk.srv.URL + "/hook",
	}, "http://x")
	require.NoError(t, err)
	nb.Kick()

	conf := sk.next(t)
	u, err := url.Parse(conf.header.Get("X-Oci-Ns-Confirmationurl"))
	require.NoError(t, err)

	_, err = nb.ConfirmSubscription(sub.ID, u.Query().Get("token"), "HTTPS")
	require.NoError(t, err)

	e.backend.SetPublisher(func(_ context.Context, topicID, title, body string, asJSON bool) error {
		msgType := ""
		if asJSON {
			msgType = "JSON"
		}

		_, perr := nb.PublishMessage(topicID, msgType, ons.PublishIn{Title: &title, Body: &body})

		return perr
	})
	e.backend.SetTopicChecker(func(id string) bool {
		_, gerr := nb.GetTopic(id, "http://x")

		return gerr == nil
	})

	return &onsEnv{backend: nb, sink: sk, topicID: topic.TopicID}
}
