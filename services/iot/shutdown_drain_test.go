package iot_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
	"github.com/blackbirdworks/gopherstack/services/iot"
)

//nolint:paralleltest // goleak inspects every goroutine in the process
func TestShutdownDrainsSiblingConfirmations(t *testing.T) {
	got, release := make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		got <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	}))

	defer srv.Close()

	h := iot.NewHandler(iot.NewInMemoryBackend(), nil)
	h.EnableRegions()

	base := goleak.IgnoreCurrent()

	_, err := h.BackendFor("eu-west-1").CreateTopicRuleDestination(&iot.CreateTopicRuleDestinationInput{
		DestinationConfiguration: &iot.TopicRuleDestinationConfiguration{
			HTTPURLConfiguration: &iot.HTTPURLDestinationConfiguration{ConfirmationURL: srv.URL},
		},
	})
	require.NoError(t, err)
	<-got

	done := make(chan struct{})

	go func() {
		h.Shutdown(context.Background())
		close(done)
	}()

	testleak.Baseline(t, "iot.(*InMemoryBackend).DrainBackground.func1")
	close(release)
	<-done

	require.NoError(t, goleak.Find(append(testleak.DefaultIgnores(), base)...))
}
