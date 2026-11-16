package gcpredis_test

import (
	"testing"
	"testing/synctest"

	"cloud.google.com/go/redis/apiv1/redispb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpredis"
)

func TestCloseStopsEngineOfInFlightCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		stop func(b *gcpredis.InMemoryBackend)
		name string
	}{
		{name: "close", stop: (*gcpredis.InMemoryBackend).Close},
		{name: "reset", stop: (*gcpredis.InMemoryBackend).Reset},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := gcpredis.NewInMemoryBackend()
				release := make(chan struct{})
				b.SetJobGate(func() { <-release })

				_, err := b.CreateInstance(&redispb.CreateInstanceRequest{
					Parent: loc, InstanceId: "late", Instance: basic(),
				})
				require.NoError(t, err)

				stopped := make(chan struct{})

				go func() {
					tt.stop(b)
					close(stopped)
				}()

				synctest.Wait()
				close(release)
				<-stopped

				assert.Zero(t, b.LiveEngines())
				b.Close()
			})
		})
	}
}
