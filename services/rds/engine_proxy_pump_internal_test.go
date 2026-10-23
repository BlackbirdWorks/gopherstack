package rds

import (
	"net"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
)

func TestPumpEndsWhenEitherSideCloses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		closeClient bool
	}{
		{name: "client closes", closeClient: true},
		{name: "upstream closes", closeClient: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				clientProxy, clientPeer := net.Pipe()
				upstreamProxy, upstreamPeer := net.Pipe()
				finished := make(chan struct{})

				go func() {
					pump(clientProxy, upstreamProxy, false)
					close(finished)
				}()

				synctest.Wait()

				if tt.closeClient {
					_ = clientPeer.Close()
				} else {
					_ = upstreamPeer.Close()
				}

				synctest.Wait()

				select {
				case <-finished:
				default:
					assert.Fail(t, "pump still running after one side closed")
				}

				_ = clientPeer.Close()
				_ = upstreamPeer.Close()
			})
		})
	}
}
