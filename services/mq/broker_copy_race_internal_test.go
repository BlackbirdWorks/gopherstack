package mq

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// A broker copy handed out before the reboot promotion must not alias the
// LogsSummary/Configurations the promotion rewrites.
func TestBrokerCopy_NotAliasedToRebootPromotion(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1")
	br := &Broker{
		BrokerID:    "b-1",
		BrokerState: BrokerStateRebooting,
		LogsSummary: &LogsSummary{Pending: &Logs{General: true, Audit: true}},
		Configurations: &Configurations{
			Current: &ConfigurationID{ID: "c-1", Revision: 1},
			Pending: &ConfigurationID{ID: "c-2", Revision: 2},
		},
	}

	held := b.copyBroker(br)

	stop := make(chan struct{})

	var wg sync.WaitGroup

	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = held.LogsSummary.General
				_ = held.Configurations.Current
				_ = held.Configurations.History
			}
		}
	})

	promoteBrokerReboot(br)
	close(stop)
	wg.Wait()

	assert.False(t, held.LogsSummary.General)
	assert.Equal(t, "c-1", held.Configurations.Current.ID)
	assert.Empty(t, held.Configurations.History)
	assert.True(t, br.LogsSummary.General)
	assert.Equal(t, "c-2", br.Configurations.Current.ID)
}
