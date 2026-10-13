package mq

import "fmt"

// RebootBroker simulates a broker reboot. The broker transitions to
// REBOOT_IN_PROGRESS once, then is restored to RUNNING on the next
// DescribeBroker / ListBrokers call so callers can observe the transition.
func (b *InMemoryBackend) RebootBroker(brokerID string) error {
	b.mu.Lock("RebootBroker")
	defer b.mu.Unlock()

	br := b.lookupBroker(brokerID)
	if br == nil {
		return fmt.Errorf("%w: broker %s not found", ErrNotFound, brokerID)
	}

	lb, managed := b.liveBrokerLocked(br.BrokerID)
	if managed && !lb.ready {
		return nil
	}

	br.BrokerState = BrokerStateRebooting

	if managed {
		b.startRebootApplyLocked(br, lb)
	}

	return nil
}
