package mediatailor

import (
	"fmt"
)

// --- ChannelPolicy operations ---

// PutChannelPolicy sets a policy on a channel.
func (b *InMemoryBackend) PutChannelPolicy(channelName, policy string) error {
	b.mu.Lock("PutChannelPolicy")
	defer b.mu.Unlock()

	if !b.channels.Has(channelName) {
		return fmt.Errorf("%w: channel %s not found", ErrNotFound, channelName)
	}

	b.channelPolicies[channelName] = policy

	return nil
}

// GetChannelPolicy returns a channel policy.
func (b *InMemoryBackend) GetChannelPolicy(channelName string) (string, error) {
	b.mu.RLock("GetChannelPolicy")
	defer b.mu.RUnlock()

	if !b.channels.Has(channelName) {
		return "", fmt.Errorf("%w: channel %s not found", ErrNotFound, channelName)
	}

	policy, ok := b.channelPolicies[channelName]
	if !ok {
		return "", fmt.Errorf("%w: no policy for channel %s", ErrNotFound, channelName)
	}

	return policy, nil
}

// DeleteChannelPolicy deletes a channel policy.
func (b *InMemoryBackend) DeleteChannelPolicy(channelName string) error {
	b.mu.Lock("DeleteChannelPolicy")
	defer b.mu.Unlock()

	if !b.channels.Has(channelName) {
		return fmt.Errorf("%w: channel %s not found", ErrNotFound, channelName)
	}

	delete(b.channelPolicies, channelName)

	return nil
}
