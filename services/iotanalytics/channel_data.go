package iotanalytics

import "fmt"

// SampleChannelData returns up to maxMessages sample messages from a channel.
// Returns InvalidRequestException for maxMessages <= 0 or > 10 (AWS behaviour).
func (b *InMemoryBackend) SampleChannelData(channelName string, maxMessages int) ([][]byte, error) {
	b.mu.RLock("SampleChannelData")
	defer b.mu.RUnlock()

	if !b.channels.Has(channelName) {
		return nil, ErrChannelNotFound
	}

	if maxMessages <= 0 || maxMessages > maxSampleMessages {
		return nil, fmt.Errorf("%w: maxMessages must be between 1 and %d", ErrValidation, maxSampleMessages)
	}

	msgs := b.channelMessages[channelName]
	if len(msgs) == 0 {
		return [][]byte{}, nil
	}

	end := min(len(msgs), maxMessages)

	result := make([][]byte, end)
	copy(result, msgs[:end])

	return result, nil
}
