package iotanalytics

import "fmt"

// SampleChannelData returns up to maxMessages sample messages from a channel,
// restricted to [startTime, endTime] when hasStart/hasEnd are set (real
// SampleChannelDataInput.StartTime/EndTime, api_op_SampleChannelData.go:45,56).
// Returns InvalidRequestException for maxMessages <= 0 or > 10 (AWS behaviour).
func (b *InMemoryBackend) SampleChannelData(
	channelName string,
	maxMessages int,
	hasStart bool, startTime float64,
	hasEnd bool, endTime float64,
) ([][]byte, error) {
	b.mu.RLock("SampleChannelData")
	defer b.mu.RUnlock()

	if !b.channels.Has(channelName) {
		return nil, ErrChannelNotFound
	}

	if maxMessages <= 0 || maxMessages > maxSampleMessages {
		return nil, fmt.Errorf("%w: maxMessages must be between 1 and %d", ErrValidation, maxSampleMessages)
	}

	msgs := b.channelMessages[channelName]
	result := make([][]byte, 0, min(len(msgs), maxMessages))

	for _, msg := range msgs {
		if len(result) >= maxMessages {
			break
		}

		if hasStart && msg.ArrivedAt < startTime {
			continue
		}

		if hasEnd && msg.ArrivedAt > endTime {
			continue
		}

		result = append(result, msg.Payload)
	}

	return result, nil
}
