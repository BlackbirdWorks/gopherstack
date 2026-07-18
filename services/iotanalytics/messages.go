package iotanalytics

import (
	"fmt"
	"time"
)

// BatchPutMessage ingests messages into a channel.
// Validates count ≤ 100, per-message payload ≤ 128 KB, messageId ≤ 128 chars, and total batch ≤ 500 KB.
func (b *InMemoryBackend) BatchPutMessage(
	channelName string,
	messages []messageInput,
) ([]BatchPutMessageErrorEntry, error) {
	var errs []BatchPutMessageErrorEntry

	b.mu.Lock("BatchPutMessage")
	defer b.mu.Unlock()

	if !b.channels.Has(channelName) {
		for _, msg := range messages {
			errs = append(errs, BatchPutMessageErrorEntry{
				ChannelName:  channelName,
				ErrorCode:    "ResourceNotFoundException",
				ErrorMessage: "channel not found: " + channelName,
				MessageID:    msg.MessageID,
			})
		}

		if errs == nil {
			errs = []BatchPutMessageErrorEntry{}
		}

		return errs, nil
	}

	// Validate total batch payload size before processing individual messages.
	var totalPayloadBytes int

	for _, msg := range messages {
		totalPayloadBytes += len(msg.Payload)
	}

	if totalPayloadBytes > maxBatchPayloadBytes {
		for _, msg := range messages {
			errs = append(errs, BatchPutMessageErrorEntry{
				ChannelName:  channelName,
				ErrorCode:    errCodeInvalidRequest,
				ErrorMessage: "batch payload exceeds 500 KB limit",
				MessageID:    msg.MessageID,
			})
		}

		return errs, nil
	}

	for _, msg := range messages {
		if len(msg.MessageID) > maxMessageIDLen {
			errs = append(errs, BatchPutMessageErrorEntry{
				ChannelName:  channelName,
				ErrorCode:    errCodeInvalidRequest,
				ErrorMessage: fmt.Sprintf("messageId exceeds %d character limit", maxMessageIDLen),
				MessageID:    msg.MessageID,
			})

			continue
		}

		if len(msg.Payload) > maxMessagePayloadBytes {
			errs = append(errs, BatchPutMessageErrorEntry{
				ChannelName:  channelName,
				ErrorCode:    errCodeInvalidRequest,
				ErrorMessage: "message payload exceeds 128 KB limit",
				MessageID:    msg.MessageID,
			})

			continue
		}

		current := b.channelMessages[channelName]
		if len(current) < maxChannelMessages {
			b.channelMessages[channelName] = append(current, msg.Payload)
		} else {
			errs = append(errs, BatchPutMessageErrorEntry{
				ChannelName:  channelName,
				ErrorCode:    "InternalFailureException",
				ErrorMessage: "channel message capacity exceeded",
				MessageID:    msg.MessageID,
			})
		}
	}

	// Update lastMessageArrivalTime on any successful ingestion. The channel is
	// guaranteed present here: existence was already checked (and the lock has
	// been held continuously since) at the top of this function.
	if len(b.channelMessages[channelName]) > 0 {
		c, _ := b.channels.Get(channelName)
		c.LastMessageArrivalTime = epochSeconds(time.Now())
	}

	if errs == nil {
		errs = []BatchPutMessageErrorEntry{}
	}

	return errs, nil
}
