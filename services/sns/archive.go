package sns

import (
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/events"
)

// parseReplayFromTimestamp parses the replayFromTimestamp field from a ReplayPolicy JSON string.
// Returns the zero time and an error when the policy is malformed or the timestamp is missing.
func parseReplayFromTimestamp(replayPolicy string) (time.Time, error) {
	if replayPolicy == "" {
		return time.Time{}, nil
	}

	var p struct {
		ReplayFromTimestamp string `json:"replayFromTimestamp"`
	}

	if err := json.Unmarshal([]byte(replayPolicy), &p); err != nil {
		return time.Time{}, fmt.Errorf(
			"%w: ReplayPolicy is not valid JSON: %s",
			ErrInvalidParameter,
			err.Error(),
		)
	}

	if p.ReplayFromTimestamp == "" {
		return time.Time{}, fmt.Errorf(
			"%w: ReplayPolicy must include replayFromTimestamp",
			ErrInvalidParameter,
		)
	}

	ts, err := time.Parse(time.RFC3339, p.ReplayFromTimestamp)
	if err != nil {
		return time.Time{}, fmt.Errorf(
			"%w: ReplayPolicy.replayFromTimestamp is not a valid RFC3339 timestamp: %s",
			ErrInvalidParameter, err.Error(),
		)
	}

	return ts, nil
}

// replayMessagesToSubscription delivers archived messages published at or after
// fromTime to the given subscription. This supports the ReplayPolicy subscription
// attribute: when set, a subscriber receives historical messages from the topic's
// archive. Real AWS restricts message archiving/replay to FIFO topics with an
// application-to-application (A2A) subscription protocol — SQS, Lambda, or
// Firehose (docs.aws.amazon.com/sns/latest/dg/message-archiving-and-replay-topic-owner.html:
// "Amazon SNS message archiving and replay is only available for
// application-to-application (A2A) FIFO topics") — so sub.Protocol here is
// always one of those three; validateReplayPolicyEligibleLocked (subscriptions.go)
// rejects ReplayPolicy on every other protocol/topic combination before this
// function can ever be reached. Delivery uses the same mechanisms as a normal
// Publish: the event emitter for SQS, and the Lambda/Firehose delivery
// functions for those two protocols.
func (b *InMemoryBackend) replayMessagesToSubscription(
	sub Subscription,
	topicArn string,
	fromTime time.Time,
) {
	var (
		toReplay   []*ArchivedMessage
		emitter    events.EventEmitter[*events.SNSPublishedEvent]
		sigVersion string
	)

	func() {
		b.mu.RLock("replayMessages")
		defer b.mu.RUnlock()

		archive := b.topicMessageArchive[topicArn]
		for _, msg := range archive {
			if !msg.Timestamp.Before(fromTime) {
				toReplay = append(toReplay, msg)
			}
		}

		emitter = b.emitter

		if topic, ok := b.topics.Get(topicArn); ok {
			sigVersion = resolveSignatureVersion(topic.Attributes[attrSignatureVersion])
		}
	}()

	for _, msg := range toReplay {
		subSnap := events.SNSSubscriptionSnapshot{
			SubscriptionARN:    sub.SubscriptionArn,
			Protocol:           sub.Protocol,
			Endpoint:           sub.Endpoint,
			FilterPolicy:       sub.FilterPolicy,
			RawMessageDelivery: sub.RawMessageDelivery,
			RedrivePolicy:      sub.RedrivePolicy,
			DeliveryPolicy:     sub.DeliveryPolicy,
		}

		// Build one shared event for this replayed message and fan it out through
		// the same per-protocol delivery functions Publish uses (SQS via the
		// emitter, Lambda/Firehose via their dedicated delivery functions).
		replayEv := b.buildPublishedEvent(
			topicArn, msg.MessageID, msg.Message, msg.Subject, msg.Attributes,
			[]events.SNSSubscriptionSnapshot{subSnap}, sigVersion,
		)

		if emitter != nil {
			_ = emitter.Emit(b.svcCtx, replayEv)
		}

		switch sub.Protocol {
		case protocolLambda:
			b.deliverToLambdaSubscriptions(replayEv)
		case protocolFirehose:
			b.deliverToFirehoseSubscriptions(replayEv)
		}
	}
}

// GetArchivedMessages returns a snapshot of all messages archived for the given
// topic ARN. Returns nil when no messages have been archived (ArchivePolicy not set
// or no messages published yet). Intended for test assertions.
func (b *InMemoryBackend) GetArchivedMessages(topicArn string) []ArchivedMessage {
	b.mu.RLock("GetArchivedMessages")
	defer b.mu.RUnlock()

	archive := b.topicMessageArchive[topicArn]
	if len(archive) == 0 {
		return nil
	}

	result := make([]ArchivedMessage, len(archive))
	for i, msg := range archive {
		result[i] = *msg
	}

	return result
}

func (b *InMemoryBackend) archivePublishedMessage(
	topicArn, messageID, message, subject string,
	attrs map[string]MessageAttribute,
) {
	attrsCopy := make(map[string]MessageAttribute, len(attrs))
	maps.Copy(attrsCopy, attrs)

	b.mu.Lock("archiveMessage")
	defer b.mu.Unlock()

	archive := b.topicMessageArchive[topicArn]
	if len(archive) >= maxArchivedMessagesPerTopic {
		overage := len(archive) - maxArchivedMessagesPerTopic + 1
		archive = archive[overage:]
	}

	b.topicMessageArchive[topicArn] = append(archive, &ArchivedMessage{
		MessageID:  messageID,
		Message:    message,
		Subject:    subject,
		Attributes: attrsCopy,
		Timestamp:  time.Now().UTC(),
	})
}
