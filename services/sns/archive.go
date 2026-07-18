package sns

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
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
// archive. Delivery uses the same mechanisms as a normal Publish (HTTP/HTTPS goroutines
// and the event emitter for SQS/Lambda/Firehose).
func (b *InMemoryBackend) replayMessagesToSubscription(
	sub Subscription,
	topicArn string,
	fromTime time.Time,
) {
	var (
		toReplay             []*ArchivedMessage
		emitter              events.EventEmitter[*events.SNSPublishedEvent]
		client               *http.Client
		signer               *notificationSigner
		sqsSender            SQSSender
		topicEffectivePolicy string
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
		client = b.httpClient
		signer = b.signer
		sqsSender = b.sqsSender

		if topic, ok := b.topics.Get(topicArn); ok {
			topicEffectivePolicy = topic.Attributes["EffectiveDeliveryPolicy"]
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

		if sub.Protocol == protocolHTTP || sub.Protocol == protocolHTTPS {
			d := httpDelivery{
				endpoint:             sub.Endpoint,
				body:                 msg.Message,
				subject:              msg.Subject,
				messageID:            msg.MessageID,
				topicARN:             topicArn,
				subscriptionARN:      sub.SubscriptionArn,
				rawDelivery:          sub.RawMessageDelivery,
				redrivePolicy:        sub.RedrivePolicy,
				deliveryPolicy:       sub.DeliveryPolicy,
				topicEffectivePolicy: topicEffectivePolicy,
				sqsSender:            sqsSender,
				signer:               signer,
			}
			deliverHTTPWithMeta(b.svcCtx, d, client, b)
		}

		// Build one shared event for this replayed message and fan it out through
		// the same per-protocol delivery functions Publish uses. Previously replay
		// only reached HTTP/HTTPS (above) and SQS (via the emitter below); a
		// subscription with a ReplayPolicy on Lambda, Firehose, SMS, or Application
		// protocol silently received nothing for archived messages.
		replayEv := b.buildPublishedEvent(
			topicArn, msg.MessageID, msg.Message, msg.Subject, msg.Attributes,
			[]events.SNSSubscriptionSnapshot{subSnap},
		)

		if emitter != nil {
			_ = emitter.Emit(b.svcCtx, replayEv)
		}

		switch sub.Protocol {
		case protocolLambda:
			b.deliverToLambdaSubscriptions(replayEv)
		case protocolFirehose:
			b.deliverToFirehoseSubscriptions(replayEv)
		case protocolSMS:
			b.deliverToSMSSubscriptions(replayEv)
		case protocolApplication:
			b.deliverToApplicationSubscriptions(replayEv)
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
