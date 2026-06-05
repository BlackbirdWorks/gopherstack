package sns

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/events"
)

// snsLambdaEnvelope is the JSON payload delivered to a Lambda function from SNS.
// It wraps the published message in an AWS-spec Records array.
type snsLambdaRecord struct {
	Sns snsLambdaNotification `json:"Sns"`
	// EventSource and EventVersion match the AWS SNS → Lambda envelope spec.
	EventVersion         string `json:"EventVersion"`
	EventSource          string `json:"EventSource"`
	EventSubscriptionArn string `json:"EventSubscriptionArn"`
}

type snsLambdaNotification struct {
	MessageAttributes map[string]snsLambdaMessageAttr `json:"MessageAttributes"`
	Type              string                          `json:"Type"`
	MessageID         string                          `json:"MessageId"`
	TopicArn          string                          `json:"TopicArn"`
	Subject           string                          `json:"Subject,omitempty"`
	Message           string                          `json:"Message"`
	Timestamp         string                          `json:"Timestamp"`
	SignatureVersion  string                          `json:"SignatureVersion"`
	Signature         string                          `json:"Signature"`
	SigningCertURL    string                          `json:"SigningCertUrl"`
	UnsubscribeURL    string                          `json:"UnsubscribeUrl"`
}

type snsLambdaMessageAttr struct {
	Type  string `json:"Type"`
	Value string `json:"Value"`
}

// buildLambdaPayload constructs the SNS → Lambda invocation payload per the AWS spec.
func buildLambdaPayload(
	ev *events.SNSPublishedEvent,
	sub events.SNSSubscriptionSnapshot,
) []byte {
	attrs := make(map[string]snsLambdaMessageAttr, len(ev.Attributes))
	for k, v := range ev.Attributes {
		attrs[k] = snsLambdaMessageAttr{Type: v.DataType, Value: v.StringValue}
	}

	record := snsLambdaRecord{
		EventVersion:         "1.0",
		EventSource:          "aws:sns",
		EventSubscriptionArn: sub.SubscriptionARN,
		Sns: snsLambdaNotification{
			Type:              messageTypeNotification,
			MessageID:         ev.MessageID,
			TopicArn:          ev.TopicARN,
			Subject:           ev.Subject,
			Message:           ev.Message,
			Timestamp:         time.Now().UTC().Format(time.RFC3339),
			SignatureVersion:  "1",
			Signature:         uuid.NewString(),
			SigningCertURL:    "",
			UnsubscribeURL:    "",
			MessageAttributes: attrs,
		},
	}

	payload, err := json.Marshal(map[string]any{"Records": []any{record}})
	if err != nil {
		return []byte(`{"Records":[]}`)
	}

	return payload
}

// deliverToLambdaSubscriptions invokes each Lambda-protocol subscription endpoint.
// It is best-effort: errors are silently dropped so one bad endpoint does not block others.
func (b *InMemoryBackend) deliverToLambdaSubscriptions(ev *events.SNSPublishedEvent) {
	lambda := b.lambdaBackend
	if lambda == nil {
		return
	}

	for _, sub := range ev.Subscriptions {
		if sub.Protocol != protocolLambda {
			continue
		}

		payload := buildLambdaPayload(ev, sub)
		// Fire-and-forget; ignore response and error per SNS semantics.
		_, _, _ = lambda.InvokeFunction(b.svcCtx, sub.Endpoint, snsLambdaInvocationType, payload)
	}
}

// deliverToFirehoseSubscriptions puts each Firehose-protocol subscription message as a batch record.
// The stream name is extracted from the subscription endpoint ARN.
func (b *InMemoryBackend) deliverToFirehoseSubscriptions(ev *events.SNSPublishedEvent) {
	firehose := b.firehoseBackend
	if firehose == nil {
		return
	}

	for _, sub := range ev.Subscriptions {
		if sub.Protocol != protocolFirehose {
			continue
		}

		streamName := firehoseStreamNameFromARN(sub.Endpoint)
		if streamName == "" {
			continue
		}

		// Deliver the raw message body as a single record.
		_, _ = firehose.PutRecordBatch(streamName, [][]byte{[]byte(ev.Message)})
	}
}

// firehoseStreamNameFromARN extracts the delivery stream name from a Firehose ARN.
// ARN format: arn:aws:firehose:<region>:<account>:deliverystream/<name>.
func firehoseStreamNameFromARN(endpoint string) string {
	const prefix = "deliverystream/"
	if idx := strings.Index(endpoint, prefix); idx >= 0 {
		return endpoint[idx+len(prefix):]
	}

	// Fall back to last path segment (URL-style endpoint).
	parts := strings.Split(endpoint, "/")

	return parts[len(parts)-1]
}
