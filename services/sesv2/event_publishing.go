package sesv2

import (
	"encoding/json"
	"net/mail"
	"slices"
	"time"
)

type eventMail struct {
	Tags             map[string][]string `json:"tags,omitempty"`
	Timestamp        string              `json:"timestamp"`
	Source           string              `json:"source"`
	SendingAccountID string              `json:"sendingAccountId"`
	MessageID        string              `json:"messageId"`
	Destination      []string            `json:"destination"`
}

type eventDelivery struct {
	Timestamp  string   `json:"timestamp"`
	Recipients []string `json:"recipients"`
}

// publishSendEvents delivers Send and Delivery events to the SNS destinations
// of the message's configuration set. Event shapes follow the SES developer
// guide's "event-publishing-retrieving-sns-examples" page.
func (b *InMemoryBackend) publishSendEvents(email Email) {
	cs := email.Options.ConfigurationSetName
	if cs == "" {
		return
	}

	b.mu.RLock("publishSendEvents")
	pub := b.snsPublisher
	dests := slices.Clone(b.eventDestinationsByConfigSet.Get(cs))
	account := b.accountID
	b.mu.RUnlock()

	if pub == nil {
		return
	}

	ts := email.Timestamp.UTC().Format(time.RFC3339)
	tags := map[string][]string{"ses:configuration-set": {cs}}

	for _, t := range email.Tags {
		tags[t.Name] = append(tags[t.Name], t.Value)
	}

	source := email.From
	if a, err := mail.ParseAddress(email.From); err == nil {
		source = a.Address
	}

	m := eventMail{
		Timestamp: ts, Source: source, SendingAccountID: account,
		MessageID: email.MessageID, Destination: email.To, Tags: tags,
	}

	for _, d := range dests {
		if !d.Enabled || d.SnsDestination == nil || d.SnsDestination.TopicArn == "" {
			continue
		}

		if slices.Contains(d.MatchingEventTypes, "SEND") {
			payload, _ := json.Marshal(map[string]any{"eventType": "Send", "mail": m, "send": struct{}{}})
			_ = pub.PublishToTopic(d.SnsDestination.TopicArn, string(payload))
		}

		if slices.Contains(d.MatchingEventTypes, "DELIVERY") {
			payload, _ := json.Marshal(map[string]any{
				"eventType": "Delivery", "mail": m,
				"delivery": eventDelivery{Timestamp: ts, Recipients: email.To},
			})
			_ = pub.PublishToTopic(d.SnsDestination.TopicArn, string(payload))
		}
	}
}
