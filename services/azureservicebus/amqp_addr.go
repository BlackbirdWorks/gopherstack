package azureservicebus

import (
	"errors"
	"net/url"
	"strings"
)

var errBadAddress = errors.New("azureservicebus: invalid AMQP address")

// amqpAddr is a parsed link address.
type amqpAddr struct {
	ref  EntityRef
	dlq  bool
	mgmt bool
	cbs  bool
}

// parseAMQPAddr understands "queue", "topic", "topic/Subscriptions/sub", each
// optionally suffixed with "/$DeadLetterQueue" or "/$management", plus "$cbs".
func parseAMQPAddr(raw string) (amqpAddr, error) {
	if u, err := url.Parse(raw); err == nil && u.Host != "" {
		raw = u.Path
	}

	segs := strings.Split(strings.Trim(raw, "/"), "/")
	if len(segs) == 1 && strings.EqualFold(segs[0], "$cbs") {
		return amqpAddr{cbs: true}, nil
	}

	var a amqpAddr

	if n := len(segs); n > 1 {
		if strings.EqualFold(segs[n-1], "$management") {
			a.mgmt, segs = true, segs[:n-1]
		}
	}

	if n := len(segs); n > 1 && strings.EqualFold(segs[n-1], "$deadletterqueue") {
		a.dlq, segs = true, segs[:n-1]
	}

	switch {
	case len(segs) == 1 && segs[0] != "":
		a.ref = EntityRef{Queue: segs[0]}
	case len(segs) == 3 && strings.EqualFold(segs[1], "subscriptions"):
		a.ref = EntityRef{Topic: segs[0], Subscription: segs[2]}
	default:
		return amqpAddr{}, errBadAddress
	}

	return a, nil
}

// resolve reports whether the addressed entity exists. A bare name is a queue
// or, failing that, a topic.
func (a *amqpAddr) resolve(b StorageBackend) bool {
	switch {
	case a.ref.Subscription != "":
		return b.SubscriptionExists(a.ref.Topic, a.ref.Subscription)
	case b.QueueExists(a.ref.Queue):
		return true
	case a.ref.Queue != "" && b.TopicExists(a.ref.Queue):
		a.ref = EntityRef{Topic: a.ref.Queue}

		return true
	}

	return false
}
