package azureeventgrid

import (
	"bytes"
	"context"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const contentJSON = "application/json; charset=utf-8"

func (j job) eventObject() []byte { return j.ds[0].Event }

func (j job) eventArray() []byte {
	events := make([][]byte, 0, len(j.ds))
	for _, d := range j.ds {
		events = append(events, d.Event)
	}

	return bytes.Join([][]byte{[]byte("["), bytes.Join(events, []byte(",")), []byte("]")}, nil)
}

func (j job) maxAttempts() int {
	n := 0
	for _, d := range j.ds {
		n = max(n, d.Attempts)
	}

	return n
}

func (b *InMemoryBackend) deliverWebhook(ctx context.Context, j job) outcome {
	body, ctype := j.eventArray(), contentJSON

	if j.spec.EventDeliverySchema == SchemaCloudEvent {
		body, ctype = j.eventObject(), "application/cloudevents+json; charset=utf-8"

		if maxEvents, _ := j.spec.Destination.batchLimits(); maxEvents > 1 {
			body, ctype = j.eventArray(), "application/cloudevents-batch+json; charset=utf-8"
		}
	}

	target, err := parseWebhookURL(j.spec.Destination.str(propEndpointURL))
	if err != nil {
		return outcome{label: outcomeGeneric, permanent: true}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(body))
	if err != nil {
		return outcome{label: outcomeGeneric, permanent: true}
	}

	maps.Copy(req.Header, j.spec.Destination.webhookHeaders(j.ds[0].Event))

	req.Header.Set("Content-Type", ctype)
	req.Header.Set("Aeg-Event-Type", "Notification")
	req.Header.Set("Aeg-Subscription-Name", strings.ToUpper(j.sub))
	req.Header.Set("Aeg-Delivery-Count", strconv.Itoa(j.maxAttempts()))
	req.Header.Set("Aeg-Metadata-Version", "1")

	resp, err := b.httpc.Do(req)
	if err != nil {
		return outcome{label: "Aborted"}
	}

	defer resp.Body.Close()

	return statusOutcome(resp.StatusCode)
}

func statusOutcome(code int) outcome {
	switch {
	case code >= http.StatusOK && code < http.StatusMultipleChoices:
		return outcome{ok: true, label: outcomeSuccess}
	case code == http.StatusBadRequest:
		return outcome{label: outcomeBadRequest, permanent: true}
	case code == http.StatusRequestEntityTooLarge:
		return outcome{label: "PayloadTooLarge", permanent: true}
	case code == http.StatusUnauthorized:
		return outcome{label: "Unauthorized"}
	case code == http.StatusNotFound:
		return outcome{label: outcomeNotFound}
	case code == http.StatusRequestTimeout:
		return outcome{label: "Timeout"}
	}

	return outcome{label: outcomeGeneric}
}

func (j job) message(body []byte) Message {
	return Message{
		ID:          j.ds[0].ID,
		Subject:     strField(decodeFields(j.ds[0].Event), "subject"),
		ContentType: "application/json",
		Body:        body,
	}
}

func (b *InMemoryBackend) targetsSnapshot() Targets {
	b.mu.RLock("targets")
	defer b.mu.RUnlock()

	return b.targets
}

func (b *InMemoryBackend) deliverTarget(j job) outcome {
	d, tg := j.spec.Destination, b.targetsSnapshot()
	rid := d.str(propResourceID)

	var err error

	switch d.EndpointType {
	case EndpointServiceBusQ:
		err = tg.SendServiceBus(resourceSegment(rid, "queues"), "", j.message(j.eventObject()))
	case EndpointServiceBusT:
		err = tg.SendServiceBus("", resourceSegment(rid, "topics"), j.message(j.eventObject()))
	case EndpointEventHub:
		err = tg.SendEventHub(
			resourceSegment(rid, "namespaces"),
			resourceSegment(rid, "eventhubs"),
			j.message(j.eventArray()),
		)
	case EndpointStorageQueue:
		ttl := time.Duration(intProp(d.Properties[propQueueTTL])) * time.Second
		err = tg.SendStorageQueue(
			resourceSegment(rid, "storageAccounts"),
			d.str(propQueueName),
			string(j.eventObject()),
			ttl,
		)
	default:
		return outcome{label: outcomeGeneric, permanent: true}
	}

	return targetOutcome(err)
}

func intProp(v any) int {
	f, _ := toFloat(v)

	return int(f)
}
