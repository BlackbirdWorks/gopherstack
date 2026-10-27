package gcpeventarc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpdispatch"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	maxPushBytes    = 16 << 20
	maxReplyBytes   = 4 << 10
	localhostSuffix = ".localhost"
	loopbackIP      = "127.0.0.1"
	attrCEID        = "ce-id"
	attrCESource    = "ce-source"
	attrCEType      = "ce-type"
	attrCESubject   = "ce-subject"
	attrCETime      = "ce-time"
)

// loopbackDialer sends *.localhost hosts (Cloud Run and Functions serving URLs) to the loopback interface.
func loopbackDialer(
	next func(ctx context.Context, network, addr string) (net.Conn, error),
) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if host, port, err := net.SplitHostPort(addr); err == nil && strings.HasSuffix(host, localhostSuffix) {
			addr = net.JoinHostPort(loopbackIP, port)
		}

		return next(ctx, network, addr)
	}
}

type pushedMessage struct {
	Attributes  map[string]string `json:"attributes,omitempty"`
	Data        string            `json:"data"`
	MessageID   string            `json:"messageId"`
	PublishTime string            `json:"publishTime"`
	OrderingKey string            `json:"orderingKey,omitempty"`
}

type pushedEnvelope struct {
	Subscription string        `json:"subscription"`
	Message      pushedMessage `json:"message"`
}

// cloudEvent is one event ready to be sent to a destination in binary content mode.
type cloudEvent struct {
	id      string
	source  string
	typ     string
	subject string
	time    string
	body    []byte
}

// PushHandler receives the Pub/Sub push requests of trigger subscriptions and forwards each message as a
// CloudEvent to the trigger's destination. A non-2xx reply makes Pub/Sub redeliver with backoff.
func (b *InMemoryBackend) PushHandler() http.Handler {
	return http.HandlerFunc(b.servePush)
}

func (b *InMemoryBackend) servePush(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, pushPath)

	b.mu.RLock("servePush")
	rec, ok := b.triggers[name]

	var trigger *eventarcpb.Trigger
	if ok {
		trigger = clone(rec.pb)
	}

	targets, workflows := b.svc.Targets, b.svc.Workflows
	b.mu.RUnlock()

	if !ok {
		http.Error(w, "unknown trigger", http.StatusNotFound)

		return
	}

	var env pushedEnvelope
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxPushBytes)).Decode(&env); err != nil {
		http.Error(w, "invalid push envelope: "+err.Error(), http.StatusBadRequest)

		return
	}

	ev, ok := buildEvent(trigger, env)
	if !ok {
		logger.Load(r.Context()).WarnContext(r.Context(), "gcpeventarc: dropping malformed transport message",
			"trigger", name, "messageId", env.Message.MessageID)
		w.WriteHeader(http.StatusNoContent)

		return
	}

	if wf := trigger.GetDestination().GetWorkflow(); wf != "" {
		b.deliverToWorkflow(w, r, workflows, wf, name, ev)

		return
	}

	target, err := destinationURL(trigger, targets)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)

		return
	}

	if err = b.send(r.Context(), trigger, target, ev); err != nil {
		logger.Load(r.Context()).DebugContext(r.Context(), "gcpeventarc: delivery failed; Pub/Sub will retry",
			"trigger", name, "target", target, "error", err)
		http.Error(w, err.Error(), http.StatusBadGateway)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// deliverToWorkflow starts an execution whose argument is the CloudEvent in structured JSON form, as Eventarc does.
func (b *InMemoryBackend) deliverToWorkflow(
	w http.ResponseWriter, r *http.Request, workflows Workflows, workflow, trigger string, ev cloudEvent,
) {
	if workflows == nil {
		http.Error(w, "no Workflows emulator is connected", http.StatusBadGateway)

		return
	}

	arg, err := workflowArgument(ev)
	if err == nil {
		err = workflows.StartExecution(r.Context(), workflow, arg)
	}

	if err != nil {
		logger.Load(r.Context()).DebugContext(r.Context(), "gcpeventarc: workflow delivery failed; Pub/Sub will retry",
			"trigger", trigger, "workflow", workflow, "error", err)
		http.Error(w, err.Error(), http.StatusBadGateway)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type structuredEvent struct {
	SpecVersion     string          `json:"specversion"`
	ID              string          `json:"id"`
	Source          string          `json:"source"`
	Type            string          `json:"type"`
	DataContentType string          `json:"datacontenttype"`
	Subject         string          `json:"subject,omitempty"`
	Time            string          `json:"time,omitempty"`
	Data            json.RawMessage `json:"data"`
}

func workflowArgument(ev cloudEvent) ([]byte, error) {
	data := json.RawMessage(ev.body)
	if !json.Valid(data) {
		quoted, err := json.Marshal(string(ev.body))
		if err != nil {
			return nil, err
		}

		data = quoted
	}

	return json.Marshal(structuredEvent{
		SpecVersion: "1.0", ID: ev.id, Source: ev.source, Type: ev.typ, Subject: ev.subject, Time: ev.time,
		DataContentType: contentTypeJSON, Data: data,
	})
}

func buildEvent(t *eventarcpb.Trigger, env pushedEnvelope) (cloudEvent, bool) {
	if eventType(t.GetEventFilters()) == typePubsubPublished {
		body, err := json.Marshal(pushedEnvelope{Subscription: env.Subscription, Message: env.Message})
		if err != nil {
			return cloudEvent{}, false
		}

		return cloudEvent{
			id:     env.Message.MessageID,
			source: pubsubSourcePrefix + t.GetTransport().GetPubsub().GetTopic(),
			typ:    typePubsubPublished,
			time:   env.Message.PublishTime,
			body:   body,
		}, true
	}

	attrs := env.Message.Attributes

	data, err := base64.StdEncoding.DecodeString(env.Message.Data)
	if err != nil || attrs[attrCEID] == "" || attrs[attrCEType] == "" || attrs[attrCESource] == "" {
		return cloudEvent{}, false
	}

	return cloudEvent{
		id: attrs[attrCEID], source: attrs[attrCESource], typ: attrs[attrCEType],
		subject: attrs[attrCESubject], time: attrs[attrCETime], body: data,
	}, true
}

func destinationURL(t *eventarcpb.Trigger, targets Targets) (string, error) {
	switch d := t.GetDestination().GetDescriptor_().(type) {
	case *eventarcpb.Destination_HttpEndpoint:
		return d.HttpEndpoint.GetUri(), nil
	case *eventarcpb.Destination_CloudRun:
		if targets == nil {
			return "", errPrecondition("No Cloud Run emulator is connected.")
		}

		project, _, _, err := parseChild(t.GetName(), kindTriggers)
		if err != nil {
			return "", err
		}

		base, err := targets.ServiceURL(project, d.CloudRun.GetRegion(), d.CloudRun.GetService())
		if err != nil {
			return "", err
		}

		return strings.TrimSuffix(base, "/") + d.CloudRun.GetPath(), nil
	default:
		return "", errPrecondition("The trigger has no deliverable destination.")
	}
}

func (b *InMemoryBackend) send(ctx context.Context, t *eventarcpb.Trigger, target string, ev cloudEvent) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(ev.body))
	if err != nil {
		return errInvalid("bad destination %q: %v", target, err)
	}

	req.Header.Set("Content-Type", contentTypeFor(t))
	gcpserve.MarkInternal(req)
	req.Header.Set("Ce-Specversion", "1.0")
	req.Header.Set("Ce-Id", ev.id)
	req.Header.Set("Ce-Source", ev.source)
	req.Header.Set("Ce-Type", ev.typ)

	if ev.time != "" {
		req.Header.Set("Ce-Time", ev.time)
	}

	if ev.subject != "" {
		req.Header.Set("Ce-Subject", ev.subject)
	}

	if sa := t.GetServiceAccount(); sa != "" {
		req.Header.Set("Authorization", "Bearer "+gcpdispatch.OIDCToken(sa, audience(target), target, b.now()))
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return errPrecondition("destination unreachable: %v", err)
	}
	defer resp.Body.Close()

	reply, _ := io.ReadAll(io.LimitReader(resp.Body, maxReplyBytes))

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return errPrecondition("destination replied %d: %s", resp.StatusCode, strings.TrimSpace(string(reply)))
	}

	return nil
}

func contentTypeFor(t *eventarcpb.Trigger) string {
	if ct := t.GetEventDataContentType(); ct != "" {
		return ct
	}

	return contentTypeJSON
}

func audience(target string) string {
	u, err := url.Parse(target)
	if err != nil {
		return target
	}

	return u.Scheme + "://" + u.Host
}

// StorageEvent is a Cloud Storage object change. Type is the event type suffix (finalized, deleted,
// archived or metadataUpdated) and Data the object resource JSON.
type StorageEvent struct {
	Time   time.Time
	Type   string
	Bucket string
	Name   string
	Data   []byte
}

// HandleStorageEvent publishes ev to the transport topic of every trigger whose filters it satisfies.
func (b *InMemoryBackend) HandleStorageEvent(ev StorageEvent) {
	typ := storageType(ev.Type)
	filterAttrs := map[string]string{
		attrType:    typ,
		attrBucket:  ev.Bucket,
		attrSubject: storageSubjectPrefix + ev.Name,
	}

	b.mu.RLock("HandleStorageEvent")

	var topics []string

	ps := b.svc.PubSub

	for _, rec := range b.triggers {
		topic := rec.pb.GetTransport().GetPubsub().GetTopic()
		if topic != "" && eventType(rec.pb.GetEventFilters()) == typ && matches(rec.pb.GetEventFilters(), filterAttrs) {
			topics = append(topics, topic)
		}
	}

	b.mu.RUnlock()

	if ps == nil || len(topics) == 0 {
		return
	}

	attrs := map[string]string{
		attrCEID:      uuid.NewString(),
		attrCESource:  storageSourcePrefix + ev.Bucket,
		attrCEType:    typ,
		attrCESubject: filterAttrs[attrSubject],
		attrCETime:    ev.Time.UTC().Format(time.RFC3339Nano),
	}

	ctx := context.Background()

	for _, topic := range topics {
		_, err := ps.Publish(&pubsubpb.PublishRequest{
			Topic:    topic,
			Messages: []*pubsubpb.PubsubMessage{{Data: ev.Data, Attributes: attrs}},
		})
		if err != nil {
			logger.Load(ctx).WarnContext(ctx, "gcpeventarc: could not queue storage event",
				"topic", topic, "type", typ, "error", err)
		}
	}
}
