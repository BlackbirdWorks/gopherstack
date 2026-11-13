package gcpeventarc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"io"
	"maps"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpdispatch"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	maxPushBytes     = 16 << 20
	maxReplyBytes    = 4 << 10
	localhostSuffix  = ".localhost"
	loopbackIP       = "127.0.0.1"
	attrCEID         = "ce-id"
	attrCESource     = "ce-source"
	attrCEType       = "ce-type"
	attrCESubject    = "ce-subject"
	attrCETime       = "ce-time"
	contentTypeProto = "application/protobuf"
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
	ext     map[string]string
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
	if err := json.UnmarshalDecode(
		jsontext.NewDecoder(http.MaxBytesReader(w, r.Body, maxPushBytes)),
		&env,
		jsonx.V1(),
	); err != nil {
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
	if err == nil {
		err = b.send(r.Context(), trigger, target, ev)
	}

	if err != nil {
		if trigger.GetRetryPolicy().GetMaxAttempts() == 1 {
			logger.Load(r.Context()).WarnContext(r.Context(),
				"gcpeventarc: delivery failed and retry_policy.max_attempts is 1; dropping the event",
				"trigger", name, "id", ev.id, "error", err)
			w.WriteHeader(http.StatusNoContent)

			return
		}

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

// workflowArgument is the CloudEvent in structured JSON form; binary (protobuf) data travels as data_base64.
func workflowArgument(ev cloudEvent) ([]byte, error) {
	out := map[string]any{
		fieldSpecVersion: cloudEventsVersion, "id": ev.id, fieldSource: ev.source, attrType: ev.typ,
	}

	for k, v := range ev.ext {
		out[k] = v
	}

	if ev.subject != "" {
		out["subject"] = ev.subject
	}

	if ev.time != "" {
		out["time"] = ev.time
	}

	if jsontext.Value(ev.body).IsValid(jsonx.V1()) {
		out["datacontenttype"], out["data"] = contentTypeJSON, jsontext.Value(ev.body)
	} else {
		out["datacontenttype"], out["data_base64"] = contentTypeProto, base64.StdEncoding.EncodeToString(ev.body)
	}

	return json.Marshal(out, jsonx.V1())
}

func buildEvent(t *eventarcpb.Trigger, env pushedEnvelope) (cloudEvent, bool) {
	if eventType(t.GetEventFilters()) == typePubsubPublished {
		body, err := json.Marshal(pushedEnvelope{Subscription: env.Subscription, Message: env.Message}, jsonx.V1())
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
		subject: attrs[attrCESubject], time: attrs[attrCETime], ext: extensionAttrs(attrs), body: data,
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
	req.Header.Set("Ce-Specversion", cloudEventsVersion)
	req.Header.Set("Ce-Id", ev.id)
	req.Header.Set("Ce-Source", ev.source)
	req.Header.Set("Ce-Type", ev.typ)

	if ev.time != "" {
		req.Header.Set("Ce-Time", ev.time)
	}

	if ev.subject != "" {
		req.Header.Set("Ce-Subject", ev.subject)
	}

	for k, v := range ev.ext {
		req.Header.Set("Ce-"+k, v)
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

	return defaultContentType(eventType(t.GetEventFilters()))
}

func audience(target string) string {
	u, err := url.Parse(target)
	if err != nil {
		return target
	}

	return u.Scheme + "://" + u.Host
}

func isCoreAttr(k string) bool {
	switch k {
	case attrCEID, attrCESource, attrCEType, attrCESubject, attrCETime:
		return true
	}

	return false
}

// extensionAttrs returns the CloudEvents extension attributes carried as ce-* transport attributes.
func extensionAttrs(attrs map[string]string) map[string]string {
	var out map[string]string

	for k, v := range attrs {
		name, ok := strings.CutPrefix(k, "ce-")
		if !ok || isCoreAttr(k) || name == "" {
			continue
		}

		if out == nil {
			out = map[string]string{}
		}

		out[name] = v
	}

	return out
}

// fanout is one event offered to every trigger it matches.
type fanout struct {
	build   func(t *eventarcpb.Trigger, typ string) (map[string]string, []byte, error)
	attrs   map[string]string
	message func() map[string]any
	project string
	types   []string
}

// publishEvent publishes the event to the transport topic of every trigger whose filters it satisfies.
func (b *InMemoryBackend) publishEvent(f fanout) {
	type target struct {
		trigger *eventarcpb.Trigger
		topic   string
		typ     string
	}

	b.mu.RLock("publishEvent")

	var targets []target

	ps := b.svc.PubSub

	for _, rec := range b.triggers {
		typ := eventType(rec.pb.GetEventFilters())
		topic := rec.pb.GetTransport().GetPubsub().GetTopic()

		if topic == "" || !slices.Contains(f.types, typ) {
			continue
		}

		if f.project != "" {
			if p, _, _, err := parseChild(rec.pb.GetName(), kindTriggers); err != nil || p != f.project {
				continue
			}
		}

		attrs := maps.Clone(f.attrs)
		attrs[attrType] = typ

		if matches(rec.pb.GetEventFilters(), attrs) {
			targets = append(targets, target{trigger: clone(rec.pb), topic: topic, typ: typ})
		}
	}

	b.mu.RUnlock()

	if f.message != nil {
		b.routeToSources(f.project, f.message())
	}

	ctx := context.Background()

	for _, t := range targets {
		if ps == nil {
			break
		}

		attrs, data, err := f.build(t.trigger, t.typ)
		if err != nil {
			logger.Load(ctx).WarnContext(ctx, "gcpeventarc: could not build event", "type", t.typ, "error", err)

			continue
		}

		_, err = ps.Publish(&pubsubpb.PublishRequest{
			Topic:    t.topic,
			Messages: []*pubsubpb.PubsubMessage{{Data: data, Attributes: attrs}},
		})
		if err != nil {
			logger.Load(ctx).WarnContext(ctx, "gcpeventarc: could not queue event",
				"topic", t.topic, "type", t.typ, "error", err)
		}
	}
}

func ceAttrs(id, source, typ, subject string, at time.Time, ext map[string]string) map[string]string {
	attrs := map[string]string{
		attrCEID: id, attrCESource: source, attrCEType: typ, attrCETime: at.UTC().Format(time.RFC3339Nano),
	}

	if subject != "" {
		attrs[attrCESubject] = subject
	}

	for k, v := range ext {
		attrs["ce-"+k] = v
	}

	return attrs
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
	subject := storageSubjectPrefix + ev.Name
	id := uuid.NewString()

	b.publishEvent(fanout{
		types: []string{typ},
		attrs: map[string]string{attrBucket: ev.Bucket, attrSubject: subject},
		message: func() map[string]any {
			return eventMessage(id, storageSourcePrefix+ev.Bucket, typ, subject, ev.Time,
				map[string]string{"bucket": ev.Bucket}, ev.Data)
		},
		build: func(_ *eventarcpb.Trigger, t string) (map[string]string, []byte, error) {
			return ceAttrs(id, storageSourcePrefix+ev.Bucket, t, subject, ev.Time, nil), ev.Data, nil
		},
	})
}
