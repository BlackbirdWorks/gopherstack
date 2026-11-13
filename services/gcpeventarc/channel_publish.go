package gcpeventarc

import (
	"context"
	"encoding/base64"
	"encoding/json/v2"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	channelRetryBase  = 10 * time.Second
	channelRetryMax   = 600 * time.Second
	channelMaxAttempt = 10
)

// PublishToChannel delivers CloudEvents to the triggers attached to a channel.
func (b *InMemoryBackend) PublishToChannel(channel string, events []map[string]any) error {
	if _, _, _, err := parseChild(channel, kindChannels); err != nil {
		return err
	}

	if len(events) == 0 {
		return errInvalid("At least one event is required.")
	}

	for _, e := range events {
		if err := checkChannelEvent(e); err != nil {
			return err
		}
	}

	b.mu.RLock("PublishToChannel")

	if _, ok := b.channels[channel]; !ok {
		b.mu.RUnlock()

		return errNotFound("Channel %q not found.", channel)
	}

	var jobs []channelJob

	for _, e := range events {
		attrs := channelEventAttrs(e)

		for _, rec := range b.triggers {
			if rec.pb.GetChannel() == channel && matches(rec.pb.GetEventFilters(), attrs) {
				jobs = append(jobs, channelJob{trigger: clone(rec.pb), event: channelCloudEvent(e, b.now())})
			}
		}
	}

	b.mu.RUnlock()

	for _, j := range jobs {
		b.schedule(0, func() { b.deliverChannelEvent(j) })
	}

	return nil
}

// PublishToChannelConnection delivers events published through a channel connection to its channel.
func (b *InMemoryBackend) PublishToChannelConnection(conn string, events []map[string]any) error {
	if _, _, _, err := parseChild(conn, "channelConnections"); err != nil {
		return err
	}

	b.mu.RLock("PublishToChannelConnection")
	c, ok := b.conns[conn]

	var channel string
	if ok {
		channel = c.GetChannel()
	}

	b.mu.RUnlock()

	if !ok {
		return errNotFound("Channel connection %q not found.", conn)
	}

	return b.PublishToChannel(channel, events)
}

type channelJob struct {
	trigger *eventarcpb.Trigger
	event   cloudEvent
	attempt int
}

func checkChannelEvent(e map[string]any) error {
	for _, a := range requiredAttrs() {
		if s, _ := e[a].(string); s == "" {
			return errInvalid("The CloudEvent is missing the required attribute %q.", a)
		}
	}

	if e[fieldSpecVersion] != cloudEventsVersion {
		return errInvalid("Unsupported CloudEvents specversion %v: want 1.0.", e[fieldSpecVersion])
	}

	return nil
}

func channelEventAttrs(e map[string]any) map[string]string {
	attrs := map[string]string{}

	for k, v := range e {
		if s, ok := v.(string); ok && k != fieldData && k != fieldDataB64 {
			attrs[k] = s
		}
	}

	return attrs
}

func isChannelCoreAttr(k string) bool {
	switch k {
	case "id", fieldSource, attrType, fieldSpecVersion, "subject", "time", fieldDataType, fieldData, fieldDataB64:
		return true
	}

	return false
}

func channelCloudEvent(e map[string]any, now time.Time) cloudEvent {
	attrs := channelEventAttrs(e)

	ev := cloudEvent{
		id: attrs["id"], source: attrs[fieldSource], typ: attrs[attrType], subject: attrs["subject"],
		time: attrs["time"],
	}

	if ev.time == "" {
		ev.time = now.UTC().Format(time.RFC3339Nano)
	}

	for k, v := range attrs {
		if isChannelCoreAttr(k) {
			continue
		}

		if ev.ext == nil {
			ev.ext = map[string]string{}
		}

		ev.ext[k] = v
	}

	switch d := e["data"].(type) {
	case nil:
		if raw, ok := e["data_base64"].(string); ok {
			ev.body, _ = base64.StdEncoding.DecodeString(raw)
		}
	case string:
		ev.body = []byte(d)
	default:
		ev.body, _ = json.Marshal(d, jsonx.V1())
	}

	return ev
}

func (b *InMemoryBackend) deliverChannelEvent(j channelJob) {
	ctx := context.Background()

	err := b.sendChannelEvent(ctx, j)
	if err == nil {
		return
	}

	t := j.trigger

	j.attempt++
	if t.GetRetryPolicy().GetMaxAttempts() == 1 || j.attempt >= channelMaxAttempt {
		logger.Load(ctx).WarnContext(ctx, "gcpeventarc: channel delivery failed; dropping the event",
			"trigger", t.GetName(), "id", j.event.id, "error", err)

		return
	}

	delay := min(channelRetryBase<<(j.attempt-1), channelRetryMax)
	b.schedule(delay, func() { b.deliverChannelEvent(j) })
}

func (b *InMemoryBackend) sendChannelEvent(ctx context.Context, j channelJob) error {
	b.mu.RLock("sendChannelEvent")
	targets, workflows := b.svc.Targets, b.svc.Workflows
	b.mu.RUnlock()

	if wf := j.trigger.GetDestination().GetWorkflow(); wf != "" {
		if workflows == nil {
			return errPrecondition("No Workflows emulator is connected.")
		}

		arg, err := workflowArgument(j.event)
		if err != nil {
			return fmt.Errorf("build workflow argument: %w", err)
		}

		return workflows.StartExecution(ctx, wf, arg)
	}

	target, err := destinationURL(j.trigger, targets)
	if err != nil {
		return err
	}

	return b.send(ctx, j.trigger, target, j.event)
}

func parseTextEvents(texts []string) ([]map[string]any, error) {
	out := make([]map[string]any, 0, len(texts))

	for _, t := range texts {
		var e map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(t)), &e, jsonx.V1()); err != nil {
			return nil, errInvalid("text_events entry is not a JSON CloudEvent: %v", err)
		}

		out = append(out, e)
	}

	return out, nil
}
