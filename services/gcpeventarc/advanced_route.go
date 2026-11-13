package gcpeventarc

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"cel.dev/cel-go/cel"
	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpdispatch"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const maxBusHops = 8

// delivery is one message on its way through a pipeline.
type pipelineJob struct {
	pipeline *eventarcpb.Pipeline
	message  map[string]any
	attempt  int
	hops     int
}

func requiredAttrs() []string { return []string{"id", fieldSource, attrType, fieldSpecVersion} }

// PublishToBus routes a CloudEvent (as a JSON object) through the enrollments of a message bus.
func (b *InMemoryBackend) PublishToBus(bus string, event map[string]any) error {
	return b.publishToBus(bus, event, 0)
}

func (b *InMemoryBackend) publishToBus(bus string, event map[string]any, hops int) error {
	if _, _, _, err := parseChild(bus, kindMessageBuses); err != nil {
		return err
	}

	for _, a := range requiredAttrs() {
		if s, _ := event[a].(string); s == "" {
			return errInvalid("The CloudEvent is missing the required attribute %q.", a)
		}
	}

	if event[fieldSpecVersion] != cloudEventsVersion {
		return errInvalid("Unsupported CloudEvents specversion %v: want 1.0.", event[fieldSpecVersion])
	}

	b.mu.RLock("publishToBus")

	if _, ok := b.advTable(busKind())[bus]; !ok {
		b.mu.RUnlock()

		return errNotFound("Message bus %q not found.", bus)
	}

	var jobs []pipelineJob

	for _, name := range sortedNames(b.advTable(enrollKind())) {
		e, _ := b.advTable(enrollKind())[name].(*eventarcpb.Enrollment)
		if e == nil || e.GetMessageBus() != bus {
			continue
		}

		p, ok := b.advTable(pipelineKind())[e.GetDestination()].(*eventarcpb.Pipeline)
		if !ok || !b.enrollmentMatches(e, event) {
			continue
		}

		jobs = append(jobs, pipelineJob{pipeline: clone(p), message: cloneMap(event), hops: hops})
	}

	b.mu.RUnlock()

	for _, j := range jobs {
		b.schedule(0, func() { b.runJob(j) })
	}

	return nil
}

func sortedNames(m map[string]proto.Message) []string { return slices.Sorted(maps.Keys(m)) }

func cloneMap(m map[string]any) map[string]any {
	raw, err := json.Marshal(m, jsonx.V1())
	if err != nil {
		return maps.Clone(m)
	}

	var out map[string]any
	if err = json.Unmarshal(raw, &out, jsonx.V1()); err != nil {
		return maps.Clone(m)
	}

	return out
}

func (b *InMemoryBackend) enrollmentMatches(e *eventarcpb.Enrollment, msg map[string]any) bool {
	if b.msgEnv == nil {
		return false
	}

	prg, err := b.compileMessageExpr(e.GetCelMatch(), cel.BoolType)
	if err != nil {
		return false
	}

	out, err := evalMessage(prg, msg)
	if err != nil {
		return false
	}

	ok, _ := out.Value().(bool)

	return ok
}

// schedule runs f after d on the injected timer; Close cancels what has not run.
func (b *InMemoryBackend) schedule(d time.Duration, f func()) {
	b.opMu.Lock("schedule")
	b.timerSeq++
	id := b.timerSeq
	b.timers[id] = func() {}
	after := b.after
	b.opMu.Unlock()

	stop := after(d, func() {
		b.opMu.Lock("schedule")
		_, live := b.timers[id]
		delete(b.timers, id)
		b.opMu.Unlock()

		if live {
			f()
		}
	})

	b.opMu.Lock("schedule")
	if _, ok := b.timers[id]; ok {
		b.timers[id] = stop
	}
	b.opMu.Unlock()
}

func (b *InMemoryBackend) runJob(j pipelineJob) {
	ctx := context.Background()

	err := b.deliverPipeline(ctx, j)
	if err == nil {
		return
	}

	j.attempt++

	maxTries := int(j.pipeline.GetRetryPolicy().GetMaxAttempts())
	if maxTries == 0 {
		maxTries = pipelineDefaultTry
	}

	if j.attempt >= maxTries {
		logger.Load(ctx).
			WarnContext(ctx, "gcpeventarc: pipeline delivery failed; max_attempts reached, dropping the message",
				"pipeline", j.pipeline.GetName(), "attempts", j.attempt, "error", err)

		return
	}

	b.schedule(retryDelay(j.pipeline.GetRetryPolicy(), j.attempt), func() { b.runJob(j) })
}

// retryDelay is min_retry_delay doubled per failed attempt and capped at max_retry_delay.
func retryDelay(r *eventarcpb.Pipeline_RetryPolicy, failures int) time.Duration {
	lo, hi := pipelineDefaultMin, pipelineDefaultMax

	if r.GetMinRetryDelay() != nil {
		lo = r.GetMinRetryDelay().AsDuration()
	}

	if r.GetMaxRetryDelay() != nil {
		hi = r.GetMaxRetryDelay().AsDuration()
	}

	d := lo
	for i := 1; i < failures && d < hi; i++ {
		d *= 2
	}

	return min(d, hi)
}

func (b *InMemoryBackend) deliverPipeline(ctx context.Context, j pipelineJob) error {
	msg := j.message

	for _, m := range j.pipeline.GetMediations() {
		prg, err := b.compileMessageExpr(m.GetTransformation().GetTransformationTemplate(), nil)
		if err != nil {
			return err
		}

		out, err := evalMessage(prg, msg)
		if err != nil {
			return fmt.Errorf("transformation: %w", err)
		}

		next, ok := toNative(out).(map[string]any)
		if !ok {
			return fmt.Errorf("transformation: %w: want a map", errCELResult)
		}

		msg = next
	}

	d := j.pipeline.GetDestinations()[0]

	switch dd := d.GetDestinationDescriptor().(type) {
	case *eventarcpb.Pipeline_Destination_HttpEndpoint_:
		return b.deliverHTTP(ctx, d, dd.HttpEndpoint, msg)
	case *eventarcpb.Pipeline_Destination_Workflow:
		return b.deliverWorkflow(ctx, dd.Workflow, msg)
	case *eventarcpb.Pipeline_Destination_Topic:
		return b.deliverTopic(dd.Topic, msg)
	case *eventarcpb.Pipeline_Destination_MessageBus:
		if j.hops+1 > maxBusHops {
			return nil
		}

		return b.publishToBus(dd.MessageBus, msg, j.hops+1)
	}

	return errPrecondition("The pipeline has no deliverable destination.")
}

func (b *InMemoryBackend) deliverWorkflow(ctx context.Context, workflow string, msg map[string]any) error {
	b.mu.RLock("deliverWorkflow")
	wf := b.svc.Workflows
	b.mu.RUnlock()

	if wf == nil {
		return errPrecondition("No Workflows emulator is connected.")
	}

	arg, err := json.Marshal(msg, jsonx.V1())
	if err != nil {
		return fmt.Errorf("encode message: %w", err)
	}

	return wf.StartExecution(ctx, workflow, arg)
}

func (b *InMemoryBackend) deliverTopic(topic string, msg map[string]any) error {
	b.mu.RLock("deliverTopic")
	ps := b.svc.PubSub
	b.mu.RUnlock()

	if ps == nil {
		return errPrecondition("No Pub/Sub emulator is connected.")
	}

	data, err := json.Marshal(msg, jsonx.V1())
	if err != nil {
		return fmt.Errorf("encode message: %w", err)
	}

	_, err = ps.Publish(&pubsubpb.PublishRequest{
		Topic: topic, Messages: []*pubsubpb.PubsubMessage{{Data: data}},
	})
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	return nil
}

// binding is the HTTP request built from a message.
type binding struct {
	headers map[string]string
	body    []byte
}

// bindMessage applies message_binding_template (a map with headers and body), else CloudEvents binary mode.
func (b *InMemoryBackend) bindMessage(tmpl string, msg map[string]any) (binding, error) {
	if tmpl == "" {
		return binaryBinding(msg), nil
	}

	prg, err := b.compileMessageExpr(tmpl, nil)
	if err != nil {
		return binding{}, err
	}

	out, err := evalMessage(prg, msg)
	if err != nil {
		return binding{}, fmt.Errorf("message_binding_template: %w", err)
	}

	res, ok := toNative(out).(map[string]any)
	if !ok {
		return binding{}, fmt.Errorf("message_binding_template: %w: want a map", errCELResult)
	}

	bd := binding{headers: map[string]string{}}

	if h, isMap := res["headers"].(map[string]any); isMap {
		for k, v := range h {
			bd.headers[k] = fmt.Sprint(v)
		}
	}

	bd.body = bodyBytes(res["body"])

	return bd, nil
}

func bodyBytes(v any) []byte {
	switch t := v.(type) {
	case nil:
		return nil
	case string:
		return []byte(t)
	default:
		raw, _ := json.Marshal(t, jsonx.V1())

		return raw
	}
}

func binaryBinding(msg map[string]any) binding {
	bd := binding{headers: map[string]string{}}

	for k, v := range msg {
		if k == fieldData || k == fieldDataB64 {
			continue
		}

		if k == fieldDataType {
			bd.headers["Content-Type"] = fmt.Sprint(v)

			continue
		}

		bd.headers["Ce-"+k] = fmt.Sprint(v)
	}

	if _, ok := bd.headers["Content-Type"]; !ok {
		bd.headers["Content-Type"] = contentTypeJSON
	}

	bd.body = bodyBytes(msg["data"])

	return bd
}

func (b *InMemoryBackend) deliverHTTP(
	ctx context.Context, d *eventarcpb.Pipeline_Destination, ep *eventarcpb.Pipeline_Destination_HttpEndpoint,
	msg map[string]any,
) error {
	bd, err := b.bindMessage(ep.GetMessageBindingTemplate(), msg)
	if err != nil {
		return err
	}

	target := ep.GetUri()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(bd.body))
	if err != nil {
		return errInvalid("bad destination %q: %v", target, err)
	}

	gcpserve.MarkInternal(req)

	for k, v := range bd.headers {
		req.Header.Set(k, v)
	}

	b.authorize(req, d.GetAuthenticationConfig(), target)

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

func (b *InMemoryBackend) authorize(
	req *http.Request,
	cfg *eventarcpb.Pipeline_Destination_AuthenticationConfig,
	target string,
) {
	switch a := cfg.GetAuthenticationMethodDescriptor().(type) {
	case *eventarcpb.Pipeline_Destination_AuthenticationConfig_GoogleOidc:
		sa := a.GoogleOidc.GetServiceAccount()
		req.Header.Set(
			"Authorization",
			"Bearer "+gcpdispatch.OIDCToken(sa, a.GoogleOidc.GetAudience(), target, b.now()),
		)
	case *eventarcpb.Pipeline_Destination_AuthenticationConfig_OauthToken:
		req.Header.Set(
			"Authorization",
			"Bearer "+gcpdispatch.OAuthToken(a.OauthToken.GetServiceAccount(), a.OauthToken.GetScope()),
		)
	}
}

// routeToSources puts an event from an emulated Google API on the message bus of every GoogleApiSource that
// subscribes to its project (an empty project, from a source that does not know it, reaches every source).
func (b *InMemoryBackend) routeToSources(project string, msg map[string]any) {
	b.mu.RLock("routeToSources")

	var buses []string

	for _, name := range sortedNames(b.advTable(sourceKind())) {
		s, _ := b.advTable(sourceKind())[name].(*eventarcpb.GoogleApiSource)
		if s != nil && sourceWants(s, project) {
			buses = append(buses, s.GetDestination())
		}
	}

	b.mu.RUnlock()

	for _, bus := range buses {
		if err := b.publishToBus(bus, msg, 0); err != nil {
			ctx := context.Background()
			logger.Load(ctx).
				DebugContext(ctx, "gcpeventarc: could not route event to message bus", "bus", bus, "error", err)
		}
	}
}

func sourceWants(s *eventarcpb.GoogleApiSource, project string) bool {
	if s.GetOrganizationSubscription().GetEnabled() || project == "" {
		return true
	}

	for _, p := range s.GetProjectSubscriptions().GetList() {
		if strings.TrimPrefix(p, "projects/") == project {
			return true
		}
	}

	return false
}

// eventMessage is a CloudEvent in the JSON object form CEL expressions see as `message`.
func eventMessage(id, source, typ, subject string, at time.Time, ext map[string]string, data []byte) map[string]any {
	msg := map[string]any{
		fieldSpecVersion: cloudEventsVersion, "id": id, fieldSource: source, "type": typ,
		"time": at.UTC().Format(time.RFC3339Nano), "datacontenttype": contentTypeJSON,
	}

	if subject != "" {
		msg["subject"] = subject
	}

	for k, v := range ext {
		msg[k] = v
	}

	var decoded any
	if json.Unmarshal(data, &decoded, jsonx.V1()) == nil {
		msg["data"] = decoded
	}

	return msg
}
