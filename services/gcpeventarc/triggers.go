package gcpeventarc

import (
	"net/url"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	defaultListPage   = 100
	contentTypeJSON   = "application/json"
	subscriptionAck   = 60
	subscriptionKeep  = 24 * 60 * 60
	defaultRetryMin   = 10 * time.Second
	defaultRetryMax   = 600 * time.Second
	suffixLen         = 8
	pushPath          = "/_eventarc/push/"
	kindTriggers      = "triggers"
	kindChannels      = "channels"
	updateMaskAll     = "*"
	maskEventFilters  = "event_filters"
	maskServiceAcct   = "service_account"
	maskDestination   = "destination"
	maskLabels        = "labels"
	maskRetry         = "retry_policy"
	maskContentType   = "event_data_content_type"
	maskCryptoKey     = "crypto_key_name"
	retryAttemptLimit = 1000
)

var (
	serviceIDRe = regexp.MustCompile(`^[a-z]([-a-z0-9]*[a-z0-9])?$`)
	accountRe   = regexp.MustCompile(`^[^@\s]+@[^@\s]+$`)
)

func shortID() string { return strings.ReplaceAll(uuid.NewString(), "-", "")[:suffixLen] }

func validateDestination(d *eventarcpb.Destination) error {
	if d == nil {
		return errInvalid("destination is required.")
	}

	switch dest := d.GetDescriptor_().(type) {
	case *eventarcpb.Destination_CloudRun:
		return validateCloudRun(dest.CloudRun)
	case *eventarcpb.Destination_HttpEndpoint:
		return validateHTTPEndpoint(dest.HttpEndpoint.GetUri())
	case *eventarcpb.Destination_CloudFunction:
		return errInvalid("destination.cloud_function is read-only; create function triggers with Cloud Functions.")
	case *eventarcpb.Destination_Workflow:
		return errPrecondition("Workflows destinations are not available: no Workflows service is emulated.")
	case *eventarcpb.Destination_Gke:
		return errPrecondition("GKE destinations are not available: no GKE service is emulated.")
	default:
		return errInvalid("destination must set one of cloud_run or http_endpoint.")
	}
}

func validateCloudRun(r *eventarcpb.CloudRun) error {
	if !serviceIDRe.MatchString(r.GetService()) {
		return errInvalid("destination.cloud_run.service %q is not a valid Cloud Run service id.", r.GetService())
	}

	if !knownLocation(r.GetRegion()) || r.GetRegion() == globalLocation {
		return errInvalid("destination.cloud_run.region %q is not a Cloud Run region.", r.GetRegion())
	}

	if p := r.GetPath(); p != "" && !strings.HasPrefix(p, "/") {
		return errInvalid("destination.cloud_run.path must start with '/'.")
	}

	return nil
}

func validateHTTPEndpoint(uri string) error {
	u, err := url.Parse(uri)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return errInvalid("destination.http_endpoint.uri %q must be an absolute http or https URI.", uri)
	}

	return nil
}

func validateTrigger(t *eventarcpb.Trigger) error {
	if err := validateFilters(t.GetEventFilters(), t.GetChannel() != ""); err != nil {
		return err
	}

	if err := validateDestination(t.GetDestination()); err != nil {
		return err
	}

	if sa := t.GetServiceAccount(); sa != "" && !accountRe.MatchString(sa) {
		return errInvalid("service_account %q is not an email address.", sa)
	}

	if ct := t.GetEventDataContentType(); ct != "" && ct != contentTypeJSON {
		return errInvalid("event_data_content_type %q is not supported: only %s.", ct, contentTypeJSON)
	}

	if n := t.GetRetryPolicy().GetMaxAttempts(); n < 0 || n > retryAttemptLimit {
		return errInvalid("retry_policy.max_attempts must be between 0 and %d.", retryAttemptLimit)
	}

	return validateLabels(t.GetLabels())
}

// transportPlan names the Pub/Sub resources a trigger uses.
type transportPlan struct {
	topic      string
	sub        string
	ownedTopic bool
}

func planTransport(project, location, id, uid string, t *eventarcpb.Trigger) (transportPlan, error) {
	suffix := strings.ReplaceAll(uid, "-", "")[:suffixLen]
	plan := transportPlan{
		sub: "projects/" + project + "/subscriptions/eventarc-" + location + "-" + id + "-sub-" + suffix,
	}

	supplied := t.GetTransport().GetPubsub().GetTopic()

	switch {
	case supplied != "":
		if !topicRe.MatchString(supplied) {
			return plan, errInvalid("transport.pubsub.topic %q must be projects/{project}/topics/{topic}.", supplied)
		}

		if eventType(t.GetEventFilters()) != typePubsubPublished {
			return plan, errInvalid("transport.pubsub.topic can only be set for %s triggers.", typePubsubPublished)
		}

		plan.topic = supplied
	default:
		plan.topic = "projects/" + project + "/topics/eventarc-" + location + "-" + id + "-" + suffix
		plan.ownedTopic = true
	}

	return plan, nil
}

// provision creates the transport topic and push subscription.
func (b *InMemoryBackend) provision(plan transportPlan, trigger string) error {
	if b.svc.PubSub == nil || plan.topic == "" {
		return nil
	}

	if b.pushBase == "" {
		return errPrecondition("The Eventarc listener is not running; event delivery cannot be set up.")
	}

	if plan.ownedTopic {
		if _, err := b.svc.PubSub.CreateTopic(&pubsubpb.Topic{Name: plan.topic}); err != nil {
			return errPrecondition("Could not create transport topic %q: %v", plan.topic, err)
		}
	} else if _, err := b.svc.PubSub.GetTopic(plan.topic); err != nil {
		return errPrecondition("Pub/Sub topic %q does not exist.", plan.topic)
	}

	_, err := b.svc.PubSub.CreateSubscription(&pubsubpb.Subscription{
		Name:  plan.sub,
		Topic: plan.topic,
		PushConfig: &pubsubpb.PushConfig{
			PushEndpoint: b.pushBase + pushPath + trigger,
		},
		AckDeadlineSeconds:       subscriptionAck,
		MessageRetentionDuration: &durationpb.Duration{Seconds: subscriptionKeep},
		RetryPolicy: &pubsubpb.RetryPolicy{
			MinimumBackoff: durationpb.New(b.retryMin),
			MaximumBackoff: durationpb.New(b.retryMax),
		},
	})
	if err != nil {
		if plan.ownedTopic {
			_ = b.svc.PubSub.DeleteTopic(plan.topic)
		}

		return errPrecondition("Could not create transport subscription %q: %v", plan.sub, err)
	}

	return nil
}

func (b *InMemoryBackend) deprovision(rec *triggerRec) {
	if b.svc.PubSub == nil {
		return
	}

	if sub := rec.pb.GetTransport().GetPubsub().GetSubscription(); sub != "" {
		_ = b.svc.PubSub.DeleteSubscription(sub)
	}

	if rec.ownedTopic {
		_ = b.svc.PubSub.DeleteTopic(rec.pb.GetTransport().GetPubsub().GetTopic())
	}
}

func (b *InMemoryBackend) buildTrigger(
	project, location, id string, in *eventarcpb.Trigger,
) (*eventarcpb.Trigger, transportPlan, error) {
	if err := validateTrigger(in); err != nil {
		return nil, transportPlan{}, err
	}

	t := clone(in)
	t.Name = childName(project, location, kindTriggers, id)
	t.Uid = uuid.NewString()
	t.CreateTime = b.ts()
	t.UpdateTime = b.ts()
	t.Conditions = map[string]*eventarcpb.StateCondition{}
	t.SatisfiesPzs = false

	if t.GetEventDataContentType() == "" {
		t.EventDataContentType = contentTypeJSON
	}

	if r := t.GetDestination().GetCloudRun(); r != nil {
		t.Destination = &eventarcpb.Destination{Descriptor_: &eventarcpb.Destination_CloudRun{CloudRun: r}}
	}

	plan, err := planTransport(project, location, id, t.GetUid(), t)
	if err != nil {
		return nil, plan, err
	}

	if t.GetChannel() != "" {
		plan = transportPlan{}
		t.Transport = nil
	} else {
		t.Transport = &eventarcpb.Transport{
			Intermediary: &eventarcpb.Transport_Pubsub{
				Pubsub: &eventarcpb.Pubsub{Topic: plan.topic, Subscription: plan.sub},
			},
		}
	}

	t.Etag = etagOf(t)

	return t, plan, nil
}

func (b *InMemoryBackend) checkChannelLocked(t *eventarcpb.Trigger) error {
	if t.GetChannel() == "" {
		return nil
	}

	if _, _, _, err := parseChild(t.GetChannel(), kindChannels); err != nil {
		return err
	}

	if _, ok := b.channels[t.GetChannel()]; !ok {
		return errNotFound("Channel %q not found.", t.GetChannel())
	}

	return nil
}

// CreateTrigger creates a trigger and its transport; the returned operation is already done.
func (b *InMemoryBackend) CreateTrigger(in *eventarcpb.CreateTriggerRequest) (*longrunningpb.Operation, error) {
	project, location, err := parseParent(in.GetParent())
	if err != nil {
		return nil, err
	}

	if !knownLocation(location) {
		return nil, errInvalid("Location %q is not supported by Eventarc.", location)
	}

	if !triggerIDRe.MatchString(in.GetTriggerId()) {
		return nil, errInvalid(
			"Invalid trigger id %q: use 3-63 lowercase letters, digits and hyphens, starting with a letter.",
			in.GetTriggerId(),
		)
	}

	if in.GetTrigger() == nil {
		return nil, errInvalid("trigger is required.")
	}

	t, plan, err := b.buildTrigger(project, location, in.GetTriggerId(), in.GetTrigger())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateTrigger")
	defer b.mu.Unlock()

	if _, ok := b.triggers[t.GetName()]; ok {
		return nil, errExists("Trigger %q already exists.", t.GetName())
	}

	if err = b.checkChannelLocked(t); err != nil {
		return nil, err
	}

	if !in.GetValidateOnly() {
		if err = b.provision(plan, t.GetName()); err != nil {
			return nil, err
		}

		b.triggers[t.GetName()] = &triggerRec{pb: t, ownedTopic: plan.ownedTopic}
	}

	return b.finish(project, location, t.GetName(), "create", clone(t))
}

// GetTrigger returns a trigger.
func (b *InMemoryBackend) GetTrigger(name string) (*eventarcpb.Trigger, error) {
	if _, _, _, err := parseChild(name, kindTriggers); err != nil {
		return nil, err
	}

	b.mu.RLock("GetTrigger")
	defer b.mu.RUnlock()

	rec, ok := b.triggers[name]
	if !ok {
		return nil, errNotFound("Trigger %q not found.", name)
	}

	return clone(rec.pb), nil
}

// ListTriggers lists the triggers of a location, or of every location for "-".
func (b *InMemoryBackend) ListTriggers(in *eventarcpb.ListTriggersRequest) (*eventarcpb.ListTriggersResponse, error) {
	project, location, err := parseParent(in.GetParent())
	if err != nil {
		return nil, err
	}

	if in.GetFilter() != "" {
		return nil, errInvalid("filter is not supported for triggers.")
	}

	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	b.mu.RLock("ListTriggers")

	var all []*eventarcpb.Trigger

	for _, rec := range b.triggers {
		if inScope(rec.pb.GetName(), project, location) {
			all = append(all, clone(rec.pb))
		}
	}

	b.mu.RUnlock()

	if err = orderTriggers(all, in.GetOrderBy()); err != nil {
		return nil, err
	}

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultListPage)

	return &eventarcpb.ListTriggersResponse{Triggers: pg.Data, NextPageToken: pg.Next}, nil
}

func inScope(name, project, location string) bool {
	p, l, _, err := parseChild(name, kindTriggers)
	if err != nil {
		p, l, _, err = parseChild(name, kindChannels)
	}

	return err == nil && p == project && (location == wildcardLocation || l == location)
}

// DeleteTrigger removes a trigger and the transport resources it owns.
func (b *InMemoryBackend) DeleteTrigger(in *eventarcpb.DeleteTriggerRequest) (*longrunningpb.Operation, error) {
	project, location, _, err := parseChild(in.GetName(), kindTriggers)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteTrigger")
	defer b.mu.Unlock()

	rec, ok := b.triggers[in.GetName()]
	if !ok {
		if in.GetAllowMissing() {
			return b.finish(project, location, in.GetName(), "delete", &eventarcpb.Trigger{Name: in.GetName()})
		}

		return nil, errNotFound("Trigger %q not found.", in.GetName())
	}

	if e := in.GetEtag(); e != "" && e != rec.pb.GetEtag() {
		return nil, errAborted("The trigger was modified since it was read; read it again.")
	}

	if !in.GetValidateOnly() {
		b.deprovision(rec)
		delete(b.triggers, in.GetName())
	}

	return b.finish(project, location, in.GetName(), "delete", clone(rec.pb))
}

func (b *InMemoryBackend) applyTriggerMask(
	next, src *eventarcpb.Trigger, paths []string,
) error {
	if len(paths) == 0 {
		paths = presentTriggerPaths(src)
	}

	for _, p := range paths {
		top, _, _ := strings.Cut(p, ".")

		switch {
		case p == updateMaskAll:
			next.EventFilters, next.ServiceAccount = src.GetEventFilters(), src.GetServiceAccount()
			next.Destination, next.Labels = src.GetDestination(), src.GetLabels()
			next.RetryPolicy, next.EventDataContentType = src.GetRetryPolicy(), src.GetEventDataContentType()
		case top == maskEventFilters:
			next.EventFilters = src.GetEventFilters()
		case top == maskServiceAcct:
			next.ServiceAccount = src.GetServiceAccount()
		case top == maskDestination:
			next.Destination = src.GetDestination()
		case top == maskLabels:
			next.Labels = src.GetLabels()
		case top == maskRetry:
			next.RetryPolicy = src.GetRetryPolicy()
		case top == maskContentType:
			next.EventDataContentType = src.GetEventDataContentType()
		default:
			return errInvalid("Unsupported update_mask path %q for Trigger.", p)
		}
	}

	return nil
}

func presentTriggerPaths(src *eventarcpb.Trigger) []string {
	var paths []string

	add := func(ok bool, p string) {
		if ok {
			paths = append(paths, p)
		}
	}

	add(len(src.GetEventFilters()) > 0, maskEventFilters)
	add(src.GetServiceAccount() != "", maskServiceAcct)
	add(src.GetDestination() != nil, maskDestination)
	add(len(src.GetLabels()) > 0, maskLabels)
	add(src.GetRetryPolicy() != nil, maskRetry)
	add(src.GetEventDataContentType() != "", maskContentType)

	return paths
}

// UpdateTrigger updates the masked fields of a trigger.
func (b *InMemoryBackend) UpdateTrigger(in *eventarcpb.UpdateTriggerRequest) (*longrunningpb.Operation, error) {
	src := in.GetTrigger()

	project, location, id, err := parseChild(src.GetName(), kindTriggers)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateTrigger")
	defer b.mu.Unlock()

	rec, ok := b.triggers[src.GetName()]
	if !ok {
		return b.upsertMissingLocked(in, project, location, id)
	}

	if e := src.GetEtag(); e != "" && e != rec.pb.GetEtag() {
		return nil, errAborted("The trigger was modified since it was read; read it again.")
	}

	next := clone(rec.pb)
	if err = b.applyTriggerMask(next, clone(src), in.GetUpdateMask().GetPaths()); err != nil {
		return nil, err
	}

	if err = validateTrigger(next); err != nil {
		return nil, err
	}

	if eventType(next.GetEventFilters()) != eventType(rec.pb.GetEventFilters()) {
		return nil, errInvalid("The type event filter cannot be changed.")
	}

	next.UpdateTime = b.ts()
	next.Etag = etagOf(next)

	if !in.GetValidateOnly() {
		rec.pb = next
	}

	return b.finish(project, location, next.GetName(), "update", clone(next))
}

func (b *InMemoryBackend) upsertMissingLocked(
	in *eventarcpb.UpdateTriggerRequest, project, location, id string,
) (*longrunningpb.Operation, error) {
	name := in.GetTrigger().GetName()

	if !in.GetAllowMissing() {
		return nil, errNotFound("Trigger %q not found.", name)
	}

	t, plan, err := b.buildTrigger(project, location, id, in.GetTrigger())
	if err != nil {
		return nil, err
	}

	if err = b.checkChannelLocked(t); err != nil {
		return nil, err
	}

	if !in.GetValidateOnly() {
		if err = b.provision(plan, name); err != nil {
			return nil, err
		}

		b.triggers[name] = &triggerRec{pb: t, ownedTopic: plan.ownedTopic}
	}

	return b.finish(project, location, name, "update", clone(t))
}
