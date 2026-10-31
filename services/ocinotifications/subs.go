package ocinotifications

import (
	"encoding/json"
	"net/http"
	"net/mail"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxEndpoint       = 512
	defaultRetryMs    = 7200000
	minRetryMs        = 10000
	maxRetryMs        = 86400000
	policyExponential = "EXPONENTIAL"
	apiBase           = "/20181201"

	protoEmail     = "EMAIL"
	protoHTTPS     = "HTTPS"
	protoCustom    = "CUSTOM_HTTPS"
	protoFunctions = "ORACLE_FUNCTIONS"
	protoPagerDuty = "PAGERDUTY"
	protoSlack     = "SLACK"
	protoSMS       = "SMS"
)

var e164 = regexp.MustCompile(`^\+[1-9][0-9]{6,14}$`)

// CreateSubscriptionIn is the CreateSubscription request body.
type CreateSubscriptionIn struct {
	Tags
	Metadata    *string `json:"metadata"`
	TopicID     string  `json:"topicId"`
	Compartment string  `json:"compartmentId"`
	Protocol    string  `json:"protocol"`
	Endpoint    string  `json:"endpoint"`
}

type retryPolicyWire struct {
	PolicyType       string `json:"policyType"`
	MaxRetryDuration int    `json:"maxRetryDuration"`
}

type deliveryPolicyWire struct {
	BackoffRetryPolicy *retryPolicyWire `json:"backoffRetryPolicy,omitempty"`
}

// UpdateSubscriptionIn is the UpdateSubscription request body.
type UpdateSubscriptionIn struct {
	Tags
	DeliveryPolicy *deliveryPolicyWire `json:"deliveryPolicy"`
}

type SubWire struct {
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	ID             string                    `json:"id"`
	TopicID        string                    `json:"topicId"`
	Protocol       string                    `json:"protocol"`
	Endpoint       string                    `json:"endpoint"`
	LifecycleState string                    `json:"lifecycleState"`
	CompartmentID  string                    `json:"compartmentId"`
	DeliverPolicy  string                    `json:"deliverPolicy"`
	Etag           string                    `json:"etag"`
	CreatedTime    int64                     `json:"createdTime"`
}

type subSummaryWire struct {
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	DeliveryPolicy deliveryPolicyWire        `json:"deliveryPolicy"`
	ID             string                    `json:"id"`
	TopicID        string                    `json:"topicId"`
	Protocol       string                    `json:"protocol"`
	Endpoint       string                    `json:"endpoint"`
	LifecycleState string                    `json:"lifecycleState"`
	CompartmentID  string                    `json:"compartmentId"`
	Etag           string                    `json:"etag"`
	CreatedTime    int64                     `json:"createdTime"`
}

type ConfirmationWire struct {
	TopicName      string `json:"topicName"`
	TopicID        string `json:"topicId"`
	Endpoint       string `json:"endpoint"`
	UnsubscribeURL string `json:"unsubscribeUrl"`
	Message        string `json:"message"`
	SubscriptionID string `json:"subscriptionId"`
}

func policyOf(s *Subscription) deliveryPolicyWire {
	return deliveryPolicyWire{
		BackoffRetryPolicy: &retryPolicyWire{PolicyType: policyExponential, MaxRetryDuration: s.MaxRetryMs},
	}
}

func subView(s *Subscription) SubWire {
	pol, _ := json.Marshal(policyOf(s)) //nolint:errchkjson // static shape

	return SubWire{
		ID:             s.ID,
		TopicID:        s.TopicID,
		Protocol:       s.Protocol,
		Endpoint:       s.Endpoint,
		LifecycleState: s.State,
		CompartmentID:  s.CompartmentID,
		DeliverPolicy:  string(pol),
		Etag:           s.Etag,
		CreatedTime:    s.TimeCreated.UnixMilli(),
		FreeformTags:   s.Tags.FreeformTags,
		DefinedTags:    s.Tags.DefinedTags,
	}
}

func subSummary(s *Subscription) subSummaryWire {
	return subSummaryWire{
		ID:             s.ID,
		TopicID:        s.TopicID,
		Protocol:       s.Protocol,
		Endpoint:       s.Endpoint,
		LifecycleState: s.State,
		CompartmentID:  s.CompartmentID,
		DeliveryPolicy: policyOf(s),
		Etag:           s.Etag,
		CreatedTime:    s.TimeCreated.UnixMilli(),
		FreeformTags:   s.Tags.FreeformTags,
		DefinedTags:    s.Tags.DefinedTags,
	}
}

func validateHTTPURL(endpoint string) *APIError {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errParam("endpoint must be a URL beginning with http: or https:")
	}

	return nil
}

// validateEndpoint checks the endpoint format each protocol expects.
func validateEndpoint(protocol, endpoint string) *APIError {
	if endpoint == "" {
		return errMissing("endpoint is required")
	}

	if len(endpoint) > maxEndpoint {
		return errParam("endpoint must be at most 512 characters")
	}

	switch protocol {
	case protoEmail:
		if a, err := mail.ParseAddress(endpoint); err != nil || a.Address != endpoint {
			return errParam("endpoint must be an email address")
		}
	case protoHTTPS, protoCustom, protoSlack:
		return validateHTTPURL(endpoint)
	case protoSMS:
		if !e164.MatchString(endpoint) {
			return errParam("endpoint must be a phone number in E.164 format")
		}
	case protoFunctions:
		if !strings.HasPrefix(endpoint, "ocid1.fnfunc.") {
			return errParam("endpoint must be the OCID of a function")
		}
	case protoPagerDuty:
	default:
		return errParam("protocol must be one of CUSTOM_HTTPS, EMAIL, HTTPS, ORACLE_FUNCTIONS, PAGERDUTY, SLACK, SMS")
	}

	return nil
}

func needsConfirmation(protocol string) bool {
	return protocol != protoFunctions && protocol != protoPagerDuty
}

func (b *InMemoryBackend) sub(id string) (*Subscription, *APIError) {
	b.sweepLocked()

	s, ok := b.subs[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return s, nil
}

func (b *InMemoryBackend) confirmationURL(s *Subscription) string {
	return s.BaseURL + apiBase + "/subscriptions/" + s.ID + "/confirmation?token=" + url.QueryEscape(s.ConfirmToken) +
		"&protocol=" + s.Protocol
}

func unsubscribeURL(s *Subscription) string {
	return s.BaseURL + apiBase + "/subscriptions/" + s.ID + "/unsubscription?token=" + url.QueryEscape(s.UnsubToken) +
		"&protocol=" + s.Protocol
}

// CreateSubscription subscribes an endpoint to a topic; protocols that need it start PENDING and are sent a
// confirmation.
func (b *InMemoryBackend) CreateSubscription(in CreateSubscriptionIn, baseURL string) (SubWire, error) {
	if in.TopicID == "" || in.Compartment == "" {
		return SubWire{}, errMissing("topicId and compartmentId are required")
	}

	if in.Protocol == "" {
		return SubWire{}, errMissing("protocol is required")
	}

	if aerr := validateEndpoint(in.Protocol, in.Endpoint); aerr != nil {
		return SubWire{}, aerr
	}

	b.mu.Lock("CreateSubscription")
	defer b.mu.Unlock()

	topic, aerr := b.activeTopic(in.TopicID)
	if aerr != nil {
		return SubWire{}, aerr
	}

	for _, s := range b.subs {
		if s.TopicID == in.TopicID && s.Protocol == in.Protocol && s.Endpoint == in.Endpoint {
			return SubWire{}, errExists("The endpoint is already subscribed to this topic")
		}
	}

	s := &Subscription{
		ID: newOCID("onssubscription"), TopicID: topic.ID, CompartmentID: in.Compartment, Protocol: in.Protocol,
		Endpoint: in.Endpoint, State: subActive, BaseURL: baseURL, TimeCreated: b.nowUTC(), Tags: in.Tags,
		ConfirmToken: uuid.NewString(), UnsubToken: uuid.NewString(), Etag: newEtag(), MaxRetryMs: defaultRetryMs,
		Seq: b.nextSeq(),
	}
	if in.Metadata != nil {
		s.Metadata = *in.Metadata
	}

	if needsConfirmation(s.Protocol) {
		s.State = subPending
		b.queueConfirmation(s, topic)
	}

	b.subs[s.ID] = s

	return subView(s), nil
}

// GetSubscription returns one subscription.
func (b *InMemoryBackend) GetSubscription(id string) (SubWire, error) {
	b.mu.Lock("GetSubscription")
	defer b.mu.Unlock()

	s, aerr := b.sub(id)
	if aerr != nil {
		return SubWire{}, aerr
	}

	return subView(s), nil
}

// ListSubscriptions lists the subscriptions of a compartment, optionally of one topic.
func (b *InMemoryBackend) ListSubscriptions(
	compartment, topicID string,
	o ListOpts,
) (page.Page[subSummaryWire], error) {
	if compartment == "" {
		return page.Page[subSummaryWire]{}, errMissing("compartmentId is required")
	}

	b.mu.Lock("ListSubscriptions")
	defer b.mu.Unlock()

	b.sweepLocked()

	var all []*Subscription

	for _, s := range b.subs {
		if s.CompartmentID == compartment && (topicID == "" || s.TopicID == topicID) {
			all = append(all, s)
		}
	}

	o.SortBy = ""
	pg := paginate(all, o, func(s *Subscription) string { return s.State },
		func(s *Subscription) (time.Time, int64) { return s.TimeCreated, s.Seq })

	out := make([]subSummaryWire, 0, len(pg.Data))
	for _, s := range pg.Data {
		out = append(out, subSummary(s))
	}

	return page.Page[subSummaryWire]{Data: out, Next: pg.Next}, nil
}

func validatePolicy(p *deliveryPolicyWire) *APIError {
	if p == nil || p.BackoffRetryPolicy == nil {
		return nil
	}

	r := p.BackoffRetryPolicy
	if r.PolicyType != policyExponential {
		return errParam("backoffRetryPolicy.policyType must be EXPONENTIAL")
	}

	if r.MaxRetryDuration < minRetryMs || r.MaxRetryDuration > maxRetryMs {
		return errParam("backoffRetryPolicy.maxRetryDuration must be between " + strconv.Itoa(minRetryMs) +
			" and " + strconv.Itoa(maxRetryMs) + " milliseconds")
	}

	return nil
}

// UpdateSubscription changes a subscription's delivery policy and tags.
func (b *InMemoryBackend) UpdateSubscription(
	id, ifMatch string,
	in UpdateSubscriptionIn,
) (UpdateSubscriptionIn, string, error) {
	if aerr := validatePolicy(in.DeliveryPolicy); aerr != nil {
		return UpdateSubscriptionIn{}, "", aerr
	}

	b.mu.Lock("UpdateSubscription")
	defer b.mu.Unlock()

	s, aerr := b.sub(id)
	if aerr != nil {
		return UpdateSubscriptionIn{}, "", aerr
	}

	if aerr = checkEtag(s.Etag, ifMatch); aerr != nil {
		return UpdateSubscriptionIn{}, "", aerr
	}

	if in.DeliveryPolicy != nil && in.DeliveryPolicy.BackoffRetryPolicy != nil {
		s.MaxRetryMs = in.DeliveryPolicy.BackoffRetryPolicy.MaxRetryDuration
	}

	s.Tags.merge(in.Tags)
	s.Etag = newEtag()
	pol := policyOf(s)

	return UpdateSubscriptionIn{DeliveryPolicy: &pol, Tags: s.Tags}, s.Etag, nil
}

func (b *InMemoryBackend) dropSubscription(id string) {
	delete(b.subs, id)

	for did, d := range b.deliveries {
		if d.SubscriptionID == id {
			delete(b.deliveries, did)
		}
	}
}

// DeleteSubscription removes a subscription and its deliveries.
func (b *InMemoryBackend) DeleteSubscription(id, ifMatch string) error {
	b.mu.Lock("DeleteSubscription")
	defer b.mu.Unlock()

	s, aerr := b.sub(id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(s.Etag, ifMatch); aerr != nil {
		return aerr
	}

	b.dropSubscription(id)

	return nil
}

// ChangeSubscriptionCompartment moves a subscription to another compartment.
func (b *InMemoryBackend) ChangeSubscriptionCompartment(id, ifMatch, compartment string) error {
	if compartment == "" {
		return errMissing("compartmentId is required")
	}

	b.mu.Lock("ChangeSubscriptionCompartment")
	defer b.mu.Unlock()

	s, aerr := b.sub(id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(s.Etag, ifMatch); aerr != nil {
		return aerr
	}

	s.CompartmentID, s.Etag = compartment, newEtag()

	return nil
}

func (b *InMemoryBackend) tokenSub(id, token, protocol string, unsub bool) (*Subscription, *Topic, *APIError) {
	s, aerr := b.sub(id)
	if aerr != nil {
		return nil, nil, aerr
	}

	want := s.ConfirmToken
	if unsub {
		want = s.UnsubToken
	}

	if token == "" || token != want || !strings.EqualFold(protocol, s.Protocol) {
		return nil, nil, errParam("The token or protocol does not match the subscription")
	}

	return s, b.topics[s.TopicID], nil
}

// ConfirmSubscription activates a PENDING subscription from the token sent to its endpoint.
func (b *InMemoryBackend) ConfirmSubscription(id, token, protocol string) (ConfirmationWire, error) {
	b.mu.Lock("ConfirmSubscription")
	defer b.mu.Unlock()

	s, topic, aerr := b.tokenSub(id, token, protocol, false)
	if aerr != nil {
		return ConfirmationWire{}, aerr
	}

	if s.State == subPending {
		s.State, s.Etag = subActive, newEtag()
	}

	b.cancelConfirmations(s.ID)

	name := ""
	if topic != nil {
		name = topic.Name
	}

	return ConfirmationWire{
		TopicName: name, TopicID: s.TopicID, Endpoint: s.Endpoint, UnsubscribeURL: unsubscribeURL(s),
		Message: "Subscription confirmed", SubscriptionID: s.ID,
	}, nil
}

// Unsubscribe removes a subscription from the token embedded in its unsubscribe link.
func (b *InMemoryBackend) Unsubscribe(id, token, protocol string) (string, error) {
	b.mu.Lock("Unsubscribe")
	defer b.mu.Unlock()

	s, _, aerr := b.tokenSub(id, token, protocol, true)
	if aerr != nil {
		return "", aerr
	}

	b.dropSubscription(s.ID)

	return "Subscription " + s.ID + " was unsubscribed", nil
}

// ResendConfirmation sends a PENDING subscription's confirmation again.
func (b *InMemoryBackend) ResendConfirmation(id string) (SubWire, string, error) {
	b.mu.Lock("ResendConfirmation")
	defer b.mu.Unlock()

	s, aerr := b.sub(id)
	if aerr != nil {
		return SubWire{}, "", aerr
	}

	if s.State != subPending {
		return SubWire{}, "", errState(
			"Subscription " + id + " is " + s.State + "; only PENDING subscriptions can be resent",
		)
	}

	topic, aerr := b.activeTopic(s.TopicID)
	if aerr != nil {
		return SubWire{}, "", aerr
	}

	b.cancelConfirmations(s.ID)
	b.queueConfirmation(s, topic)

	return subView(s), s.Etag, nil
}

func (b *InMemoryBackend) topicSubs(topicID string) []*Subscription {
	var out []*Subscription

	for _, s := range b.subs {
		if s.TopicID == topicID {
			out = append(out, s)
		}
	}

	slices.SortFunc(out, func(a, c *Subscription) int { return int(a.Seq - c.Seq) })

	return out
}

func (h *Handler) createSubscription(q *req) error {
	var in CreateSubscriptionIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, err := h.Backend.CreateSubscription(in, q.endpoint())
	if err != nil {
		return err
	}

	h.Backend.Kick()

	return q.json(http.StatusOK, v, v.Etag)
}

func (h *Handler) getSubscription(q *req) error {
	v, err := h.Backend.GetSubscription(q.param("subscriptionId"))
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, v, v.Etag)
}

func (h *Handler) listSubscriptions(q *req) error {
	pg, err := h.Backend.ListSubscriptions(q.query("compartmentId"), q.query("topicId"), q.listOpts())
	if err != nil {
		return err
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}

func (h *Handler) updateSubscription(q *req) error {
	var in UpdateSubscriptionIn
	if err := q.decode(&in); err != nil {
		return err
	}

	out, etag, err := h.Backend.UpdateSubscription(q.param("subscriptionId"), q.ifMatch(), in)
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, out, etag)
}

func (h *Handler) deleteSubscription(q *req) error {
	if err := h.Backend.DeleteSubscription(q.param("subscriptionId"), q.ifMatch()); err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) changeSubscriptionCompartment(q *req) error {
	var in changeCompartmentIn
	if err := q.decode(&in); err != nil {
		return err
	}

	if err := h.Backend.ChangeSubscriptionCompartment(
		q.param("subscriptionId"),
		q.ifMatch(),
		in.CompartmentID,
	); err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusAccepted)

	return nil
}

func (h *Handler) confirmSubscription(q *req) error {
	out, err := h.Backend.ConfirmSubscription(q.param("subscriptionId"), q.query("token"), q.query("protocol"))
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, out, "")
}

func (h *Handler) unsubscribe(q *req) error {
	msg, err := h.Backend.Unsubscribe(q.param("subscriptionId"), q.query("token"), q.query("protocol"))
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, msg, "")
}

func (h *Handler) resendConfirmation(q *req) error {
	v, etag, err := h.Backend.ResendConfirmation(q.param("subscriptionId"))
	if err != nil {
		return err
	}

	h.Backend.Kick()

	return q.json(http.StatusOK, v, etag)
}
