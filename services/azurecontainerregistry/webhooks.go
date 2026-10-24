package azurecontainerregistry

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	actionPush        = "push"
	actionDelete      = "delete"
	actionChartPush   = "chart_push"
	actionChartDelete = "chart_delete"
	actionPing        = "ping"
	statusEnabled     = "enabled"
	helmConfigType    = "application/vnd.cncf.helm.config.v1+json"
	httpVersion       = "1.1"
	maxEventsPerHook  = 100
	maxResponseBytes  = 64 << 10
	deliveryTimeout   = 10 * time.Second
	timeLayout        = time.RFC3339Nano
	wildcard          = "*"
)

// Webhook is a registry webhook fired on image and chart pushes and deletes.
type Webhook struct {
	Headers    map[string]string `json:"headers,omitempty"`
	Name       string            `json:"name"`
	ServiceURI string            `json:"serviceUri"`
	Scope      string            `json:"scope,omitempty"`
	Status     string            `json:"status"`
	Actions    []string          `json:"actions"`
}

// EventTarget is the artifact an event is about.
type EventTarget struct {
	MediaType  string `json:"mediaType,omitempty"`
	Digest     string `json:"digest,omitempty"`
	Repository string `json:"repository,omitempty"`
	Tag        string `json:"tag,omitempty"`
	Size       int64  `json:"size,omitempty"`
	Length     int64  `json:"length,omitempty"`
}

// EventRequest is the registry request that caused an event.
type EventRequest struct {
	ID        string `json:"id"`
	Host      string `json:"host"`
	Method    string `json:"method"`
	Useragent string `json:"useragent"`
}

// EventContent is the JSON body posted to a webhook's service URI.
type EventContent struct {
	Target    *EventTarget  `json:"target,omitempty"`
	Request   *EventRequest `json:"request,omitempty"`
	ID        string        `json:"id"`
	Timestamp string        `json:"timestamp"`
	Action    string        `json:"action"`
}

// EventRequestMessage is the HTTP request delivered to the service URI.
type EventRequestMessage struct {
	Headers    map[string]string `json:"headers"`
	Content    EventContent      `json:"content"`
	Method     string            `json:"method"`
	RequestURI string            `json:"requestUri"`
	Version    string            `json:"version"`
}

// EventResponseMessage is the service URI's HTTP response.
type EventResponseMessage struct {
	Headers      map[string]string `json:"headers"`
	Content      string            `json:"content"`
	ReasonPhrase string            `json:"reasonPhrase"`
	StatusCode   string            `json:"statusCode,omitempty"`
	Version      string            `json:"version"`
}

// WebhookEvent is one recorded delivery attempt.
type WebhookEvent struct {
	EventResponseMessage *EventResponseMessage `json:"eventResponseMessage,omitempty"`
	ID                   string                `json:"id"`
	EventRequestMessage  EventRequestMessage   `json:"eventRequestMessage"`
}

// RequestInfo describes the registry request behind a manifest event.
type RequestInfo struct {
	ID        string
	Host      string
	Method    string
	Useragent string
}

// ManifestChange is a manifest push or delete the registry observed.
type ManifestChange struct {
	Req        RequestInfo
	Repository string
	Tag        string
	Digest     string
	MediaType  string
	ConfigType string
	Size       int64
	Delete     bool
}

func (c ManifestChange) action() string {
	chart := c.ConfigType == helmConfigType

	switch {
	case c.Delete && chart:
		return actionChartDelete
	case c.Delete:
		return actionDelete
	case chart:
		return actionChartPush
	}

	return actionPush
}

func globMatch(pattern, s string) bool {
	switch {
	case pattern == wildcard:
		return true
	case strings.HasSuffix(pattern, wildcard):
		return strings.HasPrefix(s, strings.TrimSuffix(pattern, wildcard))
	}

	return pattern == s
}

// scopeMatches applies a webhook scope: empty is every repository, "repo" is "repo:latest", "repo:*" every tag.
func scopeMatches(scope, repo, tag string) bool {
	if scope == "" {
		return true
	}

	repoPat, tagPat, hasTag := strings.Cut(scope, ":")
	if !hasTag {
		tagPat = "latest"
	}

	return globMatch(repoPat, repo) && globMatch(tagPat, tag)
}

func (w Webhook) wants(c ManifestChange) bool {
	return strs.Equal(w.Status, statusEnabled) &&
		slices.ContainsFunc(w.Actions, func(a string) bool { return strs.Equal(a, c.action()) }) &&
		scopeMatches(w.Scope, c.Repository, c.Tag)
}

// SetWebhook creates or replaces the webhook, keeping the events it already recorded.
func (b *InMemoryBackend) SetWebhook(reg string, w Webhook) error {
	b.mu.Lock("SetWebhook")
	defer b.mu.Unlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		return ErrRegistryNotFound
	}

	if r.Webhooks == nil {
		r.Webhooks = map[string]*Webhook{}
	}

	r.Webhooks[strs.Fold(w.Name)] = &w

	return nil
}

// DeleteWebhook removes the webhook with its events; a missing one is not an error.
func (b *InMemoryBackend) DeleteWebhook(reg, name string) error {
	b.mu.Lock("DeleteWebhook")
	defer b.mu.Unlock()

	if r, ok := b.regs[strs.Fold(reg)]; ok {
		delete(r.Webhooks, strs.Fold(name))
		delete(r.Events, strs.Fold(name))
	}

	return nil
}

// HasWebhooks reports whether reg has any webhook.
func (b *InMemoryBackend) HasWebhooks(reg string) bool {
	b.mu.RLock("HasWebhooks")
	defer b.mu.RUnlock()

	r, ok := b.regs[strs.Fold(reg)]

	return ok && len(r.Webhooks) > 0
}

// WebhookEvents returns the webhook's recorded deliveries, oldest first.
func (b *InMemoryBackend) WebhookEvents(reg, name string) ([]WebhookEvent, error) {
	b.mu.RLock("WebhookEvents")
	defer b.mu.RUnlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		return nil, ErrRegistryNotFound
	}

	if _, found := r.Webhooks[strs.Fold(name)]; !found {
		return nil, ErrWebhookNotFound
	}

	return slices.Clone(r.Events[strs.Fold(name)]), nil
}

// PingWebhook delivers a ping event to the webhook and returns the event id.
func (b *InMemoryBackend) PingWebhook(reg, name string) (string, error) {
	b.mu.RLock("PingWebhook")

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		b.mu.RUnlock()

		return "", ErrRegistryNotFound
	}

	w, found := r.Webhooks[strs.Fold(name)]
	if !found {
		b.mu.RUnlock()

		return "", ErrWebhookNotFound
	}

	hook := *w

	b.mu.RUnlock()

	content := EventContent{ID: uuid.NewString(), Timestamp: b.now().UTC().Format(timeLayout), Action: actionPing}
	b.deliver(reg, hook, content)

	return content.ID, nil
}

// FireManifest delivers the change to every enabled webhook of reg whose actions and scope match.
func (b *InMemoryBackend) FireManifest(reg string, c ManifestChange) {
	b.mu.RLock("FireManifest")

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		b.mu.RUnlock()

		return
	}

	var hooks []Webhook

	for _, w := range r.Webhooks {
		if w.wants(c) {
			hooks = append(hooks, *w)
		}
	}

	b.mu.RUnlock()

	for _, w := range hooks {
		b.deliver(reg, w, b.contentFor(c))
	}
}

func (b *InMemoryBackend) contentFor(c ManifestChange) EventContent {
	return EventContent{
		ID: uuid.NewString(), Timestamp: b.now().UTC().Format(timeLayout), Action: c.action(),
		Target: &EventTarget{
			MediaType: c.MediaType, Size: c.Size, Length: c.Size, Digest: c.Digest,
			Repository: c.Repository, Tag: c.Tag,
		},
		Request: &EventRequest{ID: c.Req.ID, Host: c.Req.Host, Method: c.Req.Method, Useragent: c.Req.Useragent},
	}
}

func (b *InMemoryBackend) deliver(reg string, w Webhook, content EventContent) {
	b.workers().Go("webhook-delivery", func(ctx context.Context) { b.send(ctx, reg, w, content) })
}

func (b *InMemoryBackend) send(ctx context.Context, reg string, w Webhook, content EventContent) {
	body, err := json.Marshal(content)
	if err != nil {
		return
	}

	headers := map[string]string{"Content-Type": "application/json"}
	for k, v := range w.Headers {
		headers[k] = v
	}

	ev := WebhookEvent{
		ID: content.ID,
		EventRequestMessage: EventRequestMessage{
			Content: content, Headers: headers, Method: http.MethodPost, RequestURI: w.ServiceURI,
			Version: httpVersion,
		},
	}

	ev.EventResponseMessage = b.post(ctx, w.ServiceURI, headers, body)
	b.record(reg, w.Name, ev)
}

func (b *InMemoryBackend) post(ctx context.Context, uri string, headers map[string]string, body []byte) *EventResponseMessage {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uri, bytes.NewReader(body))
	if err != nil {
		return &EventResponseMessage{ReasonPhrase: err.Error(), Version: httpVersion, Headers: map[string]string{}}
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := b.httpc.Do(req)
	if err != nil {
		logger.Load(ctx).DebugContext(ctx, "azurecontainerregistry: webhook delivery failed", "error", err)

		return &EventResponseMessage{ReasonPhrase: err.Error(), Version: httpVersion, Headers: map[string]string{}}
	}

	defer resp.Body.Close()

	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	out := &EventResponseMessage{
		Content: string(raw), StatusCode: strconv.Itoa(resp.StatusCode),
		ReasonPhrase: strings.TrimSpace(strings.TrimPrefix(resp.Status, strconv.Itoa(resp.StatusCode))),
		Version:      httpVersion, Headers: map[string]string{},
	}

	for k := range resp.Header {
		out.Headers[k] = resp.Header.Get(k)
	}

	return out
}

func (b *InMemoryBackend) record(reg, name string, ev WebhookEvent) {
	b.mu.Lock("recordWebhookEvent")
	defer b.mu.Unlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		return
	}

	key := strs.Fold(name)
	if _, found := r.Webhooks[key]; !found {
		return
	}

	if r.Events == nil {
		r.Events = map[string][]WebhookEvent{}
	}

	events := append(r.Events[key], ev)
	if len(events) > maxEventsPerHook {
		events = slices.Clone(events[len(events)-maxEventsPerHook:])
	}

	r.Events[key] = events
}
