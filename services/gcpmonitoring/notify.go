package gcpmonitoring

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	payloadVersion = "1.2"
)

// target is a snapshot of a notification channel taken when the incident changed state.
type target struct {
	labels map[string]string
	secret map[string]string
	name   string
	typ    string
}

// Delivery records the outcome of one notification attempt.
type Delivery struct {
	At         time.Time `json:"at"`
	IncidentID string    `json:"incidentId"`
	Channel    string    `json:"channel"`
	Type       string    `json:"type"`
	State      string    `json:"state"`
	Result     string    `json:"result"`
	Error      string    `json:"error,omitempty"`
}

// Deliveries returns the most recent notification attempts, oldest first.
func (b *InMemoryBackend) Deliveries() []Delivery {
	b.mu.RLock("Deliveries")
	defer b.mu.RUnlock()

	return slices.Clone(b.deliveries)
}

func (b *InMemoryBackend) notificationLocked(
	p *monitoringpb.AlertPolicy, inc Incident, opened bool,
) notification {
	n := notification{incident: inc, policy: clonePolicy(p), opened: opened, pub: b.pub}

	for _, name := range p.GetNotificationChannels() {
		rec, ok := b.channels[name]
		if !ok || (rec.pb.GetEnabled() != nil && !rec.pb.GetEnabled().GetValue()) {
			continue
		}

		n.targets = append(n.targets, target{
			name:   name,
			typ:    rec.pb.GetType(),
			labels: maps.Clone(rec.pb.GetLabels()),
			secret: maps.Clone(rec.sensitive),
		})
	}

	return n
}

func (b *InMemoryBackend) record(d Delivery) {
	b.mu.Lock("record")
	defer b.mu.Unlock()

	b.deliveries = append(b.deliveries, d)
	if over := len(b.deliveries) - maxDeliveryLog; over > 0 {
		b.deliveries = slices.Clone(b.deliveries[over:])
	}
}

// deliver sends the notification to every target; failures are logged and recorded, never fatal.
func (b *InMemoryBackend) deliver(ctx context.Context, n notification) {
	payload, err := json.Marshal(incidentPayload(n))
	if err != nil {
		return
	}

	state := stateOpen
	if !n.opened {
		state = stateClosed
	}

	for _, t := range n.targets {
		d := Delivery{
			At: n.incident.StartedAt, IncidentID: n.incident.ID, Channel: t.name, Type: t.typ, State: state,
			Result: "delivered",
		}

		if !n.opened {
			d.At = n.incident.EndedAt
		}

		if err = b.send(ctx, n, t, payload); err != nil {
			d.Result, d.Error = "failed", err.Error()
			logger.Load(ctx).WarnContext(ctx, "gcpmonitoring: notification failed",
				"channel", t.name, "type", t.typ, "error", err)
		}

		b.record(d)
	}
}

var (
	errUnsupportedChannel = errors.New("delivery is not supported for this channel type")
	errNoPublisher        = errors.New("no Pub/Sub emulator is wired")
	errWebhookStatus      = errors.New("webhook responded with an error status")
)

func (b *InMemoryBackend) send(ctx context.Context, n notification, t target, payload []byte) error {
	switch t.typ {
	case typePubSub:
		if n.pub == nil {
			return errNoPublisher
		}

		return n.pub.Publish(t.labels[labelTopic], payload, map[string]string{"policy": n.incident.Policy})
	case typeWebhookToken, typeWebhookBasic:
		return b.postWebhook(ctx, t, payload)
	}

	return errUnsupportedChannel
}

func webhookURL(t target) (string, error) {
	u, err := url.Parse(t.labels[labelURL])
	if err != nil {
		return "", fmt.Errorf("invalid webhook url: %w", err)
	}

	if t.typ == typeWebhookToken && t.secret[labelAuthToken] != "" {
		q := u.Query()
		q.Set(labelAuthToken, t.secret[labelAuthToken])
		u.RawQuery = q.Encode()
	}

	return u.String(), nil
}

func (b *InMemoryBackend) postWebhook(ctx context.Context, t target, payload []byte) error {
	target, err := webhookURL(t)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build webhook request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	if t.typ == typeWebhookBasic {
		req.SetBasicAuth(t.labels["username"], t.secret[labelPassword])
	}

	resp, err := b.httpc.Do(req)
	if err != nil {
		return fmt.Errorf("post webhook: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("%w: %s", errWebhookStatus, resp.Status)
	}

	return nil
}

type payloadResource struct {
	Labels map[string]string `json:"labels"`
	Type   string            `json:"type"`
}

type payloadMetric struct {
	Labels      map[string]string `json:"labels"`
	Type        string            `json:"type"`
	DisplayName string            `json:"displayName"`
}

type payloadMetadata struct {
	SystemLabels map[string]string `json:"system_labels"`
	UserLabels   map[string]string `json:"user_labels"`
}

type payloadDocs struct {
	Content  string `json:"content,omitempty"`
	MimeType string `json:"mime_type,omitempty"`
	Subject  string `json:"subject,omitempty"`
}

type payloadCondition struct {
	Name        string `json:"name"`
	DisplayName string `json:"displayName"`
}

type payloadIncident struct {
	EndedAt                 *int64            `json:"ended_at"`
	Resource                payloadResource   `json:"resource"`
	Metric                  payloadMetric     `json:"metric"`
	Metadata                payloadMetadata   `json:"metadata"`
	PolicyUserLabels        map[string]string `json:"policy_user_labels"`
	Documentation           payloadDocs       `json:"documentation"`
	Condition               payloadCondition  `json:"condition"`
	IncidentID              string            `json:"incident_id"`
	ScopingProjectID        string            `json:"scoping_project_id"`
	ScopingProjectNumber    string            `json:"scoping_project_number"`
	URL                     string            `json:"url"`
	State                   string            `json:"state"`
	ResourceID              string            `json:"resource_id"`
	ResourceName            string            `json:"resource_name"`
	ResourceDisplayName     string            `json:"resource_display_name"`
	ResourceTypeDisplayName string            `json:"resource_type_display_name"`
	PolicyName              string            `json:"policy_name"`
	ConditionName           string            `json:"condition_name"`
	ThresholdValue          string            `json:"threshold_value"`
	ObservedValue           string            `json:"observed_value"`
	Severity                string            `json:"severity"`
	Summary                 string            `json:"summary"`
	StartedAt               int64             `json:"started_at"`
}

type payloadEnvelope struct {
	Version  string          `json:"version"`
	Incident payloadIncident `json:"incident"`
}

func incidentPayload(n notification) payloadEnvelope {
	inc := n.incident

	var ended *int64

	if !inc.EndedAt.IsZero() {
		v := inc.EndedAt.Unix()
		ended = &v
	}

	docs := payloadDocs{}
	if d := n.policy.GetDocumentation(); d != nil {
		docs = payloadDocs{Content: d.GetContent(), MimeType: d.GetMimeType(), Subject: d.GetSubject()}
	}

	return payloadEnvelope{Version: payloadVersion, Incident: payloadIncident{
		IncidentID:           inc.ID,
		ScopingProjectID:     inc.Project,
		ScopingProjectNumber: inc.Project,
		URL:                  incidentURL(inc),
		StartedAt:            inc.StartedAt.Unix(),
		EndedAt:              ended,
		State:                stateOf(n.opened),
		ResourceName: resourceName(
			inc,
		),
		ResourceDisplayName:     resourceName(inc),
		ResourceTypeDisplayName: inc.ResourceType,
		Resource:                payloadResource{Type: inc.ResourceType, Labels: inc.ResourceLabels},
		Metric: payloadMetric{
			Type:        inc.MetricType,
			DisplayName: inc.MetricType,
			Labels:      inc.MetricLabels,
		},
		Metadata:         payloadMetadata{SystemLabels: map[string]string{}, UserLabels: map[string]string{}},
		PolicyName:       inc.PolicyDisplayName,
		PolicyUserLabels: n.policy.GetUserLabels(),
		Documentation:    docs,
		Condition:        payloadCondition{Name: inc.Condition, DisplayName: inc.ConditionDisplayName},
		ConditionName:    inc.ConditionDisplayName,
		ThresholdValue:   fmt.Sprint(inc.Threshold),
		ObservedValue:    fmt.Sprint(inc.Observed),
		Severity:         strings.ToUpper(n.policy.GetSeverity().String()),
		Summary:          summary(inc, n.opened),
	}}
}

func stateOf(opened bool) string {
	if opened {
		return stateOpen
	}

	return stateClosed
}

func resourceName(inc Incident) string {
	for _, k := range []string{"instance_id", "service_name", "function_name", "topic_id", "host", labelProject} {
		if v := inc.ResourceLabels[k]; v != "" {
			return v
		}
	}

	return inc.ResourceType
}

func summary(inc Incident, opened bool) string {
	if opened {
		return fmt.Sprintf("%s for %s violates the condition %q (threshold %v, observed %v).",
			inc.MetricType, resourceName(inc), inc.ConditionDisplayName, inc.Threshold, inc.Observed)
	}

	return fmt.Sprintf("The condition %q on %s has recovered.", inc.ConditionDisplayName, resourceName(inc))
}

func incidentURL(inc Incident) string {
	return "https://console.cloud.google.com/monitoring/alerting/incidents/" + inc.ID + "?project=" + inc.Project
}
