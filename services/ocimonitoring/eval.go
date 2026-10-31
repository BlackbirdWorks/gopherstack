package ocimonitoring

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	noticeTypeFiring = "OK_TO_FIRING"
	noticeTypeOK     = "FIRING_TO_OK"
	noticeTypeRepeat = "REPEAT"
	noticeVersion    = 1.3
)

type notice struct {
	topic, title, body string
	asJSON             bool
}

type firing struct {
	dims  map[string]string
	value float64
}

// evaluateAll steps every enabled alarm and returns the notifications they raised; the caller holds the lock.
func (b *InMemoryBackend) evaluateAll(now time.Time) []notice {
	var out []notice

	ids := make([]string, 0, len(b.alarms))
	for id := range b.alarms {
		ids = append(ids, id)
	}

	slices.Sort(ids)

	for _, id := range ids {
		if a := b.alarms[id]; a.Enabled {
			out = append(out, b.evaluate(a, now)...)
		}
	}

	return out
}

// breaching returns the metric streams whose aggregate over the last interval satisfies the alarm condition.
func (b *InMemoryBackend) breaching(a *Alarm, q *MQL, now time.Time) []firing {
	groups := b.collectGroups(q, a.Namespace, a.MetricCompartmentID, a.ResourceGroup)
	if q.Stat == statAbsent && len(groups) == 0 {
		return []firing{{dims: map[string]string{}, value: 1}}
	}

	var out []firing

	for _, g := range groups {
		pts := pointsIn(g.points, now.Add(-q.Interval), now.Add(time.Nanosecond))

		v, ok := windowStat(q, pts)
		if !ok {
			continue
		}

		if (q.Pred != nil && q.Pred.holds(v)) || (q.Pred == nil && q.Stat == statAbsent && v == 1) {
			out = append(out, firing{dims: g.dims, value: v})
		}
	}

	return out
}

// pendingNeeded is how long the condition must already have held before the alarm fires: the first
// breaching evaluation counts toward pendingDuration, so PT1M fires at once.
func pendingNeeded(a *Alarm) time.Duration {
	d, _ := parseISODuration(a.PendingDuration)

	return max(d-minDuration, 0)
}

func (b *InMemoryBackend) evaluate(a *Alarm, now time.Time) []notice {
	q, err := ParseMQL(a.Query)
	if err != nil {
		return nil
	}

	hits := b.breaching(a, q, now)

	switch {
	case a.Status == statusOK && len(hits) > 0:
		if a.PendingSince == nil {
			a.PendingSince = &now
		}

		if now.Sub(*a.PendingSince) < pendingNeeded(a) {
			return nil
		}

		return b.transition(a, statusFiring, noticeTypeFiring, now, hits)
	case a.Status == statusOK:
		a.PendingSince = nil

		return nil
	case len(hits) == 0:
		a.PendingSince = nil

		return b.transition(a, statusOK, noticeTypeOK, now, nil)
	}

	if repeat, ok := parseISODuration(
		a.RepeatNotification,
	); ok && a.LastNotified != nil &&
		now.Sub(*a.LastNotified) >= repeat {
		return b.notices(a, noticeTypeRepeat, now, hits)
	}

	return nil
}

func (b *InMemoryBackend) transition(a *Alarm, to, kind string, now time.Time, hits []firing) []notice {
	triggered := now
	if to == statusFiring && a.PendingSince != nil {
		triggered = *a.PendingSince
	}

	a.History = append(a.History, HistoryEntry{
		Timestamp: now, TimestampTriggered: triggered, From: a.Status, To: to, Summary: "The alarm state is " + to,
	})

	if len(a.History) > maxHistory {
		a.History = a.History[len(a.History)-maxHistory:]
	}

	a.Status, a.StatusSince, a.PendingSince = to, now, nil

	return b.notices(a, kind, now, hits)
}

func (b *InMemoryBackend) notices(a *Alarm, kind string, now time.Time, hits []firing) []notice {
	if a.suppressedAt(now) {
		return nil
	}

	a.LastNotified = &now

	title, body, asJSON := renderNotice(a, kind, now, hits)

	var out []notice

	for _, d := range a.Destinations {
		if strings.HasPrefix(d, destTopicPrefix) {
			out = append(out, notice{topic: d, title: title, body: body, asJSON: asJSON})
		}
	}

	return out
}

type metaWire struct {
	ID                 string               `json:"id"`
	Status             string               `json:"status"`
	Severity           string               `json:"severity"`
	Namespace          string               `json:"namespace"`
	Query              string               `json:"query"`
	AlarmSummary       string               `json:"alarmSummary"`
	PendingDuration    string               `json:"pendingDuration"`
	Dimensions         []map[string]string  `json:"dimensions"`
	MetricValues       []map[string]float64 `json:"metricValues"`
	TotalMetricsFiring int                  `json:"totalMetricsFiring"`
}

type noticeWire struct {
	DedupeKey            string     `json:"dedupeKey"`
	Title                string     `json:"title"`
	Body                 string     `json:"body"`
	Type                 string     `json:"type"`
	Severity             string     `json:"severity"`
	Timestamp            string     `json:"timestamp"`
	NotificationType     string     `json:"notificationType"`
	AlarmMetaData        []metaWire `json:"alarmMetaData"`
	TimestampEpochMillis int64      `json:"timestampEpochMillis"`
	Version              float64    `json:"version"`
}

func renderNotice(a *Alarm, kind string, now time.Time, hits []firing) (string, string, bool) {
	title := a.Name
	if v := extra(a, "notificationTitle"); v != nil {
		title = *v
	}

	summary := a.Name
	if v := extra(a, "alarmSummary"); v != nil {
		summary = *v
	}

	status := statusFiring
	if kind == noticeTypeOK {
		status = statusOK
	}

	meta := metaWire{
		ID: a.ID, Status: status, Severity: a.Severity, Namespace: a.Namespace, Query: a.Query,
		TotalMetricsFiring: len(hits), Dimensions: []map[string]string{}, MetricValues: []map[string]float64{},
		AlarmSummary: summary, PendingDuration: a.PendingDuration,
	}

	if q, err := ParseMQL(a.Query); err == nil {
		for _, h := range hits {
			meta.Dimensions = append(meta.Dimensions, h.dims)
			meta.MetricValues = append(meta.MetricValues, map[string]float64{q.Metric: h.value})
		}
	}

	if a.MessageFormat == formatONS {
		return title, fmt.Sprintf("Alarm: %s\nStatus: %s\nSeverity: %s\nNamespace: %s\nQuery: %s\nSummary: %s\n%s",
			a.Name, status, a.Severity, a.Namespace, a.Query, summary, a.Body), false
	}

	w := noticeWire{
		DedupeKey: a.ID, Title: title, Body: a.Body, Type: kind, Severity: a.Severity,
		Timestamp: now.Format(time.RFC3339Nano), TimestampEpochMillis: now.UnixMilli(),
		NotificationType: "Grouped metrics", AlarmMetaData: []metaWire{meta}, Version: noticeVersion,
	}

	var (
		out []byte
		err error
	)

	if a.MessageFormat == formatPretty {
		out, err = json.MarshalIndent(w, "", "  ")
	} else {
		out, err = json.Marshal(w)
	}

	if err != nil {
		return title, "", false
	}

	return title, string(out), true
}

// deliver publishes the notices outside the backend lock.
func deliver(ctx context.Context, pub Publisher, ns []notice) {
	if pub == nil {
		return
	}

	for _, n := range ns {
		if err := pub(ctx, n.topic, n.title, n.body, n.asJSON); err != nil {
			logger.Load(ctx).WarnContext(ctx, "ocimonitoring: alarm notification not delivered",
				"topic", n.topic, "error", err)
		}
	}
}
