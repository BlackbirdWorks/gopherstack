package gcpmonitoring

import (
	"strconv"
	"slices"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
)

const (
	minRenotify = 30 * time.Minute
	maxRenotify = 24 * time.Hour
)

// isLogIncident reports whether the incident belongs to a log-match condition, which has its own auto-close.
func isLogIncident(p *monitoringpb.AlertPolicy, inc *Incident) bool {
	for _, c := range p.GetConditions() {
		if c.GetName() == inc.Condition {
			return c.GetConditionMatchedLog() != nil
		}
	}

	return false
}

// autoCloseDue reports whether an incident saw no data for the policy's auto_close period (default 7 days).
func autoCloseDue(p *monitoringpb.AlertPolicy, inc *Incident, now time.Time) bool {
	after := defaultAutoClose
	if d := p.GetAlertStrategy().GetAutoClose().AsDuration(); d > 0 {
		after = d
	}

	last := inc.LastSeen
	if last.IsZero() {
		last = inc.StartedAt
	}

	return now.Sub(last) >= after
}

func (b *InMemoryBackend) markNotifiedLocked(id string, targets []target, now time.Time) {
	inc, ok := b.incidents[id]
	if !ok {
		return
	}

	if inc.Notified == nil {
		inc.Notified = map[string]time.Time{}
	}

	for _, t := range targets {
		inc.Notified[t.name] = now
	}
}

// renotifyLocked builds reminder notifications for channels whose renotify_interval elapsed since their last
// notification of the open incident.
func (b *InMemoryBackend) renotifyLocked(p *monitoringpb.AlertPolicy, inc *Incident, now time.Time) []notification {
	opens, _ := policyPrompts(p)
	if !opens || len(p.GetAlertStrategy().GetNotificationChannelStrategy()) == 0 {
		return nil
	}

	if b.snoozedLocked(p.GetName(), observation{
		metricLabels: inc.MetricLabels, resourceLabels: inc.ResourceLabels,
	}, now) {
		return nil
	}

	var due []string

	for _, s := range p.GetAlertStrategy().GetNotificationChannelStrategy() {
		every := s.GetRenotifyInterval().AsDuration()
		if every <= 0 {
			continue
		}

		for _, name := range s.GetNotificationChannelNames() {
			last, ok := inc.Notified[name]
			if !ok {
				last = inc.StartedAt
			}

			if now.Sub(last) >= every && !slices.Contains(due, name) {
				due = append(due, name)
			}
		}
	}

	if len(due) == 0 {
		return nil
	}

	n := b.notificationLocked(p, cloneIncident(inc), true)
	n.reminder = true
	n.targets = slices.DeleteFunc(n.targets, func(t target) bool { return !slices.Contains(due, t.name) })

	if len(n.targets) == 0 {
		return nil
	}

	b.markNotifiedLocked(inc.ID, n.targets, now)

	return []notification{n}
}

func validateChannelStrategies(p *monitoringpb.AlertPolicy) error {
	for i, s := range p.GetAlertStrategy().GetNotificationChannelStrategy() {
		path := "alert_strategy.notification_channel_strategy[" + strconv.Itoa(i) + "]"

		if len(s.GetNotificationChannelNames()) == 0 {
			return errInvalid("Field %s.notification_channel_names had an invalid value: at least one channel "+
				"is required.", path)
		}

		for _, name := range s.GetNotificationChannelNames() {
			if !slices.Contains(p.GetNotificationChannels(), name) {
				return errInvalid("Field %s.notification_channel_names had an invalid value of %q: the channel "+
					"is not in notification_channels.", path, name)
			}
		}

		if d := s.GetRenotifyInterval(); d != nil {
			if v := d.AsDuration(); v < minRenotify || v > maxRenotify {
				return errInvalid("Field %s.renotify_interval had an invalid value of %q: the interval must be "+
					"between 30 minutes and 24 hours.", path, v)
			}
		}
	}

	return nil
}
