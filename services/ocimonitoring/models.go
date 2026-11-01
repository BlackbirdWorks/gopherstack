package ocimonitoring

import (
	"encoding/json"
	"time"
)

// Alarm statuses.
const (
	statusOK        = "OK"
	statusFiring    = "FIRING"
	statusSuspended = "SUSPENDED"
	stateActive     = "ACTIVE"
)

// Tags are the shared freeform and defined tag sets of a resource.
type Tags struct {
	FreeformTags map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags  map[string]map[string]any `json:"definedTags,omitempty"`
}

func (t *Tags) merge(in Tags) {
	if in.FreeformTags != nil {
		t.FreeformTags = in.FreeformTags
	}

	if in.DefinedTags != nil {
		t.DefinedTags = in.DefinedTags
	}
}

// Point is one stored datapoint.
type Point struct {
	T time.Time `json:"t"`
	V float64   `json:"v"`
	N int       `json:"n"`
}

// Stream is a unique metric stream: namespace, compartment, name, resource group and dimensions.
type Stream struct {
	Dimensions    map[string]string `json:"dimensions"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	Namespace     string            `json:"namespace"`
	CompartmentID string            `json:"compartmentId"`
	Name          string            `json:"name"`
	ResourceGroup string            `json:"resourceGroup,omitempty"`
	Points        []Point           `json:"points"`
}

// SuppressionSpec is the alarm suppression window.
type SuppressionSpec struct {
	From        time.Time `json:"timeSuppressFrom"`
	Until       time.Time `json:"timeSuppressUntil"`
	Description string    `json:"description,omitempty"`
}

// HistoryEntry records one alarm state change.
type HistoryEntry struct {
	Timestamp          time.Time `json:"timestamp"`
	TimestampTriggered time.Time `json:"timestampTriggered"`
	From               string    `json:"from,omitempty"`
	To                 string    `json:"to"`
	Summary            string    `json:"summary"`
}

// Alarm is a stored alarm and its evaluation state.
type Alarm struct {
	TimeCreated         time.Time         `json:"timeCreated"`
	TimeUpdated         time.Time         `json:"timeUpdated"`
	StatusSince         time.Time         `json:"statusSince"`
	PendingSince        *time.Time        `json:"pendingSince,omitempty"`
	LastNotified        *time.Time        `json:"lastNotified,omitempty"`
	Suppression         *SuppressionSpec  `json:"suppression,omitempty"`
	InSubtree           *bool             `json:"metricCompartmentIdInSubtree,omitempty"`
	PerDimension        *bool             `json:"isNotificationsPerMetricDimensionEnabled,omitempty"`
	Tags                Tags              `json:"tags"`
	Extra               map[string]string `json:"extra,omitempty"`
	Overrides           []json.RawMessage `json:"overrides,omitempty"`
	ID                  string            `json:"id"`
	Name                string            `json:"displayName"`
	CompartmentID       string            `json:"compartmentId"`
	MetricCompartmentID string            `json:"metricCompartmentId"`
	Namespace           string            `json:"namespace"`
	ResourceGroup       string            `json:"resourceGroup,omitempty"`
	Query               string            `json:"query"`
	Severity            string            `json:"severity"`
	Body                string            `json:"body,omitempty"`
	Resolution          string            `json:"resolution"`
	PendingDuration     string            `json:"pendingDuration"`
	RepeatNotification  string            `json:"repeatNotificationDuration,omitempty"`
	MessageFormat       string            `json:"messageFormat"`
	Status              string            `json:"status"`
	Etag                string            `json:"etag"`
	Destinations        []string          `json:"destinations"`
	History             []HistoryEntry    `json:"history,omitempty"`
	Seq                 int64             `json:"seq"`
	Enabled             bool              `json:"isEnabled"`
}
