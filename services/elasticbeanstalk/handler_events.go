package elasticbeanstalk

import (
	"context"
	"encoding/xml"
	"net/url"
	"time"
)

// --- Events ---

type eventDescType struct {
	ApplicationName string `xml:"ApplicationName,omitempty"`
	EnvironmentName string `xml:"EnvironmentName,omitempty"`
	EventDate       string `xml:"EventDate,omitempty"`
	Message         string `xml:"Message,omitempty"`
	Severity        string `xml:"Severity,omitempty"`
}

type describeEventsResult struct {
	Events []eventDescType `xml:"Events>member"`
}

type describeEventsResponse struct {
	XMLName              xml.Name             `xml:"DescribeEventsResponse"`
	ResponseMetadata     responseMetadata     `xml:"ResponseMetadata"`
	Xmlns                string               `xml:"xmlns,attr"`
	DescribeEventsResult describeEventsResult `xml:"DescribeEventsResult"`
}

// handleDescribeEvents returns stored events, filtered by ApplicationName, EnvironmentName,
// EnvironmentId, Severity, and StartTime. The Terraform provider calls DescribeEvents with
// Severity=ERROR and StartTime to poll for errors after environment creation/update.
func (h *Handler) handleDescribeEvents(ctx context.Context, vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	envName := vals.Get("EnvironmentName")

	// EnvironmentId filter: resolve to app/env name for backend lookup.
	if envID := vals.Get("EnvironmentId"); envID != "" {
		envs := h.Backend.DescribeEnvironments(ctx, "", nil, []string{envID})
		if len(envs) > 0 {
			appName = envs[0].ApplicationName
			envName = envs[0].EnvironmentName
		}
	}

	// Severity filter: only return events matching the requested severity.
	severityFilter := vals.Get("Severity")

	// StartTime filter: only return events with EventDate >= StartTime.
	var startTime time.Time
	if s := vals.Get("StartTime"); s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			startTime = t
		}
	}

	records := h.Backend.DescribeEvents(ctx, appName, envName)
	members := make([]eventDescType, 0, len(records))

	for _, r := range records {
		if severityFilter != "" && r.Severity != severityFilter {
			continue
		}

		if !startTime.IsZero() {
			if t, err := time.Parse(time.RFC3339, r.EventDate); err == nil && t.Before(startTime) {
				continue
			}
		}

		members = append(members, eventDescType{
			ApplicationName: r.ApplicationName,
			EnvironmentName: r.EnvironmentName,
			EventDate:       r.EventDate,
			Message:         r.Message,
			Severity:        r.Severity,
		})
	}

	return &describeEventsResponse{
		Xmlns:                ebXMLNS,
		DescribeEventsResult: describeEventsResult{Events: members},
		ResponseMetadata:     responseMetadata{RequestID: "eb-describe-events"},
	}, nil
}
