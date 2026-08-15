package elasticbeanstalk

import (
	"context"
	"encoding/xml"
	"net/url"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- Events ---

// eventDescType mirrors types.EventDescription. RequestId is not modeled:
// this handler has no per-call unique request-ID generation anywhere (every
// op's ResponseMetadata.RequestID is a fixed literal like "eb-create-app",
// not a value that varies per call), so there is nothing distinguishing to
// source per-event.
type eventDescType struct {
	ApplicationName string `xml:"ApplicationName,omitempty"`
	EnvironmentName string `xml:"EnvironmentName,omitempty"`
	PlatformArn     string `xml:"PlatformArn,omitempty"`
	TemplateName    string `xml:"TemplateName,omitempty"`
	VersionLabel    string `xml:"VersionLabel,omitempty"`
	EventDate       string `xml:"EventDate,omitempty"`
	Message         string `xml:"Message,omitempty"`
	Severity        string `xml:"Severity,omitempty"`
}

type describeEventsResult struct {
	NextToken string          `xml:"NextToken,omitempty"`
	Events    []eventDescType `xml:"Events>member"`
}

type describeEventsResponse struct {
	XMLName              xml.Name             `xml:"DescribeEventsResponse"`
	ResponseMetadata     responseMetadata     `xml:"ResponseMetadata"`
	Xmlns                string               `xml:"xmlns,attr"`
	DescribeEventsResult describeEventsResult `xml:"DescribeEventsResult"`
}

// resolveEventsEnv resolves DescribeEvents's ApplicationName/EnvironmentName
// scope, following the EnvironmentId filter (if present) to the owning
// application/environment for backend lookup.
func (h *Handler) resolveEventsEnv(ctx context.Context, vals url.Values) (string, string) {
	appName := vals.Get("ApplicationName")
	envName := vals.Get("EnvironmentName")

	if envID := vals.Get("EnvironmentId"); envID != "" {
		if envs := h.Backend.DescribeEnvironments(ctx, "", nil, []string{envID}); len(envs) > 0 {
			appName = envs[0].ApplicationName
			envName = envs[0].EnvironmentName
		}
	}

	return appName, envName
}

// eventFilter holds DescribeEvents's per-record filters (everything except
// ApplicationName/EnvironmentName/EnvironmentId, which scope the backend
// query itself -- see resolveEventsEnv/handleDescribeEvents).
type eventFilter struct {
	startTime    time.Time
	endTime      time.Time
	severity     string
	platformArn  string
	templateName string
	versionLabel string
}

// parseEventFilters reads DescribeEvents's per-record filters from the
// request. PlatformArn/TemplateName/VersionLabel match the value captured on
// the event at append time (see appendEvent).
func parseEventFilters(vals url.Values) eventFilter {
	f := eventFilter{
		severity:     vals.Get("Severity"),
		platformArn:  vals.Get("PlatformArn"),
		templateName: vals.Get("TemplateName"),
		versionLabel: vals.Get("VersionLabel"),
	}

	if s := vals.Get("StartTime"); s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			f.startTime = t
		}
	}

	if s := vals.Get("EndTime"); s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			f.endTime = t
		}
	}

	return f
}

// matches reports whether r satisfies every filter in f. StartTime/EndTime
// bound EventDate (real DescribeEventsInput: "restricts the returned
// descriptions to those that occur up to, but not including, the EndTime");
// an unparseable EventDate is treated as satisfying the time bounds (there
// is nothing to compare).
func (f eventFilter) matches(r *EventRecord) bool {
	if f.severity != "" && r.Severity != f.severity {
		return false
	}

	if f.platformArn != "" && r.PlatformArn != f.platformArn {
		return false
	}

	if f.templateName != "" && r.TemplateName != f.templateName {
		return false
	}

	if f.versionLabel != "" && r.VersionLabel != f.versionLabel {
		return false
	}

	t, err := time.Parse(time.RFC3339, r.EventDate)
	if err != nil {
		return true
	}

	if !f.startTime.IsZero() && t.Before(f.startTime) {
		return false
	}

	return f.endTime.IsZero() || t.Before(f.endTime)
}

func toEventDesc(r *EventRecord) eventDescType {
	return eventDescType{
		ApplicationName: r.ApplicationName,
		EnvironmentName: r.EnvironmentName,
		PlatformArn:     r.PlatformArn,
		TemplateName:    r.TemplateName,
		VersionLabel:    r.VersionLabel,
		EventDate:       r.EventDate,
		Message:         r.Message,
		Severity:        r.Severity,
	}
}

// handleDescribeEvents returns stored events, filtered by ApplicationName, EnvironmentName,
// EnvironmentId, Severity, and StartTime. The Terraform provider calls DescribeEvents with
// Severity=ERROR and StartTime to poll for errors after environment creation/update.
func (h *Handler) handleDescribeEvents(ctx context.Context, vals url.Values) (any, error) {
	appName, envName := h.resolveEventsEnv(ctx, vals)
	filter := parseEventFilters(vals)

	records := h.Backend.DescribeEvents(ctx, appName, envName)
	members := make([]eventDescType, 0, len(records))

	for _, r := range records {
		if filter.matches(r) {
			members = append(members, toEventDesc(r))
		}
	}

	pg := page.New(members, vals.Get("NextToken"), parseMaxRecords(vals, "MaxRecords"), defaultListLimit)

	return &describeEventsResponse{
		Xmlns: ebXMLNS,
		DescribeEventsResult: describeEventsResult{
			Events:    pg.Data,
			NextToken: pg.Next,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-events"},
	}, nil
}
