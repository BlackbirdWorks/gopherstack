package cloudwatch

import (
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// alarmMuteRuleScheduleXML is types.Rule.Schedule flattened onto the query
// protocol as Rule.Schedule.{Expression,Duration,Timezone} (botocore
// cloudwatch 2010-08-01 service-2.json shapes Rule/Schedule: default
// structure-member flattening, no locationName overrides).
type alarmMuteRuleScheduleXML struct {
	Expression string `xml:"Expression"`
	Duration   string `xml:"Duration"`
	Timezone   string `xml:"Timezone,omitempty"`
}

type alarmMuteRuleRuleXML struct {
	Schedule alarmMuteRuleScheduleXML `xml:"Schedule"`
}

// alarmMuteRuleTargetsXML is types.MuteTargets; AlarmNames is a
// non-flattened list, so the wire form is
// MuteTargets.AlarmNames.member.N (shape MuteTargetAlarmNameList has no
// "flattened" trait).
type alarmMuteRuleTargetsXML struct {
	AlarmNames []string `xml:"AlarmNames>member,omitempty"`
}

func (h *Handler) putAlarmMuteRuleFromForm(form url.Values, c *echo.Context) error {
	name := form.Get("Name")
	if name == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "Name is required")
	}

	rule := &AlarmMuteRule{
		Name:        name,
		Description: form.Get("Description"),
		Schedule: AlarmMuteRuleSchedule{
			Expression: form.Get("Rule.Schedule.Expression"),
			Duration:   form.Get("Rule.Schedule.Duration"),
			Timezone:   form.Get("Rule.Schedule.Timezone"),
		},
		AlarmNames: parseMemberList(form, "MuteTargets.AlarmNames."),
	}

	if raw := form.Get("StartDate"); raw != "" {
		start, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "StartDate must be RFC3339")
		}
		rule.StartDate = start.UTC()
	}

	if raw := form.Get("ExpireDate"); raw != "" {
		expire, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "ExpireDate must be RFC3339")
		}
		rule.ExpireDate = expire.UTC()
	}

	if err := h.Backend.PutAlarmMuteRule(rule); err != nil {
		if errors.Is(err, ErrValidation) {
			return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", err.Error())
		}

		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return nil
}

func (h *Handler) handlePutAlarmMuteRule(form url.Values, c *echo.Context) error {
	if err := h.putAlarmMuteRuleFromForm(form, c); err != nil {
		return err
	}

	type response struct {
		XMLName   xml.Name `xml:"PutAlarmMuteRuleResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDeleteAlarmMuteRule(form url.Values, c *echo.Context) error {
	name := form.Get("AlarmMuteRuleName")
	if name == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"AlarmMuteRuleName is required",
		)
	}

	if err := h.Backend.DeleteAlarmMuteRule(name); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"DeleteAlarmMuteRuleResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

// alarmMuteRuleResultXML is GetAlarmMuteRuleOutput's XML shape: flat fields,
// no MuteRule wrapper (botocore service-2.json GetAlarmMuteRuleOutput has no
// httpPayload member, so members sit directly under the *Result element).
type alarmMuteRuleResultXML struct {
	Name                 string                   `xml:"Name"`
	AlarmMuteRuleArn     string                   `xml:"AlarmMuteRuleArn"`
	Description          string                   `xml:"Description,omitempty"`
	Rule                 alarmMuteRuleRuleXML     `xml:"Rule"`
	MuteTargets          *alarmMuteRuleTargetsXML `xml:"MuteTargets,omitempty"`
	StartDate            string                   `xml:"StartDate,omitempty"`
	ExpireDate           string                   `xml:"ExpireDate,omitempty"`
	Status               string                   `xml:"Status"`
	MuteType             string                   `xml:"MuteType"`
	LastUpdatedTimestamp string                   `xml:"LastUpdatedTimestamp"`
}

func buildAlarmMuteRuleResultXML(rule *AlarmMuteRule) alarmMuteRuleResultXML {
	now := time.Now().UTC()
	result := alarmMuteRuleResultXML{
		Name:             rule.Name,
		AlarmMuteRuleArn: rule.Arn,
		Description:      rule.Description,
		Rule: alarmMuteRuleRuleXML{Schedule: alarmMuteRuleScheduleXML{
			Expression: rule.Schedule.Expression,
			Duration:   rule.Schedule.Duration,
			Timezone:   rule.Schedule.Timezone,
		}},
		Status:               rule.Status(now),
		MuteType:             rule.MuteType(),
		LastUpdatedTimestamp: rule.LastUpdatedTimestamp.UTC().Format(time.RFC3339),
	}
	if len(rule.AlarmNames) > 0 {
		result.MuteTargets = &alarmMuteRuleTargetsXML{AlarmNames: rule.AlarmNames}
	}
	if !rule.StartDate.IsZero() {
		result.StartDate = rule.StartDate.UTC().Format(time.RFC3339)
	}
	if !rule.ExpireDate.IsZero() {
		result.ExpireDate = rule.ExpireDate.UTC().Format(time.RFC3339)
	}

	return result
}

func (h *Handler) handleGetAlarmMuteRule(form url.Values, c *echo.Context) error {
	name := form.Get("AlarmMuteRuleName")
	if name == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"AlarmMuteRuleName is required",
		)
	}

	rule, err := h.Backend.GetAlarmMuteRule(name)
	if err != nil {
		return h.xmlError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	type response struct {
		XMLName   xml.Name               `xml:"GetAlarmMuteRuleResponse"`
		Xmlns     string                 `xml:"xmlns,attr"`
		RequestID string                 `xml:"ResponseMetadata>RequestId"`
		Result    alarmMuteRuleResultXML `xml:"GetAlarmMuteRuleResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    buildAlarmMuteRuleResultXML(rule),
	})
}

// alarmMuteRuleSummaryXML is types.AlarmMuteRuleSummary's XML shape -- no
// Name member (service-2.json AlarmMuteRuleSummary carries only
// AlarmMuteRuleArn, ExpireDate, Status, MuteType, LastUpdatedTimestamp).
type alarmMuteRuleSummaryXML struct {
	AlarmMuteRuleArn     string `xml:"AlarmMuteRuleArn"`
	ExpireDate           string `xml:"ExpireDate,omitempty"`
	Status               string `xml:"Status"`
	MuteType             string `xml:"MuteType"`
	LastUpdatedTimestamp string `xml:"LastUpdatedTimestamp,omitempty"`
}

func (h *Handler) handleListAlarmMuteRules(form url.Values, c *echo.Context) error {
	nextToken := form.Get("NextToken")
	maxResults, _ := strconv.Atoi(form.Get("MaxRecords"))
	alarmName := form.Get("AlarmName")
	statuses := parseMemberList(form, "Statuses.")

	p, err := h.Backend.ListAlarmMuteRules(nextToken, maxResults, alarmName, statuses)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type listResult struct {
		NextToken              string                    `xml:"NextToken,omitempty"`
		AlarmMuteRuleSummaries []alarmMuteRuleSummaryXML `xml:"AlarmMuteRuleSummaries>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"ListAlarmMuteRulesResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    listResult `xml:"ListAlarmMuteRulesResult"`
	}

	now := time.Now().UTC()
	summaries := make([]alarmMuteRuleSummaryXML, 0, len(p.Data))

	for _, rule := range p.Data {
		summary := alarmMuteRuleSummaryXML{
			AlarmMuteRuleArn: rule.Arn,
			Status:           rule.Status(now),
			MuteType:         rule.MuteType(),
		}
		if !rule.ExpireDate.IsZero() {
			summary.ExpireDate = rule.ExpireDate.UTC().Format(time.RFC3339)
		}
		if !rule.LastUpdatedTimestamp.IsZero() {
			summary.LastUpdatedTimestamp = rule.LastUpdatedTimestamp.UTC().Format(time.RFC3339)
		}

		summaries = append(summaries, summary)
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    listResult{AlarmMuteRuleSummaries: summaries, NextToken: p.Next},
	})
}
