package cloudwatch

import (
	"errors"
	"net/http"
	"time"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
)

// cborOptTime extracts a [time.Time] from a CBOR map by key, returning the
// zero value when absent. cborTime instead defaults to now, which is wrong
// for optional wire fields like ExpireDate/StartDate.
func cborOptTime(m cbor.Map, key string) time.Time {
	v, ok := m[key]
	if !ok {
		return time.Time{}
	}

	return cborValTime(v)
}

// cborAlarmMuteRuleSchedule extracts types.Rule.Schedule from the "Rule" key
// of a CBOR map (aws-sdk-go-v2 cloudwatch@v1.66.3 types/types.go:3368,3408).
func cborAlarmMuteRuleSchedule(input cbor.Map) AlarmMuteRuleSchedule {
	ruleMap, ok := input["Rule"].(cbor.Map)
	if !ok {
		return AlarmMuteRuleSchedule{}
	}

	schedMap, ok := ruleMap["Schedule"].(cbor.Map)
	if !ok {
		return AlarmMuteRuleSchedule{}
	}

	return AlarmMuteRuleSchedule{
		Expression: cborStr(schedMap, "Expression"),
		Duration:   cborStr(schedMap, "Duration"),
		Timezone:   cborStr(schedMap, "Timezone"),
	}
}

// cborMuteTargetAlarmNames extracts types.MuteTargets.AlarmNames from the
// "MuteTargets" key of a CBOR map (aws-sdk-go-v2 cloudwatch@v1.66.3
// types/types.go:3223).
func cborMuteTargetAlarmNames(input cbor.Map) []string {
	mt, ok := input["MuteTargets"].(cbor.Map)
	if !ok {
		return nil
	}

	return cborStrList(mt, "AlarmNames")
}

func (h *Handler) cborPutAlarmMuteRule(input cbor.Map, c *echo.Context) error {
	name := cborStr(input, "Name")
	if name == "" {
		return h.cborError(c, http.StatusBadRequest, "InvalidParameterValue", "Name is required")
	}

	rule := &AlarmMuteRule{
		Name:        name,
		Description: cborStr(input, "Description"),
		Schedule:    cborAlarmMuteRuleSchedule(input),
		AlarmNames:  cborMuteTargetAlarmNames(input),
		ExpireDate:  cborOptTime(input, "ExpireDate"),
		StartDate:   cborOptTime(input, "StartDate"),
	}

	if err := h.Backend.PutAlarmMuteRule(rule); err != nil {
		if errors.Is(err, ErrValidation) {
			return h.cborError(c, http.StatusBadRequest, "InvalidParameterValue", err.Error())
		}

		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	if stored, err := h.Backend.GetAlarmMuteRule(name); err == nil {
		h.applyCreationTags(input, stored.Arn)
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborGetAlarmMuteRule(input cbor.Map, c *echo.Context) error {
	name := cborStr(input, "AlarmMuteRuleName")
	if name == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"AlarmMuteRuleName is required",
		)
	}

	rule, err := h.Backend.GetAlarmMuteRule(name)
	if err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	return writeCBOR(c, buildAlarmMuteRuleCBOR(rule))
}

// buildAlarmMuteRuleRuleCBOR converts an AlarmMuteRule's schedule to the
// nested Rule{Schedule{...}} wire shape (aws-sdk-go-v2 cloudwatch@v1.66.3
// types/types.go:3368).
func buildAlarmMuteRuleRuleCBOR(rule *AlarmMuteRule) cbor.Map {
	sched := cbor.Map{
		"Expression": cbor.String(rule.Schedule.Expression),
		"Duration":   cbor.String(rule.Schedule.Duration),
	}
	if rule.Schedule.Timezone != "" {
		sched["Timezone"] = cbor.String(rule.Schedule.Timezone)
	}

	return cbor.Map{"Schedule": sched}
}

// buildAlarmMuteRuleCBOR converts an AlarmMuteRule to a GetAlarmMuteRuleOutput
// cbor.Map (aws-sdk-go-v2 cloudwatch@v1.66.3 api_op_GetAlarmMuteRule.go:74).
func buildAlarmMuteRuleCBOR(rule *AlarmMuteRule) cbor.Map {
	now := time.Now().UTC()
	out := cbor.Map{
		"AlarmMuteRuleArn":     cbor.String(rule.Arn),
		"Name":                 cbor.String(rule.Name),
		"MuteType":             cbor.String(rule.MuteType()),
		keyStatus:              cbor.String(rule.Status(now)),
		"Rule":                 buildAlarmMuteRuleRuleCBOR(rule),
		"LastUpdatedTimestamp": cborFromTime(rule.LastUpdatedTimestamp),
	}
	if rule.Description != "" {
		out["Description"] = cbor.String(rule.Description)
	}
	if len(rule.AlarmNames) > 0 {
		out["MuteTargets"] = cbor.Map{"AlarmNames": cborStringList(rule.AlarmNames)}
	}
	if !rule.ExpireDate.IsZero() {
		out["ExpireDate"] = cborFromTime(rule.ExpireDate)
	}
	if !rule.StartDate.IsZero() {
		out["StartDate"] = cborFromTime(rule.StartDate)
	}

	return out
}

func (h *Handler) cborDeleteAlarmMuteRule(input cbor.Map, c *echo.Context) error {
	name := cborStr(input, "AlarmMuteRuleName")
	if name == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"AlarmMuteRuleName is required",
		)
	}

	if err := h.Backend.DeleteAlarmMuteRule(name); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborListAlarmMuteRules(input cbor.Map, c *echo.Context) error {
	nextToken := cborStr(input, "NextToken")
	maxResults := int(cborInt32(input, "MaxRecords"))
	alarmName := cborStr(input, "AlarmName")
	statuses := cborStrList(input, "Statuses")

	p, err := h.Backend.ListAlarmMuteRules(nextToken, maxResults, alarmName, statuses)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	now := time.Now().UTC()
	summaries := make(cbor.List, 0, len(p.Data))

	for _, rule := range p.Data {
		entry := cbor.Map{
			"AlarmMuteRuleArn": cbor.String(rule.Arn),
			"MuteType":         cbor.String(rule.MuteType()),
			keyStatus:          cbor.String(rule.Status(now)),
		}
		if !rule.LastUpdatedTimestamp.IsZero() {
			entry["LastUpdatedTimestamp"] = cborFromTime(rule.LastUpdatedTimestamp)
		}
		if !rule.ExpireDate.IsZero() {
			entry["ExpireDate"] = cborFromTime(rule.ExpireDate)
		}

		summaries = append(summaries, entry)
	}

	out := cbor.Map{"AlarmMuteRuleSummaries": summaries}
	if p.Next != "" {
		out["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, out)
}
