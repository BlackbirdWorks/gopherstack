package cloudwatch

import (
	"errors"
	"net/http"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
)

// cborInt64 extracts an int64 from a CBOR map by key, without truncating to int32.
func cborInt64(m cbor.Map, key string) int64 {
	v, ok := m[key]
	if !ok {
		return 0
	}

	return cborValInt64(v)
}

// cborScheduledQueryConfiguration extracts a ScheduledQueryConfiguration from
// the "ScheduledQueryConfiguration" key of a CBOR map.
func cborScheduledQueryConfiguration(input cbor.Map) ScheduledQueryConfiguration {
	m, ok := input["ScheduledQueryConfiguration"].(cbor.Map)
	if !ok {
		return ScheduledQueryConfiguration{}
	}

	cfg := ScheduledQueryConfiguration{
		AggregationExpression: cborStr(m, "AggregationExpression"),
		QueryString:           cborStr(m, "QueryString"),
		ScheduledQueryRoleARN: cborStr(m, "ScheduledQueryRoleARN"),
		LogGroupIdentifiers:   cborStrList(m, "LogGroupIdentifiers"),
		QueryARN:              cborStr(m, "QueryARN"),
	}

	if sc, isMap := m["ScheduleConfiguration"].(cbor.Map); isMap {
		cfg.ScheduleConfiguration = ScheduleConfiguration{
			ScheduleExpression: cborStr(sc, "ScheduleExpression"),
			StartTimeOffset:    cborInt64(sc, "StartTimeOffset"),
			EndTimeOffset:      cborInt64(sc, "EndTimeOffset"),
		}
	}

	return cfg
}

func (h *Handler) cborPutLogAlarm(input cbor.Map, c *echo.Context) error {
	alarmName := cborStr(input, "AlarmName")
	if alarmName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"AlarmName is required",
		)
	}

	actionsEnabled := true
	if v, ok := input["ActionsEnabled"]; ok {
		if b, isBool := v.(cbor.Bool); isBool {
			actionsEnabled = bool(b)
		}
	}

	alarm := &LogAlarm{
		AlarmName:                   alarmName,
		ComparisonOperator:          cborStr(input, "ComparisonOperator"),
		TreatMissingData:            cborStr(input, "TreatMissingData"),
		AlarmDescription:            cborStr(input, keyAlarmDescription),
		ActionLogLineRoleArn:        cborStr(input, "ActionLogLineRoleArn"),
		Threshold:                   cborFloat(input, "Threshold"),
		QueryResultsToAlarm:         cborInt32(input, "QueryResultsToAlarm"),
		QueryResultsToEvaluate:      cborInt32(input, "QueryResultsToEvaluate"),
		ActionLogLineCount:          cborInt32(input, "ActionLogLineCount"),
		ActionsEnabled:              actionsEnabled,
		AlarmActions:                cborStrList(input, "AlarmActions"),
		OKActions:                   cborStrList(input, "OKActions"),
		InsufficientDataActions:     cborStrList(input, "InsufficientDataActions"),
		ScheduledQueryConfiguration: cborScheduledQueryConfiguration(input),
	}

	if err := h.Backend.PutLogAlarm(alarm); err != nil {
		if errors.Is(err, ErrValidation) {
			return h.cborError(c, http.StatusBadRequest, "InvalidParameterValue", err.Error())
		}

		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

// buildLogAlarmCBOR converts a LogAlarm to a cbor.Map.
func buildLogAlarmCBOR(a *LogAlarm) cbor.Map {
	m := cbor.Map{
		keyAlarmName:             cbor.String(a.AlarmName),
		keyAlarmArn:              cbor.String(a.AlarmArn),
		keyAlarmType:             cbor.String(alarmTypeLogAlarm),
		"ComparisonOperator":     cbor.String(a.ComparisonOperator),
		keyStateValue:            cbor.String(a.StateValue),
		keyStateReason:           cbor.String(a.StateReason),
		keyAlarmDescription:      cbor.String(a.AlarmDescription),
		"Threshold":              cbor.Float64(a.Threshold),
		"QueryResultsToAlarm":    cbor.Uint(uint64(a.QueryResultsToAlarm)),    //nolint:gosec // positive
		"QueryResultsToEvaluate": cbor.Uint(uint64(a.QueryResultsToEvaluate)), //nolint:gosec // positive
		keyActionsEnabled:        cbor.Bool(a.ActionsEnabled),
		"ScheduledQueryConfiguration": buildScheduledQueryConfigurationCBOR(
			a.ScheduledQueryConfiguration,
		),
	}
	if !a.StateTransitionedTimestamp.IsZero() {
		m["StateTransitionedTimestamp"] = cborFromTime(a.StateTransitionedTimestamp)
	}
	if !a.StateUpdatedTimestamp.IsZero() {
		m["StateUpdatedTimestamp"] = cborFromTime(a.StateUpdatedTimestamp)
	}
	if !a.AlarmConfigurationUpdatedTimestamp.IsZero() {
		m["AlarmConfigurationUpdatedTimestamp"] = cborFromTime(a.AlarmConfigurationUpdatedTimestamp)
	}
	if a.StateReasonData != "" {
		m["StateReasonData"] = cbor.String(a.StateReasonData)
	}
	if a.EvaluationState != "" {
		m["EvaluationState"] = cbor.String(a.EvaluationState)
	}
	if a.TreatMissingData != "" {
		m["TreatMissingData"] = cbor.String(a.TreatMissingData)
	}
	if a.ActionLogLineRoleArn != "" {
		m["ActionLogLineRoleArn"] = cbor.String(a.ActionLogLineRoleArn)
	}
	if a.ActionLogLineCount > 0 {
		m["ActionLogLineCount"] = cbor.Uint(uint64(a.ActionLogLineCount))
	}
	if len(a.AlarmActions) > 0 {
		m["AlarmActions"] = cborStringList(a.AlarmActions)
	}
	if len(a.OKActions) > 0 {
		m["OKActions"] = cborStringList(a.OKActions)
	}
	if len(a.InsufficientDataActions) > 0 {
		m["InsufficientDataActions"] = cborStringList(a.InsufficientDataActions)
	}

	return m
}

// buildScheduledQueryConfigurationCBOR converts a ScheduledQueryConfiguration
// to a cbor.Map.
func buildScheduledQueryConfigurationCBOR(cfg ScheduledQueryConfiguration) cbor.Map {
	m := cbor.Map{
		"AggregationExpression": cbor.String(cfg.AggregationExpression),
		"QueryString":           cbor.String(cfg.QueryString),
		"ScheduledQueryRoleARN": cbor.String(cfg.ScheduledQueryRoleARN),
		"ScheduleConfiguration": cbor.Map{
			"ScheduleExpression": cbor.String(cfg.ScheduleConfiguration.ScheduleExpression),
			"StartTimeOffset":    cbor.Float64(float64(cfg.ScheduleConfiguration.StartTimeOffset)),
			"EndTimeOffset":      cbor.Float64(float64(cfg.ScheduleConfiguration.EndTimeOffset)),
		},
	}
	if cfg.QueryARN != "" {
		m["QueryARN"] = cbor.String(cfg.QueryARN)
	}
	if len(cfg.LogGroupIdentifiers) > 0 {
		m["LogGroupIdentifiers"] = cborStringList(cfg.LogGroupIdentifiers)
	}

	return m
}
