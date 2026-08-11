package cloudwatch

import (
	"net/http"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
)

// buildCompositeAlarmCBOR converts a CompositeAlarm to a cbor.Map.
func buildCompositeAlarmCBOR(a *CompositeAlarm) cbor.Map {
	m := cbor.Map{
		keyAlarmName:        cbor.String(a.AlarmName),
		keyAlarmArn:         cbor.String(a.AlarmArn),
		keyAlarmType:        cbor.String("CompositeAlarm"),
		"AlarmRule":         cbor.String(a.AlarmRule),
		keyStateValue:       cbor.String(a.StateValue),
		keyStateReason:      cbor.String(a.StateReason),
		keyAlarmDescription: cbor.String(a.AlarmDescription),
		keyActionsEnabled:   cbor.Bool(a.ActionsEnabled),
	}
	if !a.CreatedAt.IsZero() {
		m["AlarmCreatedAt"] = cborFromTime(a.CreatedAt)
	}
	if !a.StateTransitionedTimestamp.IsZero() {
		m["StateTransitionedTimestamp"] = cborFromTime(a.StateTransitionedTimestamp)
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

func (h *Handler) cborPutCompositeAlarm(input cbor.Map, c *echo.Context) error {
	alarmName := cborStr(input, "AlarmName")
	if alarmName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"AlarmName is required",
		)
	}
	alarmRule := cborStr(input, "AlarmRule")
	if alarmRule == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"AlarmRule is required",
		)
	}

	actionsEnabled := true
	if v, ok := input["ActionsEnabled"]; ok {
		if b, isBool := v.(cbor.Bool); isBool {
			actionsEnabled = bool(b)
		}
	}

	alarm := &CompositeAlarm{
		AlarmName:               alarmName,
		AlarmRule:               alarmRule,
		AlarmDescription:        cborStr(input, "AlarmDescription"),
		ActionsEnabled:          actionsEnabled,
		AlarmActions:            cborStrList(input, "AlarmActions"),
		OKActions:               cborStrList(input, "OKActions"),
		InsufficientDataActions: cborStrList(input, "InsufficientDataActions"),
	}

	if err := h.Backend.PutCompositeAlarm(alarm); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	h.applyCreationTags(input, alarm.AlarmArn)

	return writeCBOR(c, cbor.Map{})
}
