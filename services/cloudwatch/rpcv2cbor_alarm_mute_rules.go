package cloudwatch

import (
	"math"
	"net/http"
	"time"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
)

func (h *Handler) cborPutAlarmMuteRule(input cbor.Map, c *echo.Context) error {
	muteName := cborStr(input, "MuteName")
	if muteName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"MuteName is required",
		)
	}

	rawDuration := cborValInt64(input["MuteDuration"])
	if rawDuration < 0 || rawDuration > math.MaxInt32 {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"MuteDuration must be between 0 and 2147483647",
		)
	}

	rule := &AlarmMuteRule{
		MuteName:      muteName,
		Description:   cborStr(input, "Description"),
		AlarmNames:    cborStrList(input, "AlarmNames"),
		MuteDuration:  int32(rawDuration),
		MuteStartTime: time.Now().UTC(),
	}

	if start := cborTime(input, "MuteStartTime"); !start.IsZero() {
		rule.MuteStartTime = start.UTC()
	}

	if err := h.Backend.PutAlarmMuteRule(rule); err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	// Keyed by MuteName, matching the (non-ARN-shaped) value
	// cborGetAlarmMuteRule already returns as AlarmMuteRuleArn.
	h.applyCreationTags(input, rule.MuteName)

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborGetAlarmMuteRule(input cbor.Map, c *echo.Context) error {
	muteName := cborStr(input, "MuteName")
	if muteName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"MuteName is required",
		)
	}

	rule, err := h.Backend.GetAlarmMuteRule(muteName)
	if err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	muteRule := cbor.Map{
		"MuteName":    cbor.String(rule.MuteName),
		"Description": cbor.String(rule.Description),
		"MuteDuration": cbor.Uint(
			uint64(rule.MuteDuration), //nolint:gosec // MuteDuration is always non-negative
		),
		"CreationTime": cborFromTime(rule.CreationTime),
		"AlarmNames":   cborStringList(rule.AlarmNames),
	}
	if !rule.MuteStartTime.IsZero() {
		muteRule["MuteStartTime"] = cborFromTime(rule.MuteStartTime)
	}

	return writeCBOR(c, cbor.Map{"MuteRule": muteRule})
}

func (h *Handler) cborDeleteAlarmMuteRule(input cbor.Map, c *echo.Context) error {
	muteName := cborStr(input, "MuteName")
	if muteName == "" {
		return h.cborError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"MuteName is required",
		)
	}

	if err := h.Backend.DeleteAlarmMuteRule(muteName); err != nil {
		return h.cborError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborListAlarmMuteRules(input cbor.Map, c *echo.Context) error {
	nextToken := cborStr(input, "NextToken")
	maxResults := int(cborInt32(input, "MaxRecords"))
	if maxResults == 0 {
		maxResults = int(cborInt32(input, "MaxResults"))
	}

	p, err := h.Backend.ListAlarmMuteRules(nextToken, maxResults)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	summaries := make(cbor.List, 0, len(p.Data))
	for _, rule := range p.Data {
		entry := cbor.Map{
			"AlarmMuteRuleArn": cbor.String(rule.MuteName),
			"Status":           cbor.String("active"),
		}
		if !rule.CreationTime.IsZero() {
			entry["LastUpdatedTimestamp"] = cborFromTime(rule.CreationTime)
		}
		summaries = append(summaries, entry)
	}

	out := cbor.Map{
		"AlarmMuteRuleSummaries": summaries,
	}
	if p.Next != "" {
		out["NextToken"] = cbor.String(p.Next)
	}

	return writeCBOR(c, out)
}
