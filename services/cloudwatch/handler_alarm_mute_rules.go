package cloudwatch

import (
	"encoding/xml"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

func (h *Handler) putAlarmMuteRuleFromForm(form url.Values, c *echo.Context) error {
	muteName := form.Get("MuteName")
	if muteName == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "MuteName is required")
	}

	var muteDuration int64
	if rawDuration := form.Get("MuteDuration"); rawDuration != "" {
		parsedDuration, err := strconv.ParseInt(rawDuration, 10, 64)
		if err != nil {
			return h.xmlError(
				c,
				http.StatusBadRequest,
				"InvalidParameterValue",
				"MuteDuration must be an integer",
			)
		}
		if parsedDuration < 0 || parsedDuration > math.MaxInt32 {
			return h.xmlError(
				c,
				http.StatusBadRequest,
				"InvalidParameterValue",
				"MuteDuration must be between 0 and 2147483647",
			)
		}
		muteDuration = parsedDuration
	}

	rule := &AlarmMuteRule{
		MuteName:      muteName,
		Description:   form.Get("Description"),
		MuteDuration:  int32(muteDuration), //nolint:gosec // bounds checked above (0..MaxInt32)
		AlarmNames:    parseMemberList(form, "AlarmNames."),
		MuteStartTime: time.Now().UTC(),
	}

	if rawStart := form.Get("MuteStartTime"); rawStart != "" {
		start, err := time.Parse(time.RFC3339, rawStart)
		if err != nil {
			return h.xmlError(
				c,
				http.StatusBadRequest,
				"InvalidParameterValue",
				"MuteStartTime must be RFC3339",
			)
		}
		rule.MuteStartTime = start.UTC()
	}

	if err := h.Backend.PutAlarmMuteRule(rule); err != nil {
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
	muteName := form.Get("MuteName")
	if muteName == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "MuteName is required")
	}

	if err := h.Backend.DeleteAlarmMuteRule(muteName); err != nil {
		return h.xmlError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"DeleteAlarmMuteRuleResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleGetAlarmMuteRule(form url.Values, c *echo.Context) error {
	muteName := form.Get("MuteName")
	if muteName == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "MuteName is required")
	}

	rule, err := h.Backend.GetAlarmMuteRule(muteName)
	if err != nil {
		return h.xmlError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	type muteRuleXML struct {
		MuteName      string   `xml:"MuteName"`
		Description   string   `xml:"Description,omitempty"`
		CreationTime  string   `xml:"CreationTime"`
		MuteStartTime string   `xml:"MuteStartTime,omitempty"`
		AlarmNames    []string `xml:"AlarmNames>member,omitempty"`
		MuteDuration  int32    `xml:"MuteDuration,omitempty"`
	}
	type result struct {
		MuteRule muteRuleXML `xml:"MuteRule"`
	}
	type response struct {
		XMLName   xml.Name `xml:"GetAlarmMuteRuleResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"GetAlarmMuteRuleResult"`
	}

	mr := muteRuleXML{
		MuteName:     rule.MuteName,
		Description:  rule.Description,
		AlarmNames:   rule.AlarmNames,
		CreationTime: rule.CreationTime.UTC().Format(time.RFC3339),
		MuteDuration: rule.MuteDuration,
	}
	if !rule.MuteStartTime.IsZero() {
		mr.MuteStartTime = rule.MuteStartTime.UTC().Format(time.RFC3339)
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    result{MuteRule: mr},
	})
}

func (h *Handler) handleListAlarmMuteRules(form url.Values, c *echo.Context) error {
	nextToken := form.Get("NextToken")
	maxResults, _ := strconv.Atoi(form.Get("MaxResults"))

	p, err := h.Backend.ListAlarmMuteRules(nextToken, maxResults)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type muteRuleXML struct {
		MuteName      string   `xml:"MuteName"`
		Description   string   `xml:"Description,omitempty"`
		CreationTime  string   `xml:"CreationTime"`
		MuteStartTime string   `xml:"MuteStartTime,omitempty"`
		AlarmNames    []string `xml:"AlarmNames>member,omitempty"`
		MuteDuration  int32    `xml:"MuteDuration,omitempty"`
	}
	type listResult struct {
		NextToken string        `xml:"NextToken,omitempty"`
		MuteRules []muteRuleXML `xml:"MuteRules>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"ListAlarmMuteRulesResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    listResult `xml:"ListAlarmMuteRulesResult"`
	}

	members := make([]muteRuleXML, 0, len(p.Data))
	for _, rule := range p.Data {
		mr := muteRuleXML{
			MuteName:     rule.MuteName,
			Description:  rule.Description,
			AlarmNames:   rule.AlarmNames,
			MuteDuration: rule.MuteDuration,
			CreationTime: rule.CreationTime.UTC().Format(time.RFC3339),
		}
		if !rule.MuteStartTime.IsZero() {
			mr.MuteStartTime = rule.MuteStartTime.UTC().Format(time.RFC3339)
		}
		members = append(members, mr)
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    listResult{MuteRules: members, NextToken: p.Next},
	})
}
