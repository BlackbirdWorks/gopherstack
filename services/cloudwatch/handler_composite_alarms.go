package cloudwatch

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// compositeAlarmToXML converts a CompositeAlarm to its XML representation.
func compositeAlarmToXML(a CompositeAlarm) compositeAlarmXMLType {
	x := compositeAlarmXMLType{
		AlarmName:               a.AlarmName,
		AlarmArn:                a.AlarmArn,
		AlarmRule:               a.AlarmRule,
		StateValue:              a.StateValue,
		StateReason:             a.StateReason,
		AlarmDescription:        a.AlarmDescription,
		AlarmActions:            a.AlarmActions,
		OKActions:               a.OKActions,
		InsufficientDataActions: a.InsufficientDataActions,
		ActionsEnabled:          a.ActionsEnabled,
	}
	if !a.StateTransitionedTimestamp.IsZero() {
		x.StateTransitionedTimestamp = a.StateTransitionedTimestamp.UTC().Format(time.RFC3339)
	}

	return x
}

// compositeAlarmXMLType is the XML representation of a CompositeAlarm.
type compositeAlarmXMLType struct {
	StateTransitionedTimestamp string   `xml:"StateTransitionedTimestamp,omitempty"`
	AlarmName                  string   `xml:"AlarmName"`
	AlarmArn                   string   `xml:"AlarmArn"`
	AlarmRule                  string   `xml:"AlarmRule"`
	StateValue                 string   `xml:"StateValue"`
	StateReason                string   `xml:"StateReason,omitempty"`
	AlarmDescription           string   `xml:"AlarmDescription,omitempty"`
	AlarmActions               []string `xml:"AlarmActions>member,omitempty"`
	OKActions                  []string `xml:"OKActions>member,omitempty"`
	InsufficientDataActions    []string `xml:"InsufficientDataActions>member,omitempty"`
	ActionsEnabled             bool     `xml:"ActionsEnabled"`
}

func (h *Handler) handlePutCompositeAlarm(form url.Values, c *echo.Context) error {
	alarmName := form.Get("AlarmName")
	if alarmName == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"AlarmName is required",
		)
	}
	alarmRule := form.Get("AlarmRule")
	if alarmRule == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValue",
			"AlarmRule is required",
		)
	}

	actionsEnabled := form.Get("ActionsEnabled") != formFalse

	alarm := &CompositeAlarm{
		AlarmName:               alarmName,
		AlarmRule:               alarmRule,
		AlarmDescription:        form.Get("AlarmDescription"),
		ActionsEnabled:          actionsEnabled,
		AlarmActions:            parseMemberList(form, "AlarmActions."),
		OKActions:               parseMemberList(form, "OKActions."),
		InsufficientDataActions: parseMemberList(form, "InsufficientDataActions."),
	}
	if err := h.Backend.PutCompositeAlarm(alarm); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"PutCompositeAlarmResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}
