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

// parseScheduledQueryConfigurationFromForm reads the
// "ScheduledQueryConfiguration.*" dotted-path members of a PutLogAlarm form
// request, matching the AWS query-protocol convention this package's other
// nested-structure fields (e.g. Dimensions.member.N) already follow.
func parseScheduledQueryConfigurationFromForm(form url.Values) ScheduledQueryConfiguration {
	const prefix = "ScheduledQueryConfiguration."

	startOffset, _ := strconv.ParseInt(form.Get(prefix+"ScheduleConfiguration.StartTimeOffset"), 10, 64)
	endOffset, _ := strconv.ParseInt(form.Get(prefix+"ScheduleConfiguration.EndTimeOffset"), 10, 64)

	return ScheduledQueryConfiguration{
		AggregationExpression: form.Get(prefix + "AggregationExpression"),
		QueryString:           form.Get(prefix + "QueryString"),
		ScheduledQueryRoleARN: form.Get(prefix + "ScheduledQueryRoleARN"),
		LogGroupIdentifiers:   parseMemberList(form, prefix+"LogGroupIdentifiers."),
		ScheduleConfiguration: ScheduleConfiguration{
			ScheduleExpression: form.Get(prefix + "ScheduleConfiguration.ScheduleExpression"),
			StartTimeOffset:    startOffset,
			EndTimeOffset:      endOffset,
		},
	}
}

func (h *Handler) handlePutLogAlarm(form url.Values, c *echo.Context) error {
	alarmName := form.Get("AlarmName")
	if alarmName == "" {
		return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", "AlarmName is required")
	}

	threshold, _ := strconv.ParseFloat(form.Get("Threshold"), 64)
	queryResultsToAlarm, _ := strconv.ParseInt(form.Get("QueryResultsToAlarm"), 10, 32)
	queryResultsToEvaluate, _ := strconv.ParseInt(form.Get("QueryResultsToEvaluate"), 10, 32)
	actionLogLineCount, _ := strconv.ParseInt(form.Get("ActionLogLineCount"), 10, 32)
	actionsEnabled := form.Get("ActionsEnabled") != formFalse

	alarm := &LogAlarm{
		AlarmName:                   alarmName,
		ComparisonOperator:          form.Get("ComparisonOperator"),
		TreatMissingData:            form.Get("TreatMissingData"),
		AlarmDescription:            form.Get("AlarmDescription"),
		ActionLogLineRoleArn:        form.Get("ActionLogLineRoleArn"),
		Threshold:                   threshold,
		QueryResultsToAlarm:         int32(queryResultsToAlarm),
		QueryResultsToEvaluate:      int32(queryResultsToEvaluate),
		ActionLogLineCount:          int32(actionLogLineCount),
		ActionsEnabled:              actionsEnabled,
		AlarmActions:                parseMemberList(form, "AlarmActions."),
		OKActions:                   parseMemberList(form, "OKActions."),
		InsufficientDataActions:     parseMemberList(form, "InsufficientDataActions."),
		ScheduledQueryConfiguration: parseScheduledQueryConfigurationFromForm(form),
	}
	if err := h.Backend.PutLogAlarm(alarm); err != nil {
		if errors.Is(err, ErrValidation) {
			return h.xmlError(c, http.StatusBadRequest, "InvalidParameterValue", err.Error())
		}

		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"PutLogAlarmResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

// scheduledQueryConfigurationXML is the XML representation of a
// ScheduledQueryConfiguration.
type scheduledQueryConfigurationXML struct {
	AggregationExpression string   `xml:"AggregationExpression"`
	QueryString           string   `xml:"QueryString"`
	ScheduledQueryRoleARN string   `xml:"ScheduledQueryRoleARN"`
	QueryARN              string   `xml:"QueryARN,omitempty"`
	LogGroupIdentifiers   []string `xml:"LogGroupIdentifiers>member,omitempty"`
	ScheduleConfiguration struct {
		ScheduleExpression string `xml:"ScheduleExpression"`
		StartTimeOffset    int64  `xml:"StartTimeOffset"`
		EndTimeOffset      int64  `xml:"EndTimeOffset,omitempty"`
	} `xml:"ScheduleConfiguration"`
}

// logAlarmXML is the XML representation of a LogAlarm.
type logAlarmXML struct {
	StateReasonData                    string                         `xml:"StateReasonData,omitempty"`
	EvaluationState                    string                         `xml:"EvaluationState,omitempty"`
	StateUpdatedTimestamp              string                         `xml:"StateUpdatedTimestamp,omitempty"`
	AlarmDescription                   string                         `xml:"AlarmDescription,omitempty"`
	ComparisonOperator                 string                         `xml:"ComparisonOperator"`
	TreatMissingData                   string                         `xml:"TreatMissingData,omitempty"`
	AlarmArn                           string                         `xml:"AlarmArn"`
	StateValue                         string                         `xml:"StateValue"`
	AlarmName                          string                         `xml:"AlarmName"`
	ActionLogLineRoleArn               string                         `xml:"ActionLogLineRoleArn,omitempty"`
	StateTransitionedTimestamp         string                         `xml:"StateTransitionedTimestamp,omitempty"`
	AlarmConfigurationUpdatedTimestamp string                         `xml:"AlarmConfigurationUpdatedTimestamp,omitempty"`
	StateReason                        string                         `xml:"StateReason,omitempty"`
	AlarmActions                       []string                       `xml:"AlarmActions>member,omitempty"`
	InsufficientDataActions            []string                       `xml:"InsufficientDataActions>member,omitempty"`
	OKActions                          []string                       `xml:"OKActions>member,omitempty"`
	ScheduledQueryConfiguration        scheduledQueryConfigurationXML `xml:"ScheduledQueryConfiguration"`
	Threshold                          float64                        `xml:"Threshold"`
	QueryResultsToAlarm                int32                          `xml:"QueryResultsToAlarm"`
	QueryResultsToEvaluate             int32                          `xml:"QueryResultsToEvaluate"`
	ActionLogLineCount                 int32                          `xml:"ActionLogLineCount,omitempty"`
	ActionsEnabled                     bool                           `xml:"ActionsEnabled"`
}

// logAlarmToXML converts a LogAlarm to its XML representation.
func logAlarmToXML(a LogAlarm) logAlarmXML {
	x := logAlarmXML{
		AlarmName:               a.AlarmName,
		AlarmArn:                a.AlarmArn,
		ComparisonOperator:      a.ComparisonOperator,
		TreatMissingData:        a.TreatMissingData,
		Threshold:               a.Threshold,
		StateValue:              a.StateValue,
		StateReason:             a.StateReason,
		StateReasonData:         a.StateReasonData,
		AlarmDescription:        a.AlarmDescription,
		EvaluationState:         a.EvaluationState,
		ActionLogLineRoleArn:    a.ActionLogLineRoleArn,
		ActionLogLineCount:      a.ActionLogLineCount,
		QueryResultsToAlarm:     a.QueryResultsToAlarm,
		QueryResultsToEvaluate:  a.QueryResultsToEvaluate,
		AlarmActions:            a.AlarmActions,
		OKActions:               a.OKActions,
		InsufficientDataActions: a.InsufficientDataActions,
		ActionsEnabled:          a.ActionsEnabled,
	}
	if !a.StateTransitionedTimestamp.IsZero() {
		x.StateTransitionedTimestamp = a.StateTransitionedTimestamp.UTC().Format(time.RFC3339)
	}
	if !a.StateUpdatedTimestamp.IsZero() {
		x.StateUpdatedTimestamp = a.StateUpdatedTimestamp.UTC().Format(time.RFC3339)
	}
	if !a.AlarmConfigurationUpdatedTimestamp.IsZero() {
		x.AlarmConfigurationUpdatedTimestamp = a.AlarmConfigurationUpdatedTimestamp.UTC().
			Format(time.RFC3339)
	}
	x.ScheduledQueryConfiguration.AggregationExpression = a.ScheduledQueryConfiguration.AggregationExpression
	x.ScheduledQueryConfiguration.QueryString = a.ScheduledQueryConfiguration.QueryString
	x.ScheduledQueryConfiguration.ScheduledQueryRoleARN = a.ScheduledQueryConfiguration.ScheduledQueryRoleARN
	x.ScheduledQueryConfiguration.QueryARN = a.ScheduledQueryConfiguration.QueryARN
	x.ScheduledQueryConfiguration.LogGroupIdentifiers = a.ScheduledQueryConfiguration.LogGroupIdentifiers
	x.ScheduledQueryConfiguration.ScheduleConfiguration.ScheduleExpression =
		a.ScheduledQueryConfiguration.ScheduleConfiguration.ScheduleExpression
	x.ScheduledQueryConfiguration.ScheduleConfiguration.StartTimeOffset =
		a.ScheduledQueryConfiguration.ScheduleConfiguration.StartTimeOffset
	x.ScheduledQueryConfiguration.ScheduleConfiguration.EndTimeOffset =
		a.ScheduledQueryConfiguration.ScheduleConfiguration.EndTimeOffset

	return x
}
