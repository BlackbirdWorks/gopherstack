package autoscaling

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"time"
)

func (h *Handler) handleBatchDeleteScheduledAction(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	actionNames := parseMembers(vals, "ScheduledActionNames.member")

	failed, err := h.Backend.BatchDeleteScheduledAction(groupName, actionNames)
	if err != nil {
		return nil, err
	}

	members := make([]xmlFailedScheduledAction, 0, len(failed))
	for _, f := range failed {
		members = append(members, xmlFailedScheduledAction(f))
	}

	return &batchDeleteScheduledActionResponse{
		Xmlns: autoscalingXMLNS,
		Result: batchDeleteScheduledActionResult{
			FailedScheduledActions: xmlFailedScheduledActionList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-batch-delete-scheduled"},
	}, nil
}

func (h *Handler) handleBatchPutScheduledUpdateGroupAction(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	actions := parseBatchScheduledActions(vals)

	failed, err := h.Backend.BatchPutScheduledUpdateGroupAction(groupName, actions)
	if err != nil {
		return nil, err
	}

	members := make([]xmlFailedScheduledAction, 0, len(failed))
	for _, f := range failed {
		members = append(members, xmlFailedScheduledAction(f))
	}

	return &batchPutScheduledUpdateGroupActionResponse{
		Xmlns: autoscalingXMLNS,
		Result: batchPutScheduledUpdateGroupActionResult{
			FailedScheduledUpdateGroupActions: xmlFailedScheduledActionList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-batch-put-scheduled"},
	}, nil
}

func (h *Handler) handleDescribeScheduledActions(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	actionNames := parseMembers(vals, "ScheduledActionNames.member")

	actions, err := h.Backend.DescribeScheduledActions(groupName, actionNames)
	if err != nil {
		return nil, err
	}

	members := make([]xmlScheduledAction, 0, len(actions))
	for _, action := range actions {
		startTime := ""
		if !action.StartTime.IsZero() {
			startTime = action.StartTime.UTC().Format(time.RFC3339)
		}

		endTime := ""
		if !action.EndTime.IsZero() {
			endTime = action.EndTime.UTC().Format(time.RFC3339)
		}

		members = append(members, xmlScheduledAction{
			ScheduledActionName:  action.ScheduledActionName,
			ScheduledActionARN:   action.ScheduledActionARN,
			AutoScalingGroupName: action.AutoScalingGroupName,
			Recurrence:           action.Recurrence,
			TimeZone:             action.TimeZone,
			StartTime:            startTime,
			EndTime:              endTime,
			DesiredCapacity:      action.DesiredCapacity,
			MinSize:              action.MinSize,
			MaxSize:              action.MaxSize,
		})
	}

	return &describeScheduledActionsResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeScheduledActionsResult{
			ScheduledUpdateGroupActions: xmlScheduledActionList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-scheduled-actions"},
	}, nil
}

// parseBatchScheduledActions parses ScheduledUpdateGroupAction entries from form values.
func parseBatchScheduledActions(vals url.Values) []ScheduledUpdateGroupAction {
	result := make([]ScheduledUpdateGroupAction, 0)

	prefix := "ScheduledUpdateGroupActions.member"

	for i := 1; ; i++ {
		nameKey := fmt.Sprintf("%s.%d.ScheduledActionName", prefix, i)
		name := vals.Get(nameKey)

		if name == "" {
			break
		}

		action := ScheduledUpdateGroupAction{
			ScheduledActionName: name,
			Recurrence:          vals.Get(fmt.Sprintf("%s.%d.Recurrence", prefix, i)),
			TimeZone:            vals.Get(fmt.Sprintf("%s.%d.TimeZone", prefix, i)),
			StartTime:           parseTimeVal(vals.Get(fmt.Sprintf("%s.%d.StartTime", prefix, i))),
			EndTime:             parseTimeVal(vals.Get(fmt.Sprintf("%s.%d.EndTime", prefix, i))),
		}

		if v := vals.Get(fmt.Sprintf("%s.%d.DesiredCapacity", prefix, i)); v != "" {
			if n, err := parseIntVal(v); err == nil {
				action.DesiredCapacity = &n
			}
		}

		if v := vals.Get(fmt.Sprintf("%s.%d.MinSize", prefix, i)); v != "" {
			if n, err := parseIntVal(v); err == nil {
				action.MinSize = &n
			}
		}

		if v := vals.Get(fmt.Sprintf("%s.%d.MaxSize", prefix, i)); v != "" {
			if n, err := parseIntVal(v); err == nil {
				action.MaxSize = &n
			}
		}

		result = append(result, action)
	}

	return result
}

type xmlFailedScheduledAction struct {
	ScheduledActionName string `xml:"ScheduledActionName"`
	ErrorCode           string `xml:"ErrorCode"`
	ErrorMessage        string `xml:"ErrorMessage"`
}

type xmlFailedScheduledActionList struct {
	Members []xmlFailedScheduledAction `xml:"member"`
}

type batchDeleteScheduledActionResult struct {
	FailedScheduledActions xmlFailedScheduledActionList `xml:"FailedScheduledActions"`
}

type batchDeleteScheduledActionResponse struct {
	XMLName          xml.Name                         `xml:"BatchDeleteScheduledActionResponse"`
	Xmlns            string                           `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata              `xml:"ResponseMetadata"`
	Result           batchDeleteScheduledActionResult `xml:"BatchDeleteScheduledActionResult"`
}

type batchPutScheduledUpdateGroupActionResult struct {
	FailedScheduledUpdateGroupActions xmlFailedScheduledActionList `xml:"FailedScheduledUpdateGroupActions"`
}

type batchPutScheduledUpdateGroupActionResponse struct {
	XMLName          xml.Name                                 `xml:"BatchPutScheduledUpdateGroupActionResponse"`
	Xmlns            string                                   `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                      `xml:"ResponseMetadata"`
	Result           batchPutScheduledUpdateGroupActionResult `xml:"BatchPutScheduledUpdateGroupActionResult"`
}

type xmlScheduledAction struct {
	DesiredCapacity      *int32 `xml:"DesiredCapacity,omitempty"`
	MinSize              *int32 `xml:"MinSize,omitempty"`
	MaxSize              *int32 `xml:"MaxSize,omitempty"`
	ScheduledActionName  string `xml:"ScheduledActionName"`
	ScheduledActionARN   string `xml:"ScheduledActionARN,omitempty"`
	AutoScalingGroupName string `xml:"AutoScalingGroupName,omitempty"`
	Recurrence           string `xml:"Recurrence,omitempty"`
	TimeZone             string `xml:"TimeZone,omitempty"`
	StartTime            string `xml:"StartTime,omitempty"`
	EndTime              string `xml:"EndTime,omitempty"`
}

type xmlScheduledActionList struct {
	Members []xmlScheduledAction `xml:"member"`
}

type describeScheduledActionsResult struct {
	NextToken                   string                 `xml:"NextToken,omitempty"`
	ScheduledUpdateGroupActions xmlScheduledActionList `xml:"ScheduledUpdateGroupActions"`
}

type describeScheduledActionsResponse struct {
	XMLName          xml.Name                       `xml:"DescribeScheduledActionsResponse"`
	Xmlns            string                         `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata            `xml:"ResponseMetadata"`
	Result           describeScheduledActionsResult `xml:"DescribeScheduledActionsResult"`
}

func (h *Handler) handlePutScheduledUpdateGroupAction(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")

	action := ScheduledUpdateGroupAction{
		ScheduledActionName: vals.Get("ScheduledActionName"),
		Recurrence:          vals.Get("Recurrence"),
		TimeZone:            vals.Get("TimeZone"),
		StartTime:           parseTimeVal(vals.Get("StartTime")),
		EndTime:             parseTimeVal(vals.Get("EndTime")),
	}

	if v := vals.Get("DesiredCapacity"); v != "" {
		if n, err := parseIntVal(v); err == nil {
			action.DesiredCapacity = &n
		}
	}

	if v := vals.Get("MinSize"); v != "" {
		if n, err := parseIntVal(v); err == nil {
			action.MinSize = &n
		}
	}

	if v := vals.Get("MaxSize"); v != "" {
		if n, err := parseIntVal(v); err == nil {
			action.MaxSize = &n
		}
	}

	if err := h.Backend.PutScheduledUpdateGroupAction(groupName, action); err != nil {
		return nil, err
	}

	return &putScheduledUpdateGroupActionResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-put-scheduled-action"},
	}, nil
}

func (h *Handler) handleDeleteScheduledAction(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	actionName := vals.Get("ScheduledActionName")

	if err := h.Backend.DeleteScheduledAction(groupName, actionName); err != nil {
		return nil, err
	}

	return &deleteScheduledActionResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-delete-scheduled-action"},
	}, nil
}

type putScheduledUpdateGroupActionResponse struct {
	XMLName          xml.Name            `xml:"PutScheduledUpdateGroupActionResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type deleteScheduledActionResponse struct {
	XMLName          xml.Name            `xml:"DeleteScheduledActionResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}
