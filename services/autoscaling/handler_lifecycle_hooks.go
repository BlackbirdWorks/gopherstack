package autoscaling

import (
	"encoding/xml"
	"fmt"
	"net/url"
)

func (h *Handler) handleCompleteLifecycleAction(vals url.Values) (any, error) {
	input := CompleteLifecycleActionInput{
		AutoScalingGroupName:  vals.Get("AutoScalingGroupName"),
		LifecycleHookName:     vals.Get("LifecycleHookName"),
		LifecycleActionToken:  vals.Get("LifecycleActionToken"),
		InstanceID:            vals.Get("InstanceId"),
		LifecycleActionResult: vals.Get("LifecycleActionResult"),
	}

	if err := h.Backend.CompleteLifecycleAction(input); err != nil {
		return nil, err
	}

	return &completeLifecycleActionResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-complete-lifecycle"},
	}, nil
}

func (h *Handler) handleDeleteLifecycleHook(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	hookName := vals.Get("LifecycleHookName")

	if err := h.Backend.DeleteLifecycleHook(groupName, hookName); err != nil {
		return nil, err
	}

	return &deleteLifecycleHookResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-delete-lifecycle-hook"},
	}, nil
}

func (h *Handler) handlePutLifecycleHook(vals url.Values) (any, error) {
	hook := LifecycleHook{
		LifecycleHookName:     vals.Get("LifecycleHookName"),
		AutoScalingGroupName:  vals.Get("AutoScalingGroupName"),
		LifecycleTransition:   vals.Get("LifecycleTransition"),
		DefaultResult:         vals.Get("DefaultResult"),
		NotificationTargetARN: vals.Get("NotificationTargetARN"),
		NotificationMetadata:  vals.Get("NotificationMetadata"),
		RoleARN:               vals.Get("RoleARN"),
	}

	if v := vals.Get("HeartbeatTimeout"); v != "" {
		n, err := parseIntVal(v)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid HeartbeatTimeout", ErrInvalidParameter)
		}

		hook.HeartbeatTimeout = n
	}

	if err := h.Backend.PutLifecycleHook(hook); err != nil {
		return nil, err
	}

	return &putLifecycleHookResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-put-lifecycle-hook"},
	}, nil
}

func (h *Handler) handleDescribeLifecycleHooks(vals url.Values) (any, error) {
	groupName := vals.Get("AutoScalingGroupName")
	hookNames := parseMembers(vals, "LifecycleHookNames.member")

	hooks, err := h.Backend.DescribeLifecycleHooks(groupName, hookNames)
	if err != nil {
		return nil, err
	}

	members := make([]xmlLifecycleHook, 0, len(hooks))
	for _, hook := range hooks {
		members = append(members, xmlLifecycleHook(hook))
	}

	return &describeLifecycleHooksResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeLifecycleHooksResult{
			LifecycleHooks: xmlLifecycleHookList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-lifecycle-hooks"},
	}, nil
}

type completeLifecycleActionResponse struct {
	XMLName          xml.Name            `xml:"CompleteLifecycleActionResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type deleteLifecycleHookResponse struct {
	XMLName          xml.Name            `xml:"DeleteLifecycleHookResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type putLifecycleHookResponse struct {
	XMLName          xml.Name            `xml:"PutLifecycleHookResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type xmlLifecycleHook struct {
	LifecycleHookName     string `xml:"LifecycleHookName"`
	AutoScalingGroupName  string `xml:"AutoScalingGroupName"`
	LifecycleTransition   string `xml:"LifecycleTransition,omitempty"`
	DefaultResult         string `xml:"DefaultResult,omitempty"`
	NotificationTargetARN string `xml:"NotificationTargetARN,omitempty"`
	NotificationMetadata  string `xml:"NotificationMetadata,omitempty"`
	RoleARN               string `xml:"RoleARN,omitempty"`
	HeartbeatTimeout      int32  `xml:"HeartbeatTimeout,omitempty"`
	GlobalTimeout         int32  `xml:"GlobalTimeout,omitempty"`
}

type xmlLifecycleHookList struct {
	Members []xmlLifecycleHook `xml:"member"`
}

type describeLifecycleHooksResult struct {
	LifecycleHooks xmlLifecycleHookList `xml:"LifecycleHooks"`
}

type describeLifecycleHooksResponse struct {
	XMLName          xml.Name                     `xml:"DescribeLifecycleHooksResponse"`
	Xmlns            string                       `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata          `xml:"ResponseMetadata"`
	Result           describeLifecycleHooksResult `xml:"DescribeLifecycleHooksResult"`
}

func (h *Handler) handleDescribeLifecycleHookTypes(_ url.Values) (any, error) {
	types, err := h.Backend.DescribeLifecycleHookTypes()
	if err != nil {
		return nil, err
	}

	members := make([]xmlStringValue, 0, len(types))
	for _, t := range types {
		members = append(members, xmlStringValue{Value: t})
	}

	return &describeLifecycleHookTypesResponse{
		Xmlns: autoscalingXMLNS,
		Result: describeLifecycleHookTypesResult{
			LifecycleHookTypes: xmlStringValueList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-describe-lifecycle-hook-types"},
	}, nil
}

func (h *Handler) handleRecordLifecycleActionHeartbeat(vals url.Values) (any, error) {
	input := RecordLifecycleActionHeartbeatInput{
		AutoScalingGroupName: vals.Get("AutoScalingGroupName"),
		LifecycleHookName:    vals.Get("LifecycleHookName"),
		LifecycleActionToken: vals.Get("LifecycleActionToken"),
		InstanceID:           vals.Get("InstanceId"),
	}

	if err := h.Backend.RecordLifecycleActionHeartbeat(input); err != nil {
		return nil, err
	}

	return &recordLifecycleActionHeartbeatResponse{
		Xmlns:            autoscalingXMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "autoscaling-record-lifecycle-heartbeat"},
	}, nil
}

type describeLifecycleHookTypesResult struct {
	LifecycleHookTypes xmlStringValueList `xml:"LifecycleHookTypes"`
}

type describeLifecycleHookTypesResponse struct {
	XMLName          xml.Name                         `xml:"DescribeLifecycleHookTypesResponse"`
	Xmlns            string                           `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata              `xml:"ResponseMetadata"`
	Result           describeLifecycleHookTypesResult `xml:"DescribeLifecycleHookTypesResult"`
}

type recordLifecycleActionHeartbeatResponse struct {
	XMLName          xml.Name            `xml:"RecordLifecycleActionHeartbeatResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}
