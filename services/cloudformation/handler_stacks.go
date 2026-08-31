package cloudformation

import (
	"encoding/xml"
	"net/url"
	"strconv"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

func (h *Handler) dispatchStackOps(action string, form url.Values, c *echo.Context) (bool, error) {
	switch action {
	case "CreateStack":

		return true, h.handleCreateStack(form, c)
	case "UpdateStack":

		return true, h.handleUpdateStack(form, c)
	case "DeleteStack":

		return true, h.handleDeleteStack(form, c)
	case "DescribeStacks":

		return true, h.handleDescribeStacks(form, c)
	case "ListStacks":

		return true, h.handleListStacks(form, c)
	case "DescribeStackEvents":

		return true, h.handleDescribeStackEvents(form, c)
	case "GetTemplate":

		return true, h.handleGetTemplate(form, c)
	case "ContinueUpdateRollback":

		return true, h.handleContinueUpdateRollback(form, c)
	case "CancelUpdateStack":

		return true, h.handleCancelUpdateStack(form, c)
	case "DescribeAccountLimits":

		return true, h.handleDescribeAccountLimits(form, c)
	case "RollbackStack":

		return true, h.handleRollbackStack(form, c)
	case "DescribeEvents":

		return true, h.handleDescribeEvents(form, c)
	case "UpdateTerminationProtection":

		return true, h.handleUpdateTerminationProtection(form, c)
	default:

		return false, nil
	}
}

// writeCreateOrUpdateStackResponse writes the shared CreateStack/UpdateStack
// response shape: both real outputs carry only OperationId and StackId
// (cloudformation@v1.76.1 api_op_CreateStack.go / api_op_UpdateStack.go),
// differing only in their XML root/result element names.
func writeCreateOrUpdateStackResponse(c *echo.Context, responseElem, resultElem, stackID string) error {
	type result struct {
		XMLName     xml.Name
		OperationID string `xml:"OperationId"`
		StackID     string `xml:"StackId"`
	}
	type response struct {
		XMLName   xml.Name
		Xmlns     string `xml:"xmlns,attr"`
		Result    result
		RequestID string `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{
		XMLName: xml.Name{Local: responseElem},
		Xmlns:   cfnNS,
		Result: result{
			XMLName:     xml.Name{Local: resultElem},
			OperationID: uuid.New().String(),
			StackID:     stackID,
		},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleCreateStack(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}
	stack, err := h.Backend.CreateStack(
		c.Request().Context(),
		stackName, form.Get("TemplateBody"),
		parseParams(form), parseStackOptions(form),
	)
	if err != nil {
		code, msg := mapCreateStackError(err)

		return h.xmlError(c, code, msg)
	}

	return writeCreateOrUpdateStackResponse(c, "CreateStackResponse", "CreateStackResult", stack.StackID)
}

func (h *Handler) handleUpdateStack(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}
	stack, err := h.Backend.UpdateStack(
		c.Request().Context(),
		stackName, form.Get("TemplateBody"),
		parseParams(form), parseStackOptions(form),
	)
	if err != nil {
		code, msg := mapUpdateStackError(err)

		return h.xmlError(c, code, msg)
	}

	return writeCreateOrUpdateStackResponse(c, "UpdateStackResponse", "UpdateStackResult", stack.StackID)
}

func (h *Handler) handleDeleteStack(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}

	if err := h.Backend.DeleteStack(c.Request().Context(), stackName); err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"DeleteStackResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDescribeStacks(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")

	type stackXML struct {
		RollbackConfiguration       *RollbackConfiguration `xml:"RollbackConfiguration,omitempty"`
		StackID                     string                 `xml:"StackId"`
		StackName                   string                 `xml:"StackName"`
		Description                 string                 `xml:"Description,omitempty"`
		StackStatus                 string                 `xml:"StackStatus"`
		StackStatusReason           string                 `xml:"StackStatusReason,omitempty"`
		CreationTime                string                 `xml:"CreationTime"`
		LastUpdatedTime             string                 `xml:"LastUpdatedTime,omitempty"`
		DeletionTime                string                 `xml:"DeletionTime,omitempty"`
		RoleARN                     string                 `xml:"RoleARN,omitempty"`
		Parameters                  []Parameter            `xml:"Parameters>member,omitempty"`
		Outputs                     []Output               `xml:"Outputs>member,omitempty"`
		Tags                        []Tag                  `xml:"Tags>member,omitempty"`
		Capabilities                []string               `xml:"Capabilities>member,omitempty"`
		NotificationARNs            []string               `xml:"NotificationARNs>member,omitempty"`
		EnableTerminationProtection bool                   `xml:"EnableTerminationProtection"`
		DisableRollback             bool                   `xml:"DisableRollback"`
		TimeoutInMinutes            int                    `xml:"TimeoutInMinutes,omitempty"`
	}

	toXML := func(s *Stack) stackXML {
		x := stackXML{
			StackID:                     s.StackID,
			StackName:                   s.StackName,
			Description:                 s.Description,
			StackStatus:                 s.StackStatus,
			StackStatusReason:           s.StackStatusReason,
			CreationTime:                s.CreationTime.UTC().Format("2006-01-02T15:04:05Z"),
			Parameters:                  s.Parameters,
			Outputs:                     s.Outputs,
			Tags:                        s.Tags,
			Capabilities:                s.Capabilities,
			NotificationARNs:            s.NotificationARNs,
			EnableTerminationProtection: s.EnableTerminationProtection,
			DisableRollback:             s.DisableRollback,
			TimeoutInMinutes:            s.TimeoutInMinutes,
			RoleARN:                     s.RoleARN,
			RollbackConfiguration:       s.RollbackConfiguration,
		}
		if s.LastUpdatedTime != nil {
			x.LastUpdatedTime = s.LastUpdatedTime.UTC().Format("2006-01-02T15:04:05Z")
		}
		if s.DeletionTime != nil {
			x.DeletionTime = s.DeletionTime.UTC().Format("2006-01-02T15:04:05Z")
		}

		return x
	}

	var stacks []stackXML

	if stackName != "" {
		s, err := h.Backend.DescribeStack(stackName)
		if err != nil {
			return h.xmlError(c, "ValidationError", err.Error())
		}
		stacks = append(stacks, toXML(s))
	} else {
		all := h.Backend.ListAll()
		for _, s := range all {
			stacks = append(stacks, toXML(s))
		}
	}

	type descResult struct {
		Stacks []stackXML `xml:"Stacks>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"DescribeStacksResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    descResult `xml:"DescribeStacksResult"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    descResult{Stacks: stacks},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleListStacks(form url.Values, c *echo.Context) error {
	statusFilter := parseMemberList(form, "StackStatusFilter.")
	nextToken := form.Get("NextToken")

	p, err := h.Backend.ListStacks(statusFilter, nextToken)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}
	summaries := p.Data

	type summaryXML struct {
		StackID           string `xml:"StackId"`
		StackName         string `xml:"StackName"`
		StackStatus       string `xml:"StackStatus"`
		StackStatusReason string `xml:"StackStatusReason,omitempty"`
		CreationTime      string `xml:"CreationTime"`
		LastUpdatedTime   string `xml:"LastUpdatedTime,omitempty"`
		DeletionTime      string `xml:"DeletionTime,omitempty"`
	}
	members := make([]summaryXML, 0, len(summaries))
	for _, s := range summaries {
		m := summaryXML{
			StackID:           s.StackID,
			StackName:         s.StackName,
			StackStatus:       s.StackStatus,
			StackStatusReason: s.StackStatusReason,
			CreationTime:      s.CreationTime.UTC().Format("2006-01-02T15:04:05Z"),
		}
		if s.LastUpdatedTime != nil {
			m.LastUpdatedTime = s.LastUpdatedTime.UTC().Format("2006-01-02T15:04:05Z")
		}
		if s.DeletionTime != nil {
			m.DeletionTime = s.DeletionTime.UTC().Format("2006-01-02T15:04:05Z")
		}
		members = append(members, m)
	}

	type listResult struct {
		NextToken      string       `xml:"NextToken,omitempty"`
		StackSummaries []summaryXML `xml:"StackSummaries>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"ListStacksResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    listResult `xml:"ListStacksResult"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    listResult{StackSummaries: members, NextToken: p.Next},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleDescribeStackEvents(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}

	p, err := h.Backend.DescribeStackEvents(stackName, form.Get("NextToken"))
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}
	events := p.Data

	type eventXML struct {
		EventID              string `xml:"EventId"`
		StackID              string `xml:"StackId"`
		StackName            string `xml:"StackName"`
		LogicalResourceID    string `xml:"LogicalResourceId"`
		PhysicalResourceID   string `xml:"PhysicalResourceId,omitempty"`
		ResourceType         string `xml:"ResourceType"`
		ResourceStatus       string `xml:"ResourceStatus"`
		ResourceStatusReason string `xml:"ResourceStatusReason,omitempty"`
		Timestamp            string `xml:"Timestamp"`
	}
	members := make([]eventXML, 0, len(events))
	for _, e := range events {
		members = append(members, eventXML{
			EventID:              e.EventID,
			StackID:              e.StackID,
			StackName:            e.StackName,
			LogicalResourceID:    e.LogicalResourceID,
			PhysicalResourceID:   e.PhysicalResourceID,
			ResourceType:         e.ResourceType,
			ResourceStatus:       e.ResourceStatus,
			ResourceStatusReason: e.ResourceStatusReason,
			Timestamp:            e.Timestamp.UTC().Format("2006-01-02T15:04:05Z"),
		})
	}

	type eventsResult struct {
		NextToken   string     `xml:"NextToken,omitempty"`
		StackEvents []eventXML `xml:"StackEvents>member"`
	}
	type response struct {
		XMLName   xml.Name     `xml:"DescribeStackEventsResponse"`
		Xmlns     string       `xml:"xmlns,attr"`
		RequestID string       `xml:"ResponseMetadata>RequestId"`
		Result    eventsResult `xml:"DescribeStackEventsResult"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    eventsResult{StackEvents: members, NextToken: p.Next},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleContinueUpdateRollback(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}

	if err := h.Backend.ContinueUpdateRollback(c.Request().Context(), stackName); err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type result struct{}
	type response struct {
		XMLName   xml.Name `xml:"ContinueUpdateRollbackResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"ContinueUpdateRollbackResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleCancelUpdateStack(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}

	if err := h.Backend.CancelUpdateStack(c.Request().Context(), stackName); err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"CancelUpdateStackResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDescribeAccountLimits(_ url.Values, c *echo.Context) error {
	limits := h.Backend.DescribeAccountLimits()

	type limitXML struct {
		Name  string `xml:"Name"`
		Value int    `xml:"Value"`
	}

	members := make([]limitXML, 0, len(limits))
	for _, l := range limits {
		members = append(members, limitXML(l))
	}

	type limitsResult struct {
		AccountLimits []limitXML `xml:"AccountLimits>member"`
	}
	type response struct {
		XMLName   xml.Name     `xml:"DescribeAccountLimitsResponse"`
		Xmlns     string       `xml:"xmlns,attr"`
		RequestID string       `xml:"ResponseMetadata>RequestId"`
		Result    limitsResult `xml:"DescribeAccountLimitsResult"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    limitsResult{AccountLimits: members},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleRollbackStack(form url.Values, c *echo.Context) error {
	name := form.Get("StackName")
	if name == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}
	stack, err := h.Backend.RollbackStack(c.Request().Context(), name)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}
	type result struct {
		OperationID string `xml:"OperationId"`
		StackID     string `xml:"StackId"`
	}
	type response struct {
		XMLName   xml.Name `xml:"RollbackStackResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"RollbackStackResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    result{OperationID: uuid.New().String(), StackID: stack.StackID},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleDescribeEvents(form url.Values, c *echo.Context) error {
	failedOnly, _ := strconv.ParseBool(form.Get("Filters.FailedEvents"))
	p, _ := h.Backend.DescribeEvents(form.Get("StackName"), form.Get("NextToken"), failedOnly)
	type evXML struct {
		EventID   string `xml:"EventId"`
		StackName string `xml:"StackName"`
		Status    string `xml:"ResourceStatus"`
	}
	members := make([]evXML, 0, len(p.Data))
	for _, e := range p.Data {
		members = append(
			members,
			evXML{EventID: e.EventID, StackName: e.StackName, Status: e.ResourceStatus},
		)
	}
	type result struct {
		NextToken   string  `xml:"NextToken,omitempty"`
		StackEvents []evXML `xml:"StackEvents>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"DescribeEventsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"DescribeEventsResult"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{NextToken: p.Next, StackEvents: members},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleUpdateTerminationProtection(form url.Values, c *echo.Context) error {
	name := form.Get("StackName")
	if name == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}
	enable := form.Get("EnableTerminationProtection") == boolTrue
	if err := h.Backend.UpdateTerminationProtection(name, enable); err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}
	type result struct {
		StackID string `xml:"StackId,omitempty"`
	}
	type response struct {
		XMLName   xml.Name `xml:"UpdateTerminationProtectionResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"UpdateTerminationProtectionResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	var stackID string
	if stack, err := h.Backend.DescribeStack(name); err == nil {
		stackID = stack.StackID
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    result{StackID: stackID},
		RequestID: uuid.New().String(),
	})
}
