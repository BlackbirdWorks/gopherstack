package cloudformation

import (
	"encoding/xml"
	"net/url"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// dispatchHookOps handles CloudFormation Hooks progress and result operations.
func (h *Handler) dispatchHookOps(action string, form url.Values, c *echo.Context) (bool, error) {
	switch action {
	case "RecordHandlerProgress":
		return true, h.handleRecordHandlerProgress(form, c)
	case "GetHookResult":
		return true, h.handleGetHookResult(form, c)
	case "ListHookResults":
		return true, h.handleListHookResults(form, c)
	case "DescribeChangeSetHooks":
		return true, h.handleDescribeChangeSetHooks(form, c)
	default:
		return false, nil
	}
}

func (h *Handler) handleRecordHandlerProgress(form url.Values, c *echo.Context) error {
	_ = h.Backend.RecordHandlerProgress(form.Get("BearerToken"), form.Get("OperationStatus"))
	type result struct{}
	type response struct {
		XMLName   xml.Name `xml:"RecordHandlerProgressResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"RecordHandlerProgressResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleGetHookResult(form url.Values, c *echo.Context) error {
	// Real GetHookResultInput's identifier field is "HookResultId", not
	// "HookResultToken" (cloudformation@v1.76.1 serializers.go:8480).
	status, err := h.Backend.GetHookResult(form.Get("HookResultId"))
	if err != nil {
		return h.xmlError(c, "HookResultNotFound", err.Error())
	}
	// Real GetHookResultOutput's status member is "Status", not "HookStatus"
	// (cloudformation@v1.76.1 deserializers.go:
	// awsAwsquery_deserializeOpDocumentGetHookResultOutput).
	type result struct {
		Status string `xml:"Status"`
	}
	type response struct {
		XMLName   xml.Name `xml:"GetHookResultResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"GetHookResultResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{Xmlns: cfnNS, Result: result{Status: status}, RequestID: uuid.New().String()},
	)
}

func (h *Handler) handleListHookResults(form url.Values, c *echo.Context) error {
	results, _ := h.Backend.ListHookResults(form.Get("HookResultToken"), form.Get("NextToken"))
	// Real HookResultSummary members are "Status" and "HookStatusReason", not
	// "HookStatus"/"ErrorCode" (cloudformation@v1.76.1 types/types.go:422).
	type hookXML struct {
		Status           string `xml:"Status,omitempty"`
		HookStatusReason string `xml:"HookStatusReason,omitempty"`
	}
	members := make([]hookXML, 0, len(results))
	for _, r := range results {
		members = append(members, hookXML{Status: r.HookStatus, HookStatusReason: r.ErrorCode})
	}
	type result struct {
		HookResults []hookXML `xml:"HookResults>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ListHookResultsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ListHookResultsResult"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{HookResults: members},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleDescribeChangeSetHooks(form url.Values, c *echo.Context) error {
	hooks, _ := h.Backend.DescribeChangeSetHooks(form.Get("StackName"), form.Get("ChangeSetName"))
	type result struct {
		Hooks []ChangeSetHook `xml:"Hooks>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"DescribeChangeSetHooksResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"DescribeChangeSetHooksResult"`
	}

	return writeXML(
		c,
		response{Xmlns: cfnNS, Result: result{Hooks: hooks}, RequestID: uuid.New().String()},
	)
}
