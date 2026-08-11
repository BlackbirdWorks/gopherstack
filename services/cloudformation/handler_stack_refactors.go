package cloudformation

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/url"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// dispatchStackRefactorOps handles stack refactor operations.
func (h *Handler) dispatchStackRefactorOps(
	action string,
	form url.Values,
	c *echo.Context,
) (bool, error) {
	switch action {
	case "CreateStackRefactor":
		return true, h.handleCreateStackRefactor(form, c)
	case "DescribeStackRefactor":
		return true, h.handleDescribeStackRefactor(form, c)
	case "ExecuteStackRefactor":
		return true, h.handleExecuteStackRefactor(form, c)
	case "ListStackRefactors":
		return true, h.handleListStackRefactors(form, c)
	case "ListStackRefactorActions":
		return true, h.handleListStackRefactorActions(form, c)
	}

	return false, nil
}

// parseResourceMappings parses the nested ResourceMappings list (verified
// against serializers.go:awsAwsquery_serializeDocumentResourceMapping —
// each member has Source.{StackName,LogicalResourceId} and a matching
// Destination).
func parseResourceMappings(form url.Values, prefix string) []ResourceMapping {
	var result []ResourceMapping
	for i := 1; ; i++ {
		p := fmt.Sprintf("%s%d.", prefix, i)
		srcStack := form.Get(p + "Source.StackName")
		srcLogical := form.Get(p + "Source.LogicalResourceId")
		dstStack := form.Get(p + "Destination.StackName")
		dstLogical := form.Get(p + "Destination.LogicalResourceId")
		if srcStack == "" && dstStack == "" {
			return result
		}
		result = append(result, ResourceMapping{
			Source:      ResourceLocation{StackName: srcStack, LogicalResourceID: srcLogical},
			Destination: ResourceLocation{StackName: dstStack, LogicalResourceID: dstLogical},
		})
	}
}

func (h *Handler) handleCreateStackRefactor(form url.Values, c *echo.Context) error {
	mappings := parseResourceMappings(form, "ResourceMappings.member.")
	enableStackCreation := form.Get("EnableStackCreation") == "true"
	id, err := h.Backend.CreateStackRefactor(form.Get("Description"), mappings, enableStackCreation)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}
	type result struct {
		StackRefactorID string `xml:"StackRefactorId"`
	}
	type response struct {
		XMLName   xml.Name `xml:"CreateStackRefactorResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"CreateStackRefactorResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{Xmlns: cfnNS, Result: result{StackRefactorID: id}, RequestID: uuid.New().String()},
	)
}

func (h *Handler) handleDescribeStackRefactor(form url.Values, c *echo.Context) error {
	status, err := h.Backend.DescribeStackRefactor(form.Get("StackRefactorId"))
	if err != nil {
		return h.xmlError(c, "StackRefactorNotFoundException", err.Error())
	}
	type result struct {
		Status string `xml:"Status"`
	}
	type response struct {
		XMLName   xml.Name `xml:"DescribeStackRefactorResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"DescribeStackRefactorResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{Xmlns: cfnNS, Result: result{Status: status}, RequestID: uuid.New().String()},
	)
}

func (h *Handler) handleExecuteStackRefactor(form url.Values, c *echo.Context) error {
	if err := h.Backend.ExecuteStackRefactor(form.Get("StackRefactorId")); err != nil {
		code := "ValidationError"
		if errors.Is(err, ErrStackRefactorNotFound) {
			code = "StackRefactorNotFoundException"
		}

		return h.xmlError(c, code, err.Error())
	}
	type response struct {
		XMLName   xml.Name `xml:"ExecuteStackRefactorResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleListStackRefactors(form url.Values, c *echo.Context) error {
	summaries, _ := h.Backend.ListStackRefactors(form.Get("NextToken"))
	type result struct {
		StackRefactorSummaries []StackRefactorSummary `xml:"StackRefactorSummaries>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ListStackRefactorsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ListStackRefactorsResult"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{StackRefactorSummaries: summaries},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleListStackRefactorActions(form url.Values, c *echo.Context) error {
	actions, _ := h.Backend.ListStackRefactorActions(form.Get("StackRefactorId"))
	type result struct {
		StackRefactorActions []StackRefactorAction `xml:"StackRefactorActions>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ListStackRefactorActionsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ListStackRefactorActionsResult"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{StackRefactorActions: actions},
			RequestID: uuid.New().String(),
		},
	)
}
