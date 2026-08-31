package cloudformation

import (
	"encoding/xml"
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
	r, err := h.Backend.DescribeStackRefactor(form.Get("StackRefactorId"))
	if err != nil {
		return h.xmlError(c, "StackRefactorNotFoundException", err.Error())
	}
	type result struct {
		StackRefactorID string `xml:"StackRefactorId"`
		Description     string `xml:"Description,omitempty"`
		Status          string `xml:"Status"`
	}
	type response struct {
		XMLName   xml.Name `xml:"DescribeStackRefactorResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"DescribeStackRefactorResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{
			Xmlns: cfnNS,
			Result: result{
				StackRefactorID: r.RefactorID,
				Description:     r.Description,
				Status:          r.Status,
			},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleExecuteStackRefactor(form url.Values, c *echo.Context) error {
	if err := h.Backend.ExecuteStackRefactor(form.Get("StackRefactorId")); err != nil {
		// ExecuteStackRefactor's own awsAwsquery_deserializeOpError switch
		// declares no typed exceptions at all -- not StackRefactorNotFoundException
		// (that's DescribeStackRefactor's), not anything else -- so every failure,
		// not-found included, reports the generic query-protocol ValidationError
		// rather than inventing a typed code this operation cannot receive.
		return h.xmlError(c, "ValidationError", err.Error())
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

// resourceLocationXML mirrors types.ResourceLocation (types.go:1178) --
// StackName and LogicalResourceId only, no other members.
type resourceLocationXML struct {
	LogicalResourceID string `xml:"LogicalResourceId,omitempty"`
	StackName         string `xml:"StackName,omitempty"`
}

// resourceMappingXML mirrors types.ResourceMapping (types.go:1195).
type resourceMappingXML struct {
	Source      *resourceLocationXML `xml:"Source,omitempty"`
	Destination *resourceLocationXML `xml:"Destination,omitempty"`
}

// stackRefactorActionXML mirrors types.StackRefactorAction (types.go:2118).
// It has no StackName/LogicalResourceId/ResourceType members of its own --
// those live nested under ResourceMapping.Source/.Destination.
type stackRefactorActionXML struct {
	ResourceMapping    *resourceMappingXML `xml:"ResourceMapping,omitempty"`
	Action             string              `xml:"Action,omitempty"`
	Description        string              `xml:"Description,omitempty"`
	PhysicalResourceID string              `xml:"PhysicalResourceId,omitempty"`
}

func toStackRefactorActionXML(a StackRefactorAction) stackRefactorActionXML {
	return stackRefactorActionXML{
		Action:             a.Action,
		Description:        a.Description,
		PhysicalResourceID: a.PhysicalResourceID,
		ResourceMapping: &resourceMappingXML{
			Source: &resourceLocationXML{
				StackName:         a.ResourceMapping.Source.StackName,
				LogicalResourceID: a.ResourceMapping.Source.LogicalResourceID,
			},
			Destination: &resourceLocationXML{
				StackName:         a.ResourceMapping.Destination.StackName,
				LogicalResourceID: a.ResourceMapping.Destination.LogicalResourceID,
			},
		},
	}
}

func (h *Handler) handleListStackRefactorActions(form url.Values, c *echo.Context) error {
	actions, _ := h.Backend.ListStackRefactorActions(form.Get("StackRefactorId"))
	members := make([]stackRefactorActionXML, 0, len(actions))
	for _, a := range actions {
		members = append(members, toStackRefactorActionXML(a))
	}
	type result struct {
		StackRefactorActions []stackRefactorActionXML `xml:"StackRefactorActions>member"`
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
			Result:    result{StackRefactorActions: members},
			RequestID: uuid.New().String(),
		},
	)
}
