package cloudformation

import (
	"encoding/xml"
	"errors"
	"net/url"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

func (h *Handler) dispatchChangeSetOps(action string, form url.Values, c *echo.Context) (bool, error) {
	switch action {
	case "CreateChangeSet":

		return true, h.handleCreateChangeSet(form, c)
	case "DescribeChangeSet":

		return true, h.handleDescribeChangeSet(form, c)
	case "ExecuteChangeSet":

		return true, h.handleExecuteChangeSet(form, c)
	case "DeleteChangeSet":

		return true, h.handleDeleteChangeSet(form, c)
	case "ListChangeSets":

		return true, h.handleListChangeSets(form, c)
	default:

		return false, nil
	}
}

func (h *Handler) handleCreateChangeSet(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	changeSetName := form.Get("ChangeSetName")
	if stackName == "" || changeSetName == "" {
		return h.xmlError(c, "ValidationError", "StackName and ChangeSetName are required")
	}

	templateBody := form.Get("TemplateBody")
	description := form.Get("Description")
	params := parseParams(form)
	capabilities := parseCapabilities(form)

	cs, err := h.Backend.CreateChangeSet(
		c.Request().Context(), stackName, changeSetName, templateBody, description, params, capabilities,
		parseTags(form),
	)
	if err != nil {
		return h.xmlError(c, "AlreadyExistsException", err.Error())
	}

	type result struct {
		ChangeSetID string `xml:"Id"`
		StackID     string `xml:"StackId"`
	}
	type response struct {
		XMLName   xml.Name `xml:"CreateChangeSetResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"CreateChangeSetResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    result{ChangeSetID: cs.ChangeSetID, StackID: cs.StackID},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleExecuteChangeSet(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	changeSetName := form.Get("ChangeSetName")

	if err := h.Backend.ExecuteChangeSet(c.Request().Context(), stackName, changeSetName); err != nil {
		if errors.Is(err, ErrChangeSetNotExecutable) {
			return h.xmlError(c, "InvalidChangeSetStatus", err.Error())
		}

		return h.xmlError(c, "ChangeSetNotFound", err.Error())
	}

	type result struct{}
	type response struct {
		XMLName   xml.Name `xml:"ExecuteChangeSetResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"ExecuteChangeSetResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDeleteChangeSet(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	changeSetName := form.Get("ChangeSetName")

	if err := h.Backend.DeleteChangeSet(stackName, changeSetName); err != nil {
		return h.xmlError(c, "ChangeSetNotFound", err.Error())
	}

	type result struct{}
	type response struct {
		XMLName   xml.Name `xml:"DeleteChangeSetResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"DeleteChangeSetResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleListChangeSets(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	nextToken := form.Get("NextToken")

	p, err := h.Backend.ListChangeSets(stackName, nextToken)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}
	summaries := p.Data

	type summaryXML struct {
		ChangeSetID   string `xml:"ChangeSetId"`
		ChangeSetName string `xml:"ChangeSetName"`
		StackID       string `xml:"StackId"`
		StackName     string `xml:"StackName"`
		Status        string `xml:"Status"`
		CreationTime  string `xml:"CreationTime"`
		Description   string `xml:"Description,omitempty"`
	}
	members := make([]summaryXML, 0, len(summaries))
	for _, s := range summaries {
		members = append(members, summaryXML{
			ChangeSetID:   s.ChangeSetID,
			ChangeSetName: s.ChangeSetName,
			StackID:       s.StackID,
			StackName:     s.StackName,
			Status:        s.Status,
			CreationTime:  s.CreationTime.UTC().Format("2006-01-02T15:04:05Z"),
			Description:   s.Description,
		})
	}

	type listResult struct {
		NextToken string       `xml:"NextToken,omitempty"`
		Summaries []summaryXML `xml:"Summaries>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"ListChangeSetsResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    listResult `xml:"ListChangeSetsResult"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    listResult{Summaries: members, NextToken: p.Next},
		RequestID: uuid.New().String(),
	})
}

// handleDescribeChangeSet returns the full DescribeChangeSet response including
// ExecutionStatus and ChangeSetType fields.
func (h *Handler) handleDescribeChangeSet(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	changeSetName := form.Get("ChangeSetName")

	cs, err := h.Backend.DescribeChangeSet(stackName, changeSetName)
	if err != nil {
		return h.xmlError(c, "ChangeSetNotFound", err.Error())
	}

	type targetXML struct {
		Attribute          string `xml:"Attribute,omitempty"`
		Name               string `xml:"Name,omitempty"`
		RequiresRecreation string `xml:"RequiresRecreation,omitempty"`
	}
	type detailXML struct {
		Target       *targetXML `xml:"Target,omitempty"`
		Evaluation   string     `xml:"Evaluation,omitempty"`
		ChangeSource string     `xml:"ChangeSource,omitempty"`
	}
	type resourceChangeXML struct {
		Action       string      `xml:"Action"`
		LogicalID    string      `xml:"LogicalResourceId"`
		PhysicalID   string      `xml:"PhysicalResourceId,omitempty"`
		ResourceType string      `xml:"ResourceType"`
		Replacement  string      `xml:"Replacement,omitempty"`
		Scope        []string    `xml:"Scope>member,omitempty"`
		Details      []detailXML `xml:"Details>member,omitempty"`
	}
	type changeXML struct {
		Type           string            `xml:"Type"`
		ResourceChange resourceChangeXML `xml:"ResourceChange"`
	}
	changes := make([]changeXML, 0, len(cs.Changes))
	for _, ch := range cs.Changes {
		details := make([]detailXML, 0, len(ch.ResourceChange.Details))
		for _, d := range ch.ResourceChange.Details {
			dx := detailXML{Evaluation: d.Evaluation, ChangeSource: d.ChangeSource}
			if d.Target != nil {
				dx.Target = &targetXML{
					Attribute:          d.Target.Attribute,
					Name:               d.Target.Name,
					RequiresRecreation: d.Target.RequiresRecreation,
				}
			}
			details = append(details, dx)
		}
		changes = append(changes, changeXML{
			Type: ch.Type,
			ResourceChange: resourceChangeXML{
				Action:       ch.ResourceChange.Action,
				LogicalID:    ch.ResourceChange.LogicalID,
				PhysicalID:   ch.ResourceChange.PhysicalID,
				ResourceType: ch.ResourceChange.ResourceType,
				Replacement:  ch.ResourceChange.Replacement,
				Scope:        ch.ResourceChange.Scope,
				Details:      details,
			},
		})
	}

	type descResult struct {
		ChangeSetID     string      `xml:"ChangeSetId"`
		ChangeSetName   string      `xml:"ChangeSetName"`
		StackID         string      `xml:"StackId"`
		StackName       string      `xml:"StackName"`
		Status          string      `xml:"Status"`
		StatusReason    string      `xml:"StatusReason,omitempty"`
		ExecutionStatus string      `xml:"ExecutionStatus,omitempty"`
		ChangeSetType   string      `xml:"ChangeSetType,omitempty"`
		CreationTime    string      `xml:"CreationTime"`
		Description     string      `xml:"Description,omitempty"`
		Capabilities    []string    `xml:"Capabilities>member,omitempty"`
		Tags            []Tag       `xml:"Tags>member,omitempty"`
		Changes         []changeXML `xml:"Changes>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"DescribeChangeSetResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    descResult `xml:"DescribeChangeSetResult"`
	}

	return writeXML(c, response{
		Xmlns: cfnNS,
		Result: descResult{
			ChangeSetID:     cs.ChangeSetID,
			ChangeSetName:   cs.ChangeSetName,
			StackID:         cs.StackID,
			StackName:       cs.StackName,
			Status:          cs.Status,
			StatusReason:    cs.StatusReason,
			ExecutionStatus: cs.ExecutionStatus,
			ChangeSetType:   cs.ChangeSetType,
			CreationTime:    cs.CreationTime.UTC().Format("2006-01-02T15:04:05Z"),
			Description:     cs.Description,
			Capabilities:    cs.Capabilities,
			Tags:            cs.Tags,
			Changes:         changes,
		},
		RequestID: uuid.New().String(),
	})
}
