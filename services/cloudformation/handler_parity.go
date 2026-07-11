package cloudformation

import (
	"encoding/xml"
	"net/url"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// describeTypeFromRegistry returns a DescribeType XML response from the backend registry.
// Returns (true, nil) if the type was found in the registry, (false, nil) if not found,
// or (true, err) if an error occurred during XML serialization.
func (h *Handler) describeTypeFromRegistry(form url.Values, c *echo.Context) (bool, error) {
	details, err := h.Backend.DescribeType(form.Get("TypeName"), form.Get("Arn"), form.Get("VersionId"))
	if err == nil {
		type typeDetailXML struct {
			TypeName         string `xml:"TypeName,omitempty"`
			TypeArn          string `xml:"Arn,omitempty"`
			Type             string `xml:"Type,omitempty"`
			Visibility       string `xml:"Visibility,omitempty"`
			Status           string `xml:"TypeVersionStatus,omitempty"`
			Description      string `xml:"Description,omitempty"`
			Schema           string `xml:"Schema,omitempty"`
			VersionID        string `xml:"VersionId,omitempty"`
			DefaultVersionID string `xml:"DefaultVersionId,omitempty"`
			DeprecatedStatus string `xml:"DeprecatedStatus,omitempty"`
			IsActivated      bool   `xml:"IsActivated"`
			IsDefaultVersion bool   `xml:"IsDefaultVersion"`
		}
		type response struct {
			XMLName   xml.Name      `xml:"DescribeTypeResponse"`
			Xmlns     string        `xml:"xmlns,attr"`
			RequestID string        `xml:"ResponseMetadata>RequestId"`
			Result    typeDetailXML `xml:"DescribeTypeResult"`
		}

		return true, writeXML(c, response{
			Xmlns: cfnNS,
			Result: typeDetailXML{
				TypeName:         details.TypeName,
				TypeArn:          details.TypeArn,
				Type:             details.Type,
				Visibility:       details.Visibility,
				Status:           details.Status,
				Description:      details.Description,
				Schema:           details.Schema,
				VersionID:        details.VersionID,
				DefaultVersionID: details.DefaultVersionID,
				IsActivated:      details.IsActivated,
				IsDefaultVersion: details.IsDefaultVersion,
				DeprecatedStatus: details.DeprecatedStatus,
			},
			RequestID: uuid.New().String(),
		})
	}

	return false, nil // not in registry — fall through to schema-based handler
}

// handleDescribeChangeSet overrides the base handler to include ExecutionStatus and ChangeSetType.
// This replaces the handler in handler.go via a separate dispatch registration.
// Note: the actual handler in handler.go already calls h.Backend.DescribeChangeSet — this file
// adds the improved XML output with the extra fields by adding a registration below.

// handleListStackSetOperationResults returns per-account/region operation results.
func (h *Handler) handleListStackSetOperationResultsParity(form url.Values, c *echo.Context) error {
	stackSetName := form.Get("StackSetName")
	operationID := form.Get("OperationId")

	if stackSetName == "" || operationID == "" {
		return h.xmlError(c, "ValidationError", "StackSetName and OperationId are required")
	}

	results, err := h.Backend.ListStackSetOperationResults(stackSetName, operationID, "")
	if err != nil {
		return h.xmlError(c, "OperationNotFoundException", err.Error())
	}

	type accountGateXML struct {
		FunctionArn string `xml:"FunctionArn,omitempty"`
		Status      string `xml:"Status,omitempty"`
	}
	type resultXML struct {
		AccountGateResult *accountGateXML `xml:"AccountGateResult,omitempty"`
		Account           string          `xml:"Account,omitempty"`
		Region            string          `xml:"Region,omitempty"`
		Status            string          `xml:"Status,omitempty"`
		StatusReason      string          `xml:"StatusReason,omitempty"`
	}
	items := make([]resultXML, 0, len(results))
	for _, r := range results {
		item := resultXML{
			Account:      r.Account,
			Region:       r.Region,
			Status:       r.Status,
			StatusReason: r.StatusReason,
		}
		if r.AccountGateResult != nil {
			item.AccountGateResult = &accountGateXML{
				FunctionArn: r.AccountGateResult.FunctionArn,
				Status:      r.AccountGateResult.Status,
			}
		}
		items = append(items, item)
	}

	type response struct {
		XMLName   xml.Name `xml:"ListStackSetOperationResultsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    any      `xml:"ListStackSetOperationResultsResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}
	type resultWrapper struct {
		Summaries []resultXML `xml:"Summaries>member"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    resultWrapper{Summaries: items},
		RequestID: uuid.New().String(),
	})
}

// handleDescribeChangeSetFull returns the full DescribeChangeSet response including
// ExecutionStatus and ChangeSetType fields.
func (h *Handler) handleDescribeChangeSetFull(form url.Values, c *echo.Context) error {
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
			CreationTime:    cs.CreationTime.Format("2006-01-02T15:04:05Z"),
			Description:     cs.Description,
			Changes:         changes,
		},
		RequestID: uuid.New().String(),
	})
}
