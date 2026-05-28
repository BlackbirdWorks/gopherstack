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
	typeName := form.Get("TypeName")
	arnVal := form.Get("Arn")
	versionID := form.Get("VersionId")

	details, err := h.Backend.DescribeType(typeName, arnVal, versionID)
	if err != nil {
		return false, nil // not in registry — fall through to schema-based handler
	}

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
		IsActivated      bool   `xml:"IsActivated"`
		IsDefaultVersion bool   `xml:"IsDefaultVersion"`
		DeprecatedStatus string `xml:"DeprecatedStatus,omitempty"`
	}
	type response struct {
		XMLName   xml.Name      `xml:"DescribeTypeResponse"`
		Xmlns     string        `xml:"xmlns,attr"`
		Result    typeDetailXML `xml:"DescribeTypeResult"`
		RequestID string        `xml:"ResponseMetadata>RequestId"`
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
		Account           string          `xml:"Account,omitempty"`
		Region            string          `xml:"Region,omitempty"`
		Status            string          `xml:"Status,omitempty"`
		StatusReason      string          `xml:"StatusReason,omitempty"`
		AccountGateResult *accountGateXML `xml:"AccountGateResult,omitempty"`
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
		XMLName   xml.Name    `xml:"ListStackSetOperationResultsResponse"`
		Xmlns     string      `xml:"xmlns,attr"`
		Result    interface{} `xml:"ListStackSetOperationResultsResult"`
		RequestID string      `xml:"ResponseMetadata>RequestId"`
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
		return h.xmlError(c, "ChangeSetNotFoundException", err.Error())
	}

	type resourceChangeXML struct {
		Action       string `xml:"Action"`
		LogicalID    string `xml:"LogicalResourceId"`
		ResourceType string `xml:"ResourceType"`
	}
	type changeXML struct {
		Type           string            `xml:"Type"`
		ResourceChange resourceChangeXML `xml:"ResourceChange"`
	}
	changes := make([]changeXML, 0, len(cs.Changes))
	for _, ch := range cs.Changes {
		changes = append(changes, changeXML{
			Type: ch.Type,
			ResourceChange: resourceChangeXML{
				Action:       ch.ResourceChange.Action,
				LogicalID:    ch.ResourceChange.LogicalID,
				ResourceType: ch.ResourceChange.ResourceType,
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
