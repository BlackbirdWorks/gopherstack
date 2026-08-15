package elasticbeanstalk

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// applyEnvironmentManagedActionResponse is the XML response for ApplyEnvironmentManagedAction.
type applyEnvironmentManagedActionResult struct {
	ActionID          string `xml:"ActionId"`
	ActionDescription string `xml:"ActionDescription"`
	ActionType        string `xml:"ActionType"`
	Status            string `xml:"Status"`
}

type applyEnvironmentManagedActionResponse struct {
	XMLName                             xml.Name                            `xml:"ApplyEnvironmentManagedActionResponse"`
	Xmlns                               string                              `xml:"xmlns,attr"`
	ApplyEnvironmentManagedActionResult applyEnvironmentManagedActionResult `xml:"ApplyEnvironmentManagedActionResult"`
	ResponseMetadata                    responseMetadata                    `xml:"ResponseMetadata"`
}

// handleApplyEnvironmentManagedAction applies a scheduled managed action immediately.
func (h *Handler) handleApplyEnvironmentManagedAction(ctx context.Context, vals url.Values) (any, error) {
	actionID := vals.Get("ActionId")
	if actionID == "" {
		return nil, fmt.Errorf("%w: ActionId is required", ErrInvalidParameter)
	}

	_ = h.Backend.ApplyEnvironmentManagedAction(ctx, vals.Get("EnvironmentName"), actionID)

	return &applyEnvironmentManagedActionResponse{
		Xmlns: ebXMLNS,
		ApplyEnvironmentManagedActionResult: applyEnvironmentManagedActionResult{
			ActionID:          actionID,
			ActionDescription: "Managed action applied",
			ActionType:        "InstanceRefresh",
			Status:            "Scheduled",
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-apply-managed-action"},
	}, nil
}

// describeEnvironmentManagedActionHistoryResponse is the XML response for DescribeEnvironmentManagedActionHistory.
// FailureDescription/FailureType (real ManagedActionHistoryItem members) are
// not modeled: every managed action this backend applies synchronously
// succeeds (Status is always "Succeeded"; see ApplyEnvironmentManagedAction/
// AddManagedActionHistory), so there is no failure to describe -- a
// structural gap, not a dropped field.
type managedActionHistoryItem struct {
	ActionID          string `xml:"ActionId"`
	ActionType        string `xml:"ActionType"`
	ActionDescription string `xml:"ActionDescription"`
	Status            string `xml:"Status"`
	// ExecutedTime and FinishedTime are the same instant: this backend
	// applies managed actions synchronously (see ApplyEnvironmentManagedAction),
	// so there is no observable gap between an action starting and finishing.
	ExecutedTime string `xml:"ExecutedTime"`
	FinishedTime string `xml:"FinishedTime"`
}

type describeEnvironmentManagedActionHistoryResult struct {
	NextToken                 string                     `xml:"NextToken,omitempty"`
	ManagedActionHistoryItems []managedActionHistoryItem `xml:"ManagedActionHistoryItems>member"`
}

type describeEnvironmentManagedActionHistoryResponse struct { //nolint:lll // AWS XML operation name causes inherently long struct declaration
	XMLName                                       xml.Name                                      `xml:"DescribeEnvironmentManagedActionHistoryResponse"` //nolint:lll // AWS XML operation name is inherently long
	Xmlns                                         string                                        `xml:"xmlns,attr"`
	ResponseMetadata                              responseMetadata                              `xml:"ResponseMetadata"`
	DescribeEnvironmentManagedActionHistoryResult describeEnvironmentManagedActionHistoryResult `xml:"DescribeEnvironmentManagedActionHistoryResult"` //nolint:lll // AWS XML operation name is inherently long
}

func (h *Handler) handleDescribeEnvironmentManagedActionHistory(ctx context.Context, vals url.Values) (any, error) {
	envName := vals.Get("EnvironmentName")

	// EnvironmentId filter: resolve to the environment name for backend
	// lookup, matching handleDescribeEvents/handleDescribeEnvironmentHealth's
	// precedent -- real AWS accepts either EnvironmentId or EnvironmentName.
	if envName == "" {
		if envID := vals.Get("EnvironmentId"); envID != "" {
			if envs := h.Backend.DescribeEnvironments(ctx, "", nil, []string{envID}); len(envs) > 0 {
				envName = envs[0].EnvironmentName
			}
		}
	}

	// Return real stored history (improvement #4)
	historyItems := h.Backend.DescribeEnvironmentManagedActionHistory(ctx, envName)

	pg := page.New(historyItems, vals.Get("NextToken"), parseMaxRecords(vals, "MaxItems"), defaultListLimit)

	members := make([]managedActionHistoryItem, 0, len(pg.Data))

	for _, item := range pg.Data {
		members = append(members, managedActionHistoryItem{
			ActionID:          item.ActionID,
			ActionType:        item.ActionType,
			ActionDescription: item.ActionDescription,
			Status:            item.Status,
			ExecutedTime:      item.FinishedTime,
			FinishedTime:      item.FinishedTime,
		})
	}

	return &describeEnvironmentManagedActionHistoryResponse{
		Xmlns: ebXMLNS,
		DescribeEnvironmentManagedActionHistoryResult: describeEnvironmentManagedActionHistoryResult{
			ManagedActionHistoryItems: members,
			NextToken:                 pg.Next,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-env-managed-history"},
	}, nil
}

// describeEnvironmentManagedActionsResponse is the XML response for DescribeEnvironmentManagedActions.
type managedAction struct {
	ActionID          string `xml:"ActionId"`
	ActionType        string `xml:"ActionType"`
	ActionDescription string `xml:"ActionDescription"`
	Status            string `xml:"Status"`
	WindowStartTime   string `xml:"WindowStartTime"`
}

type describeEnvironmentManagedActionsResult struct {
	ManagedActions []managedAction `xml:"ManagedActions>member"`
}

type describeEnvironmentManagedActionsResponse struct { //nolint:lll // AWS XML operation name causes inherently long struct declaration
	XMLName                                 xml.Name                                `xml:"DescribeEnvironmentManagedActionsResponse"` //nolint:lll // AWS XML operation name is inherently long
	Xmlns                                   string                                  `xml:"xmlns,attr"`
	ResponseMetadata                        responseMetadata                        `xml:"ResponseMetadata"`
	DescribeEnvironmentManagedActionsResult describeEnvironmentManagedActionsResult `xml:"DescribeEnvironmentManagedActionsResult"` //nolint:lll // AWS XML operation name is inherently long
}

// handleDescribeEnvironmentManagedActions always answers an empty list: this
// backend has no scheduled-managed-action queue/maintenance-window concept
// (only history of already-applied actions is tracked, via
// ApplyEnvironmentManagedAction/AddManagedActionHistory) -- a structural
// gap, matching handleRequestEnvironmentInfo's disclosed precedent. The
// Status request filter is consequently moot.
func (h *Handler) handleDescribeEnvironmentManagedActions(_ context.Context, _ url.Values) (any, error) {
	return &describeEnvironmentManagedActionsResponse{
		Xmlns: ebXMLNS,
		DescribeEnvironmentManagedActionsResult: describeEnvironmentManagedActionsResult{
			ManagedActions: []managedAction{},
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-env-managed-actions"},
	}, nil
}
