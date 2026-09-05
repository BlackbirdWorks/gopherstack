package resourcegroups

import (
	"context"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// handleGroupResources adds resources to a group.
type groupResourcesInput struct {
	Group        string   `json:"Group"`
	ResourceArns []string `json:"ResourceArns"`
}

type groupResourcesOutput struct {
	Failed    []GroupingFailedItem `json:"Failed,omitempty"`
	Pending   []GroupingFailedItem `json:"Pending,omitempty"`
	Succeeded []string             `json:"Succeeded"`
}

// isValidResourceARN reports whether s is a syntactically valid AWS ARN.
// A valid ARN starts with "arn:" and contains at least five colon separators
// (six colon-delimited segments).
func isValidResourceARN(s string) bool {
	return strings.HasPrefix(s, "arn:") && strings.Count(s, ":") >= 5
}

func (h *Handler) handleGroupResources(ctx context.Context, in *groupResourcesInput) (*groupResourcesOutput, error) {
	if in.Group == "" {
		return nil, fmt.Errorf("%w: Group is required", ErrValidation)
	}

	valid := make([]string, 0, len(in.ResourceArns))
	failed := make([]GroupingFailedItem, 0)

	for _, a := range in.ResourceArns {
		if !isValidResourceARN(a) {
			failed = append(failed, GroupingFailedItem{
				ResourceArn:  a,
				ErrorCode:    groupingErrInvalidARN,
				ErrorMessage: fmt.Sprintf("invalid ARN: %q", a),
			})

			continue
		}

		valid = append(valid, a)
	}

	succeeded, err := h.Backend.GroupResources(ctx, in.Group, valid)
	if err != nil {
		return nil, err
	}

	return &groupResourcesOutput{
		Succeeded: succeeded,
		Failed:    failed,
		Pending:   []GroupingFailedItem{},
	}, nil
}

// handleListGroupResources lists the resources associated with a group.
type listGroupResourcesInput struct { //nolint:govet // fieldalignment: readability over micro-optimization
	Filters    []ListGroupResourcesFilter `json:"Filters"`
	Group      string                     `json:"Group"`
	GroupName  string                     `json:"GroupName"`
	NextToken  string                     `json:"NextToken"`
	MaxResults int                        `json:"MaxResults"`
}

func (g *listGroupResourcesInput) resolvedName() string {
	if g.GroupName != "" {
		return g.GroupName
	}

	return g.Group
}

type listGroupResourcesItem struct {
	Identifier ResourceIdentifier `json:"Identifier"`
}

type listGroupResourcesOutput struct { //nolint:govet // fieldalignment: readability over micro-optimization
	Resources []listGroupResourcesItem `json:"Resources"`
	// ResourceIdentifiers is the deprecated predecessor of Resources ("don't
	// use this parameter, use the Resources response field instead" per the
	// real API docs). Kept populated identically to Resources for
	// backward-compatible SDK/CLI clients that still read it.
	ResourceIdentifiers []ResourceIdentifier `json:"ResourceIdentifiers"`
	QueryErrors         []queryErrorWire     `json:"QueryErrors,omitempty"`
	NextToken           string               `json:"NextToken,omitempty"`
}

func (h *Handler) handleListGroupResources(
	ctx context.Context,
	in *listGroupResourcesInput,
) (*listGroupResourcesOutput, error) {
	identifiers, nextToken, err := h.Backend.ListGroupResources(
		ctx, in.resolvedName(), in.Filters, in.NextToken, in.MaxResults,
	)
	if err != nil {
		return nil, err
	}

	items := make([]listGroupResourcesItem, 0, len(identifiers))

	for _, id := range identifiers {
		items = append(items, listGroupResourcesItem{Identifier: id})
	}

	return &listGroupResourcesOutput{
		Resources:           items,
		ResourceIdentifiers: identifiers,
		NextToken:           nextToken,
	}, nil
}

// handleListGroupingStatuses lists the grouping/ungrouping statuses for a group.
type listGroupingStatusesInput struct {
	Group      string                       `json:"Group"`
	NextToken  string                       `json:"NextToken"`
	Filters    []ListGroupingStatusesFilter `json:"Filters"`
	MaxResults int                          `json:"MaxResults"`
}

// groupingStatusItemWire is the AWS wire shape of a GroupingStatusesItem.
// UpdatedAt is serialized as a JSON number of seconds since the Unix epoch
// (the unixTimestamp format used by the rest-json protocol), not an
// RFC3339/ISO8601 string -- see pkgs/awstime.
type groupingStatusItemWire struct {
	ResourceArn  string  `json:"ResourceArn,omitempty"`
	Action       string  `json:"Action,omitempty"`
	Status       string  `json:"Status,omitempty"`
	ErrorCode    string  `json:"ErrorCode,omitempty"`
	ErrorMessage string  `json:"ErrorMessage,omitempty"`
	UpdatedAt    float64 `json:"UpdatedAt,omitempty"`
}

type listGroupingStatusesOutput struct { //nolint:govet // fieldalignment: readability over micro-optimization
	Group            string                   `json:"Group"`
	GroupingStatuses []groupingStatusItemWire `json:"GroupingStatuses"`
	NextToken        string                   `json:"NextToken,omitempty"`
}

func (h *Handler) handleListGroupingStatuses(
	ctx context.Context,
	in *listGroupingStatusesInput,
) (*listGroupingStatusesOutput, error) {
	if in.Group == "" {
		return nil, fmt.Errorf("%w: Group is required", ErrValidation)
	}

	statuses, nextToken, err := h.Backend.ListGroupingStatuses(ctx, in.Group, in.Filters, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	items := make([]groupingStatusItemWire, 0, len(statuses))
	for i := range statuses {
		items = append(items, groupingStatusItemWire{
			ResourceArn:  statuses[i].ResourceArn,
			Action:       statuses[i].Action,
			Status:       statuses[i].Status,
			ErrorCode:    statuses[i].ErrorCode,
			ErrorMessage: statuses[i].ErrorMessage,
			UpdatedAt:    awstime.Epoch(statuses[i].UpdatedAt),
		})
	}

	return &listGroupingStatusesOutput{
		Group:            in.Group,
		GroupingStatuses: items,
		NextToken:        nextToken,
	}, nil
}

// handleUngroupResources removes resources from a group.
type ungroupResourcesInput struct {
	Group        string   `json:"Group"`
	ResourceArns []string `json:"ResourceArns"`
}

type ungroupResourcesOutput struct {
	Failed    []GroupingFailedItem `json:"Failed,omitempty"`
	Pending   []GroupingFailedItem `json:"Pending,omitempty"`
	Succeeded []string             `json:"Succeeded"`
}

func (h *Handler) handleUngroupResources(
	ctx context.Context,
	in *ungroupResourcesInput,
) (*ungroupResourcesOutput, error) {
	if in.Group == "" {
		return nil, fmt.Errorf("%w: Group is required", ErrValidation)
	}

	result, err := h.Backend.UngroupResources(ctx, in.Group, in.ResourceArns)
	if err != nil {
		return nil, err
	}

	return &ungroupResourcesOutput{
		Succeeded: result.Succeeded,
		Failed:    result.Failed,
		Pending:   []GroupingFailedItem{},
	}, nil
}
