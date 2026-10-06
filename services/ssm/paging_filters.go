package ssm

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	fkExecutionID   = "ExecutionId"
	fkAssociationID = "AssociationId"
	fkResourceID    = "ResourceId"
	fkResourceType  = "ResourceType"
	fkType          = "Type"
	fkKeyID         = "KeyId"
	opEqual         = "Equal"
)

var (
	ErrInvalidNextToken           = errors.New("InvalidNextToken")
	ErrInvalidFilterKey           = errors.New("InvalidFilterKey")
	ErrInvalidFilterValue         = errors.New("InvalidFilterValue")
	ErrOpsMetadataInvalidArgument = errors.New("OpsMetadataInvalidArgumentException")
	ErrOpsItemInvalidParameter    = errors.New("OpsItemInvalidParameterException")
)

// classifySSMPagingError maps the token/filter rejection sentinels to their wire codes.
func classifySSMPagingError(reqErr error) (string, int, bool) {
	for _, s := range []error{
		ErrInvalidNextToken, ErrInvalidFilterKey, ErrInvalidFilterValue,
		ErrOpsMetadataInvalidArgument, ErrOpsItemInvalidParameter,
	} {
		if errors.Is(reqErr, s) {
			return s.Error(), http.StatusBadRequest, true
		}
	}

	return "", 0, false
}

// pageChecked is paginateSlice that rejects a malformed token with invalid instead of restarting.
func pageChecked[T any](items []T, nextToken string, maxResults, defaultMax int, invalid error) ([]T, string, error) {
	if err := validateNextToken(nextToken, invalid); err != nil {
		return nil, "", err
	}

	page, next := paginateSlice(items, nextToken, maxResults, defaultMax)

	return page, next, nil
}

func validateNextToken(token string, invalid error) error {
	if token == "" {
		return nil
	}

	if n, err := strconv.Atoi(token); err != nil || n < 0 {
		return fmt.Errorf("%w: invalid NextToken", invalid)
	}

	return nil
}

// filterParameterMetadata applies ParameterFilters, then the deprecated Filters.
func filterParameterMetadata(all []ParameterMetadata, input *DescribeParametersInput) ([]ParameterMetadata, error) {
	if len(input.ParameterFilters) > 0 {
		kept := make([]ParameterMetadata, 0, len(all))

		for _, meta := range all {
			if paramMatchesFilters(meta, input.ParameterFilters) {
				kept = append(kept, meta)
			}
		}

		all = kept
	}

	if len(input.Filters) == 0 {
		return all, nil
	}

	return filterParametersLegacy(all, input.Filters)
}

func maxOrZero(v *int32) int {
	if v == nil {
		return 0
	}

	return int(*v)
}

func maxOrZero64(v *int64) int {
	if v == nil {
		return 0
	}

	return int(*v)
}

// timeFilterValue parses an RFC3339 or epoch-seconds filter value into epoch seconds.
func timeFilterValue(v string) (float64, bool) {
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return float64(t.Unix()), true
	}

	f, err := strconv.ParseFloat(v, 64)

	return f, err == nil
}

// AssociationExecutionFilter is types.AssociationExecutionFilter.
type AssociationExecutionFilter struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
	Type  string `json:"Type"`
}

// AssociationExecutionTargetsFilter is types.AssociationExecutionTargetsFilter.
type AssociationExecutionTargetsFilter struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// StepExecutionFilter is types.StepExecutionFilter.
type StepExecutionFilter struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

// DocumentFilterListItem is types.DocumentFilter (single Value), sent as ListDocuments' DocumentFilterList.
type DocumentFilterListItem struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// ParametersFilter is the deprecated types.ParametersFilter (DescribeParameters.Filters).
type ParametersFilter struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

// OpsItemFilterKV is types.OpsItemEventFilter / types.OpsItemRelatedItemsFilter.
type OpsItemFilterKV struct {
	Key      string   `json:"Key"`
	Operator string   `json:"Operator"`
	Values   []string `json:"Values"`
}

func filterAssociationExecutions(
	execs []AssociationExecution,
	filters []AssociationExecutionFilter,
) ([]AssociationExecution, error) {
	for _, f := range filters {
		if !slices.Contains([]string{fkExecutionID, filterKeyStatus, "CreatedTime"}, f.Key) {
			return nil, fmt.Errorf("%w: unsupported filter key %q", ErrValidationException, f.Key)
		}
	}

	out := make([]AssociationExecution, 0, len(execs))

	for _, e := range execs {
		keep := true

		for _, f := range filters {
			keep = keep && assocExecutionMatches(e, f)
		}

		if keep {
			out = append(out, e)
		}
	}

	return out, nil
}

func assocExecutionMatches(e AssociationExecution, f AssociationExecutionFilter) bool {
	switch f.Key {
	case fkExecutionID:
		return e.ExecutionID == f.Value
	case filterKeyStatus:
		return e.Status == f.Value
	default:
		v, ok := timeFilterValue(f.Value)
		if !ok {
			return false
		}

		switch f.Type {
		case "LESS_THAN":
			return e.ExecutionDate < v
		case "GREATER_THAN":
			return e.ExecutionDate > v
		default:
			return int64(e.ExecutionDate) == int64(v)
		}
	}
}

func filterAssociationExecutionTargets(
	targets []AssociationExecutionTarget, filters []AssociationExecutionTargetsFilter,
) ([]AssociationExecutionTarget, error) {
	for _, f := range filters {
		if !slices.Contains([]string{filterKeyStatus, fkResourceID, fkResourceType}, f.Key) {
			return nil, fmt.Errorf("%w: unsupported filter key %q", ErrValidationException, f.Key)
		}
	}

	out := make([]AssociationExecutionTarget, 0, len(targets))

	for _, t := range targets {
		keep := true

		for _, f := range filters {
			keep = keep && assocTargetField(t, f.Key) == f.Value
		}

		if keep {
			out = append(out, t)
		}
	}

	return out, nil
}

func assocTargetField(t AssociationExecutionTarget, key string) string {
	switch key {
	case filterKeyStatus:
		return t.Status
	case fkResourceID:
		return t.ResourceID
	default:
		return t.ResourceType
	}
}

func stepExecutionFilterKeys() []string {
	return []string{"StartTimeBefore", "StartTimeAfter", "StepExecutionStatus", "StepExecutionId", "StepName", "Action"}
}

func filterStepExecutions(steps []AutomationStepExec, filters []StepExecutionFilter) ([]AutomationStepExec, error) {
	for _, f := range filters {
		if !slices.Contains(stepExecutionFilterKeys(), f.Key) {
			return nil, fmt.Errorf("%w: unsupported filter key %q", ErrInvalidFilterKey, f.Key)
		}

		if len(f.Values) == 0 {
			return nil, fmt.Errorf("%w: filter %q needs a value", ErrInvalidFilterValue, f.Key)
		}
	}

	out := make([]AutomationStepExec, 0, len(steps))

	for _, s := range steps {
		keep := true

		for _, f := range filters {
			keep = keep && stepMatches(s, f)
		}

		if keep {
			out = append(out, s)
		}
	}

	return out, nil
}

func stepMatches(s AutomationStepExec, f StepExecutionFilter) bool {
	switch f.Key {
	case "StepExecutionStatus":
		return slices.Contains(f.Values, s.StepStatus)
	case "StepExecutionId":
		return slices.Contains(f.Values, s.StepExecutionID)
	case "StepName":
		return slices.Contains(f.Values, s.StepName)
	case "Action":
		return slices.Contains(f.Values, s.Action)
	default:
		v, ok := timeFilterValue(f.Values[0])
		if !ok {
			return false
		}

		if f.Key == "StartTimeBefore" {
			return s.ExecutionStartTime <= v
		}

		return s.ExecutionStartTime >= v
	}
}

func filterMaintenanceWindows(
	wins []MaintenanceWindowIdentity,
	filters []MaintenanceWindowFilter,
) []MaintenanceWindowIdentity {
	out := make([]MaintenanceWindowIdentity, 0, len(wins))

	for _, w := range wins {
		keep := true

		for _, f := range filters {
			switch f.Key {
			case filterKeyName:
				keep = keep && slices.Contains(f.Values, w.Name)
			case "Enabled":
				keep = keep && slices.ContainsFunc(f.Values, func(v string) bool {
					return strings.EqualFold(v, strconv.FormatBool(w.Enabled))
				})
			}
		}

		if keep {
			out = append(out, w)
		}
	}

	return out
}

func parametersFilterKeys() []string { return []string{filterKeyName, fkType, fkKeyID} }

func parameterMetaField(m ParameterMetadata, key string) string {
	switch key {
	case filterKeyName:
		return m.Name
	case fkType:
		return m.Type
	default:
		return m.KeyID
	}
}

func filterParametersLegacy(all []ParameterMetadata, filters []ParametersFilter) ([]ParameterMetadata, error) {
	for _, f := range filters {
		if !slices.Contains(parametersFilterKeys(), f.Key) {
			return nil, fmt.Errorf("%w: unsupported filter key %q", ErrInvalidFilterKey, f.Key)
		}

		if len(f.Values) == 0 {
			return nil, fmt.Errorf("%w: filter %q needs a value", ErrInvalidFilterValue, f.Key)
		}
	}

	out := make([]ParameterMetadata, 0, len(all))

	for _, m := range all {
		keep := true

		for _, f := range filters {
			keep = keep && slices.Contains(f.Values, parameterMetaField(m, f.Key))
		}

		if keep {
			out = append(out, m)
		}
	}

	return out, nil
}

func filterOpsItemEvents(events []OpsItemEventSummary, filters []OpsItemFilterKV) ([]OpsItemEventSummary, error) {
	for _, f := range filters {
		if f.Key != "OpsItemId" || (f.Operator != "" && f.Operator != opEqual) {
			return nil, fmt.Errorf("%w: only OpsItemId/Equal filters are supported", ErrOpsItemInvalidParameter)
		}
	}

	out := make([]OpsItemEventSummary, 0, len(events))

	for _, e := range events {
		keep := true

		for _, f := range filters {
			keep = keep && slices.Contains(f.Values, e.OpsItemID)
		}

		if keep {
			out = append(out, e)
		}
	}

	return out, nil
}

func relatedItemField(it OpsItemRelatedItem, key string) string {
	switch key {
	case "ResourceUri":
		return it.ResourceURI
	case fkResourceType:
		return it.ResourceType
	default:
		return it.AssociationID
	}
}

func filterOpsItemRelatedItems(items []OpsItemRelatedItem, filters []OpsItemFilterKV) ([]OpsItemRelatedItem, error) {
	for _, f := range filters {
		if !slices.Contains([]string{"ResourceUri", fkResourceType, fkAssociationID}, f.Key) ||
			(f.Operator != "" && f.Operator != opEqual && f.Operator != "EQUAL") {
			return nil, fmt.Errorf("%w: unsupported filter %q/%q", ErrOpsItemInvalidParameter, f.Key, f.Operator)
		}
	}

	out := make([]OpsItemRelatedItem, 0, len(items))

	for _, it := range items {
		keep := true

		for _, f := range filters {
			keep = keep && slices.Contains(f.Values, relatedItemField(it, f.Key))
		}

		if keep {
			out = append(out, it)
		}
	}

	return out, nil
}
