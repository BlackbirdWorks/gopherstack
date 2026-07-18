package codepipeline

import (
	"context"
	"fmt"
)

// maxResultsCapRuleExecutions is the per-operation pagination cap for ListRuleExecutions.
const maxResultsCapRuleExecutions int32 = 100

type listRuleExecutionsInput struct {
	PipelineName string `json:"pipelineName"`
	NextToken    string `json:"nextToken"`
	MaxResults   int32  `json:"maxResults"`
}

type listRuleExecutionsOutput struct {
	NextToken            string           `json:"nextToken,omitempty"`
	RuleExecutionDetails []map[string]any `json:"ruleExecutionDetails"`
}

func (h *Handler) handleListRuleExecutions(
	ctx context.Context,
	in *listRuleExecutionsInput,
) (*listRuleExecutionsOutput, error) {
	if in.PipelineName == "" {
		return nil, fmt.Errorf("%w: pipelineName is required", errInvalidRequest)
	}

	items, err := h.Backend.ListRuleExecutions(ctx, in.PipelineName)
	if err != nil {
		return nil, err
	}

	page, nextToken, pErr := cpPaginate(
		items,
		in.NextToken,
		in.MaxResults,
		maxResultsCapRuleExecutions,
	)
	if pErr != nil {
		return nil, pErr
	}

	return &listRuleExecutionsOutput{NextToken: nextToken, RuleExecutionDetails: page}, nil
}

type listRuleTypesInput struct {
	RegionFilter string `json:"regionFilter"`
}

type listRuleTypesOutput struct {
	RuleTypes []map[string]any `json:"ruleTypes"`
}

func (h *Handler) handleListRuleTypes(
	_ context.Context,
	_ *listRuleTypesInput,
) (*listRuleTypesOutput, error) {
	return &listRuleTypesOutput{RuleTypes: h.Backend.ListRuleTypes()}, nil
}
