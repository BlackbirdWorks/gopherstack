package ce

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type createCostCategoryDefinitionInput struct {
	Name             string             `json:"Name"`
	RuleVersion      string             `json:"RuleVersion"`
	DefaultValue     string             `json:"DefaultValue"`
	EffectiveStart   string             `json:"EffectiveStart"`
	Rules            []costCategoryRule `json:"Rules"`
	SplitChargeRules []splitChargeRule  `json:"SplitChargeRules"`
	ResourceTags     []resourceTag      `json:"ResourceTags"`
}

type costCategoryRule struct {
	Value string `json:"Value"`
}

type splitChargeRule struct {
	Source  string   `json:"Source"`
	Method  string   `json:"Method"`
	Targets []string `json:"Targets"`
}

type createCostCategoryDefinitionOutput struct {
	CostCategoryArn string `json:"CostCategoryArn"`
	EffectiveStart  string `json:"EffectiveStart"`
}

func (h *Handler) handleCreateCostCategoryDefinition(
	_ context.Context,
	in *createCostCategoryDefinitionInput,
) (*createCostCategoryDefinitionOutput, error) {
	if in.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	rules := make([]CostCategoryRule, 0, len(in.Rules))
	for _, r := range in.Rules {
		rules = append(rules, CostCategoryRule(r))
	}

	cat, err := h.Backend.CreateCostCategoryDefinition(
		in.Name, in.RuleVersion, in.DefaultValue,
		rules, resourceTagsToMap(in.ResourceTags),
	)
	if err != nil {
		return nil, err
	}

	return &createCostCategoryDefinitionOutput{
		CostCategoryArn: cat.ARN,
		EffectiveStart:  cat.EffectiveStart,
	}, nil
}

type deleteCostCategoryDefinitionInput struct {
	CostCategoryArn string `json:"CostCategoryArn"`
}

type deleteCostCategoryDefinitionOutput struct {
	CostCategoryArn string `json:"CostCategoryArn"`
	EffectiveEnd    string `json:"EffectiveEnd"`
}

func (h *Handler) handleDeleteCostCategoryDefinition(
	_ context.Context,
	in *deleteCostCategoryDefinitionInput,
) (*deleteCostCategoryDefinitionOutput, error) {
	if in.CostCategoryArn == "" {
		return nil, fmt.Errorf("%w: CostCategoryArn is required", errInvalidRequest)
	}

	cat, err := h.Backend.DeleteCostCategoryDefinition(in.CostCategoryArn)
	if err != nil {
		return nil, err
	}

	return &deleteCostCategoryDefinitionOutput{
		CostCategoryArn: cat.ARN,
		EffectiveEnd:    effectiveStart(),
	}, nil
}

type describeCostCategoryDefinitionInput struct {
	CostCategoryArn string `json:"CostCategoryArn"`
	EffectiveOn     string `json:"EffectiveOn"`
}

type costCategoryProcessingStatus struct {
	Component string `json:"Component"`
	Status    string `json:"Status"`
}

type costCategorySummary struct {
	CostCategoryArn  string                         `json:"CostCategoryArn"`
	Name             string                         `json:"Name"`
	RuleVersion      string                         `json:"RuleVersion"`
	DefaultValue     string                         `json:"DefaultValue"`
	EffectiveStart   string                         `json:"EffectiveStart"`
	EffectiveEnd     string                         `json:"EffectiveEnd,omitempty"`
	ProcessingStatus []costCategoryProcessingStatus `json:"ProcessingStatus,omitempty"`
	Rules            []costCategoryRule             `json:"Rules"`
}

type describeCostCategoryDefinitionOutput struct {
	CostCategory costCategorySummary `json:"CostCategory"`
}

func (h *Handler) handleDescribeCostCategoryDefinition(
	_ context.Context,
	in *describeCostCategoryDefinitionInput,
) (*describeCostCategoryDefinitionOutput, error) {
	if in.CostCategoryArn == "" {
		return nil, fmt.Errorf("%w: CostCategoryArn is required", errInvalidRequest)
	}

	cat, err := h.Backend.DescribeCostCategoryDefinition(in.CostCategoryArn)
	if err != nil {
		return nil, err
	}

	rules := make([]costCategoryRule, len(cat.Rules))
	for i, r := range cat.Rules {
		rules[i] = costCategoryRule(r)
	}

	return &describeCostCategoryDefinitionOutput{
		CostCategory: costCategorySummary{
			CostCategoryArn: cat.ARN,
			Name:            cat.Name,
			RuleVersion:     cat.RuleVersion,
			DefaultValue:    cat.DefaultValue,
			EffectiveStart:  cat.EffectiveStart,
			ProcessingStatus: []costCategoryProcessingStatus{
				{Component: "COST_EXPLORER", Status: "APPLIED"},
			},
			Rules: rules,
		},
	}, nil
}

type listCostCategoryDefinitionsInput struct {
	NextToken   string `json:"NextToken"`
	EffectiveOn string `json:"EffectiveOn"`
	MaxResults  int    `json:"MaxResults"`
}

type costCategoryReference struct {
	CostCategoryArn string `json:"CostCategoryArn"`
	Name            string `json:"Name"`
	EffectiveStart  string `json:"EffectiveStart"`
}

type listCostCategoryDefinitionsOutput struct {
	NextPageToken          string                  `json:"NextPageToken,omitempty"`
	CostCategoryReferences []costCategoryReference `json:"CostCategoryReferences"`
}

func (h *Handler) handleListCostCategoryDefinitions(
	_ context.Context,
	in *listCostCategoryDefinitionsInput,
) (*listCostCategoryDefinitionsOutput, error) {
	cats, nextToken := h.Backend.ListCostCategoryDefinitions(in.MaxResults, in.NextToken)
	refs := make([]costCategoryReference, 0, len(cats))

	for _, cat := range cats {
		refs = append(refs, costCategoryReference{
			CostCategoryArn: cat.ARN,
			Name:            cat.Name,
			EffectiveStart:  cat.EffectiveStart,
		})
	}

	return &listCostCategoryDefinitionsOutput{CostCategoryReferences: refs, NextPageToken: nextToken}, nil
}

type updateCostCategoryDefinitionInput struct {
	CostCategoryArn  string             `json:"CostCategoryArn"`
	RuleVersion      string             `json:"RuleVersion"`
	DefaultValue     string             `json:"DefaultValue"`
	Rules            []costCategoryRule `json:"Rules"`
	SplitChargeRules []splitChargeRule  `json:"SplitChargeRules"`
}

type updateCostCategoryDefinitionOutput struct {
	CostCategoryArn string `json:"CostCategoryArn"`
	EffectiveStart  string `json:"EffectiveStart"`
}

func (h *Handler) handleUpdateCostCategoryDefinition(
	_ context.Context,
	in *updateCostCategoryDefinitionInput,
) (*updateCostCategoryDefinitionOutput, error) {
	if in.CostCategoryArn == "" {
		return nil, fmt.Errorf("%w: CostCategoryArn is required", errInvalidRequest)
	}

	rules := make([]CostCategoryRule, 0, len(in.Rules))
	for _, r := range in.Rules {
		rules = append(rules, CostCategoryRule(r))
	}

	splitChargeRules := make([]SplitChargeRule, 0, len(in.SplitChargeRules))
	for _, r := range in.SplitChargeRules {
		splitChargeRules = append(splitChargeRules, SplitChargeRule(r))
	}

	cat, err := h.Backend.UpdateCostCategoryDefinition(
		in.CostCategoryArn, in.RuleVersion, in.DefaultValue,
		rules, splitChargeRules,
	)
	if err != nil {
		return nil, err
	}

	return &updateCostCategoryDefinitionOutput{
		CostCategoryArn: cat.ARN,
		EffectiveStart:  cat.EffectiveStart,
	}, nil
}

type getCostCategoriesInput struct {
	TimePeriod       map[string]string `json:"TimePeriod"`
	CostCategoryName string            `json:"CostCategoryName"`
	SearchString     string            `json:"SearchString"`
	NextPageToken    string            `json:"NextPageToken"`
	MaxResults       int               `json:"MaxResults"`
}

type getCostCategoriesOutput struct {
	NextPageToken      string   `json:"NextPageToken,omitempty"`
	CostCategoryValues []string `json:"CostCategoryValues"`
	ReturnSize         int      `json:"ReturnSize"`
	TotalSize          int      `json:"TotalSize"`
}

func (h *Handler) handleGetCostCategories(
	_ context.Context,
	in *getCostCategoriesInput,
) (*getCostCategoriesOutput, error) {
	values := h.Backend.GetCostCategories(in.CostCategoryName)

	return &getCostCategoriesOutput{
		CostCategoryValues: values,
		ReturnSize:         len(values),
		TotalSize:          len(values),
	}, nil
}

type listCostCategoryResourceAssociationsInput struct {
	CostCategoryArn   string `json:"CostCategoryArn"`
	NextToken         string `json:"NextToken"`
	ResourceTagFilter []any  `json:"ResourceTagFilter"`
}

type listCostCategoryResourceAssociationsOutput struct {
	CostCategoryReference any    `json:"CostCategoryReference,omitempty"`
	NextToken             string `json:"NextToken,omitempty"`
	ResourceTagsCount     int    `json:"ResourceTagsCount"`
}

func (h *Handler) handleListCostCategoryResourceAssociations(
	_ context.Context,
	_ *listCostCategoryResourceAssociationsInput,
) (*listCostCategoryResourceAssociationsOutput, error) {
	return &listCostCategoryResourceAssociationsOutput{
		ResourceTagsCount: 0,
	}, nil
}

// buildCostCategoryOps returns the cost-category-family op dispatch entries.
func (h *Handler) buildCostCategoryOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateCostCategoryDefinition": service.WrapOp(
			h.handleCreateCostCategoryDefinition,
		),
		"DeleteCostCategoryDefinition": service.WrapOp(
			h.handleDeleteCostCategoryDefinition,
		),
		"DescribeCostCategoryDefinition": service.WrapOp(
			h.handleDescribeCostCategoryDefinition,
		),
		"ListCostCategoryDefinitions": service.WrapOp(
			h.handleListCostCategoryDefinitions,
		),
		"UpdateCostCategoryDefinition": service.WrapOp(
			h.handleUpdateCostCategoryDefinition,
		),
		"GetCostCategories": service.WrapOp(
			h.handleGetCostCategories,
		),
		"ListCostCategoryResourceAssociations": service.WrapOp(
			h.handleListCostCategoryResourceAssociations,
		),
	}
}
