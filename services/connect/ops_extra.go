package connect

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func (h *Handler) extraOps(ops map[string]restroute.OpFunc) {
	h.ruleOps(ops)
	h.templateOps(ops)
	h.predefinedOps(ops)
	h.hoursExtraOps(ops)
	h.vocabularyOps(ops)
	h.flowVersionOps(ops)
	h.viewOps(ops)
	h.searchOps(ops)
	h.proficiencyOps(ops)
	h.evaluationFormOps(ops)
	h.trafficOps(ops)
	h.flowAssociationOps(ops)
	h.securityExtraOps(ops)
	h.dataTableOps(ops)
	h.emailOps(ops)
	h.workspaceOps(ops)
}

func (h *Handler) ruleOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["CreateRule"] = replay(
		h,
		"CreateRule",
		func(in *CreateRuleInput) string { return in.ClientToken },
		b.CreateRule,
	)
	ops["DescribeRule"] = describeOp("RuleId", "Rule", b.DescribeRule)
	ops["ListRules"] = listOp(
		"RuleSummaryList",
		func(r *restroute.Request, t string, m int32) ([]RuleSummary, string, error) {
			return b.ListRules(instOf(r), r.Query.Get("eventSourceName"), r.Query.Get("publishStatus"), t, m)
		},
	)
	ops["UpdateRule"] = typedCtx(b.UpdateRule)
	ops["DeleteRule"] = deleteOp("RuleId", b.DeleteRule)
}

func (h *Handler) templateOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["CreateTaskTemplate"] = replay(
		h,
		"CreateTaskTemplate",
		func(in *CreateTaskTemplateInput) string { return in.ClientToken },
		b.CreateTaskTemplate,
	)
	ops["GetTaskTemplate"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		return passthrough(b.GetTaskTemplate(instOf(req), req.Params["TaskTemplateId"]))
	}
	ops["ListTaskTemplates"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		token, m, err := pageArgs(req)
		if err != nil {
			return nil, 0, err
		}

		items, next, err := b.ListTaskTemplates(&ListTaskTemplatesInput{
			InstanceID: instOf(req), Name: req.Query.Get("name"),
			Status: req.Query.Get("status"), NextToken: token, MaxResults: m,
		})
		if err != nil {
			return nil, 0, err
		}

		out := map[string]any{"TaskTemplates": orEmpty(items)}
		if next != "" {
			out["NextToken"] = next
		}

		return out, 0, nil
	}
	ops["UpdateTaskTemplate"] = typed(b.UpdateTaskTemplate)
	ops["DeleteTaskTemplate"] = deleteOp("TaskTemplateId", b.DeleteTaskTemplate)
	ops["UpdateContactTaskTemplate"] = typed(b.UpdateContactTaskTemplate)
}

func (h *Handler) predefinedOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["CreatePredefinedAttribute"] = typedCtx(b.CreatePredefinedAttribute)
	ops["DescribePredefinedAttribute"] = describeOp("Name", "PredefinedAttribute", b.DescribePredefinedAttribute)
	ops["ListPredefinedAttributes"] = listOp(
		"PredefinedAttributeSummaryList",
		func(r *restroute.Request, t string, m int32) ([]PredefinedAttributeSummary, string, error) {
			return b.ListPredefinedAttributes(instOf(r), t, m)
		},
	)
	ops["UpdatePredefinedAttribute"] = typedCtx(b.UpdatePredefinedAttribute)
	ops["DeletePredefinedAttribute"] = deleteOp("Name", b.DeletePredefinedAttribute)
}

func (h *Handler) hoursExtraOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["CreateHoursOfOperationOverride"] = typedCtx(b.CreateHoursOfOperationOverride)
	ops["DescribeHoursOfOperationOverride"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		o, err := b.DescribeHoursOfOperationOverride(
			instOf(req), req.Params["HoursOfOperationId"], req.Params["HoursOfOperationOverrideId"])
		if err != nil {
			return nil, 0, err
		}

		return map[string]any{"HoursOfOperationOverride": o}, 0, nil
	}
	ops["ListHoursOfOperationOverrides"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		token, m, err := pageArgs(req)
		if err != nil {
			return nil, 0, err
		}

		return passthrough(b.ListHoursOfOperationOverrides(instOf(req), req.Params["HoursOfOperationId"], token, m))
	}
	ops["UpdateHoursOfOperationOverride"] = typedCtx(b.UpdateHoursOfOperationOverride)
	ops["DeleteHoursOfOperationOverride"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		_, err := b.DeleteHoursOfOperationOverride(
			instOf(req), req.Params["HoursOfOperationId"], req.Params["HoursOfOperationOverrideId"])

		return nil, 0, err
	}
	ops["AssociateHoursOfOperations"] = typedCtx(b.AssociateHoursOfOperations)
	ops["DisassociateHoursOfOperations"] = typedCtx(b.DisassociateHoursOfOperations)
	ops["ListChildHoursOfOperations"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		token, m, err := pageArgs(req)
		if err != nil {
			return nil, 0, err
		}

		return passthrough(b.ListChildHoursOfOperations(instOf(req), req.Params["HoursOfOperationId"], token, m))
	}
	ops["GetEffectiveHoursOfOperations"] = typed(b.GetEffectiveHoursOfOperations)
}

func (h *Handler) vocabularyOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["CreateVocabulary"] = replay(
		h, "CreateVocabulary", func(in *CreateVocabularyInput) string { return in.ClientToken }, b.CreateVocabulary)
	ops["DescribeVocabulary"] = describeOp("VocabularyId", "Vocabulary", b.DescribeVocabulary)
	ops["DeleteVocabulary"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		return passthrough(b.DeleteVocabulary(instOf(req), req.Params["VocabularyId"]))
	}
	ops["SearchVocabularies"] = typed(b.SearchVocabularies)
	ops["AssociateDefaultVocabulary"] = typed(b.AssociateDefaultVocabulary)
	ops["ListDefaultVocabularies"] = typed(b.ListDefaultVocabularies)
}

func (h *Handler) flowVersionOps(ops map[string]restroute.OpFunc) {
	for _, k := range []struct{ kind, label, arnKey string }{
		{kindFlow, "ContactFlow", "ContactFlowArn"},
		{kindModule, labelModule, "ContactFlowModuleArn"},
	} {
		h.versionOps(ops, k.kind, k.label, k.arnKey)
		h.aliasOps(ops, k.kind, k.label, k.arnKey)
	}
}

func (h *Handler) versionOps(ops map[string]restroute.OpFunc, kind, label, arnKey string) {
	b := h.Backend

	ops["Create"+label+"Version"] = typedCtx(func(ctx context.Context, in *VersionInput) (*map[string]any, error) {
		out, err := b.createVersion(ctx, kind, in)
		if err != nil {
			return nil, err
		}

		return &map[string]any{arnKey: out.Arn, "Version": out.Version}, nil
	})
	ops["Delete"+label+"Version"] = typed(func(in *VersionInput) (*empty, error) { return b.deleteVersion(kind, in) })
	ops["List"+label+"Versions"] = typed(func(in *VersionInput) (*map[string]any, error) {
		items, next, err := b.listVersions(kind, in)

		return listBody(label+"VersionSummaryList", orEmpty(items), next), err
	})
}

func (h *Handler) aliasOps(ops map[string]restroute.OpFunc, kind, label, arnKey string) {
	b := h.Backend

	ops["Create"+label+"Alias"] = typedCtx(func(ctx context.Context, in *AliasInput) (*map[string]any, error) {
		out, err := b.createAlias(ctx, kind, in)
		if err != nil {
			return nil, err
		}

		return &map[string]any{arnKey: out.Arn, "Id": out.ID}, nil
	})
	ops["Describe"+label+"Alias"] = typed(func(in *AliasInput) (*map[string]any, error) {
		info, err := b.describeAlias(kind, in)
		if err != nil {
			return nil, err
		}

		return &map[string]any{label + "Alias": info}, nil
	})
	ops["Update"+label+"Alias"] = typedCtx(func(ctx context.Context, in *AliasInput) (*empty, error) {
		return b.updateAlias(ctx, kind, in)
	})
	ops["Delete"+label+"Alias"] = typed(func(in *AliasInput) (*empty, error) { return b.deleteAlias(kind, in) })
	ops["List"+label+"Aliases"] = typed(func(in *AliasInput) (*map[string]any, error) {
		items, next, err := b.listAliases(kind, in)

		return listBody(label+"AliasSummaryList", orEmpty(items), next), err
	})
}

func listBody(key string, items any, next string) *map[string]any {
	out := map[string]any{key: items}
	if next != "" {
		out["NextToken"] = next
	}

	return &out
}

func (h *Handler) viewOps(ops map[string]restroute.OpFunc) {
	b := h.Backend
	wrap := func(label string, fn func(*ViewRequest) (*View, error)) restroute.OpFunc {
		return typed(func(in *ViewRequest) (*map[string]any, error) {
			v, err := fn(in)
			if err != nil {
				return nil, err
			}

			return &map[string]any{label: v}, nil
		})
	}

	ops["CreateView"] = replay(h, "CreateView", func(in *CreateViewInput) string { return in.ClientToken },
		func(ctx context.Context, in *CreateViewInput) (*map[string]any, error) {
			v, err := b.CreateView(ctx, in)
			if err != nil {
				return nil, err
			}

			return &map[string]any{"View": v}, nil
		})
	ops["CreateViewVersion"] = wrap("View", b.CreateViewVersion)
	ops["UpdateViewContent"] = wrap("View", b.UpdateViewContent)
	ops["DescribeView"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		v, err := b.DescribeView(instOf(req), req.Params["ViewId"])
		if err != nil {
			return nil, 0, err
		}

		return map[string]any{"View": v}, 0, nil
	}
	ops["ListViews"] = typed(func(in *ViewRequest) (*map[string]any, error) {
		items, next, err := b.ListViews(in)

		return listBody("ViewsSummaryList", orEmpty(items), next), err
	})
	ops["ListViewVersions"] = typed(func(in *ViewRequest) (*map[string]any, error) {
		items, next, err := b.ListViewVersions(in)

		return listBody("ViewVersionSummaryList", orEmpty(items), next), err
	})
	ops["UpdateViewMetadata"] = typed(b.UpdateViewMetadata)
	ops["DeleteView"] = deleteOp("ViewId", b.DeleteView)
	ops["DeleteViewVersion"] = typed(b.DeleteViewVersion)
}

func (h *Handler) searchOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	for op, fn := range map[string]func(*SearchRequest) (*SearchResult, error){
		"SearchAgentStatuses":             b.SearchAgentStatuses,
		"SearchContactFlowModules":        b.SearchContactFlowModules,
		"SearchContactFlows":              b.SearchContactFlows,
		"SearchDataTables":                b.SearchDataTables,
		"SearchEmailAddresses":            b.SearchEmailAddresses,
		"SearchWorkspaceAssociations":     b.SearchWorkspaceAssociations,
		"SearchWorkspaces":                b.SearchWorkspaces,
		"SearchEvaluationForms":           b.SearchEvaluationForms,
		"SearchHoursOfOperationOverrides": b.SearchHoursOfOperationOverrides,
		"SearchHoursOfOperations":         b.SearchHoursOfOperations,
		"SearchPredefinedAttributes":      b.SearchPredefinedAttributes,
		"SearchPrompts":                   b.SearchPrompts,
		"SearchQueues":                    b.SearchQueues,
		"SearchQuickConnects":             b.SearchQuickConnects,
		"SearchResourceTags":              b.SearchResourceTags,
		"SearchRoutingProfiles":           b.SearchRoutingProfiles,
		"SearchRules":                     b.SearchRules,
		"SearchSecurityProfiles":          b.SearchSecurityProfiles,
		"SearchUserHierarchyGroups":       b.SearchUserHierarchyGroups,
		"SearchUsers":                     b.SearchUsers,
		"SearchViews":                     b.SearchViews,
	} {
		ops[op] = typed(fn)
	}
}

func (h *Handler) proficiencyOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["AssociateUserProficiencies"] = typedCtx(b.AssociateUserProficiencies)
	ops["UpdateUserProficiencies"] = typedCtx(b.UpdateUserProficiencies)
	ops["DisassociateUserProficiencies"] = typedCtx(b.DisassociateUserProficiencies)
	ops["ListUserProficiencies"] = typed(b.ListUserProficiencies)
	ops["UpdateUserConfig"] = typedCtx(b.UpdateUserConfig)
}

func (h *Handler) evaluationFormOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["CreateEvaluationForm"] = replay(
		h, "CreateEvaluationForm", func(in *FormRequest) string { return in.ClientToken },
		func(ctx context.Context, in *FormRequest) (*FormOut, error) { return b.CreateEvaluationForm(ctx, in) })
	ops["UpdateEvaluationForm"] = typedCtx(b.UpdateEvaluationForm)
	ops["ActivateEvaluationForm"] = typedCtx(b.ActivateEvaluationForm)
	ops["DeactivateEvaluationForm"] = typedCtx(b.DeactivateEvaluationForm)
	ops["DeleteEvaluationForm"] = typed(b.DeleteEvaluationForm)
	ops["DescribeEvaluationForm"] = typed(func(in *FormRequest) (*map[string]any, error) {
		f, err := b.DescribeEvaluationForm(in)
		if err != nil {
			return nil, err
		}

		return &map[string]any{"EvaluationForm": f}, nil
	})
	ops["ListEvaluationForms"] = typed(func(in *FormRequest) (*map[string]any, error) {
		items, next, err := b.ListEvaluationForms(in)

		return listBody("EvaluationFormSummaryList", orEmpty(items), next), err
	})
	ops["ListEvaluationFormVersions"] = typed(func(in *FormRequest) (*map[string]any, error) {
		items, next, err := b.ListEvaluationFormVersions(in)

		return listBody("EvaluationFormVersionSummaryList", orEmpty(items), next), err
	})
	ops["StartEvaluationFormValidation"] = typed(b.StartEvaluationFormValidation)
	ops["GetEvaluationFormValidation"] = typed(b.GetEvaluationFormValidation)
}

func (h *Handler) trafficOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["CreateTrafficDistributionGroup"] = replay(
		h, "CreateTrafficDistributionGroup", func(in *TDGRequest) string { return in.ClientToken },
		func(ctx context.Context, in *TDGRequest) (*TrafficDistributionGroupOut, error) {
			return b.CreateTrafficDistributionGroup(ctx, in)
		})
	ops["DescribeTrafficDistributionGroup"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		g, err := b.DescribeTrafficDistributionGroup(req.Params["TrafficDistributionGroupId"])
		if err != nil {
			return nil, 0, err
		}

		return map[string]any{"TrafficDistributionGroup": g}, 0, nil
	}
	ops["ListTrafficDistributionGroups"] = typed(func(in *TDGRequest) (*map[string]any, error) {
		items, next, err := b.ListTrafficDistributionGroups(in)

		return listBody("TrafficDistributionGroupSummaryList", orEmpty(items), next), err
	})
	ops["GetTrafficDistribution"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		return passthrough(b.GetTrafficDistribution(req.Params["Id"]))
	}
	ops["UpdateTrafficDistribution"] = typed(b.UpdateTrafficDistribution)
	ops["DeleteTrafficDistributionGroup"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		_, err := b.DeleteTrafficDistributionGroup(req.Params["TrafficDistributionGroupId"])

		return nil, 0, err
	}
	ops["AssociateTrafficDistributionGroupUser"] = typed(b.AssociateTrafficDistributionGroupUser)
	ops["DisassociateTrafficDistributionGroupUser"] = typed(b.DisassociateTrafficDistributionGroupUser)
	ops["ListTrafficDistributionGroupUsers"] = typed(func(in *TDGRequest) (*map[string]any, error) {
		items, next, err := b.ListTrafficDistributionGroupUsers(in)

		return listBody("TrafficDistributionGroupUserSummaryList", orEmpty(items), next), err
	})
}

func (h *Handler) flowAssociationOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["AssociateFlow"] = typed(b.AssociateFlow)
	ops["DisassociateFlow"] = typed(b.DisassociateFlow)
	ops["GetFlowAssociation"] = typed(b.GetFlowAssociation)
	ops["ListFlowAssociations"] = typed(func(in *FlowAssocRequest) (*map[string]any, error) {
		items, next, err := b.ListFlowAssociations(in)

		return listBody("FlowAssociationSummaryList", orEmpty(items), next), err
	})
	ops["BatchGetFlowAssociation"] = typed(func(in *FlowAssocRequest) (*map[string]any, error) {
		items, err := b.BatchGetFlowAssociation(in)

		return listBody("FlowAssociationSummaryList", orEmpty(items), ""), err
	})
}

func (h *Handler) securityExtraOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["AssociateSecurityProfiles"] = typedCtx(b.AssociateSecurityProfiles)
	ops["DisassociateSecurityProfiles"] = typedCtx(b.DisassociateSecurityProfiles)
	ops["ListEntitySecurityProfiles"] = typed(b.ListEntitySecurityProfiles)
	ops["ListSecurityProfileApplications"] = typed(b.ListSecurityProfileApplications)
	ops["ListSecurityProfileFlowModules"] = typed(b.ListSecurityProfileFlowModules)
	ops["ListSecurityProfileAIAgents"] = typed(b.ListSecurityProfileAIAgents)
}
