package connect

import (
	"net/http"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func extraRoutes() []restroute.Route {
	return slices.Concat(
		ruleRoutes(),
		templateRoutes(),
		predefinedRoutes(),
		hoursExtraRoutes(),
		vocabularyRoutes(),
		flowVersionRoutes(),
		viewRoutes(),
		searchRoutes(),
		proficiencyRoutes(),
		evaluationFormRoutes(),
		trafficRoutes(),
		flowAssociationRoutes(),
		securityExtraRoutes(),
		dataTableRoutes(),
		emailRoutes(),
	)
}

func ruleRoutes() []restroute.Route {
	return []restroute.Route{
		{Method: http.MethodPost, Path: "/rules/{InstanceId}", Op: "CreateRule"},
		{Method: http.MethodDelete, Path: pathRuleByID, Op: "DeleteRule"},
		{Method: http.MethodGet, Path: pathRuleByID, Op: "DescribeRule"},
		{Method: http.MethodGet, Path: "/rules/{InstanceId}", Op: "ListRules"},
		{Method: http.MethodPut, Path: pathRuleByID, Op: "UpdateRule"},
	}
}

func templateRoutes() []restroute.Route {
	const base = "/instance/{InstanceId}/task/template"

	return []restroute.Route{
		{Method: http.MethodPut, Path: base, Op: "CreateTaskTemplate"},
		{Method: http.MethodDelete, Path: base + "/{TaskTemplateId}", Op: "DeleteTaskTemplate"},
		{Method: http.MethodGet, Path: base + "/{TaskTemplateId}", Op: "GetTaskTemplate"},
		{Method: http.MethodGet, Path: base, Op: "ListTaskTemplates"},
		{Method: http.MethodPost, Path: base + "/{TaskTemplateId}", Op: "UpdateTaskTemplate"},
		{Method: http.MethodPost, Path: "/contact/task-template", Op: "UpdateContactTaskTemplate"},
	}
}

func predefinedRoutes() []restroute.Route {
	return []restroute.Route{
		{Method: http.MethodPut, Path: "/predefined-attributes/{InstanceId}", Op: "CreatePredefinedAttribute"},
		{
			Method: http.MethodDelete,
			Path:   pathPredefByName,
			Op:     "DeletePredefinedAttribute",
		},
		{Method: http.MethodGet, Path: pathPredefByName, Op: "DescribePredefinedAttribute"},
		{Method: http.MethodGet, Path: "/predefined-attributes/{InstanceId}", Op: "ListPredefinedAttributes"},
		{Method: http.MethodPost, Path: pathPredefByName, Op: "UpdatePredefinedAttribute"},
	}
}

func hoursExtraRoutes() []restroute.Route {
	const base = "/hours-of-operations/{InstanceId}/{HoursOfOperationId}"

	return []restroute.Route{
		{Method: http.MethodPut, Path: base + "/overrides", Op: "CreateHoursOfOperationOverride"},
		{
			Method: http.MethodDelete, Path: base + "/overrides/{HoursOfOperationOverrideId}",
			Op: "DeleteHoursOfOperationOverride",
		},
		{
			Method: http.MethodGet, Path: base + "/overrides/{HoursOfOperationOverrideId}",
			Op: "DescribeHoursOfOperationOverride",
		},
		{Method: http.MethodGet, Path: base + "/overrides", Op: "ListHoursOfOperationOverrides"},
		{
			Method: http.MethodPost, Path: base + "/overrides/{HoursOfOperationOverrideId}",
			Op: "UpdateHoursOfOperationOverride",
		},
		{Method: http.MethodPost, Path: base + "/associate-hours", Op: "AssociateHoursOfOperations"},
		{Method: http.MethodPost, Path: base + "/disassociate-hours", Op: "DisassociateHoursOfOperations"},
		{Method: http.MethodGet, Path: base + "/hours", Op: "ListChildHoursOfOperations"},
		{
			Method: http.MethodGet, Path: "/effective-hours-of-operations/{InstanceId}/{HoursOfOperationId}",
			Op: "GetEffectiveHoursOfOperations",
		},
	}
}

func vocabularyRoutes() []restroute.Route {
	return []restroute.Route{
		{
			Method: http.MethodPut,
			Path:   "/default-vocabulary/{InstanceId}/{LanguageCode}",
			Op:     "AssociateDefaultVocabulary",
		},
		{Method: http.MethodPost, Path: "/vocabulary/{InstanceId}", Op: "CreateVocabulary"},
		{Method: http.MethodPost, Path: "/vocabulary-remove/{InstanceId}/{VocabularyId}", Op: "DeleteVocabulary"},
		{Method: http.MethodGet, Path: "/vocabulary/{InstanceId}/{VocabularyId}", Op: "DescribeVocabulary"},
		{Method: http.MethodPost, Path: "/vocabulary-summary/{InstanceId}", Op: "SearchVocabularies"},
		{Method: http.MethodPost, Path: "/default-vocabulary-summary/{InstanceId}", Op: "ListDefaultVocabularies"},
	}
}

const flowVersionRouteCount = 16

func flowVersionRoutes() []restroute.Route {
	out := make([]restroute.Route, 0, flowVersionRouteCount)

	for _, k := range []struct{ base, label string }{
		{pathFlowBase, "ContactFlow"},
		{pathModuleBase, labelModule},
	} {
		version := "ContactFlowVersion"
		if k.label == labelModule {
			version = "ContactFlowModuleVersion"
		}

		out = append(
			out,
			restroute.Route{Method: http.MethodPut, Path: k.base + "/alias", Op: "Create" + k.label + "Alias"},
			restroute.Route{Method: http.MethodPut, Path: k.base + "/version", Op: "Create" + k.label + "Version"},
			restroute.Route{
				Method: http.MethodDelete,
				Path:   k.base + "/alias/{AliasId}",
				Op:     "Delete" + k.label + "Alias",
			},
			restroute.Route{
				Method: http.MethodDelete, Path: k.base + "/version/{" + version + "}",
				Op: "Delete" + k.label + "Version",
			},
			restroute.Route{
				Method: http.MethodGet,
				Path:   k.base + "/alias/{AliasId}",
				Op:     "Describe" + k.label + "Alias",
			},
			restroute.Route{Method: http.MethodGet, Path: k.base + "/aliases", Op: "List" + k.label + "Aliases"},
			restroute.Route{Method: http.MethodGet, Path: k.base + "/versions", Op: "List" + k.label + "Versions"},
			restroute.Route{
				Method: http.MethodPost,
				Path:   k.base + "/alias/{AliasId}",
				Op:     "Update" + k.label + "Alias",
			},
		)
	}

	return out
}

func viewRoutes() []restroute.Route {
	return []restroute.Route{
		{Method: http.MethodPut, Path: "/views/{InstanceId}", Op: "CreateView"},
		{Method: http.MethodPut, Path: "/views/{InstanceId}/{ViewId}/versions", Op: "CreateViewVersion"},
		{Method: http.MethodDelete, Path: pathViewByID, Op: "DeleteView"},
		{
			Method: http.MethodDelete,
			Path:   "/views/{InstanceId}/{ViewId}/versions/{ViewVersion}",
			Op:     "DeleteViewVersion",
		},
		{Method: http.MethodGet, Path: pathViewByID, Op: "DescribeView"},
		{Method: http.MethodGet, Path: "/views/{InstanceId}/{ViewId}/versions", Op: "ListViewVersions"},
		{Method: http.MethodGet, Path: "/views/{InstanceId}", Op: "ListViews"},
		{Method: http.MethodPost, Path: pathViewByID, Op: "UpdateViewContent"},
		{Method: http.MethodPost, Path: "/views/{InstanceId}/{ViewId}/metadata", Op: "UpdateViewMetadata"},
	}
}

// searchOps maps each Search operation to its URI.
func searchOps() map[string]string {
	return map[string]string{
		"SearchAgentStatuses":             "/search-agent-statuses",
		"SearchContactFlowModules":        "/search-contact-flow-modules",
		"SearchContactFlows":              "/search-contact-flows",
		"SearchDataTables":                "/search-data-tables",
		"SearchEmailAddresses":            "/search-email-addresses",
		"SearchEvaluationForms":           "/search-evaluation-forms",
		"SearchHoursOfOperationOverrides": "/search-hours-of-operation-overrides",
		"SearchHoursOfOperations":         "/search-hours-of-operations",
		"SearchPredefinedAttributes":      "/search-predefined-attributes",
		"SearchPrompts":                   "/search-prompts",
		"SearchQueues":                    "/search-queues",
		"SearchQuickConnects":             "/search-quick-connects",
		"SearchResourceTags":              "/search-resource-tags",
		"SearchRoutingProfiles":           "/search-routing-profiles",
		"SearchRules":                     "/search-rules",
		"SearchSecurityProfiles":          "/search-security-profiles",
		"SearchUserHierarchyGroups":       "/search-user-hierarchy-groups",
		"SearchUsers":                     "/search-users",
		"SearchViews":                     "/search-views",
	}
}

func searchRoutes() []restroute.Route {
	out := make([]restroute.Route, 0, len(searchOps()))

	for op, path := range searchOps() {
		out = append(out, restroute.Route{Method: http.MethodPost, Path: path, Op: op})
	}

	return out
}

func proficiencyRoutes() []restroute.Route {
	const base = "/users/{InstanceId}/{UserId}"

	return []restroute.Route{
		{Method: http.MethodPost, Path: base + "/associate-proficiencies", Op: "AssociateUserProficiencies"},
		{Method: http.MethodPost, Path: base + "/disassociate-proficiencies", Op: "DisassociateUserProficiencies"},
		{Method: http.MethodGet, Path: base + "/proficiencies", Op: "ListUserProficiencies"},
		{Method: http.MethodPost, Path: base + "/proficiencies", Op: "UpdateUserProficiencies"},
		{Method: http.MethodPost, Path: base + "/config", Op: "UpdateUserConfig"},
	}
}

func evaluationFormRoutes() []restroute.Route {
	const base = "/evaluation-forms/{InstanceId}"

	return []restroute.Route{
		{Method: http.MethodPut, Path: base, Op: "CreateEvaluationForm"},
		{Method: http.MethodPut, Path: base + "/{EvaluationFormId}", Op: "UpdateEvaluationForm"},
		{Method: http.MethodPost, Path: base + "/{EvaluationFormId}/activate", Op: "ActivateEvaluationForm"},
		{Method: http.MethodPost, Path: base + "/{EvaluationFormId}/deactivate", Op: "DeactivateEvaluationForm"},
		{Method: http.MethodDelete, Path: base + "/{EvaluationFormId}", Op: "DeleteEvaluationForm"},
		{Method: http.MethodGet, Path: base + "/{EvaluationFormId}", Op: "DescribeEvaluationForm"},
		{Method: http.MethodGet, Path: base, Op: "ListEvaluationForms"},
		{Method: http.MethodGet, Path: base + "/{EvaluationFormId}/versions", Op: "ListEvaluationFormVersions"},
		{
			Method: http.MethodGet,
			Path:   base + "/{EvaluationFormId}/validation-results",
			Op:     "GetEvaluationFormValidation",
		},
		{Method: http.MethodPost, Path: base + "/{EvaluationFormId}/validate", Op: "StartEvaluationFormValidation"},
	}
}

func trafficRoutes() []restroute.Route {
	const group = "/traffic-distribution-group"

	return []restroute.Route{
		{Method: http.MethodPut, Path: group, Op: "CreateTrafficDistributionGroup"},
		{
			Method: http.MethodDelete,
			Path:   group + "/{TrafficDistributionGroupId}",
			Op:     "DeleteTrafficDistributionGroup",
		},
		{Method: http.MethodGet, Path: group + "/{TrafficDistributionGroupId}", Op: "DescribeTrafficDistributionGroup"},
		{Method: http.MethodGet, Path: "/traffic-distribution-groups", Op: "ListTrafficDistributionGroups"},
		{Method: http.MethodGet, Path: "/traffic-distribution/{Id}", Op: "GetTrafficDistribution"},
		{Method: http.MethodPut, Path: "/traffic-distribution/{Id}", Op: "UpdateTrafficDistribution"},
		{
			Method: http.MethodPut,
			Path:   group + "/{TrafficDistributionGroupId}/user",
			Op:     "AssociateTrafficDistributionGroupUser",
		},
		{
			Method: http.MethodDelete, Path: group + "/{TrafficDistributionGroupId}/user",
			Op: "DisassociateTrafficDistributionGroupUser",
		},
		{
			Method: http.MethodGet,
			Path:   group + "/{TrafficDistributionGroupId}/user",
			Op:     "ListTrafficDistributionGroupUsers",
		},
	}
}

func flowAssociationRoutes() []restroute.Route {
	return []restroute.Route{
		{Method: http.MethodPut, Path: "/flow-associations/{InstanceId}", Op: "AssociateFlow"},
		{Method: http.MethodPost, Path: "/flow-associations-batch/{InstanceId}", Op: "BatchGetFlowAssociation"},
		{
			Method: http.MethodDelete,
			Path:   "/flow-associations/{InstanceId}/{ResourceId}/{ResourceType}",
			Op:     "DisassociateFlow",
		},
		{
			Method: http.MethodGet,
			Path:   "/flow-associations/{InstanceId}/{ResourceId}/{ResourceType}",
			Op:     "GetFlowAssociation",
		},
		{Method: http.MethodGet, Path: "/flow-associations-summary/{InstanceId}", Op: "ListFlowAssociations"},
	}
}

func securityExtraRoutes() []restroute.Route {
	return []restroute.Route{
		{Method: http.MethodPost, Path: "/associate-security-profiles/{InstanceId}", Op: "AssociateSecurityProfiles"},
		{
			Method: http.MethodPost,
			Path:   "/disassociate-security-profiles/{InstanceId}",
			Op:     "DisassociateSecurityProfiles",
		},
		{
			Method: http.MethodPost,
			Path:   "/entity-security-profiles-summary/{InstanceId}",
			Op:     "ListEntitySecurityProfiles",
		},
		{
			Method: http.MethodGet, Path: "/security-profiles-ai-agents/{InstanceId}/{SecurityProfileId}",
			Op: "ListSecurityProfileAIAgents",
		},
		{
			Method: http.MethodGet, Path: "/security-profiles-applications/{InstanceId}/{SecurityProfileId}",
			Op: "ListSecurityProfileApplications",
		},
		{
			Method: http.MethodGet, Path: "/security-profiles-flow-modules/{InstanceId}/{SecurityProfileId}",
			Op: "ListSecurityProfileFlowModules",
		},
	}
}
