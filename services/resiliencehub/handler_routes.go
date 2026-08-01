package resiliencehub

import "maps"

// routes returns the flat method+path -> routeEntry lookup table for all 63
// operations. Every path but the tags trio is a fixed, literal, kebab-case
// action path with no path parameters (see RouteMatcher's doc comment), so
// this table needs no segment-count branching at all -- unlike
// services/outposts' nested routeX functions, a flat map suffices here
// (cyclomatic complexity 1). Split across several routesX builders (rather
// than one 63-entry map literal) to stay under funlen's line-count limit --
// a lookup-table split, not a logic split.
func (h *Handler) routes() map[string]routeEntry {
	return mergeRoutes(
		h.routesApps(),
		h.routesAppVersions(),
		h.routesPoliciesAndAssessments(),
		h.routesRecommendationsAndTemplates(),
		h.routesGroupingMetricsAndTags(),
	)
}

// mergeRoutes combines several method+path -> routeEntry tables into one.
func mergeRoutes(tables ...map[string]routeEntry) map[string]routeEntry {
	out := make(map[string]routeEntry)

	for _, t := range tables {
		maps.Copy(out, t)
	}

	return out
}

// routesApps covers App CRUD and its version/template surface.
func (h *Handler) routesApps() map[string]routeEntry {
	return map[string]routeEntry{
		"POST create-app":           {op: "CreateApp", fn: h.handleCreateApp},
		"POST delete-app":           {op: "DeleteApp", fn: h.handleDeleteApp},
		"POST describe-app":         {op: "DescribeApp", fn: h.handleDescribeApp},
		"POST update-app":           {op: "UpdateApp", fn: h.handleUpdateApp},
		"GET list-apps":             {op: "ListApps", fn: h.handleListApps},
		"POST describe-app-version": {op: "DescribeAppVersion", fn: h.handleDescribeAppVersion},
		"POST update-app-version":   {op: "UpdateAppVersion", fn: h.handleUpdateAppVersion},
		"POST list-app-versions":    {op: "ListAppVersions", fn: h.handleListAppVersions},
		"POST publish-app-version":  {op: "PublishAppVersion", fn: h.handlePublishAppVersion},
		"POST describe-app-version-template": {
			op: "DescribeAppVersionTemplate", fn: h.handleDescribeAppVersionTemplate,
		},
		"POST put-draft-app-version-template": {
			op: "PutDraftAppVersionTemplate", fn: h.handlePutDraftAppVersionTemplate,
		},
	}
}

// routesAppVersions covers app components, resources, resource mappings,
// resolution, and import -- everything scoped under a specific AppVersion.
func (h *Handler) routesAppVersions() map[string]routeEntry {
	return mergeRoutes(h.routesAppComponents(), h.routesAppResources(), h.routesAppMappings())
}

func (h *Handler) routesAppComponents() map[string]routeEntry {
	return map[string]routeEntry{
		"POST create-app-version-app-component": {
			op: "CreateAppVersionAppComponent", fn: h.handleCreateAppVersionAppComponent,
		},
		"POST describe-app-version-app-component": {
			op: "DescribeAppVersionAppComponent", fn: h.handleDescribeAppVersionAppComponent,
		},
		"POST update-app-version-app-component": {
			op: "UpdateAppVersionAppComponent", fn: h.handleUpdateAppVersionAppComponent,
		},
		"POST delete-app-version-app-component": {
			op: "DeleteAppVersionAppComponent", fn: h.handleDeleteAppVersionAppComponent,
		},
		"POST list-app-version-app-components": {
			op: "ListAppVersionAppComponents", fn: h.handleListAppVersionAppComponents,
		},
	}
}

func (h *Handler) routesAppResources() map[string]routeEntry {
	return map[string]routeEntry{
		"POST create-app-version-resource": {op: "CreateAppVersionResource", fn: h.handleCreateAppVersionResource},
		"POST describe-app-version-resource": {
			op: "DescribeAppVersionResource", fn: h.handleDescribeAppVersionResource,
		},
		"POST update-app-version-resource": {op: "UpdateAppVersionResource", fn: h.handleUpdateAppVersionResource},
		"POST delete-app-version-resource": {op: "DeleteAppVersionResource", fn: h.handleDeleteAppVersionResource},
		"POST list-app-version-resources":  {op: "ListAppVersionResources", fn: h.handleListAppVersionResources},
		"POST list-unsupported-app-version-resources": {
			op: "ListUnsupportedAppVersionResources", fn: h.handleListUnsupportedAppVersionResources,
		},
	}
}

func (h *Handler) routesAppMappings() map[string]routeEntry {
	return map[string]routeEntry{
		"POST add-draft-app-version-resource-mappings": {
			op: "AddDraftAppVersionResourceMappings", fn: h.handleAddDraftAppVersionResourceMappings,
		},
		"POST remove-draft-app-version-resource-mappings": {
			op: "RemoveDraftAppVersionResourceMappings", fn: h.handleRemoveDraftAppVersionResourceMappings,
		},
		"POST list-app-version-resource-mappings": {
			op: "ListAppVersionResourceMappings", fn: h.handleListAppVersionResourceMappings,
		},
		"POST resolve-app-version-resources": {
			op: "ResolveAppVersionResources",
			fn: h.handleResolveAppVersionResources,
		},
		"POST describe-app-version-resources-resolution-status": {
			op: "DescribeAppVersionResourcesResolutionStatus", fn: h.handleDescribeAppVersionResourcesResolutionStatus,
		},
		"POST import-resources-to-draft-app-version": {
			op: "ImportResourcesToDraftAppVersion", fn: h.handleImportResourcesToDraftAppVersion,
		},
		"POST describe-draft-app-version-resources-import-status": {
			op: "DescribeDraftAppVersionResourcesImportStatus",
			fn: h.handleDescribeDraftAppVersionResourcesImportStatus,
		},
		"POST delete-app-input-source": {op: "DeleteAppInputSource", fn: h.handleDeleteAppInputSource},
		"POST list-app-input-sources":  {op: "ListAppInputSources", fn: h.handleListAppInputSources},
	}
}

// routesPoliciesAndAssessments covers ResiliencyPolicy CRUD and the
// AppAssessment lifecycle/read family.
func (h *Handler) routesPoliciesAndAssessments() map[string]routeEntry {
	return mergeRoutes(h.routesPolicies(), h.routesAssessments())
}

func (h *Handler) routesPolicies() map[string]routeEntry {
	return map[string]routeEntry{
		"POST create-resiliency-policy":   {op: "CreateResiliencyPolicy", fn: h.handleCreateResiliencyPolicy},
		"POST describe-resiliency-policy": {op: "DescribeResiliencyPolicy", fn: h.handleDescribeResiliencyPolicy},
		"POST update-resiliency-policy":   {op: "UpdateResiliencyPolicy", fn: h.handleUpdateResiliencyPolicy},
		"POST delete-resiliency-policy":   {op: "DeleteResiliencyPolicy", fn: h.handleDeleteResiliencyPolicy},
		"GET list-resiliency-policies":    {op: "ListResiliencyPolicies", fn: h.handleListResiliencyPolicies},
		"GET list-suggested-resiliency-policies": {
			op: "ListSuggestedResiliencyPolicies", fn: h.handleListSuggestedResiliencyPolicies,
		},
	}
}

func (h *Handler) routesAssessments() map[string]routeEntry {
	return map[string]routeEntry{
		"POST start-app-assessment":    {op: "StartAppAssessment", fn: h.handleStartAppAssessment},
		"POST describe-app-assessment": {op: "DescribeAppAssessment", fn: h.handleDescribeAppAssessment},
		"POST delete-app-assessment":   {op: "DeleteAppAssessment", fn: h.handleDeleteAppAssessment},
		"GET list-app-assessments":     {op: "ListAppAssessments", fn: h.handleListAppAssessments},
		"POST list-app-assessment-compliance-drifts": {
			op: "ListAppAssessmentComplianceDrifts", fn: h.handleListAppAssessmentComplianceDrifts,
		},
		"POST list-app-assessment-resource-drifts": {
			op: "ListAppAssessmentResourceDrifts", fn: h.handleListAppAssessmentResourceDrifts,
		},
		"POST list-app-component-compliances": {
			op: "ListAppComponentCompliances",
			fn: h.handleListAppComponentCompliances,
		},
	}
}

// routesRecommendationsAndTemplates covers the four recommendation-list
// families, BatchUpdateRecommendationStatus, and RecommendationTemplate CRUD.
func (h *Handler) routesRecommendationsAndTemplates() map[string]routeEntry {
	return mergeRoutes(h.routesRecommendations(), h.routesTemplates())
}

func (h *Handler) routesRecommendations() map[string]routeEntry {
	return map[string]routeEntry{
		"POST list-alarm-recommendations": {op: "ListAlarmRecommendations", fn: h.handleListAlarmRecommendations},
		"POST list-sop-recommendations":   {op: "ListSopRecommendations", fn: h.handleListSopRecommendations},
		"POST list-test-recommendations":  {op: "ListTestRecommendations", fn: h.handleListTestRecommendations},
		"POST list-app-component-recommendations": {
			op: "ListAppComponentRecommendations", fn: h.handleListAppComponentRecommendations,
		},
		"POST batch-update-recommendation-status": {
			op: "BatchUpdateRecommendationStatus", fn: h.handleBatchUpdateRecommendationStatus,
		},
	}
}

func (h *Handler) routesTemplates() map[string]routeEntry {
	return map[string]routeEntry{
		"POST create-recommendation-template": {
			op: "CreateRecommendationTemplate",
			fn: h.handleCreateRecommendationTemplate,
		},
		"POST delete-recommendation-template": {
			op: "DeleteRecommendationTemplate",
			fn: h.handleDeleteRecommendationTemplate,
		},
		"GET list-recommendation-templates": {
			op: "ListRecommendationTemplates",
			fn: h.handleListRecommendationTemplates,
		},
	}
}

// routesGroupingMetricsAndTags covers the resource-grouping-recommendation
// family, metrics/metrics-export, and the /tags/{resourceArn} trio.
func (h *Handler) routesGroupingMetricsAndTags() map[string]routeEntry {
	return mergeRoutes(h.routesGrouping(), h.routesMetrics(), h.routesTags())
}

func (h *Handler) routesGrouping() map[string]routeEntry {
	return map[string]routeEntry{
		"POST start-resource-grouping-recommendation-task": {
			op: "StartResourceGroupingRecommendationTask", fn: h.handleStartResourceGroupingRecommendationTask,
		},
		"POST describe-resource-grouping-recommendation-task": {
			op: "DescribeResourceGroupingRecommendationTask", fn: h.handleDescribeResourceGroupingRecommendationTask,
		},
		"GET list-resource-grouping-recommendations": {
			op: "ListResourceGroupingRecommendations", fn: h.handleListResourceGroupingRecommendations,
		},
		"POST accept-resource-grouping-recommendations": {
			op: "AcceptResourceGroupingRecommendations", fn: h.handleAcceptResourceGroupingRecommendations,
		},
		"POST reject-resource-grouping-recommendations": {
			op: "RejectResourceGroupingRecommendations", fn: h.handleRejectResourceGroupingRecommendations,
		},
	}
}

func (h *Handler) routesMetrics() map[string]routeEntry {
	return map[string]routeEntry{
		"POST list-metrics":            {op: "ListMetrics", fn: h.handleListMetrics},
		"POST start-metrics-export":    {op: "StartMetricsExport", fn: h.handleStartMetricsExport},
		"POST describe-metrics-export": {op: "DescribeMetricsExport", fn: h.handleDescribeMetricsExport},
	}
}

func (h *Handler) routesTags() map[string]routeEntry {
	return map[string]routeEntry{
		"GET tags":    {op: "ListTagsForResource", fn: h.handleListTagsForResource},
		"POST tags":   {op: "TagResource", fn: h.handleTagResource},
		"DELETE tags": {op: "UntagResource", fn: h.handleUntagResource},
	}
}
