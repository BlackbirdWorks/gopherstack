package mgn

import "maps"

// routes returns the flat routeKey -> routeEntry lookup table for all 95
// operations. Split across several routesX builders (rather than one
// 95-entry map literal) purely to stay under funlen's line-count limit --
// a lookup-table split, not a logic split (decomposition, not suppression --
// see .claude/memories/parity-principles.md's ban on cyclop/funlen/gocyclo/
// gocognit suppressions).
func (h *Handler) routes() map[string]routeEntry {
	return mergeRoutes(
		h.routesSourceServersAndJobs(),
		h.routesConfigTemplates(),
		h.routesAppsAndWaves(),
		h.routesConnectorsVcenterExportImport(),
		h.routesActionsAndServiceInit(),
		h.routesTags(),
		h.routesNetworkMigrationDefinitions(),
		h.routesNetworkMigrationJobs(),
	)
}

func mergeRoutes(tables ...map[string]routeEntry) map[string]routeEntry {
	out := make(map[string]routeEntry)

	for _, t := range tables {
		maps.Copy(out, t)
	}

	return out
}

// routesSourceServersAndJobs covers family A (16 ops) and family B (3 ops).
func (h *Handler) routesSourceServersAndJobs() map[string]routeEntry {
	return map[string]routeEntry{
		"POST DescribeSourceServers": {op: "DescribeSourceServers", fn: h.handleDescribeSourceServers},
		"POST UpdateSourceServer":    {op: "UpdateSourceServer", fn: h.handleUpdateSourceServer},
		"POST UpdateSourceServerReplicationType": {
			op: "UpdateSourceServerReplicationType", fn: h.handleUpdateSourceServerReplicationType,
		},
		"POST DeleteSourceServer":         {op: "DeleteSourceServer", fn: h.handleDeleteSourceServer},
		"POST ChangeServerLifeCycleState": {op: "ChangeServerLifeCycleState", fn: h.handleChangeServerLifeCycleState},
		"POST DisconnectFromService":      {op: "DisconnectFromService", fn: h.handleDisconnectFromService},
		"POST FinalizeCutover":            {op: "FinalizeCutover", fn: h.handleFinalizeCutover},
		"POST MarkAsArchived":             {op: "MarkAsArchived", fn: h.handleMarkAsArchived},
		"POST StartTest":                  {op: "StartTest", fn: h.handleStartTest},
		"POST StartCutover":               {op: "StartCutover", fn: h.handleStartCutover},
		"POST StartReplication":           {op: "StartReplication", fn: h.handleStartReplication},
		"POST StopReplication":            {op: "StopReplication", fn: h.handleStopReplication},
		"POST PauseReplication":           {op: "PauseReplication", fn: h.handlePauseReplication},
		"POST ResumeReplication":          {op: "ResumeReplication", fn: h.handleResumeReplication},
		"POST RetryDataReplication":       {op: "RetryDataReplication", fn: h.handleRetryDataReplication},
		"POST TerminateTargetInstances":   {op: "TerminateTargetInstances", fn: h.handleTerminateTargetInstances},
		"POST DescribeJobs":               {op: "DescribeJobs", fn: h.handleDescribeJobs},
		"POST DescribeJobLogItems":        {op: "DescribeJobLogItems", fn: h.handleDescribeJobLogItems},
		"POST DeleteJob":                  {op: "DeleteJob", fn: h.handleDeleteJob},
	}
}

// routesConfigTemplates covers family C (6 ops) and family D (6 ops).
func (h *Handler) routesConfigTemplates() map[string]routeEntry {
	return mergeRoutes(h.routesLaunchConfig(), h.routesReplicationConfig())
}

func (h *Handler) routesLaunchConfig() map[string]routeEntry {
	return map[string]routeEntry{
		"POST GetLaunchConfiguration":    {op: "GetLaunchConfiguration", fn: h.handleGetLaunchConfiguration},
		"POST UpdateLaunchConfiguration": {op: "UpdateLaunchConfiguration", fn: h.handleUpdateLaunchConfiguration},
		"POST CreateLaunchConfigurationTemplate": {
			op: "CreateLaunchConfigurationTemplate", fn: h.handleCreateLaunchConfigurationTemplate,
		},
		"POST DeleteLaunchConfigurationTemplate": {
			op: "DeleteLaunchConfigurationTemplate", fn: h.handleDeleteLaunchConfigurationTemplate,
		},
		"POST DescribeLaunchConfigurationTemplates": {
			op: "DescribeLaunchConfigurationTemplates", fn: h.handleDescribeLaunchConfigurationTemplates,
		},
		"POST UpdateLaunchConfigurationTemplate": {
			op: "UpdateLaunchConfigurationTemplate", fn: h.handleUpdateLaunchConfigurationTemplate,
		},
	}
}

func (h *Handler) routesReplicationConfig() map[string]routeEntry {
	return map[string]routeEntry{
		"POST GetReplicationConfiguration": {
			op: "GetReplicationConfiguration",
			fn: h.handleGetReplicationConfiguration,
		},
		"POST UpdateReplicationConfiguration": {
			op: "UpdateReplicationConfiguration", fn: h.handleUpdateReplicationConfiguration,
		},
		"POST CreateReplicationConfigurationTemplate": {
			op: "CreateReplicationConfigurationTemplate", fn: h.handleCreateReplicationConfigurationTemplate,
		},
		"POST DeleteReplicationConfigurationTemplate": {
			op: "DeleteReplicationConfigurationTemplate", fn: h.handleDeleteReplicationConfigurationTemplate,
		},
		"POST DescribeReplicationConfigurationTemplates": {
			op: "DescribeReplicationConfigurationTemplates", fn: h.handleDescribeReplicationConfigurationTemplates,
		},
		"POST UpdateReplicationConfigurationTemplate": {
			op: "UpdateReplicationConfigurationTemplate", fn: h.handleUpdateReplicationConfigurationTemplate,
		},
	}
}

// routesAppsAndWaves covers family E (8 ops) and family F (8 ops).
func (h *Handler) routesAppsAndWaves() map[string]routeEntry {
	return mergeRoutes(h.routesApplications(), h.routesWaves())
}

func (h *Handler) routesApplications() map[string]routeEntry {
	return map[string]routeEntry{
		"POST CreateApplication":    {op: "CreateApplication", fn: h.handleCreateApplication},
		"POST UpdateApplication":    {op: "UpdateApplication", fn: h.handleUpdateApplication},
		"POST DeleteApplication":    {op: "DeleteApplication", fn: h.handleDeleteApplication},
		"POST ListApplications":     {op: "ListApplications", fn: h.handleListApplications},
		"POST ArchiveApplication":   {op: "ArchiveApplication", fn: h.handleArchiveApplication},
		"POST UnarchiveApplication": {op: "UnarchiveApplication", fn: h.handleUnarchiveApplication},
		"POST AssociateSourceServers": {
			op: "AssociateSourceServers", fn: h.handleAssociateSourceServers,
		},
		"POST DisassociateSourceServers": {
			op: "DisassociateSourceServers", fn: h.handleDisassociateSourceServers,
		},
	}
}

func (h *Handler) routesWaves() map[string]routeEntry {
	return map[string]routeEntry{
		"POST CreateWave":    {op: "CreateWave", fn: h.handleCreateWave},
		"POST UpdateWave":    {op: "UpdateWave", fn: h.handleUpdateWave},
		"POST DeleteWave":    {op: "DeleteWave", fn: h.handleDeleteWave},
		"POST ListWaves":     {op: "ListWaves", fn: h.handleListWaves},
		"POST ArchiveWave":   {op: "ArchiveWave", fn: h.handleArchiveWave},
		"POST UnarchiveWave": {op: "UnarchiveWave", fn: h.handleUnarchiveWave},
		"POST AssociateApplications": {
			op: "AssociateApplications", fn: h.handleAssociateApplications,
		},
		"POST DisassociateApplications": {
			op: "DisassociateApplications", fn: h.handleDisassociateApplications,
		},
	}
}

// routesConnectorsVcenterExportImport covers family G (4 ops), family H (2
// ops), and family I (8 ops).
func (h *Handler) routesConnectorsVcenterExportImport() map[string]routeEntry {
	return mergeRoutes(h.routesConnectors(), h.routesVcenterClients(), h.routesExportImport())
}

func (h *Handler) routesConnectors() map[string]routeEntry {
	return map[string]routeEntry{
		"POST CreateConnector": {op: "CreateConnector", fn: h.handleCreateConnector},
		"POST UpdateConnector": {op: "UpdateConnector", fn: h.handleUpdateConnector},
		"POST DeleteConnector": {op: "DeleteConnector", fn: h.handleDeleteConnector},
		"POST ListConnectors":  {op: "ListConnectors", fn: h.handleListConnectors},
	}
}

func (h *Handler) routesVcenterClients() map[string]routeEntry {
	return map[string]routeEntry{
		"GET DescribeVcenterClients": {op: "DescribeVcenterClients", fn: h.handleDescribeVcenterClients},
		"POST DeleteVcenterClient":   {op: "DeleteVcenterClient", fn: h.handleDeleteVcenterClient},
	}
}

func (h *Handler) routesExportImport() map[string]routeEntry {
	return map[string]routeEntry{
		"POST StartExport":      {op: "StartExport", fn: h.handleStartExport},
		"POST ListExports":      {op: "ListExports", fn: h.handleListExports},
		"POST ListExportErrors": {op: "ListExportErrors", fn: h.handleListExportErrors},
		"POST StartImport":      {op: "StartImport", fn: h.handleStartImport},
		"POST ListImports":      {op: "ListImports", fn: h.handleListImports},
		"POST ListImportErrors": {op: "ListImportErrors", fn: h.handleListImportErrors},
		"POST StartImportFileEnrichment": {
			op: "StartImportFileEnrichment", fn: h.handleStartImportFileEnrichment,
		},
		"POST ListImportFileEnrichments": {
			op: "ListImportFileEnrichments", fn: h.handleListImportFileEnrichments,
		},
	}
}

// routesActionsAndServiceInit covers family J (6 ops) and family K (2 ops).
func (h *Handler) routesActionsAndServiceInit() map[string]routeEntry {
	return mergeRoutes(h.routesActions(), h.routesServiceInit())
}

func (h *Handler) routesActions() map[string]routeEntry {
	return map[string]routeEntry{
		"POST PutSourceServerAction":    {op: "PutSourceServerAction", fn: h.handlePutSourceServerAction},
		"POST ListSourceServerActions":  {op: "ListSourceServerActions", fn: h.handleListSourceServerActions},
		"POST RemoveSourceServerAction": {op: "RemoveSourceServerAction", fn: h.handleRemoveSourceServerAction},
		"POST PutTemplateAction":        {op: "PutTemplateAction", fn: h.handlePutTemplateAction},
		"POST ListTemplateActions":      {op: "ListTemplateActions", fn: h.handleListTemplateActions},
		"POST RemoveTemplateAction":     {op: "RemoveTemplateAction", fn: h.handleRemoveTemplateAction},
	}
}

func (h *Handler) routesServiceInit() map[string]routeEntry {
	return map[string]routeEntry{
		"POST InitializeService":   {op: "InitializeService", fn: h.handleInitializeService},
		"POST ListManagedAccounts": {op: "ListManagedAccounts", fn: h.handleListManagedAccounts},
	}
}

// routesTags covers family L (3 ops) -- the only ops keyed by the literal
// "tags" operation segment regardless of the ARN segment that follows (see
// operationSegment's doc comment).
func (h *Handler) routesTags() map[string]routeEntry {
	return map[string]routeEntry{
		"GET tags":    {op: "ListTagsForResource", fn: h.handleListTagsForResource},
		"POST tags":   {op: "TagResource", fn: h.handleTagResource},
		"DELETE tags": {op: "UntagResource", fn: h.handleUntagResource},
	}
}

// routesNetworkMigrationDefinitions covers family M (13 ops).
func (h *Handler) routesNetworkMigrationDefinitions() map[string]routeEntry {
	return mergeRoutes(h.routesNMDefinitionsCRUD(), h.routesNMMapperAndMapping())
}

func (h *Handler) routesNMDefinitionsCRUD() map[string]routeEntry {
	return map[string]routeEntry{
		"POST CreateNetworkMigrationDefinition": {
			op: "CreateNetworkMigrationDefinition", fn: h.handleCreateNetworkMigrationDefinition,
		},
		"POST GetNetworkMigrationDefinition": {
			op: "GetNetworkMigrationDefinition", fn: h.handleGetNetworkMigrationDefinition,
		},
		"POST UpdateNetworkMigrationDefinition": {
			op: "UpdateNetworkMigrationDefinition", fn: h.handleUpdateNetworkMigrationDefinition,
		},
		"POST DeleteNetworkMigrationDefinition": {
			op: "DeleteNetworkMigrationDefinition", fn: h.handleDeleteNetworkMigrationDefinition,
		},
		"POST ListNetworkMigrationDefinitions": {
			op: "ListNetworkMigrationDefinitions", fn: h.handleListNetworkMigrationDefinitions,
		},
	}
}

func (h *Handler) routesNMMapperAndMapping() map[string]routeEntry {
	return map[string]routeEntry{
		"POST GetNetworkMigrationMapperSegmentConstruct": {
			op: "GetNetworkMigrationMapperSegmentConstruct", fn: h.handleGetNetworkMigrationMapperSegmentConstruct,
		},
		"POST ListNetworkMigrationMapperSegmentConstructs": {
			op: "ListNetworkMigrationMapperSegmentConstructs", fn: h.handleListNetworkMigrationMapperSegmentConstructs,
		},
		"POST ListNetworkMigrationMapperSegments": {
			op: "ListNetworkMigrationMapperSegments", fn: h.handleListNetworkMigrationMapperSegments,
		},
		"POST UpdateNetworkMigrationMapperSegment": {
			op: "UpdateNetworkMigrationMapperSegment", fn: h.handleUpdateNetworkMigrationMapperSegment,
		},
		"POST ListNetworkMigrationMappings": {
			op: "ListNetworkMigrationMappings", fn: h.handleListNetworkMigrationMappings,
		},
		"POST ListNetworkMigrationMappingUpdates": {
			op: "ListNetworkMigrationMappingUpdates", fn: h.handleListNetworkMigrationMappingUpdates,
		},
		"POST StartNetworkMigrationMapping": {
			op: "StartNetworkMigrationMapping", fn: h.handleStartNetworkMigrationMapping,
		},
		"POST StartNetworkMigrationMappingUpdate": {
			op: "StartNetworkMigrationMappingUpdate", fn: h.handleStartNetworkMigrationMappingUpdate,
		},
	}
}

// routesNetworkMigrationJobs covers family N (10 ops).
func (h *Handler) routesNetworkMigrationJobs() map[string]routeEntry {
	return mergeRoutes(h.routesNMAnalysisAndCodeGen(), h.routesNMDeploymentAndExecutions())
}

func (h *Handler) routesNMAnalysisAndCodeGen() map[string]routeEntry {
	return map[string]routeEntry{
		"POST StartNetworkMigrationAnalysis": {
			op: "StartNetworkMigrationAnalysis", fn: h.handleStartNetworkMigrationAnalysis,
		},
		"POST ListNetworkMigrationAnalyses": {
			op: "ListNetworkMigrationAnalyses", fn: h.handleListNetworkMigrationAnalyses,
		},
		"POST ListNetworkMigrationAnalysisResults": {
			op: "ListNetworkMigrationAnalysisResults", fn: h.handleListNetworkMigrationAnalysisResults,
		},
		"POST StartNetworkMigrationCodeGeneration": {
			op: "StartNetworkMigrationCodeGeneration", fn: h.handleStartNetworkMigrationCodeGeneration,
		},
		"POST ListNetworkMigrationCodeGenerations": {
			op: "ListNetworkMigrationCodeGenerations", fn: h.handleListNetworkMigrationCodeGenerations,
		},
		"POST ListNetworkMigrationCodeGenerationSegments": {
			op: "ListNetworkMigrationCodeGenerationSegments", fn: h.handleListNetworkMigrationCodeGenerationSegments,
		},
	}
}

func (h *Handler) routesNMDeploymentAndExecutions() map[string]routeEntry {
	return map[string]routeEntry{
		"POST StartNetworkMigrationDeployment": {
			op: "StartNetworkMigrationDeployment", fn: h.handleStartNetworkMigrationDeployment,
		},
		"POST ListNetworkMigrationDeployments": {
			op: "ListNetworkMigrationDeployments", fn: h.handleListNetworkMigrationDeployments,
		},
		"POST ListNetworkMigrationDeployedStacks": {
			op: "ListNetworkMigrationDeployedStacks", fn: h.handleListNetworkMigrationDeployedStacks,
		},
		"POST ListNetworkMigrationExecutions": {
			op: "ListNetworkMigrationExecutions", fn: h.handleListNetworkMigrationExecutions,
		},
	}
}
