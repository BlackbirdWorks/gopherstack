package mgn

import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func sourceServerKeyFn(v *SourceServer) string { return v.SourceServerID }

func launchConfigKeyFn(v *LaunchConfiguration) string { return v.SourceServerID }

func replicationConfigKeyFn(v *ReplicationConfiguration) string { return v.SourceServerID }

func launchTemplateKeyFn(v *LaunchConfigurationTemplate) string {
	return v.LaunchConfigurationTemplateID
}

func replicationTemplateKeyFn(v *ReplicationConfigurationTemplate) string {
	return v.ReplicationConfigurationTemplateID
}

func jobKeyFn(v *Job) string { return v.JobID }

func jobLogKeyFn(v *JobLog) string { return v.LogID }

func jobLogByJobIndexKeyFn(v *JobLog) string { return v.JobID }

func applicationKeyFn(v *Application) string { return v.ApplicationID }

func waveKeyFn(v *Wave) string { return v.WaveID }

func connectorKeyFn(v *Connector) string { return v.ConnectorID }

func vcenterClientKeyFn(v *VcenterClient) string { return v.VcenterClientID }

func exportTaskKeyFn(v *ExportTask) string { return v.ExportID }

func importTaskKeyFn(v *ImportTask) string { return v.ImportID }

func importFileEnrichmentKeyFn(v *ImportFileEnrichment) string { return v.JobID }

// actionKey builds the composite primary key a SourceServerActionDocument or
// TemplateActionDocument is stored under: both real wire types are keyed by
// (parent ID, ActionID) together (PutSourceServerAction/PutTemplateAction
// both take ActionID plus their own parent-scoping ID as required input),
// but neither wire type carries a single globally-unique ID field of its
// own -- so this backend's Table needs a synthetic composite string key.
func actionKey(parentID, actionID string) string { return parentID + "#" + actionID }

func sourceServerActionKeyFn(v *SourceServerActionDocument) string {
	return actionKey(v.SourceServerID, v.ActionID)
}

func sourceServerActionByServerIndexKeyFn(v *SourceServerActionDocument) string {
	return v.SourceServerID
}

func templateActionKeyFn(v *TemplateActionDocument) string {
	return actionKey(v.LaunchConfigurationTemplateID, v.ActionID)
}

func templateActionByTemplateIndexKeyFn(v *TemplateActionDocument) string {
	return v.LaunchConfigurationTemplateID
}

func nmDefinitionKeyFn(v *NetworkMigrationDefinition) string { return v.NetworkMigrationDefinitionID }

// nmExecutionKey builds the composite primary key a NetworkMigrationExecution
// is stored under: every op that addresses one scopes it by BOTH
// NetworkMigrationDefinitionID and NetworkMigrationExecutionID (confirmed by
// direct read of every StartNetworkMigration*/ListNetworkMigration* Input
// struct in this family -- PARITY.md family M/N tables), so execution IDs
// are only unique WITHIN a definition, not globally.
func nmExecutionKey(definitionID, executionID string) string { return definitionID + "#" + executionID }

func nmExecutionKeyFn(v *NetworkMigrationExecution) string {
	return nmExecutionKey(v.NetworkMigrationDefinitionID, v.NetworkMigrationExecutionID)
}

func nmExecutionByDefIndexKeyFn(v *NetworkMigrationExecution) string {
	return v.NetworkMigrationDefinitionID
}

func nmJobKeyFn(v *NetworkMigrationJob) string { return v.JobID }

func nmJobByExecutionIndexKeyFn(v *NetworkMigrationJob) string {
	return nmExecutionKey(v.NetworkMigrationDefinitionID, v.NetworkMigrationExecutionID)
}

// registerAllTables registers every resource collection exactly once. Must
// be called during construction only -- see services/outposts/store_setup.go's
// doc comment for why (store.Register panics on a duplicate name).
func registerAllTables(b *InMemoryBackend) {
	b.sourceServers = store.Register(b.registry, "sourceServers", store.New(sourceServerKeyFn))
	b.launchConfigs = store.Register(b.registry, "launchConfigs", store.New(launchConfigKeyFn))
	b.replicationConfigs = store.Register(b.registry, "replicationConfigs", store.New(replicationConfigKeyFn))
	b.launchTemplates = store.Register(b.registry, "launchTemplates", store.New(launchTemplateKeyFn))
	b.replicationTemplates = store.Register(b.registry, "replicationTemplates", store.New(replicationTemplateKeyFn))

	b.jobs = store.Register(b.registry, "jobs", store.New(jobKeyFn))
	b.jobLogs = store.Register(b.registry, "jobLogs", store.New(jobLogKeyFn))
	b.jobLogsByJob = b.jobLogs.AddIndex("byJob", jobLogByJobIndexKeyFn)

	b.applications = store.Register(b.registry, "applications", store.New(applicationKeyFn))
	b.waves = store.Register(b.registry, "waves", store.New(waveKeyFn))
	b.connectors = store.Register(b.registry, "connectors", store.New(connectorKeyFn))
	b.vcenterClients = store.Register(b.registry, "vcenterClients", store.New(vcenterClientKeyFn))
	b.exportTasks = store.Register(b.registry, "exportTasks", store.New(exportTaskKeyFn))
	b.importTasks = store.Register(b.registry, "importTasks", store.New(importTaskKeyFn))
	b.importFileEnrichments = store.Register(
		b.registry, "importFileEnrichments", store.New(importFileEnrichmentKeyFn),
	)

	b.sourceServerActions = store.Register(b.registry, "sourceServerActions", store.New(sourceServerActionKeyFn))
	b.sourceServerActionsByServer = b.sourceServerActions.AddIndex("byServer", sourceServerActionByServerIndexKeyFn)

	b.templateActions = store.Register(b.registry, "templateActions", store.New(templateActionKeyFn))
	b.templateActionsByTemplate = b.templateActions.AddIndex("byTemplate", templateActionByTemplateIndexKeyFn)

	b.nmDefinitions = store.Register(b.registry, "nmDefinitions", store.New(nmDefinitionKeyFn))

	b.nmExecutions = store.Register(b.registry, "nmExecutions", store.New(nmExecutionKeyFn))
	b.nmExecutionsByDef = b.nmExecutions.AddIndex("byDefinition", nmExecutionByDefIndexKeyFn)

	b.nmJobs = store.Register(b.registry, "nmJobs", store.New(nmJobKeyFn))
	b.nmJobsByExecution = b.nmJobs.AddIndex("byExecution", nmJobByExecutionIndexKeyFn)
}
