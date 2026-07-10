package codebuild

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/sesv2 /
// services/medialive (data-driven, direct registration -- see pkgs/store's
// package doc for the underlying primitive).
//
// Every convertible value type here already carries its own identity as a
// real (non-json:"-") field -- Project.Name, Build.ID, Fleet.Name,
// ReportGroup.Name, Report.Arn, BuildBatch.ID, CommandExecution.ID,
// Sandbox.ID, Webhook.ProjectName, SourceCredentials.Arn -- so none of the 10
// tables below need a "dirty" ephemeral-DTO registry treatment (cf.
// services/ses / services/codecommit): all 10 are "clean" tables registered
// directly on b.registry, and persistence.go drives every one of them
// through b.registry.SnapshotAll() / RestoreAll().
//
// The five former one-to-one ARN reverse-lookup maps (projectARNIndex,
// buildARNIndex, fleetARNIndex, reportGroupARNIndex, batchARNIndex) and the
// five former one-to-many per-parent grouping maps (buildsByProject,
// sandboxesByProject, batchesByProject, commandsBySandbox, reportsByGroup)
// are replaced by companion *store.Index values on the owning table, kept
// consistent automatically by store.Table.Put/Delete/Restore.
//
// Left as a plain map (not store.Table-backed): resourcePolicies
// (map[string]string) -- its values are plain strings, not *T, so it does
// not fit store.Table's keyed-by-identity-value shape. See persistence.go.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func projectKeyFn(v *Project) string    { return v.Name }
func projectARNKeyFn(v *Project) string { return v.Arn }

func buildKeyFn(v *Build) string        { return v.ID }
func buildARNKeyFn(v *Build) string     { return v.Arn }
func buildProjectKeyFn(v *Build) string { return v.ProjectName }

func fleetKeyFn(v *Fleet) string    { return v.Name }
func fleetARNKeyFn(v *Fleet) string { return v.Arn }

func reportGroupKeyFn(v *ReportGroup) string    { return v.Name }
func reportGroupARNKeyFn(v *ReportGroup) string { return v.Arn }

func reportKeyFn(v *Report) string         { return v.Arn }
func reportGroupArnKeyFn(v *Report) string { return v.ReportGroupArn }

func buildBatchKeyFn(v *BuildBatch) string        { return v.ID }
func buildBatchARNKeyFn(v *BuildBatch) string     { return v.Arn }
func buildBatchProjectKeyFn(v *BuildBatch) string { return v.ProjectName }

func commandExecutionKeyFn(v *CommandExecution) string        { return v.ID }
func commandExecutionSandboxKeyFn(v *CommandExecution) string { return v.SandboxID }

func sandboxKeyFn(v *Sandbox) string        { return v.ID }
func sandboxProjectKeyFn(v *Sandbox) string { return v.ProjectName }

func webhookKeyFn(v *Webhook) string { return v.ProjectName }

func sourceCredentialsKeyFn(v *SourceCredentials) string { return v.Arn }

// registerAllTables registers every backend resource table (and its
// secondary indexes) exactly once. It must be called during construction
// only, immediately after b.registry is created -- store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.projects = store.Register(b.registry, "projects", store.New(projectKeyFn))
	b.projectsByARN = b.projects.AddIndex("byARN", projectARNKeyFn)

	b.builds = store.Register(b.registry, "builds", store.New(buildKeyFn))
	b.buildsByARN = b.builds.AddIndex("byARN", buildARNKeyFn)
	b.buildsByProject = b.builds.AddIndex("byProject", buildProjectKeyFn)

	b.fleets = store.Register(b.registry, "fleets", store.New(fleetKeyFn))
	b.fleetsByARN = b.fleets.AddIndex("byARN", fleetARNKeyFn)

	b.reportGroups = store.Register(b.registry, "reportGroups", store.New(reportGroupKeyFn))
	b.reportGroupsByARN = b.reportGroups.AddIndex("byARN", reportGroupARNKeyFn)

	b.reports = store.Register(b.registry, "reports", store.New(reportKeyFn))
	b.reportsByGroup = b.reports.AddIndex("byGroup", reportGroupArnKeyFn)

	b.buildBatches = store.Register(b.registry, "buildBatches", store.New(buildBatchKeyFn))
	b.buildBatchesByARN = b.buildBatches.AddIndex("byARN", buildBatchARNKeyFn)
	b.buildBatchesByProject = b.buildBatches.AddIndex("byProject", buildBatchProjectKeyFn)

	b.commandExecutions = store.Register(b.registry, "commandExecutions", store.New(commandExecutionKeyFn))
	b.commandExecutionsBySandbox = b.commandExecutions.AddIndex("bySandbox", commandExecutionSandboxKeyFn)

	b.sandboxes = store.Register(b.registry, "sandboxes", store.New(sandboxKeyFn))
	b.sandboxesByProject = b.sandboxes.AddIndex("byProject", sandboxProjectKeyFn)

	b.webhooks = store.Register(b.registry, "webhooks", store.New(webhookKeyFn))

	b.sourceCredentials = store.Register(b.registry, "sourceCredentials", store.New(sourceCredentialsKeyFn))
}
