package amplify

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T (or nested map[string]map[string]*T / triple-nested
// map[string]map[string]map[string]*T) resource field on InMemoryBackend is
// replaced with a *store.Table[T], following the pattern established by
// services/sesv2 (commit history noted in sesv2/store_setup.go) and
// services/dax / services/vpclattice (direct + byParent Index).
//
// Every convertible value type here already carries its own identity as a
// real (non-json:"-") field: App.AppID, Branch.{AppID,BranchName},
// Job.{AppID,BranchName,JobID}, DomainAssociation.{AppID,DomainName},
// Webhook.WebhookID (+ AppID), BackendEnvironment.{AppID,EnvironmentName},
// Artifact.ArtifactID. So all seven tables below are "clean" tables
// registered directly on b.registry -- no ephemeral DTO registry (the
// services/ses / services/codecommit "dirty" pattern) is needed here.
//
// branches, jobs, domains, and backendEnvironments were previously nested
// maps keyed by appID then a child name (jobs nested a third level, by
// branchName then jobID). Each is flattened into a single Table keyed by a
// composite "<appID>|<child>" (jobs: "<appID>|<branchName>|<jobID>") string,
// with a secondary store.Index grouping by the parent scope for the "all
// children of parent X" lookups (ListBranches/ListJobs/ListDomainAssociations/
// ListBackendEnvironments) the nested maps used to answer directly. This
// mirrors the services/codecommit conversion (branches/commits nested under
// repositoryName, flattened to "<repo>|<child>" + a "byRepo" index).
//
// webhooksByApp (a map[string][]string reverse-lookup of webhookID by appID)
// is not a map[string]*T at all -- it is a grouping map -- so per the
// pkgs/store rules it becomes a store.Index on the webhooks table (keyed by
// Webhook.AppID) instead of its own Table. It is eliminated as a struct
// field entirely; ListWebhooks and DeleteWebhook's index-maintenance code
// now go through webhooksByApp (the *store.Index[Webhook]) instead.
//
// apps and artifacts were already flat (not nested), so each is a direct
// *store.Table[T] keyed by its own real identity field (AppID / ArtifactID).
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func appKeyFn(v *App) string { return v.AppID }

// branchKey/jobKey/domainKey/backendEnvKey build the composite store.Table
// primary key ("parent|child", jobKey: "appID|branchName|jobID") shared by
// every flattened nested-map collection. Access sites use these directly so
// the exact same key is used for Put/Get/Has/Delete.
func branchKey(appID, branchName string) string { return appID + "|" + branchName }
func branchKeyFn(v *Branch) string              { return branchKey(v.AppID, v.BranchName) }
func branchAppIndexKeyFn(v *Branch) string      { return v.AppID }

func jobKey(appID, branchName, jobID string) string {
	return appID + "|" + branchName + "|" + jobID
}
func jobKeyFn(v *Job) string            { return jobKey(v.AppID, v.BranchName, v.JobID) }
func jobBranchIndexKeyFn(v *Job) string { return branchKey(v.AppID, v.BranchName) }

func domainKey(appID, domainName string) string       { return appID + "|" + domainName }
func domainKeyFn(v *DomainAssociation) string         { return domainKey(v.AppID, v.DomainName) }
func domainAppIndexKeyFn(v *DomainAssociation) string { return v.AppID }

func webhookKeyFn(v *Webhook) string         { return v.WebhookID }
func webhookAppIndexKeyFn(v *Webhook) string { return v.AppID }

func backendEnvKey(appID, environmentName string) string { return appID + "|" + environmentName }
func backendEnvKeyFn(v *BackendEnvironment) string {
	return backendEnvKey(v.AppID, v.EnvironmentName)
}
func backendEnvAppIndexKeyFn(v *BackendEnvironment) string { return v.AppID }

func artifactKeyFn(v *Artifact) string { return v.ArtifactID }

// registerAllTables registers every converted resource collection on
// b.registry exactly once. It must be called during construction only
// (immediately after b.registry is created), never on every reset -- runtime
// resets go through b.registry.ResetAll() instead.
func registerAllTables(b *InMemoryBackend) {
	b.apps = store.Register(b.registry, "apps", store.New(appKeyFn))

	b.branches = store.Register(b.registry, "branches", store.New(branchKeyFn))
	b.branchesByApp = b.branches.AddIndex("byApp", branchAppIndexKeyFn)

	b.jobs = store.Register(b.registry, "jobs", store.New(jobKeyFn))
	b.jobsByBranch = b.jobs.AddIndex("byBranch", jobBranchIndexKeyFn)

	b.domains = store.Register(b.registry, "domains", store.New(domainKeyFn))
	b.domainsByApp = b.domains.AddIndex("byApp", domainAppIndexKeyFn)

	b.webhooks = store.Register(b.registry, "webhooks", store.New(webhookKeyFn))
	b.webhooksByApp = b.webhooks.AddIndex("byApp", webhookAppIndexKeyFn)

	b.backendEnvironments = store.Register(
		b.registry, "backendEnvironments", store.New(backendEnvKeyFn),
	)
	b.backendEnvironmentsByApp = b.backendEnvironments.AddIndex("byApp", backendEnvAppIndexKeyFn)

	b.artifacts = store.Register(b.registry, "artifacts", store.New(artifactKeyFn))
}
