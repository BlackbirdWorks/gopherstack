package codebuild_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

// newPersistenceTestBackend creates a backend with one populated entry in
// every store.Table (and, transitively, every secondary store.Index) plus
// the one raw map left un-converted (resourcePolicies), so a Snapshot from it
// exercises the entire persisted surface of the backend.
func newPersistenceTestBackend(t *testing.T) *codebuild.InMemoryBackend {
	t.Helper()

	b := codebuild.NewInMemoryBackend("000000000000", "us-east-1")

	proj, err := b.CreateProject(codebuild.ProjectConfig{
		Name: "proj1",
		Source: &codebuild.ProjectSource{
			Type:     "GITHUB",
			Location: "https://github.com/example/repo",
		},
		Artifacts: &codebuild.ProjectArtifacts{Type: "NO_ARTIFACTS"},
		Environment: &codebuild.ProjectEnvironment{
			Type: "LINUX_CONTAINER", Image: "img", ComputeType: "BUILD_GENERAL1_SMALL",
		},
		Tags: map[string]string{"env": "test"},
	})
	require.NoError(t, err)

	_, err = b.StartBuild(proj.Name, codebuild.StartBuildConfig{})
	require.NoError(t, err)

	_, err = b.CreateFleet("fleet1", 1, codebuild.CreateFleetOptions{
		ComputeType: "BUILD_GENERAL1_SMALL", EnvironmentType: "LINUX_CONTAINER",
		Tags: map[string]string{"k": "v"},
	})
	require.NoError(t, err)

	rg, err := b.CreateReportGroup("rg1", "TEST", codebuild.ReportExportConfig{ExportConfigType: "NO_EXPORT"}, nil)
	require.NoError(t, err)

	b.AddReportInternal(&codebuild.Report{
		Arn:            "arn:aws:codebuild:us-east-1:000000000000:report/rg1:r1",
		ReportGroupArn: rg.Arn,
		Status:         "SUCCEEDED",
	})

	_, err = b.StartBuildBatch(proj.Name)
	require.NoError(t, err)

	sb, err := b.StartSandbox(proj.Name)
	require.NoError(t, err)

	_, err = b.StartCommandExecution(sb.ID, "echo hi", "SHELL")
	require.NoError(t, err)

	_, err = b.CreateWebhook(proj.Name, "main", "GITHUB", nil, codebuild.WebhookConfig{})
	require.NoError(t, err)

	_, err = b.ImportSourceCredentials("PERSONAL_ACCESS_TOKEN", "GITHUB", "tok")
	require.NoError(t, err)

	require.NoError(t, b.PutResourcePolicy(rg.Arn, `{"Version":"2012-10-17"}`))

	return b
}

// TestInMemoryBackend_SnapshotRestore_FullState round-trips every store.Table
// (projects, builds, fleets, reportGroups, reports, buildBatches,
// commandExecutions, sandboxes, webhooks, sourceCredentials), every secondary
// store.Index derived from them, and the one raw map left un-converted
// (resourcePolicies) through Snapshot -> Restore into a fresh backend.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := newPersistenceTestBackend(t)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// projects table + projectsByARN index (lookupByNameOrARN via BatchGetProjects).
	projects, notFound := fresh.BatchGetProjects([]string{"proj1"})
	require.Empty(t, notFound)
	require.Len(t, projects, 1)
	assert.Equal(t, "test", projects[0].Tags["env"])

	// projectsByARN index, looked up through BatchGetProjects' name-or-ARN fallback.
	projectsByARN, notFound := fresh.BatchGetProjects([]string{projects[0].Arn})
	require.Empty(t, notFound)
	require.Len(t, projectsByARN, 1)
	assert.Equal(t, "proj1", projectsByARN[0].Name)

	// builds table + buildsByARN + buildsByProject indexes.
	buildIDs, err := fresh.ListBuildsForProject("proj1")
	require.NoError(t, err)
	require.Len(t, buildIDs, 1)

	builds, notFound := fresh.BatchGetBuilds([]string{buildIDs[0]})
	require.Empty(t, notFound)
	require.Len(t, builds, 1)

	buildsByARN, notFound := fresh.BatchGetBuilds([]string{builds[0].Arn})
	require.Empty(t, notFound)
	require.Len(t, buildsByARN, 1)
	assert.Equal(t, builds[0].ID, buildsByARN[0].ID)

	// fleets table + fleetsByARN index.
	fleets, notFound := fresh.BatchGetFleets([]string{"fleet1"})
	require.Empty(t, notFound)
	require.Len(t, fleets, 1)
	assert.Equal(t, "v", fleets[0].Tags["k"])

	fleetsByARN, notFound := fresh.BatchGetFleets([]string{fleets[0].Arn})
	require.Empty(t, notFound)
	require.Len(t, fleetsByARN, 1)

	// reportGroups table + reportGroupsByARN index.
	rgArn := "arn:aws:codebuild:us-east-1:000000000000:report-group/rg1"
	reportGroups, notFound := fresh.BatchGetReportGroups([]string{rgArn})
	require.Empty(t, notFound)
	require.Len(t, reportGroups, 1)
	assert.Equal(t, "rg1", reportGroups[0].Name)

	// reports table + reportsByGroup index.
	reportArns := fresh.ListReportsForReportGroup(rgArn, "")
	require.Len(t, reportArns, 1)
	assert.Equal(t, "arn:aws:codebuild:us-east-1:000000000000:report/rg1:r1", reportArns[0])

	// buildBatches table + buildBatchesByARN + buildBatchesByProject indexes.
	batchIDs, err := fresh.ListBuildBatchesForProject("proj1", "")
	require.NoError(t, err)
	require.Len(t, batchIDs, 1)

	batches, notFound := fresh.BatchGetBuildBatches([]string{batchIDs[0]})
	require.Empty(t, notFound)
	require.Len(t, batches, 1)

	// sandboxes table + sandboxesByProject index.
	sandboxIDs, err := fresh.ListSandboxesForProject("proj1")
	require.NoError(t, err)
	require.Len(t, sandboxIDs, 1)

	// commandExecutions table + commandExecutionsBySandbox index.
	ces, err := fresh.ListCommandExecutionsForSandbox(sandboxIDs[0])
	require.NoError(t, err)
	require.Len(t, ces, 1)
	assert.Equal(t, "echo hi", ces[0].Command)

	// webhooks table (keyed directly by projectName, no secondary index).
	_, err = fresh.CreateWebhook("proj1", "main", "GITHUB", nil, codebuild.WebhookConfig{})
	require.ErrorIs(t, err, codebuild.ErrAlreadyExists, "webhook must already exist after restore")

	// sourceCredentials table.
	creds := fresh.ListSourceCredentials()
	require.Len(t, creds, 1)
	assert.Equal(t, "GITHUB", creds[0].ServerType)

	// resourcePolicies raw map (left un-converted, persisted alongside the tables).
	policy, err := fresh.GetResourcePolicy(rgArn)
	require.NoError(t, err)
	assert.JSONEq(t, `{"Version":"2012-10-17"}`, policy)

	// Sanity: account/region carried through too.
	assert.Equal(t, "us-east-1", fresh.Region())
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including the pre-Phase-3.3
// format, which decodes with Version == 0) is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := newPersistenceTestBackend(t)

	// A syntactically valid but version-less/mismatched snapshot.
	err := b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, b.ProjectCount())
	assert.Equal(t, 0, b.BuildCount())

	projects := b.ListProjects()
	assert.Empty(t, projects)

	_, err = b.GetResourcePolicy("arn:aws:codebuild:us-east-1:000000000000:report-group/rg1")
	require.ErrorIs(t, err, codebuild.ErrNotFound)
}

// TestInMemoryBackend_RestoreInvalidData verifies malformed JSON surfaces as
// an error rather than being silently discarded (that path is reserved for a
// syntactically valid but version-mismatched snapshot; see
// TestInMemoryBackend_RestoreVersionMismatch).
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestHandler_SnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to the backend (the wiring cli.go's generic setupPersistence
// relies on).
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	h := codebuild.NewHandler(newPersistenceTestBackend(t))

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := codebuild.NewHandler(codebuild.NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, h2.Restore(t.Context(), snap))

	projects := h2.Backend.ListProjects()
	require.Len(t, projects, 1)
	assert.Equal(t, "proj1", projects[0])
}

// TestCodeBuild_PersistenceSnapshotRestore round-trips a project and one of its
// builds through Snapshot -> Restore, verifying the buildsByProject index survives.
func TestCodeBuild_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	b := codebuild.NewInMemoryBackend("000000000000", "us-east-1")

	// Create a project and start a build.
	src := codebuild.ProjectSource{Type: "NO_SOURCE"}
	arts := codebuild.ProjectArtifacts{Type: "NO_ARTIFACTS"}
	env := codebuild.ProjectEnvironment{
		Type:        "LINUX_CONTAINER",
		Image:       "aws/codebuild/standard:5.0",
		ComputeType: "BUILD_GENERAL1_SMALL",
	}
	_, err := b.CreateProject(codebuild.ProjectConfig{
		Name:        "snap-proj",
		ServiceRole: "arn:aws:iam::000000000000:role/codebuild",
		Source:      &src,
		Artifacts:   &arts,
		Environment: &env,
	})
	require.NoError(t, err)

	build, err := b.StartBuild("snap-proj", codebuild.StartBuildConfig{})
	require.NoError(t, err)
	require.NotEmpty(t, build.ID)

	// Snapshot and restore.
	h := codebuild.NewHandler(b)
	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := codebuild.NewHandler(b2)
	require.NoError(t, h2.Restore(t.Context(), snap))

	// Project is restored.
	projs := b2.ListProjects()
	require.Len(t, projs, 1)
	assert.Equal(t, "snap-proj", projs[0])

	// Build is restored.
	builds, notFound := b2.BatchGetBuilds([]string{build.ID})
	assert.Empty(t, notFound)
	require.Len(t, builds, 1)
	assert.Equal(t, "snap-proj", builds[0].ProjectName)

	// ListBuildsForProject uses the project index.
	ids, err := b2.ListBuildsForProject("snap-proj")
	require.NoError(t, err)
	require.Len(t, ids, 1)
	assert.Equal(t, build.ID, ids[0])
}

// TestHandler_NewOperations_PersistenceRoundTrip round-trips fleets, report groups,
// reports, build batches, command executions, and sandboxes through Snapshot -> Restore.
func TestHandler_NewOperations_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := codebuild.NewInMemoryBackend("000000000000", "us-east-1")

	// Seed a fleet.
	_, err := b.CreateFleet("persist-fleet", 2, codebuild.CreateFleetOptions{
		ComputeType: "BUILD_GENERAL1_SMALL", EnvironmentType: "LINUX_CONTAINER",
	})
	require.NoError(t, err)

	// Seed a report group.
	_, err = b.CreateReportGroup("persist-rg", "TEST", codebuild.ReportExportConfig{}, nil)
	require.NoError(t, err)

	// Seed a report.
	b.AddReportInternal(&codebuild.Report{
		Arn:    "arn:aws:codebuild:us-east-1:000000000000:report/persist-rg:r1",
		Status: "SUCCEEDED",
	})

	// Seed a build batch.
	b.AddBuildBatchInternal(&codebuild.BuildBatch{
		ID:               "persist-proj:bb1",
		Arn:              "arn:aws:codebuild:us-east-1:000000000000:build-batch/persist-proj:bb1",
		ProjectName:      "persist-proj",
		BuildBatchStatus: "SUCCEEDED",
	})

	// Seed a command execution.
	b.AddCommandExecutionInternal(&codebuild.CommandExecution{
		ID:        "ce1",
		SandboxID: "sb1",
		Status:    "SUCCEEDED",
	})

	// Seed a sandbox.
	b.AddSandboxInternal(&codebuild.Sandbox{
		ID:     "sb1",
		Arn:    "arn:aws:codebuild:us-east-1:000000000000:sandbox/sb1",
		Status: "RUNNING",
	})

	// Snapshot and restore.
	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := codebuild.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Verify fleet is restored.
	fleets, notFound := b2.BatchGetFleets([]string{"persist-fleet"})
	assert.Empty(t, notFound)
	require.Len(t, fleets, 1)
	assert.Equal(t, "persist-fleet", fleets[0].Name)

	// Verify report group is restored.
	rgs, notFound := b2.BatchGetReportGroups(
		[]string{"arn:aws:codebuild:us-east-1:000000000000:report-group/persist-rg"},
	)
	assert.Empty(t, notFound)
	require.Len(t, rgs, 1)
	assert.Equal(t, "persist-rg", rgs[0].Name)

	// Verify report is restored.
	reports, notFound := b2.BatchGetReports([]string{"arn:aws:codebuild:us-east-1:000000000000:report/persist-rg:r1"})
	assert.Empty(t, notFound)
	require.Len(t, reports, 1)

	// Verify build batch is restored.
	batches, notFound := b2.BatchGetBuildBatches([]string{"persist-proj:bb1"})
	assert.Empty(t, notFound)
	require.Len(t, batches, 1)

	// Verify command execution is restored.
	ces, notFound := b2.BatchGetCommandExecutions("sb1", []string{"ce1"})
	assert.Empty(t, notFound)
	require.Len(t, ces, 1)

	// Verify sandbox is restored.
	sandboxes, notFound := b2.BatchGetSandboxes([]string{"sb1"})
	assert.Empty(t, notFound)
	require.Len(t, sandboxes, 1)
}
