package amplify_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amplify"
)

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including the pre-Phase-3.3
// format, which had no persistence at all and so decodes with Version == 0)
// is discarded cleanly rather than partially decoded: the backend resets to
// empty state and Restore returns no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.CreateApp("seed-app", "", "", "", nil)
	require.NoError(t, err)

	// A syntactically valid but version-less/mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	apps, _, err := b.ListApps("", 0)
	require.NoError(t, err)
	assert.Empty(t, apps)
}

func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	h := amplify.NewHandler(newTestBackend())

	_, err := h.Backend.CreateApp("delegate-app", "", "", "", nil)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := amplify.NewHandler(newTestBackend())
	require.NoError(t, h2.Restore(t.Context(), snap))

	apps, _, err := h2.Backend.ListApps("", 0)
	require.NoError(t, err)
	require.Len(t, apps, 1)
	assert.Equal(t, "delegate-app", apps[0].Name)
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every resource family the Phase 3.3 pkgs/store
// conversion touched: apps, branches (composite-keyed, byApp index), jobs
// (triple-composite-keyed, byBranch index), domain associations
// (composite-keyed, byApp index), webhooks (direct-keyed, byApp index
// replacing the old webhooksByApp map), and backend environments
// (composite-keyed, byApp index). There are no other persisted fields left
// as plain maps -- every InMemoryBackend resource field converted to a
// store.Table.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := newTestBackend()

	app, err := original.CreateApp("my-app", "desc", "https://github.com/x/y", "WEB", map[string]string{
		"env": "prod",
	})
	require.NoError(t, err)

	branch, err := original.CreateBranch(
		app.AppID, "main", "main branch", "PRODUCTION", true, map[string]string{"k": "v"},
	)
	require.NoError(t, err)

	job, err := original.StartJob(app.AppID, branch.BranchName, "RELEASE", "abc123", "commit msg")
	require.NoError(t, err)

	domain, err := original.CreateDomainAssociation(
		app.AppID, "example.com",
		[]amplify.SubDomainSetting{{Prefix: "www", BranchName: branch.BranchName}},
		true,
	)
	require.NoError(t, err)

	webhook, err := original.CreateWebhook(app.AppID, branch.BranchName, "wh desc")
	require.NoError(t, err)

	env, err := original.CreateBackendEnvironment(app.AppID, "dev", "my-stack", "s3://artifacts")
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := newTestBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	gotApp, err := fresh.GetApp(app.AppID)
	require.NoError(t, err)
	assert.Equal(t, "my-app", gotApp.Name)
	assert.Equal(t, "prod", gotApp.Tags.Clone()["env"])

	gotBranch, err := fresh.GetBranch(app.AppID, branch.BranchName)
	require.NoError(t, err)
	assert.Equal(t, "main branch", gotBranch.Description)
	assert.Equal(t, "v", gotBranch.Tags.Clone()["k"])

	branches, _, err := fresh.ListBranches(app.AppID, "", 0)
	require.NoError(t, err)
	require.Len(t, branches, 1)

	gotJob, err := fresh.GetJob(app.AppID, branch.BranchName, job.JobID)
	require.NoError(t, err)
	assert.Equal(t, "abc123", gotJob.CommitID)

	jobs, _, err := fresh.ListJobs(app.AppID, branch.BranchName, "", 0)
	require.NoError(t, err)
	require.Len(t, jobs, 1)

	gotDomain, err := fresh.GetDomainAssociation(app.AppID, domain.DomainName)
	require.NoError(t, err)
	require.Len(t, gotDomain.SubDomains, 1)
	assert.Equal(t, "www", gotDomain.SubDomains[0].SubDomainSetting.Prefix)

	domains, _, err := fresh.ListDomainAssociations(app.AppID, "", 0)
	require.NoError(t, err)
	require.Len(t, domains, 1)

	gotWebhook, err := fresh.GetWebhook(webhook.WebhookID)
	require.NoError(t, err)
	assert.Equal(t, "wh desc", gotWebhook.Description)

	webhooks, _, err := fresh.ListWebhooks(app.AppID, "", 0)
	require.NoError(t, err)
	require.Len(t, webhooks, 1)

	gotEnv, err := fresh.GetBackendEnvironment(app.AppID, env.EnvironmentName)
	require.NoError(t, err)
	assert.Equal(t, "my-stack", gotEnv.StackName)

	envs, _, err := fresh.ListBackendEnvironments(app.AppID, "", 0)
	require.NoError(t, err)
	require.Len(t, envs, 1)
}

// TestInMemoryBackend_DeleteApp_CascadesBranches exercises the cascade-delete
// path added by the Phase 3.3 conversion: the branchesByApp index must be
// cloned before iterating, since deleting from the underlying table mutates
// the index's backing slice.
func TestInMemoryBackend_DeleteApp_CascadesBranches(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	app, err := b.CreateApp("cascade-app", "", "", "", nil)
	require.NoError(t, err)

	_, err = b.CreateBranch(app.AppID, "b1", "", "", false, nil)
	require.NoError(t, err)
	_, err = b.CreateBranch(app.AppID, "b2", "", "", false, nil)
	require.NoError(t, err)

	require.NoError(t, b.DeleteApp(app.AppID))

	app2, err := b.CreateApp("cascade-app-2", "", "", "", nil)
	require.NoError(t, err)

	branches, _, err := b.ListBranches(app2.AppID, "", 0)
	require.NoError(t, err)
	assert.Empty(t, branches)

	_, _, err = b.ListBranches(app.AppID, "", 0)
	require.Error(t, err)
}
