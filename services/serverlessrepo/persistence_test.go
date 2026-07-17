package serverlessrepo_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/serverlessrepo"
)

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend("123456789012", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including the pre-Phase-3.3
// format, which decodes with Version == 0) is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreateApplication(
		"seed-app", "desc", "author", "", "", nil, "", "", "",
	)
	require.NoError(t, err)

	// A syntactically valid but version-less/mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, serverlessrepo.ApplicationCount(b))
}

func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	h := serverlessrepo.NewHandler(serverlessrepo.NewInMemoryBackend("123456789012", "us-east-1"))
	_, err := h.Backend.CreateApplication(
		"delegate-app", "desc", "author", "", "", nil, "", "", "",
	)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := serverlessrepo.NewHandler(serverlessrepo.NewInMemoryBackend("123456789012", "us-east-1"))
	require.NoError(t, h2.Restore(t.Context(), snap))

	apps := h2.Backend.ListApplications()
	require.Len(t, apps, 1)
	assert.Equal(t, "delegate-app", apps[0].Name)
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every resource family the Phase 3.3 pkgs/store
// conversion touched: the clean applications table, the three flattened
// dirty tables (appVersions, cfTemplates, cfChangeSets) and their byApp
// secondary indexes, plus the two plain maps left un-converted (appPolicies,
// appDependencies).
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := serverlessrepo.NewInMemoryBackend("123456789012", "us-east-1")

	app, err := original.CreateApplication(
		"my-app", "a description", "an author", "https://example.com/src", "1.0.0",
		[]string{"serverless"}, "https://example.com", "https://example.com/license", "MIT",
	)
	require.NoError(t, err)
	assert.Equal(t, "my-app", app.Name)

	_, err = original.SetApplicationReadmeURL("my-app", "https://example.com/readme")
	require.NoError(t, err)

	v1, err := original.CreateApplicationVersion("my-app", "1.0.0", "https://example.com/src", "")
	require.NoError(t, err)
	assert.Equal(t, "1.0.0", v1.SemanticVersion)

	_, err = original.CreateApplicationVersion("my-app", "2.0.0", "https://example.com/src2", "")
	require.NoError(t, err)

	// A second application whose version shares the same semantic version as
	// my-app's -- this only round-trips correctly if appVersions really is
	// keyed by the composite "appName#semanticVersion", not by
	// semanticVersion alone.
	_, err = original.CreateApplication(
		"other-app", "other description", "other author", "https://example.com/src", "",
		nil, "", "", "",
	)
	require.NoError(t, err)

	_, err = original.CreateApplicationVersion("other-app", "1.0.0", "https://example.com/other-src", "")
	require.NoError(t, err)

	tmpl, err := original.CreateCloudFormationTemplate("my-app", "1.0.0")
	require.NoError(t, err)

	cs, err := original.CreateCloudFormationChangeSetWithOptions(
		"my-app", "my-stack", "my-changeset", "1.0.0",
		serverlessrepo.CreateCloudFormationChangeSetOptions{
			Capabilities: []string{"CAPABILITY_IAM"},
			Tags:         []serverlessrepo.Tag{{Key: "env", Value: "test"}},
		},
	)
	require.NoError(t, err)

	// A second application reusing the identical changeSetName/stackName --
	// only round-trips correctly if cfChangeSets is keyed by the composite
	// "appName#changeSetID", not by changeSetID alone.
	otherCS, err := original.CreateCloudFormationChangeSetWithOptions(
		"other-app", "my-stack", "my-changeset", "1.0.0",
		serverlessrepo.CreateCloudFormationChangeSetOptions{},
	)
	require.NoError(t, err)

	_, err = original.PutApplicationPolicy("my-app", []*serverlessrepo.ApplicationPolicyStatement{
		{Actions: []string{"Deploy"}, Principals: []string{"123456789012"}},
	})
	require.NoError(t, err)

	dependency := serverlessrepo.ApplicationDependency{
		ApplicationID:   "arn:aws:serverlessrepo:us-east-1:123456789012:applications/nested-app",
		SemanticVersion: "3.0.0",
	}
	require.NoError(t, original.AddApplicationDependencyInternal("my-app", "1.0.0", dependency))

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := serverlessrepo.NewInMemoryBackend("000000000000", "us-west-2")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	restoredApp, err := fresh.GetApplication("my-app")
	require.NoError(t, err)
	assert.Equal(t, "a description", restoredApp.Description)
	assert.Equal(t, "https://example.com/readme", restoredApp.ReadmeURL)
	assert.Equal(t, []string{"serverless"}, restoredApp.Labels)

	_, err = fresh.GetApplication("other-app")
	require.NoError(t, err)

	restoredV1, err := fresh.GetApplicationVersion("my-app", "1.0.0")
	require.NoError(t, err)
	assert.Equal(t, "https://example.com/src", restoredV1.SourceCodeURL)

	restoredOtherV1, err := fresh.GetApplicationVersion("other-app", "1.0.0")
	require.NoError(t, err)
	assert.Equal(t, "https://example.com/other-src", restoredOtherV1.SourceCodeURL)

	versions, err := fresh.ListApplicationVersions("my-app")
	require.NoError(t, err)
	require.Len(t, versions, 2)
	assert.Equal(t, "1.0.0", versions[0].SemanticVersion)
	assert.Equal(t, "2.0.0", versions[1].SemanticVersion)

	restoredTmpl, err := fresh.GetCloudFormationTemplate("my-app", tmpl.TemplateID)
	require.NoError(t, err)
	assert.Equal(t, tmpl.TemplateURL, restoredTmpl.TemplateURL)

	assert.Equal(t, 1, serverlessrepo.TemplateCount(fresh, "my-app"))
	assert.Equal(t, 1, serverlessrepo.ChangeSetCount(fresh, "my-app"))
	assert.Equal(t, 1, serverlessrepo.ChangeSetCount(fresh, "other-app"))
	// The identical stackName/changeSetName for both apps means the derived
	// ChangeSetID ARN is identical too; only the appName#changeSetID
	// composite primary key keeps the two change sets from colliding.
	assert.Equal(t, cs.ChangeSetID, otherCS.ChangeSetID)

	policy, err := fresh.GetApplicationPolicy("my-app")
	require.NoError(t, err)
	require.Len(t, policy, 1)
	assert.Equal(t, []string{"Deploy"}, policy[0].Actions)

	deps, err := fresh.ListApplicationDependencies("my-app", "1.0.0")
	require.NoError(t, err)
	require.Len(t, deps, 1)
	assert.Equal(t, "3.0.0", deps[0].SemanticVersion)
}

// TestInMemoryBackend_DeleteApplication_CascadesVersionsTemplatesChangeSets
// exercises the cascade-delete path added by the Phase 3.3 conversion: each
// byApp index must be cloned before iterating, since deleting from the
// underlying table mutates the index's backing slice.
func TestInMemoryBackend_DeleteApplication_CascadesVersionsTemplatesChangeSets(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateApplication("app1", "desc", "author", "https://example.com/src", "", nil, "", "", "")
	require.NoError(t, err)

	_, err = b.CreateApplicationVersion("app1", "1.0.0", "https://example.com/src", "")
	require.NoError(t, err)
	_, err = b.CreateApplicationVersion("app1", "2.0.0", "https://example.com/src", "")
	require.NoError(t, err)

	_, err = b.CreateCloudFormationTemplate("app1", "1.0.0")
	require.NoError(t, err)
	_, err = b.CreateCloudFormationTemplate("app1", "2.0.0")
	require.NoError(t, err)

	_, err = b.CreateCloudFormationChangeSetWithOptions(
		"app1", "stack1", "cs1", "1.0.0", serverlessrepo.CreateCloudFormationChangeSetOptions{},
	)
	require.NoError(t, err)
	_, err = b.CreateCloudFormationChangeSetWithOptions(
		"app1", "stack1", "cs2", "1.0.0", serverlessrepo.CreateCloudFormationChangeSetOptions{},
	)
	require.NoError(t, err)

	require.NoError(t, b.DeleteApplication("app1"))

	assert.Equal(t, 0, serverlessrepo.VersionCount(b, "app1"))
	assert.Equal(t, 0, serverlessrepo.TemplateCount(b, "app1"))
	assert.Equal(t, 0, serverlessrepo.ChangeSetCount(b, "app1"))

	// Recreating the application must not resurrect any of the deleted
	// children under the byApp indexes.
	_, err = b.CreateApplication("app1", "desc2", "author2", "https://example.com/src", "", nil, "", "", "")
	require.NoError(t, err)

	versions, err := b.ListApplicationVersions("app1")
	require.NoError(t, err)
	assert.Empty(t, versions)
}

func TestServerlessRepoPersistence_WithTemplateAndPolicy(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateApplication("app1", "desc1", "author1", "", "1.0.0", nil, "", "", "")
	require.NoError(t, err)

	_, err = b.CreateApplicationVersion("app1", "1.0.0", "https://github.com/example", "")
	require.NoError(t, err)

	_, err = b.CreateCloudFormationTemplate("app1", "1.0.0")
	require.NoError(t, err)

	_, err = b.PutApplicationPolicy("app1", []*serverlessrepo.ApplicationPolicyStatement{
		{Actions: []string{"deploy"}, Principals: []string{"*"}},
	})
	require.NoError(t, err)

	// Snapshot
	h := serverlessrepo.NewHandler(b)
	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	// Restore into a fresh backend
	b2 := serverlessrepo.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := serverlessrepo.NewHandler(b2)
	require.NoError(t, h2.Restore(t.Context(), snap))

	assert.Equal(t, 1, serverlessrepo.ApplicationCount(b2))
	assert.Equal(t, 1, serverlessrepo.VersionCount(b2, "app1"))
	assert.Equal(t, 1, serverlessrepo.TemplateCount(b2, "app1"))
	assert.Equal(t, 1, serverlessrepo.PolicyStatementCount(b2, "app1"))
}

func TestServerlessRepoPersistence(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateApplication("app1", "desc1", "author1", "", "1.0.0", nil, "", "", "")
	require.NoError(t, err)

	_, err = b.CreateApplication("app2", "desc2", "author2", "", "2.0.0", nil, "", "", "")
	require.NoError(t, err)

	// Snapshot
	h := serverlessrepo.NewHandler(b)
	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	// Restore into a fresh backend
	b2 := serverlessrepo.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := serverlessrepo.NewHandler(b2)
	require.NoError(t, h2.Restore(t.Context(), snap))

	apps := b2.ListApplications()
	require.Len(t, apps, 2)
	assert.Equal(t, "app1", apps[0].Name)
	assert.Equal(t, "app2", apps[1].Name)
}

// TestSnapshot_RestoreAllResourceTypes exercises a full Snapshot->Restore
// round trip via the HTTP-facing Handler, covering every resource family:
// applications, versions, CloudFormation templates and change sets, and
// nested application dependencies.
func TestSnapshot_RestoreAllResourceTypes(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("snap-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	_, err = b.CreateApplicationVersion("snap-app", "1.0.0", "https://example.com", "")
	require.NoError(t, err)

	_, err = b.CreateCloudFormationTemplate("snap-app", "1.0.0")
	require.NoError(t, err)

	_, err = b.CreateCloudFormationChangeSet("snap-app", "stack", "cs", "1.0.0")
	require.NoError(t, err)

	require.NoError(t, b.AddApplicationDependencyInternal("snap-app", "1.0.0", serverlessrepo.ApplicationDependency{
		ApplicationID:   "arn:aws:serverlessrepo:us-east-1:000000000000:applications/child",
		SemanticVersion: "2.0.0",
	}))

	snap := b.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	b2 := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, serverlessrepo.ApplicationCount(b2))
	assert.Equal(t, 1, serverlessrepo.VersionCount(b2, "snap-app"))
	assert.Equal(t, 1, serverlessrepo.TemplateCount(b2, "snap-app"))
	assert.Equal(t, 1, serverlessrepo.ChangeSetCount(b2, "snap-app"))

	deps, depErr := b2.ListApplicationDependencies("snap-app", "1.0.0")
	require.NoError(t, depErr)
	assert.Len(t, deps, 1)
	assert.Equal(t, "2.0.0", deps[0].SemanticVersion)
}

// TestSnapshot_DeepCopiesPolicies verifies that Snapshot deep-copies policy
// statements: mutating the live backend's policy after taking a snapshot must
// not affect what a subsequent Restore of that snapshot observes.
func TestSnapshot_DeepCopiesPolicies(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	_, err := b.CreateApplication("my-app", "desc", "author", "", "", nil, "", "", "")
	require.NoError(t, err)

	_, err = b.PutApplicationPolicy("my-app", []*serverlessrepo.ApplicationPolicyStatement{
		{Actions: []string{"deploy"}, Principals: []string{"*"}},
	})
	require.NoError(t, err)

	// Snapshot, then mutate original, restore should have original
	snap := b.Snapshot(t.Context())

	// Overwrite policy
	_, err = b.PutApplicationPolicy("my-app", []*serverlessrepo.ApplicationPolicyStatement{
		{Actions: []string{"Deploy"}, Principals: []string{"111"}},
	})
	require.NoError(t, err)

	// Restore into a new backend
	b2 := serverlessrepo.NewInMemoryBackend(testAccountID, "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	stmts, err := b2.GetApplicationPolicy("my-app")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	assert.Equal(t, "deploy", stmts[0].Actions[0])
}
