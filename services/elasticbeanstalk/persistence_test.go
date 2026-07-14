package elasticbeanstalk_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
)

func TestElasticBeanstalk_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *elasticbeanstalk.InMemoryBackend)
		verify func(t *testing.T, b *elasticbeanstalk.InMemoryBackend)
		name   string
	}{
		{
			name:  "empty",
			setup: func(_ *elasticbeanstalk.InMemoryBackend) {},
			verify: func(t *testing.T, b *elasticbeanstalk.InMemoryBackend) {
				t.Helper()

				assert.Empty(t, b.DescribeApplications(context.Background(), nil))
			},
		},
		{
			name: "application_preserved",
			setup: func(b *elasticbeanstalk.InMemoryBackend) {
				_, _ = b.CreateApplication(context.Background(), "my-app", "desc", map[string]string{"env": "prod"})
			},
			verify: func(t *testing.T, b *elasticbeanstalk.InMemoryBackend) {
				t.Helper()

				apps := b.DescribeApplications(context.Background(), nil)
				require.Len(t, apps, 1)
				assert.Equal(t, "my-app", apps[0].ApplicationName)
				assert.Equal(t, "prod", apps[0].Tags["env"])
				// Verify ARN index rebuilt - tag ops should work
				gotTags, err := b.ListTagsForResource(context.Background(), apps[0].ApplicationARN)
				require.NoError(t, err)
				assert.Equal(t, "prod", gotTags["env"])
			},
		},
		{
			name: "batch1_environment_and_version_state_preserved",
			setup: func(b *elasticbeanstalk.InMemoryBackend) {
				ctx := context.Background()
				_, _ = b.CreateEnvironment(
					ctx, "app", "env", "stack", "", nil,
					elasticbeanstalk.CreateEnvironmentParams{
						VersionLabel: "v1",
						OptionSettings: []elasticbeanstalk.OptionSetting{
							{Namespace: "aws:ec2:vpc", OptionName: "VPCId", Value: "vpc-1"},
						},
					},
				)
				_, _ = b.CreateApplicationVersionWithParams(ctx, "app", "v1",
					elasticbeanstalk.ApplicationVersionParams{
						Process:               true,
						AutoCreateApplication: true,
						SourceBuildInformation: &elasticbeanstalk.SourceBuildInformation{
							SourceType: "CodeCommit", SourceRepository: "repo", SourceLocation: "main",
						},
					},
				)
			},
			verify: func(t *testing.T, b *elasticbeanstalk.InMemoryBackend) {
				t.Helper()

				envs := b.DescribeEnvironments(context.Background(), "app", []string{"env"}, nil)
				require.Len(t, envs, 1)
				assert.Equal(t, "v1", envs[0].VersionLabel)
				assert.Equal(t, "vpc-1", envs[0].OptionSettings[0].Value)

				versions := b.DescribeApplicationVersions(context.Background(), "app", []string{"v1"})
				require.Len(t, versions, 1)
				assert.Equal(t, "CodeCommit", versions[0].SourceBuildInformation.SourceType)
			},
		},
		{
			name:   "full_state_round_trip",
			setup:  setupFullState,
			verify: verifyFullState,
		},
		{
			name: "swap_cnames_survives_restore",
			setup: func(b *elasticbeanstalk.InMemoryBackend) {
				ctx := context.Background()
				_, _ = b.CreateApplication(ctx, "swap-app", "", nil)
				_, _ = b.CreateEnvironment(ctx, "swap-app", "env-a", "stack", "", nil,
					elasticbeanstalk.CreateEnvironmentParams{})
				_, _ = b.CreateEnvironment(ctx, "swap-app", "env-b", "stack", "", nil,
					elasticbeanstalk.CreateEnvironmentParams{})
				_ = b.SwapEnvironmentCNAMEs(ctx, "env-a", "env-b")
			},
			verify: verifySwappedCNAMEsSurviveRestore,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1")
			tt.setup(b)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1")
			err := b2.Restore(t.Context(), snap)
			require.NoError(t, err)

			tt.verify(t, b2)
		})
	}
}

// setupFullState seeds one instance of every store.Table-backed resource
// (applications, environments, appVersions, configTemplates,
// platformVersions) plus every raw-left, still-persisted map
// (managedActionHistory, events, envCounters), so
// TestElasticBeanstalk_PersistenceSnapshotRestore's "full_state_round_trip"
// case exercises the whole Phase 3.3 conversion in one Snapshot->Restore
// round trip.
func setupFullState(b *elasticbeanstalk.InMemoryBackend) {
	ctx := context.Background()

	_, _ = b.CreateApplication(ctx, "full-app", "full app desc", map[string]string{"k": "v"})
	_, _ = b.CreateEnvironment(ctx, "full-app", "full-env", "stack", "desc", nil,
		elasticbeanstalk.CreateEnvironmentParams{VersionLabel: "v1"})
	_, _ = b.CreateApplicationVersionWithParams(ctx, "full-app", "v1",
		elasticbeanstalk.ApplicationVersionParams{Process: true})
	_, _ = b.CreateConfigurationTemplate(ctx, "full-app", "full-tmpl", "desc", "stack", nil)
	_, _ = b.CreatePlatformVersion(ctx, "full-plat", "1.0.0", nil)
	b.AddManagedActionHistory(ctx, "full-env", "action-1", "InstanceRefresh", "applied", "Scheduled")
	// UpdateEnvironment appends a second event on top of CreateEnvironment's own.
	_, _ = b.UpdateEnvironmentWithParams(ctx, "full-app", "full-env",
		elasticbeanstalk.UpdateEnvironmentParams{Description: "updated"})
	// A second environment bumps envCounters past 1 so a mis-restored
	// counter (reset to 0) is observable post-restore.
	_, _ = b.CreateEnvironment(ctx, "full-app", "full-env-2", "stack", "", nil,
		elasticbeanstalk.CreateEnvironmentParams{})
}

// verifyFullState checks every resource setupFullState seeded survived the
// round trip, including that every store.Table secondary index (byARN,
// byName, byCNAME) was correctly rebuilt by Restore -- not just that the
// primary Get-by-key path works.
func verifyFullState(t *testing.T, b *elasticbeanstalk.InMemoryBackend) {
	t.Helper()

	ctx := context.Background()

	apps := b.DescribeApplications(ctx, nil)
	require.Len(t, apps, 1)
	assert.Equal(t, "v", apps[0].Tags["k"])

	envs := b.DescribeEnvironments(ctx, "full-app", nil, nil)
	require.Len(t, envs, 2)

	versions := b.DescribeApplicationVersions(ctx, "full-app", nil)
	require.Len(t, versions, 1)

	templates := b.DescribeConfigurationTemplates(ctx, "full-app")
	require.Len(t, templates, 1)

	platforms := b.ListPlatformVersions(ctx)
	require.Len(t, platforms, 1)

	history := b.DescribeEnvironmentManagedActionHistory(ctx, "full-env")
	require.Len(t, history, 1)
	assert.Equal(t, "action-1", history[0].ActionID)

	events := b.DescribeEvents(ctx, "full-app", "full-env")
	assert.NotEmpty(t, events, "the raw-left events map must survive the round trip")

	verifyFullStateIndexesRebuilt(t, b, apps[0].ApplicationARN)
}

// verifyFullStateIndexesRebuilt exercises every store.Table secondary index
// touched by the Phase 3.3 conversion (applicationsByARN, environmentsByName,
// envCounters) to prove Restore rebuilds them rather than leaving them
// stale/empty, split out to keep verifyFullState's cognitive complexity low.
func verifyFullStateIndexesRebuilt(t *testing.T, b *elasticbeanstalk.InMemoryBackend, appARN string) {
	t.Helper()

	ctx := context.Background()

	tags, err := b.ListTagsForResource(ctx, appARN)
	require.NoError(t, err, "applicationsByARN index must be rebuilt by Restore")
	assert.Equal(t, "v", tags["k"])

	require.NoError(t, b.AssociateEnvironmentOperationsRole(ctx, "full-env", "role-1"),
		"environmentsByName index must be rebuilt by Restore")

	// envCounters restored: a freshly created third environment must not
	// collide with full-env/full-env-2's IDs (e-00000001 / e-00000002).
	env3, err := b.CreateEnvironment(ctx, "full-app", "full-env-3", "stack", "", nil,
		elasticbeanstalk.CreateEnvironmentParams{})
	require.NoError(t, err)
	assert.Equal(t, "e-00000003", env3.EnvironmentID)
}

// verifySwappedCNAMEsSurviveRestore proves the pkgs/store gotcha fix in
// SwapEnvironmentCNAMEs (delete both entries, mutate CNAME, re-Put both) is
// itself durable across a Snapshot->Restore round trip: the swapped CNAME
// values, and the environmentsByCNAME index built from them, must not revert
// to or leak the pre-swap state.
func verifySwappedCNAMEsSurviveRestore(t *testing.T, b *elasticbeanstalk.InMemoryBackend) {
	t.Helper()

	ctx := context.Background()

	envs := b.DescribeEnvironments(ctx, "swap-app", nil, nil)
	require.Len(t, envs, 2)

	var envA, envB *elasticbeanstalk.Environment

	for _, e := range envs {
		switch e.EnvironmentName {
		case "env-a":
			envA = e
		case "env-b":
			envB = e
		}
	}

	require.NotNil(t, envA)
	require.NotNil(t, envB)
	assert.Contains(t, envA.CNAME, "env-b", "env-a must still hold env-b's pre-swap CNAME after restore")
	assert.Contains(t, envB.CNAME, "env-a", "env-b must still hold env-a's pre-swap CNAME after restore")

	// The swapped CNAME must resolve as taken via the rebuilt
	// environmentsByCNAME index, not the stale pre-swap association.
	available, _ := b.CheckDNSAvailability(ctx, "env-b")
	assert.False(t, available, "env-a's current (post-swap) CNAME prefix must be reported as taken")
}

// TestElasticBeanstalk_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestElasticBeanstalk_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreateApplication(t.Context(), "seed-app", "", nil)
	require.NoError(t, err)

	// A syntactically valid but version-mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, b.ApplicationCount())
}

// TestElasticBeanstalk_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot with no version field at all (the pre-Phase-3.3 shape) decodes
// with Version == 0, which mismatches elasticbeanstalkSnapshotVersion and is
// discarded the same way any other incompatible version is -- not partially
// applied.
func TestElasticBeanstalk_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	b := elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.CreateApplication(t.Context(), "seed-app", "", nil)
	require.NoError(t, err)

	// Pre-Phase-3.3 shape: plain region-nested resource maps, no "version" or
	// "tables" key.
	err = b.Restore(t.Context(), []byte(
		`{"applications":{"us-east-1":{"old":{"applicationName":"old"}}}}`,
	))
	require.NoError(t, err)

	assert.Equal(t, 0, b.ApplicationCount())
}

// TestElasticBeanstalk_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestElasticBeanstalk_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := elasticbeanstalk.NewInMemoryBackend("123456789012", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}
