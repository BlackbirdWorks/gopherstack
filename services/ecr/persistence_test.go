package ecr_test

// store_persistence_test.go verifies the Phase 3.3 pkgs/store conversion's
// Snapshot/Restore round-trip: every store.Table registered on b.registry
// (repos, images, imageScanFindings, pullThroughCacheRules,
// repositoryCreationTemplates, lifecyclePolicies, lifecyclePolicyPreviews,
// repositoryPolicies, accountSettings, pullTimeUpdateExclusions), plus the
// state deliberately left as plain maps/fields (repoTags, registryPolicy,
// registryScanningConfig, replicationConfig, signingConfig), survive a full
// Snapshot -> fresh backend -> Restore cycle.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

//nolint:maintidx // exhaustively exercises every store.Table plus every raw-map/scalar field in one round trip.
func TestStorePersistence_FullStateRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b1 := ecr.NewInMemoryBackend("123456789012", "us-east-1", "localhost:5000")

	// repos
	repo, err := b1.CreateRepository(ctx, "persist-repo", "MUTABLE", false, "AES256", "")
	require.NoError(t, err)

	// images (composite repo+digest key + repo index)
	img, err := b1.PutImage(ctx, "persist-repo", ecr.Image{
		ImageManifest: `{"schemaVersion":2}`,
		ImageID:       ecr.ImageIdentifier{ImageTag: "v1"},
	})
	require.NoError(t, err)

	// imageScanFindings (composite repo+digest key + repo index)
	_, err = b1.StartImageScan(ctx, "persist-repo", ecr.ImageIdentifier{ImageDigest: img.ImageDigest})
	require.NoError(t, err)

	// pullThroughCacheRules
	_, err = b1.CreatePullThroughCacheRule(
		ctx, "cache-prefix", "https://upstream.example.com", "", "", "", "")
	require.NoError(t, err)

	// repositoryCreationTemplates
	_, err = b1.CreateRepositoryCreationTemplate(ctx, &ecr.RepositoryCreationTemplate{
		Prefix:      "tmpl-prefix",
		Description: "a template",
	})
	require.NoError(t, err)

	// lifecyclePolicies (+ applies immediately, exercising lifecyclePolicyPreviews too)
	_, err = b1.PutLifecyclePolicy(ctx, "persist-repo",
		`{"rules":[{"rulePriority":1,"description":"d","selection":`+
			`{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":100},"action":{"type":"expire"}}]}`)
	require.NoError(t, err)

	_, err = b1.StartLifecyclePolicyPreview(ctx, "persist-repo", "")
	require.NoError(t, err)

	// repositoryPolicies
	_, err = b1.SetRepositoryPolicy(ctx, "persist-repo", `{"Version":"2012-10-17","Statement":[]}`)
	require.NoError(t, err)

	// accountSettings
	_, err = b1.PutAccountSetting(ctx, "basicScanTypeVersion", "AWS_NATIVE")
	require.NoError(t, err)

	// pullTimeUpdateExclusions
	_, err = b1.RegisterPullTimeUpdateExclusion(ctx, "arn:aws:iam::123456789012:role/excluded")
	require.NoError(t, err)

	// registryPolicy (scalar field)
	_, err = b1.PutRegistryPolicy(ctx, `{"Version":"2012-10-17","Statement":[]}`)
	require.NoError(t, err)

	// registryScanningConfig (scalar field)
	_, err = b1.PutRegistryScanningConfiguration(ctx, &ecr.RegistryScanningSettings{ScanType: "ENHANCED"})
	require.NoError(t, err)

	// replicationConfig (scalar field)
	_, err = b1.PutReplicationConfiguration(ctx, &ecr.ReplicationConfig{
		Rules: []ecr.ReplicationRule{{
			Destinations: []ecr.ReplicationDestination{{Region: "eu-west-1"}},
		}},
	})
	require.NoError(t, err)

	// signingConfig (scalar field)
	_, err = b1.PutSigningConfiguration(ctx, &ecr.SigningSettings{
		Rules: []ecr.SigningRule{{SigningProfileArn: "arn:aws:signer:us-east-1:123456789012:profile/p"}},
	})
	require.NoError(t, err)

	// repoTags (raw map, keyed by ARN)
	require.NoError(t, b1.TagResource(ctx, repo.RepositoryARN, map[string]string{"env": "prod"}))

	snap := b1.Snapshot(ctx)
	require.NotNil(t, snap)

	b2 := ecr.NewInMemoryBackend("123456789012", "us-east-1", "localhost:5000")
	require.NoError(t, b2.Restore(ctx, snap))

	// repos
	gotRepos, err := b2.DescribeRepositories(ctx, nil)
	require.NoError(t, err)
	require.Len(t, gotRepos, 1)
	assert.Equal(t, "persist-repo", gotRepos[0].RepositoryName)

	// images
	gotImages, err := b2.DescribeImages(ctx, "persist-repo", nil)
	require.NoError(t, err)
	require.Len(t, gotImages, 1)
	assert.Equal(t, img.ImageDigest, gotImages[0].ImageDigest)

	// imageScanFindings
	findings, _, err := b2.DescribeImageScanFindings(
		ctx, "persist-repo", ecr.ImageIdentifier{ImageDigest: img.ImageDigest}, 0, "")
	require.NoError(t, err)
	assert.Equal(t, "persist-repo", findings.RepositoryName)

	// pullThroughCacheRules
	rules, err := b2.DescribePullThroughCacheRules(ctx, nil)
	require.NoError(t, err)
	require.Len(t, rules, 1)
	assert.Equal(t, "cache-prefix", rules[0].EcrRepositoryPrefix)

	// repositoryCreationTemplates
	tmpls, err := b2.DescribeRepositoryCreationTemplates(ctx, nil)
	require.NoError(t, err)
	require.Len(t, tmpls, 1)
	assert.Equal(t, "tmpl-prefix", tmpls[0].Prefix)

	// lifecyclePolicies
	policy, err := b2.GetLifecyclePolicy(ctx, "persist-repo")
	require.NoError(t, err)
	assert.Contains(t, policy.LifecyclePolicyText, "imageCountMoreThan")

	// lifecyclePolicyPreviews
	preview, err := b2.GetLifecyclePolicyPreview(ctx, "persist-repo")
	require.NoError(t, err)
	assert.Contains(t, preview.LifecyclePolicyText, "imageCountMoreThan")

	// repositoryPolicies
	repoPolicy, err := b2.GetRepositoryPolicy(ctx, "persist-repo")
	require.NoError(t, err)
	assert.Contains(t, repoPolicy.PolicyText, "2012-10-17")

	// accountSettings
	settingValue, err := b2.GetAccountSetting(ctx, "basicScanTypeVersion")
	require.NoError(t, err)
	assert.Equal(t, "AWS_NATIVE", settingValue)

	// pullTimeUpdateExclusions
	exclusions, err := b2.ListPullTimeUpdateExclusions(ctx)
	require.NoError(t, err)
	require.Len(t, exclusions, 1)
	assert.Equal(t, "arn:aws:iam::123456789012:role/excluded", exclusions[0].PrincipalArn)

	// registryPolicy
	regPolicy, err := b2.GetRegistryPolicy(ctx)
	require.NoError(t, err)
	assert.Contains(t, regPolicy.PolicyText, "2012-10-17")

	// registryScanningConfig
	scanCfg, err := b2.GetRegistryScanningConfiguration(ctx)
	require.NoError(t, err)
	assert.Equal(t, "ENHANCED", scanCfg.ScanType)

	// replicationConfig
	registryDesc, err := b2.DescribeRegistry(ctx)
	require.NoError(t, err)
	require.NotNil(t, registryDesc.ReplicationConfiguration)
	require.Len(t, registryDesc.ReplicationConfiguration.Rules, 1)
	assert.Equal(t, "eu-west-1", registryDesc.ReplicationConfiguration.Rules[0].Destinations[0].Region)

	// signingConfig
	signCfg, err := b2.GetSigningConfiguration(ctx)
	require.NoError(t, err)
	require.Len(t, signCfg.Rules, 1)
	assert.Equal(t, "arn:aws:signer:us-east-1:123456789012:profile/p", signCfg.Rules[0].SigningProfileArn)

	// repoTags
	gotTags, err := b2.ListTagsForResource(ctx, repo.RepositoryARN)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod"}, gotTags)
}

// TestStorePersistence_IncompatibleVersionDiscardsCleanly verifies that a
// pre-Phase-3.3 (unversioned) snapshot -- or any snapshot whose version
// doesn't match the current shape -- is discarded rather than partially
// decoded, matching the services/sqs precedent.
func TestStorePersistence_IncompatibleVersionDiscardsCleanly(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := ecr.NewInMemoryBackend("123456789012", "us-east-1", "localhost:5000")

	_, err := b.CreateRepository(ctx, "seed-repo", "MUTABLE", false, "AES256", "")
	require.NoError(t, err)

	// An old-shape (pre-conversion, unversioned) snapshot: no "tables"/"version"
	// fields at all, just the legacy top-level nested maps.
	oldSnapshot := []byte(`{"repos":{"legacy-repo":{"repositoryName":"legacy-repo"}}}`)

	require.NoError(t, b.Restore(ctx, oldSnapshot))

	gotRepos, err := b.DescribeRepositories(ctx, nil)
	require.NoError(t, err)
	assert.Empty(t, gotRepos, "incompatible-version snapshot must reset to empty, not partially decode")
}

// TestStorePersistence_V1ScanFindingsAttributesDiscarded proves
// gopherstack-hjdd's fix: a v1 snapshot holding ImageScanFinding.Attributes
// in the pre-d83f4b5d3 map[string]string shape must be discarded cleanly now
// that ecrSnapshotVersion is 2, rather than erroring Restore outright when
// the registered imageScanFindings table's array-typed field can't decode a
// JSON object.
func TestStorePersistence_V1ScanFindingsAttributesDiscarded(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := ecr.NewInMemoryBackend("123456789012", "us-east-1", "localhost:5000")

	_, err := b.CreateRepository(ctx, "seed-repo", "MUTABLE", false, "AES256", "")
	require.NoError(t, err)

	v1Snapshot := []byte(`{
		"version": 1,
		"tables": {
			"imageScanFindings": [{
				"repositoryName": "old-repo",
				"registryId": "123456789012",
				"imageId": {"imageDigest": "sha256:deadbeef"},
				"status": "COMPLETE",
				"description": "old",
				"findings": [{
					"name": "CVE-old",
					"attributes": {"package_name": "openssl"}
				}]
			}]
		}
	}`)

	require.NoError(t, b.Restore(ctx, v1Snapshot),
		"a v1 snapshot must be discarded via the version guard, not error out of RestoreAll")

	gotRepos, err := b.DescribeRepositories(ctx, nil)
	require.NoError(t, err)
	assert.Empty(t, gotRepos, "incompatible-version snapshot must reset to empty, not partially decode")
}

// TestPersistence_TagsRoundTrip verifies that resource tags (a raw map, not a
// store.Table) survive a snapshot/restore cycle.
func TestPersistence_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags map[string]string
		name string
		arn  string
	}{
		{
			name: "tags_survive_snapshot_restore",
			arn:  "arn:aws:ecr:us-east-1:123456789012:repository/persist-repo",
			tags: map[string]string{"env": "prod", "owner": "team"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b1 := ecr.NewInMemoryBackend("123456789012", "us-east-1", "localhost:5000")
			require.NoError(t, b1.TagResource(context.Background(), tt.arn, tt.tags))

			snap := b1.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := ecr.NewInMemoryBackend("123456789012", "us-east-1", "localhost:5000")
			require.NoError(t, b2.Restore(t.Context(), snap))

			got, err := b2.ListTagsForResource(context.Background(), tt.arn)
			require.NoError(t, err)
			assert.Equal(t, tt.tags, got)
		})
	}
}
