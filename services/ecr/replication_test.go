package ecr_test

// replication_test.go — verifies that DescribeImageReplicationStatus is backed
// by real per-destination state derived from the registry replication
// configuration, and that PutReplicationConfiguration/DescribeRegistry
// round-trip cross-region, cross-account, and filtered replication rules.

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

func putReplication(t *testing.T, b *ecr.InMemoryBackend, rules []ecr.ReplicationRule) {
	t.Helper()

	_, err := b.PutReplicationConfiguration(context.Background(), &ecr.ReplicationConfig{Rules: rules})
	require.NoError(t, err)
}

func TestReplication_DestinationsDerivedFromConfig(t *testing.T) {
	t.Parallel()

	// Backend region is us-east-1, account 123456789012.
	tests := []struct {
		name        string
		rules       []ecr.ReplicationRule
		repo        string
		wantRegions []string
	}{
		{
			name: "no filters replicates to all cross-region destinations",
			rules: []ecr.ReplicationRule{{
				Destinations: []ecr.ReplicationDestination{
					{Region: "us-west-2", RegistryID: "123456789012"},
					{Region: "eu-west-1", RegistryID: "210987654321"},
				},
			}},
			repo:        "app",
			wantRegions: []string{"eu-west-1", "us-west-2"},
		},
		{
			name: "same region and account destination is excluded",
			rules: []ecr.ReplicationRule{{
				Destinations: []ecr.ReplicationDestination{
					{Region: "us-east-1", RegistryID: "123456789012"}, // == source, skipped
					{Region: "us-east-1", RegistryID: "999988887777"}, // cross-account, kept
				},
			}},
			repo:        "app",
			wantRegions: []string{"us-east-1"},
		},
		{
			name: "repositoryFilters gate which repos replicate",
			rules: []ecr.ReplicationRule{{
				Destinations:      []ecr.ReplicationDestination{{Region: "us-west-2", RegistryID: "123456789012"}},
				RepositoryFilters: []ecr.RepositoryFilter{{Filter: "prod", FilterType: "PREFIX"}},
			}},
			repo:        "dev-app",
			wantRegions: nil, // dev-app does not match "prod" prefix
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			b.CreateRepoInternal(tt.repo)
			seedImage(b, tt.repo, "sha256:img", "v1", time.Now())
			putReplication(t, b, tt.rules)

			out, err := b.DescribeImageReplicationStatus(context.Background(), tt.repo,
				ecr.ImageIdentifier{ImageDigest: "sha256:img"})
			require.NoError(t, err)

			got := make([]string, 0, len(out.ReplicationStatuses))
			for _, s := range out.ReplicationStatuses {
				got = append(got, s.Region)
			}
			assert.ElementsMatch(t, tt.wantRegions, got)
		})
	}
}

func TestReplication_StatusSettlesFromInProgressToComplete(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.CreateRepoInternal("app")
	b.SetReplicationSettleDelayForTest(time.Hour)
	seedImage(b, "app", "sha256:img", "v1", time.Now())
	putReplication(t, b, []ecr.ReplicationRule{{
		Destinations: []ecr.ReplicationDestination{{Region: "us-west-2", RegistryID: "123456789012"}},
	}})

	out, err := b.DescribeImageReplicationStatus(context.Background(), "app",
		ecr.ImageIdentifier{ImageDigest: "sha256:img"})
	require.NoError(t, err)
	require.Len(t, out.ReplicationStatuses, 1)
	assert.Equal(t, "IN_PROGRESS", out.ReplicationStatuses[0].Status,
		"freshly pushed image within settle window is IN_PROGRESS")

	// Age the image past the settle delay -> replication reports COMPLETE.
	b.AgeImageForTest("app", "sha256:img", 2*time.Hour)

	out, err = b.DescribeImageReplicationStatus(context.Background(), "app",
		ecr.ImageIdentifier{ImageDigest: "sha256:img"})
	require.NoError(t, err)
	require.Len(t, out.ReplicationStatuses, 1)
	assert.Equal(t, "COMPLETE", out.ReplicationStatuses[0].Status)
	assert.Equal(t, "123456789012", out.ReplicationStatuses[0].RegistryID)
}

func TestReplication_NoConfig_EmptyStatuses(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.CreateRepoInternal("app")
	seedImage(b, "app", "sha256:img", "v1", time.Now())

	out, err := b.DescribeImageReplicationStatus(context.Background(), "app",
		ecr.ImageIdentifier{ImageDigest: "sha256:img"})
	require.NoError(t, err)
	assert.Empty(t, out.ReplicationStatuses)
}

func TestReplicationConfiguration_CrossRegion(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	rec := doAccuracy(t, h, "PutReplicationConfiguration", map[string]any{
		"replicationConfiguration": map[string]any{
			"rules": []map[string]any{
				{
					"destinations": []map[string]any{
						{"region": "us-west-2", "registryId": "123456789012"},
						{"region": "eu-west-1", "registryId": "123456789012"},
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	cfg, _ := out["replicationConfiguration"].(map[string]any)
	rules, _ := cfg["rules"].([]any)
	require.Len(t, rules, 1)
	rule := rules[0].(map[string]any)
	destinations, _ := rule["destinations"].([]any)
	assert.Len(t, destinations, 2, "two cross-region destinations must be stored")
}

func TestReplicationConfiguration_CrossAccount(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	rec := doAccuracy(t, h, "PutReplicationConfiguration", map[string]any{
		"replicationConfiguration": map[string]any{
			"rules": []map[string]any{
				{
					"destinations": []map[string]any{
						{"region": "us-east-1", "registryId": "999999999999"},
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doAccuracy(t, h, "DescribeRegistry", map[string]any{})
	require.Equal(t, http.StatusOK, descRec.Code)

	out := parseAccuracy(t, descRec)
	repCfg, _ := out["replicationConfiguration"].(map[string]any)
	rules, _ := repCfg["rules"].([]any)
	require.Len(t, rules, 1)
	rule := rules[0].(map[string]any)
	destinations, _ := rule["destinations"].([]any)
	require.Len(t, destinations, 1)
	dest := destinations[0].(map[string]any)
	assert.Equal(t, "999999999999", dest["registryId"],
		"cross-account registryId must be preserved")
}

func TestReplicationConfiguration_WithRepositoryFilter(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	rec := doAccuracy(t, h, "PutReplicationConfiguration", map[string]any{
		"replicationConfiguration": map[string]any{
			"rules": []map[string]any{
				{
					"destinations": []map[string]any{
						{"region": "ap-southeast-1", "registryId": "123456789012"},
					},
					"repositoryFilters": []map[string]any{
						{"filter": "prod/*", "filterType": "PREFIX_MATCH"},
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doAccuracy(t, h, "DescribeRegistry", map[string]any{})
	require.Equal(t, http.StatusOK, descRec.Code)

	out := parseAccuracy(t, descRec)
	repCfg, _ := out["replicationConfiguration"].(map[string]any)
	rules, _ := repCfg["rules"].([]any)
	require.Len(t, rules, 1)
	rule := rules[0].(map[string]any)
	filters, _ := rule["repositoryFilters"].([]any)
	require.Len(t, filters, 1)
	filter := filters[0].(map[string]any)
	assert.Equal(t, "prod/*", filter["filter"])
	assert.Equal(t, "PREFIX_MATCH", filter["filterType"])
}

func TestReplicationConfiguration_Clear(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	doAccuracy(t, h, "PutReplicationConfiguration", map[string]any{
		"replicationConfiguration": map[string]any{
			"rules": []map[string]any{
				{"destinations": []map[string]any{{"region": "us-west-2", "registryId": "123456789012"}}},
			},
		},
	})

	// Clear by setting empty rules.
	clearRec := doAccuracy(t, h, "PutReplicationConfiguration", map[string]any{
		"replicationConfiguration": map[string]any{
			"rules": []map[string]any{},
		},
	})
	require.Equal(t, http.StatusOK, clearRec.Code)

	descRec := doAccuracy(t, h, "DescribeRegistry", map[string]any{})
	out := parseAccuracy(t, descRec)
	repCfg, _ := out["replicationConfiguration"].(map[string]any)
	rules, _ := repCfg["rules"].([]any)
	assert.Empty(t, rules, "replication rules must be clearable")
}

func TestDescribeImageReplicationStatus_ReturnsStatus(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "replication-repo")

	// A replication status is reported per configured destination; with no
	// replication configuration the list is (correctly) empty, so configure one.
	repCfg := doAccuracy(t, h, "PutReplicationConfiguration", map[string]any{
		"replicationConfiguration": map[string]any{
			"rules": []any{
				map[string]any{
					"destinations": []any{
						map[string]any{"region": "us-west-2", "registryId": "000000000000"},
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, repCfg.Code)

	digest := mustPutImage(t, h, "replication-repo", "v1.0", `{"schemaVersion":2,"repl":"test"}`)

	// Wait briefly for async replication to complete
	time.Sleep(20 * time.Millisecond)

	rec := doAccuracy(t, h, "DescribeImageReplicationStatus", map[string]any{
		"repositoryName": "replication-repo",
		"imageId": map[string]any{
			"imageDigest": digest,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "replication-repo", out["repositoryName"])
	statuses, _ := out["replicationStatuses"].([]any)
	require.NotEmpty(t, statuses, "replicationStatuses must be present")
	status := statuses[0].(map[string]any)
	assert.NotEmpty(t, status["status"], "replication status must not be empty")
}

func TestDescribeImageReplicationStatus_ByTag(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "replication-tag-repo")
	mustPutImage(t, h, "replication-tag-repo", "release", `{"schemaVersion":2,"repl":"tag"}`)

	rec := doAccuracy(t, h, "DescribeImageReplicationStatus", map[string]any{
		"repositoryName": "replication-tag-repo",
		"imageId": map[string]any{
			"imageTag": "release",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "replication-tag-repo", out["repositoryName"])
}

func TestReplicationConfiguration_CRUD(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	cfg := ecr.ReplicationConfig{
		Rules: []ecr.ReplicationRule{
			{
				Destinations: []ecr.ReplicationDestination{
					{Region: "eu-west-1", RegistryID: "111122223333"},
				},
			},
		},
	}

	out, err := b.PutReplicationConfiguration(context.Background(), &cfg)
	require.NoError(t, err)
	assert.Len(t, out.Rules, 1)

	reg, err := b.DescribeRegistry(context.Background())
	require.NoError(t, err)
	require.NotNil(t, reg.ReplicationConfiguration)
	require.Len(t, reg.ReplicationConfiguration.Rules, 1)
	assert.Equal(t, "eu-west-1", reg.ReplicationConfiguration.Rules[0].Destinations[0].Region)
}

func TestPutReplicationConfiguration_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	cfg := map[string]any{
		"rules": []map[string]any{
			{
				"destinations": []map[string]any{
					{"region": "us-west-2", "registryId": "999999999999"},
				},
			},
		},
	}

	putRec := doAccuracy(t, h, "PutReplicationConfiguration", map[string]any{
		"replicationConfiguration": cfg,
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	descRec := doAccuracy(t, h, "DescribeRegistry", map[string]any{})
	require.Equal(t, http.StatusOK, descRec.Code)
	out := parseAccuracy(t, descRec)
	repCfg, ok := out["replicationConfiguration"].(map[string]any)
	require.True(t, ok, "replicationConfiguration must be present after PutReplicationConfiguration")
	rules, _ := repCfg["rules"].([]any)
	assert.Len(t, rules, 1)
}
