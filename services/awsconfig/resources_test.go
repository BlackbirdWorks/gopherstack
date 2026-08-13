package awsconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func TestAWSConfigBackend_BatchGetAggregateResourceConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup                func(t *testing.T, b *awsconfig.InMemoryBackend)
		name                 string
		aggregatorName       string
		identifiers          []awsconfig.AggregateResourceIdentifier
		wantItemCount        int
		wantUnprocessedCount int
	}{
		{
			name:           "undiscovered_resource_is_unprocessed",
			aggregatorName: "my-aggregator",
			identifiers: []awsconfig.AggregateResourceIdentifier{
				{
					SourceAccountID: "000000000000",
					SourceRegion:    "us-east-1",
					ResourceID:      "i-abc",
					ResourceType:    "AWS::EC2::Instance",
				},
			},
			wantItemCount:        0,
			wantUnprocessedCount: 1,
		},
		{
			name:                 "empty_identifiers",
			aggregatorName:       "my-aggregator",
			identifiers:          []awsconfig.AggregateResourceIdentifier{},
			wantItemCount:        0,
			wantUnprocessedCount: 0,
		},
		{
			name: "discovered_resource_is_returned",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutResourceConfig("AWS::EC2::Instance", "i-abc", `{}`))
			},
			aggregatorName: "my-aggregator",
			identifiers: []awsconfig.AggregateResourceIdentifier{
				{
					SourceAccountID: "000000000000",
					SourceRegion:    "us-east-1",
					ResourceID:      "i-abc",
					ResourceType:    "AWS::EC2::Instance",
				},
				{ResourceID: "i-missing", ResourceType: "AWS::EC2::Instance"},
			},
			wantItemCount:        1,
			wantUnprocessedCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			items, unprocessed := b.BatchGetAggregateResourceConfig(tt.aggregatorName, tt.identifiers)
			assert.Len(t, items, tt.wantItemCount)
			assert.Len(t, unprocessed, tt.wantUnprocessedCount)
		})
	}
}

func TestAWSConfigBackend_BatchGetResourceConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup                func(t *testing.T, b *awsconfig.InMemoryBackend)
		name                 string
		keys                 []awsconfig.ResourceKey
		wantItemCount        int
		wantUnprocessedCount int
	}{
		{
			name: "undiscovered_resource_is_unprocessed",
			keys: []awsconfig.ResourceKey{
				{ResourceType: "AWS::EC2::Instance", ResourceID: "i-abc"},
			},
			wantItemCount:        0,
			wantUnprocessedCount: 1,
		},
		{
			name:                 "empty_keys",
			keys:                 []awsconfig.ResourceKey{},
			wantItemCount:        0,
			wantUnprocessedCount: 0,
		},
		{
			name: "discovered_resource_is_returned",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutResourceConfig("AWS::EC2::Instance", "i-abc", `{}`))
			},
			keys: []awsconfig.ResourceKey{
				{ResourceType: "AWS::EC2::Instance", ResourceID: "i-abc"},
				{ResourceType: "AWS::EC2::Instance", ResourceID: "i-missing"},
			},
			wantItemCount:        1,
			wantUnprocessedCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			items, unprocessed := b.BatchGetResourceConfig(tt.keys)
			assert.Len(t, items, tt.wantItemCount)
			assert.Len(t, unprocessed, tt.wantUnprocessedCount)
		})
	}
}

func TestPutResourceConfig(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	err := b.PutResourceConfig("AWS::S3::Bucket", "my-bucket", `{"Name":"my-bucket"}`)
	if err != nil {
		t.Fatalf("PutResourceConfig: %v", err)
	}

	items := b.GetResourceConfigHistory("AWS::S3::Bucket", "my-bucket")
	if len(items) != 1 || items[0].ResourceID != "my-bucket" {
		t.Fatalf("GetResourceConfigHistory: %v", items)
	}
}

func TestGetResourceConfigHistory_NotFound(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	items := b.GetResourceConfigHistory("AWS::S3::Bucket", "nonexistent")
	if len(items) != 0 {
		t.Fatalf("expected empty, got %v", items)
	}
}

func TestListDiscoveredResources(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_ = b.PutResourceConfig("AWS::S3::Bucket", "bucket1", "{}")
	_ = b.PutResourceConfig("AWS::S3::Bucket", "bucket2", "{}")
	_ = b.PutResourceConfig("AWS::EC2::Instance", "i-123", "{}")

	s3Items := b.ListDiscoveredResources("AWS::S3::Bucket")
	if len(s3Items) != 2 {
		t.Fatalf("expected 2 S3 items, got %d", len(s3Items))
	}

	ec2Items := b.ListDiscoveredResources("AWS::EC2::Instance")
	if len(ec2Items) != 1 {
		t.Fatalf("expected 1 EC2 item, got %d", len(ec2Items))
	}
}

func TestGetAggregateDiscoveredResourceCounts(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	if b.GetAggregateDiscoveredResourceCounts() != 0 {
		t.Fatal("expected 0 initially")
	}

	_ = b.PutResourceConfig("AWS::S3::Bucket", "b1", "{}")
	_ = b.PutResourceConfig("AWS::S3::Bucket", "b2", "{}")
	_ = b.PutResourceConfig("AWS::EC2::Instance", "i1", "{}")

	if got := b.GetAggregateDiscoveredResourceCounts(); got != 3 {
		t.Fatalf("expected 3, got %d", got)
	}
}

func TestGetAggregateResourceConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(t *testing.T, b *awsconfig.InMemoryBackend)
		name           string
		aggregatorName string
		identifier     awsconfig.AggregateResourceIdentifier
		wantErr        error
		wantResourceID string
	}{
		{
			name:           "unknown_aggregator_errors",
			aggregatorName: "no-such-aggregator",
			identifier:     awsconfig.AggregateResourceIdentifier{ResourceType: "AWS::S3::Bucket", ResourceID: "b1"},
			wantErr:        awsconfig.ErrNoSuchAggregator,
		},
		{
			name: "undiscovered_resource_errors",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigurationAggregator("my-aggregator", nil, nil, nil))
			},
			aggregatorName: "my-aggregator",
			identifier: awsconfig.AggregateResourceIdentifier{
				ResourceType: "AWS::S3::Bucket",
				ResourceID:   "missing",
			},
			wantErr: awsconfig.ErrResourceNotDiscovered,
		},
		{
			name: "returns_the_requested_resource_not_an_arbitrary_one",
			setup: func(t *testing.T, b *awsconfig.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutConfigurationAggregator("my-aggregator", nil, nil, nil))
				require.NoError(t, b.PutResourceConfig("AWS::EC2::Instance", "i-first", "{}"))
				require.NoError(t, b.PutResourceConfig("AWS::S3::Bucket", "my-bucket", "{}"))
			},
			aggregatorName: "my-aggregator",
			identifier: awsconfig.AggregateResourceIdentifier{
				ResourceType: "AWS::S3::Bucket",
				ResourceID:   "my-bucket",
			},
			wantResourceID: "my-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := awsconfig.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(t, b)
			}

			item, err := b.GetAggregateResourceConfig(tt.aggregatorName, tt.identifier)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, item)
			assert.Equal(t, tt.wantResourceID, item.ResourceID)
		})
	}
}

func TestAWSConfigBackend_ListAggregateDiscoveredResources(t *testing.T) {
	t.Parallel()

	t.Run("unknown_aggregator_errors", func(t *testing.T) {
		t.Parallel()

		b := awsconfig.NewInMemoryBackend()
		_, err := b.ListAggregateDiscoveredResources("does-not-exist", "AWS::S3::Bucket", "", "", "")
		require.Error(t, err)
		assert.ErrorIs(t, err, awsconfig.ErrNoSuchAggregator)
	})

	t.Run("returns_local_discovered_resources_tagged_with_local_source", func(t *testing.T) {
		t.Parallel()

		b := awsconfig.NewInMemoryBackend()
		require.NoError(t, b.PutConfigurationAggregator("agg1", nil, nil, nil))
		require.NoError(t, b.PutResourceConfig("AWS::S3::Bucket", "bucket1", "{}"))

		out, err := b.ListAggregateDiscoveredResources("agg1", "AWS::S3::Bucket", "", "", "")
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.Equal(t, "bucket1", out[0].ResourceID)
		assert.Equal(t, "123456789012", out[0].SourceAccountID)
		assert.Equal(t, "us-east-1", out[0].SourceRegion)
	})

	t.Run("account_filter_excludes_non_matching_account", func(t *testing.T) {
		t.Parallel()

		b := awsconfig.NewInMemoryBackend()
		require.NoError(t, b.PutConfigurationAggregator("agg1", nil, nil, nil))
		require.NoError(t, b.PutResourceConfig("AWS::S3::Bucket", "bucket1", "{}"))

		out, err := b.ListAggregateDiscoveredResources("agg1", "AWS::S3::Bucket", "999999999999", "", "")
		require.NoError(t, err)
		assert.Empty(t, out)
	})
}
