package resourcegroups_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

// TestGroupingStatusOnUngroup verifies that failed ungroup operations are
// recorded in grouping status history.
func TestGroupingStatusOnUngroup(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateGroup(context.Background(), "status-group", "", nil, nil, nil)
	_, _ = b.GroupResources(context.Background(), "status-group", []string{"arn:aws:s3:::b1"})

	result, err := b.UngroupResources(context.Background(),
		"status-group",
		[]string{"arn:aws:s3:::b1", "arn:aws:s3:::nonmember"},
	)
	require.NoError(t, err)

	assert.Len(t, result.Succeeded, 1)
	assert.Len(t, result.Failed, 1)
	assert.Equal(t, "arn:aws:s3:::nonmember", result.Failed[0].ResourceArn)
	assert.Equal(t, "RESOURCE_NOT_FOUND", result.Failed[0].ErrorCode)

	statuses, _, err := b.ListGroupingStatuses(context.Background(), "status-group", nil, "", 0)
	require.NoError(t, err)

	var successCount, failCount int
	for _, s := range statuses {
		if s.Action == "UNGROUP" {
			switch s.Status {
			case "SUCCESS":
				successCount++
			case "FAILED":
				failCount++
			}
		}
	}

	assert.Equal(t, 1, successCount)
	assert.Equal(t, 1, failCount)
}

// TestGroupResources_NoDuplicates verifies duplicate ARNs are de-duped.
func TestGroupResources_NoDuplicates(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateGroup(context.Background(), "g1", "", nil, nil, nil)

	_, err := b.GroupResources(context.Background(), "g1", []string{"arn:aws:s3:::b1", "arn:aws:s3:::b1"})
	require.NoError(t, err)

	// Should only store one copy.
	assert.Equal(t, 1, resourcegroups.GroupResourceCount(b))
}

// TestGroupResources_EmptyARN verifies that an empty ARN slice is a no-op.
func TestGroupResources_EmptyARN(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateGroup(context.Background(), "empty-arn-group", "", nil, nil, nil)
	require.NoError(t, err)

	succeeded, err := b.GroupResources(context.Background(), "empty-arn-group", []string{})
	require.NoError(t, err)
	assert.Empty(t, succeeded)

	ids, _, err := b.ListGroupResources(context.Background(), "empty-arn-group", nil, "", 0)
	require.NoError(t, err)
	assert.Empty(t, ids)
}

// TestGroupResources_DuplicateIgnored verifies duplicate add is idempotent.
func TestGroupResources_DuplicateIgnored(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateGroup(context.Background(), "dedup-group", "", nil, nil, nil)
	require.NoError(t, err)

	arn := "arn:aws:s3:::unique-bucket"

	_, err = b.GroupResources(context.Background(), "dedup-group", []string{arn, arn})
	require.NoError(t, err)

	ids, _, err := b.ListGroupResources(context.Background(), "dedup-group", nil, "", 0)
	require.NoError(t, err)
	require.Len(t, ids, 1)

	// Adding the same ARN again also produces only one copy.
	_, err = b.GroupResources(context.Background(), "dedup-group", []string{arn})
	require.NoError(t, err)

	ids, _, err = b.ListGroupResources(context.Background(), "dedup-group", nil, "", 0)
	require.NoError(t, err)
	assert.Len(t, ids, 1)
}

// TestListGroupResources_Pagination verifies pagination of resource lists.
func TestListGroupResources_Pagination(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateGroup(context.Background(), "g1", "", nil, nil, nil)
	require.NoError(t, err)

	arns := []string{
		"arn:aws:s3:::bucket-1",
		"arn:aws:s3:::bucket-2",
		"arn:aws:s3:::bucket-3",
		"arn:aws:s3:::bucket-4",
		"arn:aws:s3:::bucket-5",
	}
	_, err = b.GroupResources(context.Background(), "g1", arns)
	require.NoError(t, err)

	// Page 1: 2 resources.
	page1, tok1, err := b.ListGroupResources(context.Background(), "g1", nil, "", 2)
	require.NoError(t, err)
	assert.Len(t, page1, 2)
	require.NotEmpty(t, tok1)

	// Page 2: 2 more.
	page2, tok2, err := b.ListGroupResources(context.Background(), "g1", nil, tok1, 2)
	require.NoError(t, err)
	assert.Len(t, page2, 2)
	require.NotEmpty(t, tok2)

	// Page 3: remaining 1.
	page3, tok3, err := b.ListGroupResources(context.Background(), "g1", nil, tok2, 2)
	require.NoError(t, err)
	assert.Len(t, page3, 1)
	assert.Empty(t, tok3)

	// Collect all and verify all 5 are returned with no duplicates.
	all := append(append(page1, page2...), page3...)
	seen := make(map[string]bool)
	for _, id := range all {
		assert.False(t, seen[id.ResourceArn], "duplicate ARN: %s", id.ResourceArn)
		seen[id.ResourceArn] = true
	}
	assert.Len(t, seen, 5)
}

// TestListGroupResources_ByARN verifies resources can be listed by group ARN.
func TestListGroupResources_ByARN(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	g, err := b.CreateGroup(context.Background(), "res-by-arn", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.GroupResources(context.Background(), "res-by-arn", []string{"arn:aws:s3:::bucket"})
	require.NoError(t, err)

	ids, _, err := b.ListGroupResources(context.Background(), g.ARN, nil, "", 0)
	require.NoError(t, err)
	require.Len(t, ids, 1)
	assert.Equal(t, "arn:aws:s3:::bucket", ids[0].ResourceArn)
}

// TestListGroupResources_FilterByResourceType verifies resource-type filter.
func TestListGroupResources_FilterByResourceType(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateGroup(context.Background(), "mixed-group", "", nil, nil, nil)
	require.NoError(t, err)

	arns := []string{
		"arn:aws:s3:::my-bucket",
		"arn:aws:ec2:us-east-1:000000000000:instance/i-aaa",
		"arn:aws:ec2:us-east-1:000000000000:volume/vol-bbb",
		"arn:aws:lambda:us-east-1:000000000000:function:my-fn",
	}
	_, err = b.GroupResources(context.Background(), "mixed-group", arns)
	require.NoError(t, err)

	tests := []struct { //nolint:govet // field order optimized for readability
		name       string
		filterVals []string
		wantCount  int
		wantTypes  []string
	}{
		{
			name:       "filter_s3_only",
			filterVals: []string{"AWS::S3::Bucket"},
			wantCount:  1,
			wantTypes:  []string{"AWS::S3::Bucket"},
		},
		{
			name:       "filter_ec2_instance",
			filterVals: []string{"AWS::EC2::Instance"},
			wantCount:  1,
			wantTypes:  []string{"AWS::EC2::Instance"},
		},
		{
			name:       "filter_ec2_all",
			filterVals: []string{"AWS::EC2::Instance", "AWS::EC2::Volume"},
			wantCount:  2,
		},
		{
			name:       "filter_no_match",
			filterVals: []string{"AWS::RDS::DBInstance"},
			wantCount:  0,
		},
		{
			name:      "no_filter_returns_all",
			wantCount: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var filters []resourcegroups.ListGroupResourcesFilter
			if len(tt.filterVals) > 0 {
				filters = []resourcegroups.ListGroupResourcesFilter{
					{Name: "resource-type", Values: tt.filterVals},
				}
			}

			ids, _, listErr := b.ListGroupResources(context.Background(), "mixed-group", filters, "", 0)
			require.NoError(t, listErr)
			assert.Len(t, ids, tt.wantCount)

			for _, wantType := range tt.wantTypes {
				found := false
				for _, id := range ids {
					if id.ResourceType == wantType {
						found = true

						break
					}
				}
				assert.True(t, found, "expected resource type %s not found", wantType)
			}
		})
	}
}

// TestGroupResources_RejectsQueryBasedGroup verifies Group/UngroupResources reject a
// group that has a ResourceQuery: the real API works only with static
// membership groups, never groups auto-populated by a tag or CloudFormation
// query (AWS docs: GroupResources "You can only use this operation with"
// AWS::EC2::HostManagement/CapacityReservationPool/ApplicationGroup groups;
// UngroupResources "doesn't work with any resource groups that are
// automatically populated by tag-based or CloudFormation stack-based
// queries").
func TestGroupResources_RejectsQueryBasedGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(b *resourcegroups.InMemoryBackend) error
		name string
	}{
		{
			name: "group",
			call: func(b *resourcegroups.InMemoryBackend) error {
				_, err := b.GroupResources(context.Background(), "query-group", []string{"arn:aws:s3:::b1"})

				return err
			},
		},
		{
			name: "ungroup",
			call: func(b *resourcegroups.InMemoryBackend) error {
				_, err := b.UngroupResources(context.Background(), "query-group", []string{"arn:aws:s3:::b1"})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateGroup(context.Background(), "query-group", "",
				&resourcegroups.ResourceQuery{Type: "TAG_FILTERS_1_0", Query: `{}`}, nil, nil)
			require.NoError(t, err)

			opErr := tt.call(b)
			require.Error(t, opErr)
			assert.ErrorIs(t, opErr, resourcegroups.ErrValidation)
		})
	}
}

// TestListGroupingStatuses_Pagination verifies NextToken pagination.
func TestListGroupingStatuses_Pagination(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateGroup(context.Background(), "status-paged", "", nil, nil, nil)
	require.NoError(t, err)

	// Add 5 resources to generate 5 status entries.
	arns := []string{
		"arn:aws:ec2:us-east-1:000000000000:instance/i-aaa",
		"arn:aws:ec2:us-east-1:000000000000:instance/i-bbb",
		"arn:aws:ec2:us-east-1:000000000000:instance/i-ccc",
		"arn:aws:ec2:us-east-1:000000000000:instance/i-ddd",
		"arn:aws:ec2:us-east-1:000000000000:instance/i-eee",
	}
	_, err = b.GroupResources(context.Background(), "status-paged", arns)
	require.NoError(t, err)

	page1, tok1, err := b.ListGroupingStatuses(context.Background(), "status-paged", nil, "", 2)
	require.NoError(t, err)
	assert.Len(t, page1, 2)
	require.NotEmpty(t, tok1)

	page2, tok2, err := b.ListGroupingStatuses(context.Background(), "status-paged", nil, tok1, 2)
	require.NoError(t, err)
	assert.Len(t, page2, 2)
	require.NotEmpty(t, tok2)

	page3, tok3, err := b.ListGroupingStatuses(context.Background(), "status-paged", nil, tok2, 2)
	require.NoError(t, err)
	assert.Len(t, page3, 1)
	assert.Empty(t, tok3)
}
