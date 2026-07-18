package resourcegroups_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

// TestResourceTypeFromARN verifies AWS::Service::Type extraction from ARNs.
func TestResourceTypeFromARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		arn      string
		wantType string
	}{
		{
			name:     "s3_bucket",
			arn:      "arn:aws:s3:::my-bucket",
			wantType: "AWS::S3::Bucket",
		},
		{
			name:     "ec2_instance",
			arn:      "arn:aws:ec2:us-east-1:123456789012:instance/i-abcdef",
			wantType: "AWS::EC2::Instance",
		},
		{
			name:     "ec2_volume",
			arn:      "arn:aws:ec2:us-east-1:123456789012:volume/vol-abc",
			wantType: "AWS::EC2::Volume",
		},
		{
			name:     "ec2_vpc",
			arn:      "arn:aws:ec2:us-east-1:123456789012:vpc/vpc-abc",
			wantType: "AWS::EC2::VPC",
		},
		{
			name:     "ec2_subnet",
			arn:      "arn:aws:ec2:us-east-1:123456789012:subnet/subnet-abc",
			wantType: "AWS::EC2::Subnet",
		},
		{
			name:     "lambda_function",
			arn:      "arn:aws:lambda:us-east-1:123456789012:function:my-func",
			wantType: "AWS::Lambda::Function",
		},
		{
			name:     "rds_instance",
			arn:      "arn:aws:rds:us-east-1:123456789012:db:my-db",
			wantType: "AWS::RDS::DBInstance",
		},
		{
			name:     "rds_cluster",
			arn:      "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster",
			wantType: "AWS::RDS::DBCluster",
		},
		{
			name:     "iam_role",
			arn:      "arn:aws:iam::123456789012:role/my-role",
			wantType: "AWS::IAM::Role",
		},
		{
			name:     "dynamodb_table",
			arn:      "arn:aws:dynamodb:us-east-1:123456789012:table/my-table",
			wantType: "AWS::DynamoDB::Table",
		},
		{
			name:     "kinesis_stream",
			arn:      "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
			wantType: "AWS::Kinesis::Stream",
		},
		{
			name:     "sns_topic",
			arn:      "arn:aws:sns:us-east-1:123456789012:MyTopic",
			wantType: "AWS::SNS::Topic",
		},
		{
			name:     "sqs_queue",
			arn:      "arn:aws:sqs:us-east-1:123456789012:MyQueue",
			wantType: "AWS::SQS::Queue",
		},
		{
			name:     "ecr_repository",
			arn:      "arn:aws:ecr:us-east-1:123456789012:repository/my-repo",
			wantType: "AWS::ECR::Repository",
		},
		{
			name:     "kms_key",
			arn:      "arn:aws:kms:us-east-1:123456789012:key/abc-123",
			wantType: "AWS::KMS::Key",
		},
		{
			name:     "unknown_service",
			arn:      "arn:aws:unknownsvc:us-east-1:123456789012:thing/abc",
			wantType: "",
		},
		{
			name:     "malformed_too_short",
			arn:      "arn:aws:s3",
			wantType: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateGroup(context.Background(), "type-group", "", nil, nil, nil)
			require.NoError(t, err)

			_, err = b.GroupResources(context.Background(), "type-group", []string{tt.arn})
			require.NoError(t, err)

			ids, _, err := b.ListGroupResources(context.Background(), "type-group", nil, "", 0)
			require.NoError(t, err)
			require.Len(t, ids, 1)
			assert.Equal(t, tt.wantType, ids[0].ResourceType)
		})
	}
}

// TestSearchResources_ResourceTypeFilter verifies type-based filtering.
func TestSearchResources_ResourceTypeFilter(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateGroup(context.Background(), "multi-type", "", nil, nil, nil)
	require.NoError(t, err)

	arns := []string{
		"arn:aws:s3:::bucket-a",
		"arn:aws:ec2:us-east-1:000000000000:instance/i-aaa",
		"arn:aws:lambda:us-east-1:000000000000:function:fn-a",
	}
	_, err = b.GroupResources(context.Background(), "multi-type", arns)
	require.NoError(t, err)

	tests := []struct { //nolint:govet // field order optimized for readability
		name          string
		queryJSON     string
		wantCount     int
		wantTypeFound string
	}{
		{
			name:          "filter_s3_only",
			queryJSON:     `{"ResourceTypeFilters":["AWS::S3::Bucket"]}`,
			wantCount:     1,
			wantTypeFound: "AWS::S3::Bucket",
		},
		{
			name:          "filter_ec2_instance",
			queryJSON:     `{"ResourceTypeFilters":["AWS::EC2::Instance"]}`,
			wantCount:     1,
			wantTypeFound: "AWS::EC2::Instance",
		},
		{
			name:      "filter_s3_and_lambda",
			queryJSON: `{"ResourceTypeFilters":["AWS::S3::Bucket","AWS::Lambda::Function"]}`,
			wantCount: 2,
		},
		{
			name:      "all_supported_returns_all",
			queryJSON: `{"ResourceTypeFilters":["AWS::AllSupported"]}`,
			wantCount: 3,
		},
		{
			name:      "empty_type_filters_returns_all",
			queryJSON: `{"ResourceTypeFilters":[]}`,
			wantCount: 3,
		},
		{
			name:      "no_match_returns_empty",
			queryJSON: `{"ResourceTypeFilters":["AWS::RDS::DBInstance"]}`,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			q := &resourcegroups.ResourceQuery{
				Type:  "TAG_FILTERS_1_0",
				Query: tt.queryJSON,
			}
			results, _, searchErr := b.SearchResources(context.Background(), q, "", 0)
			require.NoError(t, searchErr)
			assert.Len(t, results, tt.wantCount)

			if tt.wantTypeFound != "" {
				found := false
				for _, id := range results {
					if id.ResourceType == tt.wantTypeFound {
						found = true

						break
					}
				}
				assert.True(t, found, "expected type %s not found in results", tt.wantTypeFound)
			}
		})
	}
}

// TestSearchResources_CloudFormationQuery verifies CLOUDFORMATION_STACK_1_0 query.
func TestSearchResources_CloudFormationQuery(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateGroup(context.Background(), "cf-group", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.GroupResources(context.Background(), "cf-group", []string{"arn:aws:s3:::my-bucket"})
	require.NoError(t, err)

	q := &resourcegroups.ResourceQuery{
		Type:  "CLOUDFORMATION_STACK_1_0",
		Query: `{"StackIdentifier":"arn:aws:cloudformation:us-east-1:000000000000:stack/s/id"}`,
	}
	// CloudFormation query returns all grouped resources (no type restriction in our impl).
	results, _, err := b.SearchResources(context.Background(), q, "", 0)
	require.NoError(t, err)
	assert.Len(t, results, 1)
}

// TestSearchResources_Pagination verifies NextToken pagination.
func TestSearchResources_Pagination(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateGroup(context.Background(), "search-group", "", nil, nil, nil)
	require.NoError(t, err)

	arns := []string{
		"arn:aws:s3:::bucket-a",
		"arn:aws:s3:::bucket-b",
		"arn:aws:s3:::bucket-c",
		"arn:aws:s3:::bucket-d",
	}
	_, err = b.GroupResources(context.Background(), "search-group", arns)
	require.NoError(t, err)

	q := &resourcegroups.ResourceQuery{Type: "TAG_FILTERS_1_0", Query: `{"ResourceTypeFilters":["AWS::AllSupported"]}`}

	page1, tok1, err := b.SearchResources(context.Background(), q, "", 2)
	require.NoError(t, err)
	assert.Len(t, page1, 2)
	require.NotEmpty(t, tok1)

	page2, tok2, err := b.SearchResources(context.Background(), q, tok1, 2)
	require.NoError(t, err)
	assert.Len(t, page2, 2)
	assert.Empty(t, tok2)

	// No duplicates.
	seen := make(map[string]bool)
	for _, id := range append(page1, page2...) {
		assert.False(t, seen[id.ResourceArn])
		seen[id.ResourceArn] = true
	}
}

// TestSearchResources_NilQuery verifies nil query returns all resources.
func TestSearchResources_NilQuery(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateGroup(context.Background(), "g1", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.GroupResources(context.Background(), "g1", []string{"arn:aws:s3:::b1", "arn:aws:s3:::b2"})
	require.NoError(t, err)

	// nil query = match all (backwards-compatible behavior).
	results, _, err := b.SearchResources(context.Background(), nil, "", 0)
	require.NoError(t, err)
	assert.Len(t, results, 2)
}

// TestSearchResources_DeduplicatesWithType verifies ResourceType in deduped results.
func TestSearchResources_DeduplicatesWithType(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateGroup(context.Background(), "g1", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.CreateGroup(context.Background(), "g2", "", nil, nil, nil)
	require.NoError(t, err)

	sharedARN := "arn:aws:s3:::shared-bucket"
	_, err = b.GroupResources(context.Background(), "g1", []string{sharedARN})
	require.NoError(t, err)
	_, err = b.GroupResources(context.Background(), "g2", []string{sharedARN})
	require.NoError(t, err)

	results, _, err := b.SearchResources(context.Background(), nil, "", 0)
	require.NoError(t, err)
	require.Len(t, results, 1, "deduplicated across groups")
	assert.Equal(t, sharedARN, results[0].ResourceArn)
	assert.Equal(t, "AWS::S3::Bucket", results[0].ResourceType)
}

// TestSearchResources_DeduplicatesAcrossGroups verifies cross-group de-dup.
func TestSearchResources_DeduplicatesAcrossGroups(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateGroup(context.Background(), "g1", "", nil, nil, nil)
	_, _ = b.CreateGroup(context.Background(), "g2", "", nil, nil, nil)

	// Same ARN added to both groups.
	_, _ = b.GroupResources(context.Background(), "g1", []string{"arn:aws:s3:::shared"})
	_, _ = b.GroupResources(context.Background(), "g2", []string{"arn:aws:s3:::shared"})

	results, _, err := b.SearchResources(context.Background(), nil, "", 0)
	require.NoError(t, err)
	assert.Len(t, results, 1)
}
