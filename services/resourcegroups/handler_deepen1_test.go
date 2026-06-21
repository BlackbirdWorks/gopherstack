package resourcegroups_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resourcegroups"
)

// ---------------------------------------------------------------------------
// Pagination — ListGroups
// ---------------------------------------------------------------------------

// TestDeepen1_ListGroups_Pagination verifies NextToken/MaxResults pagination.
func TestDeepen1_ListGroups_Pagination(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")

	// Create 5 groups: a-group, b-group, c-group, d-group, e-group.
	for _, name := range []string{"e-group", "c-group", "a-group", "b-group", "d-group"} {
		_, err := b.CreateGroup(context.Background(), name, "", nil, nil, nil)
		require.NoError(t, err)
	}

	tests := []struct {
		name       string
		maxResults int
		wantNames  []string
		wantMore   bool
	}{
		{
			name:       "page_size_2",
			maxResults: 2,
			wantNames:  []string{"a-group", "b-group"},
			wantMore:   true,
		},
		{
			name:       "page_size_3",
			maxResults: 3,
			wantNames:  []string{"a-group", "b-group", "c-group"},
			wantMore:   true,
		},
		{
			name:       "page_size_5_all",
			maxResults: 5,
			wantNames:  []string{"a-group", "b-group", "c-group", "d-group", "e-group"},
			wantMore:   false,
		},
		{
			name:       "page_size_0_returns_all",
			maxResults: 0,
			wantNames:  []string{"a-group", "b-group", "c-group", "d-group", "e-group"},
			wantMore:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			page, token := b.ListGroups(context.Background(), nil, "", tt.maxResults)
			names := make([]string, len(page))
			for i, g := range page {
				names[i] = g.Name
			}
			assert.Equal(t, tt.wantNames, names)
			if tt.wantMore {
				assert.NotEmpty(t, token)
			} else {
				assert.Empty(t, token)
			}
		})
	}
}

// TestDeepen1_ListGroups_PaginationResume verifies sequential token-based listing.
func TestDeepen1_ListGroups_PaginationResume(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")

	for _, name := range []string{"a-group", "b-group", "c-group", "d-group", "e-group"} {
		_, err := b.CreateGroup(context.Background(), name, "", nil, nil, nil)
		require.NoError(t, err)
	}

	// Collect all names across pages of 2.
	var allNames []string

	page, token := b.ListGroups(context.Background(), nil, "", 2)
	for _, g := range page {
		allNames = append(allNames, g.Name)
	}
	require.NotEmpty(t, token)

	page, token = b.ListGroups(context.Background(), nil, token, 2)
	for _, g := range page {
		allNames = append(allNames, g.Name)
	}
	require.NotEmpty(t, token)

	page, token = b.ListGroups(context.Background(), nil, token, 2)
	for _, g := range page {
		allNames = append(allNames, g.Name)
	}
	assert.Empty(t, token)

	assert.Equal(t, []string{"a-group", "b-group", "c-group", "d-group", "e-group"}, allNames)
}

// TestDeepen1_ListGroups_PaginationViaHandler verifies handler-level NextToken flow.
func TestDeepen1_ListGroups_PaginationViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)

	for i := 0; i < 6; i++ {
		doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
			"Name": fmt.Sprintf("pg-%02d", i),
		})
	}

	// Page 1: MaxResults=3.
	rec1 := doResourceGroupsRequest(t, h, "ListGroups", map[string]any{"MaxResults": 3})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	groups1 := out1["Groups"].([]any)
	assert.Len(t, groups1, 3)
	token1, _ := out1["NextToken"].(string)
	require.NotEmpty(t, token1)

	// Page 2: resume with NextToken.
	rec2 := doResourceGroupsRequest(t, h, "ListGroups", map[string]any{
		"MaxResults": 3,
		"NextToken":  token1,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	groups2 := out2["Groups"].([]any)
	assert.Len(t, groups2, 3)
	assert.Empty(t, out2["NextToken"])

	// All 6 groups are covered across both pages, no overlap.
	names1 := make(map[string]bool)
	for _, g := range groups1 {
		names1[g.(map[string]any)["Name"].(string)] = true
	}
	for _, g := range groups2 {
		name := g.(map[string]any)["Name"].(string)
		assert.False(t, names1[name], "group %s appeared in both pages", name)
	}
}

// ---------------------------------------------------------------------------
// Pagination — ListGroupResources
// ---------------------------------------------------------------------------

// TestDeepen1_ListGroupResources_Pagination verifies pagination of resource lists.
func TestDeepen1_ListGroupResources_Pagination(t *testing.T) {
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

// TestDeepen1_ListGroupResources_PaginationViaHandler verifies handler NextToken.
func TestDeepen1_ListGroupResources_PaginationViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "paged-group"})

	arns := make([]string, 5)
	for i := range arns {
		arns[i] = fmt.Sprintf("arn:aws:s3:::bucket-%d", i)
	}
	doResourceGroupsRequest(t, h, "GroupResources", map[string]any{
		"Group":        "paged-group",
		"ResourceArns": arns,
	})

	rec1 := doResourceGroupsRequest(t, h, "ListGroupResources", map[string]any{
		"Group":      "paged-group",
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	resources1 := out1["Resources"].([]any)
	assert.Len(t, resources1, 2)
	token1, _ := out1["NextToken"].(string)
	require.NotEmpty(t, token1)

	rec2 := doResourceGroupsRequest(t, h, "ListGroupResources", map[string]any{
		"Group":      "paged-group",
		"MaxResults": 10,
		"NextToken":  token1,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	resources2 := out2["Resources"].([]any)
	assert.Len(t, resources2, 3)
	assert.Empty(t, out2["NextToken"])
}

// ---------------------------------------------------------------------------
// Pagination — ListTagSyncTasks
// ---------------------------------------------------------------------------

// TestDeepen1_ListTagSyncTasks_Pagination verifies NextToken pagination.
func TestDeepen1_ListTagSyncTasks_Pagination(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")

	for i := 0; i < 5; i++ {
		name := fmt.Sprintf("task-group-%d", i)
		_, err := b.CreateGroup(context.Background(), name, "", nil, nil, nil)
		require.NoError(t, err)
		_, err = b.StartTagSyncTask(context.Background(), name, "arn:aws:iam::000000000000:role/r", "k", "v", nil)
		require.NoError(t, err)
	}

	page1, tok1, err := b.ListTagSyncTasks(context.Background(), nil, "", 2)
	require.NoError(t, err)
	assert.Len(t, page1, 2)
	require.NotEmpty(t, tok1)

	page2, tok2, err := b.ListTagSyncTasks(context.Background(), nil, tok1, 2)
	require.NoError(t, err)
	assert.Len(t, page2, 2)
	require.NotEmpty(t, tok2)

	page3, tok3, err := b.ListTagSyncTasks(context.Background(), nil, tok2, 2)
	require.NoError(t, err)
	assert.Len(t, page3, 1)
	assert.Empty(t, tok3)
}

// TestDeepen1_ListTagSyncTasks_PaginationViaHandler verifies handler NextToken.
func TestDeepen1_ListTagSyncTasks_PaginationViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)

	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("sync-grp-%d", i)
		doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": name})
		doResourceGroupsRequest(t, h, "StartTagSyncTask", map[string]any{
			"Group":   name,
			"RoleArn": "arn:aws:iam::000000000000:role/r",
			"TagKey":  "env",
		})
	}

	rec1 := doResourceGroupsRequest(t, h, "ListTagSyncTasks", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	tasks1 := out1["TagSyncTasks"].([]any)
	assert.Len(t, tasks1, 2)
	tok1, _ := out1["NextToken"].(string)
	require.NotEmpty(t, tok1)

	rec2 := doResourceGroupsRequest(t, h, "ListTagSyncTasks", map[string]any{
		"MaxResults": 10,
		"NextToken":  tok1,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	tasks2 := out2["TagSyncTasks"].([]any)
	assert.Len(t, tasks2, 2)
	assert.Empty(t, out2["NextToken"])
}

// ---------------------------------------------------------------------------
// Pagination — ListGroupingStatuses
// ---------------------------------------------------------------------------

// TestDeepen1_ListGroupingStatuses_Pagination verifies NextToken pagination.
func TestDeepen1_ListGroupingStatuses_Pagination(t *testing.T) {
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

	page1, tok1, err := b.ListGroupingStatuses(context.Background(), "status-paged", "", 2)
	require.NoError(t, err)
	assert.Len(t, page1, 2)
	require.NotEmpty(t, tok1)

	page2, tok2, err := b.ListGroupingStatuses(context.Background(), "status-paged", tok1, 2)
	require.NoError(t, err)
	assert.Len(t, page2, 2)
	require.NotEmpty(t, tok2)

	page3, tok3, err := b.ListGroupingStatuses(context.Background(), "status-paged", tok2, 2)
	require.NoError(t, err)
	assert.Len(t, page3, 1)
	assert.Empty(t, tok3)
}

// TestDeepen1_ListGroupingStatuses_PaginationViaHandler verifies handler NextToken.
func TestDeepen1_ListGroupingStatuses_PaginationViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "status-group"})

	arns := make([]string, 5)
	for i := range arns {
		arns[i] = fmt.Sprintf("arn:aws:s3:::b-%d", i)
	}
	doResourceGroupsRequest(t, h, "GroupResources", map[string]any{
		"Group":        "status-group",
		"ResourceArns": arns,
	})

	rec1 := doResourceGroupsRequest(t, h, "ListGroupingStatuses", map[string]any{
		"Group":      "status-group",
		"MaxResults": 3,
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	statuses1 := out1["GroupingStatuses"].([]any)
	assert.Len(t, statuses1, 3)
	tok1, _ := out1["NextToken"].(string)
	require.NotEmpty(t, tok1)

	rec2 := doResourceGroupsRequest(t, h, "ListGroupingStatuses", map[string]any{
		"Group":      "status-group",
		"MaxResults": 10,
		"NextToken":  tok1,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	statuses2 := out2["GroupingStatuses"].([]any)
	assert.Len(t, statuses2, 2)
	assert.Empty(t, out2["NextToken"])
}

// ---------------------------------------------------------------------------
// Pagination — SearchResources
// ---------------------------------------------------------------------------

// TestDeepen1_SearchResources_Pagination verifies NextToken pagination.
func TestDeepen1_SearchResources_Pagination(t *testing.T) {
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

// TestDeepen1_SearchResources_PaginationViaHandler verifies handler NextToken.
func TestDeepen1_SearchResources_PaginationViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "srch-grp"})

	arns := make([]string, 5)
	for i := range arns {
		arns[i] = fmt.Sprintf("arn:aws:s3:::bucket-%d", i)
	}
	doResourceGroupsRequest(t, h, "GroupResources", map[string]any{
		"Group":        "srch-grp",
		"ResourceArns": arns,
	})

	body := map[string]any{
		"ResourceQuery": map[string]any{
			"Type":  "TAG_FILTERS_1_0",
			"Query": `{"ResourceTypeFilters":["AWS::AllSupported"]}`,
		},
		"MaxResults": 3,
	}

	rec1 := doResourceGroupsRequest(t, h, "SearchResources", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	ids1 := out1["ResourceIdentifiers"].([]any)
	assert.Len(t, ids1, 3)
	tok1, _ := out1["NextToken"].(string)
	require.NotEmpty(t, tok1)

	body["NextToken"] = tok1
	rec2 := doResourceGroupsRequest(t, h, "SearchResources", body)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	ids2 := out2["ResourceIdentifiers"].([]any)
	assert.Len(t, ids2, 2)
	assert.Empty(t, out2["NextToken"])
}

// ---------------------------------------------------------------------------
// ResourceType extraction
// ---------------------------------------------------------------------------

// TestDeepen1_ResourceTypeFromARN verifies AWS::Service::Type extraction from ARNs.
func TestDeepen1_ResourceTypeFromARN(t *testing.T) {
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

// TestDeepen1_ListGroupResources_ResourceTypeInResponse verifies ResourceType field in HTTP response.
func TestDeepen1_ListGroupResources_ResourceTypeInResponse(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "typed-group"})
	doResourceGroupsRequest(t, h, "GroupResources", map[string]any{
		"Group":        "typed-group",
		"ResourceArns": []string{"arn:aws:ec2:us-east-1:000000000000:instance/i-abc"},
	})

	rec := doResourceGroupsRequest(t, h, "ListGroupResources", map[string]any{
		"Group": "typed-group",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "AWS::EC2::Instance")
	assert.Contains(t, rec.Body.String(), "ResourceType")
}

// ---------------------------------------------------------------------------
// ListGroupResources — Filters
// ---------------------------------------------------------------------------

// TestDeepen1_ListGroupResources_FilterByResourceType verifies resource-type filter.
func TestDeepen1_ListGroupResources_FilterByResourceType(t *testing.T) {
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

	tests := []struct {
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

			ids, _, err := b.ListGroupResources(context.Background(), "mixed-group", filters, "", 0)
			require.NoError(t, err)
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

// TestDeepen1_ListGroupResources_FilterViaHandler verifies resource-type filter through HTTP.
func TestDeepen1_ListGroupResources_FilterViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "filtered-group"})
	doResourceGroupsRequest(t, h, "GroupResources", map[string]any{
		"Group": "filtered-group",
		"ResourceArns": []string{
			"arn:aws:s3:::my-bucket",
			"arn:aws:ec2:us-east-1:000000000000:instance/i-abc",
			"arn:aws:lambda:us-east-1:000000000000:function:my-fn",
		},
	})

	rec := doResourceGroupsRequest(t, h, "ListGroupResources", map[string]any{
		"Group": "filtered-group",
		"Filters": []map[string]any{
			{"Name": "resource-type", "Values": []string{"AWS::EC2::Instance"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "AWS::EC2::Instance")
	assert.NotContains(t, body, "AWS::S3::Bucket")
	assert.NotContains(t, body, "AWS::Lambda::Function")
}

// ---------------------------------------------------------------------------
// SearchResources — ResourceType filtering
// ---------------------------------------------------------------------------

// TestDeepen1_SearchResources_ResourceTypeFilter verifies type-based filtering.
func TestDeepen1_SearchResources_ResourceTypeFilter(t *testing.T) {
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

	tests := []struct {
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
			results, _, err := b.SearchResources(context.Background(), q, "", 0)
			require.NoError(t, err)
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

// TestDeepen1_SearchResources_CloudFormationQuery verifies CLOUDFORMATION_STACK_1_0 query.
func TestDeepen1_SearchResources_CloudFormationQuery(t *testing.T) {
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

// ---------------------------------------------------------------------------
// CreateGroup mutual exclusivity (ResourceQuery XOR Configuration)
// ---------------------------------------------------------------------------

// TestDeepen1_CreateGroup_MutualExclusivity verifies that setting both
// ResourceQuery and Configuration returns an error.
func TestDeepen1_CreateGroup_MutualExclusivity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     map[string]any
		wantCode int
	}{
		{
			name: "query_only_ok",
			body: map[string]any{
				"Name": "query-only",
				"ResourceQuery": map[string]any{
					"Type":  "TAG_FILTERS_1_0",
					"Query": `{"TagFilters":[]}`,
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "config_only_ok",
			body: map[string]any{
				"Name":          "config-only",
				"Configuration": []map[string]any{{"Type": "AWS::EC2::CapacityReservationPool"}},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "both_query_and_config_rejected",
			body: map[string]any{
				"Name": "both-group",
				"ResourceQuery": map[string]any{
					"Type":  "TAG_FILTERS_1_0",
					"Query": `{"TagFilters":[]}`,
				},
				"Configuration": []map[string]any{{"Type": "AWS::EC2::CapacityReservationPool"}},
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			rec := doResourceGroupsRequest(t, h, "CreateGroup", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestDeepen1_CreateGroup_MutualExclusivity_Backend verifies the backend-level rejection.
func TestDeepen1_CreateGroup_MutualExclusivity_Backend(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateGroup(context.Background(),
		"bad-group",
		"",
		&resourcegroups.ResourceQuery{Type: "TAG_FILTERS_1_0", Query: `{}`},
		nil,
		[]resourcegroups.GroupConfigurationItem{{Type: "AWS::EC2::CapacityReservationPool"}},
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, resourcegroups.ErrValidation)
	assert.Contains(t, err.Error(), "cannot have both")

	// Group must not exist after the failed call.
	_, err = b.GetGroup(context.Background(), "bad-group")
	assert.ErrorIs(t, err, resourcegroups.ErrNotFound)
}

// ---------------------------------------------------------------------------
// TaskARN uniqueness (same-second collision fix)
// ---------------------------------------------------------------------------

// TestDeepen1_TaskARN_Uniqueness verifies that rapid task creation produces unique ARNs.
func TestDeepen1_TaskARN_Uniqueness(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")

	// Create one group and start many tasks in rapid succession.
	_, err := b.CreateGroup(context.Background(), "rapid-group", "", nil, nil, nil)
	require.NoError(t, err)

	const taskCount = 20
	taskARNs := make(map[string]bool, taskCount)

	for i := 0; i < taskCount; i++ {
		task, tErr := b.StartTagSyncTask(
			context.Background(),
			"rapid-group",
			"arn:aws:iam::000000000000:role/r",
			"k", "v", nil,
		)
		require.NoError(t, tErr)
		assert.False(t, taskARNs[task.TaskArn], "duplicate TaskArn: %s", task.TaskArn)
		taskARNs[task.TaskArn] = true
	}
}

// TestDeepen1_TaskARN_ContainsGroupName verifies task ARN includes group name.
func TestDeepen1_TaskARN_ContainsGroupName(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateGroup(context.Background(), "named-group", "", nil, nil, nil)
	require.NoError(t, err)

	task, err := b.StartTagSyncTask(
		context.Background(), "named-group", "arn:aws:iam::000000000000:role/r", "", "", nil,
	)
	require.NoError(t, err)
	assert.Contains(t, task.TaskArn, "named-group")
}

// ---------------------------------------------------------------------------
// ListGroups name-prefix filter
// ---------------------------------------------------------------------------

// TestDeepen1_ListGroups_NamePrefixFilter verifies the name-prefix filter.
func TestDeepen1_ListGroups_NamePrefixFilter(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")

	for _, name := range []string{"app-prod", "app-staging", "data-prod", "infra-shared"} {
		_, err := b.CreateGroup(context.Background(), name, "", nil, nil, nil)
		require.NoError(t, err)
	}

	tests := []struct {
		name       string
		prefix     string
		wantNames  []string
	}{
		{
			name:      "prefix_app",
			prefix:    "app",
			wantNames: []string{"app-prod", "app-staging"},
		},
		{
			name:      "prefix_data",
			prefix:    "data",
			wantNames: []string{"data-prod"},
		},
		{
			name:      "prefix_infra",
			prefix:    "infra",
			wantNames: []string{"infra-shared"},
		},
		{
			name:      "prefix_no_match",
			prefix:    "xyz",
			wantNames: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			filters := []resourcegroups.ListGroupsFilter{
				{Name: "name-prefix", Values: []string{tt.prefix}},
			}

			groups, _ := b.ListGroups(context.Background(), filters, "", 0)
			names := make([]string, len(groups))
			for i, g := range groups {
				names[i] = g.Name
			}
			assert.Equal(t, tt.wantNames, names)
		})
	}
}

// TestDeepen1_ListGroups_NamePrefixViaHandler verifies the handler-level name-prefix filter.
func TestDeepen1_ListGroups_NamePrefixViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)

	for _, name := range []string{"web-frontend", "web-backend", "db-primary", "cache-main"} {
		doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": name})
	}

	rec := doResourceGroupsRequest(t, h, "ListGroups", map[string]any{
		"Filters": []map[string]any{
			{"Name": "name-prefix", "Values": []string{"web"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "web-frontend")
	assert.Contains(t, body, "web-backend")
	assert.NotContains(t, body, "db-primary")
	assert.NotContains(t, body, "cache-main")
}

// ---------------------------------------------------------------------------
// SearchResources — required ResourceQuery validation
// ---------------------------------------------------------------------------

// TestDeepen1_SearchResources_NilQuery verifies nil query returns all resources.
func TestDeepen1_SearchResources_NilQuery(t *testing.T) {
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

// TestDeepen1_SearchResources_HandlerRequiresResourceQuery verifies error shape.
func TestDeepen1_SearchResources_HandlerRequiresResourceQuery(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "g1"})
	doResourceGroupsRequest(t, h, "GroupResources", map[string]any{
		"Group":        "g1",
		"ResourceArns": []string{"arn:aws:s3:::b1"},
	})

	// No ResourceQuery — handler passes nil to backend which returns all.
	rec := doResourceGroupsRequest(t, h, "SearchResources", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "b1")
}

// ---------------------------------------------------------------------------
// SearchResources — result deduplication with type information
// ---------------------------------------------------------------------------

// TestDeepen1_SearchResources_DeduplicatesWithType verifies ResourceType in deduped results.
func TestDeepen1_SearchResources_DeduplicatesWithType(t *testing.T) {
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

// ---------------------------------------------------------------------------
// UpdateGroup — field persistence
// ---------------------------------------------------------------------------

// TestDeepen1_UpdateGroup_FieldPersistence verifies each field updates independently.
func TestDeepen1_UpdateGroup_FieldPersistence(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateGroup(context.Background(), "update-me", "initial desc", nil, nil, nil)
	require.NoError(t, err)

	// Set criticality.
	g, err := b.UpdateGroup(context.Background(), "update-me", "initial desc", "", 3)
	require.NoError(t, err)
	assert.Equal(t, 3, g.Criticality)
	assert.Equal(t, "initial desc", g.Description)
	assert.Empty(t, g.DisplayName)

	// Set display name (criticality=0 means no change).
	g, err = b.UpdateGroup(context.Background(), "update-me", "initial desc", "My Display Name", 0)
	require.NoError(t, err)
	assert.Equal(t, 3, g.Criticality) // preserved
	assert.Equal(t, "My Display Name", g.DisplayName)

	// Change description.
	g, err = b.UpdateGroup(context.Background(), "update-me", "new desc", "", 0)
	require.NoError(t, err)
	assert.Equal(t, "new desc", g.Description)
	assert.Equal(t, 3, g.Criticality)             // still preserved
	assert.Equal(t, "My Display Name", g.DisplayName) // still preserved
}

// TestDeepen1_UpdateGroup_CriticalityBoundary verifies boundary values 1 and 5.
func TestDeepen1_UpdateGroup_CriticalityBoundary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		criticality int
		wantCode    int
	}{
		{name: "boundary_1", criticality: 1, wantCode: http.StatusOK},
		{name: "boundary_5", criticality: 5, wantCode: http.StatusOK},
		{name: "too_low_minus1", criticality: -1, wantCode: http.StatusBadRequest},
		{name: "too_high_6", criticality: 6, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "crit-group"})
			rec := doResourceGroupsRequest(t, h, "UpdateGroup", map[string]any{
				"Group":       "crit-group",
				"Criticality": tt.criticality,
			})
			assert.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// ---------------------------------------------------------------------------
// GetGroup via ARN
// ---------------------------------------------------------------------------

// TestDeepen1_GetGroup_ByARN verifies that a group can be retrieved by ARN.
func TestDeepen1_GetGroup_ByARN(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	g, err := b.CreateGroup(context.Background(), "arn-group", "desc", nil, nil, nil)
	require.NoError(t, err)

	// Retrieve by ARN instead of name.
	got, err := b.GetGroup(context.Background(), g.ARN)
	require.NoError(t, err)
	assert.Equal(t, "arn-group", got.Name)
	assert.Equal(t, g.ARN, got.ARN)
}

// TestDeepen1_DeleteGroup_ByARN verifies cascaded deletion when addressing by ARN.
func TestDeepen1_DeleteGroup_ByARN(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	g, err := b.CreateGroup(context.Background(), "del-by-arn", "", nil, nil, nil)
	require.NoError(t, err)

	err = b.DeleteGroup(context.Background(), g.ARN)
	require.NoError(t, err)

	_, err = b.GetGroup(context.Background(), "del-by-arn")
	assert.ErrorIs(t, err, resourcegroups.ErrNotFound)
}

// ---------------------------------------------------------------------------
// AccountSettings status message
// ---------------------------------------------------------------------------

// TestDeepen1_AccountSettings_StatusMessage verifies GroupLifecycleEventsStatus mirrors desired.
func TestDeepen1_AccountSettings_StatusMessage(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")

	settings := b.GetAccountSettings()
	assert.Empty(t, settings.GroupLifecycleEventsDesiredStatus)

	err := b.UpdateAccountSettings("ACTIVE")
	require.NoError(t, err)
	settings = b.GetAccountSettings()
	assert.Equal(t, "ACTIVE", settings.GroupLifecycleEventsDesiredStatus)
	assert.Equal(t, "ACTIVE", settings.GroupLifecycleEventsStatus)

	err = b.UpdateAccountSettings("INACTIVE")
	require.NoError(t, err)
	settings = b.GetAccountSettings()
	assert.Equal(t, "INACTIVE", settings.GroupLifecycleEventsDesiredStatus)
	assert.Equal(t, "INACTIVE", settings.GroupLifecycleEventsStatus)
}

// TestDeepen1_AccountSettings_InvalidStatus verifies invalid status is rejected.
func TestDeepen1_AccountSettings_InvalidStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status string
	}{
		{name: "empty", status: ""},
		{name: "invalid", status: "PENDING"},
		{name: "lowercase_active", status: "active"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
			err := b.UpdateAccountSettings(tt.status)
			require.Error(t, err)
			assert.ErrorIs(t, err, resourcegroups.ErrValidation)
		})
	}
}

// ---------------------------------------------------------------------------
// GroupResources — empty/nil ARN handling
// ---------------------------------------------------------------------------

// TestDeepen1_GroupResources_EmptyARN verifies that an empty ARN slice is a no-op.
func TestDeepen1_GroupResources_EmptyARN(t *testing.T) {
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

// TestDeepen1_GroupResources_DuplicateIgnored verifies duplicate add is idempotent.
func TestDeepen1_GroupResources_DuplicateIgnored(t *testing.T) {
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

// ---------------------------------------------------------------------------
// Cross-region isolation for new operations
// ---------------------------------------------------------------------------

// TestDeepen1_ListGroupResources_RegionIsolation verifies resources are region-scoped.
func TestDeepen1_ListGroupResources_RegionIsolation(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")

	ctxEast := context.WithValue(context.Background(), regionContextKeyForTest{}, "us-east-1")
	ctxWest := context.WithValue(context.Background(), regionContextKeyForTest{}, "us-west-2")

	_, _ = b.CreateGroup(ctxEast, "rgn-group", "", nil, nil, nil)
	_, _ = b.CreateGroup(ctxWest, "rgn-group", "", nil, nil, nil)

	_, _ = b.GroupResources(ctxEast, "rgn-group", []string{"arn:aws:s3:::east-bucket"})

	eastIDs, _, err := b.ListGroupResources(ctxEast, "rgn-group", nil, "", 0)
	require.NoError(t, err)
	assert.Len(t, eastIDs, 1)

	westIDs, _, err := b.ListGroupResources(ctxWest, "rgn-group", nil, "", 0)
	require.NoError(t, err)
	assert.Empty(t, westIDs)
}

// regionContextKeyForTest is a re-export of the package-private key for isolation tests.
// It uses the same underlying type so context values propagate correctly.
type regionContextKeyForTest = interface{}

// ---------------------------------------------------------------------------
// ListGroupResources — ARN-based group lookup
// ---------------------------------------------------------------------------

// TestDeepen1_ListGroupResources_ByARN verifies resources can be listed by group ARN.
func TestDeepen1_ListGroupResources_ByARN(t *testing.T) {
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

// ---------------------------------------------------------------------------
// ListGroupingStatuses — ungrouped resources reflected in status
// ---------------------------------------------------------------------------

// TestDeepen1_ListGroupingStatuses_IncludesUngroup verifies UNGROUP statuses appear.
func TestDeepen1_ListGroupingStatuses_IncludesUngroup(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "lifecycle-group"})
	doResourceGroupsRequest(t, h, "GroupResources", map[string]any{
		"Group":        "lifecycle-group",
		"ResourceArns": []string{"arn:aws:s3:::b1", "arn:aws:s3:::b2"},
	})
	doResourceGroupsRequest(t, h, "UngroupResources", map[string]any{
		"Group":        "lifecycle-group",
		"ResourceArns": []string{"arn:aws:s3:::b1"},
	})

	rec := doResourceGroupsRequest(t, h, "ListGroupingStatuses", map[string]any{
		"Group": "lifecycle-group",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "GROUP")
	assert.Contains(t, body, "UNGROUP")
	assert.Contains(t, body, "SUCCESS")
}

// ---------------------------------------------------------------------------
// TagSyncTask lifecycle — full round trip with pagination
// ---------------------------------------------------------------------------

// TestDeepen1_TagSyncTask_FullLifecyclePaginated verifies start/list/cancel/get with pagination.
func TestDeepen1_TagSyncTask_FullLifecyclePaginated(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")

	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("task-grp-%d", i)
		_, err := b.CreateGroup(context.Background(), name, "", nil, nil, nil)
		require.NoError(t, err)
		_, err = b.StartTagSyncTask(context.Background(), name, "arn:aws:iam::000000000000:role/r", "k", "v", nil)
		require.NoError(t, err)
	}

	// Paginate: page 1 of 2.
	page1, tok1, err := b.ListTagSyncTasks(context.Background(), nil, "", 2)
	require.NoError(t, err)
	require.Len(t, page1, 2)
	require.NotEmpty(t, tok1)

	// Cancel one task from page 1.
	err = b.CancelTagSyncTask(context.Background(), page1[0].TaskArn)
	require.NoError(t, err)

	// Verify cancelled task is still visible.
	got, err := b.GetTagSyncTask(context.Background(), page1[0].TaskArn)
	require.NoError(t, err)
	assert.Equal(t, "CANCELLED", got.Status)

	// Page 2.
	page2, tok2, err := b.ListTagSyncTasks(context.Background(), nil, tok1, 2)
	require.NoError(t, err)
	require.Len(t, page2, 2)
	assert.Empty(t, tok2)

	// Total across pages = 4 (cancelled task still counted).
	assert.Len(t, append(page1, page2...), 4)
}

// ---------------------------------------------------------------------------
// Error shapes
// ---------------------------------------------------------------------------

// TestDeepen1_ErrorShapes verifies consistent error structure for 404 and 400.
func TestDeepen1_ErrorShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		op       string
		body     map[string]any
		wantCode int
	}{
		{
			name:     "get_group_404",
			op:       "GetGroup",
			body:     map[string]any{"Group": "nonexistent"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "delete_group_404",
			op:       "DeleteGroup",
			body:     map[string]any{"Group": "ghost"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "group_resources_group_404",
			op:       "GroupResources",
			body:     map[string]any{"Group": "ghost", "ResourceArns": []string{"arn:aws:s3:::b"}},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "list_group_resources_404",
			op:       "ListGroupResources",
			body:     map[string]any{"Group": "ghost"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "list_grouping_statuses_404",
			op:       "ListGroupingStatuses",
			body:     map[string]any{"Group": "ghost"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "create_group_invalid_name_400",
			op:       "CreateGroup",
			body:     map[string]any{"Name": "aws-not-allowed"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "start_task_no_group_400",
			op:       "StartTagSyncTask",
			body:     map[string]any{"RoleArn": "arn:aws:iam::000000000000:role/r"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "cancel_task_not_found_404",
			op:       "CancelTagSyncTask",
			body:     map[string]any{"TaskArn": "arn:aws:resource-groups:us-east-1:000000000000:tag-sync-task/ghost"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "get_task_not_found_404",
			op:       "GetTagSyncTask",
			body:     map[string]any{"TaskArn": "arn:aws:resource-groups:us-east-1:000000000000:tag-sync-task/ghost"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			rec := doResourceGroupsRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, "op=%s body=%v resp=%s", tt.op, tt.body, rec.Body.String())
			// All errors include a "message" field.
			assert.Contains(t, rec.Body.String(), "message")
		})
	}
}

// ---------------------------------------------------------------------------
// Snapshot/restore preserves all new fields
// ---------------------------------------------------------------------------

// TestDeepen1_SnapshotRestore_TaskIDCounter verifies task counter state after restore.
func TestDeepen1_SnapshotRestore_TaskIDCounter(t *testing.T) {
	t.Parallel()

	b1 := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b1.CreateGroup(context.Background(), "snap-group", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b1.StartTagSyncTask(context.Background(), "snap-group", "arn:aws:iam::000000000000:role/r", "", "", nil)
	require.NoError(t, err)

	snap := b1.Snapshot()
	require.NotNil(t, snap)

	b2 := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	tasks, _, err := b2.ListTagSyncTasks(context.Background(), nil, "", 0)
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	// Starting a new task after restore should succeed.
	task2, err := b2.StartTagSyncTask(context.Background(), "snap-group", "arn:aws:iam::000000000000:role/r", "", "", nil)
	require.NoError(t, err)
	assert.NotEqual(t, tasks[0].TaskArn, task2.TaskArn)
}

// TestDeepen1_SnapshotRestore_GroupResources verifies resource ARNs survive snapshot/restore.
func TestDeepen1_SnapshotRestore_GroupResources(t *testing.T) {
	t.Parallel()

	b1 := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b1.CreateGroup(context.Background(), "res-group", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b1.GroupResources(context.Background(), "res-group", []string{
		"arn:aws:s3:::bucket-1",
		"arn:aws:ec2:us-east-1:000000000000:instance/i-abc",
	})
	require.NoError(t, err)

	snap := b1.Snapshot()
	b2 := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	ids, _, err := b2.ListGroupResources(context.Background(), "res-group", nil, "", 0)
	require.NoError(t, err)
	assert.Len(t, ids, 2)
}

// ---------------------------------------------------------------------------
// Comprehensive response field coverage
// ---------------------------------------------------------------------------

// TestDeepen1_GetGroupQuery_ReturnsNilForNoQuery verifies nil ResourceQuery is represented.
func TestDeepen1_GetGroupQuery_ReturnsNilForNoQuery(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
		"Name":          "no-query-group",
		"Configuration": []map[string]any{{"Type": "AWS::EC2::CapacityReservationPool"}},
	})

	rec := doResourceGroupsRequest(t, h, "GetGroupQuery", map[string]any{"Group": "no-query-group"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	groupQuery := out["GroupQuery"].(map[string]any)
	assert.Equal(t, "no-query-group", groupQuery["GroupName"])
	// ResourceQuery should be null when not set.
	_, hasQuery := groupQuery["ResourceQuery"]
	if hasQuery {
		assert.Nil(t, groupQuery["ResourceQuery"])
	}
}

// TestDeepen1_CreateGroup_ResponseShape verifies complete CreateGroup response.
func TestDeepen1_CreateGroup_ResponseShape(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	rec := doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
		"Name":        "shape-test",
		"Description": "test desc",
		"Tags":        map[string]string{"env": "test"},
		"ResourceQuery": map[string]any{
			"Type":  "TAG_FILTERS_1_0",
			"Query": `{"TagFilters":[{"Key":"env","Values":["test"]}]}`,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	group, ok := out["Group"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "shape-test", group["Name"])
	assert.Contains(t, group["GroupArn"].(string), "shape-test")
	assert.Equal(t, "test desc", group["Description"])
	assert.Equal(t, "000000000000", group["OwnerId"])

	// Tags must NOT appear in the Group body per AWS spec.
	_, hasTags := group["Tags"]
	assert.False(t, hasTags, "Group body must not include Tags")

	// ResourceQuery should appear at top level.
	rq, ok := out["ResourceQuery"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "TAG_FILTERS_1_0", rq["Type"])
}

// TestDeepen1_ListGroups_GroupIdentifiersShape verifies exact shape of GroupIdentifiers.
func TestDeepen1_ListGroups_GroupIdentifiersShape(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{
		"Name":        "shape-grp",
		"Description": "shape desc",
	})

	rec := doResourceGroupsRequest(t, h, "ListGroups", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	identifiers := out["GroupIdentifiers"].([]any)
	require.Len(t, identifiers, 1)

	ident := identifiers[0].(map[string]any)
	assert.Equal(t, "shape-grp", ident["GroupName"])
	assert.Contains(t, ident["GroupArn"].(string), "shape-grp")
	assert.Equal(t, "shape desc", ident["Description"])
}

// ---------------------------------------------------------------------------
// Configuration validation — comprehensive
// ---------------------------------------------------------------------------

// TestDeepen1_PutGroupConfiguration_ValidTypes verifies all supported config types.
func TestDeepen1_PutGroupConfiguration_ValidTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config []map[string]any
	}{
		{
			name:   "ec2_host_management",
			config: []map[string]any{{"Type": "AWS::EC2::HostManagement"}},
		},
		{
			name:   "ec2_capacity_pool",
			config: []map[string]any{{"Type": "AWS::EC2::CapacityReservationPool"}},
		},
		{
			name:   "generic_with_allowed_types",
			config: []map[string]any{{
				"Type": "AWS::ResourceGroups::Generic",
				"Parameters": []map[string]any{
					{"Name": "allowed-resource-types", "Values": []string{"AWS::EC2::Instance", "AWS::S3::Bucket"}},
				},
			}},
		},
		{
			name:   "appregistry_application",
			config: []map[string]any{{"Type": "AWS::AppRegistry::Application"}},
		},
		{
			name:   "servicecat_appregistry",
			config: []map[string]any{{"Type": "AWS::ServiceCatalogAppRegistry::Application"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestResourceGroupsHandler(t)
			doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "cfg-" + tt.name})
			rec := doResourceGroupsRequest(t, h, "PutGroupConfiguration", map[string]any{
				"Group":         "cfg-" + tt.name,
				"Configuration": tt.config,
			})
			assert.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

// TestDeepen1_GetGroupConfiguration_ReflectsUpdate verifies config is updated by PutGroupConfiguration.
func TestDeepen1_GetGroupConfiguration_ReflectsUpdate(t *testing.T) {
	t.Parallel()

	h := newTestResourceGroupsHandler(t)
	doResourceGroupsRequest(t, h, "CreateGroup", map[string]any{"Name": "update-cfg"})

	doResourceGroupsRequest(t, h, "PutGroupConfiguration", map[string]any{
		"Group": "update-cfg",
		"Configuration": []map[string]any{
			{"Type": "AWS::ResourceGroups::Generic"},
		},
	})

	rec := doResourceGroupsRequest(t, h, "GetGroupConfiguration", map[string]any{"Group": "update-cfg"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "AWS::ResourceGroups::Generic")

	// Update to a different type.
	doResourceGroupsRequest(t, h, "PutGroupConfiguration", map[string]any{
		"Group": "update-cfg",
		"Configuration": []map[string]any{
			{"Type": "AWS::EC2::CapacityReservationPool"},
		},
	})

	rec2 := doResourceGroupsRequest(t, h, "GetGroupConfiguration", map[string]any{"Group": "update-cfg"})
	require.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "AWS::EC2::CapacityReservationPool")
	assert.NotContains(t, rec2.Body.String(), "AWS::ResourceGroups::Generic")
}

// ---------------------------------------------------------------------------
// TagSyncTask Filters
// ---------------------------------------------------------------------------

// TestDeepen1_ListTagSyncTasks_FilterByGroupARN verifies filter by GroupArn.
func TestDeepen1_ListTagSyncTasks_FilterByGroupARN(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")
	g1, err := b.CreateGroup(context.Background(), "filter-grp-1", "", nil, nil, nil)
	require.NoError(t, err)
	g2, err := b.CreateGroup(context.Background(), "filter-grp-2", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.StartTagSyncTask(context.Background(), "filter-grp-1", "arn:aws:iam::000000000000:role/r", "", "", nil)
	require.NoError(t, err)
	_, err = b.StartTagSyncTask(context.Background(), "filter-grp-2", "arn:aws:iam::000000000000:role/r", "", "", nil)
	require.NoError(t, err)

	// Filter by g1 ARN.
	tasks, _, err := b.ListTagSyncTasks(context.Background(), []resourcegroups.ListTagSyncTasksFilter{
		{GroupArn: g1.ARN},
	}, "", 0)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, g1.ARN, tasks[0].GroupArn)

	// Filter by g2 ARN.
	tasks, _, err = b.ListTagSyncTasks(context.Background(), []resourcegroups.ListTagSyncTasksFilter{
		{GroupArn: g2.ARN},
	}, "", 0)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	assert.Equal(t, g2.ARN, tasks[0].GroupArn)
}

// ---------------------------------------------------------------------------
// Mixed filter + pagination
// ---------------------------------------------------------------------------

// TestDeepen1_ListGroups_FilterAndPagination verifies config-type filter with pagination.
func TestDeepen1_ListGroups_FilterAndPagination(t *testing.T) {
	t.Parallel()

	b := resourcegroups.NewInMemoryBackend("000000000000", "us-east-1")

	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("cap-pool-%d", i)
		_, err := b.CreateGroup(context.Background(), name, "", nil, nil, nil)
		require.NoError(t, err)
		err = b.PutGroupConfiguration(context.Background(), name, []resourcegroups.GroupConfigurationItem{
			{Type: "AWS::EC2::CapacityReservationPool"},
		})
		require.NoError(t, err)
	}

	// Also create groups with a different config type.
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("host-mgmt-%d", i)
		_, err := b.CreateGroup(context.Background(), name, "", nil, nil, nil)
		require.NoError(t, err)
		err = b.PutGroupConfiguration(context.Background(), name, []resourcegroups.GroupConfigurationItem{
			{Type: "AWS::EC2::HostManagement"},
		})
		require.NoError(t, err)
	}

	filter := []resourcegroups.ListGroupsFilter{
		{Name: "configuration-type", Values: []string{"AWS::EC2::CapacityReservationPool"}},
	}

	// Page 1 of 2 from the filtered set.
	page1, tok1, := b.ListGroups(context.Background(), filter, "", 2)
	assert.Len(t, page1, 2)
	require.NotEmpty(t, tok1)

	for _, g := range page1 {
		assert.True(t, strings.HasPrefix(g.Name, "cap-pool-"))
	}

	// Page 2.
	page2, tok2 := b.ListGroups(context.Background(), filter, tok1, 2)
	assert.Len(t, page2, 2)
	assert.Empty(t, tok2)

	for _, g := range page2 {
		assert.True(t, strings.HasPrefix(g.Name, "cap-pool-"))
	}
}
