package ec2_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestEC2Handler_CreateTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "create_tags_on_vpc",
			body: "Action=CreateTags&Version=2016-11-15" +
				"&ResourceId.1=vpc-default" +
				"&Tag.1.Key=Name&Tag.1.Value=my-vpc" +
				"&Tag.2.Key=Env&Tag.2.Value=test",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateTagsResponse", "true"},
		},
		{
			name: "create_tags_multiple_resources",
			body: "Action=CreateTags&Version=2016-11-15" +
				"&ResourceId.1=vpc-default&ResourceId.2=subnet-default" +
				"&Tag.1.Key=Project&Tag.1.Value=demo",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateTagsResponse", "true"},
		},
		{
			name:         "create_tags_no_resources",
			body:         "Action=CreateTags&Version=2016-11-15&Tag.1.Key=Name&Tag.1.Value=x",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateTagsResponse"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestEC2Handler_DeleteTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setupBody    string
		deleteBody   string
		wantContains []string
		wantCode     int
	}{
		{
			name:      "delete_specific_tag_key",
			setupBody: "Action=CreateTags&Version=2016-11-15&ResourceId.1=vpc-default&Tag.1.Key=Name&Tag.1.Value=test",
			deleteBody: "Action=DeleteTags&Version=2016-11-15" +
				"&ResourceId.1=vpc-default&Tag.1.Key=Name",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteTagsResponse", "true"},
		},
		{
			name:         "delete_tags_no_resources",
			setupBody:    "",
			deleteBody:   "Action=DeleteTags&Version=2016-11-15&Tag.1.Key=Name",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteTagsResponse"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			if tt.setupBody != "" {
				setupRec := postForm(t, h, tt.setupBody)
				require.Equal(t, http.StatusOK, setupRec.Code)
			}

			rec := postForm(t, h, tt.deleteBody)

			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestEC2Handler_DescribeTags_ReflectsCreatedTags(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create tags on the default VPC.
	createRec := postForm(t, h,
		"Action=CreateTags&Version=2016-11-15"+
			"&ResourceId.1=vpc-default"+
			"&Tag.1.Key=Name&Tag.1.Value=my-vpc")
	require.Equal(t, http.StatusOK, createRec.Code)

	// DescribeTags should return the tag.
	descRec := postForm(t, h, "Action=DescribeTags&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, descRec.Code)
	assert.Contains(t, descRec.Body.String(), "vpc-default")
	assert.Contains(t, descRec.Body.String(), "Name")
	assert.Contains(t, descRec.Body.String(), "my-vpc")
}

func TestEC2Handler_DescribeTags_AfterDelete(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Create a tag.
	createRec := postForm(t, h,
		"Action=CreateTags&Version=2016-11-15"+
			"&ResourceId.1=vpc-default"+
			"&Tag.1.Key=Name&Tag.1.Value=to-delete")
	require.Equal(t, http.StatusOK, createRec.Code)

	// Delete the tag.
	deleteRec := postForm(t, h,
		"Action=DeleteTags&Version=2016-11-15"+
			"&ResourceId.1=vpc-default"+
			"&Tag.1.Key=Name")
	require.Equal(t, http.StatusOK, deleteRec.Code)

	// DescribeTags should no longer contain the tag value.
	descRec := postForm(t, h, "Action=DescribeTags&Version=2016-11-15")
	assert.Equal(t, http.StatusOK, descRec.Code)
	assert.NotContains(t, descRec.Body.String(), "to-delete")
}

func TestInMemoryBackend_CreateDeleteDescribeTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, bk *ec2.InMemoryBackend)
		name         string
		resourceIDs  []string
		wantContains []ec2.TagEntry
		wantCount    int
	}{
		{
			name: "create_and_describe",
			setup: func(t *testing.T, bk *ec2.InMemoryBackend) {
				t.Helper()

				err := bk.CreateTags([]string{"vpc-default"}, map[string]string{"Name": "test-vpc"})
				require.NoError(t, err)
			},
			resourceIDs: nil,
			wantCount:   1,
			wantContains: []ec2.TagEntry{
				{ResourceID: "vpc-default", Key: "Name", Value: "test-vpc", ResourceType: "vpc"},
			},
		},
		{
			name: "create_multiple_resources",
			setup: func(t *testing.T, bk *ec2.InMemoryBackend) {
				t.Helper()

				err := bk.CreateTags(
					[]string{"vpc-default", "subnet-default"},
					map[string]string{"Env": "prod"},
				)
				require.NoError(t, err)
			},
			resourceIDs: nil,
			wantCount:   2,
		},
		{
			name: "delete_clears_key",
			setup: func(t *testing.T, bk *ec2.InMemoryBackend) {
				t.Helper()

				err := bk.CreateTags(
					[]string{"vpc-default"},
					map[string]string{"Name": "old", "Env": "dev"},
				)
				require.NoError(t, err)

				err = bk.DeleteTags([]string{"vpc-default"}, []string{"Name"})
				require.NoError(t, err)
			},
			resourceIDs: nil,
			wantCount:   1,
			wantContains: []ec2.TagEntry{
				{ResourceID: "vpc-default", Key: "Env", Value: "dev", ResourceType: "vpc"},
			},
		},
		{
			name: "filter_by_resource_id",
			setup: func(t *testing.T, bk *ec2.InMemoryBackend) {
				t.Helper()

				// Create a second VPC so we have two distinct tagable VPCs.
				vpc2, err := bk.CreateVpc("10.0.0.0/16")
				require.NoError(t, err)

				err = bk.CreateTags(
					[]string{"vpc-default", vpc2.ID},
					map[string]string{"Key": "val"},
				)
				require.NoError(t, err)
			},
			resourceIDs: []string{"vpc-default"},
			wantCount:   1,
			wantContains: []ec2.TagEntry{
				{ResourceID: "vpc-default", Key: "Key", Value: "val", ResourceType: "vpc"},
			},
		},
		{
			name: "delete_empty_keys_is_noop",
			setup: func(t *testing.T, bk *ec2.InMemoryBackend) {
				t.Helper()

				err := bk.CreateTags([]string{"vpc-default"}, map[string]string{"Name": "keep-me"})
				require.NoError(t, err)

				// Empty keys: should be a no-op, tag must remain.
				err = bk.DeleteTags([]string{"vpc-default"}, []string{})
				require.NoError(t, err)
			},
			resourceIDs: nil,
			wantCount:   1,
			wantContains: []ec2.TagEntry{
				{ResourceID: "vpc-default", Key: "Name", Value: "keep-me", ResourceType: "vpc"},
			},
		},
		{
			name: "delete_all_keys_removes_resource_entry",
			setup: func(t *testing.T, bk *ec2.InMemoryBackend) {
				t.Helper()

				err := bk.CreateTags([]string{"vpc-default"}, map[string]string{"Name": "gone"})
				require.NoError(t, err)

				err = bk.DeleteTags([]string{"vpc-default"}, []string{"Name"})
				require.NoError(t, err)
			},
			resourceIDs: nil,
			wantCount:   0,
		},
		{
			name: "create_tags_nonexistent_resource_returns_error",
			setup: func(t *testing.T, bk *ec2.InMemoryBackend) {
				t.Helper()

				err := bk.CreateTags(
					[]string{"vpc-does-not-exist"},
					map[string]string{"Key": "val"},
				)
				require.Error(t, err)
			},
			resourceIDs: nil,
			wantCount:   0,
		},
		{
			name: "create_tags_atomic_on_mixed_resources",
			setup: func(t *testing.T, bk *ec2.InMemoryBackend) {
				t.Helper()

				// First ID exists, second does not — neither should be tagged.
				err := bk.CreateTags(
					[]string{"vpc-default", "vpc-does-not-exist"},
					map[string]string{"Key": "val"},
				)
				require.Error(t, err)
			},
			resourceIDs: []string{"vpc-default"},
			wantCount:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(t, bk)

			entries := bk.DescribeTags(tt.resourceIDs)
			assert.Len(t, entries, tt.wantCount)

			for _, want := range tt.wantContains {
				found := false
				for _, e := range entries {
					if e.ResourceID == want.ResourceID && e.Key == want.Key &&
						e.Value == want.Value {
						assert.Equal(t, want.ResourceType, e.ResourceType)
						found = true

						break
					}
				}

				assert.True(t, found, "expected tag entry not found: %+v", want)
			}
		})
	}
}

func TestEC2Handler_DescribeTags_FilterByResourceID(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Tag two resources.
	createRec := postForm(t, h,
		"Action=CreateTags&Version=2016-11-15"+
			"&ResourceId.1=vpc-default&ResourceId.2=subnet-default"+
			"&Tag.1.Key=Name&Tag.1.Value=tagged")
	require.Equal(t, http.StatusOK, createRec.Code)

	// Filter by resource-id using Filter.1.Name=resource-id; only vpc-default should appear.
	descRec := postForm(t, h,
		"Action=DescribeTags&Version=2016-11-15"+
			"&Filter.1.Name=resource-id&Filter.1.Value.1=vpc-default")
	assert.Equal(t, http.StatusOK, descRec.Code)
	assert.Contains(t, descRec.Body.String(), "vpc-default")
	assert.NotContains(t, descRec.Body.String(), "subnet-default")
}

func TestEC2Handler_DescribeTags_MultipleFilters(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createRec := postForm(t, h,
		"Action=CreateTags&Version=2016-11-15"+
			"&ResourceId.1=vpc-default"+
			"&Tag.1.Key=Env&Tag.1.Value=prod")
	require.Equal(t, http.StatusOK, createRec.Code)

	// Send a non-resource-id filter first, then resource-id filter.
	descRec := postForm(t, h,
		"Action=DescribeTags&Version=2016-11-15"+
			"&Filter.1.Name=key&Filter.1.Value.1=Env"+
			"&Filter.2.Name=resource-id&Filter.2.Value.1=vpc-default")
	assert.Equal(t, http.StatusOK, descRec.Code)
	assert.Contains(t, descRec.Body.String(), "vpc-default")
	assert.Contains(t, descRec.Body.String(), "prod")
}

func TestEC2Handler_ExtractResource_ResourceId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "ResourceId.1",
			body: "Action=CreateTags&Version=2016-11-15&ResourceId.1=vpc-abc123",
			want: "vpc-abc123",
		},
		{
			name: "InstanceId_still_works",
			body: "Action=DescribeInstances&Version=2016-11-15&InstanceId.1=i-abc123",
			want: "i-abc123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestEC2Handler_TagSpecification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		idTag        string // XML element name containing the created resource ID
		wantTagKey   string
		wantTagValue string
	}{
		{
			name: "create_vpc_with_tag_specification",
			body: "Action=CreateVpc&Version=2016-11-15" +
				"&CidrBlock=10.77.0.0/16" +
				"&TagSpecification.1.ResourceType=vpc" +
				"&TagSpecification.1.Tag.1.Key=Name" +
				"&TagSpecification.1.Tag.1.Value=my-tagged-vpc",
			idTag:        "vpcId",
			wantTagKey:   "Name",
			wantTagValue: "my-tagged-vpc",
		},
		{
			name: "create_subnet_with_tag_specification",
			body: "Action=CreateSubnet&Version=2016-11-15" +
				"&VpcId=vpc-default" +
				"&CidrBlock=172.31.32.0/24" +
				"&TagSpecification.1.ResourceType=subnet" +
				"&TagSpecification.1.Tag.1.Key=Name" +
				"&TagSpecification.1.Tag.1.Value=my-tagged-subnet",
			idTag:        "subnetId",
			wantTagKey:   "Name",
			wantTagValue: "my-tagged-subnet",
		},
		{
			name: "create_security_group_with_tag_specification",
			body: "Action=CreateSecurityGroup&Version=2016-11-15" +
				"&GroupName=tagged-sg" +
				"&GroupDescription=test" +
				"&VpcId=vpc-default" +
				"&TagSpecification.1.ResourceType=security-group" +
				"&TagSpecification.1.Tag.1.Key=Env" +
				"&TagSpecification.1.Tag.1.Value=staging",
			idTag:        "groupId",
			wantTagKey:   "Env",
			wantTagValue: "staging",
		},
		{
			name: "run_instances_with_tag_specification",
			body: "Action=RunInstances&Version=2016-11-15" +
				"&ImageId=ami-12345678" +
				"&InstanceType=t2.micro" +
				"&MinCount=1&MaxCount=1" +
				"&TagSpecification.1.ResourceType=instance" +
				"&TagSpecification.1.Tag.1.Key=Name" +
				"&TagSpecification.1.Tag.1.Value=my-instance",
			idTag:        "instanceId",
			wantTagKey:   "Name",
			wantTagValue: "my-instance",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			// Extract the resource ID from the creation response XML.
			resourceID := extractXMLValue(t, rec.Body.String(), tt.idTag)
			require.NotEmpty(
				t,
				resourceID,
				"resource ID element %q should be present in response",
				tt.idTag,
			)

			// DescribeTags with a resource-id filter to check only this specific resource.
			descRec := postForm(t, h,
				"Action=DescribeTags&Version=2016-11-15"+
					"&Filter.1.Name=resource-id&Filter.1.Value.1="+resourceID)
			require.Equal(t, http.StatusOK, descRec.Code)

			tagMap := extractTagsFromDescribeTagsXML(t, descRec.Body.String())
			assert.Equal(
				t,
				tt.wantTagValue,
				tagMap[tt.wantTagKey],
				"resource %s should have tag %s=%s (got tags: %v)",
				resourceID,
				tt.wantTagKey,
				tt.wantTagValue,
				tagMap,
			)
		})
	}
}

// extractXMLValue extracts the text content of the first occurrence of the given XML element.
// Returns the element text, or an empty string if the element is not found.
