package cloudtrail_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

// TestCloudTrailTags exercises AddTags, RemoveTags, and ListTags.
func TestCloudTrailTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "add_and_list_tags",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "my-trail",
					"S3BucketName": "my-bucket",
				})
				createResp := parseCloudTrailResp(t, createRec)
				trailARN := createResp["TrailARN"].(string)

				rec := doCloudTrailOp(t, h, "AddTags", map[string]any{
					"ResourceId": trailARN,
					"TagsList": []map[string]string{
						{"Key": "Env", "Value": "test"},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				listRec := doCloudTrailOp(t, h, "ListTags", map[string]any{
					"ResourceIdList": []string{trailARN},
				})
				assert.Equal(t, http.StatusOK, listRec.Code)
				listResp := parseCloudTrailResp(t, listRec)
				resourceTagList, ok := listResp["ResourceTagList"].([]any)
				require.True(t, ok)
				assert.Len(t, resourceTagList, 1)
				item := resourceTagList[0].(map[string]any)
				assert.Equal(t, trailARN, item["ResourceId"])
				tagsList := item["TagsList"].([]any)
				assert.Len(t, tagsList, 1)
				tag := tagsList[0].(map[string]any)
				assert.Equal(t, "Env", tag["Key"])
				assert.Equal(t, "test", tag["Value"])
			},
		},
		{
			name: "remove_tags",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateTrail", map[string]any{
					"Name":         "my-trail",
					"S3BucketName": "my-bucket",
				})
				createResp := parseCloudTrailResp(t, createRec)
				trailARN := createResp["TrailARN"].(string)

				doCloudTrailOp(t, h, "AddTags", map[string]any{
					"ResourceId": trailARN,
					"TagsList": []map[string]string{
						{"Key": "Env", "Value": "test"},
						{"Key": "Project", "Value": "foo"},
					},
				})
				rec := doCloudTrailOp(t, h, "RemoveTags", map[string]any{
					"ResourceId": trailARN,
					"TagsList": []map[string]string{
						{"Key": "Env"},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "add_tags_not_found",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				rec := doCloudTrailOp(t, h, "AddTags", map[string]any{
					"ResourceId": "arn:aws:cloudtrail:us-east-1:123456789012:trail/missing",
					"TagsList":   []map[string]string{},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}

// TestTagsOnAllResources exercises AddTags/RemoveTags/ListTags on channels, dashboards, event data stores.
func TestTagsOnAllResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, h *cloudtrail.Handler)
		name string
	}{
		{
			name: "tags_on_channel",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateChannel", map[string]any{
					"Name": "tagged-chan", "Source": "src",
				})
				createResp := parseCloudTrailResp(t, createRec)
				chanARN := createResp["ChannelArn"].(string)

				addRec := doCloudTrailOp(t, h, "AddTags", map[string]any{
					"ResourceId": chanARN,
					"TagsList":   []map[string]string{{"Key": "Env", "Value": "prod"}},
				})
				assert.Equal(t, http.StatusOK, addRec.Code)

				listRec := doCloudTrailOp(t, h, "ListTags", map[string]any{
					"ResourceIdList": []string{chanARN},
				})
				assert.Equal(t, http.StatusOK, listRec.Code)
				listResp := parseCloudTrailResp(t, listRec)
				tagList := listResp["ResourceTagList"].([]any)
				require.Len(t, tagList, 1)
				item := tagList[0].(map[string]any)
				assert.Equal(t, chanARN, item["ResourceId"])
				assert.NotEmpty(t, item["TagsList"])
			},
		},
		{
			name: "tags_on_dashboard",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateDashboard", map[string]any{
					"Name": "tagged-dash",
				})
				createResp := parseCloudTrailResp(t, createRec)
				dashARN := createResp["DashboardArn"].(string)

				addRec := doCloudTrailOp(t, h, "AddTags", map[string]any{
					"ResourceId": dashARN,
					"TagsList":   []map[string]string{{"Key": "Team", "Value": "platform"}},
				})
				assert.Equal(t, http.StatusOK, addRec.Code)

				removeRec := doCloudTrailOp(t, h, "RemoveTags", map[string]any{
					"ResourceId": dashARN,
					"TagsList":   []map[string]string{{"Key": "Team"}},
				})
				assert.Equal(t, http.StatusOK, removeRec.Code)
			},
		},
		{
			name: "tags_on_event_data_store",
			ops: func(t *testing.T, h *cloudtrail.Handler) {
				t.Helper()
				createRec := doCloudTrailOp(t, h, "CreateEventDataStore", map[string]any{
					"Name": "tagged-eds",
				})
				createResp := parseCloudTrailResp(t, createRec)
				edsARN := createResp["EventDataStoreArn"].(string)

				addRec := doCloudTrailOp(t, h, "AddTags", map[string]any{
					"ResourceId": edsARN,
					"TagsList":   []map[string]string{{"Key": "Cost", "Value": "team-a"}},
				})
				assert.Equal(t, http.StatusOK, addRec.Code)

				listRec := doCloudTrailOp(t, h, "ListTags", map[string]any{
					"ResourceIdList": []string{edsARN},
				})
				assert.Equal(t, http.StatusOK, listRec.Code)
				listResp := parseCloudTrailResp(t, listRec)
				tagList := listResp["ResourceTagList"].([]any)
				require.Len(t, tagList, 1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestCloudTrailHandler()
			tt.ops(t, h)
		})
	}
}
