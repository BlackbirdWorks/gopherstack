package dms_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dms"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoveTagsFromResource(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	createRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "ri-for-tags",
		"ReplicationInstanceClass":      "dms.t3.medium",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	riArn := parseJSON(t, createRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

	addRec := doDMS(t, h, "AddTagsToResource", map[string]any{
		"ResourceArn": riArn,
		"Tags": []map[string]any{
			{"Key": "k1", "Value": "v1"},
			{"Key": "k2", "Value": "v2"},
		},
	})
	require.Equal(t, http.StatusOK, addRec.Code)

	rec := doDMS(t, h, "RemoveTagsFromResource", map[string]any{
		"ResourceArn": riArn,
		"TagKeys":     []string{"k1"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	listRec := doDMS(t, h, "ListTagsForResource", map[string]any{"ResourceArn": riArn})
	require.Equal(t, http.StatusOK, listRec.Code)
	tagList := parseJSON(t, listRec)["TagList"].([]any)
	assert.Len(t, tagList, 1)
	assert.Equal(t, "k2", tagList[0].(map[string]any)["Key"])
}

func TestHandler_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "add_and_list_tags_on_replication_instance",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				create := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "tag-inst",
					"ReplicationInstanceClass":      "dms.t3.medium",
				})
				require.Equal(t, http.StatusOK, create.Code)
				createResp := parseJSON(t, create)
				arn := createResp["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

				addRec := doDMS(t, h, "AddTagsToResource", map[string]any{
					"ResourceArn": arn,
					"Tags": []map[string]string{
						{"Key": "Project", "Value": "MyProject"},
					},
				})
				assert.Equal(t, http.StatusOK, addRec.Code)

				listRec := doDMS(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": arn,
				})
				assert.Equal(t, http.StatusOK, listRec.Code)
				listResp := parseJSON(t, listRec)
				tags, ok := listResp["TagList"].([]any)
				require.True(t, ok)
				require.Len(t, tags, 1)
				tag := tags[0].(map[string]any)
				assert.Equal(t, "Project", tag["Key"])
				assert.Equal(t, "MyProject", tag["Value"])
			},
		},
		{
			name: "list_tags_not_found",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": "arn:aws:dms:us-east-1:123:rep:nonexistent",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestDMSHandler()
			tt.run(t, h)
		})
	}
}

func TestHandler_TagsOnEndpointAndTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "tags_on_endpoint",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				create := doDMS(t, h, "CreateEndpoint", map[string]any{
					"EndpointIdentifier": "tagged-ep",
					"EndpointType":       "source",
					"EngineName":         "mysql",
				})
				require.Equal(t, http.StatusOK, create.Code)
				createResp := parseJSON(t, create)
				arn := createResp["Endpoint"].(map[string]any)["EndpointArn"].(string)

				addRec := doDMS(t, h, "AddTagsToResource", map[string]any{
					"ResourceArn": arn,
					"Tags":        []map[string]string{{"Key": "Owner", "Value": "team"}},
				})
				assert.Equal(t, http.StatusOK, addRec.Code)

				listRec := doDMS(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": arn,
				})
				assert.Equal(t, http.StatusOK, listRec.Code)
				listResp := parseJSON(t, listRec)
				tags := listResp["TagList"].([]any)
				require.Len(t, tags, 1)
				assert.Equal(t, "Owner", tags[0].(map[string]any)["Key"])
			},
		},
		{
			name: "tags_on_task",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				instRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "tag-task-inst",
					"ReplicationInstanceClass":      "dms.t3.medium",
				})
				require.Equal(t, http.StatusOK, instRec.Code)
				instArn := parseJSON(t, instRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

				srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
					"EndpointIdentifier": "tag-task-src",
					"EndpointType":       "source",
					"EngineName":         "mysql",
				})
				require.Equal(t, http.StatusOK, srcRec.Code)
				srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

				dstRec := doDMS(t, h, "CreateEndpoint", map[string]any{
					"EndpointIdentifier": "tag-task-dst",
					"EndpointType":       "target",
					"EngineName":         "s3",
				})
				require.Equal(t, http.StatusOK, dstRec.Code)
				dstArn := parseJSON(t, dstRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

				taskRec := doDMS(t, h, "CreateReplicationTask", map[string]any{
					"ReplicationTaskIdentifier": "tagged-task",
					"SourceEndpointArn":         srcArn,
					"TargetEndpointArn":         dstArn,
					"ReplicationInstanceArn":    instArn,
					"MigrationType":             "full-load",
				})
				require.Equal(t, http.StatusOK, taskRec.Code)
				taskArn := parseJSON(t, taskRec)["ReplicationTask"].(map[string]any)["ReplicationTaskArn"].(string)

				addRec := doDMS(t, h, "AddTagsToResource", map[string]any{
					"ResourceArn": taskArn,
					"Tags":        []map[string]string{{"Key": "Stage", "Value": "prod"}},
				})
				assert.Equal(t, http.StatusOK, addRec.Code)

				listRec := doDMS(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": taskArn,
				})
				assert.Equal(t, http.StatusOK, listRec.Code)
				listResp := parseJSON(t, listRec)
				tags := listResp["TagList"].([]any)
				require.Len(t, tags, 1)
				assert.Equal(t, "Stage", tags[0].(map[string]any)["Key"])
			},
		},
		{
			name: "add_tags_not_found",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "AddTagsToResource", map[string]any{
					"ResourceArn": "arn:aws:dms:us-east-1:123:rep:nonexistent",
					"Tags":        []map[string]string{{"Key": "K", "Value": "V"}},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestDMSHandler()
			tt.run(t, h)
		})
	}
}

func TestHandler_TagsOnNewResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "tags_on_data_migration",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				create := doDMS(t, h, "CreateDataMigration", map[string]any{
					"DataMigrationName": "tag-dm",
					"DataMigrationType": "full-load",
					"Tags": []map[string]string{
						{"Key": "Phase", "Value": "alpha"},
					},
				})
				require.Equal(t, http.StatusOK, create.Code)
				dmArn := parseJSON(t, create)["DataMigration"].(map[string]any)["DataMigrationArn"].(string)

				listRec := doDMS(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": dmArn,
				})
				assert.Equal(t, http.StatusOK, listRec.Code)
				tags := parseJSON(t, listRec)["TagList"].([]any)
				require.Len(t, tags, 1)
				assert.Equal(t, "Phase", tags[0].(map[string]any)["Key"])
			},
		},
		{
			name: "tags_on_data_provider",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				create := doDMS(t, h, "CreateDataProvider", map[string]any{
					"DataProviderName": "tag-dp",
					"Engine":           "oracle",
					"Tags": []map[string]string{
						{"Key": "Owner", "Value": "dba"},
					},
				})
				require.Equal(t, http.StatusOK, create.Code)
				dpArn := parseJSON(t, create)["DataProvider"].(map[string]any)["DataProviderArn"].(string)

				listRec := doDMS(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": dpArn,
				})
				assert.Equal(t, http.StatusOK, listRec.Code)
				tags := parseJSON(t, listRec)["TagList"].([]any)
				require.Len(t, tags, 1)
				assert.Equal(t, "Owner", tags[0].(map[string]any)["Key"])
			},
		},
		{
			name: "tags_on_instance_profile",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				create := doDMS(t, h, "CreateInstanceProfile", map[string]any{
					"InstanceProfileName": "tag-ip",
					"Tags": []map[string]string{
						{"Key": "Tier", "Value": "prod"},
					},
				})
				require.Equal(t, http.StatusOK, create.Code)
				ipArn := parseJSON(t, create)["InstanceProfile"].(map[string]any)["InstanceProfileArn"].(string)

				addRec := doDMS(t, h, "AddTagsToResource", map[string]any{
					"ResourceArn": ipArn,
					"Tags": []map[string]string{
						{"Key": "Extra", "Value": "value"},
					},
				})
				assert.Equal(t, http.StatusOK, addRec.Code)

				listRec := doDMS(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": ipArn,
				})
				assert.Equal(t, http.StatusOK, listRec.Code)
				tags := parseJSON(t, listRec)["TagList"].([]any)
				assert.Len(t, tags, 2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestDMSHandler()
			tt.run(t, h)
		})
	}
}

func TestSortedListTags(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	create := doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "tagtest",
		"ReplicationInstanceClass":      "dms.t3.medium",
		"Tags": []map[string]any{
			{"Key": "zebra", "Value": "z"},
			{"Key": "alpha", "Value": "a"},
			{"Key": "middle", "Value": "m"},
		},
	})
	require.Equal(t, http.StatusOK, create.Code)
	arnStr := parseJSON(t, create)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

	listRec := doDMS(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": arnStr,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	tagList := parseJSON(t, listRec)["TagList"].([]any)
	require.Len(t, tagList, 3)
	assert.Equal(t, "alpha", tagList[0].(map[string]any)["Key"])
	assert.Equal(t, "middle", tagList[1].(map[string]any)["Key"])
	assert.Equal(t, "zebra", tagList[2].(map[string]any)["Key"])
}

func TestARNIndexedTagOps(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	createRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "arn-tag-inst",
		"ReplicationInstanceClass":      "dms.t3.medium",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	instARN := parseJSON(t, createRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

	addRec := doDMS(t, h, "AddTagsToResource", map[string]any{
		"ResourceArn": instARN,
		"Tags":        []map[string]any{{"Key": "team", "Value": "platform"}},
	})
	assert.Equal(t, http.StatusOK, addRec.Code)

	listRec := doDMS(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": instARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	tagList := parseJSON(t, listRec)["TagList"].([]any)
	require.Len(t, tagList, 1)
	assert.Equal(t, "team", tagList[0].(map[string]any)["Key"])
}

func TestHandler_TagsOnMigrationProject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "create_with_tags_and_list",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				createRec := doDMS(t, h, "CreateMigrationProject", map[string]any{
					"MigrationProjectName": "tagged-mp",
					"Tags": []map[string]string{
						{"Key": "env", "Value": "prod"},
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				mpArn := parseJSON(t, createRec)["MigrationProject"].(map[string]any)["MigrationProjectArn"].(string)

				listRec := doDMS(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": mpArn,
				})
				require.Equal(t, http.StatusOK, listRec.Code)
				tagList := parseJSON(t, listRec)["TagList"].([]any)
				require.Len(t, tagList, 1)
				assert.Equal(t, "env", tagList[0].(map[string]any)["Key"])
			},
		},
		{
			name: "add_tags_after_create",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				createRec := doDMS(t, h, "CreateMigrationProject", map[string]any{
					"MigrationProjectName": "add-tag-mp",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				mpArn := parseJSON(t, createRec)["MigrationProject"].(map[string]any)["MigrationProjectArn"].(string)

				addRec := doDMS(t, h, "AddTagsToResource", map[string]any{
					"ResourceArn": mpArn,
					"Tags":        []map[string]string{{"Key": "owner", "Value": "team"}},
				})
				assert.Equal(t, http.StatusOK, addRec.Code)

				listRec := doDMS(t, h, "ListTagsForResource", map[string]any{"ResourceArn": mpArn})
				require.Equal(t, http.StatusOK, listRec.Code)
				tagList := parseJSON(t, listRec)["TagList"].([]any)
				assert.Len(t, tagList, 1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestDMSHandler()
			tt.run(t, h)
		})
	}
}

func TestHandler_TagsOnReplicationSubnetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "create_with_tags_and_list",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				createRec := doDMS(t, h, "CreateReplicationSubnetGroup", map[string]any{
					"ReplicationSubnetGroupIdentifier":  "tagged-sg",
					"ReplicationSubnetGroupDescription": "test",
					"SubnetIds":                         []string{"subnet-1"},
					"Tags": []map[string]string{
						{"Key": "env", "Value": "test"},
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				// Real ReplicationSubnetGroup has no Arn field on the wire; build
				// it from the deterministic arn:aws:dms:<region>:<account>:subgrp:<id> format.
				sgArn := "arn:aws:dms:us-east-1:123456789012:subgrp:tagged-sg"

				listRec := doDMS(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": sgArn,
				})
				require.Equal(t, http.StatusOK, listRec.Code)
				tagList := parseJSON(t, listRec)["TagList"].([]any)
				require.Len(t, tagList, 1)
				assert.Equal(t, "env", tagList[0].(map[string]any)["Key"])
			},
		},
		{
			name: "add_and_remove_tags",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				createRec := doDMS(t, h, "CreateReplicationSubnetGroup", map[string]any{
					"ReplicationSubnetGroupIdentifier":  "tag-rm-sg",
					"ReplicationSubnetGroupDescription": "test",
					"SubnetIds":                         []string{"subnet-1"},
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				// Real ReplicationSubnetGroup has no Arn field on the wire; build
				// it from the deterministic arn:aws:dms:<region>:<account>:subgrp:<id> format.
				sgArn := "arn:aws:dms:us-east-1:123456789012:subgrp:tag-rm-sg"

				doDMS(t, h, "AddTagsToResource", map[string]any{
					"ResourceArn": sgArn,
					"Tags": []map[string]string{
						{"Key": "k1", "Value": "v1"},
						{"Key": "k2", "Value": "v2"},
					},
				})

				removeRec := doDMS(t, h, "RemoveTagsFromResource", map[string]any{
					"ResourceArn": sgArn,
					"TagKeys":     []string{"k1"},
				})
				assert.Equal(t, http.StatusOK, removeRec.Code)

				listRec := doDMS(t, h, "ListTagsForResource", map[string]any{"ResourceArn": sgArn})
				require.Equal(t, http.StatusOK, listRec.Code)
				tagList := parseJSON(t, listRec)["TagList"].([]any)
				assert.Len(t, tagList, 1)
				assert.Equal(t, "k2", tagList[0].(map[string]any)["Key"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestDMSHandler()
			tt.run(t, h)
		})
	}
}

func TestHandler_TagsOnReplicationConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "create_with_tags_and_list",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				createRec := doDMS(t, h, "CreateReplicationConfig", map[string]any{
					"ReplicationConfigIdentifier": "tagged-rc",
					"ReplicationType":             "full-load",
					"SourceEndpointArn":           "arn:src",
					"TargetEndpointArn":           "arn:tgt",
					"Tags": []map[string]string{
						{"Key": "tier", "Value": "prod"},
					},
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				rcArn := parseJSON(t, createRec)["ReplicationConfig"].(map[string]any)["ReplicationConfigArn"].(string)

				listRec := doDMS(t, h, "ListTagsForResource", map[string]any{
					"ResourceArn": rcArn,
				})
				require.Equal(t, http.StatusOK, listRec.Code)
				tagList := parseJSON(t, listRec)["TagList"].([]any)
				require.Len(t, tagList, 1)
				assert.Equal(t, "tier", tagList[0].(map[string]any)["Key"])
			},
		},
		{
			name: "add_tags_to_existing_config",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				createRec := doDMS(t, h, "CreateReplicationConfig", map[string]any{
					"ReplicationConfigIdentifier": "add-tag-rc",
					"ReplicationType":             "cdc",
					"SourceEndpointArn":           "arn:src",
					"TargetEndpointArn":           "arn:tgt",
				})
				require.Equal(t, http.StatusOK, createRec.Code)
				rcArn := parseJSON(t, createRec)["ReplicationConfig"].(map[string]any)["ReplicationConfigArn"].(string)

				addRec := doDMS(t, h, "AddTagsToResource", map[string]any{
					"ResourceArn": rcArn,
					"Tags":        []map[string]string{{"Key": "owner", "Value": "dba"}},
				})
				assert.Equal(t, http.StatusOK, addRec.Code)

				listRec := doDMS(t, h, "ListTagsForResource", map[string]any{"ResourceArn": rcArn})
				require.Equal(t, http.StatusOK, listRec.Code)
				tagList := parseJSON(t, listRec)["TagList"].([]any)
				assert.Len(t, tagList, 1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestDMSHandler()
			tt.run(t, h)
		})
	}
}
