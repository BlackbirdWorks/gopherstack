package emr_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── DescribeCluster: Tags [] not absent ──────────────────────────────────────

func TestAudit2_DescribeCluster_TagsEmptyNotAbsent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "no tags at creation returns Tags:[]",
			body: map[string]any{"Name": "notag-cluster"},
		},
		{
			name: "empty tags slice at creation returns Tags:[]",
			body: map[string]any{"Name": "emptytag-cluster", "Tags": []any{}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doEMRRequest(t, h, "RunJobFlow", tt.body)
			require.Equal(t, http.StatusOK, createRec.Code)

			var create struct {
				JobFlowID string `json:"JobFlowId"`
			}
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

			rec := doEMRRequest(t, h, "DescribeCluster", map[string]any{"ClusterId": create.JobFlowID})
			require.Equal(t, http.StatusOK, rec.Code)

			var raw map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

			cluster, ok := raw["Cluster"].(map[string]any)
			require.True(t, ok, "Cluster must be an object")

			tags, hasKey := cluster["Tags"]
			assert.True(t, hasKey, "DescribeCluster must include 'Tags' key even when empty")
			assert.IsType(t, []any{}, tags, "'Tags' must be an array, not null or absent")
			assert.Empty(t, tags, "'Tags' must be [] not populated")
		})
	}
}

func TestAudit2_DescribeCluster_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name": "tagged-cluster",
		"Tags": []map[string]any{{"Key": "env", "Value": "test"}},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	rec := doEMRRequest(t, h, "DescribeCluster", map[string]any{"ClusterId": create.JobFlowID})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	cluster := raw["Cluster"].(map[string]any)
	tags := cluster["Tags"].([]any)
	require.Len(t, tags, 1)
	tag := tags[0].(map[string]any)
	assert.Equal(t, "env", tag["Key"])
	assert.Equal(t, "test", tag["Value"])
}

// ── DescribeCluster: Tags [] after RemoveTags empties all tags ────────────────

func TestAudit2_DescribeCluster_TagsEmptyAfterRemove(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name": "rmtag-cluster",
		"Tags": []map[string]any{{"Key": "k", "Value": "v"}},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	rmRec := doEMRRequest(t, h, "RemoveTags", map[string]any{
		"ResourceId": create.JobFlowID,
		"TagKeys":    []string{"k"},
	})
	require.Equal(t, http.StatusOK, rmRec.Code)

	rec := doEMRRequest(t, h, "DescribeCluster", map[string]any{"ClusterId": create.JobFlowID})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	cluster := raw["Cluster"].(map[string]any)
	tags, hasKey := cluster["Tags"]
	assert.True(t, hasKey, "Tags key must be present after RemoveTags empties all tags")
	assert.IsType(t, []any{}, tags, "Tags must be [] not null after all tags removed")
	assert.Empty(t, tags)
}

// ── DescribeNotebookExecution: Tags [] not absent ────────────────────────────

func TestAudit2_NotebookExecution_TagsEmptyNotAbsent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "no tags returns Tags:[]",
			body: map[string]any{
				"EditorId":              "e-ED1",
				"NotebookExecutionName": "no-tag-run",
				"ExecutionEngineConfig": map[string]any{"Id": "j-1"},
			},
		},
		{
			name: "empty tags returns Tags:[]",
			body: map[string]any{
				"EditorId":              "e-ED2",
				"NotebookExecutionName": "empty-tag-run",
				"ExecutionEngineConfig": map[string]any{"Id": "j-2"},
				"Tags":                  []any{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			startRec := doEMRRequest(t, h, "StartNotebookExecution", tt.body)
			require.Equal(t, http.StatusOK, startRec.Code)

			var start struct {
				NotebookExecutionID string `json:"NotebookExecutionId"`
			}
			require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &start))

			rec := doEMRRequest(t, h, "DescribeNotebookExecution", map[string]any{
				"NotebookExecutionId": start.NotebookExecutionID,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var raw map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

			ne, ok := raw["NotebookExecution"].(map[string]any)
			require.True(t, ok, "NotebookExecution must be an object")

			tags, hasKey := ne["Tags"]
			assert.True(t, hasKey, "DescribeNotebookExecution must include 'Tags' key even when empty")
			assert.IsType(t, []any{}, tags, "'Tags' must be an array, not null or absent")
			assert.Empty(t, tags, "'Tags' must be [] not populated")
		})
	}
}

// ── DescribeStudio: Tags [] and SubnetIds [] not absent ──────────────────────

func TestAudit2_DescribeStudio_TagsEmptyNotAbsent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "CreateStudio", map[string]any{
		"Name":                     "notag-studio",
		"AuthMode":                 "IAM",
		"DefaultS3Location":        "s3://bucket/prefix",
		"EngineSecurityGroupId":    "sg-engine",
		"ServiceRole":              "arn:aws:iam::000000000000:role/svc",
		"VpcId":                    "vpc-1",
		"WorkspaceSecurityGroupId": "sg-ws",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		StudioID string `json:"StudioId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	rec := doEMRRequest(t, h, "DescribeStudio", map[string]any{"StudioId": create.StudioID})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	studio, ok := raw["Studio"].(map[string]any)
	require.True(t, ok, "Studio must be an object")

	tags, hasTagsKey := studio["Tags"]
	assert.True(t, hasTagsKey, "DescribeStudio must include 'Tags' key even when empty")
	assert.IsType(t, []any{}, tags, "'Tags' must be an array, not null or absent")
	assert.Empty(t, tags)
}

func TestAudit2_DescribeStudio_SubnetIdsEmptyNotAbsent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "CreateStudio", map[string]any{
		"Name":                     "nosubnet-studio",
		"AuthMode":                 "IAM",
		"DefaultS3Location":        "s3://bucket/prefix",
		"EngineSecurityGroupId":    "sg-engine",
		"ServiceRole":              "arn:aws:iam::000000000000:role/svc",
		"VpcId":                    "vpc-1",
		"WorkspaceSecurityGroupId": "sg-ws",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		StudioID string `json:"StudioId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	rec := doEMRRequest(t, h, "DescribeStudio", map[string]any{"StudioId": create.StudioID})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	studio := raw["Studio"].(map[string]any)

	subnets, hasSubnetsKey := studio["SubnetIds"]
	assert.True(t, hasSubnetsKey, "DescribeStudio must include 'SubnetIds' key even when empty")
	assert.IsType(t, []any{}, subnets, "'SubnetIds' must be an array, not null or absent")
	assert.Empty(t, subnets)
}

// ── ListStudioSessionMappings: [] not null when no mappings ─────────────────

func TestAudit2_ListStudioSessionMappings_EmptyNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "CreateStudio", map[string]any{
		"Name":                     "mapping-studio",
		"AuthMode":                 "IAM",
		"DefaultS3Location":        "s3://bucket/prefix",
		"EngineSecurityGroupId":    "sg-engine",
		"ServiceRole":              "arn:aws:iam::000000000000:role/svc",
		"VpcId":                    "vpc-1",
		"WorkspaceSecurityGroupId": "sg-ws",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		StudioID string `json:"StudioId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	rec := doEMRRequest(t, h, "ListStudioSessionMappings", map[string]any{
		"StudioId": create.StudioID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	mappings, hasKey := raw["SessionMappings"]
	assert.True(t, hasKey, "ListStudioSessionMappings must include 'SessionMappings' key")
	assert.IsType(t, []any{}, mappings, "'SessionMappings' must be [] not null when empty")
	assert.Empty(t, mappings)
}

// ── DescribeJobFlows: [] not null when no clusters match ────────────────────

func TestAudit2_DescribeJobFlows_EmptyNotNull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "empty backend returns JobFlows:[]",
			body: map[string]any{},
		},
		{
			name: "filter by nonexistent IDs returns JobFlows:[]",
			body: map[string]any{"JobFlowIds": []string{"j-DOESNOTEXIST"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doEMRRequest(t, h, "DescribeJobFlows", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var raw map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

			flows, hasKey := raw["JobFlows"]
			assert.True(t, hasKey, "DescribeJobFlows must include 'JobFlows' key")
			assert.IsType(t, []any{}, flows, "'JobFlows' must be [] not null when empty")
			assert.Empty(t, flows)
		})
	}
}
