package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

// TestECS_RegisterTaskDefinition_EchoesTags verifies that RegisterTaskDefinitionOutput
// carries the supplied tags at the top level, unconditionally -- unlike
// DescribeTaskDefinition/ListTagsForResource, which gate tags behind the
// `include=TAGS` option (ecs@v1.90.0 deserializers.go:32428,
// awsAwsjson11_deserializeOpDocumentRegisterTaskDefinitionOutput). Drives the
// real aws-sdk-go-v2 client so a dropped or mis-keyed field decodes to a zero
// value instead of failing outright.
func TestECS_RegisterTaskDefinition_EchoesTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)

	out, err := client.RegisterTaskDefinition(t.Context(), &ecssdk.RegisterTaskDefinitionInput{
		Family: aws.String("tagged-fam"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("c"), Image: aws.String("nginx"), Essential: aws.Bool(true)},
		},
		Tags: []ecstypes.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.Tags, "RegisterTaskDefinitionOutput.Tags must echo the supplied tags")
	assert.Equal(t, "env", aws.ToString(out.Tags[0].Key))
	assert.Equal(t, "prod", aws.ToString(out.Tags[0].Value))

	// Cross-check against DescribeTaskDefinition(include=TAGS), the other
	// surface that reports the same registration tags.
	desc, err := client.DescribeTaskDefinition(t.Context(), &ecssdk.DescribeTaskDefinitionInput{
		TaskDefinition: out.TaskDefinition.TaskDefinitionArn,
		Include:        []ecstypes.TaskDefinitionField{"TAGS"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, desc.Tags)
	assert.Equal(t, aws.ToString(out.Tags[0].Key), aws.ToString(desc.Tags[0].Key))
	assert.Equal(t, aws.ToString(out.Tags[0].Value), aws.ToString(desc.Tags[0].Value))
}

func TestECS_RegisterTaskDefinition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
		wantRev  int
	}{
		{
			name: "success",
			input: map[string]any{
				"family": "nginx-task",
				"containerDefinitions": []map[string]any{
					{"name": "nginx", "image": "nginx:latest", "essential": true},
				},
			},
			wantCode: http.StatusOK,
			wantRev:  1,
		},
		{
			name:     "missing family",
			input:    map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECSRequest(t, h, "RegisterTaskDefinition", tt.input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				td, ok := resp["taskDefinition"].(map[string]any)
				require.True(t, ok)
				assert.NotEmpty(t, td["taskDefinitionArn"])
				assert.Equal(t, tt.wantRev, int(td["revision"].(float64)))
				assert.Equal(t, "ACTIVE", td["status"])
			}
		})
	}
}

func TestECS_RegisterTaskDefinition_MultipleRevisions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := 1; i <= 3; i++ {
		rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
			"family": "my-task",
			"containerDefinitions": []map[string]any{
				{"name": "app", "image": "app:latest"},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		td := resp["taskDefinition"].(map[string]any)
		assert.Equal(t, i, int(td["revision"].(float64)))
	}
}

func TestECS_DescribeTaskDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Register a task definition first.
	rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "web",
		"containerDefinitions": []map[string]any{
			{"name": "web", "image": "nginx:latest"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe by family name.
	rec2 := doECSRequest(t, h, "DescribeTaskDefinition", map[string]any{"taskDefinition": "web"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

	td := resp["taskDefinition"].(map[string]any)
	assert.Equal(t, "web", td["family"])

	// Not found.
	rec3 := doECSRequest(
		t,
		h,
		"DescribeTaskDefinition",
		map[string]any{"taskDefinition": "nonexistent"},
	)
	assert.Equal(t, http.StatusBadRequest, rec3.Code)
}

func TestECS_DeregisterTaskDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "temp-task",
		"containerDefinitions": []map[string]any{
			{"name": "app", "image": "busybox"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	tdArn := createResp["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

	rec2 := doECSRequest(t, h, "DeregisterTaskDefinition", map[string]any{"taskDefinition": tdArn})
	require.Equal(t, http.StatusOK, rec2.Code)

	var deregResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &deregResp))

	td := deregResp["taskDefinition"].(map[string]any)
	assert.Equal(t, "INACTIVE", td["status"])
}

func TestECS_ListTaskDefinitions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	families := []string{"app-a", "app-b", "other"}
	for _, f := range families {
		rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
			"family":               f,
			"containerDefinitions": []map[string]any{{"name": "c", "image": "busybox"}},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// List all.
	rec := doECSRequest(t, h, "ListTaskDefinitions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	arns := resp["taskDefinitionArns"].([]any)
	assert.GreaterOrEqual(t, len(arns), 3)

	// Filter by prefix.
	rec2 := doECSRequest(t, h, "ListTaskDefinitions", map[string]any{"familyPrefix": "app"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

	arns2 := resp2["taskDefinitionArns"].([]any)
	assert.Len(t, arns2, 2)
}

func TestECS_Backend_TaskDefinitionByRevision(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	_, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "rev-task",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "v1"}},
	})
	require.NoError(t, err)

	_, err = backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "rev-task",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "v2"}},
	})
	require.NoError(t, err)

	// Describe by family:revision shorthand.
	td, err := backend.DescribeTaskDefinition("rev-task:1")
	require.NoError(t, err)
	assert.Equal(t, 1, td.Revision)
	assert.Equal(t, "v1", td.ContainerDefinitions[0].Image)

	td2, err := backend.DescribeTaskDefinition("rev-task:2")
	require.NoError(t, err)
	assert.Equal(t, 2, td2.Revision)
	assert.Equal(t, "v2", td2.ContainerDefinitions[0].Image)
}

func TestECS_Handler_ListTaskDefinitions_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "ListTaskDefinitions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	arns, ok := resp["taskDefinitionArns"].([]any)
	require.True(t, ok)
	assert.Empty(t, arns)
}

func TestECS_Handler_DeregisterTaskDefinition_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "DeregisterTaskDefinition", map[string]any{
		"taskDefinition": "nonexistent-family",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestECS_Backend_DeregisterTaskDefinition_NotFoundByARN(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	_, err := backend.DeregisterTaskDefinition(
		"arn:aws:ecs:us-east-1:000000000000:task-definition/nonexistent:1",
	)
	require.Error(t, err)
}

func TestECS_ListTaskDefinitions_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create 5 task definitions.
	for _, family := range []string{"svc-a", "svc-b", "svc-c", "svc-d", "svc-e"} {
		rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
			"family":               family,
			"containerDefinitions": []map[string]any{{"name": "c", "image": "busybox"}},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		body          map[string]any
		name          string
		wantMinCount  int
		wantNextToken bool
	}{
		{
			name:         "all task definitions",
			body:         map[string]any{},
			wantMinCount: 5,
		},
		{
			name:          "paginated maxResults=2",
			body:          map[string]any{"maxResults": 2},
			wantMinCount:  2,
			wantNextToken: true,
		},
		{
			name:         "paginated maxResults=10 returns all",
			body:         map[string]any{"maxResults": 10},
			wantMinCount: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doECSRequest(t, h, "ListTaskDefinitions", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			arns := resp["taskDefinitionArns"].([]any)
			assert.GreaterOrEqual(t, len(arns), tt.wantMinCount)

			if tt.wantNextToken {
				assert.NotEmpty(t, resp["nextToken"])
			}
		})
	}
}

func TestECS_ListTaskDefinitions_TokenChaining(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, family := range []string{"page-a", "page-b", "page-c"} {
		rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
			"family":               family,
			"containerDefinitions": []map[string]any{{"name": "c", "image": "busybox"}},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// First page.
	rec := doECSRequest(t, h, "ListTaskDefinitions", map[string]any{"maxResults": 2})
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))

	arns1 := page1["taskDefinitionArns"].([]any)
	assert.Len(t, arns1, 2)

	nextToken, ok := page1["nextToken"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, nextToken)

	// Second page using the token.
	rec2 := doECSRequest(
		t,
		h,
		"ListTaskDefinitions",
		map[string]any{"maxResults": 2, "nextToken": nextToken},
	)
	require.Equal(t, http.StatusOK, rec2.Code)

	var page2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &page2))

	arns2 := page2["taskDefinitionArns"].([]any)
	assert.GreaterOrEqual(t, len(arns2), 1)

	// No duplicates.
	seen := make(map[string]bool)
	for _, a := range arns1 {
		seen[a.(string)] = true
	}

	for _, a := range arns2 {
		assert.False(t, seen[a.(string)], "duplicate task def ARN in page 2: %s", a)
	}
}

func TestECS_TaskDefinitionRevisionCap(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Register 105 revisions (beyond the cap of 100).
	for range 105 {
		rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
			"family": "capped-family",
			"containerDefinitions": []map[string]any{
				{"name": "app", "image": "app:latest"},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// The latest revision should be 105.
	rec := doECSRequest(
		t,
		h,
		"DescribeTaskDefinition",
		map[string]any{"taskDefinition": "capped-family"},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	td := resp["taskDefinition"].(map[string]any)
	latestRev := int(td["revision"].(float64))
	assert.Equal(t, 105, latestRev, "latest revision should be 105")

	// Listing all revisions should not exceed the cap.
	listRec := doECSRequest(
		t,
		h,
		"ListTaskDefinitions",
		map[string]any{"familyPrefix": "capped-family"},
	)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	arns := listResp["taskDefinitionArns"].([]any)
	assert.LessOrEqual(t, len(arns), 100, "stored revisions should not exceed the cap")
}

func TestTaskDefinition_MultipleRevisions_SameFamily(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Register 3 revisions of same family
	for range 3 {
		resp := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
			"family":               "rev-family",
			"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
		})
		require.Equal(t, http.StatusOK, resp.Code)
	}

	// List task definitions for this family
	listResp := doECSRequest(t, h, "ListTaskDefinitions", map[string]any{
		"familyPrefix": "rev-family",
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	arns := listOut["taskDefinitionArns"].([]any)
	assert.Len(t, arns, 3)

	// ListTaskDefinitionFamilies should return the family once
	famResp := doECSRequest(t, h, "ListTaskDefinitionFamilies", map[string]any{
		"familyPrefix": "rev-family",
	})
	require.Equal(t, http.StatusOK, famResp.Code)
	var famOut map[string]any
	require.NoError(t, json.Unmarshal(famResp.Body.Bytes(), &famOut))
	families := famOut["families"].([]any)
	assert.Len(t, families, 1)
	assert.Equal(t, "rev-family", families[0])
}

func TestTaskDefinition_Deregister_Specific_Revision(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for range 3 {
		doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
			"family":               "dereg-rev-family",
			"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
		})
	}

	// Describe revision 1 to get its ARN
	descResp := doECSRequest(t, h, "DescribeTaskDefinition", map[string]any{
		"taskDefinition": "dereg-rev-family:1",
	})
	require.Equal(t, http.StatusOK, descResp.Code)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &descOut))
	td := descOut["taskDefinition"].(map[string]any)
	rev1Arn := td["taskDefinitionArn"].(string)
	require.NotEmpty(t, rev1Arn)

	// Deregister revision 1
	deregResp := doECSRequest(t, h, "DeregisterTaskDefinition", map[string]any{
		"taskDefinition": rev1Arn,
	})
	require.Equal(t, http.StatusOK, deregResp.Code)
	var deregOut map[string]any
	require.NoError(t, json.Unmarshal(deregResp.Body.Bytes(), &deregOut))
	deregTd := deregOut["taskDefinition"].(map[string]any)
	assert.Equal(t, "INACTIVE", deregTd["status"])

	// Revisions 2 and 3 should still be active
	listResp := doECSRequest(t, h, "ListTaskDefinitions", map[string]any{
		"familyPrefix": "dereg-rev-family",
		"status":       "ACTIVE",
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	arns := listOut["taskDefinitionArns"].([]any)
	assert.Len(t, arns, 2)
}

func TestTaskDefinition_DeleteDefinitions_Batch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	arns := make([]string, 3)
	for i := range 3 {
		resp := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
			"family":               "batch-del-family",
			"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
		})
		var out map[string]any
		require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
		// Deregister first (required before delete)
		td := out["taskDefinition"].(map[string]any)
		arn := td["taskDefinitionArn"].(string)
		doECSRequest(t, h, "DeregisterTaskDefinition", map[string]any{"taskDefinition": arn})
		arns[i] = arn
	}

	deleteResp := doECSRequest(t, h, "DeleteTaskDefinitions", map[string]any{
		"taskDefinitions": arns,
	})
	require.Equal(t, http.StatusOK, deleteResp.Code)

	var deleteOut map[string]any
	require.NoError(t, json.Unmarshal(deleteResp.Body.Bytes(), &deleteOut))
	deleted := deleteOut["taskDefinitions"].([]any)
	assert.Len(t, deleted, 3)
}

func TestTaskDefinition_ListByStatus_Active_Inactive(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Register 2 revisions
	for range 2 {
		doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
			"family":               "status-filter-family",
			"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
		})
	}

	// Deregister revision 1
	doECSRequest(t, h, "DeregisterTaskDefinition", map[string]any{
		"taskDefinition": "status-filter-family:1",
	})

	// List ACTIVE
	activeResp := doECSRequest(t, h, "ListTaskDefinitions", map[string]any{
		"familyPrefix": "status-filter-family",
		"status":       "ACTIVE",
	})
	var activeOut map[string]any
	require.NoError(t, json.Unmarshal(activeResp.Body.Bytes(), &activeOut))
	assert.Len(t, activeOut["taskDefinitionArns"].([]any), 1)

	// List INACTIVE
	inactiveResp := doECSRequest(t, h, "ListTaskDefinitions", map[string]any{
		"familyPrefix": "status-filter-family",
		"status":       "INACTIVE",
	})
	var inactiveOut map[string]any
	require.NoError(t, json.Unmarshal(inactiveResp.Body.Bytes(), &inactiveOut))
	assert.Len(t, inactiveOut["taskDefinitionArns"].([]any), 1)
}

func TestTaskDefinitionFamilies_MultipleStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "fam-a",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "fam-b",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	// Deregister fam-b:1
	doECSRequest(t, h, "DeregisterTaskDefinition", map[string]any{"taskDefinition": "fam-b:1"})

	// List ACTIVE families
	resp := doECSRequest(t, h, "ListTaskDefinitionFamilies", map[string]any{
		"status": "ACTIVE",
	})
	require.Equal(t, http.StatusOK, resp.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	families := out["families"].([]any)
	famNames := make([]string, 0, len(families))
	for _, f := range families {
		famNames = append(famNames, f.(string))
	}
	assert.Contains(t, famNames, "fam-a")
	assert.NotContains(t, famNames, "fam-b")
}

func TestECS_DeleteTaskDefinitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input            map[string]any
		setupFn          func(h *ecs.Handler) string
		name             string
		wantDeletedCount int
		wantFailureCount int
		wantCode         int
	}{
		{
			name: "delete INACTIVE task definition succeeds",
			setupFn: func(h *ecs.Handler) string {
				rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
					"family":               "my-family",
					"containerDefinitions": []map[string]any{{"name": "c", "image": "nginx"}},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var r map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r))
				arn := r["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

				doECSRequest(
					t,
					h,
					"DeregisterTaskDefinition",
					map[string]any{"taskDefinition": arn},
				)

				return arn
			},
			wantCode:         http.StatusOK,
			wantDeletedCount: 1,
			wantFailureCount: 0,
		},
		{
			name: "delete ACTIVE task definition returns failure",
			setupFn: func(h *ecs.Handler) string {
				rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
					"family":               "active-family",
					"containerDefinitions": []map[string]any{{"name": "c", "image": "nginx"}},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var r map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r))

				return r["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)
			},
			wantCode:         http.StatusOK,
			wantDeletedCount: 0,
			wantFailureCount: 1,
		},
		{
			name:     "missing task definitions returns 400",
			input:    map[string]any{"taskDefinitions": []string{}},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "non-existent ARN becomes failure",
			setupFn: func(_ *ecs.Handler) string {
				return "nonexistent-arn"
			},
			wantCode:         http.StatusOK,
			wantDeletedCount: 0,
			wantFailureCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var input map[string]any

			if tt.setupFn != nil {
				arn := tt.setupFn(h)
				input = map[string]any{"taskDefinitions": []string{arn}}
			} else {
				input = tt.input
			}

			rec := doECSRequest(t, h, "DeleteTaskDefinitions", input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			deleted := resp["taskDefinitions"].([]any)
			failures := resp["failures"].([]any)

			assert.Len(t, deleted, tt.wantDeletedCount)
			assert.Len(t, failures, tt.wantFailureCount)
		})
	}
}

// TestHandler_ListTaskDefinitions_StatusFilter verifies the status filter:
// the default (ACTIVE) view hides a deregistered revision while status=INACTIVE
// surfaces it.
func TestHandler_ListTaskDefinitions_StatusFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		status       string
		wantContains string
	}{
		{
			name:         "active only excludes deregistered revision",
			status:       "",
			wantContains: "myapp:2",
		},
		{
			name:         "inactive lists deregistered revision",
			status:       "INACTIVE",
			wantContains: "myapp:1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Register two revisions.
			rec1 := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
				"family":               "myapp",
				"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx:1"}},
			})
			require.Equal(t, http.StatusOK, rec1.Code)
			var out1 map[string]any
			require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
			arn1 := out1["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

			rec2 := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
				"family":               "myapp",
				"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx:2"}},
			})
			require.Equal(t, http.StatusOK, rec2.Code)

			// Deregister revision 1.
			derecRec := doECSRequest(t, h, "DeregisterTaskDefinition", map[string]any{
				"taskDefinition": arn1,
			})
			require.Equal(t, http.StatusOK, derecRec.Code)

			listInput := map[string]any{"familyPrefix": "myapp"}
			if tt.status != "" {
				listInput["status"] = tt.status
			}

			listRec := doECSRequest(t, h, "ListTaskDefinitions", listInput)
			require.Equal(t, http.StatusOK, listRec.Code)

			var listOut map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
			arns := listOut["taskDefinitionArns"].([]any)
			require.Len(t, arns, 1)
			assert.Contains(t, arns[0].(string), tt.wantContains)
		})
	}
}

// TestECS_ListTaskDefinitionFamilies verifies ListTaskDefinitionFamilies.
func TestECS_ListTaskDefinitionFamilies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    map[string]any
		families []string
		wantLen  int
	}{
		{
			name:    "empty",
			input:   map[string]any{},
			wantLen: 0,
		},
		{
			name:     "all families",
			families: []string{"web", "worker"},
			input:    map[string]any{},
			wantLen:  2,
		},
		{
			name:     "prefix filter",
			families: []string{"web", "worker", "backend"},
			input:    map[string]any{"familyPrefix": "w"},
			wantLen:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, family := range tt.families {
				doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
					"family":               family,
					"containerDefinitions": []map[string]any{{"name": "c1", "image": "nginx"}},
				})
			}

			rec := doECSRequest(t, h, "ListTaskDefinitionFamilies", tt.input)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			families, ok := resp["families"].([]any)
			require.True(t, ok)
			assert.Len(t, families, tt.wantLen)
		})
	}
}
