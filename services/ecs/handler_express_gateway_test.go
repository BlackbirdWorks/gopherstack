package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

func TestECS_CreateExpressGatewayService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantName string
		wantCode int
	}{
		{
			name: "creates express gateway service",
			input: map[string]any{
				"executionRoleArn":      "arn:aws:iam::000000000000:role/exec-role",
				"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra-role",
				"serviceName":           "my-express-svc",
			},
			wantCode: http.StatusOK,
			wantName: "my-express-svc",
		},
		{
			name: "missing executionRoleArn returns 400",
			input: map[string]any{
				"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra-role",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing infrastructureRoleArn returns 400",
			input: map[string]any{
				"executionRoleArn": "arn:aws:iam::000000000000:role/exec-role",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "with tags",
			input: map[string]any{
				"executionRoleArn":      "arn:aws:iam::000000000000:role/exec-role",
				"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra-role",
				"serviceName":           "tagged-svc",
				"tags":                  []map[string]any{{"key": "env", "value": "prod"}},
			},
			wantCode: http.StatusOK,
			wantName: "tagged-svc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECSRequest(t, h, "CreateExpressGatewayService", tt.input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			svc, ok := resp["service"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantName, svc["serviceName"])
			assert.Equal(t, "ACTIVE", svc["status"])
			assert.NotEmpty(t, svc["serviceArn"])
		})
	}
}

func TestECS_CreateExpressGatewayService_DuplicateARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	input := map[string]any{
		"executionRoleArn":      "arn:aws:iam::000000000000:role/exec-role",
		"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra-role",
		"serviceName":           "my-svc",
	}

	doECSRequest(t, h, "CreateExpressGatewayService", input)
	rec := doECSRequest(t, h, "CreateExpressGatewayService", input)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "AlreadyExists")
}

func TestECS_DeleteExpressGatewayService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createFn func(h *ecs.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "delete existing service",
			createFn: func(h *ecs.Handler) string {
				rec := doECSRequest(t, h, "CreateExpressGatewayService", map[string]any{
					"executionRoleArn":      "arn:aws:iam::000000000000:role/exec",
					"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra",
					"serviceName":           "svc-to-delete",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var r map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r))

				return r["service"].(map[string]any)["serviceArn"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "delete non-existent returns 400",
			createFn: func(_ *ecs.Handler) string {
				return "arn:nonexistent"
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "empty serviceArn returns 400",
			createFn: func(_ *ecs.Handler) string {
				return ""
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			serviceArn := tt.createFn(h)

			rec := doECSRequest(
				t,
				h,
				"DeleteExpressGatewayService",
				map[string]any{"serviceArn": serviceArn},
			)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			svc, ok := resp["service"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, serviceArn, svc["serviceArn"])
		})
	}
}

func TestECS_DescribeExpressGatewayService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createFn func(h *ecs.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "describe existing service",
			createFn: func(h *ecs.Handler) string {
				rec := doECSRequest(t, h, "CreateExpressGatewayService", map[string]any{
					"executionRoleArn":      "arn:aws:iam::000000000000:role/exec",
					"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra",
					"serviceName":           "my-express",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var r map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r))

				return r["service"].(map[string]any)["serviceArn"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "describe non-existent returns 400",
			createFn: func(_ *ecs.Handler) string {
				return "arn:nonexistent"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			serviceArn := tt.createFn(h)

			rec := doECSRequest(
				t,
				h,
				"DescribeExpressGatewayService",
				map[string]any{"serviceArn": serviceArn},
			)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			svc, ok := resp["service"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, serviceArn, svc["serviceArn"])
			assert.Equal(t, "ACTIVE", svc["status"])
		})
	}
}

func TestECS_ExpressGatewayService_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doECSRequest(t, h, "CreateExpressGatewayService", map[string]any{
		"executionRoleArn":      "arn:aws:iam::000000000000:role/exec",
		"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra",
		"serviceName":           "rt-svc",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	arn := createResp["service"].(map[string]any)["serviceArn"].(string)

	// Describe
	rec = doECSRequest(t, h, "DescribeExpressGatewayService", map[string]any{"serviceArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doECSRequest(t, h, "DeleteExpressGatewayService", map[string]any{"serviceArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe after delete returns 400
	rec = doECSRequest(t, h, "DescribeExpressGatewayService", map[string]any{"serviceArn": arn})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestECS_UpdateExpressGatewayService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "updates service",
			wantCode: http.StatusOK,
		},
		{
			name:     "not found",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var serviceArn string

			if tt.wantCode == http.StatusOK {
				rec := doECSRequest(t, h, "CreateExpressGatewayService", map[string]any{
					"executionRoleArn":      "arn:aws:iam::000000000000:role/exec",
					"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra",
					"serviceName":           "my-gw-service",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				svc := createResp["service"].(map[string]any)
				serviceArn = svc["serviceArn"].(string)
			} else {
				serviceArn = "arn:aws:ecs:us-east-1:000000000000:service/nonexistent"
			}

			rec := doECSRequest(t, h, "UpdateExpressGatewayService", map[string]any{
				"serviceArn":            serviceArn,
				"executionRoleArn":      "arn:aws:iam::000000000000:role/new-exec",
				"infrastructureRoleArn": "arn:aws:iam::000000000000:role/new-infra",
			})
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			svc, ok := resp["service"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "arn:aws:iam::000000000000:role/new-exec", svc["executionRoleArn"])
			assert.Equal(
				t,
				"arn:aws:iam::000000000000:role/new-infra",
				svc["infrastructureRoleArn"],
			)
		})
	}
}

func TestExpressGatewayService_DeepCopy_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tagKey  string
		tagVal  string
		wantTag string
		mutate  bool
	}{
		{
			name:    "tags are independent after create",
			tagKey:  "team",
			tagVal:  "platform",
			mutate:  true,
			wantTag: "platform",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

			tags := []ecs.Tag{{Key: tt.tagKey, Value: tt.tagVal}}
			svc, err := b.CreateExpressGatewayService(ecs.CreateExpressGatewayServiceInput{
				ExecutionRoleArn:      "arn:aws:iam::000000000000:role/exec",
				InfrastructureRoleArn: "arn:aws:iam::000000000000:role/infra",
				ServiceName:           "gw-test",
				Tags:                  tags,
			})
			require.NoError(t, err)

			if tt.mutate && len(svc.Tags) > 0 {
				svc.Tags[0].Value = "mutated"
			}

			got, err := b.DescribeExpressGatewayService(svc.ServiceArn)
			require.NoError(t, err)
			require.Len(t, got.Tags, 1)
			assert.Equal(t, tt.wantTag, got.Tags[0].Value)
		})
	}
}

// TestExpressGatewayService_TagResource_VisibleOnDescribe proves that a
// TagResource call on an Express service ARN is reflected on a subsequent
// DescribeExpressGatewayService(include=[TAGS]) call. Previously
// svc.Tags (echoed on Create/Describe/Update) and the resourceTags side map
// (updated by TagResource/UntagResource, read by ListTagsForResource) were
// two independent, never-synchronized copies: TagResource "succeeded" but
// was invisible on Describe, and creation-time tags were invisible to
// ListTagsForResource.
func TestExpressGatewayService_TagResource_VisibleOnDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createResp := doECSRequest(t, h, "CreateExpressGatewayService", map[string]any{
		"executionRoleArn":      "arn:aws:iam::000000000000:role/exec-role",
		"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra-role",
		"serviceName":           "tag-sync-svc",
	})
	require.Equal(t, http.StatusOK, createResp.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createResp.Body.Bytes(), &createOut))
	serviceArn := createOut["service"].(map[string]any)["serviceArn"].(string)

	tagResp := doECSRequest(t, h, "TagResource", map[string]any{
		"resourceArn": serviceArn,
		"tags":        []map[string]any{{"key": "team", "value": "platform"}},
	})
	require.Equal(t, http.StatusOK, tagResp.Code)

	// ListTagsForResource sees it immediately.
	listResp := doECSRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": serviceArn,
	})
	require.Equal(t, http.StatusOK, listResp.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	listTags := listOut["tags"].([]any)
	require.Len(t, listTags, 1)
	assert.Equal(t, "team", listTags[0].(map[string]any)["key"])

	// DescribeExpressGatewayService with Include=[TAGS] also sees it.
	descResp := doECSRequest(t, h, "DescribeExpressGatewayService", map[string]any{
		"serviceArn": serviceArn,
		"include":    []string{"TAGS"},
	})
	require.Equal(t, http.StatusOK, descResp.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &descOut))
	descTags := descOut["service"].(map[string]any)["tags"].([]any)
	require.Len(t, descTags, 1)
	assert.Equal(t, "team", descTags[0].(map[string]any)["key"])

	// Without Include=[TAGS], tags are omitted.
	noTagsResp := doECSRequest(t, h, "DescribeExpressGatewayService", map[string]any{
		"serviceArn": serviceArn,
	})
	require.Equal(t, http.StatusOK, noTagsResp.Code)

	var noTagsOut map[string]any
	require.NoError(t, json.Unmarshal(noTagsResp.Body.Bytes(), &noTagsOut))
	assert.Nil(t, noTagsOut["service"].(map[string]any)["tags"])
}
