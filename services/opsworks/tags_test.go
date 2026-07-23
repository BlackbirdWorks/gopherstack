package opsworks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opsworks"
)

// TestTags verifies tag operations on OpsWorks resources.
func TestTags(t *testing.T) {
	t.Parallel()

	createStack := func(h *opsworks.Handler) (string, string) {
		rec := doTarget(t, h, "CreateStack", map[string]any{
			"Name":                      "tagged-stack",
			"Region":                    "us-east-1",
			"DefaultInstanceProfileArn": "arn:aws:iam::000000000000:instance-profile/test",
			"ServiceRoleArn":            "arn:aws:iam::000000000000:role/test",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		stackID := parseJSON(t, rec.Body.Bytes())["StackId"].(string)
		arn := "arn:aws:opsworks:us-east-1:000000000000:stack/" + stackID

		return stackID, arn
	}

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler, arn string)
		name  string
	}{
		{
			name: "TagResource and ListTags round-trip",
			check: func(t *testing.T, h *opsworks.Handler, arn string) {
				t.Helper()
				rec := doTarget(t, h, "TagResource", map[string]any{
					"ResourceArn": arn,
					"Tags":        map[string]string{"env": "prod", "team": "ops"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "ListTags", map[string]any{"ResourceArn": arn})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				tags, ok := resp["Tags"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "prod", tags["env"])
				assert.Equal(t, "ops", tags["team"])
			},
		},
		{
			name: "UntagResource removes specified keys",
			check: func(t *testing.T, h *opsworks.Handler, arn string) {
				t.Helper()
				doTarget(t, h, "TagResource", map[string]any{
					"ResourceArn": arn,
					"Tags":        map[string]string{"env": "prod", "team": "ops"},
				})

				rec := doTarget(t, h, "UntagResource", map[string]any{
					"ResourceArn": arn,
					"TagKeys":     []string{"env"},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "ListTags", map[string]any{"ResourceArn": arn})
				resp := parseJSON(t, rec.Body.Bytes())
				tags := resp["Tags"].(map[string]any)
				assert.NotContains(t, tags, "env")
				assert.Contains(t, tags, "team")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, arn := createStack(h)
			tt.check(t, h, arn)
		})
	}
}

// TestTagResourceRejectsNonStackOrLayerARN verifies TagResource only
// accepts a stack or layer ARN, matching the real API's documented
// contract ("The stack or layer's Amazon Resource Number (ARN)" -- confirmed
// against aws-sdk-go-v2/service/opsworks@v1.31.0's api_op_TagResource.go,
// api_op_UntagResource.go, and api_op_ListTags.go doc comments). Instances
// and apps are not independently taggable resources on the real API.
func TestTagResourceRejectsNonStackOrLayerARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		buildResource func(t *testing.T, h *opsworks.Handler) string
		name          string
	}{
		{
			name: "layer ARN is accepted",
			buildResource: func(t *testing.T, h *opsworks.Handler) string {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)

				return "arn:aws:opsworks:us-east-1:000000000000:layer/" + layerID
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceArn := tt.buildResource(t, h)

			rec := doTarget(t, h, "TagResource", map[string]any{
				"ResourceArn": resourceArn,
				"Tags":        map[string]string{"env": "prod"},
			})
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}

	t.Run("instance ARN is rejected", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		stackID := createTestStack(t, h)
		layerID := createTestLayer(t, h, stackID)
		instanceID := createTestInstance(t, h, stackID, layerID)

		rec := doTarget(t, h, "TagResource", map[string]any{
			"ResourceArn": "arn:aws:opsworks:us-east-1:000000000000:instance/" + instanceID,
			"Tags":        map[string]string{"env": "prod"},
		})
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("app ARN is rejected", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		stackID := createTestStack(t, h)
		rec := doTarget(t, h, "CreateApp", map[string]any{
			"StackId": stackID, "Name": "app", "Type": "other",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		appID := parseJSON(t, rec.Body.Bytes())["AppId"].(string)

		rec = doTarget(t, h, "TagResource", map[string]any{
			"ResourceArn": "arn:aws:opsworks:us-east-1:000000000000:app/" + appID,
			"Tags":        map[string]string{"env": "prod"},
		})
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

// TestListTagsPagination verifies ListTags respects MaxResults/NextToken.
func TestListTagsPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "MaxResults limits returned tags",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				stackArn := "arn:aws:opsworks:us-east-1:000000000000:stack/" + stackID

				// Add 5 tags.
				doTarget(t, h, "TagResource", map[string]any{
					"ResourceArn": stackArn,
					"Tags": map[string]string{
						"a": "1", "b": "2", "c": "3", "d": "4", "e": "5",
					},
				})

				// Request only 2.
				rec := doTarget(t, h, "ListTags", map[string]any{
					"ResourceArn": stackArn,
					"MaxResults":  2,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				tags := resp["Tags"].(map[string]any)
				assert.Len(t, tags, 2)
				assert.NotEmpty(t, resp["NextToken"])
			},
		},
		{
			name: "NextToken continues pagination",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				stackArn := "arn:aws:opsworks:us-east-1:000000000000:stack/" + stackID

				doTarget(t, h, "TagResource", map[string]any{
					"ResourceArn": stackArn,
					"Tags": map[string]string{
						"a": "1", "b": "2", "c": "3",
					},
				})

				rec := doTarget(t, h, "ListTags", map[string]any{
					"ResourceArn": stackArn,
					"MaxResults":  2,
				})
				resp := parseJSON(t, rec.Body.Bytes())
				nextToken := resp["NextToken"].(string)

				rec = doTarget(t, h, "ListTags", map[string]any{
					"ResourceArn": stackArn,
					"NextToken":   nextToken,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp2 := parseJSON(t, rec.Body.Bytes())
				tags2 := resp2["Tags"].(map[string]any)
				assert.Len(t, tags2, 1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.check(t, h)
		})
	}
}
