package ecr_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	realismManifest = `{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json",` +
		`"config":{"mediaType":"x","size":1,"digest":"sha256:aa"},"layers":[]}`
	realismLifecycle = `{"rules":[{"rulePriority":1,"action":{"type":"expire"},` +
		`"selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":5}}]}`
)

func TestECRWire_InputValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		action   string
		wantType string
		wantCode int
	}{
		{
			name: "repo uppercase", action: "CreateRepository",
			body:     map[string]any{"repositoryName": "Bad Name"},
			wantCode: http.StatusBadRequest, wantType: "InvalidParameterException",
		},
		{
			name: "repo trailing slash", action: "CreateRepository",
			body:     map[string]any{"repositoryName": "ns/"},
			wantCode: http.StatusBadRequest, wantType: "InvalidParameterException",
		},
		{
			name: "repo namespaced ok", action: "CreateRepository",
			body: map[string]any{"repositoryName": "ns/sub.repo-1_x"}, wantCode: http.StatusOK,
		},
		{
			name: "manifest not json", action: "PutImage",
			body:     map[string]any{"repositoryName": "rr", "imageManifest": "notjson", "imageTag": "v1"},
			wantCode: http.StatusBadRequest, wantType: "InvalidParameterException",
		},
		{
			name: "manifest ok", action: "PutImage",
			body:     map[string]any{"repositoryName": "rr", "imageManifest": realismManifest, "imageTag": "v1"},
			wantCode: http.StatusOK,
		},
		{
			name: "policy not json", action: "SetRepositoryPolicy",
			body:     map[string]any{"repositoryName": "rr", "policyText": "{bad"},
			wantCode: http.StatusBadRequest, wantType: "InvalidParameterException",
		},
		{
			name: "lifecycle bad rule", action: "PutLifecyclePolicy",
			body: map[string]any{
				"repositoryName":      "rr",
				"lifecyclePolicyText": `{"rules":[{"rulePriority":1}]}`,
			},
			wantCode: http.StatusBadRequest, wantType: "InvalidParameterException",
		},
		{
			name: "lifecycle ok", action: "PutLifecyclePolicy",
			body:     map[string]any{"repositoryName": "rr", "lifecyclePolicyText": realismLifecycle},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyHandler()
			mustCreateRepo(t, h, "rr")

			rec := doAccuracy(t, h, tt.action, tt.body)
			require.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

			if tt.wantType == "" {
				return
			}

			var out map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantType, out["__type"])
			assert.False(t, strings.HasPrefix(out["message"], tt.wantType), out["message"])
		})
	}
}

func TestECRWire_TagLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		count    int
		wantCode int
	}{
		{name: "at limit", count: 50, wantCode: http.StatusOK},
		{name: "over limit", count: 51, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyHandler()
			mustCreateRepo(t, h, "rr")

			tags := make([]map[string]string, 0, tt.count)
			for i := range tt.count {
				tags = append(tags, map[string]string{"Key": fmt.Sprintf("k%d", i), "Value": "v"})
			}

			rec := doAccuracy(t, h, "TagResource", map[string]any{
				"resourceArn": "arn:aws:ecr:us-east-1:000000000000:repository/rr", "tags": tags,
			})
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
		})
	}
}
