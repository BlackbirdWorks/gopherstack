package ecr_test

// pull_through_cache_test.go — verifies pull_through_cache.go: Create/Describe
// /Update/Delete/Validate PullThroughCacheRule, upstream-registry variants,
// prefix filtering, and pagination.

import (
	"context"
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

func TestPullThroughCacheRule_UpstreamRegistryVariants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		prefix           string
		upstreamURL      string
		upstreamRegistry string
	}{
		{
			name: "ecr_public", prefix: "ecr-public",
			upstreamURL: "https://public.ecr.aws", upstreamRegistry: "ECR_PUBLIC",
		},
		{
			name: "quay", prefix: "quay",
			upstreamURL: "https://quay.io", upstreamRegistry: "QUAY",
		},
		{
			name: "docker_hub", prefix: "dockerhub",
			upstreamURL: "https://registry-1.docker.io", upstreamRegistry: "DOCKER_HUB",
		},
		{
			name: "k8s", prefix: "k8s",
			upstreamURL: "https://registry.k8s.io", upstreamRegistry: "K8S",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyHandler()

			rec := doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
				"ecrRepositoryPrefix": tt.prefix,
				"upstreamRegistryUrl": tt.upstreamURL,
				"upstreamRegistry":    tt.upstreamRegistry,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			out := parseAccuracy(t, rec)
			assert.Equal(t, tt.prefix, out["ecrRepositoryPrefix"])
			assert.Equal(t, tt.upstreamRegistry, out["upstreamRegistry"])
		})
	}
}

func TestPullThroughCacheRule_UpstreamRepositoryPrefix(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	rec := doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix":      "quay-prefix",
		"upstreamRegistryUrl":      "https://quay.io",
		"upstreamRegistry":         "QUAY",
		"upstreamRepositoryPrefix": "myorg",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "myorg", out["upstreamRepositoryPrefix"])
}

func TestPullThroughCacheRule_WithCredentials(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	credArn := "arn:aws:secretsmanager:us-east-1:123456789012:secret/ecr-cred"

	rec := doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "private-hub",
		"upstreamRegistryUrl": "https://my-registry.example.com",
		"credentialArn":       credArn,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, credArn, out["credentialArn"])
}

func TestPullThroughCacheRule_Update_Credential(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "update-cred",
		"upstreamRegistryUrl": "https://my-registry.example.com",
	})

	newCred := "arn:aws:secretsmanager:us-east-1:123456789012:secret/new-cred"
	updateRec := doAccuracy(t, h, "UpdatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "update-cred",
		"credentialArn":       newCred,
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	out := parseAccuracy(t, updateRec)
	assert.Equal(t, newCred, out["credentialArn"])
}

func TestPullThroughCacheRule_Validate_Missing_Returns_Invalid(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	rec := doAccuracy(t, h, "ValidatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "nonexistent",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, false, out["isValid"],
		"missing rule must validate as invalid")
	assert.NotEmpty(t, out["failure"], "failure message must be set for missing rule")
}

func TestPullThroughCacheRule_Validate_Existing_Returns_Valid(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "valid-rule",
		"upstreamRegistryUrl": "https://registry-1.docker.io",
	})

	rec := doAccuracy(t, h, "ValidatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "valid-rule",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, true, out["isValid"])
}

func TestPullThroughCacheRule_DescribeByPrefix(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	for _, prefix := range []string{"r1", "r2", "r3"} {
		doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
			"ecrRepositoryPrefix": prefix,
			"upstreamRegistryUrl": "https://registry-1.docker.io",
		})
	}

	rec := doAccuracy(t, h, "DescribePullThroughCacheRules", map[string]any{
		"ecrRepositoryPrefixes": []string{"r1", "r3"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	rules, _ := out["pullThroughCacheRules"].([]any)
	require.Len(t, rules, 2, "filtering by prefix must return only requested rules")
}

func TestPullThroughCacheRule_Duplicate_Returns400(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "dup-rule",
		"upstreamRegistryUrl": "https://registry-1.docker.io",
	})

	rec := doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "dup-rule",
		"upstreamRegistryUrl": "https://registry-1.docker.io",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"duplicate rule must return 400")
	out := parseAccuracy(t, rec)
	assert.Equal(t, "PullThroughCacheRuleAlreadyExistsException", out["__type"])
}

func TestDeletePullThroughCacheRule_NotFound_Returns404(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "DeletePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "ghost-prefix",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestDescribePullThroughCacheRules_NotFound_Returns404(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "DescribePullThroughCacheRules", map[string]any{
		"ecrRepositoryPrefixes": []string{"ghost"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestDescribePullThroughCacheRules_FilterByPrefix(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	for _, prefix := range []string{"ecr-pub", "quay", "docker-hub"} {
		doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
			"ecrRepositoryPrefix": prefix,
			"upstreamRegistryUrl": "https://" + prefix + ".example.com",
		})
	}

	rec := doAccuracy(t, h, "DescribePullThroughCacheRules", map[string]any{
		"ecrRepositoryPrefixes": []string{"quay"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	rules, _ := out["pullThroughCacheRules"].([]any)
	require.Len(t, rules, 1, "filter by prefix must return only the matching rule")
	rule := rules[0].(map[string]any)
	assert.Equal(t, "quay", rule["ecrRepositoryPrefix"])
}

func TestDescribePullThroughCacheRules_MissingPrefix_Errors(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	rec := doAccuracy(t, h, "DescribePullThroughCacheRules", map[string]any{
		"ecrRepositoryPrefixes": []string{"does-not-exist"},
	})
	assert.NotEqual(t, http.StatusOK, rec.Code,
		"filter for non-existent prefix must return an error")
}

func TestDescribePullThroughCacheRules_NoFilter_ReturnsAll(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	for _, prefix := range []string{"prefix-a", "prefix-b"} {
		doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
			"ecrRepositoryPrefix": prefix,
			"upstreamRegistryUrl": "https://example.com/" + prefix,
		})
	}

	rec := doAccuracy(t, h, "DescribePullThroughCacheRules", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	rules, _ := out["pullThroughCacheRules"].([]any)
	assert.Len(t, rules, 2, "no filter must return all rules")
}

func TestUpdatePullThroughCacheRule_CredentialUpdated(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "update-cred-2",
		"upstreamRegistryUrl": "https://registry.example.com",
	})

	newCred := "arn:aws:secretsmanager:us-east-1:123456789012:secret/ecr-cred"
	rec := doAccuracy(t, h, "UpdatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "update-cred-2",
		"credentialArn":       newCred,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, newCred, out["credentialArn"], "credentialArn must be updated")
	assert.Equal(t, "update-cred-2", out["ecrRepositoryPrefix"], "prefix must be preserved")
	assert.Equal(t, "https://registry.example.com", out["upstreamRegistryUrl"],
		"upstreamRegistryUrl must not be changed by update")
}

func TestUpdatePullThroughCacheRule_CustomRoleUpdated(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "update-role",
		"upstreamRegistryUrl": "https://registry2.example.com",
	})

	newRole := "arn:aws:iam::123456789012:role/ecr-pull-role"
	rec := doAccuracy(t, h, "UpdatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "update-role",
		"customRoleArn":       newRole,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, newRole, out["customRoleArn"])
}

func TestUpdatePullThroughCacheRule_NotFound_Errors(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	rec := doAccuracy(t, h, "UpdatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "no-such-prefix",
		"credentialArn":       "arn:aws:secretsmanager:us-east-1:123456789012:secret/x",
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

func TestValidatePullThroughCacheRule_ExistingRule_IsValid(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "validate-me",
		"upstreamRegistryUrl": "https://registry.example.com",
	})

	rec := doAccuracy(t, h, "ValidatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "validate-me",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, true, out["isValid"])
	assert.Equal(t, "validate-me", out["ecrRepositoryPrefix"])
	assert.Empty(t, out["failure"])
}

func TestValidatePullThroughCacheRule_MissingRule_NotValid(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	rec := doAccuracy(t, h, "ValidatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "nonexistent-validate",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, false, out["isValid"])
	assert.NotEmpty(t, out["failure"], "failure message must be present when rule is not found")
}

func TestPullThroughCacheRule_Create_Describe_Delete(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()

	createRec := doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "dockerhub-accuracy",
		"upstreamRegistryUrl": "https://registry-1.docker.io",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	createOut := parseAccuracy(t, createRec)
	assert.Equal(t, "dockerhub-accuracy", createOut["ecrRepositoryPrefix"])
	assert.Greater(t, createOut["createdAt"].(float64), float64(0))

	descRec := doAccuracy(t, h, "DescribePullThroughCacheRules", map[string]any{})
	require.Equal(t, http.StatusOK, descRec.Code)
	descOut := parseAccuracy(t, descRec)
	rules, _ := descOut["pullThroughCacheRules"].([]any)
	assert.Len(t, rules, 1)

	delRec := doAccuracy(t, h, "DeletePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "dockerhub-accuracy",
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	descRec2 := doAccuracy(t, h, "DescribePullThroughCacheRules", map[string]any{})
	descOut2 := parseAccuracy(t, descRec2)
	rules2, _ := descOut2["pullThroughCacheRules"].([]any)
	assert.Empty(t, rules2, "rule must be deleted")
}

func TestDescribePullThroughCacheRules_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		prefixes   []string
		maxResults int
		wantNext   bool
	}{
		{
			name:       "all_results_fit_no_token",
			prefixes:   []string{"alpha/", "beta/"},
			maxResults: 10,
			wantNext:   false,
		},
		{
			name:       "token_emitted_and_round_trips",
			prefixes:   []string{"alpha/", "beta/", "gamma/"},
			maxResults: 2,
			wantNext:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newHandlerWithBackend()
			ctx := context.Background()

			for _, prefix := range tt.prefixes {
				_, err := b.CreatePullThroughCacheRule(
					ctx,
					prefix,
					"public.ecr.aws",
					"",
					"",
					"",
					"",
				)
				require.NoError(t, err)
			}

			rec := doAccuracy(t, h, "DescribePullThroughCacheRules", map[string]any{
				"maxResults": tt.maxResults,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			out := parseAccuracy(t, rec)
			nextToken, _ := out["nextToken"].(string)

			if !tt.wantNext {
				assert.Empty(t, nextToken, "no nextToken when all rules fit")

				return
			}

			require.NotEmpty(t, nextToken, "nextToken must be emitted when truncated")

			decoded, err := base64.StdEncoding.DecodeString(nextToken)
			require.NoError(t, err, "nextToken must be valid base64")

			cursorPrefix := string(decoded)
			assert.Contains(t, tt.prefixes, cursorPrefix)

			// Round-trip.
			rec2 := doAccuracy(t, h, "DescribePullThroughCacheRules", map[string]any{
				"maxResults": tt.maxResults,
				"nextToken":  nextToken,
			})
			require.Equal(t, http.StatusOK, rec2.Code)

			out2 := parseAccuracy(t, rec2)
			rules1, _ := out["pullThroughCacheRules"].([]any)
			rules2, _ := out2["pullThroughCacheRules"].([]any)
			assert.Equal(t, len(tt.prefixes), len(rules1)+len(rules2))
		})
	}
}

func TestECR_CreatePullThroughCacheRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		ecrRepositoryPrefix string
		upstreamRegistryURL string
		wantPrefix          string
		wantStatus          int
	}{
		{
			name:                "creates rule successfully",
			ecrRepositoryPrefix: "docker-hub",
			upstreamRegistryURL: "registry-1.docker.io",
			wantStatus:          http.StatusOK,
			wantPrefix:          "docker-hub",
		},
		{
			name:                "empty prefix returns error",
			ecrRepositoryPrefix: "",
			upstreamRegistryURL: "registry-1.docker.io",
			wantStatus:          http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doECRRequest(t, h, "CreatePullThroughCacheRule", map[string]any{
				"ecrRepositoryPrefix": tt.ecrRepositoryPrefix,
				"upstreamRegistryUrl": tt.upstreamRegistryURL,
			})
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				out := parseAccuracy(t, rec)
				assert.Equal(t, tt.wantPrefix, out["ecrRepositoryPrefix"])
			}
		})
	}
}

func TestECR_CreatePullThroughCacheRule_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECRRequest(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "docker-hub",
		"upstreamRegistryUrl": "registry-1.docker.io",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doECRRequest(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "docker-hub",
		"upstreamRegistryUrl": "registry-1.docker.io",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Contains(t, out["__type"], "PullThroughCacheRuleAlreadyExistsException")
}

func TestECR_DeletePullThroughCacheRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		setup               func(*ecr.Handler)
		ecrRepositoryPrefix string
		wantStatus          int
	}{
		{
			name: "deletes existing rule",
			setup: func(h *ecr.Handler) {
				rec := doECRRequest(t, h, "CreatePullThroughCacheRule", map[string]any{
					"ecrRepositoryPrefix": "docker-hub",
					"upstreamRegistryUrl": "registry-1.docker.io",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			ecrRepositoryPrefix: "docker-hub",
			wantStatus:          http.StatusOK,
		},
		{
			name:                "nonexistent rule returns not found",
			ecrRepositoryPrefix: "nonexistent",
			wantStatus:          http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doECRRequest(t, h, "DeletePullThroughCacheRule", map[string]any{
				"ecrRepositoryPrefix": tt.ecrRepositoryPrefix,
			})
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				out := parseAccuracy(t, rec)
				assert.Equal(t, tt.ecrRepositoryPrefix, out["ecrRepositoryPrefix"])
			}
		})
	}
}
