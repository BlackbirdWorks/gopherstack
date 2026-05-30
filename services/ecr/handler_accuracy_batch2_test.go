package ecr_test

// handler_accuracy_batch2_test.go — ECR AWS-accuracy audit batch-2 (go-cpcn)
//
// Covers: BatchGetRepositoryScanningConfiguration mix, PutImageScanningConfiguration
// round-trip, DescribeRepositories pagination, DescribePullThroughCacheRules filter,
// UpdatePullThroughCacheRule, ValidatePullThroughCacheRule, DescribeRepositoryCreationTemplates
// filter, UpdateRepositoryCreationTemplate, StartLifecyclePolicyPreview round-trip,
// ListImageReferrers, UpdateImageStorageClass, DescribeImageReplicationStatus,
// DescribeImageSigningStatus, IMMUTABLE repository tag rejection, BatchCheckLayerAvailability
// mix, GetDownloadURLForLayer format, DescribeImages missing imageId error,
// PutRegistryScanningConfiguration round-trip.

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

// ── helpers shared by batch-2 tests ─────────────────────────────────────────

func newBatch2Handler() *ecr.Handler {
	return ecr.NewHandler(ecr.NewInMemoryBackend("123456789012", "us-east-1", "localhost:5000"), nil)
}

func newBatch2Backend() *ecr.InMemoryBackend {
	return ecr.NewInMemoryBackend("123456789012", "us-east-1", "localhost:5000")
}

// ── §1 BatchGetRepositoryScanningConfiguration ────────────────────────────────

func TestBatch2_BatchGetRepositoryScanningConfiguration_AllExist(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	mustCreateRepo(t, h, "scan-repo-a")
	mustCreateRepo(t, h, "scan-repo-b")

	rec := doAccuracy(t, h, "BatchGetRepositoryScanningConfiguration", map[string]any{
		"repositoryNames": []string{"scan-repo-a", "scan-repo-b"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	configs, _ := out["scanningConfigurations"].([]any)
	failures, _ := out["failures"].([]any)

	assert.Len(t, configs, 2, "both repos should return scanning configurations")
	assert.Empty(t, failures)
}

func TestBatch2_BatchGetRepositoryScanningConfiguration_MixedExistence(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	mustCreateRepo(t, h, "scan-exists")

	rec := doAccuracy(t, h, "BatchGetRepositoryScanningConfiguration", map[string]any{
		"repositoryNames": []string{"scan-exists", "scan-missing"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	configs, _ := out["scanningConfigurations"].([]any)
	failures, _ := out["failures"].([]any)

	assert.Len(t, configs, 1, "only existing repo should appear in scanningConfigurations")
	assert.Len(t, failures, 1, "non-existent repo should appear in failures")

	failure := failures[0].(map[string]any)
	assert.Equal(t, "scan-missing", failure["repositoryName"])
	assert.NotEmpty(t, failure["failureCode"])
}

func TestBatch2_BatchGetRepositoryScanningConfiguration_ScanOnPush_Reflected(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName": "scan-sop-repo",
		"imageScanningConfiguration": map[string]any{
			"scanOnPush": true,
		},
	})

	rec := doAccuracy(t, h, "BatchGetRepositoryScanningConfiguration", map[string]any{
		"repositoryNames": []string{"scan-sop-repo"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	configs, _ := out["scanningConfigurations"].([]any)
	require.Len(t, configs, 1)

	cfg := configs[0].(map[string]any)
	assert.Equal(t, true, cfg["scanOnPush"])
}

// ── §2 PutImageScanningConfiguration round-trip ───────────────────────────────

func TestBatch2_PutImageScanningConfiguration_EnableDisable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		initialSOP bool
		updatedSOP bool
	}{
		{name: "enable_scan_on_push", initialSOP: false, updatedSOP: true},
		{name: "disable_scan_on_push", initialSOP: true, updatedSOP: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newBatch2Handler()
			doAccuracy(t, h, "CreateRepository", map[string]any{
				"repositoryName": tt.name,
				"imageScanningConfiguration": map[string]any{
					"scanOnPush": tt.initialSOP,
				},
			})

			rec := doAccuracy(t, h, "PutImageScanningConfiguration", map[string]any{
				"repositoryName": tt.name,
				"imageScanningConfiguration": map[string]any{
					"scanOnPush": tt.updatedSOP,
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			out := parseAccuracy(t, rec)
			cfg, _ := out["imageScanningConfiguration"].(map[string]any)
			assert.Equal(t, tt.updatedSOP, cfg["scanOnPush"])
			assert.Equal(t, tt.name, out["repositoryName"])
		})
	}
}

func TestBatch2_PutImageScanningConfiguration_PersistsViaDescribeRepositories(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	mustCreateRepo(t, h, "persist-scan-repo")

	doAccuracy(t, h, "PutImageScanningConfiguration", map[string]any{
		"repositoryName": "persist-scan-repo",
		"imageScanningConfiguration": map[string]any{
			"scanOnPush": true,
		},
	})

	rec := doAccuracy(t, h, "DescribeRepositories", map[string]any{
		"repositoryNames": []string{"persist-scan-repo"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repos, _ := out["repositories"].([]any)
	require.Len(t, repos, 1)

	repo := repos[0].(map[string]any)
	cfg, _ := repo["imageScanningConfiguration"].(map[string]any)
	assert.Equal(t, true, cfg["scanOnPush"], "PutImageScanningConfiguration must persist via DescribeRepositories")
}

// ── §3 DescribeRepositories pagination ────────────────────────────────────────

func TestBatch2_DescribeRepositories_Pagination_NextToken(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	for i := range 5 {
		mustCreateRepo(t, h, fmt.Sprintf("page-repo-%02d", i))
	}

	// First page: maxResults=2.
	rec := doAccuracy(t, h, "DescribeRepositories", map[string]any{
		"maxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repos, _ := out["repositories"].([]any)
	require.Len(t, repos, 2)
	nextToken, _ := out["nextToken"].(string)
	require.NotEmpty(t, nextToken, "maxResults=2 with 5 repos must return a nextToken")

	// Second page.
	rec2 := doAccuracy(t, h, "DescribeRepositories", map[string]any{
		"maxResults": 2,
		"nextToken":  nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	out2 := parseAccuracy(t, rec2)
	repos2, _ := out2["repositories"].([]any)
	require.Len(t, repos2, 2)

	// Page 1 and page 2 must not overlap.
	page1Names := make(map[string]bool)
	for _, r := range repos {
		rm := r.(map[string]any)
		page1Names[rm["repositoryName"].(string)] = true
	}
	for _, r := range repos2 {
		rm := r.(map[string]any)
		assert.False(t, page1Names[rm["repositoryName"].(string)], "page 2 must not overlap page 1")
	}
}

func TestBatch2_DescribeRepositories_Pagination_LastPage_NoNextToken(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	mustCreateRepo(t, h, "last-page-a")
	mustCreateRepo(t, h, "last-page-b")

	rec := doAccuracy(t, h, "DescribeRepositories", map[string]any{
		"maxResults": 10,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	nextToken, hasToken := out["nextToken"].(string)
	if hasToken {
		assert.Empty(t, nextToken, "last page must not return a nextToken")
	}
}

// ── §4 DescribePullThroughCacheRules filter ────────────────────────────────────

func TestBatch2_DescribePullThroughCacheRules_FilterByPrefix(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
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

func TestBatch2_DescribePullThroughCacheRules_MissingPrefix_Errors(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := doAccuracy(t, h, "DescribePullThroughCacheRules", map[string]any{
		"ecrRepositoryPrefixes": []string{"does-not-exist"},
	})
	assert.NotEqual(t, http.StatusOK, rec.Code,
		"filter for non-existent prefix must return an error")
}

func TestBatch2_DescribePullThroughCacheRules_NoFilter_ReturnsAll(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
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

// ── §5 UpdatePullThroughCacheRule ─────────────────────────────────────────────

func TestBatch2_UpdatePullThroughCacheRule_CredentialUpdated(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	doAccuracy(t, h, "CreatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "update-cred",
		"upstreamRegistryUrl": "https://registry.example.com",
	})

	newCred := "arn:aws:secretsmanager:us-east-1:123456789012:secret/ecr-cred"
	rec := doAccuracy(t, h, "UpdatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "update-cred",
		"credentialArn":       newCred,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, newCred, out["credentialArn"], "credentialArn must be updated")
	assert.Equal(t, "update-cred", out["ecrRepositoryPrefix"], "prefix must be preserved")
	assert.Equal(t, "https://registry.example.com", out["upstreamRegistryUrl"],
		"upstreamRegistryUrl must not be changed by update")
}

func TestBatch2_UpdatePullThroughCacheRule_CustomRoleUpdated(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
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

func TestBatch2_UpdatePullThroughCacheRule_NotFound_Errors(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := doAccuracy(t, h, "UpdatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "no-such-prefix",
		"credentialArn":       "arn:aws:secretsmanager:us-east-1:123456789012:secret/x",
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// ── §6 ValidatePullThroughCacheRule ───────────────────────────────────────────

func TestBatch2_ValidatePullThroughCacheRule_ExistingRule_IsValid(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
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

func TestBatch2_ValidatePullThroughCacheRule_MissingRule_NotValid(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := doAccuracy(t, h, "ValidatePullThroughCacheRule", map[string]any{
		"ecrRepositoryPrefix": "nonexistent-validate",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, false, out["isValid"])
	assert.NotEmpty(t, out["failure"], "failure message must be present when rule is not found")
}

// ── §7 DescribeRepositoryCreationTemplates filter ─────────────────────────────

func TestBatch2_DescribeRepositoryCreationTemplates_FilterByPrefix(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	for _, prefix := range []string{"app/", "infra/", "data/"} {
		doAccuracy(t, h, "CreateRepositoryCreationTemplate", map[string]any{
			"prefix": prefix,
		})
	}

	rec := doAccuracy(t, h, "DescribeRepositoryCreationTemplates", map[string]any{
		"prefixes": []string{"infra/"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	tmpls, _ := out["repositoryCreationTemplates"].([]any)
	require.Len(t, tmpls, 1, "filter by prefix must return only the matching template")
	tmpl := tmpls[0].(map[string]any)
	assert.Equal(t, "infra/", tmpl["prefix"])
}

func TestBatch2_DescribeRepositoryCreationTemplates_NoFilter_ReturnsAll(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	for _, prefix := range []string{"team-a/", "team-b/"} {
		doAccuracy(t, h, "CreateRepositoryCreationTemplate", map[string]any{
			"prefix": prefix,
		})
	}

	rec := doAccuracy(t, h, "DescribeRepositoryCreationTemplates", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	tmpls, _ := out["repositoryCreationTemplates"].([]any)
	assert.Len(t, tmpls, 2)
}

func TestBatch2_DescribeRepositoryCreationTemplates_RegistryID_Present(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	doAccuracy(t, h, "CreateRepositoryCreationTemplate", map[string]any{
		"prefix": "check-registry/",
	})

	rec := doAccuracy(t, h, "DescribeRepositoryCreationTemplates", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "123456789012", out["registryId"],
		"DescribeRepositoryCreationTemplates must include registryId")
}

// ── §8 UpdateRepositoryCreationTemplate ──────────────────────────────────────

func TestBatch2_UpdateRepositoryCreationTemplate_DescriptionUpdated(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	doAccuracy(t, h, "CreateRepositoryCreationTemplate", map[string]any{
		"prefix":      "update-tmpl/",
		"description": "original",
	})

	rec := doAccuracy(t, h, "UpdateRepositoryCreationTemplate", map[string]any{
		"prefix":      "update-tmpl/",
		"description": "updated",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	tmpl, _ := out["repositoryCreationTemplate"].(map[string]any)
	assert.Equal(t, "updated", tmpl["description"])
	assert.Equal(t, "update-tmpl/", tmpl["prefix"])
}

func TestBatch2_UpdateRepositoryCreationTemplate_NotFound_Errors(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := doAccuracy(t, h, "UpdateRepositoryCreationTemplate", map[string]any{
		"prefix":      "no-such-template/",
		"description": "new desc",
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

func TestBatch2_UpdateRepositoryCreationTemplate_AppliedForPreserved(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	doAccuracy(t, h, "CreateRepositoryCreationTemplate", map[string]any{
		"prefix":     "applied-tmpl/",
		"appliedFor": []string{"REPLICATION", "PULL_THROUGH_CACHE"},
	})

	rec := doAccuracy(t, h, "UpdateRepositoryCreationTemplate", map[string]any{
		"prefix":      "applied-tmpl/",
		"description": "changed desc",
		"appliedFor":  []string{"REPLICATION", "PULL_THROUGH_CACHE"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	tmpl, _ := out["repositoryCreationTemplate"].(map[string]any)
	appliedFor, _ := tmpl["appliedFor"].([]any)
	assert.Len(t, appliedFor, 2)
}

// ── §9 StartLifecyclePolicyPreview + GetLifecyclePolicyPreview ─────────────────

func TestBatch2_LifecyclePolicyPreview_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	mustCreateRepo(t, h, "preview-repo")
	policyText := `{"rules":[{"rulePriority":1,` +
		`"selection":{"tagStatus":"untagged","countType":"imageCountMoreThan","countNumber":1},` +
		`"action":{"type":"expire"}}]}`
	doAccuracy(t, h, "PutLifecyclePolicy", map[string]any{
		"repositoryName":      "preview-repo",
		"lifecyclePolicyText": policyText,
	})

	startRec := doAccuracy(t, h, "StartLifecyclePolicyPreview", map[string]any{
		"repositoryName": "preview-repo",
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	startOut := parseAccuracy(t, startRec)
	assert.Equal(t, "preview-repo", startOut["repositoryName"])
	assert.Equal(t, "COMPLETE", startOut["status"])

	getRec := doAccuracy(t, h, "GetLifecyclePolicyPreview", map[string]any{
		"repositoryName": "preview-repo",
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	getOut := parseAccuracy(t, getRec)
	assert.Equal(t, "preview-repo", getOut["repositoryName"])
	assert.Equal(t, "COMPLETE", getOut["status"])
	assert.Equal(t, policyText, getOut["lifecyclePolicyText"],
		"GetLifecyclePolicyPreview must return the policy text used for the preview")
}

func TestBatch2_StartLifecyclePolicyPreview_WithInlinePolicy(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	mustCreateRepo(t, h, "inline-preview")

	inlinePolicy := `{"rules":[]}`
	rec := doAccuracy(t, h, "StartLifecyclePolicyPreview", map[string]any{
		"repositoryName":      "inline-preview",
		"lifecyclePolicyText": inlinePolicy,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, inlinePolicy, out["lifecyclePolicyText"])
}

func TestBatch2_GetLifecyclePolicyPreview_NotStarted_Errors(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	mustCreateRepo(t, h, "no-preview")

	rec := doAccuracy(t, h, "GetLifecyclePolicyPreview", map[string]any{
		"repositoryName": "no-preview",
	})
	assert.NotEqual(t, http.StatusOK, rec.Code,
		"GetLifecyclePolicyPreview without StartLifecyclePolicyPreview must error")
}

// ── §10 ListImageReferrers ────────────────────────────────────────────────────

func TestBatch2_ListImageReferrers_NoReferrers_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	mustCreateRepo(t, h, "referrer-repo")
	digest := mustPutImage(t, h, "referrer-repo", "v1", `{"schemaVersion":2,"referrer":"test"}`)

	rec := doAccuracy(t, h, "ListImageReferrers", map[string]any{
		"repositoryName": "referrer-repo",
		"subjectId": map[string]any{
			"imageDigest": digest,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	referrers, _ := out["referrers"].([]any)
	assert.Empty(t, referrers, "image with no referrers must return an empty list")
}

func TestBatch2_ListImageReferrers_NonExistentImage_Errors(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	mustCreateRepo(t, h, "referrer-repo-err")

	rec := doAccuracy(t, h, "ListImageReferrers", map[string]any{
		"repositoryName": "referrer-repo-err",
		"subjectId": map[string]any{
			"imageDigest": "sha256:0000000000000000000000000000000000000000000000000000000000000000",
		},
	})
	assert.NotEqual(t, http.StatusOK, rec.Code,
		"non-existent image digest must return an error")
}

// ── §11 UpdateImageStorageClass ───────────────────────────────────────────────

func TestBatch2_UpdateImageStorageClass_Archive(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	mustCreateRepo(t, h, "storage-repo")
	digest := mustPutImage(t, h, "storage-repo", "latest", `{"schemaVersion":2,"storage":"test"}`)

	rec := doAccuracy(t, h, "UpdateImageStorageClass", map[string]any{
		"repositoryName": "storage-repo",
		"imageId": map[string]any{
			"imageDigest": digest,
		},
		"targetStorageClass": "ARCHIVE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "ARCHIVED", out["imageStatus"],
		"ARCHIVE storage class must set imageStatus to ARCHIVED")
	assert.Equal(t, "storage-repo", out["repositoryName"])
	assert.Equal(t, "123456789012", out["registryId"])
}

func TestBatch2_UpdateImageStorageClass_Standard_RestoresActive(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	mustCreateRepo(t, h, "restore-storage-repo")
	digest := mustPutImage(t, h, "restore-storage-repo", "v1", `{"schemaVersion":2,"restore":"test"}`)

	// Archive then restore.
	doAccuracy(t, h, "UpdateImageStorageClass", map[string]any{
		"repositoryName":     "restore-storage-repo",
		"imageId":            map[string]any{"imageDigest": digest},
		"targetStorageClass": "ARCHIVE",
	})

	rec := doAccuracy(t, h, "UpdateImageStorageClass", map[string]any{
		"repositoryName":     "restore-storage-repo",
		"imageId":            map[string]any{"imageDigest": digest},
		"targetStorageClass": "STANDARD",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "ACTIVE", out["imageStatus"],
		"STANDARD storage class must restore imageStatus to ACTIVE")
}

// ── §12 DescribeImageReplicationStatus ───────────────────────────────────────

func TestBatch2_DescribeImageReplicationStatus_ReturnsStatus(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	mustCreateRepo(t, h, "replication-repo")
	digest := mustPutImage(t, h, "replication-repo", "v1.0", `{"schemaVersion":2,"repl":"test"}`)

	rec := doAccuracy(t, h, "DescribeImageReplicationStatus", map[string]any{
		"repositoryName": "replication-repo",
		"imageId": map[string]any{
			"imageDigest": digest,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "replication-repo", out["repositoryName"])
	statuses, _ := out["replicationStatuses"].([]any)
	require.NotEmpty(t, statuses, "replicationStatuses must be present")
	status := statuses[0].(map[string]any)
	assert.NotEmpty(t, status["status"], "replication status must not be empty")
}

func TestBatch2_DescribeImageReplicationStatus_ByTag(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	mustCreateRepo(t, h, "replication-tag-repo")
	mustPutImage(t, h, "replication-tag-repo", "release", `{"schemaVersion":2,"repl":"tag"}`)

	rec := doAccuracy(t, h, "DescribeImageReplicationStatus", map[string]any{
		"repositoryName": "replication-tag-repo",
		"imageId": map[string]any{
			"imageTag": "release",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "replication-tag-repo", out["repositoryName"])
}

// ── §13 DescribeImageSigningStatus ────────────────────────────────────────────

func TestBatch2_DescribeImageSigningStatus_NoSigningConfig_StatusComplete(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	mustCreateRepo(t, h, "signing-repo-no-cfg")
	digest := mustPutImage(t, h, "signing-repo-no-cfg", "v1", `{"schemaVersion":2,"sign":"none"}`)

	rec := doAccuracy(t, h, "DescribeImageSigningStatus", map[string]any{
		"repositoryName": "signing-repo-no-cfg",
		"imageId": map[string]any{
			"imageDigest": digest,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	statuses, _ := out["signingStatuses"].([]any)
	require.NotEmpty(t, statuses)
	s0 := statuses[0].(map[string]any)
	assert.Equal(t, "COMPLETE", s0["status"])
}

func TestBatch2_DescribeImageSigningStatus_WithSigningConfig_ProfileArnReflected(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	mustCreateRepo(t, h, "signing-repo-with-cfg")
	digest := mustPutImage(t, h, "signing-repo-with-cfg", "v2", `{"schemaVersion":2,"sign":"cfg"}`)

	profileArn := "arn:aws:signer:us-east-1:123456789012:/signing-profiles/MyProfile"
	doAccuracy(t, h, "PutSigningConfiguration", map[string]any{
		"signingConfiguration": map[string]any{
			"rules": []map[string]any{
				{
					"signingProfileArn": profileArn,
					"repositoryFilters": []map[string]any{
						{"filter": "*", "filterType": "WILDCARD"},
					},
				},
			},
		},
	})

	rec := doAccuracy(t, h, "DescribeImageSigningStatus", map[string]any{
		"repositoryName": "signing-repo-with-cfg",
		"imageId": map[string]any{
			"imageDigest": digest,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	statuses, _ := out["signingStatuses"].([]any)
	require.NotEmpty(t, statuses)
	s0 := statuses[0].(map[string]any)
	assert.Equal(t, profileArn, s0["signingProfileArn"],
		"signing profile ARN must be reflected in DescribeImageSigningStatus when signing config is set")
}

// ── §14 IMMUTABLE repository tag rejection ────────────────────────────────────

func TestBatch2_ImmutableRepo_RejectRetag(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName":     "immutable-repo",
		"imageTagMutability": "IMMUTABLE",
	})

	// Push first image with tag v1.
	mustPutImage(t, h, "immutable-repo", "v1", `{"schemaVersion":2,"v":1}`)

	// Try to push a different manifest with same tag v1 — must fail.
	rec := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "immutable-repo",
		"imageManifest":  `{"schemaVersion":2,"v":2}`,
		"imageTag":       "v1",
	})
	// AWS returns 400 with ImageTagAlreadyExistsException for IMMUTABLE tag violations.
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"IMMUTABLE repo must reject retagging an existing tag to a different digest")
}

func TestBatch2_ImmutableRepo_SameManifestSameTag_Idempotent(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName":     "immutable-idempotent",
		"imageTagMutability": "IMMUTABLE",
	})

	manifest := `{"schemaVersion":2,"idempotent":true}`
	mustPutImage(t, h, "immutable-idempotent", "stable", manifest)

	// Same manifest + same tag = same digest → should succeed (idempotent).
	rec := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "immutable-idempotent",
		"imageManifest":  manifest,
		"imageTag":       "stable",
	})
	assert.Equal(t, http.StatusOK, rec.Code,
		"IMMUTABLE repo must allow re-push of same manifest with same tag")
}

func TestBatch2_ImmutableRepo_NewTag_Allowed(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	doAccuracy(t, h, "CreateRepository", map[string]any{
		"repositoryName":     "immutable-new-tag",
		"imageTagMutability": "IMMUTABLE",
	})

	mustPutImage(t, h, "immutable-new-tag", "v1", `{"schemaVersion":2,"n":1}`)

	// New tag for new manifest — must succeed.
	rec := doAccuracy(t, h, "PutImage", map[string]any{
		"repositoryName": "immutable-new-tag",
		"imageManifest":  `{"schemaVersion":2,"n":2}`,
		"imageTag":       "v2",
	})
	assert.Equal(t, http.StatusOK, rec.Code,
		"IMMUTABLE repo must allow new tags")
}

// ── §15 BatchCheckLayerAvailability ──────────────────────────────────────────

func TestBatch2_BatchCheckLayerAvailability_AllAvailable(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateRepository("layer-repo", "MUTABLE", false, "", "")
	require.NoError(t, err)

	digest := "sha256:aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	_, err = b.CompleteLayerUpload("layer-repo", "upload-1", []string{digest})
	require.NoError(t, err)

	h := ecr.NewHandler(b, nil)
	rec := doAccuracy(t, h, "BatchCheckLayerAvailability", map[string]any{
		"repositoryName": "layer-repo",
		"layerDigests":   []string{digest},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	layers, _ := out["layers"].([]any)
	failures, _ := out["failures"].([]any)

	require.Len(t, layers, 1)
	require.Empty(t, failures)
	layer := layers[0].(map[string]any)
	assert.Equal(t, "AVAILABLE", layer["layerAvailability"])
	assert.Equal(t, digest, layer["layerDigest"])
}

func TestBatch2_BatchCheckLayerAvailability_Mixed(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateRepository("mixed-layer-repo", "MUTABLE", false, "", "")
	require.NoError(t, err)

	presentDigest := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	missingDigest := "sha256:9999999999999999999999999999999999999999999999999999999999999999"
	_, err = b.CompleteLayerUpload("mixed-layer-repo", "upload-x", []string{presentDigest})
	require.NoError(t, err)

	h := ecr.NewHandler(b, nil)
	rec := doAccuracy(t, h, "BatchCheckLayerAvailability", map[string]any{
		"repositoryName": "mixed-layer-repo",
		"layerDigests":   []string{presentDigest, missingDigest},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	layers, _ := out["layers"].([]any)
	failures, _ := out["failures"].([]any)

	assert.Len(t, layers, 1, "one present layer must appear in layers")
	assert.Len(t, failures, 1, "one missing layer must appear in failures")
	failure := failures[0].(map[string]any)
	assert.Equal(t, missingDigest, failure["layerDigest"])
	assert.Equal(t, "MissingLayerDigest", failure["failureCode"])
}

func TestBatch2_BatchCheckLayerAvailability_RepoNotFound_Errors(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := doAccuracy(t, h, "BatchCheckLayerAvailability", map[string]any{
		"repositoryName": "does-not-exist",
		"layerDigests":   []string{"sha256:abcdef"},
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// ── §16 GetDownloadUrlForLayer ────────────────────────────────────────────────

func TestBatch2_GetDownloadUrlForLayer_URLFormat(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateRepository("download-repo", "MUTABLE", false, "", "")
	require.NoError(t, err)

	digest := "sha256:deadbeef00000000000000000000000000000000000000000000000000000000"
	_, err = b.CompleteLayerUpload("download-repo", "upload-dl", []string{digest})
	require.NoError(t, err)

	h := ecr.NewHandler(b, nil)
	rec := doAccuracy(t, h, "GetDownloadUrlForLayer", map[string]any{
		"repositoryName": "download-repo",
		"layerDigest":    digest,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	downloadURL, _ := out["downloadUrl"].(string)
	assert.NotEmpty(t, downloadURL, "downloadUrl must be present")
	assert.True(t, strings.HasPrefix(downloadURL, "http://"),
		"downloadUrl must start with http://")
	assert.Contains(t, downloadURL, "download-repo",
		"downloadUrl must contain the repository name")
	assert.Contains(t, downloadURL, digest,
		"downloadUrl must contain the layer digest")
	assert.Equal(t, digest, out["layerDigest"],
		"layerDigest in response must match the requested digest")
}

func TestBatch2_GetDownloadUrlForLayer_MissingLayer_Errors(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	mustCreateRepo(t, h, "missing-layer-repo")

	rec := doAccuracy(t, h, "GetDownloadUrlForLayer", map[string]any{
		"repositoryName": "missing-layer-repo",
		"layerDigest":    "sha256:0000000000000000000000000000000000000000000000000000000000000000",
	})
	assert.NotEqual(t, http.StatusOK, rec.Code,
		"GetDownloadUrlForLayer for non-existent layer must error")
}

// ── §17 DescribeImages with imageIds — missing image returns error ────────────

func TestBatch2_DescribeImages_ByDigest_NonExistent_Errors(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	mustCreateRepo(t, h, "desc-missing-repo")

	rec := doAccuracy(t, h, "DescribeImages", map[string]any{
		"repositoryName": "desc-missing-repo",
		"imageIds": []map[string]any{
			{"imageDigest": "sha256:9999999999999999999999999999999999999999999999999999999999999999"},
		},
	})
	assert.NotEqual(t, http.StatusOK, rec.Code,
		"DescribeImages with non-existent imageId must return an error")
}

func TestBatch2_DescribeImages_EmptyRepo_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	mustCreateRepo(t, h, "empty-desc-repo")

	rec := doAccuracy(t, h, "DescribeImages", map[string]any{
		"repositoryName": "empty-desc-repo",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	details, _ := out["imageDetails"].([]any)
	assert.Empty(t, details)
}

// ── §18 PutRegistryScanningConfiguration round-trip ──────────────────────────

func TestBatch2_PutRegistryScanningConfiguration_ScanTypeEnhanced(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := doAccuracy(t, h, "PutRegistryScanningConfiguration", map[string]any{
		"scanType": "ENHANCED",
		"rules": []map[string]any{
			{
				"scanFrequency": "CONTINUOUS_SCAN",
				"repositoryFilters": []map[string]any{
					{"filter": "*", "filterType": "WILDCARD"},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	cfg, _ := out["scanningConfiguration"].(map[string]any)
	assert.Equal(t, "ENHANCED", cfg["scanType"])
	rules, _ := cfg["rules"].([]any)
	require.Len(t, rules, 1)
}

func TestBatch2_PutRegistryScanningConfiguration_PersistedViaGet(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	doAccuracy(t, h, "PutRegistryScanningConfiguration", map[string]any{
		"scanType": "ENHANCED",
		"rules":    []map[string]any{},
	})

	rec := doAccuracy(t, h, "GetRegistryScanningConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	cfg, _ := out["scanningConfiguration"].(map[string]any)
	assert.Equal(t, "ENHANCED", cfg["scanType"],
		"PutRegistryScanningConfiguration must persist via GetRegistryScanningConfiguration")
}

func TestBatch2_PutRegistryScanningConfiguration_DefaultScanTypeBasic(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	// A fresh backend must return BASIC as the default scan type.
	rec := doAccuracy(t, h, "GetRegistryScanningConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	cfg, _ := out["scanningConfiguration"].(map[string]any)
	assert.Equal(t, "BASIC", cfg["scanType"],
		"default scan type must be BASIC before any PutRegistryScanningConfiguration call")
}
