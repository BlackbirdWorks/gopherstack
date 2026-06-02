package eks_test

// handler_audit2_test.go — AWS-accuracy audit batch-2 for EKS (go-8i2i).
//
// Covers: tag key/value length limits, max 50 tags per resource,
// CreateNodegroup subnets required, CreateNodegroup nodeRole required,
// initial tags on CreateCluster and CreateNodegroup validated.

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Gap 1: TagResource — key/value length limits
// ---------------------------------------------------------------------------

func TestAudit2_TagResource_KeyTooLong_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	longKey := strings.Repeat("k", 129)
	rec := doREST(t, h, http.MethodPost, "/tags/arn:aws:eks:us-east-1:123456789012:cluster/c1",
		map[string]any{"tags": map[string]string{longKey: "v"}})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit2_TagResource_KeyEmpty_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	rec := doREST(t, h, http.MethodPost, "/tags/arn:aws:eks:us-east-1:123456789012:cluster/c1",
		map[string]any{"tags": map[string]string{"": "v"}})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit2_TagResource_ValueTooLong_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	longVal := strings.Repeat("v", 257)
	rec := doREST(t, h, http.MethodPost, "/tags/arn:aws:eks:us-east-1:123456789012:cluster/c1",
		map[string]any{"tags": map[string]string{"k": longVal}})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit2_TagResource_ValidBoundary_Accepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		key   string
		value string
		name  string
	}{
		{name: "key_128", key: strings.Repeat("k", 128), value: "v"},
		{name: "value_256", key: "k", value: strings.Repeat("v", 256)},
		{name: "value_empty", key: "k", value: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler()
			doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

			rec := doREST(t, h, http.MethodPost, "/tags/arn:aws:eks:us-east-1:123456789012:cluster/c1",
				map[string]any{"tags": map[string]string{tt.key: tt.value}})
			assert.Equal(t, http.StatusOK, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// Gap 2: TagResource — max 50 tags per resource
// ---------------------------------------------------------------------------

func TestAudit2_TagResource_MaxTags_Enforced(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	clusterARN := "arn:aws:eks:us-east-1:123456789012:cluster/c1"

	// Add 50 tags in batches.
	for i := range 5 {
		batch := map[string]string{}
		for j := range 10 {
			batch[strings.Repeat("a", 1)+string(rune('a'+i))+string(rune('0'+j))] = "v"
		}
		rec := doREST(t, h, http.MethodPost, "/tags/"+clusterARN,
			map[string]any{"tags": batch})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Adding one more tag must fail.
	rec := doREST(t, h, http.MethodPost, "/tags/"+clusterARN,
		map[string]any{"tags": map[string]string{"extra": "v"}})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Gap 3: CreateCluster — initial tags validated
// ---------------------------------------------------------------------------

func TestAudit2_CreateCluster_InitialTagKeyTooLong_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	longKey := strings.Repeat("k", 129)
	rec := doREST(t, h, http.MethodPost, "/clusters", map[string]any{
		"name": "bad-tag-cluster",
		"tags": map[string]string{longKey: "v"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit2_CreateCluster_InitialTagValueTooLong_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	longVal := strings.Repeat("v", 257)
	rec := doREST(t, h, http.MethodPost, "/clusters", map[string]any{
		"name": "bad-tag-cluster",
		"tags": map[string]string{"k": longVal},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit2_CreateCluster_ValidTags_Accepted(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	rec := doREST(t, h, http.MethodPost, "/clusters", map[string]any{
		"name": "ok-tag-cluster",
		"tags": map[string]string{"Env": "prod", "Team": "platform"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// Gap 4: CreateNodegroup — subnets required
// ---------------------------------------------------------------------------

func TestAudit2_CreateNodegroup_SubnetsRequired(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	rec := doREST(t, h, http.MethodPost, "/clusters/c1/node-groups", map[string]any{
		"nodegroupName": "ng-no-subnets",
		"nodeRole":      "arn:aws:iam::123456789012:role/ng",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit2_CreateNodegroup_SubnetsRequired_EmptySlice_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	rec := doREST(t, h, http.MethodPost, "/clusters/c1/node-groups", map[string]any{
		"nodegroupName": "ng-empty-subnets",
		"nodeRole":      "arn:aws:iam::123456789012:role/ng",
		"subnets":       []string{},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit2_CreateNodegroup_SubnetsNotRequired_WithLaunchTemplate(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	// subnets omitted but launchTemplate present — must succeed.
	rec := doREST(t, h, http.MethodPost, "/clusters/c1/node-groups", map[string]any{
		"nodegroupName": "ng-lt-nosubnets",
		"nodeRole":      "arn:aws:iam::123456789012:role/ng",
		"launchTemplate": map[string]any{
			"id":      "lt-0123456789abcdef0",
			"version": "$Default",
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// Gap 5: CreateNodegroup — nodeRole required
// ---------------------------------------------------------------------------

func TestAudit2_CreateNodegroup_NodeRoleRequired(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	rec := doREST(t, h, http.MethodPost, "/clusters/c1/node-groups", map[string]any{
		"nodegroupName": "ng-no-role",
		"subnets":       []string{"subnet-abc"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Gap 6: CreateNodegroup — initial tags validated
// ---------------------------------------------------------------------------

func TestAudit2_CreateNodegroup_InitialTagKeyTooLong_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	longKey := strings.Repeat("k", 129)
	rec := doREST(t, h, http.MethodPost, "/clusters/c1/node-groups", map[string]any{
		"nodegroupName": "ng-bad-tag",
		"nodeRole":      "arn:aws:iam::123456789012:role/ng",
		"subnets":       []string{"subnet-abc"},
		"tags":          map[string]string{longKey: "v"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit2_CreateNodegroup_ValidTags_Accepted(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler()
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "c1"})

	rec := doREST(t, h, http.MethodPost, "/clusters/c1/node-groups", map[string]any{
		"nodegroupName": "ng-ok-tag",
		"nodeRole":      "arn:aws:iam::123456789012:role/ng",
		"subnets":       []string{"subnet-abc"},
		"tags":          map[string]string{"Env": "prod"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
