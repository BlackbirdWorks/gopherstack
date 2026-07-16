package ecr_test

// lifecycle_test.go — verifies that lifecycle policies actually expire and
// DELETE matching images (not just store policy text), that expiry runs on
// PutLifecyclePolicy, via StartLifecyclePolicyPreview's selection logic, and
// via the janitor background sweep; that rule priority / one-rule-per-image
// semantics hold; and that tagPrefixList/tagPatternList/countType selection
// criteria are correct. This is lifecycle.go's expiry-evaluation logic, as
// distinct from lifecycle_policy.go's CRUD surface (see lifecycle_policy_test.go).

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

// seedImage adds an image with an explicit push time and tag to a repository.
func seedImage(b *ecr.InMemoryBackend, repo, digest, tag string, pushedAt time.Time) {
	img := ecr.Image{
		ImageDigest:   digest,
		ImageManifest: fmt.Sprintf(`{"schemaVersion":2,"d":%q}`, digest),
		ImageID:       ecr.ImageIdentifier{ImageDigest: digest, ImageTag: tag},
		ImagePushedAt: pushedAt,
	}
	b.AddImageInternal(repo, img)
}

func expirePolicy(sel string) string {
	return `{"rules":[{"rulePriority":1,"action":{"type":"expire"},"selection":` + sel + `}]}`
}

func TestLifecycle_PutPolicy_ActuallyDeletesImages(t *testing.T) {
	t.Parallel()

	now := time.Now()

	tests := []struct {
		name          string
		selection     string
		seed          func(b *ecr.InMemoryBackend, repo string)
		wantRemaining []string // digests that must survive
		wantDeleted   []string // digests that must be gone
	}{
		{
			name:      "imageCountMoreThan keeps newest N deletes rest",
			selection: `{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":3}`,
			seed: func(b *ecr.InMemoryBackend, repo string) {
				for i := range 5 {
					seedImage(b, repo, fmt.Sprintf("sha256:c%02d", i),
						fmt.Sprintf("v%d", i), now.Add(time.Duration(i)*time.Hour))
				}
			},
			wantRemaining: []string{"sha256:c02", "sha256:c03", "sha256:c04"},
			wantDeleted:   []string{"sha256:c00", "sha256:c01"},
		},
		{
			name:      "sinceImagePushed deletes images older than threshold",
			selection: `{"tagStatus":"any","countType":"sinceImagePushed","countUnit":"days","countNumber":30}`,
			seed: func(b *ecr.InMemoryBackend, repo string) {
				seedImage(b, repo, "sha256:old", "old", now.Add(-60*24*time.Hour))
				seedImage(b, repo, "sha256:new", "new", now.Add(-1*24*time.Hour))
			},
			wantRemaining: []string{"sha256:new"},
			wantDeleted:   []string{"sha256:old"},
		},
		{
			name:      "untagged only expires untagged images",
			selection: `{"tagStatus":"untagged","countType":"imageCountMoreThan","countNumber":0}`,
			seed: func(b *ecr.InMemoryBackend, repo string) {
				seedImage(b, repo, "sha256:tagged", "rel", now)
				seedImage(b, repo, "sha256:untag", "", now)
			},
			wantRemaining: []string{"sha256:tagged"},
			wantDeleted:   []string{"sha256:untag"},
		},
		{
			name:      "tagPrefixList matches literal prefixes",
			selection: `{"tagStatus":"tagged","tagPrefixList":["prod-"],"countType":"imageCountMoreThan","countNumber":0}`,
			seed: func(b *ecr.InMemoryBackend, repo string) {
				seedImage(b, repo, "sha256:prod", "prod-123", now)
				seedImage(b, repo, "sha256:dev", "dev-123", now)
			},
			wantRemaining: []string{"sha256:dev"},
			wantDeleted:   []string{"sha256:prod"},
		},
		{
			name:      "tagPatternList matches wildcard glob",
			selection: `{"tagStatus":"tagged","tagPatternList":["*-rc"],"countType":"imageCountMoreThan","countNumber":0}`,
			seed: func(b *ecr.InMemoryBackend, repo string) {
				seedImage(b, repo, "sha256:rc", "v1-rc", now)
				seedImage(b, repo, "sha256:ga", "v1-ga", now)
			},
			wantRemaining: []string{"sha256:ga"},
			wantDeleted:   []string{"sha256:rc"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			b.CreateRepoInternal("lc")
			tt.seed(b, "lc")

			_, err := b.PutLifecyclePolicy(context.Background(), "lc", expirePolicy(tt.selection))
			require.NoError(t, err)

			for _, d := range tt.wantDeleted {
				assert.False(t, b.HasImageDigest("lc", d), "digest %s must be deleted", d)
			}
			for _, d := range tt.wantRemaining {
				assert.True(t, b.HasImageDigest("lc", d), "digest %s must survive", d)
			}
			assert.Equal(t, len(tt.wantRemaining), b.RepoImageCount("lc"))
		})
	}
}

func TestLifecycle_RulePriority_OneRulePerImage(t *testing.T) {
	t.Parallel()

	// Rule 1 (higher priority) expires untagged images; rule 2 expires all but
	// the newest 1. A tagged newest image survives; an untagged image is claimed
	// by rule 1, and older tagged images fall to rule 2.
	b := newBackend(t)
	b.CreateRepoInternal("lc")

	now := time.Now()
	seedImage(b, "lc", "sha256:newest", "v3", now.Add(3*time.Hour))
	seedImage(b, "lc", "sha256:mid", "v2", now.Add(2*time.Hour))
	seedImage(b, "lc", "sha256:untag", "", now.Add(1*time.Hour))

	rule1 := `{"rulePriority":1,"action":{"type":"expire"},"selection":` +
		`{"tagStatus":"untagged","countType":"imageCountMoreThan","countNumber":0}}`
	rule2 := `{"rulePriority":2,"action":{"type":"expire"},"selection":` +
		`{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":1}}`
	policy := `{"rules":[` + rule1 + `,` + rule2 + `]}`

	_, err := b.PutLifecyclePolicy(context.Background(), "lc", policy)
	require.NoError(t, err)

	assert.True(t, b.HasImageDigest("lc", "sha256:newest"), "newest tagged image survives")
	assert.False(t, b.HasImageDigest("lc", "sha256:untag"), "untagged image expired by rule 1")
	assert.False(t, b.HasImageDigest("lc", "sha256:mid"), "older tagged image expired by rule 2")
}

func TestLifecycle_Janitor_ExpiresInBackground(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.CreateRepoInternal("lc")

	now := time.Now()
	for i := range 4 {
		seedImage(b, "lc", fmt.Sprintf("sha256:j%02d", i),
			fmt.Sprintf("v%d", i), now.Add(time.Duration(i)*time.Hour))
	}

	// Store the policy directly WITHOUT triggering the on-put evaluation, then
	// let the janitor perform the expiry — proving background evaluation works.
	b.AddLifecyclePolicyInternal("lc",
		expirePolicy(`{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":2}`))
	require.Equal(t, 4, b.RepoImageCount("lc"))

	j := ecr.NewJanitor(b, time.Minute)
	j.SweepOnce(context.Background())

	assert.Equal(t, 2, b.RepoImageCount("lc"), "janitor must expire down to countNumber")
	assert.False(t, b.LifecycleLastEvaluatedForTest("lc").IsZero(),
		"janitor records last-evaluated time")
}

func TestLifecycle_LastEvaluatedAt_IsReal(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.CreateRepoInternal("lc")
	seedImage(b, "lc", "sha256:a", "v1", time.Now())

	// No policy yet -> Get returns LifecyclePolicyNotFound.
	_, err := b.GetLifecyclePolicy(context.Background(), "lc")
	require.Error(t, err)

	_, err = b.PutLifecyclePolicy(context.Background(), "lc",
		expirePolicy(`{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":10}`))
	require.NoError(t, err)

	got, err := b.GetLifecyclePolicy(context.Background(), "lc")
	require.NoError(t, err)
	assert.False(t, got.LastEvaluatedAt.IsZero(), "LastEvaluatedAt must reflect a real evaluation")

	stored := b.LifecycleLastEvaluatedForTest("lc")
	assert.Equal(t, stored, got.LastEvaluatedAt, "Get must echo the stored evaluation time, not time.Now()")
}

func TestLifecycleEvaluator_MultiTagUntaggedDetection(t *testing.T) {
	t.Parallel()

	// An image with two tags where the primary tag is cleared but a tag still
	// exists in digestTagsIndex must NOT be classified as "untagged" by the
	// lifecycle evaluator.
	b := newBackend(t)
	b.CreateRepoInternal("lc-repo")

	digest := "sha256:1234"
	b.AddImageInternal("lc-repo", makeImage(digest, "v1"))
	// Simulate a second tag by calling PutImage directly (which updates digestTagsIndex).
	img2 := makeImage(digest, "stable")
	_, err := b.PutImage(context.Background(), "lc-repo", img2)
	require.NoError(t, err)

	// A lifecycle rule targeting "untagged" images must NOT expire this image.
	policy := expirePolicy(`{"tagStatus":"untagged","countType":"imageCountMoreThan","countNumber":0}`)

	preview, err := b.StartLifecyclePolicyPreview(context.Background(), "lc-repo", policy)
	require.NoError(t, err)

	for _, id := range preview.PreviewResults {
		assert.NotEqual(t, digest, id.ImageDigest,
			"multi-tagged image must not be classified as untagged")
	}
}

func TestLifecycleEvaluator_UntaggedOnly(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.CreateRepoInternal("lc-repo2")

	digestTagged := "sha256:tagged1"
	digestUntagged := "sha256:untagged1"
	b.AddImageInternal("lc-repo2", makeImage(digestTagged, "release"))
	b.AddImageInternal("lc-repo2", ecr.Image{
		ImageDigest:   digestUntagged,
		ImageManifest: `{"schemaVersion":2}`,
		ImageID:       ecr.ImageIdentifier{ImageDigest: digestUntagged},
	})

	policy := expirePolicy(`{"tagStatus":"untagged","countType":"imageCountMoreThan","countNumber":0}`)

	preview, err := b.StartLifecyclePolicyPreview(context.Background(), "lc-repo2", policy)
	require.NoError(t, err)

	expired := make(map[string]bool)
	for _, id := range preview.PreviewResults {
		expired[id.ImageDigest] = true
	}

	assert.True(t, expired[digestUntagged], "untagged image must be expired")
	assert.False(t, expired[digestTagged], "tagged image must NOT be expired")
}

func TestLifecycleEvaluator_ImageCountMoreThan(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.CreateRepoInternal("lc-count-repo")

	now := time.Now()
	for i := range 5 {
		digest := fmt.Sprintf("sha256:img%02d", i)
		img := ecr.Image{
			ImageDigest:   digest,
			ImageManifest: `{"schemaVersion":2}`,
			ImageID:       ecr.ImageIdentifier{ImageDigest: digest, ImageTag: fmt.Sprintf("v%d", i)},
			ImagePushedAt: now.Add(time.Duration(i) * time.Hour),
		}
		b.AddImageInternal("lc-count-repo", img)
	}

	policy := expirePolicy(`{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":3}`)

	preview, err := b.StartLifecyclePolicyPreview(context.Background(), "lc-count-repo", policy)
	require.NoError(t, err)

	assert.Len(t, preview.PreviewResults, 2, "imageCountMoreThan:3 with 5 images must expire 2")
}

func TestLifecyclePolicyPreview_SinceImagePushed(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.CreateRepoInternal("age-repo")

	now := time.Now()
	old := ecr.Image{
		ImageDigest:   "sha256:old",
		ImageManifest: `{"schemaVersion":2}`,
		ImageID:       ecr.ImageIdentifier{ImageDigest: "sha256:old", ImageTag: "old"},
		ImagePushedAt: now.AddDate(0, 0, -10),
	}
	fresh := ecr.Image{
		ImageDigest:   "sha256:fresh",
		ImageManifest: `{"schemaVersion":2}`,
		ImageID:       ecr.ImageIdentifier{ImageDigest: "sha256:fresh", ImageTag: "fresh"},
		ImagePushedAt: now.AddDate(0, 0, -1),
	}
	b.AddImageInternal("age-repo", old)
	b.AddImageInternal("age-repo", fresh)

	policy := expirePolicy(`{"tagStatus":"any","countType":"sinceImagePushed","countNumber":5,"countUnit":"days"}`)

	preview, err := b.StartLifecyclePolicyPreview(context.Background(), "age-repo", policy)
	require.NoError(t, err)

	expired := make(map[string]bool)
	for _, id := range preview.PreviewResults {
		expired[id.ImageDigest] = true
	}

	assert.True(t, expired["sha256:old"], "image older than 5 days must be expired")
	assert.False(t, expired["sha256:fresh"], "image newer than 5 days must not be expired")
}

func TestLifecyclePolicy_DeletesViaDescribeImages(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	mustCreateRepo(t, h, "lc")

	for i, tag := range []string{"a", "b", "c", "d"} {
		manifest := `{"schemaVersion":2,"i":` + string(rune('0'+i)) + `}`
		mustPutImage(t, h, "lc", tag, manifest)
	}

	policy := `{"rules":[{"rulePriority":1,"action":{"type":"expire"},` +
		`"selection":{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":2}}]}`

	rec := doAccuracy(t, h, "PutLifecyclePolicy", map[string]any{
		"repositoryName":      "lc",
		"lifecyclePolicyText": policy,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec = doAccuracy(t, h, "DescribeImages", map[string]any{"repositoryName": "lc"})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	out := parseAccuracy(t, rec)
	details, _ := out["imageDetails"].([]any)
	assert.Len(t, details, 2, "lifecycle policy must expire images down to countNumber via DescribeImages")
}
