package ecr_test

// lifecycle_archive_pulled_transitioned_test.go -- covers three documented
// ECR lifecycle-policy evaluation behaviors this evaluator silently ignored.
// Behaviour source: docs.aws.amazon.com/AmazonECR/latest/userguide/
// LifecyclePolicies.html ("Lifecycle policy evaluation rules") for the
// per-image semantics, and lifecycle_policy_examples.html for the actual
// action wire shape -- action.type is "expire"|"transition" (never
// "archive"; ImageActionType has no such value -- aws-sdk-go-v2/service/ecr
// types/enums.go:101), with a sibling "targetStorageClass":"archive" field
// present only when type=="transition" (types.LifecyclePolicyRuleAction /
// types.LifecyclePolicyTargetStorageClass, enums.go:325).
//
//  1. Rules whose action.type is "transition" (not just "expire") were
//     skipped entirely by evaluateLifecyclePolicy's top-level loop, so an
//     archive-transition rule matched nothing and transitioned nothing.
//  2. countType "sinceImagePulled" ("all images whose last_recorded_pulltime
//     is older than the specified number of days ... are archived. If an
//     image was never pulled, the image's pushed_at_time is used instead...
//     If ... never pulled since [a restore], the image's last_activated_at
//     is used instead") had no case in applyRule's switch, so it matched
//     nothing (fell to the trailing "return nil").
//  3. countType "sinceImageTransitioned" ("all archived images whose
//     last_archived_at is older than the specified number of days ... are
//     expired") likewise had no case.
//
// All three are implementable without guessing: the backend already stamps
// LastRecordedPullTime/LastActivatedAt/LastArchivedAt/StorageClass/
// ImageStatus (UpdateImageStorageClass, BatchGetImage, GetDownloadUrlForLayer)
// -- the count types just never consulted them.

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

func archivePolicy(sel string) string {
	return `{"rules":[{"rulePriority":1,"action":{"type":"transition","targetStorageClass":"archive"},` +
		`"selection":` + sel + `}]}`
}

// TestLifecycle_ArchiveAction_TransitionsStorageClass drives a
// transition/targetStorageClass=archive action rule (imageCountMoreThan:0,
// i.e. archive every matching image) and asserts the image survives
// (archiving is not deletion) but transitions to StorageClass=ARCHIVE /
// ImageStatus=ARCHIVED with a stamped LastArchivedAt -- the same transition
// UpdateImageStorageClass performs directly.
func TestLifecycle_ArchiveAction_TransitionsStorageClass(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.CreateRepoInternal("lc-archive")
	seedImage(b, "lc-archive", "sha256:a1", "v1", time.Now())

	_, err := b.PutLifecyclePolicy(context.Background(), "lc-archive",
		archivePolicy(`{"tagStatus":"any","countType":"imageCountMoreThan","countNumber":0}`))
	require.NoError(t, err)

	assert.True(t, b.HasImageDigest("lc-archive", "sha256:a1"), "archive must not delete the image")

	imgs, err := b.DescribeImages(context.Background(), "lc-archive", nil)
	require.NoError(t, err)
	require.Len(t, imgs, 1)
	assert.Equal(t, "ARCHIVE", imgs[0].StorageClass)
	assert.Equal(t, "ARCHIVED", imgs[0].ImageStatus)
	assert.False(t, imgs[0].LastArchivedAt.IsZero(), "LastArchivedAt must be stamped")
}

// TestLifecycle_SinceImagePulled_FallbackChain covers the three-way fallback
// documented on countType=sinceImagePulled: LastRecordedPullTime when present
// and not stale relative to a restore, else LastActivatedAt (restored but
// never pulled since), else ImagePushedAt (never pulled at all).
func TestLifecycle_SinceImagePulled_FallbackChain(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.CreateRepoInternal("lc-pulled")
	now := time.Now()

	// Pulled recently (5d ago) despite an old push (60d ago): must survive a
	// 30-day threshold via LastRecordedPullTime.
	b.AddImageInternal("lc-pulled", ecr.Image{
		ImageDigest:          "sha256:recent-pull",
		ImageManifest:        `{"schemaVersion":2,"d":"recent-pull"}`,
		ImageID:              ecr.ImageIdentifier{ImageDigest: "sha256:recent-pull", ImageTag: "a"},
		ImagePushedAt:        now.AddDate(0, 0, -60),
		LastRecordedPullTime: now.AddDate(0, 0, -5),
	})

	// Never pulled, pushed 60 days ago: must expire via the ImagePushedAt fallback.
	b.AddImageInternal("lc-pulled", ecr.Image{
		ImageDigest:   "sha256:never-pulled-old",
		ImageManifest: `{"schemaVersion":2,"d":"never-pulled-old"}`,
		ImageID:       ecr.ImageIdentifier{ImageDigest: "sha256:never-pulled-old", ImageTag: "b"},
		ImagePushedAt: now.AddDate(0, 0, -60),
	})

	// Restored 60 days ago and never pulled since (LastRecordedPullTime is a
	// stale pre-restore pull from 90 days ago): must expire via the
	// LastActivatedAt fallback, NOT survive on the stale pull record.
	b.AddImageInternal("lc-pulled", ecr.Image{
		ImageDigest:          "sha256:restored-not-repulled",
		ImageManifest:        `{"schemaVersion":2,"d":"restored-not-repulled"}`,
		ImageID:              ecr.ImageIdentifier{ImageDigest: "sha256:restored-not-repulled", ImageTag: "c"},
		ImagePushedAt:        now.AddDate(0, 0, -120),
		LastRecordedPullTime: now.AddDate(0, 0, -90),
		LastActivatedAt:      now.AddDate(0, 0, -60),
	})

	policy := archivePolicy(`{"tagStatus":"any","countType":"sinceImagePulled","countUnit":"days","countNumber":30}`)

	preview, err := b.StartLifecyclePolicyPreview(context.Background(), "lc-pulled", policy)
	require.NoError(t, err)

	matched := make(map[string]bool)
	for _, e := range preview.PreviewResults {
		matched[e.ImageDigest] = true
	}

	assert.False(t, matched["sha256:recent-pull"], "recently pulled image must survive")
	assert.True(t, matched["sha256:never-pulled-old"], "never-pulled old image must expire via pushedAt fallback")
	assert.True(t, matched["sha256:restored-not-repulled"],
		"restored-and-not-repulled image must expire via lastActivatedAt fallback, not its stale pull record")
}

// TestLifecycle_SinceImageTransitioned_OnlyArchivedImages covers countType=
// sinceImageTransitioned: only already-archived images are candidates, and
// the threshold is against LastArchivedAt, not ImagePushedAt.
func TestLifecycle_SinceImageTransitioned_OnlyArchivedImages(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.CreateRepoInternal("lc-transitioned")
	now := time.Now()

	// Archived 60 days ago (but pushed only 1 day ago -- proves the threshold
	// uses LastArchivedAt, not ImagePushedAt): must expire at a 30-day threshold.
	b.AddImageInternal("lc-transitioned", ecr.Image{
		ImageDigest:    "sha256:archived-old",
		ImageManifest:  `{"schemaVersion":2,"d":"archived-old"}`,
		ImageID:        ecr.ImageIdentifier{ImageDigest: "sha256:archived-old", ImageTag: "a"},
		ImagePushedAt:  now.AddDate(0, 0, -1),
		ImageStatus:    "ARCHIVED",
		StorageClass:   "ARCHIVE",
		LastArchivedAt: now.AddDate(0, 0, -60),
	})

	// Never archived (still STANDARD/ACTIVE) but pushed a long time ago: must
	// NOT match -- sinceImageTransitioned only considers archived images.
	b.AddImageInternal("lc-transitioned", ecr.Image{
		ImageDigest:   "sha256:never-archived",
		ImageManifest: `{"schemaVersion":2,"d":"never-archived"}`,
		ImageID:       ecr.ImageIdentifier{ImageDigest: "sha256:never-archived", ImageTag: "b"},
		ImagePushedAt: now.AddDate(0, 0, -90),
	})

	_, err := b.PutLifecyclePolicy(context.Background(), "lc-transitioned",
		expirePolicy(`{"tagStatus":"any","countType":"sinceImageTransitioned","countUnit":"days","countNumber":30}`))
	require.NoError(t, err)

	assert.False(t, b.HasImageDigest("lc-transitioned", "sha256:archived-old"),
		"image archived past the threshold must be expired (deleted)")
	assert.True(t, b.HasImageDigest("lc-transitioned", "sha256:never-archived"),
		"a never-archived image must not match sinceImageTransitioned regardless of age")
}

// TestLifecycle_Selection_StorageClass_GeneralFilter covers "storageClass"
// as a general selection filter (a sibling of tagStatus in the policy
// template, not exclusive to sinceImageTransitioned): a rule scoped to
// storageClass="standard" must not touch already-archived images even under
// an unrelated countType like imageCountMoreThan.
func TestLifecycle_Selection_StorageClass_GeneralFilter(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	b.CreateRepoInternal("lc-storageclass")
	now := time.Now()

	b.AddImageInternal("lc-storageclass", ecr.Image{
		ImageDigest:    "sha256:archived",
		ImageManifest:  `{"schemaVersion":2,"d":"archived"}`,
		ImageID:        ecr.ImageIdentifier{ImageDigest: "sha256:archived", ImageTag: "a"},
		ImagePushedAt:  now.AddDate(0, 0, -5),
		ImageStatus:    "ARCHIVED",
		StorageClass:   "ARCHIVE",
		LastArchivedAt: now.AddDate(0, 0, -5),
	})
	b.AddImageInternal("lc-storageclass", ecr.Image{
		ImageDigest:   "sha256:standard-old",
		ImageManifest: `{"schemaVersion":2,"d":"standard-old"}`,
		ImageID:       ecr.ImageIdentifier{ImageDigest: "sha256:standard-old", ImageTag: "b"},
		ImagePushedAt: now.AddDate(0, 0, -10),
	})
	b.AddImageInternal("lc-storageclass", ecr.Image{
		ImageDigest:   "sha256:standard-new",
		ImageManifest: `{"schemaVersion":2,"d":"standard-new"}`,
		ImageID:       ecr.ImageIdentifier{ImageDigest: "sha256:standard-new", ImageTag: "c"},
		ImagePushedAt: now,
	})

	_, err := b.PutLifecyclePolicy(context.Background(), "lc-storageclass",
		expirePolicy(`{"tagStatus":"any","storageClass":"standard","countType":"imageCountMoreThan","countNumber":1}`))
	require.NoError(t, err)

	assert.True(t, b.HasImageDigest("lc-storageclass", "sha256:archived"),
		"storageClass=standard selection must not touch an already-archived image")
	assert.False(t, b.HasImageDigest("lc-storageclass", "sha256:standard-old"),
		"the older of the two standard images must still expire under imageCountMoreThan:1")
	assert.True(t, b.HasImageDigest("lc-storageclass", "sha256:standard-new"),
		"the newest standard image survives imageCountMoreThan:1")
}
