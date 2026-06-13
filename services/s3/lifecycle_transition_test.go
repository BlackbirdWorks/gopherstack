package s3_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	sdk_s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// TestS3Lifecycle_StorageClassTransitions verifies that the janitor applies
// storage class transitions from lifecycle rules to matching objects.
func TestS3Lifecycle_StorageClassTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, b *s3.InMemoryBackend)
		verify    func(t *testing.T, b *s3.InMemoryBackend)
		name      string
		bucket    string
		lcXML     string
		wantClass string
	}{
		{
			name:   "days_based_transition_updates_storage_class",
			bucket: "tr-days",
			lcXML: `<LifecycleConfiguration>
<Rule>
  <ID>move-to-glacier</ID>
  <Status>Enabled</Status>
  <Filter><Prefix></Prefix></Filter>
  <Transition><Days>30</Days><StorageClass>GLACIER</StorageClass></Transition>
</Rule>
</LifecycleConfiguration>`,
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "tr-days")
				mustPutObject(t, b, "tr-days", "old-obj.txt", []byte("data"))
				// Backdate to 31 days ago so the 30-day rule fires.
				s3.BackdateObjectForTest(
					b,
					"tr-days",
					"old-obj.txt",
					time.Now().Add(-31*24*time.Hour),
				)
			},
			wantClass: "GLACIER",
			verify: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				require.Eventually(t, func() bool {
					out, err := b.HeadObject(t.Context(), &sdk_s3.HeadObjectInput{
						Bucket: aws.String("tr-days"),
						Key:    aws.String("old-obj.txt"),
					})

					return err == nil && string(out.StorageClass) == "GLACIER"
				}, 500*time.Millisecond, 10*time.Millisecond, "object must be transitioned to GLACIER")
			},
		},
		{
			name:   "days_based_transition_records_history",
			bucket: "tr-hist",
			lcXML: `<LifecycleConfiguration>
<Rule>
  <ID>archive-rule</ID>
  <Status>Enabled</Status>
  <Filter><Prefix></Prefix></Filter>
  <Transition><Days>10</Days><StorageClass>STANDARD_IA</StorageClass></Transition>
</Rule>
</LifecycleConfiguration>`,
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "tr-hist")
				mustPutObject(t, b, "tr-hist", "doc.txt", []byte("data"))
				s3.BackdateObjectForTest(b, "tr-hist", "doc.txt", time.Now().Add(-11*24*time.Hour))
			},
			wantClass: "STANDARD_IA",
			verify: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				// Wait for transition to fire.
				require.Eventually(t, func() bool {
					history := s3.StorageClassTransitionsForObject(b, "tr-hist", "doc.txt")

					return len(history) >= 1
				}, 500*time.Millisecond, 10*time.Millisecond, "transition history must be recorded")

				history := s3.StorageClassTransitionsForObject(b, "tr-hist", "doc.txt")
				require.Len(t, history, 1)
				assert.Equal(t, "STANDARD", history[0].FromClass)
				assert.Equal(t, "STANDARD_IA", history[0].ToClass)
				assert.Equal(t, "archive-rule", history[0].RuleID)
				assert.False(t, history[0].TransitionedAt.IsZero())
			},
		},
		{
			name:   "date_based_transition_fires_for_past_date",
			bucket: "tr-date",
			lcXML: `<LifecycleConfiguration>
<Rule>
  <ID>past-date-rule</ID>
  <Status>Enabled</Status>
  <Filter><Prefix></Prefix></Filter>
  <Transition><Date>2020-01-01T00:00:00Z</Date><StorageClass>DEEP_ARCHIVE</StorageClass></Transition>
</Rule>
</LifecycleConfiguration>`,
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "tr-date")
				mustPutObject(t, b, "tr-date", "archive.bin", []byte("payload"))
			},
			wantClass: "DEEP_ARCHIVE",
			verify: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				require.Eventually(t, func() bool {
					out, err := b.HeadObject(t.Context(), &sdk_s3.HeadObjectInput{
						Bucket: aws.String("tr-date"),
						Key:    aws.String("archive.bin"),
					})

					return err == nil && string(out.StorageClass) == "DEEP_ARCHIVE"
				}, 500*time.Millisecond, 10*time.Millisecond, "object must be transitioned to DEEP_ARCHIVE")
			},
		},
		{
			name:   "transition_not_duplicate_when_already_target_class",
			bucket: "tr-idem",
			lcXML: `<LifecycleConfiguration>
<Rule>
  <ID>no-dup-rule</ID>
  <Status>Enabled</Status>
  <Filter><Prefix></Prefix></Filter>
  <Transition><Days>1</Days><StorageClass>GLACIER</StorageClass></Transition>
</Rule>
</LifecycleConfiguration>`,
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "tr-idem")
				mustPutObject(t, b, "tr-idem", "obj.txt", []byte("data"))
				s3.BackdateObjectForTest(b, "tr-idem", "obj.txt", time.Now().Add(-2*24*time.Hour))
			},
			wantClass: "GLACIER",
			verify: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				// Let janitor run two ticks — transition must appear only once.
				time.Sleep(30 * time.Millisecond)

				history := s3.StorageClassTransitionsForObject(b, "tr-idem", "obj.txt")
				// Must have been transitioned at least once; no duplicates.
				assert.NotEmpty(t, history)
				assert.Equal(t, "GLACIER", history[len(history)-1].ToClass)

				// All history entries must be STANDARD → GLACIER (no duplicate from→GLACIER).
				for _, h := range history {
					assert.Equal(t, "GLACIER", h.ToClass)
					assert.NotEqual(
						t,
						"GLACIER",
						h.FromClass,
						"must not record GLACIER→GLACIER no-op",
					)
				}
			},
		},
		{
			name:   "disabled_rule_does_not_transition",
			bucket: "tr-disabled",
			lcXML: `<LifecycleConfiguration>
<Rule>
  <ID>disabled-tr</ID>
  <Status>Disabled</Status>
  <Filter><Prefix></Prefix></Filter>
  <Transition><Days>1</Days><StorageClass>GLACIER</StorageClass></Transition>
</Rule>
</LifecycleConfiguration>`,
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "tr-disabled")
				mustPutObject(t, b, "tr-disabled", "file.txt", []byte("data"))
				s3.BackdateObjectForTest(
					b,
					"tr-disabled",
					"file.txt",
					time.Now().Add(-2*24*time.Hour),
				)
			},
			verify: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				time.Sleep(50 * time.Millisecond)

				out, err := b.HeadObject(t.Context(), &sdk_s3.HeadObjectInput{
					Bucket: aws.String("tr-disabled"),
					Key:    aws.String("file.txt"),
				})
				require.NoError(t, err)
				assert.NotEqual(
					t,
					"GLACIER",
					string(out.StorageClass),
					"disabled rule must not transition",
				)

				history := s3.StorageClassTransitionsForObject(b, "tr-disabled", "file.txt")
				assert.Empty(t, history, "no transition history for disabled rule")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3.NewInMemoryBackend(nil)
			tt.setup(t, b)

			err := b.PutBucketLifecycleConfiguration(t.Context(), tt.bucket, tt.lcXML)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			go newFastJanitor(b).Run(ctx)

			tt.verify(t, b)
		})
	}
}

// TestS3Lifecycle_NoncurrentVersionTransitions verifies that lifecycle rules
// transition noncurrent object versions to a different storage class.
func TestS3Lifecycle_NoncurrentVersionTransitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		bucket string
		lcXML  string
	}{
		{
			name:   "noncurrent_version_transitions_to_glacier",
			bucket: "nv-tr-bucket",
			lcXML: `<LifecycleConfiguration>
<Rule>
  <ID>nv-to-glacier</ID>
  <Status>Enabled</Status>
  <Filter><Prefix></Prefix></Filter>
  <NoncurrentVersionTransition>
    <NoncurrentDays>1</NoncurrentDays>
    <StorageClass>GLACIER</StorageClass>
  </NoncurrentVersionTransition>
</Rule>
</LifecycleConfiguration>`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3.NewInMemoryBackend(nil)
			mustCreateBucket(t, b, tt.bucket)

			// Enable versioning so we get noncurrent versions.
			_, err := b.PutBucketVersioning(t.Context(), &sdk_s3.PutBucketVersioningInput{
				Bucket: aws.String(tt.bucket),
				VersioningConfiguration: &sdk_s3types.VersioningConfiguration{
					Status: sdk_s3types.BucketVersioningStatusEnabled,
				},
			})
			require.NoError(t, err)

			// Write two versions: v1 becomes noncurrent, v2 is latest.
			mustPutObject(t, b, tt.bucket, "obj.txt", []byte("v1"))
			mustPutObject(t, b, tt.bucket, "obj.txt", []byte("v2"))

			// Backdate v1 (noncurrent) to be older than NoncurrentDays.
			// We need to backdate the noncurrent version specifically.
			// BackdateObjectForTest backdates ALL versions — sufficient here since
			// the rule only acts on non-latest and we just need them old.
			s3.BackdateObjectForTest(b, tt.bucket, "obj.txt", time.Now().Add(-2*24*time.Hour))

			err = b.PutBucketLifecycleConfiguration(t.Context(), tt.bucket, tt.lcXML)
			require.NoError(t, err)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			go newFastJanitor(b).Run(ctx)

			// Wait for noncurrent version to be transitioned.
			require.Eventually(t, func() bool {
				out, listErr := b.ListObjectVersions(t.Context(), &sdk_s3.ListObjectVersionsInput{
					Bucket: aws.String(tt.bucket),
				})
				if listErr != nil {
					return false
				}
				for _, ver := range out.Versions {
					if !aws.ToBool(ver.IsLatest) && string(ver.StorageClass) == "GLACIER" {
						return true
					}
				}

				return false
			}, 500*time.Millisecond, 10*time.Millisecond, "noncurrent version must be transitioned to GLACIER")

			// Latest version must still be STANDARD.
			out, err := b.ListObjectVersions(t.Context(), &sdk_s3.ListObjectVersionsInput{
				Bucket: aws.String(tt.bucket),
			})
			require.NoError(t, err)

			for _, ver := range out.Versions {
				if aws.ToBool(ver.IsLatest) {
					assert.NotEqual(t, "GLACIER", string(ver.StorageClass),
						"latest version must not be transitioned")
				}
			}
		})
	}
}
