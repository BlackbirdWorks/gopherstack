package s3_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	sdk_s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// TestListObjectVersions_RacesWithNoncurrentStorageClassTransition reproduces
// gopherstack-2egy: snapshotVersions (via ListObjectVersions) read
// ver.StorageClass under only bucket.mu.RLock, while the janitor's
// applyNoncurrentStorageClassTransitions writes it under obj.mu. Run with
// -race; many objects widen the sweep's write window against a concurrent
// reader loop.
func TestListObjectVersions_RacesWithNoncurrentStorageClassTransition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "concurrent_list_and_noncurrent_transition_sweep"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3.NewInMemoryBackend(nil)
			const bucket = "race-nv-tr-bucket"

			mustCreateBucket(t, b, bucket)

			_, err := b.PutBucketVersioning(t.Context(), &sdk_s3.PutBucketVersioningInput{
				Bucket: aws.String(bucket),
				VersioningConfiguration: &sdk_s3types.VersioningConfiguration{
					Status: sdk_s3types.BucketVersioningStatusEnabled,
				},
			})
			require.NoError(t, err)

			const numObjects = 128
			for i := range numObjects {
				key := fmt.Sprintf("obj-%03d.txt", i)
				mustPutObject(t, b, bucket, key, []byte("v1"))
				mustPutObject(t, b, bucket, key, []byte("v2"))
				s3.BackdateObjectForTest(b, bucket, key, time.Now().Add(-48*time.Hour))
			}

			lcXML := `<LifecycleConfiguration>
<Rule>
  <ID>nv-to-glacier</ID>
  <Status>Enabled</Status>
  <Filter><Prefix></Prefix></Filter>
  <NoncurrentVersionTransition>
    <NoncurrentDays>1</NoncurrentDays>
    <StorageClass>GLACIER</StorageClass>
  </NoncurrentVersionTransition>
</Rule>
</LifecycleConfiguration>`
			require.NoError(t, b.PutBucketLifecycleConfiguration(t.Context(), bucket, lcXML))

			janitor := s3.NewJanitor(b, s3.Settings{})

			var wg sync.WaitGroup

			var listErr error

			wg.Add(2)

			go func() {
				defer wg.Done()

				for range 10 {
					janitor.SweepOnce(t.Context())
				}
			}()

			go func() {
				defer wg.Done()

				for range 50 {
					if _, lerr := b.ListObjectVersions(t.Context(), &sdk_s3.ListObjectVersionsInput{
						Bucket: aws.String(bucket),
					}); lerr != nil {
						listErr = lerr
					}
				}
			}()

			wg.Wait()
			require.NoError(t, listErr)
		})
	}
}
