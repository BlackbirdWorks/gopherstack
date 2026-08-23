package s3_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// setupStorageClassTransitionRace creates a bucket of numObjects backdated
// objects and an Enabled lifecycle rule that transitions their (current)
// storage class to GLACIER after 1 day, then returns the backend, the
// object keys, and a janitor ready to sweep.
func setupStorageClassTransitionRace(
	t *testing.T,
	bucket string,
	numObjects int,
) (*s3.InMemoryBackend, []string, *s3.Janitor) {
	t.Helper()

	b := s3.NewInMemoryBackend(nil)

	mustCreateBucket(t, b, bucket)

	keys := make([]string, numObjects)
	for i := range numObjects {
		keys[i] = fmt.Sprintf("obj-%03d.txt", i)
		mustPutObject(t, b, bucket, keys[i], []byte("v1"))
		s3.BackdateObjectForTest(b, bucket, keys[i], time.Now().Add(-48*time.Hour))
	}

	lcXML := `<LifecycleConfiguration>
<Rule>
  <ID>cur-to-glacier</ID>
  <Status>Enabled</Status>
  <Filter><Prefix></Prefix></Filter>
  <Transition>
    <Days>1</Days>
    <StorageClass>GLACIER</StorageClass>
  </Transition>
</Rule>
</LifecycleConfiguration>`
	require.NoError(t, b.PutBucketLifecycleConfiguration(t.Context(), bucket, lcXML))

	return b, keys, s3.NewJanitor(b, s3.Settings{})
}

// TestGetObject_RacesWithStorageClassTransition reproduces a live-pointer
// escape distinct from the already-fixed gopherstack-2egy
// (version_snapshot_race_test.go, ListObjectVersions vs. the *noncurrent*
// transition sweep). GetObject's resolveObjectVersion returns the live
// *StoredObjectVersion stored in obj.Versions; the caller only holds
// obj.mu.RLock while extracting a few scalar fields into locals, then reads
// ver.StorageClass (via buildGetObjectOutput) *after* obj.mu.RUnlock has
// already fired. The lifecycle janitor's applyStorageClassTransitions
// (current-version transition, not noncurrent) writes ver.StorageClass on
// that same object under obj.mu.Lock, racing the unsynchronized read.
func TestGetObject_RacesWithStorageClassTransition(t *testing.T) {
	t.Parallel()

	const numSweepers = 4
	const numReaders = 8

	bucket := "race-cur-tr-bucket"
	b, keys, janitor := setupStorageClassTransitionRace(t, bucket, 512)

	var wg sync.WaitGroup
	wg.Add(numSweepers + numReaders)

	for range numSweepers {
		go func() {
			defer wg.Done()

			for range 20 {
				janitor.SweepOnce(t.Context())
			}
		}()
	}

	for range numReaders {
		go func() {
			defer wg.Done()

			for range 20 {
				for _, key := range keys {
					if _, gerr := b.GetObject(t.Context(), &sdk_s3.GetObjectInput{
						Bucket: aws.String(bucket),
						Key:    aws.String(key),
					}); gerr != nil {
						t.Error(gerr)

						return
					}
				}
			}
		}()
	}

	wg.Wait()
}

// TestListObjects_RacesWithStorageClassTransition reproduces a third instance
// of the same live-pointer escape. ListObjects (V1) resolves each object's
// latest version via snapshotLatestVersions, which reads it under
// obj.mu.RLock but returns the bare *StoredObjectVersion by design (see its
// doc comment) so callers can sort/truncate before paying for wire-struct
// allocation. objectFromVersion then reads latest.StorageClass well after
// that lock released -- unlike its sibling ListObjectVersions, whose
// snapshotVersions (already fixed for gopherstack-2egy) copies every field
// into a value-typed versionSnapshot before unlocking.
func TestListObjects_RacesWithStorageClassTransition(t *testing.T) {
	t.Parallel()

	const numSweepers = 4
	const numReaders = 8

	bucket := "race-cur-tr-bucket-list"
	b, _, janitor := setupStorageClassTransitionRace(t, bucket, 512)

	var wg sync.WaitGroup
	wg.Add(numSweepers + numReaders)

	for range numSweepers {
		go func() {
			defer wg.Done()

			for range 20 {
				janitor.SweepOnce(t.Context())
			}
		}()
	}

	for range numReaders {
		go func() {
			defer wg.Done()

			for range 20 {
				if _, lerr := b.ListObjects(t.Context(), &sdk_s3.ListObjectsInput{
					Bucket: aws.String(bucket),
				}); lerr != nil {
					t.Error(lerr)

					return
				}
			}
		}()
	}

	wg.Wait()
}

// TestHeadObject_RacesWithStorageClassTransition is HeadObject's counterpart
// to TestGetObject_RacesWithStorageClassTransition: HeadObject also resolves
// to the live *StoredObjectVersion inside an obj.mu.RLock closure, then reads
// ver.StorageClass (among other fields) after the closure -- and its
// RUnlock -- has already returned.
func TestHeadObject_RacesWithStorageClassTransition(t *testing.T) {
	t.Parallel()

	const numSweepers = 4
	const numReaders = 8

	bucket := "race-cur-tr-bucket-head"
	b, keys, janitor := setupStorageClassTransitionRace(t, bucket, 512)

	var wg sync.WaitGroup
	wg.Add(numSweepers + numReaders)

	for range numSweepers {
		go func() {
			defer wg.Done()

			for range 20 {
				janitor.SweepOnce(t.Context())
			}
		}()
	}

	for range numReaders {
		go func() {
			defer wg.Done()

			for range 20 {
				for _, key := range keys {
					if _, herr := b.HeadObject(t.Context(), &sdk_s3.HeadObjectInput{
						Bucket: aws.String(bucket),
						Key:    aws.String(key),
					}); herr != nil {
						t.Error(herr)

						return
					}
				}
			}
		}()
	}

	wg.Wait()
}
