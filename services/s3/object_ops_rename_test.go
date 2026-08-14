package s3_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// TestRenameObject_ConcurrentGetOnExistingTarget_NoRace is a regression test
// for a real data race in InMemoryBackend.RenameObject (objects.go): when the
// rename target key already has an object ("existing"), the old code wrote
// existing.LatestVersionID and existing.Versions[...] while holding only
// bucket.mu and srcObj.mu -- never existing.mu. A concurrent GetObject/
// HeadObject on the target key only takes bucket.mu briefly to look up the
// object pointer, then releases it and reads existing.Versions/
// LatestVersionID under existing.mu.RLock alone, so the two goroutines shared
// no common lock during the actual read/write. -race confirmed this as a live
// data race (unsynchronized map write in RenameObject racing a concurrent
// GetObject's map read in findLatestVersion), not just a theoretical gap.
// Fixed by taking existing.mu.Lock() around the mutation.
func TestRenameObject_ConcurrentGetOnExistingTarget_NoRace(t *testing.T) {
	t.Parallel()

	_, backend := newTestHandler(t)
	bucket := "rename-race-bucket"
	mustCreateBucket(t, backend, bucket)

	ctx := context.Background()

	_, err := backend.PutObject(ctx, &sdk_s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String("dst"), Body: strings.NewReader("d"),
	})
	if err != nil {
		t.Fatalf("seed PutObject: %v", err)
	}

	deadline := time.Now().Add(300 * time.Millisecond)
	var wg sync.WaitGroup
	var renames, reads int64

	wg.Add(2)
	go func() {
		defer wg.Done()
		for time.Now().Before(deadline) {
			if _, putErr := backend.PutObject(ctx, &sdk_s3.PutObjectInput{
				Bucket: aws.String(bucket), Key: aws.String("src"), Body: strings.NewReader("s"),
			}); putErr != nil {
				continue
			}
			if renameErr := backend.RenameObject(ctx, bucket, "src", "dst"); renameErr == nil {
				atomic.AddInt64(&renames, 1)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for time.Now().Before(deadline) {
			out, getErr := backend.GetObject(ctx, &sdk_s3.GetObjectInput{
				Bucket: aws.String(bucket), Key: aws.String("dst"),
			})
			if getErr == nil {
				_ = out.Body.Close()
				atomic.AddInt64(&reads, 1)
			}
		}
	}()
	wg.Wait()

	// The interesting assertion is the absence of a -race report (the test
	// harness fails the run on one); these just confirm real contention happened.
	if renames == 0 || reads == 0 {
		t.Fatalf("test did not generate contention: renames=%d reads=%d", renames, reads)
	}
}
