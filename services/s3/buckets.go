package s3

import (
	"context"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func (b *InMemoryBackend) CreateBucket(
	ctx context.Context,
	input *s3.CreateBucketInput,
) (*s3.CreateBucketOutput, error) {
	// Prefer the LocationConstraint from the input (set by the SDK for non-us-east-1
	// regions) over the region extracted from the context, so the bucket is stored
	// in the region the caller actually requested.
	region := getRegionFromS3Context(ctx, b.defaultRegion)
	if input.CreateBucketConfiguration != nil &&
		input.CreateBucketConfiguration.LocationConstraint != "" {
		region = string(input.CreateBucketConfiguration.LocationConstraint)
	}

	b.mu.Lock("CreateBucket")
	defer b.mu.Unlock()

	bucketName := *input.Bucket

	// Use b.buckets for O(1) global-uniqueness check. Since this is a
	// single-tenant mock, a pre-existing bucket is always owned by the
	// caller → return BucketAlreadyOwnedByYou (not BucketAlreadyExists).
	// Pending-delete buckets remain in the table and still block re-creation,
	// matching real S3 behaviour (you must wait for deletion to complete).
	if b.buckets.Has(bucketName) {
		return nil, ErrBucketAlreadyOwnedByYou
	}

	b.buckets.Put(&StoredBucket{
		Name:         bucketName,
		Region:       region,
		CreationDate: time.Now().UTC(),
		Objects:      make(map[string]*StoredObject),
		// Versioning is intentionally not set: new buckets have never had versioning
		// configured, which AWS represents as an empty VersioningConfiguration element.
		mu:                        lockmetrics.New("s3.bucket." + bucketName),
		AnalyticsConfigs:          make(map[string]string),
		IntelligentTieringConfigs: make(map[string]string),
		InventoryConfigs:          make(map[string]string),
		MetricsConfigs:            make(map[string]string),
		// S3 Express directory buckets use the naming convention {name}--{az-id}--x-s3.
		// Detect this at creation time so ListBuckets and ListDirectoryBuckets can
		// correctly partition general-purpose vs. directory buckets.
		IsDirectoryBucket: strings.HasSuffix(bucketName, "--x-s3"),
	})

	return &s3.CreateBucketOutput{
		Location: aws.String("/" + bucketName),
	}, nil
}

func (b *InMemoryBackend) DeleteBucket(
	_ context.Context,
	input *s3.DeleteBucketInput,
) (*s3.DeleteBucketOutput, error) {
	bucketName := *input.Bucket

	b.mu.Lock("DeleteBucket")
	defer b.mu.Unlock()

	// Use b.buckets for O(1) lookup. Pending buckets remain in the table so
	// that idempotent deletes can be detected without a region scan.
	bucket, ok := b.buckets.Get(bucketName)
	if !ok {
		return nil, ErrNoSuchBucket
	}

	// If already pending deletion, return success (idempotent).
	if bucket.DeletePending {
		return &s3.DeleteBucketOutput{}, nil
	}

	// Real S3 refuses to delete a bucket that still has content: any object,
	// object version, or delete marker (409 BucketNotEmpty) — the caller must
	// empty the bucket first. b.mu is held exclusively for the whole check, so
	// no concurrent PutObject/CompleteMultipartUpload/etc. that has already
	// passed its own b.mu lookup can race in a new object between the check
	// and marking the bucket pending.
	bucket.mu.RLock("DeleteBucket")
	hasObjects := len(bucket.Objects) > 0
	bucket.mu.RUnlock()

	if hasObjects {
		return nil, ErrBucketNotEmpty
	}

	// In-progress (incomplete) multipart uploads also block deletion in real
	// S3, even though they never appear in ListObjects/ListObjectsV2 — a
	// well-documented gotcha where DeleteBucket fails with BucketNotEmpty
	// despite the bucket looking empty until the uploads are aborted.
	if len(b.uploadsByBucket.Get(bucketName)) > 0 {
		return nil, ErrBucketNotEmpty
	}

	// Mark bucket as pending — the Janitor will remove it (it is already
	// empty, so this completes on the next sweep tick without draining).
	bucket.DeletePending = true

	return &s3.DeleteBucketOutput{}, nil
}

func (b *InMemoryBackend) HeadBucket(
	_ context.Context,
	input *s3.HeadBucketInput,
) (*s3.HeadBucketOutput, error) {
	b.mu.RLock("HeadBucket")
	defer b.mu.RUnlock()

	if _, err := b.getBucket(*input.Bucket); err != nil {
		return nil, err
	}

	return &s3.HeadBucketOutput{}, nil
}

func (b *InMemoryBackend) ListBuckets(
	_ context.Context,
	_ *s3.ListBucketsInput,
) (*s3.ListBucketsOutput, error) {
	// Snapshot bucket data under lock, release immediately.
	var buckets []types.Bucket
	func() {
		b.mu.RLock("ListBuckets")
		defer b.mu.RUnlock()

		all := b.buckets.All()
		buckets = make([]types.Bucket, 0, len(all))
		for _, bucket := range all {
			if bucket.DeletePending || bucket.IsDirectoryBucket {
				continue
			}
			buckets = append(buckets, types.Bucket{
				Name:         aws.String(bucket.Name),
				CreationDate: aws.Time(bucket.CreationDate),
				// Real S3 only echoes BucketRegion on a paginated ListBuckets
				// request; this backend implements no pagination/filtering, so it's
				// always populated, matching the whole point of the field: dashboard
				// visibility into a bucket's real region.
				//
				// Unlike GetBucketLocation's LocationConstraint (EMPTY for
				// us-east-1, a legacy quirk), BucketRegion reports the literal
				// region string, "us-east-1" included -- confirmed against the real
				// ListBuckets doc's paginated examples. No empty-string special-casing.
				BucketRegion: aws.String(bucket.Region),
			})
		}
	}()

	// Sort outside the lock
	sort.Slice(buckets, func(i, j int) bool {
		return *buckets[i].Name < *buckets[j].Name
	})

	return &s3.ListBucketsOutput{
		Buckets: buckets,
		Owner: &types.Owner{
			DisplayName: aws.String("Gopherstack"),
			ID:          aws.String("placeholder-id"),
		},
	}, nil
}

// ListDirectoryBuckets returns all S3 Express directory buckets (name suffix
// --x-s3) owned by the account, excluding general-purpose buckets. Matches
// AWS behaviour where ListBuckets and ListDirectoryBuckets partition the two
// bucket types into separate lists.
func (b *InMemoryBackend) ListDirectoryBuckets(_ context.Context) ([]types.Bucket, error) {
	buckets := make([]types.Bucket, 0)
	func() {
		b.mu.RLock("ListDirectoryBuckets")
		defer b.mu.RUnlock()

		for _, bucket := range b.buckets.All() {
			if bucket.DeletePending || !bucket.IsDirectoryBucket {
				continue
			}

			buckets = append(buckets, types.Bucket{
				Name:         aws.String(bucket.Name),
				CreationDate: aws.Time(bucket.CreationDate),
			})
		}
	}()

	sort.Slice(buckets, func(i, j int) bool {
		return *buckets[i].Name < *buckets[j].Name
	})

	return buckets, nil
}

// Regions returns all distinct regions that contain at least one active bucket.
func (b *InMemoryBackend) Regions() []string {
	b.mu.RLock("Regions")
	defer b.mu.RUnlock()

	seen := make(map[string]struct{})
	var regions []string

	for _, bucket := range b.buckets.All() {
		if bucket.DeletePending {
			continue
		}

		if _, ok := seen[bucket.Region]; ok {
			continue
		}

		seen[bucket.Region] = struct{}{}
		regions = append(regions, bucket.Region)
	}

	sort.Strings(regions)

	return regions
}

// BucketsByRegion returns a snapshot of all Bucket SDK objects in the given region.
// Returns all buckets (cross-region) if region is empty.
func (b *InMemoryBackend) BucketsByRegion(region string) []types.Bucket {
	b.mu.RLock("BucketsByRegion")
	defer b.mu.RUnlock()

	var buckets []types.Bucket

	for _, bucket := range b.buckets.All() {
		if region != "" && bucket.Region != region {
			continue
		}

		if bucket.DeletePending {
			continue
		}

		buckets = append(buckets, types.Bucket{
			Name:         aws.String(bucket.Name),
			CreationDate: aws.Time(bucket.CreationDate),
		})
	}

	sort.Slice(buckets, func(i, j int) bool {
		return *buckets[i].Name < *buckets[j].Name
	})

	return buckets
}

// CreateSession returns a stub session response for a bucket (S3 Express One Zone).
func (b *InMemoryBackend) CreateSession(_ context.Context, bucketName string) (string, error) {
	var err error
	func() {
		b.mu.RLock("CreateSession")
		defer b.mu.RUnlock()

		_, err = b.getBucket(bucketName)
	}()

	if err != nil {
		return "", err
	}

	const sessionXML = `<CreateSessionResponse xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		`<Credentials>` +
		`<SessionToken>gopherstack-mock-session-token</SessionToken>` +
		`<SecretAccessKey>gopherstack-mock-secret</SecretAccessKey>` +
		`<AccessKeyId>gopherstack-mock-access-key</AccessKeyId>` +
		`<Expiration>2099-01-01T00:00:00Z</Expiration>` +
		`</Credentials></CreateSessionResponse>`

	return sessionXML, nil
}

func (b *InMemoryBackend) GetBucketMetadata(
	_ context.Context,
	bucketName string,
) (string, string, []types.Tag, error) {
	var bucket *StoredBucket
	var ok bool
	func() {
		b.mu.RLock("GetBucketMetadata")
		defer b.mu.RUnlock()

		bucket, ok = b.buckets.Get(bucketName)
	}()

	if !ok {
		return "", "", nil, ErrNoSuchBucket
	}

	var region, lcXML string
	var tags []types.Tag
	func() {
		bucket.mu.RLock("GetBucketMetadata")
		defer bucket.mu.RUnlock()

		region = bucket.Region
		lcXML = bucket.LifecycleConfig
		tags = slices.Clone(bucket.Tags)
	}()

	return region, lcXML, tags, nil
}
