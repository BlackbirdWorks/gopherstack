package macie2_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/macie2"
)

func TestBucketsAndBatchCustomDataIdentifiers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "describe_buckets_returns_empty",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodPost, "/datasources/s3", map[string]any{
					"criteria": map[string]any{},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				buckets, _ := resp["buckets"].([]any)
				assert.Empty(t, buckets)
			},
		},
		{
			name: "get_bucket_statistics_returns_zeros",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodPost, "/datasources/s3/statistics", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				count, _ := resp["bucketCount"].(float64)
				assert.InDelta(t, 0, count, 0.0001)
			},
		},
		{
			name: "batch_get_custom_data_identifiers",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// Create a CDI first
				createRec := doRequest(t, h, http.MethodPost, "/custom-data-identifiers", map[string]any{
					"name":  "test-cdi",
					"regex": `\d{4}-\d{4}`,
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var createResp map[string]string
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
				cdiID := createResp["customDataIdentifierId"]

				// BatchGetCustomDataIdentifiers
				rec := doRequest(t, h, http.MethodPost, "/custom-data-identifiers/get", map[string]any{
					"ids": []string{cdiID},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var batchResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &batchResp))
				items, _ := batchResp["customDataIdentifiers"].([]any)
				assert.Len(t, items, 1)
			},
		},
		{
			name: "search_resources_returns_empty",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodPost, "/datasources/search-resources", map[string]any{
					"bucketCriteria": map[string]any{},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				results, _ := resp["matchingResources"].([]any)
				assert.Empty(t, results)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}

func newBucketBackend() *macie2.InMemoryBackend {
	return macie2.NewInMemoryBackend("000000000000", "us-east-1")
}

func newBucketHandlerAndBackend(t *testing.T) (*macie2.Handler, *macie2.InMemoryBackend) {
	t.Helper()

	b := newBucketBackend()

	return macie2.NewHandler(b), b
}

func makeBucket(
	name, region, access, enc string,
	objectCount, sizeBytes int64,
) macie2.S3BucketMetadata {
	return macie2.S3BucketMetadata{
		AccountID:               "000000000000",
		BucketArn:               fmt.Sprintf("arn:aws:s3:::%s", name),
		BucketName:              name,
		Region:                  region,
		ClassifiableObjectCount: objectCount,
		ClassifiableSizeInBytes: sizeBytes,
		ObjectCount:             objectCount,
		SizeInBytes:             sizeBytes,
		PublicAccess:            access,
		EncryptionType:          enc,
		SharedAccess:            "NOT_SHARED",
	}
}

func seedBucket(
	t *testing.T,
	b *macie2.InMemoryBackend,
	name, region, access, enc string,
	objectCount, sizeBytes int64,
) {
	t.Helper()
	macie2.SeedS3Bucket(b, makeBucket(name, region, access, enc, objectCount, sizeBytes))
}

func describeBuckets(t *testing.T, h *macie2.Handler, criteria map[string]any) []any {
	t.Helper()

	body := map[string]any{}
	if len(criteria) > 0 {
		body["criteria"] = criteria
	}

	rec := doRequest(t, h, http.MethodPost, "/datasources/s3", body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	buckets, ok := resp["buckets"].([]any)
	if !ok {
		return []any{}
	}

	return buckets
}

func getBucketStatistics(t *testing.T, h *macie2.Handler) map[string]any {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/datasources/s3/statistics", nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

// --- DescribeBuckets: empty state ---

func TestBuckets_DescribeBuckets_Empty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		criteria map[string]any
		name     string
	}{
		{name: "no_criteria", criteria: nil},
		{name: "empty_criteria", criteria: map[string]any{}},
		{name: "name_filter_no_match", criteria: map[string]any{
			"bucketName": map[string]any{"value": "nonexistent"},
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newBucketHandlerAndBackend(t)
			buckets := describeBuckets(t, h, tc.criteria)
			assert.Empty(t, buckets)
		})
	}
}

// --- DescribeBuckets: single bucket round-trip ---

func TestBuckets_DescribeBuckets_SingleRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		bucketName  string
		region      string
		access      string
		enc         string
		objectCount int64
		sizeBytes   int64
	}{
		{
			name:        "public_aes256",
			bucketName:  "my-public-bucket",
			region:      "us-east-1",
			access:      "PUBLIC",
			enc:         "AES256",
			objectCount: 100,
			sizeBytes:   1024 * 1024,
		},
		{
			name:        "not_public_kms",
			bucketName:  "my-private-bucket",
			region:      "us-west-2",
			access:      "NOT_PUBLIC",
			enc:         "aws:kms",
			objectCount: 50,
			sizeBytes:   512 * 1024,
		},
		{
			name:        "empty_bucket",
			bucketName:  "my-empty-bucket",
			region:      "eu-west-1",
			access:      "NOT_PUBLIC",
			enc:         "AES256",
			objectCount: 0,
			sizeBytes:   0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, b := newBucketHandlerAndBackend(t)
			seedBucket(
				t,
				b,
				tc.bucketName,
				tc.region,
				tc.access,
				tc.enc,
				tc.objectCount,
				tc.sizeBytes,
			)

			buckets := describeBuckets(t, h, nil)
			require.Len(t, buckets, 1)

			bkt := buckets[0].(map[string]any)
			assert.Equal(t, fmt.Sprintf("arn:aws:s3:::%s", tc.bucketName), bkt["bucketArn"])
			assert.Equal(t, tc.bucketName, bkt["bucketName"])
			assert.Equal(t, tc.region, bkt["region"])
			assert.Equal(t, "000000000000", bkt["accountId"])
			assert.InDelta(t, float64(tc.objectCount), bkt["objectCount"], 1e-9)
			assert.InDelta(t, float64(tc.sizeBytes), bkt["sizeInBytes"], 1e-9)

			publicAccess, ok := bkt["publicAccess"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tc.access, publicAccess["effectivePermission"])

			enc, ok := bkt["serverSideEncryption"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tc.enc, enc["type"])
		})
	}
}

// --- DescribeBuckets: multiple buckets ---

func TestBuckets_DescribeBuckets_Multiple(t *testing.T) {
	t.Parallel()

	h, b := newBucketHandlerAndBackend(t)

	seedBucket(t, b, "bucket-alpha", "us-east-1", "NOT_PUBLIC", "AES256", 100, 1024)
	seedBucket(t, b, "bucket-beta", "us-west-2", "PUBLIC", "aws:kms", 200, 2048)
	seedBucket(t, b, "bucket-gamma", "eu-west-1", "NOT_PUBLIC", "AES256", 50, 512)

	buckets := describeBuckets(t, h, nil)
	require.Len(t, buckets, 3)

	names := make([]string, 0, 3)
	for _, bkt := range buckets {
		m := bkt.(map[string]any)
		names = append(names, m["bucketName"].(string))
	}

	assert.Contains(t, names, "bucket-alpha")
	assert.Contains(t, names, "bucket-beta")
	assert.Contains(t, names, "bucket-gamma")
}

// --- DescribeBuckets: sort order ---

func TestBuckets_DescribeBuckets_SortOrder(t *testing.T) {
	t.Parallel()

	h, b := newBucketHandlerAndBackend(t)

	seedBucket(t, b, "zzz-last", "us-east-1", "NOT_PUBLIC", "AES256", 0, 0)
	seedBucket(t, b, "aaa-first", "us-east-1", "NOT_PUBLIC", "AES256", 0, 0)
	seedBucket(t, b, "mmm-middle", "us-east-1", "NOT_PUBLIC", "AES256", 0, 0)

	buckets := describeBuckets(t, h, nil)
	require.Len(t, buckets, 3)

	names := make([]string, 0, 3)
	for _, bkt := range buckets {
		m := bkt.(map[string]any)
		names = append(names, m["bucketName"].(string))
	}

	assert.Equal(t, "aaa-first", names[0])
	assert.Equal(t, "mmm-middle", names[1])
	assert.Equal(t, "zzz-last", names[2])
}

// --- DescribeBuckets: filter by name ---

func TestBuckets_DescribeBuckets_FilterByName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filter    string
		wantNames []string
		wantCount int
	}{
		{
			name:      "exact_match",
			filter:    "prod-logs",
			wantCount: 1,
			wantNames: []string{"prod-logs"},
		},
		{
			name:      "substring_match",
			filter:    "prod",
			wantCount: 2,
			wantNames: []string{"prod-logs", "prod-data"},
		},
		{
			name:      "no_match",
			filter:    "nonexistent",
			wantCount: 0,
			wantNames: []string{},
		},
		{
			name:      "common_prefix",
			filter:    "my-",
			wantCount: 3,
			wantNames: []string{"my-alpha", "my-beta", "my-gamma"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, b := newBucketHandlerAndBackend(t)

			seedBucket(t, b, "prod-logs", "us-east-1", "NOT_PUBLIC", "AES256", 0, 0)
			seedBucket(t, b, "prod-data", "us-east-1", "NOT_PUBLIC", "AES256", 0, 0)
			seedBucket(t, b, "staging-logs", "us-east-1", "NOT_PUBLIC", "AES256", 0, 0)
			seedBucket(t, b, "my-alpha", "us-east-1", "NOT_PUBLIC", "AES256", 0, 0)
			seedBucket(t, b, "my-beta", "us-east-1", "NOT_PUBLIC", "AES256", 0, 0)
			seedBucket(t, b, "my-gamma", "us-east-1", "NOT_PUBLIC", "AES256", 0, 0)

			buckets := describeBuckets(t, h, map[string]any{
				"bucketName": map[string]any{"value": tc.filter},
			})

			assert.Len(t, buckets, tc.wantCount)

			if len(tc.wantNames) > 0 {
				names := make([]string, 0, len(buckets))
				for _, bkt := range buckets {
					m := bkt.(map[string]any)
					names = append(names, m["bucketName"].(string))
				}
				for _, wn := range tc.wantNames {
					assert.Contains(t, names, wn)
				}
			}
		})
	}
}

// --- DescribeBuckets: filter by region ---

func TestBuckets_DescribeBuckets_FilterByRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		region    string
		wantCount int
	}{
		{name: "us_east_1", region: "us-east-1", wantCount: 2},
		{name: "us_west_2", region: "us-west-2", wantCount: 1},
		{name: "eu_west_1", region: "eu-west-1", wantCount: 1},
		{name: "ap_southeast_1", region: "ap-southeast-1", wantCount: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, b := newBucketHandlerAndBackend(t)

			seedBucket(t, b, "east-1", "us-east-1", "NOT_PUBLIC", "AES256", 0, 0)
			seedBucket(t, b, "east-2", "us-east-1", "NOT_PUBLIC", "AES256", 0, 0)
			seedBucket(t, b, "west-1", "us-west-2", "NOT_PUBLIC", "AES256", 0, 0)
			seedBucket(t, b, "eu-1", "eu-west-1", "NOT_PUBLIC", "AES256", 0, 0)

			buckets := describeBuckets(t, h, map[string]any{
				"region": map[string]any{"value": tc.region},
			})

			assert.Len(t, buckets, tc.wantCount)

			for _, bkt := range buckets {
				m := bkt.(map[string]any)
				assert.Equal(t, tc.region, m["region"])
			}
		})
	}
}

// --- DescribeBuckets: count ---

func TestBuckets_Count(t *testing.T) {
	t.Parallel()

	_, b := newBucketHandlerAndBackend(t)

	assert.Equal(t, 0, macie2.S3BucketCount(b))

	seedBucket(t, b, "bucket-1", "us-east-1", "NOT_PUBLIC", "AES256", 0, 0)
	assert.Equal(t, 1, macie2.S3BucketCount(b))

	seedBucket(t, b, "bucket-2", "us-east-1", "NOT_PUBLIC", "AES256", 0, 0)
	assert.Equal(t, 2, macie2.S3BucketCount(b))
}

// --- GetBucketStatistics: empty state ---

func TestBuckets_GetBucketStatistics_Empty(t *testing.T) {
	t.Parallel()

	h, _ := newBucketHandlerAndBackend(t)
	stats := getBucketStatistics(t, h)

	assert.InDelta(t, float64(0), stats["bucketCount"], 1e-9)
	assert.InDelta(t, float64(0), stats["classifiableObjectCount"], 1e-9)
	assert.InDelta(t, float64(0), stats["classifiableSizeInBytes"], 1e-9)
	assert.InDelta(t, float64(0), stats["objectCount"], 1e-9)
	assert.InDelta(t, float64(0), stats["sizeInBytes"], 1e-9)
}

// --- GetBucketStatistics: aggregation ---

func TestBuckets_GetBucketStatistics_Aggregation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                    string
		buckets                 []macie2.S3BucketMetadata
		wantBucketCount         float64
		wantClassifiableObjects float64
		wantClassifiableSize    float64
	}{
		{
			name: "all_classifiable",
			buckets: []macie2.S3BucketMetadata{
				makeBucket("b1", "us-east-1", "NOT_PUBLIC", "AES256", 10, 1000),
				makeBucket("b2", "us-east-1", "NOT_PUBLIC", "AES256", 20, 2000),
			},
			wantBucketCount:         2,
			wantClassifiableObjects: 30,
			wantClassifiableSize:    3000,
		},
		{
			name: "mixed_classifiable",
			buckets: []macie2.S3BucketMetadata{
				makeBucket("c1", "us-east-1", "NOT_PUBLIC", "AES256", 5, 500),
				makeBucket("c2", "us-east-1", "NOT_PUBLIC", "AES256", 0, 0),
				makeBucket("c3", "us-east-1", "PUBLIC", "AES256", 15, 1500),
			},
			wantBucketCount:         3,
			wantClassifiableObjects: 20,
			wantClassifiableSize:    2000,
		},
		{
			name: "none_classifiable",
			buckets: []macie2.S3BucketMetadata{
				makeBucket("d1", "us-east-1", "NOT_PUBLIC", "AES256", 0, 0),
				makeBucket("d2", "us-east-1", "NOT_PUBLIC", "AES256", 0, 0),
			},
			wantBucketCount:         2,
			wantClassifiableObjects: 0,
			wantClassifiableSize:    0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, b := newBucketHandlerAndBackend(t)

			for _, bkt := range tc.buckets {
				macie2.SeedS3Bucket(b, bkt)
			}

			stats := getBucketStatistics(t, h)
			assert.InDelta(t, tc.wantBucketCount, stats["bucketCount"], 1e-9)
			assert.InDelta(t, tc.wantClassifiableObjects, stats["classifiableObjectCount"], 1e-9)
			assert.InDelta(t, tc.wantClassifiableSize, stats["classifiableSizeInBytes"], 1e-9)
		})
	}
}

// --- GetBucketStatistics: counts by permission ---

func TestBuckets_GetBucketStatistics_PermissionCounts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		access        string
		wantPublic    float64
		wantNotPublic float64
		wantUnknown   float64
	}{
		{
			name:          "public_only",
			access:        "PUBLIC",
			wantPublic:    1,
			wantNotPublic: 0,
			wantUnknown:   0,
		},
		{
			name:          "not_public_only",
			access:        "NOT_PUBLIC",
			wantPublic:    0,
			wantNotPublic: 1,
			wantUnknown:   0,
		},
		{
			name:          "unknown",
			access:        "UNKNOWN",
			wantPublic:    0,
			wantNotPublic: 0,
			wantUnknown:   1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, b := newBucketHandlerAndBackend(t)
			seedBucket(t, b, "perm-test-bucket", "us-east-1", tc.access, "AES256", 0, 0)

			stats := getBucketStatistics(t, h)

			permCounts, ok := stats["bucketCountByEffectivePermission"].(map[string]any)
			require.True(t, ok)
			assert.InDelta(t, tc.wantPublic, permCounts["PUBLIC"], 1e-9)
			assert.InDelta(t, tc.wantNotPublic, permCounts["NOT_PUBLIC"], 1e-9)
			assert.InDelta(t, tc.wantUnknown, permCounts["UNKNOWN"], 1e-9)
		})
	}
}

// --- GetBucketStatistics: counts by encryption ---

func TestBuckets_GetBucketStatistics_EncryptionCounts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		enc      string
		wantAES  float64
		wantKMS  float64
		wantNone float64
	}{
		{
			name:     "aes256",
			enc:      "AES256",
			wantAES:  1,
			wantKMS:  0,
			wantNone: 0,
		},
		{
			name:     "kms",
			enc:      "aws:kms",
			wantAES:  0,
			wantKMS:  1,
			wantNone: 0,
		},
		{
			name:     "none",
			enc:      "",
			wantAES:  0,
			wantKMS:  0,
			wantNone: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, b := newBucketHandlerAndBackend(t)
			seedBucket(t, b, "enc-test-bucket", "us-east-1", "NOT_PUBLIC", tc.enc, 0, 0)

			stats := getBucketStatistics(t, h)

			encCounts, ok := stats["bucketCountByEncryptionType"].(map[string]any)
			require.True(t, ok)
			assert.InDelta(t, tc.wantAES, encCounts["AES256"], 1e-9)
			assert.InDelta(t, tc.wantKMS, encCounts["aws:kms"], 1e-9)
			assert.InDelta(t, tc.wantNone, encCounts["NONE"], 1e-9)
		})
	}
}

// --- GetBucketStatistics: mixed permissions and encryption ---

func TestBuckets_GetBucketStatistics_Mixed(t *testing.T) {
	t.Parallel()

	h, b := newBucketHandlerAndBackend(t)

	seedBucket(t, b, "pub-aes", "us-east-1", "PUBLIC", "AES256", 10, 1000)
	seedBucket(t, b, "pub-kms", "us-east-1", "PUBLIC", "aws:kms", 5, 500)
	seedBucket(t, b, "priv-aes", "us-east-1", "NOT_PUBLIC", "AES256", 20, 2000)
	seedBucket(t, b, "empty-priv", "us-east-1", "NOT_PUBLIC", "AES256", 0, 0)

	stats := getBucketStatistics(t, h)

	assert.InDelta(t, float64(4), stats["bucketCount"], 1e-9)
	assert.InDelta(t, float64(35), stats["classifiableObjectCount"], 1e-9)
	assert.InDelta(t, float64(3500), stats["classifiableSizeInBytes"], 1e-9)
	assert.InDelta(t, float64(35), stats["objectCount"], 1e-9)
	assert.InDelta(t, float64(3500), stats["sizeInBytes"], 1e-9)

	permCounts := stats["bucketCountByEffectivePermission"].(map[string]any)
	assert.InDelta(t, float64(2), permCounts["PUBLIC"], 1e-9)
	assert.InDelta(t, float64(2), permCounts["NOT_PUBLIC"], 1e-9)

	encCounts := stats["bucketCountByEncryptionType"].(map[string]any)
	assert.InDelta(t, float64(3), encCounts["AES256"], 1e-9)
	assert.InDelta(t, float64(1), encCounts["aws:kms"], 1e-9)
}

// --- Snapshot/Restore preserves buckets ---

func TestBuckets_SnapshotRestore(t *testing.T) {
	t.Parallel()

	b := newBucketBackend()
	seedBucket(t, b, "snap-bucket-1", "us-east-1", "NOT_PUBLIC", "AES256", 10, 1000)
	seedBucket(t, b, "snap-bucket-2", "us-west-2", "PUBLIC", "aws:kms", 5, 500)

	snap := b.Snapshot(t.Context())

	b2 := newBucketBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	h2 := macie2.NewHandler(b2)
	buckets := describeBuckets(t, h2, nil)

	require.Len(t, buckets, 2)

	names := make([]string, 0, 2)
	for _, bkt := range buckets {
		m := bkt.(map[string]any)
		names = append(names, m["bucketName"].(string))
	}
	assert.Contains(t, names, "snap-bucket-1")
	assert.Contains(t, names, "snap-bucket-2")
}

// --- Reset clears buckets ---

func TestBuckets_Reset(t *testing.T) {
	t.Parallel()

	b := newBucketBackend()
	h := macie2.NewHandler(b)

	seedBucket(t, b, "reset-bucket", "us-east-1", "NOT_PUBLIC", "AES256", 0, 0)
	assert.Equal(t, 1, macie2.S3BucketCount(b))

	b.Reset()

	assert.Equal(t, 0, macie2.S3BucketCount(b))
	buckets := describeBuckets(t, h, nil)
	assert.Empty(t, buckets)

	stats := getBucketStatistics(t, h)
	assert.InDelta(t, float64(0), stats["bucketCount"], 1e-9)
}

// --- DescribeBuckets response structure ---

func TestBuckets_ResponseStructure(t *testing.T) {
	t.Parallel()

	h, b := newBucketHandlerAndBackend(t)
	seedBucket(t, b, "structure-bucket", "us-east-1", "NOT_PUBLIC", "AES256", 100, 10240)

	buckets := describeBuckets(t, h, nil)
	require.Len(t, buckets, 1)

	bkt := buckets[0].(map[string]any)
	assert.Contains(t, bkt, "accountId")
	assert.Contains(t, bkt, "bucketArn")
	assert.Contains(t, bkt, "bucketName")
	assert.Contains(t, bkt, "region")
	assert.Contains(t, bkt, "classifiableObjectCount")
	assert.Contains(t, bkt, "classifiableSizeInBytes")
	assert.Contains(t, bkt, "objectCount")
	assert.Contains(t, bkt, "sizeInBytes")
	assert.Contains(t, bkt, "publicAccess")
	assert.Contains(t, bkt, "serverSideEncryption")
	assert.Contains(t, bkt, "sharedAccess")
}

// --- GetBucketStatistics response structure ---

func TestBuckets_StatisticsResponseStructure(t *testing.T) {
	t.Parallel()

	h, _ := newBucketHandlerAndBackend(t)
	stats := getBucketStatistics(t, h)

	assert.Contains(t, stats, "bucketCount")
	assert.Contains(t, stats, "classifiableObjectCount")
	assert.Contains(t, stats, "classifiableSizeInBytes")
	assert.Contains(t, stats, "objectCount")
	assert.Contains(t, stats, "sizeInBytes")
	assert.Contains(t, stats, "bucketCountByEffectivePermission")
	assert.Contains(t, stats, "bucketCountByEncryptionType")
	assert.Contains(t, stats, "bucketCountByObjectEncryptionRequirement")
	assert.Contains(t, stats, "bucketCountBySharedAccessType")
	assert.Contains(t, stats, "unclassifiableObjectCount")
	assert.Contains(t, stats, "unclassifiableObjectSizeInBytes")
}

// --- stable sort order for buckets ---

func TestBuckets_StableSortOrder(t *testing.T) {
	t.Parallel()

	h, b := newBucketHandlerAndBackend(t)

	seedBucket(t, b, "zzz-bucket", "us-east-1", "NOT_PUBLIC", "AES256", 0, 0)
	seedBucket(t, b, "aaa-bucket", "us-east-1", "NOT_PUBLIC", "AES256", 0, 0)
	seedBucket(t, b, "mmm-bucket", "us-east-1", "NOT_PUBLIC", "AES256", 0, 0)

	first := describeBuckets(t, h, nil)
	second := describeBuckets(t, h, nil)

	require.Len(t, first, 3)

	for i := range first {
		b1 := first[i].(map[string]any)
		b2 := second[i].(map[string]any)
		assert.Equal(t, b1["bucketName"], b2["bucketName"])
	}
}

// --- ARN format ---

func TestBuckets_ARNFormat(t *testing.T) {
	t.Parallel()

	h, b := newBucketHandlerAndBackend(t)
	seedBucket(t, b, "arn-format-bucket", "us-east-1", "NOT_PUBLIC", "AES256", 0, 0)

	buckets := describeBuckets(t, h, nil)
	require.Len(t, buckets, 1)

	bkt := buckets[0].(map[string]any)
	assert.Equal(t, "arn:aws:s3:::arn-format-bucket", bkt["bucketArn"])
}
