package macie2

import (
	"sort"
	"strings"
)

// AddS3Bucket seeds an S3 bucket into the backend for DescribeBuckets.
func (b *InMemoryBackend) AddS3Bucket(bucket S3BucketMetadata) {
	b.mu.Lock("AddS3Bucket")
	defer b.mu.Unlock()

	cp := bucket
	b.s3Buckets.Put(&cp)
}

// DescribeBuckets returns S3 bucket metadata, filtered by criteria.
func (b *InMemoryBackend) DescribeBuckets(criteria map[string]any) ([]map[string]any, error) {
	b.mu.RLock("DescribeBuckets")
	defer b.mu.RUnlock()

	buckets := b.s3Buckets.All()
	all := make([]*S3BucketMetadata, 0, len(buckets))

	for _, bkt := range buckets {
		if !matchesBucketCriteria(bkt, criteria) {
			continue
		}

		cp := *bkt
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].BucketName < all[j].BucketName
	})

	out := make([]map[string]any, 0, len(all))

	for _, bkt := range all {
		out = append(out, bucketToMap(bkt))
	}

	return out, nil
}

// matchesBucketCriteria returns true when the bucket matches all filter criteria.
func matchesBucketCriteria(bkt *S3BucketMetadata, criteria map[string]any) bool {
	if len(criteria) == 0 {
		return true
	}

	if nameFilter, ok := criteria["bucketName"]; ok {
		if m, mOk := nameFilter.(map[string]any); mOk {
			if v, vOk := m["value"].(string); vOk && !strings.Contains(bkt.BucketName, v) {
				return false
			}
		}
	}

	if regionFilter, ok := criteria["region"]; ok {
		if m, mOk := regionFilter.(map[string]any); mOk {
			if v, vOk := m["value"].(string); vOk && bkt.Region != v {
				return false
			}
		}
	}

	return true
}

// bucketToMap converts S3BucketMetadata to the wire format for DescribeBuckets.
func bucketToMap(bkt *S3BucketMetadata) map[string]any {
	return map[string]any{
		"accountId":               bkt.AccountID,
		"bucketArn":               bkt.BucketArn,
		"bucketName":              bkt.BucketName,
		"region":                  bkt.Region,
		"classifiableObjectCount": bkt.ClassifiableObjectCount,
		"classifiableSizeInBytes": bkt.ClassifiableSizeInBytes,
		"objectCount":             bkt.ObjectCount,
		"sizeInBytes":             bkt.SizeInBytes,
		"publicAccess": map[string]any{
			"effectivePermission": bkt.PublicAccess,
		},
		"serverSideEncryption": map[string]any{
			keyType: bkt.EncryptionType,
		},
		"sharedAccess": bkt.SharedAccess,
		"tags":         bkt.Tags,
	}
}

// GetBucketStatistics returns aggregate S3 statistics computed from stored buckets.
func (b *InMemoryBackend) GetBucketStatistics(_ string) (map[string]any, error) {
	b.mu.RLock("GetBucketStatistics")
	defer b.mu.RUnlock()

	buckets := b.s3Buckets.All()
	bucketCount := int64(len(buckets))

	var classifiableObjectCount int64
	var classifiableSizeInBytes int64
	var objectCount int64
	var sizeInBytes int64

	permCounts := map[string]int64{"PUBLIC": 0, "NOT_PUBLIC": 0, "UNKNOWN": 0}
	encCounts := map[string]int64{"AES256": 0, "aws:kms": 0, "NONE": 0}

	for _, bkt := range buckets {
		classifiableObjectCount += bkt.ClassifiableObjectCount
		classifiableSizeInBytes += bkt.ClassifiableSizeInBytes
		objectCount += bkt.ObjectCount
		sizeInBytes += bkt.SizeInBytes

		switch bkt.PublicAccess {
		case "PUBLIC":
			permCounts["PUBLIC"]++
		case "NOT_PUBLIC":
			permCounts["NOT_PUBLIC"]++
		default:
			permCounts["UNKNOWN"]++
		}

		switch bkt.EncryptionType {
		case "AES256":
			encCounts["AES256"]++
		case "aws:kms":
			encCounts["aws:kms"]++
		default:
			encCounts["NONE"]++
		}
	}

	return map[string]any{
		"bucketCount":                              bucketCount,
		"bucketCountByEffectivePermission":         permCounts,
		"bucketCountByEncryptionType":              encCounts,
		"bucketCountByObjectEncryptionRequirement": map[string]any{},
		"bucketCountBySharedAccessType":            map[string]any{},
		"classifiableObjectCount":                  classifiableObjectCount,
		"classifiableSizeInBytes":                  classifiableSizeInBytes,
		"objectCount":                              objectCount,
		"sizeInBytes":                              sizeInBytes,
		"unclassifiableObjectCount":                map[string]any{},
		"unclassifiableObjectSizeInBytes":          map[string]any{},
	}, nil
}

// SearchResources searches S3 resources (always returns empty — no real S3 scanning).
func (b *InMemoryBackend) SearchResources(_ map[string]any, _ int, _ string) ([]map[string]any, string, error) {
	return []map[string]any{}, "", nil
}
