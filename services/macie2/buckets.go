package macie2

import (
	"slices"
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

// BucketCriterion mirrors types.BucketCriteriaAdditionalProperties -- the
// real per-property operator set (eq/neq/prefix string-valued, gt/gte/lt/lte
// int64-valued), confirmed against
// aws-sdk-go-v2/service/macie2@v1.54.4/serializers.go:6840
// (awsRestjson1_serializeDocumentBucketCriteriaAdditionalProperties).
type BucketCriterion struct {
	Prefix *string
	Gt     *int64
	Gte    *int64
	Lt     *int64
	Lte    *int64
	Eq     []string
	Neq    []string
}

// BucketSortCriteria mirrors types.BucketSortCriteria.
type BucketSortCriteria struct {
	AttributeName string
	OrderBy       string
}

// Bucket property names DescribeBuckets' own AWS user-guide documents
// (monitoring-s3-inventory-filter.html), backed by this model's fields.
const (
	bucketFieldName                    = "bucketName"
	bucketFieldAccountID               = "accountId"
	bucketFieldRegion                  = "region"
	bucketFieldSharedAccess            = "sharedAccess"
	bucketFieldEffectivePermission     = "publicAccess.effectivePermission"
	bucketFieldObjectCount             = "objectCount"
	bucketFieldSizeInBytes             = "sizeInBytes"
	bucketFieldClassifiableObjectCount = "classifiableObjectCount"
	bucketFieldClassifiableSizeInBytes = "classifiableSizeInBytes"
	sortOrderDesc                      = "DESC"
)

// bucketStringField/bucketIntField resolve the property names above to this
// backend's S3BucketMetadata. Every other documented property (e.g.
// jobDetails.isMonitoredByJob, replicationDetails.replicatedExternally,
// objectCountByEncryptionType.*) has no backing field on this model and is
// left unfiltered rather than invented.
func bucketStringField(bkt *S3BucketMetadata, name string) (string, bool) {
	switch name {
	case bucketFieldName:
		return bkt.BucketName, true
	case bucketFieldAccountID:
		return bkt.AccountID, true
	case bucketFieldRegion:
		return bkt.Region, true
	case bucketFieldSharedAccess:
		return bkt.SharedAccess, true
	case bucketFieldEffectivePermission:
		return bkt.PublicAccess, true
	}

	return "", false
}

func bucketIntField(bkt *S3BucketMetadata, name string) (int64, bool) {
	switch name {
	case bucketFieldObjectCount:
		return bkt.ObjectCount, true
	case bucketFieldSizeInBytes:
		return bkt.SizeInBytes, true
	case bucketFieldClassifiableObjectCount:
		return bkt.ClassifiableObjectCount, true
	case bucketFieldClassifiableSizeInBytes:
		return bkt.ClassifiableSizeInBytes, true
	}

	return 0, false
}

func matchesStringCriterion(v string, c BucketCriterion) bool {
	if len(c.Eq) > 0 && !slices.Contains(c.Eq, v) {
		return false
	}

	if len(c.Neq) > 0 && slices.Contains(c.Neq, v) {
		return false
	}

	return c.Prefix == nil || strings.HasPrefix(v, *c.Prefix)
}

func matchesIntCriterion(v int64, c BucketCriterion) bool {
	if c.Gt != nil && v <= *c.Gt {
		return false
	}

	if c.Gte != nil && v < *c.Gte {
		return false
	}

	if c.Lt != nil && v >= *c.Lt {
		return false
	}

	return c.Lte == nil || v <= *c.Lte
}

func matchesBucketCriterion(bkt *S3BucketMetadata, name string, c BucketCriterion) bool {
	if v, ok := bucketStringField(bkt, name); ok {
		return matchesStringCriterion(v, c)
	}

	if v, ok := bucketIntField(bkt, name); ok {
		return matchesIntCriterion(v, c)
	}

	return true
}

// matchesBucketCriteria returns true when bkt matches every criterion (AND
// logic across properties, per DescribeBuckets' own documentation).
func matchesBucketCriteria(bkt *S3BucketMetadata, criteria map[string]BucketCriterion) bool {
	for name, c := range criteria {
		if !matchesBucketCriterion(bkt, name, c) {
			return false
		}
	}

	return true
}

func sortBuckets(buckets []*S3BucketMetadata, sortBy *BucketSortCriteria) {
	if sortBy == nil {
		sort.Slice(buckets, func(i, k int) bool {
			if buckets[i].BucketName != buckets[k].BucketName {
				return buckets[i].BucketName < buckets[k].BucketName
			}

			return buckets[i].BucketArn < buckets[k].BucketArn
		})

		return
	}

	desc := sortBy.OrderBy == sortOrderDesc

	sort.Slice(buckets, func(i, k int) bool {
		var less, tied bool

		switch sortBy.AttributeName {
		case bucketFieldAccountID:
			less, tied = buckets[i].AccountID < buckets[k].AccountID, buckets[i].AccountID == buckets[k].AccountID
		case bucketFieldClassifiableObjectCount:
			less = buckets[i].ClassifiableObjectCount < buckets[k].ClassifiableObjectCount
			tied = buckets[i].ClassifiableObjectCount == buckets[k].ClassifiableObjectCount
		case bucketFieldClassifiableSizeInBytes:
			less = buckets[i].ClassifiableSizeInBytes < buckets[k].ClassifiableSizeInBytes
			tied = buckets[i].ClassifiableSizeInBytes == buckets[k].ClassifiableSizeInBytes
		case bucketFieldObjectCount:
			less, tied = buckets[i].ObjectCount < buckets[k].ObjectCount, buckets[i].ObjectCount == buckets[k].ObjectCount
		case bucketFieldSizeInBytes:
			less, tied = buckets[i].SizeInBytes < buckets[k].SizeInBytes, buckets[i].SizeInBytes == buckets[k].SizeInBytes
		case bucketFieldName:
			less, tied = buckets[i].BucketName < buckets[k].BucketName, buckets[i].BucketName == buckets[k].BucketName
		default:
			// sensitivityScore is a documented AttributeName value, but this
			// backend has no sensitivity-scan data to sort by -- leave order
			// unchanged for it rather than inventing a score.
			return false
		}

		if tied {
			// BucketArn is this table's unique key (store_setup.go); breaking ties on
			// it keeps a total order so the offset-based page.NewHMAC cursor can't
			// drop or duplicate buckets that tie on the requested attribute.
			return buckets[i].BucketArn < buckets[k].BucketArn
		}

		if desc {
			return !less
		}

		return less
	})
}

// DescribeBuckets returns a page of S3 bucket metadata, filtered by criteria
// and sorted by sortBy.
func (b *InMemoryBackend) DescribeBuckets(
	criteria map[string]BucketCriterion, sortBy *BucketSortCriteria, token string, limit int,
) ([]map[string]any, string, error) {
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

	sortBuckets(all, sortBy)

	pageItems, next := paginate(all, token, b.paginationSecret, limit)

	out := make([]map[string]any, 0, len(pageItems))

	for _, bkt := range pageItems {
		out = append(out, bucketToMap(bkt))
	}

	return out, next, nil
}

// bucketToMap converts S3BucketMetadata to the wire format for DescribeBuckets.
func bucketToMap(bkt *S3BucketMetadata) map[string]any {
	return map[string]any{
		bucketFieldAccountID:               bkt.AccountID,
		"bucketArn":                        bkt.BucketArn,
		bucketFieldName:                    bkt.BucketName,
		bucketFieldRegion:                  bkt.Region,
		bucketFieldClassifiableObjectCount: bkt.ClassifiableObjectCount,
		bucketFieldClassifiableSizeInBytes: bkt.ClassifiableSizeInBytes,
		bucketFieldObjectCount:             bkt.ObjectCount,
		bucketFieldSizeInBytes:             bkt.SizeInBytes,
		"publicAccess": map[string]any{
			"effectivePermission": bkt.PublicAccess,
		},
		"serverSideEncryption": map[string]any{
			keyType: bkt.EncryptionType,
		},
		bucketFieldSharedAccess: bkt.SharedAccess,
		"tags":                  bkt.Tags,
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
		bucketFieldClassifiableObjectCount:         classifiableObjectCount,
		bucketFieldClassifiableSizeInBytes:         classifiableSizeInBytes,
		bucketFieldObjectCount:                     objectCount,
		bucketFieldSizeInBytes:                     sizeInBytes,
		"unclassifiableObjectCount":                map[string]any{},
		"unclassifiableObjectSizeInBytes":          map[string]any{},
	}, nil
}
