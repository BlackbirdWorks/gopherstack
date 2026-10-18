package s3vectors

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	maxDimension     = 4096
	maxTopK          = 100
	maxPutBatch      = 500
	maxKeyBatch      = 100
	maxKeyLen        = 1024
	maxMetadataBytes = 40 * 1024
	maxFilterBytes   = 2 * 1024
	maxMetadataKeys  = 50
	maxNonFilterable = 10
	maxTags          = 50
	maxTagKey        = 128
	maxTagValue      = 256
	maxBuckets       = 10000
	maxIndexes       = 10000
	minNameLen       = 3
	maxNameLen       = 63
	maxListBuckets   = 500
	maxListVectors   = 1000
	defaultListVecs  = 500
	maxSegments      = 16
)

var (
	bucketNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)
	indexNamePattern  = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)
	kmsARNPattern     = regexp.MustCompile(`^arn:aws[a-z-]*:kms:[a-z0-9-]+:[0-9]{12}:key/.+$`)
)

func validateBucketName(name string) error {
	if len(name) < minNameLen || len(name) > maxNameLen || !bucketNamePattern.MatchString(name) {
		return invalid("vectorBucketName must be 3-63 characters of lowercase letters, digits and hyphens")
	}

	return nil
}

func validateIndexName(name string) error {
	if len(name) < minNameLen || len(name) > maxNameLen || !indexNamePattern.MatchString(name) {
		return invalid("indexName must be 3-63 characters of lowercase letters, digits, hyphens and dots")
	}

	return nil
}

func validateEncryption(e *encryptionIn) (Encryption, error) {
	if e == nil || e.SseType == "" {
		return Encryption{SseType: "AES256"}, nil
	}

	switch e.SseType {
	case "AES256":
		if e.KmsKeyArn != "" {
			return Encryption{}, invalid("kmsKeyArn is only valid with sseType aws:kms")
		}
	case "aws:kms":
		if e.KmsKeyArn != "" && !kmsARNPattern.MatchString(e.KmsKeyArn) {
			return Encryption{}, invalid("kmsKeyArn %q is not a valid KMS key ARN", e.KmsKeyArn)
		}
	default:
		return Encryption{}, invalid("sseType must be AES256 or aws:kms")
	}

	return Encryption{SseType: e.SseType, KmsKeyArn: e.KmsKeyArn}, nil
}

func validateTags(tags map[string]string) error {
	if len(tags) > maxTags {
		return invalid("a resource can have at most %d tags", maxTags)
	}

	for k, v := range tags {
		if k == "" || utf8.RuneCountInString(k) > maxTagKey {
			return invalid("tag keys must be between 1 and %d characters", maxTagKey)
		}

		if utf8.RuneCountInString(v) > maxTagValue {
			return invalid("tag values must be at most %d characters", maxTagValue)
		}

		if strings.HasPrefix(strings.ToLower(k), "aws:") {
			return invalid("tag keys starting with aws: are reserved")
		}
	}

	return nil
}
