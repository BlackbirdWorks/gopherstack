package s3

import (
	"net"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3 object-tagging limits (AWS): at most 10 tags per object; tag keys up to 128
// characters and values up to 256 characters.
const (
	maxObjectTags   = 10
	maxTagKeyLength = 128
	maxTagValueLen  = 256
)

// validateObjectTags enforces AWS's per-object tag-set limits. It returns
// ErrTooManyTags when more than 10 tags are present and ErrInvalidTag when a key
// or value exceeds its length limit (or a key is empty).
func validateObjectTags(tags []types.Tag) error {
	if len(tags) > maxObjectTags {
		return ErrTooManyTags
	}

	for _, t := range tags {
		key := aws.ToString(t.Key)
		if key == "" || len(key) > maxTagKeyLength || len(aws.ToString(t.Value)) > maxTagValueLen {
			return ErrInvalidTag
		}
	}

	return nil
}

// validateTaggingHeader parses and validates an X-Amz-Tagging header value
// ("k1=v1&k2=v2"). An empty header is valid (no tags). A malformed query or a
// tag set that violates the limits returns an error.
func validateTaggingHeader(header string) error {
	if header == "" {
		return nil
	}

	values, err := url.ParseQuery(header)
	if err != nil {
		return ErrInvalidTag
	}

	tags := make([]types.Tag, 0, len(values))
	for k, v := range values {
		val := ""
		if len(v) > 0 {
			val = v[0]
		}
		tags = append(tags, types.Tag{Key: aws.String(k), Value: aws.String(val)})
	}

	return validateObjectTags(tags)
}

// IsValidBucketName validates an S3 bucket name based on AWS rules.
// Rules summary:
// - 3 to 63 characters long.
// - Only lowercase letters, numbers, dots (.), and hyphens (-).
// - Begins and ends with a letter or number.
// - No adjacent dots.
// - Not formatted as an IP address.
func IsValidBucketName(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}

	if !isValidBucketCharacters(name) {
		return false
	}

	// Must begin and end with a letter or number
	if !isAlphanumericLowercase(name[0]) || !isAlphanumericLowercase(name[len(name)-1]) {
		return false
	}

	// Not an IP address
	if net.ParseIP(name) != nil {
		return false
	}

	return !isReservedBucketName(name)
}

func isValidBucketCharacters(name string) bool {
	for i, c := range name {
		switch {
		case c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9':
		case c == '.':
			if i > 0 && name[i-1] == '.' {
				return false
			}
		case c == '-':
		default:
			return false
		}
	}

	return true
}

func isReservedBucketName(name string) bool {
	// Reserved prefixes
	if strings.HasPrefix(name, "xn--") ||
		strings.HasPrefix(name, "sthree-") ||
		strings.HasPrefix(name, "sthree-configurator") {
		return true
	}

	// Reserved suffixes
	return strings.HasSuffix(name, "-s3alias") || strings.HasSuffix(name, "--ol-s3")
}

func isAlphanumericLowercase(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
}

// IsValidObjectKey validates an S3 object key based on AWS rules.
// Rules summary:
// - Up to 1024 bytes in UTF-8.
func IsValidObjectKey(key string) bool {
	return len(key) > 0 && len(key) <= 1024
}
