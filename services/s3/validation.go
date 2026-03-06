package s3

import (
	"net"
	"strings"
)

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
