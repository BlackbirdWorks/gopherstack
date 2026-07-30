package s3tables

import (
	"fmt"
	"regexp"
	"strings"
)

// Real S3 Tables naming rules, confirmed against
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets-naming.html
// (the aws-sdk-go-v2 client only validates required-ness client-side --
// validateOpCreateTableBucketInput etc. -- so an invalid name really does
// reach the server unrejected unless this backend enforces it itself).
const (
	bucketNameMinLen = 3
	bucketNameMaxLen = 63

	tableOrNamespaceNameMinLen = 1
	tableOrNamespaceNameMaxLen = 255
)

// bucketNamePattern matches table bucket names: lowercase letters, digits,
// and hyphens, beginning and ending with a letter or number (no leading or
// trailing hyphen, no underscores or periods anywhere).
var bucketNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*[a-z0-9]$`)

// tableOrNamespaceNamePattern matches table and namespace names: lowercase
// letters, digits, and underscores, beginning with a letter or number (no
// hyphens or periods anywhere).
var tableOrNamespaceNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_]*$`)

// bucketReservedPrefixes returns the prefixes that must not prefix a table
// bucket name.
func bucketReservedPrefixes() []string {
	return []string{"xn--", "sthree-", "amzn-s3-demo-", "aws"}
}

// bucketReservedSuffixes returns the suffixes that must not suffix a table
// bucket name.
func bucketReservedSuffixes() []string {
	return []string{"-s3alias", "--ol-s3", "--x-s3", "--table-s3"}
}

// namespaceReservedPrefix must not prefix a namespace name.
const namespaceReservedPrefix = "aws"

// validateBucketName enforces real S3 Tables table-bucket naming rules:
// 3-63 characters; lowercase letters, digits, and hyphens only; must begin
// and end with a letter or number; must not contain underscores or
// periods; must not start with a reserved prefix or end with a reserved
// suffix.
func validateBucketName(name string) error {
	if len(name) < bucketNameMinLen || len(name) > bucketNameMaxLen {
		return fmt.Errorf(
			"%w: table bucket name %q must be between %d and %d characters long",
			ErrInvalidBucketName, name, bucketNameMinLen, bucketNameMaxLen,
		)
	}

	if !bucketNamePattern.MatchString(name) {
		return fmt.Errorf(
			"%w: table bucket name %q must consist only of lowercase letters, numbers, and "+
				"hyphens, and must begin and end with a letter or number",
			ErrInvalidBucketName, name,
		)
	}

	for _, prefix := range bucketReservedPrefixes() {
		if strings.HasPrefix(name, prefix) {
			return fmt.Errorf(
				"%w: table bucket name %q must not start with the reserved prefix %q",
				ErrInvalidBucketName, name, prefix,
			)
		}
	}

	for _, suffix := range bucketReservedSuffixes() {
		if strings.HasSuffix(name, suffix) {
			return fmt.Errorf(
				"%w: table bucket name %q must not end with the reserved suffix %q",
				ErrInvalidBucketName, name, suffix,
			)
		}
	}

	return nil
}

// validateTableOrNamespaceName enforces the real S3 Tables naming rule
// shared by table and namespace names: 1-255 characters; lowercase
// letters, digits, and underscores only; must begin with a letter or
// number; must not contain hyphens or periods.
func validateTableOrNamespaceName(name string) error {
	if len(name) < tableOrNamespaceNameMinLen || len(name) > tableOrNamespaceNameMaxLen {
		return fmt.Errorf(
			"%w: name %q must be between %d and %d characters long",
			ErrInvalidName, name, tableOrNamespaceNameMinLen, tableOrNamespaceNameMaxLen,
		)
	}

	if !tableOrNamespaceNamePattern.MatchString(name) {
		return fmt.Errorf(
			"%w: name %q must consist only of lowercase letters, numbers, and underscores, "+
				"and must begin with a letter or number",
			ErrInvalidName, name,
		)
	}

	return nil
}

// validateNamespaceParts validates every segment of a (possibly compound)
// namespace path against validateTableOrNamespaceName, and additionally
// rejects a namespace starting with the reserved "aws" prefix.
func validateNamespaceParts(namespace []string) error {
	for _, part := range namespace {
		if err := validateTableOrNamespaceName(part); err != nil {
			return err
		}

		if strings.HasPrefix(part, namespaceReservedPrefix) {
			return fmt.Errorf(
				"%w: namespace %q must not start with the reserved prefix %q",
				ErrInvalidName, part, namespaceReservedPrefix,
			)
		}
	}

	return nil
}
