package arn

import (
	"errors"
	"fmt"
	"strings"
)

// ErrPartitionMismatch is returned when an ARN's partition differs from the partition of the request's region.
var ErrPartitionMismatch = errors.New("ARN partition does not match the request partition")

const arnPrefix = "arn:"

const (
	partitionAWS  = "aws"
	partitionCN   = "aws-cn"
	partitionGov  = "aws-us-gov"
	partitionISO  = "aws-iso"
	partitionISOB = "aws-iso-b"
	partitionISOE = "aws-iso-e"
	partitionISOF = "aws-iso-f"
)

func knownPartition(p string) bool {
	switch p {
	case partitionAWS, partitionCN, partitionGov, partitionISO, partitionISOB, partitionISOE, partitionISOF:
		return true
	default:
		return false
	}
}

// Partition returns the partition segment of arnStr, or "" when arnStr is not a well-formed ARN of a known partition.
func Partition(arnStr string) string {
	rest, ok := strings.CutPrefix(arnStr, arnPrefix)
	if !ok {
		return ""
	}

	part, _, found := strings.Cut(rest, ":")
	if !found {
		return ""
	}

	if !knownPartition(part) {
		return ""
	}

	return part
}

// ValidateForRegion returns an ErrPartitionMismatch-wrapping error when arnStr names a known partition other than
// the one region belongs to. Non-ARN input and empty regions pass.
func ValidateForRegion(region, arnStr string) error {
	if region == "" {
		return nil
	}

	got := Partition(arnStr)
	if got == "" {
		return nil
	}

	if want := PartitionForRegion(region); got != want {
		return fmt.Errorf("%w: ARN %q is in partition %q but region %q is in partition %q",
			ErrPartitionMismatch, arnStr, got, region, want)
	}

	return nil
}

// FindMismatch scans text for embedded ARNs and returns the first whose partition differs from partition.
// With tolerateCommercial, "aws" ARNs never count as mismatches: global services (IAM, S3, ...) still emit
// them regardless of the request region.
func FindMismatch(text, partition string, tolerateCommercial bool) (string, bool) {
	for rest := text; ; {
		i := strings.Index(rest, arnPrefix)
		if i < 0 {
			return "", false
		}

		rest = rest[i:]

		if got := Partition(rest); got != "" && got != partition && (!tolerateCommercial || got != partitionAWS) {
			return arnToken(rest), true
		}

		rest = rest[len(arnPrefix):]
	}
}

func arnToken(s string) string {
	const maxLen = 256

	end := strings.IndexAny(s, " \t\r\n\"'<>&,;\\")
	if end < 0 || end > maxLen {
		end = min(len(s), maxLen)
	}

	return s[:end]
}

// S3 builds an S3 resource ARN in the partition of region.
func S3(region, resource string) string {
	return fmt.Sprintf("arn:%s:s3:::%s", PartitionForRegion(region), resource)
}

// WithPartition rewrites the partition of arnStr to match region. Non-ARNs are returned unchanged.
func WithPartition(region, arnStr string) string {
	got := Partition(arnStr)
	if got == "" {
		return arnStr
	}

	return arnPrefix + PartitionForRegion(region) + arnStr[len(arnPrefix)+len(got):]
}

// HasService reports whether arnStr is an ARN of the given service in any known partition.
func HasService(arnStr, svc string) bool {
	part := Partition(arnStr)
	if part == "" {
		return false
	}

	rest := arnStr[len(arnPrefix)+len(part)+1:]
	got, _, found := strings.Cut(rest, ":")

	return found && got == svc
}
