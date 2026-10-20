package arn

import (
	"os"
	"strings"
)

// EnvPartitionStrict set to "false" disables cross-partition ARN rejection.
const EnvPartitionStrict = "GOPHERSTACK_PARTITION_STRICT"

// PartitionStrict reports whether cross-partition ARNs are rejected (the default).
func PartitionStrict() bool {
	return !strings.EqualFold(os.Getenv(EnvPartitionStrict), "false")
}

func nonCommercialPartitions() []string {
	return []string{partitionCN, partitionGov, partitionISO, partitionISOB, partitionISOE, partitionISOF}
}

// Canonical rewrites every non-commercial ARN partition in text to "aws" (plain and URL-encoded), the form
// global-service backends store and evaluate policies in.
func Canonical(text string) string {
	if !strings.Contains(text, "arn:aws-") && !strings.Contains(text, "arn%3Aaws-") {
		return text
	}

	for _, p := range nonCommercialPartitions() {
		text = strings.ReplaceAll(text, "arn:"+p+":", "arn:aws:")
		text = strings.ReplaceAll(text, "arn%3A"+p+"%3A", "arn%3Aaws%3A")
	}

	return text
}

// Localize rewrites every "aws" partition ARN in text to partition (plain and URL-encoded).
func Localize(text, partition string) string {
	if partition == "" || partition == partitionAWS {
		return text
	}

	text = strings.ReplaceAll(text, "arn:aws:", "arn:"+partition+":")

	return strings.ReplaceAll(text, "arn%3Aaws%3A", "arn%3A"+partition+"%3A")
}
