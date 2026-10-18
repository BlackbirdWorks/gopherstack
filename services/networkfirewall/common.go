package networkfirewall

import (
	"math"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	maxNameLen        = 128
	maxDescriptionLen = 512
	maxTags           = 50
	maxTagKeyLen      = 128
	maxTagValueLen    = 256
	defaultPageSize   = 100
	maxPageSize       = 100

	encryptionCustomerKMS = "CUSTOMER_KMS"
	encryptionAWSOwned    = "AWS_OWNED_KMS_KEY"
)

var nameRE = regexp.MustCompile(`^[a-zA-Z0-9-]+$`)

func validateName(label, name string) error {
	if name == "" || len(name) > maxNameLen || !nameRE.MatchString(name) {
		return invalid("%s must be 1-%d characters of letters, digits and hyphens", label, maxNameLen)
	}

	return nil
}

func validateDescription(desc string) error {
	if len(desc) > maxDescriptionLen {
		return invalid("Description must be at most %d characters", maxDescriptionLen)
	}

	return nil
}

func validateTags(m map[string]string) error {
	if len(m) > maxTags {
		return invalid("A resource can have at most %d tags", maxTags)
	}

	for k, v := range m {
		if k == "" || len(k) > maxTagKeyLen || strings.HasPrefix(strings.ToLower(k), "aws:") {
			return invalid("Invalid tag key %q", k)
		}

		if len(v) > maxTagValueLen {
			return invalid("Tag value for %q exceeds %d characters", k, maxTagValueLen)
		}
	}

	return nil
}

func validateEncryption(e *EncryptionConfiguration) error {
	if e == nil {
		return nil
	}

	switch e.Type {
	case encryptionCustomerKMS:
		if e.KeyID == "" {
			return invalid("EncryptionConfiguration.KeyId is required when Type is %s", encryptionCustomerKMS)
		}
	case encryptionAWSOwned:
	default:
		return invalid("EncryptionConfiguration.Type %q is not valid", e.Type)
	}

	return nil
}

func normalizeEncryption(e *EncryptionConfiguration) *EncryptionConfiguration {
	if e == nil {
		return &EncryptionConfiguration{Type: encryptionAWSOwned}
	}

	cp := *e

	return &cp
}

func targetNeeded(arn, name string) error {
	if arn == "" && name == "" {
		return invalid("You must specify either the ARN or the name of the resource")
	}

	return nil
}

func checkToken(have, got string) error {
	if got != "" && got != have {
		return fmtInvalidToken()
	}

	return nil
}

func requireToken(have, got string) error {
	if got == "" {
		return invalid("UpdateToken is required")
	}

	return checkToken(have, got)
}

func epoch(t time.Time) *float64 {
	e := awstime.Epoch(t)

	return &e
}

func toInt32(n int) int32 { return int32(min(n, math.MaxInt32)) } //nolint:gosec // clamped

func tagList(m map[string]string) []Tag {
	if len(m) == 0 {
		return nil
	}

	return tags.MapToKV(m)
}

func tagMap(l []Tag) map[string]string {
	if len(l) == 0 {
		return nil
	}

	return tags.MapFromKV(l)
}

func cloneStrings(s []string) []string { return slices.Clone(s) }

func clampLimit(maxResults *int32) (int, error) {
	if maxResults == nil {
		return defaultPageSize, nil
	}

	if *maxResults < 1 || *maxResults > maxPageSize {
		return 0, invalid("MaxResults must be between 1 and %d", maxPageSize)
	}

	return int(*maxResults), nil
}

func paginate[T any](all []T, nextToken string, maxResults *int32) (page.Page[T], error) {
	limit, err := clampLimit(maxResults)
	if err != nil {
		return page.Page[T]{}, err
	}

	if err = page.ValidateToken(nextToken); err != nil {
		return page.Page[T]{}, invalid("NextToken is not valid")
	}

	return page.New(all, nextToken, limit, defaultPageSize), nil
}
