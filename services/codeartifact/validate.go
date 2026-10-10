package codeartifact

import (
	"fmt"
	"regexp"
)

const (
	packageFormatGeneric = "generic"
	maxDescriptionLength = 1000
	minDomainNameLength  = 2
	maxDomainNameLength  = 50
	minRepoNameLength    = 2
	maxRepoNameLength    = 100
)

var (
	domainNamePattern = regexp.MustCompile(`^[a-z][a-z0-9\-]{0,48}[a-z0-9]$`)
	repoNamePattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._\-]{1,99}$`)
	kmsKeyArnPattern  = regexp.MustCompile(`^arn:[a-z\-]+:kms:[a-z0-9\-]+:\d{12}:key/[A-Za-z0-9\-]+$`)
)

// ValidateDomainName checks the documented domain name constraints.
func ValidateDomainName(name string) error {
	if len(name) < minDomainNameLength || len(name) > maxDomainNameLength || !domainNamePattern.MatchString(name) {
		return fmt.Errorf(
			"%w: domain name must be %d-%d characters, start with a lowercase letter and contain only "+
				"lowercase letters, digits and hyphens",
			ErrValidation, minDomainNameLength, maxDomainNameLength,
		)
	}

	return nil
}

// ValidateRepositoryName checks the documented repository name constraints.
func ValidateRepositoryName(name string) error {
	if len(name) < minRepoNameLength || len(name) > maxRepoNameLength || !repoNamePattern.MatchString(name) {
		return fmt.Errorf(
			"%w: repository name must be %d-%d characters, start with a letter or digit and contain only "+
				"letters, digits, periods, underscores and hyphens",
			ErrValidation, minRepoNameLength, maxRepoNameLength,
		)
	}

	return nil
}

func validateDescription(desc string) error {
	if len(desc) > maxDescriptionLength {
		return fmt.Errorf("%w: description must be %d characters or fewer", ErrValidation, maxDescriptionLength)
	}

	return nil
}

func validateEncryptionKey(key string) error {
	if key != "" && !kmsKeyArnPattern.MatchString(key) {
		return fmt.Errorf("%w: encryptionKey must be a KMS key ARN", ErrValidation)
	}

	return nil
}

func validPackageFormat(format string) bool {
	switch format {
	case "npm", "pypi", "maven", "nuget", packageFormatGeneric, "ruby", "swift", "cargo":
		return true
	}

	return false
}

func validatePackageFormat(format string) error {
	if format != "" && !validPackageFormat(format) {
		return fmt.Errorf("%w: format %q is not a valid package format", ErrValidation, format)
	}

	return nil
}

func validExternalConnection(name string) bool {
	switch name {
	case "public:npmjs", "public:nuget-org", "public:pypi", "public:maven-central", "public:maven-googleandroid",
		"public:maven-gradleplugins", "public:maven-commonsware", "public:maven-clojars", "public:ruby-gems-org",
		"public:crates-io", "public:maven-apacheorg", "public:swift-package-index":
		return true
	}

	return false
}

func validateExternalConnectionName(name string) error {
	if !validExternalConnection(name) {
		return fmt.Errorf("%w: %q is not a valid external connection", ErrValidation, name)
	}

	return nil
}
