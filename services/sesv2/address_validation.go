package sesv2

import (
	"fmt"
	"net/mail"
	"regexp"
	"strings"
)

const (
	maxIdentityLen  = 255
	maxConfigSetLen = 64
)

var (
	configSetNameRe = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	domainLabelRe   = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9-]*[A-Za-z0-9])?$`)
)

func validateConfigurationSetName(name string) error {
	if len(name) > maxConfigSetLen || !configSetNameRe.MatchString(name) {
		return fmt.Errorf(
			"%w: configuration set name must be 1-%d characters of letters, numbers, hyphens and underscores",
			ErrInvalidInput, maxConfigSetLen,
		)
	}

	return nil
}

func validDomainName(s string) bool {
	if s == "" || len(s) > maxIdentityLen {
		return false
	}

	for label := range strings.SplitSeq(strings.TrimSuffix(s, "."), ".") {
		if len(label) > 63 || !domainLabelRe.MatchString(label) {
			return false
		}
	}

	return true
}

// validateIdentityName accepts an email address or a domain name.
func validateIdentityName(identity string) error {
	if local, domain, ok := strings.Cut(identity, "@"); ok {
		if local == "" || strings.ContainsAny(local, " <>,;\"") || !validDomainName(domain) {
			return fmt.Errorf("%w: %q is not a valid email address", ErrInvalidInput, identity)
		}

		return nil
	}

	if !validDomainName(identity) {
		return fmt.Errorf("%w: %q is not a valid email address or domain name", ErrInvalidInput, identity)
	}

	return nil
}

// validateSendAddress accepts "user@example.com" and "Name <user@example.com>".
func validateSendAddress(addr string) error {
	parsed, err := mail.ParseAddress(addr)
	if err != nil || !strings.Contains(parsed.Address, "@") {
		return fmt.Errorf("%w: Illegal address %q", ErrInvalidInput, addr)
	}

	return nil
}

func templateMissing(name string) error {
	return fmt.Errorf("%w: %s", ErrNotFound, "Template "+name+" does not exist.")
}

func configSetMissing(name string) error {
	return fmt.Errorf("%w: %s", ErrNotFound, "Configuration set <"+name+"> does not exist.")
}

func identityMissing(name string) error {
	return fmt.Errorf("%w: %s", ErrNotFound, "Email identity "+name+" does not exist.")
}
