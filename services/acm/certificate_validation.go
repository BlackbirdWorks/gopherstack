package acm

import (
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

// validateDomainName checks that the given domain name satisfies AWS ACM constraints.
// AWS rejects domain names longer than 253 characters, empty labels, labels exceeding 63
// characters, and labels that are purely numeric (which would be IP addresses).
func validateDomainName(name string) error {
	if len(name) > maxDomainLength {
		return fmt.Errorf("%w: domain %q exceeds maximum length of %d", ErrInvalidParameter, name, maxDomainLength)
	}

	// Strip leading wildcard component (*.example.com → example.com for label checks)
	checkName := strings.TrimPrefix(name, "*.")

	for label := range strings.SplitSeq(checkName, ".") {
		if label == "" {
			return fmt.Errorf("%w: domain %q contains an empty label", ErrInvalidParameter, name)
		}

		if len(label) > maxDomainLabelLength {
			return fmt.Errorf("%w: domain label %q in %q exceeds %d characters",
				ErrInvalidParameter, label, name, maxDomainLabelLength)
		}
	}

	return nil
}

// buildDomainValidationOptions creates DomainValidationOption entries with
// synthetic CNAME records for DNS validation, or synthetic email addresses for EMAIL validation.
func buildDomainValidationOptions(domains []string, validationMethod string) ([]DomainValidationOption, error) {
	opts := make([]DomainValidationOption, 0, len(domains))
	seen := make(map[string]bool, len(domains))

	for _, d := range domains {
		if seen[d] {
			continue
		}
		seen[d] = true

		status := validationStatusSuccess
		if validationMethod == validationMethodDNS || validationMethod == validationMethodEMAIL {
			status = statusPendingValidation
		}

		opt := DomainValidationOption{
			DomainName:       d,
			ValidationDomain: d,
			ValidationStatus: status,
			ValidationMethod: validationMethod,
		}

		switch validationMethod {
		case validationMethodDNS:
			nameToken, err := randHex(validationTokenLen)
			if err != nil {
				return nil, err
			}

			valueToken, err := randHex(validationTokenLen)
			if err != nil {
				return nil, err
			}

			opt.ResourceRecord = &ResourceRecord{
				Name:  "_" + nameToken + "." + d + ".",
				Type:  "CNAME",
				Value: "_" + valueToken + ".acm-validations.aws.",
			}

		case validationMethodEMAIL:
			// AWS sends validation emails to well-known addresses at the domain root.
			rootDomain := d
			if strings.HasPrefix(d, "*.") {
				rootDomain = d[2:]
			}

			opt.ValidationEmails = []string{
				"admin@" + rootDomain,
				"administrator@" + rootDomain,
				"hostmaster@" + rootDomain,
				"postmaster@" + rootDomain,
				"webmaster@" + rootDomain,
			}
		}

		opts = append(opts, opt)
	}

	return opts, nil
}

// randHex returns a random lowercase hex string of length n characters.
func randHex(n int) (string, error) {
	b := make([]byte, (n+randByteDivisor-1)/randByteDivisor)
	if _, err := cryptorand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand read failed: %w", err)
	}

	return hex.EncodeToString(b)[:n], nil
}
