package acm

import (
	cryptorand "crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

// certArnPattern matches the ACM certificate ARN shape:
// arn:<partition>:acm:<region>:<account>:certificate/<id>
// This mirrors the pattern the real SDK validates client-side
// (arn:[\w+=/,.@-]+:acm:[\w+=/,.@-]*:[0-9]+:[\w+=,.@-]+(/[\w+=,.@-]+)*) but is
// narrowed to the "certificate/" resource type, since that is the only
// resource shape a CertificateArn field ever carries.
var certArnPattern = regexp.MustCompile(`^arn:[\w+=/,.@-]+:acm:[\w+=/,.@-]*:[0-9]+:certificate/[\w+=,.@-]+$`)

// validateCertArn checks that a non-empty CertificateArn matches the expected
// ACM ARN shape, returning ErrInvalidArn (InvalidArnException) if it does
// not. An empty ARN is intentionally not flagged here -- callers that
// require a non-empty CertificateArn already surface their own
// ValidationException with a clearer "CertificateArn is required" message,
// and read paths correctly fall through to ErrCertNotFound for "".
func validateCertArn(certARN string) error {
	if certARN == "" {
		return nil
	}

	if !certArnPattern.MatchString(certARN) {
		return fmt.Errorf("%w: %q is not a valid ACM certificate ARN", ErrInvalidArn, certARN)
	}

	return nil
}

// validateManagedBy checks a RequestCertificate ManagedBy input against real
// AWS's CertificateManagedBy enum, which currently defines a single value,
// "CLOUDFRONT" (aws-sdk-go-v2/service/acm/types.CertificateManagedByCloudfront).
// An empty value (the common, caller-managed case) is left alone. Validated
// before the certificate is created so a bad value never leaves an orphaned
// certificate behind, matching validateDomainValidationOptions' reasoning.
func validateManagedBy(managedBy string) error {
	if managedBy == "" || managedBy == certManagedByCloudfront {
		return nil
	}

	return fmt.Errorf("%w: ManagedBy must be %s", ErrInvalidParameter, certManagedByCloudfront)
}

// isSameOrSuperdomain reports whether validationDomain is the same as, or a
// superdomain of, domain -- the constraint AWS enforces on
// DomainValidationOption.ValidationDomain (RequestCertificate input). A
// leading "*." wildcard on domain is stripped before comparison, matching
// AWS's own handling of wildcard certificate requests.
func isSameOrSuperdomain(domain, validationDomain string) bool {
	d := strings.TrimPrefix(domain, "*.")
	if validationDomain == d {
		return true
	}

	return strings.HasSuffix(d, "."+validationDomain)
}

// validateDomainValidationOptions checks a RequestCertificate
// DomainValidationOptions input against the domains actually being
// requested (domainName + sans). Each entry's DomainName must reference one
// of those domains, and its ValidationDomain must be the same as or a
// superdomain of that DomainName -- violations return
// ErrInvalidDomainValidationOptions (InvalidDomainValidationOptionsException),
// matching real AWS.
func validateDomainValidationOptions(
	allDomains []string, opts []domainValidationOptionOverride,
) (map[string]string, error) {
	if len(opts) == 0 {
		return nil, nil //nolint:nilnil // absent overrides map is a meaningful "use defaults" signal
	}

	domainSet := make(map[string]struct{}, len(allDomains))
	for _, d := range allDomains {
		domainSet[d] = struct{}{}
	}

	overrides := make(map[string]string, len(opts))

	for _, o := range opts {
		if _, ok := domainSet[o.DomainName]; !ok {
			return nil, fmt.Errorf(
				"%w: DomainName %q is not part of this certificate request",
				ErrInvalidDomainValidationOptions, o.DomainName,
			)
		}

		if o.ValidationDomain == "" || !isSameOrSuperdomain(o.DomainName, o.ValidationDomain) {
			return nil, fmt.Errorf(
				"%w: ValidationDomain %q for %q must be the domain itself or a superdomain",
				ErrInvalidDomainValidationOptions, o.ValidationDomain, o.DomainName,
			)
		}

		overrides[o.DomainName] = o.ValidationDomain
	}

	return overrides, nil
}

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

// domainValidationOptionOverride is the parsed form of one RequestCertificate
// DomainValidationOptions input entry (see requestCertificateInput in
// handler_certificates.go), naming the ValidationDomain AWS should use for a
// given DomainName's EMAIL validation.
type domainValidationOptionOverride struct {
	DomainName       string
	ValidationDomain string
}

// buildDomainValidationOptions creates DomainValidationOption entries with
// synthetic CNAME records for DNS validation, or synthetic email addresses
// for EMAIL validation. overrides maps DomainName -> caller-supplied
// ValidationDomain (from RequestCertificate's DomainValidationOptions input,
// already validated by validateDomainValidationOptions); a domain absent
// from overrides defaults to using itself as its own ValidationDomain.
func buildDomainValidationOptions(
	domains []string, validationMethod string, overrides map[string]string,
) ([]DomainValidationOption, error) {
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

		validationDomain := d
		if vd, ok := overrides[d]; ok {
			validationDomain = vd
		}

		opt := DomainValidationOption{
			DomainName:       d,
			ValidationDomain: validationDomain,
			ValidationStatus: status,
			ValidationMethod: validationMethod,
		}

		switch validationMethod {
		case validationMethodDNS:
			nameToken, err := randHex()
			if err != nil {
				return nil, err
			}

			valueToken, err := randHex()
			if err != nil {
				return nil, err
			}

			opt.ResourceRecord = &ResourceRecord{
				Name:  "_" + nameToken + "." + d + ".",
				Type:  "CNAME",
				Value: "_" + valueToken + ".acm-validations.aws.",
			}

		case validationMethodEMAIL:
			// AWS sends validation emails to well-known addresses at the
			// validation domain root (the DomainName's superdomain when a
			// ValidationDomain override was supplied, otherwise itself).
			rootDomain := strings.TrimPrefix(validationDomain, "*.")

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

// randHex returns a random lowercase hex string of length validationTokenLen
// characters. Every call site (certificate DNS validation tokens here and
// ACME domain-validation prevalidation tokens in acme_domain_validations.go)
// wants the same length, so it is a constant rather than a parameter.
func randHex() (string, error) {
	const n = validationTokenLen

	b := make([]byte, (n+randByteDivisor-1)/randByteDivisor)
	if _, err := cryptorand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand read failed: %w", err)
	}

	return hex.EncodeToString(b)[:n], nil
}
