package acm

import "time"

const (
	validationMethodDNS      = "DNS"
	validationMethodEMAIL    = "EMAIL"
	statusPendingValidation  = "PENDING_VALIDATION"
	statusIssued             = "ISSUED"
	statusRevoked            = "REVOKED"
	statusInactive           = "INACTIVE"
	statusExpired            = "EXPIRED"
	statusValidationTimedOut = "VALIDATION_TIMED_OUT"
	statusFailed             = "FAILED"
	validationStatusSuccess  = "SUCCESS"
	validationTokenLen       = 8
	autoValidateDelayMS      = 100
	randByteDivisor          = 2
	certTypeImported         = "IMPORTED"
	certTypePrivate          = "PRIVATE"
	certValidityDuration     = 365 * 24 * time.Hour

	defaultDaysBeforeExpiry = int32(45)

	transparencyLoggingEnabled  = "ENABLED"
	transparencyLoggingDisabled = "DISABLED"

	// keyAlgorithmEC is the AWS ACM key algorithm name for ECDSA P-256 keys.
	keyAlgorithmEC = "EC_prime256v1"
	// signatureAlgorithmECDSA is the signature algorithm string for ECDSA with SHA-256.

	// maxDomainLength is the maximum length of a domain name per RFC 1035 / AWS ACM constraints.
	maxDomainLength = 253
	// maxDomainLabelLength is the maximum length of a single DNS label (component between dots).
	maxDomainLabelLength = 63

	// keyUsageDigitalSignature is the AWS string for the X.509 digitalSignature key usage.
	keyUsageDigitalSignature = "DIGITAL_SIGNATURE"
	// extKeyUsageServerAuth is the AWS string for the X.509 serverAuth extended key usage.
	extKeyUsageServerAuth = "TLS_WEB_SERVER_AUTHENTICATION"

	// listCertSortByCreatedAt is the sort-by field name for creation timestamp.
	listCertSortByCreatedAt = "CREATED_AT"

	// certManagedByCloudfront is the only real value of the CertificateManagedBy
	// enum (aws-sdk-go-v2/service/acm/types.CertificateManagedByCloudfront) --
	// RequestCertificate.ManagedBy / CertificateDetail.ManagedBy /
	// CertificateSummary.ManagedBy all share this single-value enum.
	certManagedByCloudfront = "CLOUDFRONT"
)

// ResourceRecord holds the CNAME record used for DNS certificate validation.
type ResourceRecord struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

// DomainValidationOption holds the validation details for a single domain.
type DomainValidationOption struct {
	ResourceRecord   *ResourceRecord `json:"resourceRecord,omitempty"`
	DomainName       string          `json:"domainName"`
	ValidationDomain string          `json:"validationDomain"`
	ValidationStatus string          `json:"validationStatus"`
	ValidationMethod string          `json:"validationMethod"`
	// ValidationEmails is populated when ValidationMethod is EMAIL.
	ValidationEmails []string `json:"validationEmails,omitempty"`
}

// Certificate represents an ACM certificate.
type Certificate struct {
	CreatedAt                          time.Time       `json:"createdAt"`
	NotBefore                          time.Time       `json:"notBefore"`
	NotAfter                           time.Time       `json:"notAfter"`
	RevokedAt                          *time.Time      `json:"revokedAt,omitempty"`
	IssuedAt                           *time.Time      `json:"issuedAt,omitempty"`
	ImportedAt                         *time.Time      `json:"importedAt,omitempty"`
	RenewalSummary                     *RenewalSummary `json:"renewalSummary,omitempty"`
	CertificateBody                    string          `json:"certificateBody,omitempty"`
	IdempotencyToken                   string          `json:"idempotencyToken,omitempty"`
	Subject                            string          `json:"subject,omitempty"`
	Issuer                             string          `json:"issuer,omitempty"`
	KeyAlgorithm                       string          `json:"keyAlgorithm,omitempty"`
	SignatureAlgorithm                 string          `json:"signatureAlgorithm,omitempty"`
	Status                             string          `json:"status"`
	Type                               string          `json:"type"`
	RevocationReason                   string          `json:"revocationReason,omitempty"`
	DomainName                         string          `json:"domainName"`
	ValidationMethod                   string          `json:"validationMethod,omitempty"`
	RenewalEligibility                 string          `json:"renewalEligibility,omitempty"`
	CertificateChain                   string          `json:"certificateChain,omitempty"`
	Serial                             string          `json:"serial,omitempty"`
	CertificateTransparencyLoggingPref string          `json:"certTransparencyLoggingPref,omitempty"`
	PrivateKey                         string          `json:"privateKey,omitempty"`
	CertificateAuthorityArn            string          `json:"certificateAuthorityArn,omitempty"`
	KeyID                              string          `json:"keyId,omitempty"`
	ExportPref                         string          `json:"exportPref,omitempty"`
	FailureReason                      string          `json:"failureReason,omitempty"`
	region                             string
	ARN                                string                   `json:"arn"`
	ManagedBy                          string                   `json:"managedBy,omitempty"`
	InUseBy                            []string                 `json:"inUseBy,omitempty"`
	KeyUsage                           []string                 `json:"keyUsage,omitempty"`
	ExtendedKeyUsage                   []string                 `json:"extendedKeyUsage,omitempty"`
	SubjectAlternativeNames            []string                 `json:"subjectAlternativeNames,omitempty"`
	DomainValidationOptions            []DomainValidationOption `json:"domainValidationOptions,omitempty"`
	Exported                           bool                     `json:"exported,omitempty"`
}

// AccountConfig holds account-level ACM configuration.
type AccountConfig struct {
	DaysBeforeExpiry int32 `json:"daysBeforeExpiry"`
}

// accountIdempotencyEntry stores the settings associated with a PutAccountConfiguration idempotency token.
type accountIdempotencyEntry struct {
	CreatedAt        time.Time `json:"createdAt"`
	DaysBeforeExpiry int32     `json:"daysBeforeExpiry"`
}

// certIdempotencyEntry stores the ARN associated with a RequestCertificate idempotency token.
type certIdempotencyEntry struct {
	CreatedAt time.Time `json:"createdAt"`
	ARN       string    `json:"arn"`
}

const (
	renewalEligibilityEligible   = "ELIGIBLE"
	renewalEligibilityIneligible = "INELIGIBLE"

	// renewalStatusPendingValidation is the AWS RenewalStatus when a renewal is awaiting validation.
	renewalStatusPendingValidation = "PENDING_VALIDATION"
)

// RenewalSummary describes the state of an ACM managed renewal for a certificate.
type RenewalSummary struct {
	// UpdatedAt is when the renewal summary was last updated. Required
	// (always present) on the real AWS wire.
	UpdatedAt time.Time `json:"updatedAt"`
	// RenewalStatus is the status of the renewal (e.g. PENDING_VALIDATION, SUCCESS).
	RenewalStatus string `json:"RenewalStatus"`
	// RenewalStatusReason is set when RenewalStatus is FAILED, describing why.
	RenewalStatusReason string `json:"renewalStatusReason,omitempty"`
	// DomainValidationOptions contains per-domain validation details for the renewal.
	DomainValidationOptions []DomainValidationOption `json:"DomainValidationOptions,omitempty"`
}

// copyCert returns a deep copy of a Certificate, ensuring all pointer fields
// and the DomainValidationOptions slice are not shared with the original.
func copyCert(c *Certificate) Certificate {
	cp := *c

	if c.RevokedAt != nil {
		t := *c.RevokedAt
		cp.RevokedAt = &t
	}

	if c.IssuedAt != nil {
		t := *c.IssuedAt
		cp.IssuedAt = &t
	}

	if c.ImportedAt != nil {
		t := *c.ImportedAt
		cp.ImportedAt = &t
	}

	if len(c.SubjectAlternativeNames) > 0 {
		cp.SubjectAlternativeNames = append([]string(nil), c.SubjectAlternativeNames...)
	}

	if len(c.InUseBy) > 0 {
		cp.InUseBy = append([]string(nil), c.InUseBy...)
	}

	if len(c.KeyUsage) > 0 {
		cp.KeyUsage = append([]string(nil), c.KeyUsage...)
	}

	if len(c.ExtendedKeyUsage) > 0 {
		cp.ExtendedKeyUsage = append([]string(nil), c.ExtendedKeyUsage...)
	}

	if len(c.DomainValidationOptions) > 0 {
		cp.DomainValidationOptions = make([]DomainValidationOption, len(c.DomainValidationOptions))
		copy(cp.DomainValidationOptions, c.DomainValidationOptions)

		for i, dvo := range c.DomainValidationOptions {
			if dvo.ResourceRecord != nil {
				rr := *dvo.ResourceRecord
				cp.DomainValidationOptions[i].ResourceRecord = &rr
			}

			if len(dvo.ValidationEmails) > 0 {
				cp.DomainValidationOptions[i].ValidationEmails = append([]string(nil), dvo.ValidationEmails...)
			}
		}
	}

	if c.RenewalSummary != nil {
		rs := *c.RenewalSummary
		if len(c.RenewalSummary.DomainValidationOptions) > 0 {
			rs.DomainValidationOptions = append(
				[]DomainValidationOption(nil),
				c.RenewalSummary.DomainValidationOptions...,
			)
		}

		cp.RenewalSummary = &rs
	}

	return cp
}
