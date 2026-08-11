package acm

import "time"

const (
	validationMethodDNS      = "DNS"
	validationMethodEMAIL    = "EMAIL"
	validationMethodHTTP     = "HTTP"
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
	// keyAlgorithmRSA1024/2048/3072/4096 and keyAlgorithmECSecp384r1/521r1
	// are the remaining real values of the KeyAlgorithm enum
	// (aws-sdk-go-v2/service/acm/types.KeyAlgorithm, enums.go:416-442, v1.43.4).
	keyAlgorithmRSA1024     = "RSA_1024"
	keyAlgorithmRSA2048     = "RSA_2048"
	keyAlgorithmRSA3072     = "RSA_3072"
	keyAlgorithmRSA4096     = "RSA_4096"
	keyAlgorithmECSecp384r1 = "EC_secp384r1"
	keyAlgorithmECSecp521r1 = "EC_secp521r1"
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

// HTTPRedirect holds HTTP-based domain validation details, populated only
// when ValidationMethod is HTTP -- real AWS restricts this to AMAZON_ISSUED
// certificates requested through CloudFront (types.DomainValidation.HttpRedirect
// doc comment, types.go:1053-1056, v1.43.4); gopherstack has no CloudFront
// issuance pipeline, so this is a synthetic value in the correct wire shape,
// same class as the synthetic DNS ResourceRecord/EMAIL addresses below.
type HTTPRedirect struct {
	RedirectFrom string `json:"redirectFrom"`
	RedirectTo   string `json:"redirectTo"`
}

// DomainValidationOption holds the validation details for a single domain.
type DomainValidationOption struct {
	ResourceRecord   *ResourceRecord `json:"resourceRecord,omitempty"`
	HTTPRedirect     *HTTPRedirect   `json:"httpRedirect,omitempty"`
	DomainName       string          `json:"domainName"`
	ValidationDomain string          `json:"validationDomain"`
	ValidationStatus string          `json:"validationStatus"`
	ValidationMethod string          `json:"validationMethod"`
	// ValidationEmails is populated when ValidationMethod is EMAIL.
	ValidationEmails []string `json:"validationEmails,omitempty"`
}

// Certificate represents an ACM certificate.
type Certificate struct {
	CreatedAt        time.Time       `json:"createdAt"`
	NotBefore        time.Time       `json:"notBefore"`
	NotAfter         time.Time       `json:"notAfter"`
	RevokedAt        *time.Time      `json:"revokedAt,omitempty"`
	IssuedAt         *time.Time      `json:"issuedAt,omitempty"`
	ImportedAt       *time.Time      `json:"importedAt,omitempty"`
	RenewalSummary   *RenewalSummary `json:"renewalSummary,omitempty"`
	CertificateBody  string          `json:"certificateBody,omitempty"`
	IdempotencyToken string          `json:"idempotencyToken,omitempty"`
	Subject          string          `json:"subject,omitempty"`
	Issuer           string          `json:"issuer,omitempty"`
	// SubjectCommonName/IssuerCommonName are the CommonName (CN) RDN component
	// parsed/generated separately from the flattened Subject/Issuer strings
	// above. Real AWS's X509Attributes.Subject/Issuer are structured
	// DistinguishedName objects whose CommonName member is just the CN, e.g.
	// "example.com" -- not the whole rendered DN
	// ("CN=example.com,OU=...,O=...,C=US") that Subject/Issuer hold here for
	// display. See crypto.go and SearchCertificates' Subject filter (CommonName
	// is the only member the real SubjectFilter union defines).
	SubjectCommonName                  string `json:"subjectCommonName,omitempty"`
	IssuerCommonName                   string `json:"issuerCommonName,omitempty"`
	KeyAlgorithm                       string `json:"keyAlgorithm,omitempty"`
	SignatureAlgorithm                 string `json:"signatureAlgorithm,omitempty"`
	Status                             string `json:"status"`
	Type                               string `json:"type"`
	RevocationReason                   string `json:"revocationReason,omitempty"`
	DomainName                         string `json:"domainName"`
	ValidationMethod                   string `json:"validationMethod,omitempty"`
	RenewalEligibility                 string `json:"renewalEligibility,omitempty"`
	CertificateChain                   string `json:"certificateChain,omitempty"`
	Serial                             string `json:"serial,omitempty"`
	CertificateTransparencyLoggingPref string `json:"certTransparencyLoggingPref,omitempty"`
	PrivateKey                         string `json:"privateKey,omitempty"`
	CertificateAuthorityArn            string `json:"certificateAuthorityArn,omitempty"`
	KeyID                              string `json:"keyId,omitempty"`
	ExportPref                         string `json:"exportPref,omitempty"`
	FailureReason                      string `json:"failureReason,omitempty"`
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

// copyDomainValidationOptions deep-copies a DomainValidationOption slice so
// none of its pointer/slice fields are shared with the original.
func copyDomainValidationOptions(src []DomainValidationOption) []DomainValidationOption {
	dst := make([]DomainValidationOption, len(src))
	copy(dst, src)

	for i, dvo := range src {
		if dvo.ResourceRecord != nil {
			rr := *dvo.ResourceRecord
			dst[i].ResourceRecord = &rr
		}

		if dvo.HTTPRedirect != nil {
			hr := *dvo.HTTPRedirect
			dst[i].HTTPRedirect = &hr
		}

		if len(dvo.ValidationEmails) > 0 {
			dst[i].ValidationEmails = append([]string(nil), dvo.ValidationEmails...)
		}
	}

	return dst
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
		cp.DomainValidationOptions = copyDomainValidationOptions(c.DomainValidationOptions)
	}

	if c.RenewalSummary != nil {
		rs := *c.RenewalSummary
		if len(c.RenewalSummary.DomainValidationOptions) > 0 {
			rs.DomainValidationOptions = copyDomainValidationOptions(c.RenewalSummary.DomainValidationOptions)
		}

		cp.RenewalSummary = &rs
	}

	return cp
}
