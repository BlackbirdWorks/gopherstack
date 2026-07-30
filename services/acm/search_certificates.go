package acm

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// certificateExportDisabled mirrors the AWS CertificateExport enum's
// "DISABLED" value, used as Certificate.ExportPref's implicit zero-value
// meaning (see that field's doc in models.go).
const certificateExportDisabled = "DISABLED"

// certificateExportEnabled mirrors the AWS CertificateExport enum's
// "ENABLED" value -- the opt-in RequestCertificate's Options.Export needs to
// hold before an AMAZON_ISSUED certificate is exportable. See
// validateCertExportable in certificates.go.
const certificateExportEnabled = "ENABLED"

// searchTimestampRange is the parsed form of an X509AttributeFilter
// NotAfter/NotBefore TimestampRange member. A nil bound is unbounded on that
// side, matching the real API's "inclusive, either end optional" semantics.
type searchTimestampRange struct {
	Start *time.Time
	End   *time.Time
}

func (r searchTimestampRange) matches(t time.Time) bool {
	if r.Start != nil && t.Before(*r.Start) {
		return false
	}

	if r.End != nil && t.After(*r.End) {
		return false
	}

	return true
}

// certMetadataFilter is the parsed form of one CertificateFilter's
// AcmCertificateMetadataFilter union member (exactly one field non-nil/non-
// empty at a time). Members with no gopherstack-tracked equivalent
// (AcmeAccountId, AcmeEndpointArn, CertificateKeyPairOrigin) are
// intentionally included but never match anything real -- see
// CertificateSearchResult's own AcmCertificateMetadata gap in PARITY.md:
// gopherstack tracks no such data for any certificate, so honestly matching
// nothing is correct-by-absence rather than fabricated. ManagedBy IS tracked
// (Certificate.ManagedBy, set via RequestCertificate's ManagedBy input) and
// matches for real -- see the matches() switch below.
type certMetadataFilter struct {
	Status                   *string
	Type                     *string
	ValidationMethod         *string
	RenewalStatus            *string
	ExportOption             *string
	Exported                 *bool
	InUse                    *bool
	AcmeAccountID            *string
	AcmeEndpointArn          *string
	ManagedBy                *string
	CertificateKeyPairOrigin *string
}

func (f certMetadataFilter) matches(c *Certificate) bool {
	switch {
	case f.Status != nil:
		return c.Status == *f.Status
	case f.Type != nil:
		return c.Type == *f.Type
	case f.ValidationMethod != nil:
		return c.ValidationMethod == *f.ValidationMethod
	case f.RenewalStatus != nil:
		return c.RenewalSummary != nil && c.RenewalSummary.RenewalStatus == *f.RenewalStatus
	case f.ExportOption != nil:
		pref := c.ExportPref
		if pref == "" {
			pref = certificateExportDisabled // DISABLED is the zero-value default, see Certificate.ExportPref doc
		}

		return pref == *f.ExportOption
	case f.Exported != nil:
		return c.Exported == *f.Exported
	case f.InUse != nil:
		return (len(c.InUseBy) > 0) == *f.InUse
	case f.ManagedBy != nil:
		return c.ManagedBy == *f.ManagedBy
	default:
		// AcmeAccountID/AcmeEndpointArn/CertificateKeyPairOrigin: no tracked
		// data, honestly never matches.
		return false
	}
}

// x509Filter is the parsed form of one CertificateFilter's
// X509AttributeFilter union member.
type x509Filter struct {
	KeyAlgorithm      *string
	KeyUsage          *string
	ExtendedKeyUsage  *string
	SerialNumber      *string
	SANDnsName        *stringComparison
	SubjectCommonName *stringComparison
	NotAfter          *searchTimestampRange
	NotBefore         *searchTimestampRange
}

type stringComparison struct {
	Value      string
	Comparison string // EQUALS | CONTAINS
}

func (c stringComparison) matchesAny(values []string) bool {
	for _, v := range values {
		switch c.Comparison {
		case "CONTAINS":
			if strings.Contains(v, c.Value) {
				return true
			}
		default: // EQUALS, and any unrecognized operator falls back to exact match
			if v == c.Value {
				return true
			}
		}
	}

	return false
}

func (f x509Filter) matches(c *Certificate) bool {
	switch {
	case f.KeyAlgorithm != nil:
		return c.KeyAlgorithm == *f.KeyAlgorithm
	case f.KeyUsage != nil:
		return slices.Contains(c.KeyUsage, *f.KeyUsage)
	case f.ExtendedKeyUsage != nil:
		return slices.Contains(c.ExtendedKeyUsage, *f.ExtendedKeyUsage)
	case f.SerialNumber != nil:
		return c.Serial == *f.SerialNumber
	case f.SANDnsName != nil:
		return f.SANDnsName.matchesAny(c.SubjectAlternativeNames)
	case f.SubjectCommonName != nil:
		// The real SubjectFilter union currently defines only CommonName
		// (SubjectFilterMemberCommonName) -- confirmed against
		// aws-sdk-go-v2/service/acm@v1.43.0/types/types.go -- so this is a
		// complete implementation of X509AttributeFilter.Subject, not a
		// partial one. c.SubjectCommonName is captured directly from
		// pkix.Name.CommonName at certificate creation/import time
		// (crypto.go), not re-derived from the flattened Subject string.
		return f.SubjectCommonName.matchesAny([]string{c.SubjectCommonName})
	case f.NotAfter != nil:
		return f.NotAfter.matches(c.NotAfter)
	case f.NotBefore != nil:
		return f.NotBefore.matches(c.NotBefore)
	default:
		return false
	}
}

// certLeafFilter is one CertificateFilter union member.
type certLeafFilter struct {
	CertificateArn *string
	Metadata       *certMetadataFilter
	X509           *x509Filter
}

func (f certLeafFilter) matches(c *Certificate) bool {
	switch {
	case f.CertificateArn != nil:
		return c.ARN == *f.CertificateArn
	case f.Metadata != nil:
		return f.Metadata.matches(c)
	case f.X509 != nil:
		return f.X509.matches(c)
	default:
		return true
	}
}

// certFilterStatement is a node in the recursive And/Or/Not/Filter
// CertificateFilterStatement tree.
type certFilterStatement struct {
	Not    *certFilterStatement
	Filter *certLeafFilter
	And    []certFilterStatement
	Or     []certFilterStatement
}

func (s certFilterStatement) matches(c *Certificate) bool {
	switch {
	case s.And != nil:
		for _, sub := range s.And {
			if !sub.matches(c) {
				return false
			}
		}

		return true
	case s.Or != nil:
		for _, sub := range s.Or {
			if sub.matches(c) {
				return true
			}
		}

		return false
	case s.Not != nil:
		return !s.Not.matches(c)
	case s.Filter != nil:
		return s.Filter.matches(c)
	default:
		return true
	}
}

// SearchCertificatesParams holds the parsed SearchCertificates input.
type SearchCertificatesParams struct {
	Filter     *certFilterStatement
	SortBy     string
	SortOrder  string
	NextToken  string
	MaxResults int
}

// searchSortComparators maps each supported SearchCertificates SortBy value
// to a less-than comparator. A table (rather than a switch) keeps
// searchSortLess's cyclomatic complexity flat regardless of how many sort
// fields the real API adds.
//
//nolint:gochecknoglobals // read-only lookup table initialized once at startup
var searchSortComparators = map[string]func(a, b *Certificate) bool{
	"NOT_AFTER":      func(a, b *Certificate) bool { return a.NotAfter.Before(b.NotAfter) },
	"NOT_BEFORE":     func(a, b *Certificate) bool { return a.NotBefore.Before(b.NotBefore) },
	"STATUS":         func(a, b *Certificate) bool { return a.Status < b.Status },
	"RENEWAL_STATUS": func(a, b *Certificate) bool { return renewalStatusOf(a) < renewalStatusOf(b) },
	"EXPORTED":       func(a, b *Certificate) bool { return !a.Exported && b.Exported },
	"IN_USE":         func(a, b *Certificate) bool { return len(a.InUseBy) < len(b.InUseBy) },
	"KEY_ALGORITHM":  func(a, b *Certificate) bool { return a.KeyAlgorithm < b.KeyAlgorithm },
	"TYPE":           func(a, b *Certificate) bool { return a.Type < b.Type },
	"REVOKED_AT": func(a, b *Certificate) bool {
		return timeOrZero(a.RevokedAt).Before(timeOrZero(b.RevokedAt))
	},
	"RENEWAL_ELIGIBILITY": func(a, b *Certificate) bool { return a.RenewalEligibility < b.RenewalEligibility },
	"ISSUED_AT": func(a, b *Certificate) bool {
		return timeOrZero(a.IssuedAt).Before(timeOrZero(b.IssuedAt))
	},
	"EXPORT_OPTION":     func(a, b *Certificate) bool { return a.ExportPref < b.ExportPref },
	"VALIDATION_METHOD": func(a, b *Certificate) bool { return a.ValidationMethod < b.ValidationMethod },
	"MANAGED_BY":        func(a, b *Certificate) bool { return a.ManagedBy < b.ManagedBy },
	"IMPORTED_AT": func(a, b *Certificate) bool {
		return timeOrZero(a.ImportedAt).Before(timeOrZero(b.ImportedAt))
	},
	// COMMON_NAME: real data since this pass -- SubjectCommonName is captured
	// directly from pkix.Name.CommonName at certificate creation/import time
	// (crypto.go), no longer only the flattened Subject string.
	"COMMON_NAME":           func(a, b *Certificate) bool { return a.SubjectCommonName < b.SubjectCommonName },
	listCertSortByCreatedAt: func(a, b *Certificate) bool { return a.CreatedAt.Before(b.CreatedAt) },
}

// searchSortLess compares two certificates for SearchCertificates' SortBy.
// CERTIFICATE_ARN and every SortBy value gopherstack tracks no real data for
// (ACME_ENDPOINT_ARN, ACME_ACCOUNT_ID, CERTIFICATE_KEY_PAIR_ORIGIN) fall back
// to the same stable ARN ordering ListCertificates uses when it has no real
// value to sort on.
func searchSortLess(sortBy string, a, b *Certificate) bool {
	if cmp, ok := searchSortComparators[sortBy]; ok {
		return cmp(a, b)
	}

	return a.ARN < b.ARN
}

func renewalStatusOf(c *Certificate) string {
	if c.RenewalSummary == nil {
		return ""
	}

	return c.RenewalSummary.RenewalStatus
}

func timeOrZero(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}

	return *t
}

// SearchCertificates returns certificates in the request region matching
// p.Filter (or all of them, if p.Filter is nil), sorted and paginated per p.
func (b *InMemoryBackend) SearchCertificates(
	ctx context.Context, p SearchCertificatesParams,
) (page.Page[Certificate], error) {
	if err := page.ValidateToken(p.NextToken); err != nil {
		return page.Page[Certificate]{}, fmt.Errorf("%w: invalid NextToken", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("SearchCertificates")
	defer b.mu.RUnlock()

	regionCerts := b.certsByRegion.Get(region)
	results := make([]Certificate, 0, len(regionCerts))

	for _, c := range regionCerts {
		if p.Filter == nil || p.Filter.matches(c) {
			results = append(results, copyCert(c))
		}
	}

	descending := strings.EqualFold(p.SortOrder, "DESCENDING")
	sort.Slice(results, func(i, j int) bool {
		less := searchSortLess(p.SortBy, &results[i], &results[j])
		if descending {
			return searchSortLess(p.SortBy, &results[j], &results[i])
		}

		return less
	})

	const searchDefaultMaxItems = 100

	return page.New(results, p.NextToken, p.MaxResults, searchDefaultMaxItems), nil
}
