package acm

import (
	"context"
	"encoding/json"
	"time"
)

type dnsNameFilterWire struct {
	ComparisonOperator string `json:"ComparisonOperator"`
	Value              string `json:"Value"`
}

type subjectAlternativeNameFilterWire struct {
	DNSName *dnsNameFilterWire `json:"DnsName,omitempty"`
}

type timestampRangeWire struct {
	Start *float64 `json:"Start,omitempty"`
	End   *float64 `json:"End,omitempty"`
}

type x509AttributeFilterWire struct {
	ExtendedKeyUsage       *string                           `json:"ExtendedKeyUsage,omitempty"`
	KeyAlgorithm           *string                           `json:"KeyAlgorithm,omitempty"`
	KeyUsage               *string                           `json:"KeyUsage,omitempty"`
	NotAfter               *timestampRangeWire               `json:"NotAfter,omitempty"`
	NotBefore              *timestampRangeWire               `json:"NotBefore,omitempty"`
	SerialNumber           *string                           `json:"SerialNumber,omitempty"`
	SubjectAlternativeName *subjectAlternativeNameFilterWire `json:"SubjectAlternativeName,omitempty"`
}

type acmCertificateMetadataFilterWire struct {
	AcmeAccountID            *string `json:"AcmeAccountId,omitempty"`
	AcmeEndpointArn          *string `json:"AcmeEndpointArn,omitempty"`
	CertificateKeyPairOrigin *string `json:"CertificateKeyPairOrigin,omitempty"`
	ExportOption             *string `json:"ExportOption,omitempty"`
	ManagedBy                *string `json:"ManagedBy,omitempty"`
	RenewalStatus            *string `json:"RenewalStatus,omitempty"`
	Status                   *string `json:"Status,omitempty"`
	Type                     *string `json:"Type,omitempty"`
	ValidationMethod         *string `json:"ValidationMethod,omitempty"`
	Exported                 *bool   `json:"Exported,omitempty"`
	InUse                    *bool   `json:"InUse,omitempty"`
}

type certificateFilterWire struct {
	CertificateArn               *string                           `json:"CertificateArn,omitempty"`
	AcmCertificateMetadataFilter *acmCertificateMetadataFilterWire `json:"AcmCertificateMetadataFilter,omitempty"`
	X509AttributeFilter          *x509AttributeFilterWire          `json:"X509AttributeFilter,omitempty"`
}

// filterStatementWire is the recursive CertificateFilterStatement union:
// exactly one of And/Or/Not/Filter is present per node on the real wire.
type filterStatementWire struct {
	Not    *filterStatementWire   `json:"Not,omitempty"`
	Filter *certificateFilterWire `json:"Filter,omitempty"`
	And    []filterStatementWire  `json:"And,omitempty"`
	Or     []filterStatementWire  `json:"Or,omitempty"`
}

func epochPtr(f *float64) *time.Time {
	if f == nil {
		return nil
	}

	t := time.Unix(int64(*f), 0).UTC()

	return &t
}

func (w timestampRangeWire) toRange() *searchTimestampRange {
	return &searchTimestampRange{Start: epochPtr(w.Start), End: epochPtr(w.End)}
}

func (w x509AttributeFilterWire) toFilter() *x509Filter {
	f := &x509Filter{
		KeyAlgorithm:     w.KeyAlgorithm,
		KeyUsage:         w.KeyUsage,
		ExtendedKeyUsage: w.ExtendedKeyUsage,
		SerialNumber:     w.SerialNumber,
	}
	if w.NotAfter != nil {
		f.NotAfter = w.NotAfter.toRange()
	}

	if w.NotBefore != nil {
		f.NotBefore = w.NotBefore.toRange()
	}

	if w.SubjectAlternativeName != nil && w.SubjectAlternativeName.DNSName != nil {
		f.SANDnsName = &stringComparison{
			Value:      w.SubjectAlternativeName.DNSName.Value,
			Comparison: w.SubjectAlternativeName.DNSName.ComparisonOperator,
		}
	}

	return f
}

func (w acmCertificateMetadataFilterWire) toFilter() *certMetadataFilter {
	return &certMetadataFilter{
		Status:                   w.Status,
		Type:                     w.Type,
		ValidationMethod:         w.ValidationMethod,
		RenewalStatus:            w.RenewalStatus,
		ExportOption:             w.ExportOption,
		Exported:                 w.Exported,
		InUse:                    w.InUse,
		AcmeAccountID:            w.AcmeAccountID,
		AcmeEndpointArn:          w.AcmeEndpointArn,
		ManagedBy:                w.ManagedBy,
		CertificateKeyPairOrigin: w.CertificateKeyPairOrigin,
	}
}

func (w certificateFilterWire) toLeaf() *certLeafFilter {
	leaf := &certLeafFilter{CertificateArn: w.CertificateArn}
	if w.AcmCertificateMetadataFilter != nil {
		leaf.Metadata = w.AcmCertificateMetadataFilter.toFilter()
	}

	if w.X509AttributeFilter != nil {
		leaf.X509 = w.X509AttributeFilter.toFilter()
	}

	return leaf
}

func (w filterStatementWire) toStatement() certFilterStatement {
	s := certFilterStatement{}
	if len(w.And) > 0 {
		s.And = make([]certFilterStatement, len(w.And))
		for i, sub := range w.And {
			s.And[i] = sub.toStatement()
		}
	}

	if len(w.Or) > 0 {
		s.Or = make([]certFilterStatement, len(w.Or))
		for i, sub := range w.Or {
			s.Or[i] = sub.toStatement()
		}
	}

	if w.Not != nil {
		sub := w.Not.toStatement()
		s.Not = &sub
	}

	if w.Filter != nil {
		s.Filter = w.Filter.toLeaf()
	}

	return s
}

type distinguishedNameWire struct {
	CommonName string `json:"CommonName,omitempty"`
}

type generalNameWire struct {
	DNSName string `json:"DnsName,omitempty"`
}

type x509AttributesWire struct {
	Issuer                  *distinguishedNameWire `json:"Issuer,omitempty"`
	Subject                 *distinguishedNameWire `json:"Subject,omitempty"`
	KeyAlgorithm            string                 `json:"KeyAlgorithm,omitempty"`
	SerialNumber            string                 `json:"SerialNumber,omitempty"`
	ExtendedKeyUsages       []string               `json:"ExtendedKeyUsages,omitempty"`
	KeyUsages               []string               `json:"KeyUsages,omitempty"`
	SubjectAlternativeNames []generalNameWire      `json:"SubjectAlternativeNames,omitempty"`
	NotAfter                int64                  `json:"NotAfter,omitempty"`
	NotBefore               int64                  `json:"NotBefore,omitempty"`
}

type acmCertificateMetadataWire struct {
	ImportedAt         *int64 `json:"ImportedAt,omitempty"`
	IssuedAt           *int64 `json:"IssuedAt,omitempty"`
	RevokedAt          *int64 `json:"RevokedAt,omitempty"`
	ExportOption       string `json:"ExportOption,omitempty"`
	ManagedBy          string `json:"ManagedBy,omitempty"`
	RenewalEligibility string `json:"RenewalEligibility,omitempty"`
	RenewalStatus      string `json:"RenewalStatus,omitempty"`
	Status             string `json:"Status"`
	Type               string `json:"Type"`
	ValidationMethod   string `json:"ValidationMethod,omitempty"`
	CreatedAt          int64  `json:"CreatedAt"`
	Exported           bool   `json:"Exported"`
	InUse              bool   `json:"InUse"`
}

type certificateMetadataWire struct {
	AcmCertificateMetadata *acmCertificateMetadataWire `json:"AcmCertificateMetadata,omitempty"`
}

type certificateSearchResultWire struct {
	CertificateMetadata *certificateMetadataWire `json:"CertificateMetadata,omitempty"`
	X509Attributes      *x509AttributesWire      `json:"X509Attributes,omitempty"`
	CertificateArn      string                   `json:"CertificateArn"`
}

func unixPtr(t *time.Time) *int64 {
	if t == nil {
		return nil
	}

	v := t.Unix()

	return &v
}

func certToSearchResult(c *Certificate) certificateSearchResultWire {
	sans := make([]generalNameWire, 0, len(c.SubjectAlternativeNames))
	for _, s := range c.SubjectAlternativeNames {
		sans = append(sans, generalNameWire{DNSName: s})
	}

	exportOption := c.ExportPref
	if exportOption == "" {
		exportOption = certificateExportDisabled
	}

	return certificateSearchResultWire{
		CertificateArn: c.ARN,
		CertificateMetadata: &certificateMetadataWire{
			AcmCertificateMetadata: &acmCertificateMetadataWire{
				Status:             c.Status,
				Type:               c.Type,
				ValidationMethod:   c.ValidationMethod,
				RenewalEligibility: c.RenewalEligibility,
				RenewalStatus:      renewalStatusOf(c),
				ExportOption:       exportOption,
				ManagedBy:          c.ManagedBy,
				Exported:           c.Exported,
				InUse:              len(c.InUseBy) > 0,
				CreatedAt:          c.CreatedAt.Unix(),
				ImportedAt:         unixPtr(c.ImportedAt),
				IssuedAt:           unixPtr(c.IssuedAt),
				RevokedAt:          unixPtr(c.RevokedAt),
			},
		},
		X509Attributes: &x509AttributesWire{
			Issuer:                  &distinguishedNameWire{CommonName: c.Issuer},
			Subject:                 &distinguishedNameWire{CommonName: c.Subject},
			KeyAlgorithm:            c.KeyAlgorithm,
			SerialNumber:            c.Serial,
			KeyUsages:               c.KeyUsage,
			ExtendedKeyUsages:       c.ExtendedKeyUsage,
			SubjectAlternativeNames: sans,
			NotAfter:                c.NotAfter.Unix(),
			NotBefore:               c.NotBefore.Unix(),
		},
	}
}

type searchCertificatesInput struct {
	FilterStatement *filterStatementWire `json:"FilterStatement"`
	SortBy          string               `json:"SortBy"`
	SortOrder       string               `json:"SortOrder"`
	NextToken       string               `json:"NextToken"`
	MaxResults      int32                `json:"MaxResults"`
}

type searchCertificatesOutput struct {
	NextToken string                        `json:"NextToken,omitempty"`
	Results   []certificateSearchResultWire `json:"Results"`
}

func (h *Handler) jsonSearchCertificates(ctx context.Context, body []byte) (any, error) {
	var input searchCertificatesInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	params := SearchCertificatesParams{
		SortBy:     input.SortBy,
		SortOrder:  input.SortOrder,
		NextToken:  input.NextToken,
		MaxResults: int(input.MaxResults),
	}

	if input.FilterStatement != nil {
		s := input.FilterStatement.toStatement()
		params.Filter = &s
	}

	pg, err := h.Backend.SearchCertificates(ctx, params)
	if err != nil {
		return nil, err
	}

	results := make([]certificateSearchResultWire, 0, len(pg.Data))
	for i := range pg.Data {
		results = append(results, certToSearchResult(&pg.Data[i]))
	}

	return &searchCertificatesOutput{Results: results, NextToken: pg.Next}, nil
}
