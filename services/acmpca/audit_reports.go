package acmpca

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// CreateCertificateAuthorityAuditReport creates a new audit report for the given CA.
func (b *InMemoryBackend) CreateCertificateAuthorityAuditReport(
	ctx context.Context,
	caARN string,
	s3BucketName string,
	responseFormat string,
) (*AuditReport, error) {
	if err := validateRequiredParameter(caARN, "CertificateAuthorityArn"); err != nil {
		return nil, err
	}

	if s3BucketName == "" {
		return nil, fmt.Errorf("%w: S3BucketName is required", ErrInvalidParameter)
	}

	format := strings.ToUpper(responseFormat)
	if format != auditReportFormatJSON && format != auditReportFormatCSV {
		return nil, fmt.Errorf("%w: AuditReportResponseFormat must be JSON or CSV", ErrInvalidParameter)
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateCertificateAuthorityAuditReport")
	defer b.mu.Unlock()

	auditCA, ok := b.caGet(region, caARN)
	if !ok {
		return nil, fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	if auditCA.Status != caStatusActive {
		return nil, fmt.Errorf("%w: CA %s must be ACTIVE to create an audit report", ErrInvalidState, caARN)
	}

	id, err := newRandomID()
	if err != nil {
		return nil, err
	}

	report := &AuditReport{
		CreatedAt:               time.Now().UTC(),
		AuditReportID:           id,
		CertificateAuthorityArn: caARN,
		S3BucketName:            s3BucketName,
		S3Key:                   fmt.Sprintf("%s%s.%s", reportResourcePrefix, id, strings.ToLower(format)),
		Status:                  auditReportStatus,
		region:                  region,
	}
	b.auditReportPut(report)

	cp := copyAuditReport(report)

	return &cp, nil
}

// DescribeCertificateAuthorityAuditReport returns the audit report for the given CA.
func (b *InMemoryBackend) DescribeCertificateAuthorityAuditReport(
	ctx context.Context,
	caARN string,
	auditReportID string,
) (*AuditReport, error) {
	if err := validateRequiredParameter(caARN, "CertificateAuthorityArn"); err != nil {
		return nil, err
	}

	if err := validateRequiredParameter(auditReportID, "AuditReportId"); err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeCertificateAuthorityAuditReport")
	defer b.mu.RUnlock()

	if _, ok := b.caGet(region, caARN); !ok {
		return nil, fmt.Errorf("%w: CA %s not found", ErrCANotFound, caARN)
	}

	report, ok := b.auditReportGet(region, auditReportID)
	if !ok || report.CertificateAuthorityArn != caARN {
		return nil, fmt.Errorf("%w: audit report %s not found", ErrAuditReportNotFound, auditReportID)
	}

	cp := copyAuditReport(report)

	return &cp, nil
}

func copyAuditReport(report *AuditReport) AuditReport {
	return *report
}
