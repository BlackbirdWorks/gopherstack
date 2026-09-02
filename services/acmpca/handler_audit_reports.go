package acmpca

import (
	"context"
	"encoding/json"
)

type createCertificateAuthorityAuditReportInput struct {
	AuditReportResponseFormat string `json:"AuditReportResponseFormat"`
	CertificateAuthorityArn   string `json:"CertificateAuthorityArn"`
	S3BucketName              string `json:"S3BucketName"`
}

type createCertificateAuthorityAuditReportOutput struct {
	AuditReportID string `json:"AuditReportId,omitempty"`
	S3Key         string `json:"S3Key,omitempty"`
}

type describeCertificateAuthorityAuditReportInput struct {
	AuditReportID           string `json:"AuditReportId"`
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
}

type describeCertificateAuthorityAuditReportOutput struct {
	AuditReportStatus string `json:"AuditReportStatus,omitempty"`
	S3BucketName      string `json:"S3BucketName,omitempty"`
	S3Key             string `json:"S3Key,omitempty"`
	CreatedAt         int64  `json:"CreatedAt,omitempty"`
}

func (h *Handler) jsonCreateAuditReport(ctx context.Context, body []byte) (any, error) {
	var input createCertificateAuthorityAuditReportInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidArgs
	}

	report, err := h.Backend.CreateCertificateAuthorityAuditReport(
		ctx,
		input.CertificateAuthorityArn,
		input.S3BucketName,
		input.AuditReportResponseFormat,
	)
	if err != nil {
		return nil, err
	}

	return &createCertificateAuthorityAuditReportOutput{
		AuditReportID: report.AuditReportID,
		S3Key:         report.S3Key,
	}, nil
}

func (h *Handler) jsonDescribeAuditReport(ctx context.Context, body []byte) (any, error) {
	var input describeCertificateAuthorityAuditReportInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidArgs
	}

	report, err := h.Backend.DescribeCertificateAuthorityAuditReport(
		ctx,
		input.CertificateAuthorityArn,
		input.AuditReportID,
	)
	if err != nil {
		return nil, err
	}

	out := &describeCertificateAuthorityAuditReportOutput{
		AuditReportStatus: report.Status,
		S3BucketName:      report.S3BucketName,
		S3Key:             report.S3Key,
	}
	if !report.CreatedAt.IsZero() {
		out.CreatedAt = report.CreatedAt.Unix()
	}

	return out, nil
}
