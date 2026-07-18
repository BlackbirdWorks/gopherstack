package acmpca

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type validityInput struct {
	Type  string `json:"Type"`
	Value int64  `json:"Value"`
}

type issueCertificateInput struct {
	CertificateAuthorityArn string        `json:"CertificateAuthorityArn"`
	Csr                     string        `json:"Csr"`
	SigningAlgorithm        string        `json:"SigningAlgorithm"`
	Validity                validityInput `json:"Validity"`
}

type issueCertificateOutput struct {
	CertificateArn string `json:"CertificateArn"`
}

type getCertificateInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	CertificateArn          string `json:"CertificateArn"`
}

type getCertificateOutput struct {
	Certificate      string `json:"Certificate"`
	CertificateChain string `json:"CertificateChain,omitempty"`
}

type revokeCertificateInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	CertificateSerial       string `json:"CertificateSerial"`
	RevocationReason        string `json:"RevocationReason"`
}

type revokeCertificateOutput struct{}

func (h *Handler) jsonIssueCert(ctx context.Context, body []byte) (any, error) {
	var input issueCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	csrPEM, err := decodeBase64Field(input.Csr, "Csr")
	if err != nil {
		return nil, err
	}

	var days int
	switch input.Validity.Type {
	case "YEARS":
		days = int(input.Validity.Value) * daysPerYear
	case "MONTHS":
		days = int(input.Validity.Value) * daysPerMonth
	case "DAYS", "":
		days = int(input.Validity.Value)
	case "END_DATE", "ABSOLUTE":
		endDate := time.Unix(input.Validity.Value, 0)
		days = int(time.Until(endDate).Hours() / hoursPerDay)
		if days <= 0 {
			days = 1
		}
	default:
		return nil, fmt.Errorf("%w: unsupported Validity.Type %q (must be DAYS, MONTHS, YEARS, or END_DATE)",
			ErrInvalidParameter, input.Validity.Type)
	}

	cert, err := h.Backend.IssueCertificate(ctx, input.CertificateAuthorityArn, csrPEM, days)
	if err != nil {
		return nil, err
	}

	return &issueCertificateOutput{CertificateArn: cert.ARN}, nil
}

func (h *Handler) jsonGetCert(ctx context.Context, body []byte) (any, error) {
	var input getCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	cert, err := h.Backend.GetCertificate(ctx, input.CertificateAuthorityArn, input.CertificateArn)
	if err != nil {
		return nil, err
	}

	caChain := ""
	if certPEM, chainPEM, chainErr := h.Backend.GetCertificateAuthorityCertificate(
		ctx,
		input.CertificateAuthorityArn,
	); chainErr == nil && certPEM != "" {
		caChain = certPEM
		if chainPEM != "" {
			caChain = certPEM + chainPEM
		}
	}

	return &getCertificateOutput{Certificate: cert.CertBody, CertificateChain: caChain}, nil
}

func (h *Handler) jsonRevokeCert(ctx context.Context, body []byte) (any, error) {
	var input revokeCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.RevokeCertificate(
		ctx,
		input.CertificateAuthorityArn,
		input.CertificateSerial,
		input.RevocationReason,
	); err != nil {
		return nil, err
	}

	return &revokeCertificateOutput{}, nil
}
