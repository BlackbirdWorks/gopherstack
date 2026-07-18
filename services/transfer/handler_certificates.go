package transfer

import (
	"context"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

type deleteCertificateInput struct {
	CertificateID string `json:"CertificateId"`
}

func (h *Handler) handleDeleteCertificate(
	_ context.Context,
	in *deleteCertificateInput,
) (*struct{}, error) {
	if in.CertificateID == "" {
		return nil, fmt.Errorf("%w: CertificateId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteCertificate(in.CertificateID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// certificateARN builds the ARN for a Transfer certificate.
func certificateARN(accountID, region, certificateID string) string {
	return arn.Build("transfer", region, accountID, "certificate/"+certificateID)
}

// parseCertDate parses an RFC3339 date string for use as a certificate date fallback.
// When the certificate body is non-empty, dates come from the PEM, so this returns zero.
func parseCertDate(body, dateStr string) time.Time {
	if body != "" || dateStr == "" {
		return time.Time{}
	}

	t, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return time.Time{}
	}

	return t
}

type importCertificateInput struct {
	Usage         string              `json:"Usage"`
	Body          string              `json:"Certificate"`
	Description   string              `json:"Description,omitempty"`
	NotBeforeDate string              `json:"NotBeforeDate,omitempty"` // RFC3339
	NotAfterDate  string              `json:"NotAfterDate,omitempty"`  // RFC3339
	Tags          []map[string]string `json:"Tags"`
}

type importCertificateOutput struct {
	CertificateID string `json:"CertificateId"`
}

func (h *Handler) handleImportCertificate(
	_ context.Context,
	in *importCertificateInput,
) (*importCertificateOutput, error) {
	if in.Usage == "" {
		return nil, fmt.Errorf("%w: Usage is required", errInvalidRequest)
	}

	switch in.Usage {
	case "SIGNING", "ENCRYPTION", "TLS":
		// valid
	default:
		return nil, fmt.Errorf(
			"%w: Usage must be SIGNING, ENCRYPTION, or TLS, got %q",
			errInvalidRequest,
			in.Usage,
		)
	}

	tags := tagsFromList(in.Tags)

	// If body is provided, the backend will parse PEM and extract dates.
	// Only use user-provided dates as fallback when no body.
	notBefore := parseCertDate(in.Body, in.NotBeforeDate)
	notAfter := parseCertDate(in.Body, in.NotAfterDate)

	c, err := h.Backend.ImportCertificate(
		in.Usage,
		in.Body,
		in.Description,
		notBefore,
		notAfter,
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &importCertificateOutput{CertificateID: c.CertificateID}, nil
}

type describeCertificateInput struct {
	CertificateID string `json:"CertificateId"`
}

type describeCertificateOutput struct {
	Certificate map[string]any `json:"Certificate"`
}

func (h *Handler) handleDescribeCertificate(
	_ context.Context,
	in *describeCertificateInput,
) (*describeCertificateOutput, error) {
	if in.CertificateID == "" {
		return nil, fmt.Errorf("%w: CertificateId is required", errInvalidRequest)
	}

	c, err := h.Backend.DescribeCertificate(in.CertificateID)
	if err != nil {
		return nil, err
	}

	certMap := map[string]any{
		"CertificateId": c.CertificateID,
		"Usage":         c.Usage,
		keyDescription:  c.Description,
		keyStatus:       c.Status,
		keyArn:          certificateARN(c.AccountID, c.Region, c.CertificateID),
	}

	if c.Body != "" {
		certMap["Certificate"] = c.Body
	}

	if !c.NotBeforeDate.IsZero() {
		certMap["NotBeforeDate"] = c.NotBeforeDate.Format(time.RFC3339)
	}

	if !c.NotAfterDate.IsZero() {
		certMap["NotAfterDate"] = c.NotAfterDate.Format(time.RFC3339)
	}

	return &describeCertificateOutput{
		Certificate: certMap,
	}, nil
}

type listCertificatesInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listCertificatesOutput struct {
	NextToken    string           `json:"NextToken,omitempty"`
	Certificates []map[string]any `json:"Certificates"`
}

func (h *Handler) handleListCertificates(
	_ context.Context,
	in *listCertificatesInput,
) (*listCertificatesOutput, error) {
	items := h.Backend.ListCertificates()
	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, c := range page {
		out[i] = map[string]any{
			"CertificateId": c.CertificateID,
			"Usage":         c.Usage,
			keyStatus:       c.Status,
			keyArn:          certificateARN(c.AccountID, c.Region, c.CertificateID),
		}
	}

	return &listCertificatesOutput{Certificates: out, NextToken: next}, nil
}

type updateCertificateInput struct {
	CertificateID string `json:"CertificateId"`
	Description   string `json:"Description"`
}

type updateCertificateOutput struct {
	CertificateID string `json:"CertificateId"`
}

func (h *Handler) handleUpdateCertificate(
	_ context.Context,
	in *updateCertificateInput,
) (*updateCertificateOutput, error) {
	if in.CertificateID == "" {
		return nil, fmt.Errorf("%w: CertificateId is required", errInvalidRequest)
	}

	c, err := h.Backend.UpdateCertificate(in.CertificateID, in.Description)
	if err != nil {
		return nil, err
	}

	return &updateCertificateOutput{CertificateID: c.CertificateID}, nil
}
