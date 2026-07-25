package transfer

import (
	"context"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
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
	Usage            string              `json:"Usage"`
	Body             string              `json:"Certificate"`
	CertificateChain string              `json:"CertificateChain,omitempty"`
	PrivateKey       string              `json:"PrivateKey,omitempty"`
	Description      string              `json:"Description,omitempty"`
	NotBeforeDate    string              `json:"NotBeforeDate,omitempty"` // RFC3339
	NotAfterDate     string              `json:"NotAfterDate,omitempty"`  // RFC3339
	ActiveDate       string              `json:"ActiveDate,omitempty"`    // RFC3339
	InactiveDate     string              `json:"InactiveDate,omitempty"`  // RFC3339
	Tags             []map[string]string `json:"Tags"`
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

	c, err := h.Backend.ImportCertificateFull(&ImportCertificateInput{
		Usage:            in.Usage,
		Body:             in.Body,
		CertificateChain: in.CertificateChain,
		PrivateKey:       in.PrivateKey,
		Description:      in.Description,
		NotBefore:        notBefore,
		NotAfter:         notAfter,
		ActiveDate:       parseCertDate("", in.ActiveDate),
		InactiveDate:     parseCertDate("", in.InactiveDate),
		Tags:             tags,
	})
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
		"Type":          certificateType(c),
		keyDescription:  c.Description,
		keyStatus:       c.Status,
		keyArn:          certificateARN(c.AccountID, c.Region, c.CertificateID),
	}

	if c.Body != "" {
		certMap["Certificate"] = c.Body
	}

	if c.CertificateChain != "" {
		certMap["CertificateChain"] = c.CertificateChain
	}

	if c.Serial != "" {
		certMap["Serial"] = c.Serial
	}

	if !c.NotBeforeDate.IsZero() {
		certMap["NotBeforeDate"] = awstime.Epoch(c.NotBeforeDate)
	}

	if !c.NotAfterDate.IsZero() {
		certMap["NotAfterDate"] = awstime.Epoch(c.NotAfterDate)
	}

	if !c.ActiveDate.IsZero() {
		certMap["ActiveDate"] = awstime.Epoch(c.ActiveDate)
	}

	if !c.InactiveDate.IsZero() {
		certMap["InactiveDate"] = awstime.Epoch(c.InactiveDate)
	}

	return &describeCertificateOutput{
		Certificate: certMap,
	}, nil
}

// certificateType returns the real-AWS CertificateType ("CERTIFICATE" or
// "CERTIFICATE_WITH_PRIVATE_KEY") for a certificate.
func certificateType(c *Certificate) string {
	if c.HasPrivateKey {
		return "CERTIFICATE_WITH_PRIVATE_KEY"
	}

	return "CERTIFICATE"
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
		item := map[string]any{
			"CertificateId": c.CertificateID,
			"Type":          certificateType(c),
			keyDescription:  c.Description,
			keyStatus:       c.Status,
			keyArn:          certificateARN(c.AccountID, c.Region, c.CertificateID),
		}

		if !c.ActiveDate.IsZero() {
			item["ActiveDate"] = awstime.Epoch(c.ActiveDate)
		}

		if !c.InactiveDate.IsZero() {
			item["InactiveDate"] = awstime.Epoch(c.InactiveDate)
		}

		out[i] = item
	}

	return &listCertificatesOutput{Certificates: out, NextToken: next}, nil
}

type updateCertificateInput struct {
	CertificateID string `json:"CertificateId"`
	Description   string `json:"Description"`
	ActiveDate    string `json:"ActiveDate,omitempty"`   // RFC3339
	InactiveDate  string `json:"InactiveDate,omitempty"` // RFC3339
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

	c, err := h.Backend.UpdateCertificateFull(&UpdateCertificateInput{
		CertificateID: in.CertificateID,
		Description:   in.Description,
		ActiveDate:    parseCertDate("", in.ActiveDate),
		InactiveDate:  parseCertDate("", in.InactiveDate),
	})
	if err != nil {
		return nil, err
	}

	return &updateCertificateOutput{CertificateID: c.CertificateID}, nil
}
