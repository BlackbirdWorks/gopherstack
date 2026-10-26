package sesv2

import (
	"encoding/json"
	"fmt"

	"github.com/labstack/echo/v5"
)

type identityCertificateOutput struct {
	CertificateArn string `json:"CertificateArn"`
	FromAddress    string `json:"FromAddress"`
	Status         string `json:"Status"`
}

type listEmailIdentityCertificatesOutput struct {
	NextToken    string                      `json:"NextToken,omitempty"`
	Certificates []identityCertificateOutput `json:"Certificates"`
}

func decodeBody(c *echo.Context, in any) error {
	if err := json.NewDecoder(c.Request().Body).Decode(in); err != nil {
		return fmt.Errorf("%w: invalid request body: %s", ErrInvalidParameter, err.Error())
	}

	return nil
}

func (h *Handler) handleAssociateEmailIdentityCertificate(c *echo.Context) (any, error) {
	var in struct {
		CertificateArn string `json:"CertificateArn"`
		EmailIdentity  string `json:"EmailIdentity"`
		FromAddress    string `json:"FromAddress"`
	}

	if err := decodeBody(c, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.AssociateEmailIdentityCertificate(
		in.EmailIdentity,
		in.FromAddress,
		in.CertificateArn,
	); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleDisassociateEmailIdentityCertificate(c *echo.Context) (any, error) {
	var in struct {
		EmailIdentity string `json:"EmailIdentity"`
		FromAddress   string `json:"FromAddress"`
	}

	if err := decodeBody(c, &in); err != nil {
		return nil, err
	}

	if err := h.Backend.DisassociateEmailIdentityCertificate(in.EmailIdentity, in.FromAddress); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleListEmailIdentityCertificates(c *echo.Context) (any, error) {
	var in struct {
		EmailIdentity string `json:"EmailIdentity"`
		NextToken     string `json:"NextToken"`
		PageSize      int    `json:"PageSize"`
	}

	if err := decodeBody(c, &in); err != nil {
		return nil, err
	}

	pg, err := h.Backend.ListEmailIdentityCertificates(in.EmailIdentity, in.NextToken, max(in.PageSize, 0))
	if err != nil {
		return nil, err
	}

	out := &listEmailIdentityCertificatesOutput{
		NextToken:    pg.Next,
		Certificates: make([]identityCertificateOutput, 0, len(pg.Data)),
	}

	for _, cert := range pg.Data {
		out.Certificates = append(out.Certificates, identityCertificateOutput{
			CertificateArn: cert.CertificateARN,
			FromAddress:    cert.FromAddress,
			Status:         cert.Status,
		})
	}

	return out, nil
}
