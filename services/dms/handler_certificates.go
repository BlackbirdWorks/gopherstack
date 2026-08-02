package dms

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type deleteCertificateInput struct {
	CertificateArn *string `json:"CertificateArn"`
}

type certificateJSON struct {
	CertificateIdentifier string `json:"CertificateIdentifier"`
	CertificateArn        string `json:"CertificateArn"`
	CertificatePem        string `json:"CertificatePem,omitempty"`
}

type deleteCertificateOutput struct {
	Certificate certificateJSON `json:"Certificate"`
}

func certToJSON(c *Certificate) certificateJSON {
	return certificateJSON{
		CertificateIdentifier: c.CertificateIdentifier,
		CertificateArn:        c.CertificateArn,
		CertificatePem:        c.CertificatePem,
	}
}

func (h *Handler) handleDeleteCertificate(
	ctx context.Context, in *deleteCertificateInput,
) (*deleteCertificateOutput, error) {
	cert, err := h.Backend.DeleteCertificate(ctx, ptrconv.String(in.CertificateArn))
	if err != nil {
		return nil, err
	}

	return &deleteCertificateOutput{Certificate: certToJSON(cert)}, nil
}

type describeCertificatesInput struct {
	Marker     *string `json:"Marker"`
	MaxRecords *int32  `json:"MaxRecords"`
}

type describeCertificatesOutput struct {
	Marker       *string           `json:"Marker,omitempty"`
	Certificates []certificateJSON `json:"Certificates"`
}

func (h *Handler) handleDescribeCertificates(
	ctx context.Context, in *describeCertificatesInput,
) (*describeCertificatesOutput, error) {
	list, err := h.Backend.DescribeCertificates(ctx)
	if err != nil {
		return nil, err
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].CertificateIdentifier < list[j].CertificateIdentifier
	})

	all := make([]certificateJSON, 0, len(list))
	for _, cert := range list {
		all = append(all, certToJSON(cert))
	}

	data, nextMarker := dmsPaginate(all, in.Marker, in.MaxRecords)

	return &describeCertificatesOutput{Certificates: data, Marker: nextMarker}, nil
}

type importCertificateInput struct {
	CertificateIdentifier *string `json:"CertificateIdentifier"`
	CertificatePem        *string `json:"CertificatePem"`
}

type importCertificateOutput struct {
	Certificate certificateJSON `json:"Certificate"`
}

func (h *Handler) handleImportCertificate(
	ctx context.Context, in *importCertificateInput,
) (*importCertificateOutput, error) {
	identifier := ptrconv.String(in.CertificateIdentifier)
	if identifier == "" {
		return nil, fmt.Errorf("%w: CertificateIdentifier is required", ErrValidation)
	}

	cert, err := h.Backend.ImportCertificate(ctx, identifier, ptrconv.String(in.CertificatePem))
	if err != nil {
		return nil, err
	}

	return &importCertificateOutput{Certificate: certToJSON(cert)}, nil
}

// opsCertificates returns the dispatch-table entries for the certificates operation family.
func (h *Handler) opsCertificates() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opDeleteCertificate: service.WrapOp(h.handleDeleteCertificate),
		opDescribeCertificates: service.WrapOp(
			h.handleDescribeCertificates,
		),
		opImportCertificate: service.WrapOp(h.handleImportCertificate),
	}
}
