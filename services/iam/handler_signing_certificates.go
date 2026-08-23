package iam

import (
	"encoding/xml"
	"net/url"
)

// iamSigningCertificateDispatch returns dispatch entries for signing
// certificates added in the completeness pass (List/Delete/Update/Upload).
func (h *Handler) iamSigningCertificateDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"ListSigningCertificates": func(vals url.Values, reqID string) (any, error) {
			certs, err := h.Backend.ListSigningCertificates(vals.Get("UserName"))
			if err != nil {
				return nil, err
			}

			xmlCerts := make([]signingCertXML, 0, len(certs))
			for _, c := range certs {
				xmlCerts = append(xmlCerts, signingCertXML{
					CertificateID:   c.CertificateID,
					UserName:        c.UserName,
					CertificateBody: c.CertificateBody,
					Status:          c.Status,
					UploadDate:      isoTime(c.UploadDate),
				})
			}

			return &listSigningCertificatesResponse{
				XMLName: xml.Name{Local: "ListSigningCertificatesResponse"},
				Xmlns:   iamXMLNS,
				ListSigningCertificatesResult: listSigningCertificatesResult{
					Certificates: xmlCerts,
					IsTruncated:  false,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteSigningCertificate": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteSigningCertificate(vals.Get("UserName"), vals.Get("CertificateId")); err != nil {
				return nil, err
			}

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "DeleteSigningCertificateResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UpdateSigningCertificate": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.UpdateSigningCertificate(
				vals.Get("UserName"), vals.Get("CertificateId"), vals.Get("Status"),
			); err != nil {
				return nil, err
			}

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "UpdateSigningCertificateResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UploadSigningCertificate": func(vals url.Values, reqID string) (any, error) {
			cert, err := h.Backend.UploadSigningCertificate(
				vals.Get("UserName"), vals.Get("CertificateBody"),
			)
			if err != nil {
				return nil, err
			}

			return &uploadSigningCertificateResponse{
				XMLName: xml.Name{Local: "UploadSigningCertificateResponse"},
				Xmlns:   iamXMLNS,
				UploadSigningCertificateResult: uploadSigningCertificateResult{
					Certificate: signingCertXML{
						CertificateID:   cert.CertificateID,
						UserName:        cert.UserName,
						CertificateBody: cert.CertificateBody,
						Status:          cert.Status,
						UploadDate:      isoTime(cert.UploadDate),
					},
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}
