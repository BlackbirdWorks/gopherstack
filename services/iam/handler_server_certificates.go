package iam

import (
	"encoding/xml"
	"net/url"
)

// iamServerCertReadDispatch returns the List/Get dispatch entries for server certificates.
func (h *Handler) iamServerCertReadDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"ListServerCertificates": func(vals url.Values, reqID string) (any, error) {
			certs, err := h.Backend.ListServerCertificates(vals.Get("PathPrefix"))
			if err != nil {
				return nil, err
			}

			members := make([]serverCertMetaXML, 0, len(certs))
			for _, c := range certs {
				members = append(members, serverCertMetaXML{
					ServerCertificateName: c.ServerCertificateName,
					ServerCertificateID:   c.ServerCertificateID,
					Arn:                   c.Arn,
					Path:                  c.Path,
					UploadDate:            isoTime(c.UploadDate),
				})
			}

			return &listServerCertificatesResponse{
				XMLName: xml.Name{Local: "ListServerCertificatesResponse"},
				Xmlns:   iamXMLNS,
				ListServerCertificatesResult: listServerCertificatesResult{
					ServerCertificateMetadataList: members,
					IsTruncated:                   false,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetServerCertificate": func(vals url.Values, reqID string) (any, error) {
			cert, err := h.Backend.GetServerCertificate(vals.Get("ServerCertificateName"))
			if err != nil {
				return nil, err
			}

			return &getServerCertificateResponse{
				XMLName: xml.Name{Local: "GetServerCertificateResponse"},
				Xmlns:   iamXMLNS,
				GetServerCertificateResult: getServerCertificateResult{
					ServerCertificate: serverCertXML{
						ServerCertificateMetadata: serverCertMetaXML{
							ServerCertificateName: cert.ServerCertificateName,
							ServerCertificateID:   cert.ServerCertificateID,
							Arn:                   cert.Arn,
							Path:                  cert.Path,
							UploadDate:            isoTime(cert.UploadDate),
						},
						CertificateBody:  cert.CertificateBody,
						CertificateChain: cert.CertificateChain,
					},
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamServerCertWriteDispatch returns the Upload/Update/Delete dispatch
// entries for server certificates. Split out from iamServerCertReadDispatch
// so neither function needs a funlen exemption.
func (h *Handler) iamServerCertWriteDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"DeleteServerCertificate": func(vals url.Values, reqID string) (any, error) {
			name := vals.Get("ServerCertificateName")
			if err := h.Backend.DeleteServerCertificate(name); err != nil {
				return nil, err
			}

			h.deleteTags("cert:" + name)

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "DeleteServerCertificateResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UpdateServerCertificate": func(vals url.Values, reqID string) (any, error) {
			oldName := vals.Get("ServerCertificateName")
			newName := vals.Get("NewServerCertificateName")

			if err := h.Backend.UpdateServerCertificate(
				oldName,
				newName,
				vals.Get("NewPath"),
			); err != nil {
				return nil, err
			}

			if newName != "" && newName != oldName {
				h.renameTags("cert:"+oldName, "cert:"+newName)
			}

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "UpdateServerCertificateResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UploadServerCertificate": func(vals url.Values, reqID string) (any, error) {
			cert, err := h.Backend.UploadServerCertificate(
				vals.Get("ServerCertificateName"),
				vals.Get("Path"),
				vals.Get("CertificateBody"),
				vals.Get("CertificateChain"),
			)
			if err != nil {
				return nil, err
			}

			return &uploadServerCertificateResponse{
				XMLName: xml.Name{Local: "UploadServerCertificateResponse"},
				Xmlns:   iamXMLNS,
				UploadServerCertificateResult: uploadServerCertificateResult{
					ServerCertificateMetadata: serverCertMetaXML{
						ServerCertificateName: cert.ServerCertificateName,
						ServerCertificateID:   cert.ServerCertificateID,
						Arn:                   cert.Arn,
						Path:                  cert.Path,
						UploadDate:            isoTime(cert.UploadDate),
					},
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamServerCertTagsDispatch returns the tag dispatch entries for server certificates.
func (h *Handler) iamServerCertTagsDispatch() map[string]iamActionFn {
	return h.resourceTagDispatch("ServerCertificate", "cert:", "ServerCertificateName")
}
