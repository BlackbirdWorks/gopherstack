package iam

import (
	"encoding/xml"
	"errors"
	"maps"
	"net/url"
	"time"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func isRoleNotFound(err error) bool { return errors.Is(err, ErrRoleNotFound) }

// jobStatusCompleted is the status returned by async IAM job stubs.
const jobStatusCompleted = "COMPLETED"

// iamCompletenessDispatchTable returns the dispatch table for all previously
// notImplemented IAM operations.  Trivial stubs return empty-but-valid
// responses; key operations have real logic.
func (h *Handler) iamCompletenessDispatchTable() map[string]iamActionFn {
	combined := make(map[string]iamActionFn)
	maps.Copy(combined, h.iamInstanceProfileTagDispatch())
	maps.Copy(combined, h.iamMFADeviceDispatch())
	maps.Copy(combined, h.iamOIDCTagDispatch())
	maps.Copy(combined, h.iamSAMLTagDispatch())
	maps.Copy(combined, h.iamServerCertDispatch())
	maps.Copy(combined, h.iamSSHSigningDispatch())
	maps.Copy(combined, h.iamOrgsDispatch())
	maps.Copy(combined, h.iamDelegationDispatch())
	maps.Copy(combined, h.iamOrgsReportDispatch())

	return combined
}

// ----- Instance Profile Tags -----

//nolint:dupl // tag dispatch functions share structure by design
func (h *Handler) iamInstanceProfileTagDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"ListInstanceProfileTags": func(vals url.Values, reqID string) (any, error) {
			name := vals.Get("InstanceProfileName")
			tags := h.getTags("ip:" + name)
			members := make([]svcTags.KV, 0, len(tags))
			for k, v := range tags {
				members = append(members, svcTags.KV{Key: k, Value: v})
			}

			return &iamListTagsResponse{
				XMLName: xml.Name{Local: "ListInstanceProfileTagsResponse"},
				Xmlns:   iamXMLNS,
				Result: iamListTagsResult{
					XMLName: xml.Name{Local: "ListInstanceProfileTagsResult"},
					Tags:    members,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"TagInstanceProfile": func(vals url.Values, reqID string) (any, error) {
			h.setTags("ip:"+vals.Get("InstanceProfileName"), parseIAMTags(vals))

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "TagInstanceProfileResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UntagInstanceProfile": func(vals url.Values, reqID string) (any, error) {
			h.removeTags("ip:"+vals.Get("InstanceProfileName"), parseIAMTagKeys(vals))

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "UntagInstanceProfileResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// ----- MFA Devices -----

func (h *Handler) iamMFADeviceDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"ListMFADevices": func(vals url.Values, reqID string) (any, error) {
			userName := vals.Get("UserName")
			devices, err := h.Backend.ListMFADevicesForUser(userName)
			if err != nil {
				// If user not found, return empty list (matches AWS behavior for optional UserName).
				devices = nil
			}

			members := make([]mfaDeviceXML, 0, len(devices))
			for _, d := range devices {
				members = append(members, mfaDeviceXML{
					UserName:     h.Backend.GetMFADeviceOwner(d.SerialNumber),
					SerialNumber: d.SerialNumber,
					EnableDate:   isoTime(d.CreateDate),
				})
			}

			return &listMFADevicesResponse{
				XMLName: xml.Name{Local: "ListMFADevicesResponse"},
				Xmlns:   iamXMLNS,
				ListMFADevicesResult: listMFADevicesResult{
					MFADevices:  members,
					IsTruncated: false,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListMFADeviceTags": func(vals url.Values, reqID string) (any, error) {
			serial := vals.Get("SerialNumber")
			tags := h.getTags("mfa:" + serial)
			members := make([]svcTags.KV, 0, len(tags))
			for k, v := range tags {
				members = append(members, svcTags.KV{Key: k, Value: v})
			}

			return &iamListTagsResponse{
				XMLName:          xml.Name{Local: "ListMFADeviceTagsResponse"},
				Xmlns:            iamXMLNS,
				Result:           iamListTagsResult{XMLName: xml.Name{Local: "ListMFADeviceTagsResult"}, Tags: members},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"TagMFADevice": func(vals url.Values, reqID string) (any, error) {
			h.setTags("mfa:"+vals.Get("SerialNumber"), parseIAMTags(vals))

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "TagMFADeviceResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UntagMFADevice": func(vals url.Values, reqID string) (any, error) {
			h.removeTags("mfa:"+vals.Get("SerialNumber"), parseIAMTagKeys(vals))

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "UntagMFADeviceResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeactivateMFADevice": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeactivateMFADevice(vals.Get("UserName"), vals.Get("SerialNumber")); err != nil {
				return nil, err
			}

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "DeactivateMFADeviceResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"EnableMFADevice": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.EnableMFADevice(
				vals.Get("UserName"), vals.Get("SerialNumber"),
				vals.Get("AuthenticationCode1"), vals.Get("AuthenticationCode2"),
			); err != nil {
				return nil, err
			}

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "EnableMFADeviceResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ResyncMFADevice": func(_ url.Values, reqID string) (any, error) {
			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "ResyncMFADeviceResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// ----- OIDC Provider Tags -----

//nolint:dupl // tag dispatch functions share structure by design
func (h *Handler) iamOIDCTagDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"ListOpenIDConnectProviderTags": func(vals url.Values, reqID string) (any, error) {
			arn := vals.Get("OpenIDConnectProviderArn")
			tags := h.getTags("oidc:" + arn)
			members := make([]svcTags.KV, 0, len(tags))
			for k, v := range tags {
				members = append(members, svcTags.KV{Key: k, Value: v})
			}

			return &iamListTagsResponse{
				XMLName: xml.Name{Local: "ListOpenIDConnectProviderTagsResponse"},
				Xmlns:   iamXMLNS,
				Result: iamListTagsResult{
					XMLName: xml.Name{Local: "ListOpenIDConnectProviderTagsResult"},
					Tags:    members,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"TagOpenIDConnectProvider": func(vals url.Values, reqID string) (any, error) {
			h.setTags("oidc:"+vals.Get("OpenIDConnectProviderArn"), parseIAMTags(vals))

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "TagOpenIDConnectProviderResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UntagOpenIDConnectProvider": func(vals url.Values, reqID string) (any, error) {
			h.removeTags("oidc:"+vals.Get("OpenIDConnectProviderArn"), parseIAMTagKeys(vals))

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "UntagOpenIDConnectProviderResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// ----- SAML Provider Tags -----

//nolint:dupl // tag dispatch functions share structure by design
func (h *Handler) iamSAMLTagDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"ListSAMLProviderTags": func(vals url.Values, reqID string) (any, error) {
			arn := vals.Get("SAMLProviderArn")
			tags := h.getTags("saml:" + arn)
			members := make([]svcTags.KV, 0, len(tags))
			for k, v := range tags {
				members = append(members, svcTags.KV{Key: k, Value: v})
			}

			return &iamListTagsResponse{
				XMLName: xml.Name{Local: "ListSAMLProviderTagsResponse"},
				Xmlns:   iamXMLNS,
				Result: iamListTagsResult{
					XMLName: xml.Name{Local: "ListSAMLProviderTagsResult"},
					Tags:    members,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"TagSAMLProvider": func(vals url.Values, reqID string) (any, error) {
			h.setTags("saml:"+vals.Get("SAMLProviderArn"), parseIAMTags(vals))

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "TagSAMLProviderResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UntagSAMLProvider": func(vals url.Values, reqID string) (any, error) {
			h.removeTags("saml:"+vals.Get("SAMLProviderArn"), parseIAMTagKeys(vals))

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "UntagSAMLProviderResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// ----- Server Certificates -----

//nolint:funlen // contains all operations for this IAM resource type
func (h *Handler) iamServerCertDispatch() map[string]iamActionFn {
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
		"ListServerCertificateTags": func(vals url.Values, reqID string) (any, error) {
			name := vals.Get("ServerCertificateName")
			tags := h.getTags("cert:" + name)
			members := make([]svcTags.KV, 0, len(tags))
			for k, v := range tags {
				members = append(members, svcTags.KV{Key: k, Value: v})
			}

			return &iamListTagsResponse{
				XMLName: xml.Name{Local: "ListServerCertificateTagsResponse"},
				Xmlns:   iamXMLNS,
				Result: iamListTagsResult{
					XMLName: xml.Name{Local: "ListServerCertificateTagsResult"},
					Tags:    members,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"TagServerCertificate": func(vals url.Values, reqID string) (any, error) {
			h.setTags("cert:"+vals.Get("ServerCertificateName"), parseIAMTags(vals))

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "TagServerCertificateResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UntagServerCertificate": func(vals url.Values, reqID string) (any, error) {
			h.removeTags("cert:"+vals.Get("ServerCertificateName"), parseIAMTagKeys(vals))

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "UntagServerCertificateResponse"},
				Xmlns:            iamXMLNS,
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
		"DeleteServerCertificate": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteServerCertificate(vals.Get("ServerCertificateName")); err != nil {
				return nil, err
			}

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "DeleteServerCertificateResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UpdateServerCertificate": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.UpdateServerCertificate(
				vals.Get("ServerCertificateName"),
				vals.Get("NewServerCertificateName"),
				vals.Get("NewPath"),
			); err != nil {
				return nil, err
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

// ----- SSH Public Keys and Signing Certificates -----

//nolint:funlen,gocognit // contains all operations for this IAM resource type
func (h *Handler) iamSSHSigningDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"ListSSHPublicKeys": func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.ListSSHPublicKeys(vals.Get("UserName"), vals.Get("Marker"), iamDefaultMaxItems)
			if err != nil {
				return nil, err
			}

			members := make([]sshPublicKeyMetaXML, 0, len(p.Data))
			for _, k := range p.Data {
				members = append(members, sshPublicKeyMetaXML{
					UserName:       k.UserName,
					SSHPublicKeyID: k.SSHPublicKeyID,
					Status:         k.Status,
					UploadDate:     isoTime(k.UploadDate),
				})
			}

			return &listSSHPublicKeysResponse{
				XMLName: xml.Name{Local: "ListSSHPublicKeysResponse"},
				Xmlns:   iamXMLNS,
				ListSSHPublicKeysResult: listSSHPublicKeysResult{
					SSHPublicKeys: members,
					IsTruncated:   p.Next != "",
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetSSHPublicKey": func(vals url.Values, reqID string) (any, error) {
			key, err := h.Backend.GetSSHPublicKey(vals.Get("UserName"), vals.Get("SSHPublicKeyId"))
			if err != nil {
				return nil, err
			}

			return &getSSHPublicKeyResponse{
				XMLName: xml.Name{Local: "GetSSHPublicKeyResponse"},
				Xmlns:   iamXMLNS,
				GetSSHPublicKeyResult: getSSHPublicKeyResult{
					SSHPublicKey: sshPublicKeyXML{
						UserName:         key.UserName,
						SSHPublicKeyID:   key.SSHPublicKeyID,
						Fingerprint:      key.Fingerprint,
						SSHPublicKeyBody: key.SSHPublicKeyBody,
						Status:           key.Status,
						UploadDate:       isoTime(key.UploadDate),
					},
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteSSHPublicKey": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteSSHPublicKey(vals.Get("UserName"), vals.Get("SSHPublicKeyId")); err != nil {
				return nil, err
			}

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "DeleteSSHPublicKeyResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UpdateSSHPublicKey": func(vals url.Values, reqID string) (any, error) {
			userName := vals.Get("UserName")
			keyID := vals.Get("SSHPublicKeyId")
			status := vals.Get("Status")

			if err := h.Backend.UpdateSSHPublicKey(userName, keyID, status); err != nil {
				return nil, err
			}

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "UpdateSSHPublicKeyResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UploadSSHPublicKey": func(vals url.Values, reqID string) (any, error) {
			key, err := h.Backend.UploadSSHPublicKey(vals.Get("UserName"), vals.Get("SSHPublicKeyBody"))
			if err != nil {
				return nil, err
			}

			return &uploadSSHPublicKeyResponse{
				XMLName: xml.Name{Local: "UploadSSHPublicKeyResponse"},
				Xmlns:   iamXMLNS,
				UploadSSHPublicKeyResult: uploadSSHPublicKeyResult{
					SSHPublicKey: sshPublicKeyXML{
						UserName:         key.UserName,
						SSHPublicKeyID:   key.SSHPublicKeyID,
						Fingerprint:      key.Fingerprint,
						SSHPublicKeyBody: key.SSHPublicKeyBody,
						Status:           key.Status,
						UploadDate:       isoTime(key.UploadDate),
					},
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
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
			if err := h.Backend.DeleteSigningCertificate(vals.Get("CertificateId")); err != nil {
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
				vals.Get("CertificateId"), vals.Get("Status"),
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
		"ResetServiceSpecificCredential": func(vals url.Values, reqID string) (any, error) {
			return &resetServiceSpecificCredentialResponse{
				XMLName: xml.Name{Local: "ResetServiceSpecificCredentialResponse"},
				Xmlns:   iamXMLNS,
				ResetServiceSpecificCredentialResult: resetSSCResult{
					ServiceSpecificCredential: serviceSpecificCredXML{
						UserName:                    vals.Get("UserName"),
						ServiceSpecificCredentialID: vals.Get("ServiceSpecificCredentialId"),
						ServiceName:                 "codecommit.amazonaws.com",
						ServiceUserName:             vals.Get("UserName") + "-at-123456789",
						ServicePassword:             "reset-password-" + newRequestID(),
						Status:                      accessKeyStatusActive,
						CreateDate:                  isoTime(time.Now()),
					},
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteServiceLinkedRole": func(vals url.Values, reqID string) (any, error) {
			roleName := vals.Get("RoleName")
			// Idempotent: ignore "not found" to match AWS async-deletion semantics.
			if err := h.Backend.DeleteServiceLinkedRole(roleName); err != nil && !isRoleNotFound(err) {
				return nil, err
			}

			return &deleteServiceLinkedRoleResponse{
				XMLName: xml.Name{Local: "DeleteServiceLinkedRoleResponse"},
				Xmlns:   iamXMLNS,
				DeleteServiceLinkedRoleResult: deleteServiceLinkedRoleResult{
					DeletionTaskID: "task/" + roleName + "/" + newRequestID(),
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// ----- Organizations stubs -----

func (h *Handler) iamOrgsDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"DisableOrganizationsRootCredentialsManagement": func(_ url.Values, reqID string) (any, error) {
			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "DisableOrganizationsRootCredentialsManagementResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DisableOrganizationsRootSessions": func(_ url.Values, reqID string) (any, error) {
			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "DisableOrganizationsRootSessionsResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DisableOutboundWebIdentityFederation": func(_ url.Values, reqID string) (any, error) {
			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "DisableOutboundWebIdentityFederationResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"EnableOrganizationsRootCredentialsManagement": func(_ url.Values, reqID string) (any, error) {
			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "EnableOrganizationsRootCredentialsManagementResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"EnableOrganizationsRootSessions": func(_ url.Values, reqID string) (any, error) {
			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "EnableOrganizationsRootSessionsResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"EnableOutboundWebIdentityFederation": func(_ url.Values, reqID string) (any, error) {
			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "EnableOutboundWebIdentityFederationResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListOrganizationsFeatures": func(_ url.Values, reqID string) (any, error) {
			return &listOrganizationsFeaturesResponse{
				XMLName: xml.Name{Local: "ListOrganizationsFeaturesResponse"},
				Xmlns:   iamXMLNS,
				ListOrganizationsFeaturesResult: listOrganizationsFeaturesResult{
					OrganizationFeatures: []string{},
					RootID:               "",
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListPoliciesGrantingServiceAccess": func(_ url.Values, reqID string) (any, error) {
			return &listPoliciesGrantingServiceAccessResponse{
				XMLName: xml.Name{Local: "ListPoliciesGrantingServiceAccessResponse"},
				Xmlns:   iamXMLNS,
				ListPoliciesGrantingServiceAccessResult: listPGSAResult{
					PolicyGroups: []string{},
					IsTruncated:  false,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// ----- Delegation stubs -----

func (h *Handler) iamDelegationDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"GetDelegationRequest": func(_ url.Values, reqID string) (any, error) {
			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "GetDelegationRequestResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetHumanReadableSummary": func(_ url.Values, reqID string) (any, error) {
			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "GetHumanReadableSummaryResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetOutboundWebIdentityFederationInfo": func(_ url.Values, reqID string) (any, error) {
			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "GetOutboundWebIdentityFederationInfoResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListDelegationRequests": func(_ url.Values, reqID string) (any, error) {
			return &listDelegationRequestsResponse{
				XMLName: xml.Name{Local: "ListDelegationRequestsResponse"},
				Xmlns:   iamXMLNS,
				ListDelegationRequestsResult: listDelegationRequestsResult{
					DelegationRequests: []string{},
					IsTruncated:        false,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"RejectDelegationRequest": func(_ url.Values, reqID string) (any, error) {
			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "RejectDelegationRequestResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"SendDelegationToken": func(_ url.Values, reqID string) (any, error) {
			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "SendDelegationTokenResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UpdateDelegationRequest": func(_ url.Values, reqID string) (any, error) {
			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "UpdateDelegationRequestResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// ----- Organizations report stubs -----

func (h *Handler) iamOrgsReportDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"GenerateOrganizationsAccessReport": func(vals url.Values, reqID string) (any, error) {
			jobID := h.Backend.GenerateOrganizationsAccessReport(vals.Get("EntityPath"))

			return &generateOrganizationsAccessReportResponse{
				XMLName: xml.Name{Local: "GenerateOrganizationsAccessReportResponse"},
				Xmlns:   iamXMLNS,
				GenerateOrganizationsAccessReportResult: generateOrgAccessReportResult{
					JobID: jobID,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GenerateServiceLastAccessedDetails": func(_ url.Values, reqID string) (any, error) {
			return &generateServiceLastAccessedDetailsResponse{
				XMLName: xml.Name{Local: "GenerateServiceLastAccessedDetailsResponse"},
				Xmlns:   iamXMLNS,
				GenerateServiceLastAccessedDetailsResult: generateSLADResult{
					JobID: "sladjob-" + reqID,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetOrganizationsAccessReport": func(vals url.Values, reqID string) (any, error) {
			jobID := vals.Get("JobId")
			status, createdAt, found := h.Backend.GetOrganizationsAccessReport(jobID)
			if !found {
				status = jobStatusCompleted
				createdAt = time.Now()
			}

			return &getOrganizationsAccessReportResponse{
				XMLName: xml.Name{Local: "GetOrganizationsAccessReportResponse"},
				Xmlns:   iamXMLNS,
				GetOrganizationsAccessReportResult: getOrgAccessReportResult{
					JobStatus:                   status,
					JobCreationDate:             isoTime(createdAt),
					AccessDetails:               []string{},
					IsTruncated:                 false,
					NumberOfServicesAccessible:  0,
					NumberOfServicesNotAccessed: 0,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetServiceLastAccessedDetailsWithEntities": func(_ url.Values, reqID string) (any, error) {
			return &getServiceLastAccessedDetailsWithEntitiesResponse{
				XMLName: xml.Name{Local: "GetServiceLastAccessedDetailsWithEntitiesResponse"},
				Xmlns:   iamXMLNS,
				GetServiceLastAccessedDetailsWithEntitiesResult: getSLADWithEntitiesResult{
					JobStatus:         jobStatusCompleted,
					JobCreationDate:   isoTime(time.Now()),
					EntityDetailsList: []string{},
					IsTruncated:       false,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}
