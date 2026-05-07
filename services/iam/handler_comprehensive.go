package iam

import (
	"encoding/xml"
	"maps"
	"net/url"
	"time"
)

// xmlLocalName is a helper to construct an xml.Name with only the Local field set.
func xmlLocalName(name string) xml.Name {
	return xml.Name{Local: name}
}

// IAM operation name constants for comprehensive operations.
const (
	opUploadSSHPublicKey             = "UploadSSHPublicKey"
	opGetSSHPublicKey                = "GetSSHPublicKey"
	opListSSHPublicKeys              = "ListSSHPublicKeys"
	opUpdateSSHPublicKey             = "UpdateSSHPublicKey"
	opDeleteSSHPublicKey             = "DeleteSSHPublicKey"
	opEnableMFADevice                = "EnableMFADevice"
	opDeactivateMFADevice            = "DeactivateMFADevice"
	opListMFADevices                 = "ListMFADevices"
	opGenerateServiceLastAccessed    = "GenerateServiceLastAccessedDetails"
	opGetServiceLastAccessed         = "GetServiceLastAccessedDetails"
	opResetServiceSpecificCredential = "ResetServiceSpecificCredential"
	opCreateVirtualMFADevice         = "CreateVirtualMFADevice"
)

// iamComprehensiveDispatchTable returns dispatch entries for comprehensive IAM operations:
// SSH keys, MFA device linking, real access advisor, reset service-specific credential.
// These entries override earlier stub implementations.
func (h *Handler) iamComprehensiveDispatchTable() map[string]iamActionFn {
	combined := make(map[string]iamActionFn)
	maps.Copy(combined, h.iamSSHKeyUploadGetDispatch())
	maps.Copy(combined, h.iamSSHKeyListDeleteDispatch())
	maps.Copy(combined, h.iamMFALinkDispatch())
	maps.Copy(combined, h.iamAccessAdvisorDispatch())
	maps.Copy(combined, h.iamSSCResetDispatch())
	maps.Copy(combined, h.iamVirtualMFAFullDispatch())

	return combined
}

// iamSSHKeyUploadGetDispatch wires UploadSSHPublicKey and GetSSHPublicKey with real storage.
func (h *Handler) iamSSHKeyUploadGetDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		opUploadSSHPublicKey: func(vals url.Values, reqID string) (any, error) {
			key, err := h.Backend.UploadSSHPublicKey(
				vals.Get("UserName"),
				vals.Get("SSHPublicKeyBody"),
			)
			if err != nil {
				return nil, err
			}

			return &uploadSSHPublicKeyResponse{
				XMLName: xmlLocalName("UploadSSHPublicKeyResponse"),
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

		opGetSSHPublicKey: func(vals url.Values, reqID string) (any, error) {
			key, err := h.Backend.GetSSHPublicKey(
				vals.Get("UserName"),
				vals.Get("SSHPublicKeyId"),
			)
			if err != nil {
				return nil, err
			}

			return &getSSHPublicKeyResponse{
				XMLName: xmlLocalName("GetSSHPublicKeyResponse"),
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
	}
}

// iamSSHKeyListDeleteDispatch wires ListSSHPublicKeys, UpdateSSHPublicKey, DeleteSSHPublicKey.
func (h *Handler) iamSSHKeyListDeleteDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		opListSSHPublicKeys: func(vals url.Values, reqID string) (any, error) {
			userName := vals.Get("UserName")

			p, err := h.Backend.ListSSHPublicKeys(
				userName,
				vals.Get("Marker"),
				parseMaxItems(vals.Get("MaxItems")),
			)
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
				XMLName: xmlLocalName("ListSSHPublicKeysResponse"),
				Xmlns:   iamXMLNS,
				ListSSHPublicKeysResult: listSSHPublicKeysResult{
					SSHPublicKeys: members,
					IsTruncated:   p.Next != "",
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		opUpdateSSHPublicKey: func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.UpdateSSHPublicKey(
				vals.Get("UserName"),
				vals.Get("SSHPublicKeyId"),
				vals.Get("Status"),
			); err != nil {
				return nil, err
			}

			return &iamSimpleTagResponse{
				XMLName:          xmlLocalName("UpdateSSHPublicKeyResponse"),
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		opDeleteSSHPublicKey: func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteSSHPublicKey(
				vals.Get("UserName"),
				vals.Get("SSHPublicKeyId"),
			); err != nil {
				return nil, err
			}

			return &iamSimpleTagResponse{
				XMLName:          xmlLocalName("DeleteSSHPublicKeyResponse"),
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamMFALinkDispatch wires EnableMFADevice, DeactivateMFADevice, and ListMFADevices.
func (h *Handler) iamMFALinkDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		opEnableMFADevice: func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.EnableMFADevice(
				vals.Get("UserName"),
				vals.Get("SerialNumber"),
				vals.Get("AuthenticationCode1"),
				vals.Get("AuthenticationCode2"),
			); err != nil {
				return nil, err
			}

			return &iamSimpleTagResponse{
				XMLName:          xmlLocalName("EnableMFADeviceResponse"),
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		opDeactivateMFADevice: func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeactivateMFADevice(
				vals.Get("UserName"),
				vals.Get("SerialNumber"),
			); err != nil {
				return nil, err
			}

			return &iamSimpleTagResponse{
				XMLName:          xmlLocalName("DeactivateMFADeviceResponse"),
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		opListMFADevices: func(vals url.Values, reqID string) (any, error) {
			userName := vals.Get("UserName")

			var devices []VirtualMFADevice

			if userName != "" {
				var err error

				devices, err = h.Backend.ListMFADevicesForUser(userName)
				if err != nil {
					return nil, err
				}
			}

			members := make([]mfaDeviceXML, 0, len(devices))

			for _, d := range devices {
				members = append(members, mfaDeviceXML{
					UserName:     userName,
					SerialNumber: d.SerialNumber,
					EnableDate:   isoTime(d.CreateDate),
				})
			}

			return &listMFADevicesResponse{
				XMLName: xmlLocalName("ListMFADevicesResponse"),
				Xmlns:   iamXMLNS,
				ListMFADevicesResult: listMFADevicesResult{
					MFADevices:  members,
					IsTruncated: false,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamAccessAdvisorDispatch wires real GenerateServiceLastAccessedDetails and GetServiceLastAccessedDetails.
func (h *Handler) iamAccessAdvisorDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		opGenerateServiceLastAccessed: func(vals url.Values, reqID string) (any, error) {
			entityARN := vals.Get("Arn")
			jobID := h.Backend.GenerateServiceLastAccessedDetailsForEntity(entityARN)

			return &generateServiceLastAccessedDetailsResponse{
				XMLName: xmlLocalName("GenerateServiceLastAccessedDetailsResponse"),
				Xmlns:   iamXMLNS,
				GenerateServiceLastAccessedDetailsResult: generateSLADResult{
					JobID: jobID,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		opGetServiceLastAccessed: func(vals url.Values, reqID string) (any, error) {
			jobID := vals.Get("JobId")

			status, details, err := h.Backend.GetServiceLastAccessedDetails(jobID)
			if err != nil {
				return nil, err
			}

			now := isoTime(time.Now().UTC())
			xmlDetails := make([]ServiceLastAccessedDetailXML, 0, len(details))

			for _, d := range details {
				entry := ServiceLastAccessedDetailXML{
					ServiceName:                d.ServiceName,
					ServiceNamespace:           d.ServiceNamespace,
					TotalAuthenticatedEntities: d.TotalAuthenticatedEntities,
				}

				if !d.LastAuthenticated.IsZero() {
					entry.LastAuthenticated = isoTime(d.LastAuthenticated)
					entry.LastAuthenticatedArn = d.LastAuthenticatedArn
				}

				xmlDetails = append(xmlDetails, entry)
			}

			return &GetServiceLastAccessedDetailsResponse{
				Xmlns: iamXMLNS,
				GetServiceLastAccessedDetailsResult: GetServiceLastAccessedDetailsResult{
					JobStatus:            status,
					JobCreationDate:      now,
					JobCompletionDate:    now,
					ServicesLastAccessed: xmlDetails,
					IsTruncated:          false,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamSSCResetDispatch wires ResetServiceSpecificCredential to real credential storage.
func (h *Handler) iamSSCResetDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		opResetServiceSpecificCredential: func(vals url.Values, reqID string) (any, error) {
			cred, err := h.Backend.ResetServiceSpecificCredentialFull(
				vals.Get("UserName"),
				vals.Get("ServiceSpecificCredentialId"),
			)
			if err != nil {
				return nil, err
			}

			return &resetServiceSpecificCredentialResponse{
				XMLName: xmlLocalName("ResetServiceSpecificCredentialResponse"),
				Xmlns:   iamXMLNS,
				ResetServiceSpecificCredentialResult: resetSSCResult{
					ServiceSpecificCredential: serviceSpecificCredXML{
						UserName:                    cred.UserName,
						ServiceSpecificCredentialID: cred.ServiceSpecificCredentialID,
						ServiceName:                 cred.ServiceName,
						ServiceUserName:             cred.ServiceUserName,
						ServicePassword:             cred.ServicePassword,
						Status:                      cred.Status,
						CreateDate:                  isoTime(cred.CreateDate),
					},
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamVirtualMFAFullDispatch overrides CreateVirtualMFADevice with QR code and seed support.
func (h *Handler) iamVirtualMFAFullDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		opCreateVirtualMFADevice: func(vals url.Values, reqID string) (any, error) {
			device, err := h.Backend.CreateVirtualMFADeviceFull(
				vals.Get("VirtualMFADeviceName"),
				vals.Get("Path"),
			)
			if err != nil {
				return nil, err
			}

			return &CreateVirtualMFADeviceResponse{
				Xmlns: iamXMLNS,
				CreateVirtualMFADeviceResult: CreateVirtualMFADeviceResult{
					VirtualMFADevice: VirtualMFADeviceXML{
						SerialNumber:     device.SerialNumber,
						Base32StringSeed: device.Base32StringSeed,
						QRCodePNG:        device.QRCodePNG,
					},
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}
