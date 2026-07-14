package iam

import (
	"encoding/xml"
	"net/url"
	"time"
)

// toServiceSpecificCredentialXML converts a ServiceSpecificCredential to its XML representation.
func toServiceSpecificCredentialXML(cred *ServiceSpecificCredential) ServiceSpecificCredentialXML {
	return ServiceSpecificCredentialXML{
		UserName:                    cred.UserName,
		ServiceName:                 cred.ServiceName,
		ServiceUserName:             cred.ServiceUserName,
		ServicePassword:             cred.ServicePassword,
		ServiceSpecificCredentialID: cred.ServiceSpecificCredentialID,
		Status:                      cred.Status,
		CreateDate:                  isoTime(cred.CreateDate),
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

// iamServiceSpecificCredDispatch adds ListServiceSpecificCredentials and DeleteServiceSpecificCredential.
func (h *Handler) iamServiceSpecificCredDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"ListServiceSpecificCredentials": func(vals url.Values, reqID string) (any, error) {
			creds, err := h.Backend.ListServiceSpecificCredentials(
				vals.Get("UserName"), vals.Get("ServiceName"),
			)
			if err != nil {
				return nil, err
			}

			xmlCreds := make([]ServiceSpecificCredentialMetadataXML, 0, len(creds))
			for i := range creds {
				c := &creds[i]
				xmlCreds = append(xmlCreds, ServiceSpecificCredentialMetadataXML{
					UserName:                    c.UserName,
					ServiceName:                 c.ServiceName,
					ServiceUserName:             c.ServiceUserName,
					ServiceSpecificCredentialID: c.ServiceSpecificCredentialID,
					Status:                      c.Status,
					CreateDate:                  isoTime(c.CreateDate),
				})
			}

			return &ListServiceSpecificCredentialsResponse{
				Xmlns: iamXMLNS,
				Result: ListServiceSpecificCredentialsResult{
					ServiceSpecificCredentials: xmlCreds,
				},
				Meta: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteServiceSpecificCredential": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteServiceSpecificCredential(
				vals.Get("UserName"), vals.Get("ServiceSpecificCredentialId"),
			); err != nil {
				return nil, err
			}

			return &DeleteServiceSpecificCredentialResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamRefinement2CredTable handles credential management operations.
func (h *Handler) iamRefinement2CredTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"UpdateServiceSpecificCredential": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.UpdateServiceSpecificCredential(
				vals.Get("UserName"),
				vals.Get("ServiceSpecificCredentialId"),
				vals.Get("Status"),
			); err != nil {
				return nil, err
			}

			return &UpdateServiceSpecificCredentialResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		"GetMFADevice": func(vals url.Values, reqID string) (any, error) {
			serial := vals.Get("SerialNumber")

			dev, owner, err := h.Backend.GetVirtualMFADevice(serial)
			if err != nil {
				return nil, err
			}

			userName := owner
			if userName == "" {
				// Fall back to the UserName supplied in the request when the
				// device is not yet associated with a user.
				userName = vals.Get("UserName")
			}

			return &GetMFADeviceResponse{
				Xmlns: iamXMLNS,
				GetMFADeviceResult: GetMFADeviceResult{
					UserName:     userName,
					SerialNumber: dev.SerialNumber,
					EnableDate:   isoTime(dev.CreateDate),
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamResetServiceSpecificCredentialCompletenessDispatch returns the
// ResetServiceSpecificCredential dispatch entry added in the completeness
// pass. It is shadowed at runtime (buildDispatchTable merges
// iamComprehensiveDispatchTable, whose iamSSCResetDispatch entry calls the
// real backend, after iamCompletenessDispatchTable), so this stub-style
// implementation never actually serves a request; kept verbatim as part of a
// pure reorganization (no behavior change).
func (h *Handler) iamResetServiceSpecificCredentialCompletenessDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
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
	}
}
