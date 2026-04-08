package iam

import (
	"maps"
	"net/url"
)

// formValueTrue is the string "true" as submitted via HTML form values.
const formValueTrue = "true"

// iamNewOpsDispatchTable returns the dispatch entries for the 10 new IAM operations.
func (h *Handler) iamNewOpsDispatchTable() map[string]iamActionFn {
	table := make(map[string]iamActionFn)
	maps.Copy(table, h.iamNewOpsAccountActions())
	maps.Copy(table, h.iamNewOpsPolicyActions())
	maps.Copy(table, h.iamNewOpsRoleAndCredentialActions())
	maps.Copy(table, h.iamNewOpsDelegationAndOIDCActions())

	return table
}

// iamNewOpsAccountActions returns dispatch entries for account-level new operations.
func (h *Handler) iamNewOpsAccountActions() map[string]iamActionFn {
	return map[string]iamActionFn{
		"CreateAccountAlias": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.CreateAccountAlias(vals.Get("AccountAlias")); err != nil {
				return nil, err
			}

			return &CreateAccountAliasResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		"ChangePassword": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.ChangePassword(vals.Get("NewPassword")); err != nil {
				return nil, err
			}

			return &ChangePasswordResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamNewOpsPolicyActions returns dispatch entries for policy version new operations.
func (h *Handler) iamNewOpsPolicyActions() map[string]iamActionFn {
	return map[string]iamActionFn{
		"CreatePolicyVersion": func(vals url.Values, reqID string) (any, error) {
			setAsDefault := vals.Get("SetAsDefault") == formValueTrue

			pv, err := h.Backend.CreatePolicyVersion(
				vals.Get("PolicyArn"),
				vals.Get("PolicyDocument"),
				setAsDefault,
			)
			if err != nil {
				return nil, err
			}

			return &CreatePolicyVersionResponse{
				Xmlns: iamXMLNS,
				CreatePolicyVersionResult: CreatePolicyVersionResult{
					PolicyVersion: PolicyVersionXML{
						VersionID:        pv.VersionID,
						IsDefaultVersion: pv.IsDefaultVersion,
						CreateDate:       isoTime(pv.CreateDate),
					},
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamNewOpsRoleAndCredentialActions returns dispatch entries for role/credential new operations.
func (h *Handler) iamNewOpsRoleAndCredentialActions() map[string]iamActionFn {
	return map[string]iamActionFn{
		"CreateServiceLinkedRole": func(vals url.Values, reqID string) (any, error) {
			role, err := h.Backend.CreateServiceLinkedRole(
				vals.Get("AWSServiceName"),
				vals.Get("Description"),
				vals.Get("CustomSuffix"),
			)
			if err != nil {
				return nil, err
			}

			return &CreateServiceLinkedRoleResponse{
				Xmlns: iamXMLNS,
				CreateServiceLinkedRoleResult: CreateServiceLinkedRoleResult{
					Role: toRoleXML(role),
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		"CreateServiceSpecificCredential": func(vals url.Values, reqID string) (any, error) {
			cred, err := h.Backend.CreateServiceSpecificCredential(
				vals.Get("UserName"),
				vals.Get("ServiceName"),
			)
			if err != nil {
				return nil, err
			}

			return &CreateServiceSpecificCredentialResponse{
				Xmlns: iamXMLNS,
				CreateServiceSpecificCredentialResult: CreateServiceSpecificCredentialResult{
					ServiceSpecificCredential: toServiceSpecificCredentialXML(cred),
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		"CreateVirtualMFADevice": func(vals url.Values, reqID string) (any, error) {
			device, err := h.Backend.CreateVirtualMFADevice(
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
						SerialNumber: device.SerialNumber,
					},
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamNewOpsDelegationAndOIDCActions returns dispatch entries for delegation and OIDC new operations.
func (h *Handler) iamNewOpsDelegationAndOIDCActions() map[string]iamActionFn {
	return map[string]iamActionFn{
		"CreateDelegationRequest": func(vals url.Values, reqID string) (any, error) {
			req, err := h.Backend.CreateDelegationRequest(vals.Get("TargetAccountId"))
			if err != nil {
				return nil, err
			}

			return &CreateDelegationRequestResponse{
				Xmlns: iamXMLNS,
				CreateDelegationRequestResult: CreateDelegationRequestResult{
					DelegationRequest: DelegationRequestXML{
						DelegationID:    req.DelegationID,
						TargetAccountID: req.TargetAccountID,
						Status:          req.Status,
						CreateDate:      isoTime(req.CreateDate),
					},
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		"AcceptDelegationRequest": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.AcceptDelegationRequest(vals.Get("DelegationId")); err != nil {
				return nil, err
			}

			return &AcceptDelegationRequestResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		"AssociateDelegationRequest": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.AssociateDelegationRequest(
				vals.Get("DelegationId"),
				vals.Get("PolicyArn"),
			); err != nil {
				return nil, err
			}

			return &AssociateDelegationRequestResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		"AddClientIDToOpenIDConnectProvider": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.AddClientIDToOpenIDConnectProvider(
				vals.Get("OpenIDConnectProviderArn"),
				vals.Get("ClientID"),
			); err != nil {
				return nil, err
			}

			return &AddClientIDToOpenIDConnectProviderResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

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
