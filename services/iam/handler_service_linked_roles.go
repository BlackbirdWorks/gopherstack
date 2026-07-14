package iam

import (
	"encoding/xml"
	"errors"
	"net/url"
)

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

// iamServiceLinkedRoleStatusDispatch adds GetServiceLinkedRoleDeletionStatus.
func (h *Handler) iamServiceLinkedRoleStatusDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"GetServiceLinkedRoleDeletionStatus": func(vals url.Values, reqID string) (any, error) {
			status, err := h.Backend.GetServiceLinkedRoleDeletionStatus(vals.Get("DeletionTaskId"))
			if err != nil {
				return nil, err
			}

			return &GetServiceLinkedRoleDeletionStatusResponse{
				Xmlns: iamXMLNS,
				GetServiceLinkedRoleDeletionStatusResult: GetServiceLinkedRoleDeletionStatusResult{
					Status: status,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func isRoleNotFound(err error) bool { return errors.Is(err, ErrRoleNotFound) }

// iamDeleteServiceLinkedRoleDispatch returns the DeleteServiceLinkedRole
// dispatch entry added in the completeness pass.
func (h *Handler) iamDeleteServiceLinkedRoleDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
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
