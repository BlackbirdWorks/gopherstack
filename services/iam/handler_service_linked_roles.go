package iam

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/url"
	"strconv"
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
			ageDays, convErr := strconv.Atoi(vals.Get("CredentialAgeDays"))
			if vals.Get("CredentialAgeDays") != "" && (convErr != nil || ageDays < 1) {
				return nil, fmt.Errorf("%w: CredentialAgeDays must be a positive integer", ErrInvalidInput)
			}

			cred, err := h.Backend.CreateServiceSpecificCredentialWithAge(
				vals.Get("UserName"),
				vals.Get("ServiceName"),
				ageDays,
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
