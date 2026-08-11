package iam

import (
	"fmt"
	"net/url"
	"strconv"
)

func (h *Handler) iamUserDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"CreateUser": func(vals url.Values, reqID string) (any, error) {
			u, err := h.Backend.CreateUser(vals.Get("UserName"), vals.Get("Path"), vals.Get("PermissionsBoundary"))
			if err != nil {
				return nil, err
			}

			// CreateUser accepts an optional Tags.member.N parameter to tag the
			// user at creation time (real AWS: "A list of tags that you want to
			// attach to the new user"). This was previously accepted-but-dropped.
			if tags := parseIAMTags(vals); len(tags) > 0 {
				if tagErr := h.Backend.TagUser(u.UserName, tags); tagErr != nil {
					return nil, tagErr
				}

				u.Tags = tags
			}

			return &CreateUserResponse{
				Xmlns:            iamXMLNS,
				CreateUserResult: CreateUserResult{User: toUserXML(u)},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetUser": func(vals url.Values, reqID string) (any, error) {
			u, err := h.Backend.GetUser(vals.Get("UserName"))
			if err != nil {
				return nil, err
			}

			return &GetUserResponse{
				Xmlns:            iamXMLNS,
				GetUserResult:    GetUserResult{User: toUserXML(u)},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteUser": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteUser(vals.Get("UserName")); err != nil {
				return nil, err
			}

			return &DeleteUserResponse{Xmlns: iamXMLNS, ResponseMetadata: ResponseMetadata{RequestID: reqID}}, nil
		},
		opListUsers: func(vals url.Values, reqID string) (any, error) {
			p, err := h.Backend.ListUsers(vals.Get("Marker"), parseMaxItems(vals.Get("MaxItems")))
			if err != nil {
				return nil, err
			}

			xmlUsers := make([]UserXML, 0, len(p.Data))
			for i := range p.Data {
				xmlUsers = append(xmlUsers, toUserXML(&p.Data[i]))
			}

			return &ListUsersResponse{
				Xmlns: iamXMLNS,
				ListUsersResult: ListUsersResult{
					Users:       xmlUsers,
					IsTruncated: p.Next != "",
					Marker:      p.Next,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func toUserXML(u *User) UserXML {
	x := UserXML{
		Path:       u.Path,
		UserName:   u.UserName,
		UserID:     u.UserID,
		Arn:        u.Arn,
		CreateDate: isoTime(u.CreateDate),
		Tags:       tagsToXML(u.Tags),
	}

	if u.PermissionsBoundary != "" {
		x.PermissionsBoundary = &PermissionsBoundaryXML{
			PermissionsBoundaryArn:  u.PermissionsBoundary,
			PermissionsBoundaryType: xmlElemPolicy,
		}
	}

	return x
}

// iamEntityUpdateDispatch adds UpdateUser, UpdateRole, and UpdateRoleDescription.
func (h *Handler) iamEntityUpdateDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"UpdateUser": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.UpdateUser(
				vals.Get("UserName"), vals.Get("NewPath"), vals.Get("NewUserName"),
			); err != nil {
				return nil, err
			}

			return &UpdateUserResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UpdateRole": func(vals url.Values, reqID string) (any, error) {
			roleName := vals.Get("RoleName")
			if path := vals.Get("Path"); path != "" {
				return nil, fmt.Errorf(
					"%w: role Path is immutable; use a new role to change the path",
					ErrInvalidAction,
				)
			}

			if err := h.Backend.UpdateRole(roleName, vals.Get("Description")); err != nil {
				return nil, err
			}

			if err := h.applyUpdateRoleMaxSessionDuration(roleName, vals.Get("MaxSessionDuration")); err != nil {
				return nil, err
			}

			r, err := h.Backend.GetRole(roleName)
			if err != nil {
				return nil, err
			}

			return &UpdateRoleResponse{
				Xmlns:            iamXMLNS,
				UpdateRoleResult: UpdateRoleResult{Role: toRoleXML(r)},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UpdateRoleDescription": func(vals url.Values, reqID string) (any, error) {
			roleName := vals.Get("RoleName")
			if err := h.Backend.UpdateRole(roleName, vals.Get("Description")); err != nil {
				return nil, err
			}

			r, err := h.Backend.GetRole(roleName)
			if err != nil {
				return nil, err
			}

			return &UpdateRoleDescriptionResponse{
				Xmlns:                       iamXMLNS,
				UpdateRoleDescriptionResult: UpdateRoleDescriptionResult{Role: toRoleXML(r)},
				ResponseMetadata:            ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// applyUpdateRoleMaxSessionDuration validates and applies UpdateRole's optional
// MaxSessionDuration, a no-op when msd is empty.
func (h *Handler) applyUpdateRoleMaxSessionDuration(roleName, msd string) error {
	if msd == "" {
		return nil
	}

	d, parseErr := strconv.ParseInt(msd, 10, 32)
	if parseErr != nil || d < minMaxSessionDuration || d > maxMaxSessionDuration {
		return fmt.Errorf(
			"%w: MaxSessionDuration must be between %d and %d",
			ErrValidationError, minMaxSessionDuration, maxMaxSessionDuration,
		)
	}

	if err := h.Backend.UpdateRoleMaxSessionDuration(roleName, int32(d)); err != nil {
		return fmt.Errorf("updating max session duration for role %s: %w", roleName, err)
	}

	return nil
}
