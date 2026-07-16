package cognitoidp

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// attributeListToMap converts a slice of Cognito attribute types to a map.
func attributeListToMap(attrs []attributeType) map[string]string {
	m := make(map[string]string, len(attrs))
	for _, a := range attrs {
		m[a.Name] = a.Value
	}

	return m
}

// sortedAttributeList converts a map to a sorted slice of Cognito attribute types.
// Sorting by name ensures deterministic output, matching AWS behaviour.
func sortedAttributeList(m map[string]string) []attributeType {
	if len(m) == 0 {
		return []attributeType{}
	}

	keys := collections.SortedKeys(m)

	out := make([]attributeType, 0, len(m))
	for _, k := range keys {
		out = append(out, attributeType{Name: k, Value: m[k]})
	}

	return out
}

func (h *Handler) handleUpdateUserAttributes(
	_ context.Context,
	in *updateUserAttributesInput,
) (*updateUserAttributesOutput, error) {
	attrs := attributeListToMap(in.UserAttributes)
	if err := h.Backend.UpdateUserAttributes(in.AccessToken, attrs); err != nil {
		return nil, err
	}

	return &updateUserAttributesOutput{}, nil
}

func (h *Handler) handleAdminUpdateUserAttributes(
	_ context.Context,
	in *adminUpdateUserAttributesInput,
) (*adminUpdateUserAttributesOutput, error) {
	attrs := attributeListToMap(in.UserAttributes)
	if err := h.Backend.AdminUpdateUserAttributes(in.UserPoolID, in.Username, attrs); err != nil {
		return nil, err
	}

	return &adminUpdateUserAttributesOutput{}, nil
}

func (h *Handler) handleAddCustomAttributes(
	_ context.Context,
	in *addCustomAttributesInput,
) (*addCustomAttributesOutput, error) {
	if err := h.Backend.AddCustomAttributes(in.UserPoolID, in.CustomAttributes); err != nil {
		return nil, err
	}

	return &addCustomAttributesOutput{}, nil
}

func (h *Handler) handleAdminDeleteUserAttributes(
	_ context.Context,
	in *adminDeleteUserAttributesInput,
) (*adminDeleteUserAttributesOutput, error) {
	if err := h.Backend.AdminDeleteUserAttributes(in.UserPoolID, in.Username, in.UserAttributeNames); err != nil {
		return nil, err
	}

	return &adminDeleteUserAttributesOutput{}, nil
}

func (h *Handler) handleDeleteUserAttributes(
	_ context.Context,
	in *deleteUserAttributesInput,
) (*deleteUserAttributesOutput, error) {
	if err := h.Backend.DeleteUserAttributes(in.AccessToken, in.UserAttributeNames); err != nil {
		return nil, err
	}

	return &deleteUserAttributesOutput{}, nil
}

func (h *Handler) handleVerifyUserAttribute(
	_ context.Context,
	in *verifyUserAttributeInput,
) (*verifyUserAttributeOutput, error) {
	if err := h.Backend.VerifyUserAttribute(in.AccessToken, in.AttributeName, in.Code); err != nil {
		return nil, err
	}

	return &verifyUserAttributeOutput{}, nil
}

func (h *Handler) handleGetUserAttributeVerificationCodeFull(
	_ context.Context,
	in *getUserAttributeVerifCodeFullInput,
) (*getUserAttributeVerifCodeFullOutput, error) {
	_, dest, medium, err := h.Backend.GetUserAttributeVerificationCode(in.AccessToken, in.AttributeName)
	if err != nil {
		return nil, err
	}

	return &getUserAttributeVerifCodeFullOutput{
		CodeDeliveryDetails: map[string]string{
			keyDeliveryMedium: medium,
			keyDestination:    dest,
			keyAttributeName:  in.AttributeName,
		},
	}, nil
}

func (h *Handler) handleVerifyUserAttributeFull(
	_ context.Context,
	in *verifyUserAttributeFullInput,
) (*verifyUserAttributeFullOutput, error) {
	if err := h.Backend.VerifyUserAttributeWithCode(in.AccessToken, in.AttributeName, in.Code); err != nil {
		return nil, err
	}

	return &verifyUserAttributeFullOutput{}, nil
}

func (h *Handler) handleGetUserAttributeVerificationCode(
	_ context.Context,
	in *getUserAttributeVerificationCodeInput,
) (*getUserAttributeVerificationCodeOutput, error) {
	return &getUserAttributeVerificationCodeOutput{
		CodeDeliveryDetails: map[string]string{
			keyDeliveryMedium: medEmail,
			keyDestination:    "user@example.com",
			keyAttributeName:  in.AttributeName,
		},
	}, nil
}

func (h *Handler) attributesOpsA() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"DeleteUserAttributes":      service.WrapOp(h.handleDeleteUserAttributes),
		"VerifyUserAttribute":       service.WrapOp(h.handleVerifyUserAttribute),
		"UpdateUserAttributes":      service.WrapOp(h.handleUpdateUserAttributes),
		"AdminUpdateUserAttributes": service.WrapOp(h.handleAdminUpdateUserAttributes),
		"AddCustomAttributes":       service.WrapOp(h.handleAddCustomAttributes),
		"AdminDeleteUserAttributes": service.WrapOp(h.handleAdminDeleteUserAttributes),
	}
}

func (h *Handler) attributesOpsB() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"GetUserAttributeVerificationCode": service.WrapOp(h.handleGetUserAttributeVerificationCode),
	}
}

func (h *Handler) attributesOpsC() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opGetUserAttributeVerifCode: wrapAccuracy(h.handleGetUserAttributeVerificationCodeFull),
		opVerifyUserAttribute:       wrapAccuracy(h.handleVerifyUserAttributeFull),
	}
}
