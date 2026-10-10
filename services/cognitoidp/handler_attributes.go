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
	res, err := h.Backend.UpdateUserAttributesWithDelivery(in.AccessToken, attributeListToMap(in.UserAttributes))
	if err != nil {
		return nil, err
	}

	details, err := h.deliverAttributeCodes(res, in.ClientMetadata, true)
	if err != nil {
		return nil, err
	}

	return &updateUserAttributesOutput{CodeDeliveryDetailsList: details}, nil
}

func (h *Handler) handleAdminUpdateUserAttributes(
	_ context.Context,
	in *adminUpdateUserAttributesInput,
) (*adminUpdateUserAttributesOutput, error) {
	res, err := h.Backend.AdminUpdateUserAttributesWithDelivery(
		in.UserPoolID, in.Username, attributeListToMap(in.UserAttributes))
	if err != nil {
		return nil, err
	}

	if _, err = h.deliverAttributeCodes(res, in.ClientMetadata, false); err != nil {
		return nil, err
	}

	return &adminUpdateUserAttributesOutput{}, nil
}

// deliverAttributeCodes fires CustomMessage_UpdateUserAttribute for each verification message
// and, when includeCode is set, returns the CodeDeliveryDetailsList wire entries.
func (h *Handler) deliverAttributeCodes(
	res *AttributeUpdateResult, meta map[string]string, includeCode bool,
) ([]map[string]string, error) {
	var out []map[string]string

	for _, d := range res.Deliveries {
		message, subject, err := h.Backend.InvokeCustomMessageTriggerForUser(
			res.PoolID, res.ClientID, res.Username, d.Code, triggerSourceCustomMessageUpdateAttr, meta)
		if err != nil {
			return nil, err
		}

		entry := map[string]string{
			keyDeliveryMedium: d.DeliveryMedium, keyDestination: d.Destination, keyAttributeName: d.AttributeName,
		}
		if includeCode {
			entry[keyConfirmationCode] = d.Code
		}

		if message != "" {
			entry[keyCustomMessage] = message
		}

		if subject != "" {
			entry[keyCustomMessageSubject] = subject
		}

		out = append(out, entry)
	}

	return out, nil
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

func (h *Handler) handleGetUserAttributeVerificationCodeFull(
	_ context.Context,
	in *getUserAttributeVerifCodeFullInput,
) (*getUserAttributeVerifCodeFullOutput, error) {
	code, dest, medium, err := h.Backend.GetUserAttributeVerificationCode(in.AccessToken, in.AttributeName)
	if err != nil {
		return nil, err
	}

	poolID, username, clientID, err := h.Backend.AccessTokenIdentity(in.AccessToken)
	if err != nil {
		return nil, err
	}

	details := map[string]string{
		keyDeliveryMedium: medium,
		keyDestination:    dest,
		keyAttributeName:  in.AttributeName,
	}

	message, subject, err := h.Backend.InvokeCustomMessageTriggerForUser(
		poolID, clientID, username, code, triggerSourceCustomMessageVerifyAttr, in.ClientMetadata)
	if err != nil {
		return nil, err
	}

	if message != "" {
		details[keyCustomMessage] = message
	}

	if subject != "" {
		details[keyCustomMessageSubject] = subject
	}

	return &getUserAttributeVerifCodeFullOutput{CodeDeliveryDetails: details}, nil
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

func (h *Handler) attributesOpsA() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"DeleteUserAttributes":      service.WrapOp(h.handleDeleteUserAttributes),
		"UpdateUserAttributes":      service.WrapOp(h.handleUpdateUserAttributes),
		"AdminUpdateUserAttributes": service.WrapOp(h.handleAdminUpdateUserAttributes),
		"AddCustomAttributes":       service.WrapOp(h.handleAddCustomAttributes),
		"AdminDeleteUserAttributes": service.WrapOp(h.handleAdminDeleteUserAttributes),
	}
}

func (h *Handler) attributesOpsC() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opGetUserAttributeVerifCode: wrapAccuracy(h.handleGetUserAttributeVerificationCodeFull),
		opVerifyUserAttribute:       wrapAccuracy(h.handleVerifyUserAttributeFull),
	}
}
