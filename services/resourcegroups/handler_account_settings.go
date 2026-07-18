package resourcegroups

import "context"

// handleGetAccountSettings returns the account-level Resource Groups settings.
type getAccountSettingsInput struct{}

type getAccountSettingsOutput struct {
	AccountSettings AccountSettings `json:"AccountSettings"`
}

func (h *Handler) handleGetAccountSettings(
	_ context.Context,
	_ *getAccountSettingsInput,
) (*getAccountSettingsOutput, error) {
	settings := h.Backend.GetAccountSettings()

	return &getAccountSettingsOutput{AccountSettings: settings}, nil
}

// handleUpdateAccountSettings updates account-level lifecycle event settings.
type updateAccountSettingsInput struct {
	GroupLifecycleEventsDesiredStatus string `json:"GroupLifecycleEventsDesiredStatus"`
}

type updateAccountSettingsOutput struct {
	AccountSettings AccountSettings `json:"AccountSettings"`
}

func (h *Handler) handleUpdateAccountSettings(
	_ context.Context,
	in *updateAccountSettingsInput,
) (*updateAccountSettingsOutput, error) {
	if err := h.Backend.UpdateAccountSettings(in.GroupLifecycleEventsDesiredStatus); err != nil {
		return nil, err
	}

	settings := h.Backend.GetAccountSettings()

	return &updateAccountSettingsOutput{AccountSettings: settings}, nil
}
