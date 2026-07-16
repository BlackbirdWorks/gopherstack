package cognitoidp

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func (h *Handler) handleSetUICustomizationFull(
	_ context.Context,
	in *setUICustomizationFullInput,
) (*setUICustomizationFullOutput, error) {
	ui, err := h.Backend.SetUICustomizationFull(in.UserPoolID, in.ClientID, in.CSS, in.ImageData)
	if err != nil {
		return nil, err
	}

	return &setUICustomizationFullOutput{UICustomization: toUICustomizationJSON(ui)}, nil
}

func (h *Handler) handleGetUICustomizationFull(
	_ context.Context,
	in *getUICustomizationFullInput,
) (*getUICustomizationFullOutput, error) {
	ui, err := h.Backend.GetUICustomizationFull(in.UserPoolID, in.ClientID)
	if err != nil {
		return nil, err
	}

	return &getUICustomizationFullOutput{UICustomization: toUICustomizationJSON(ui)}, nil
}

func toUICustomizationJSON(ui *UICustomization) *uiCustomizationJSON {
	out := &uiCustomizationJSON{
		UserPoolID: ui.UserPoolID,
		ClientID:   ui.ClientID,
		CSS:        ui.CSS,
		ImageURL:   ui.ImageURL,
	}

	if !ui.CreatedAt.IsZero() {
		out.CreationDate = float64(ui.CreatedAt.Unix())
	}

	if !ui.LastModifiedAt.IsZero() {
		out.LastModifiedDate = float64(ui.LastModifiedAt.Unix())
	}

	return out
}

func (h *Handler) handleCreateManagedLoginBranding(
	_ context.Context,
	in *createManagedLoginBrandingInput,
) (*createManagedLoginBrandingOutput, error) {
	mlb, err := h.Backend.CreateManagedLoginBranding(in.UserPoolID, in.ClientID)
	if err != nil {
		return nil, err
	}

	return &createManagedLoginBrandingOutput{
		ManagedLoginBranding: &managedLoginBrandingType{
			UserPoolID:             mlb.UserPoolID,
			ManagedLoginBrandingID: mlb.ManagedLoginBrandingID,
		},
	}, nil
}

func (h *Handler) handleDeleteManagedLoginBranding(
	_ context.Context,
	in *deleteManagedLoginBrandingInput,
) (*deleteManagedLoginBrandingOutput, error) {
	if err := h.Backend.DeleteManagedLoginBranding(in.UserPoolID, in.ManagedLoginBrandingID); err != nil {
		return nil, err
	}

	return &deleteManagedLoginBrandingOutput{}, nil
}

func (h *Handler) handleDescribeManagedLoginBranding(
	_ context.Context,
	in *describeManagedLoginBrandingInput,
) (*describeManagedLoginBrandingOutput, error) {
	mlb, err := h.Backend.DescribeManagedLoginBranding(in.UserPoolID, in.ManagedLoginBrandingID)
	if err != nil {
		return nil, err
	}

	return &describeManagedLoginBrandingOutput{
		ManagedLoginBranding: &managedLoginBrandingType{
			ManagedLoginBrandingID: mlb.ManagedLoginBrandingID,
			UserPoolID:             mlb.UserPoolID,
		},
	}, nil
}

func (h *Handler) handleDescribeManagedLoginBrandingByClient(
	_ context.Context,
	in *describeManagedLoginBrandingByClientInput,
) (*describeManagedLoginBrandingByClientOutput, error) {
	mlb, err := h.Backend.DescribeManagedLoginBrandingByClient(in.UserPoolID, in.ClientID)
	if err != nil {
		return nil, err
	}

	return &describeManagedLoginBrandingByClientOutput{
		ManagedLoginBranding: &managedLoginBrandingType{
			ManagedLoginBrandingID: mlb.ManagedLoginBrandingID,
			UserPoolID:             mlb.UserPoolID,
		},
	}, nil
}

func (h *Handler) handleUpdateManagedLoginBranding(
	_ context.Context,
	in *updateManagedLoginBrandingInput,
) (*updateManagedLoginBrandingOutput, error) {
	mlb, err := h.Backend.UpdateManagedLoginBranding(in.UserPoolID, in.ManagedLoginBrandingID)
	if err != nil {
		return nil, err
	}

	return &updateManagedLoginBrandingOutput{
		ManagedLoginBranding: &managedLoginBrandingType{
			ManagedLoginBrandingID: mlb.ManagedLoginBrandingID,
			UserPoolID:             mlb.UserPoolID,
		},
	}, nil
}

func (h *Handler) handleGetUICustomization(
	_ context.Context,
	in *getUICustomizationInput,
) (*getUICustomizationOutput, error) {
	ui, err := h.Backend.GetUICustomization(in.UserPoolID, in.ClientID)
	if err != nil {
		return nil, err
	}

	return &getUICustomizationOutput{UICustomization: &uiCustomizationType{
		UserPoolID: ui.UserPoolID,
		ClientID:   ui.ClientID,
		CSS:        ui.CSS,
	}}, nil
}

func (h *Handler) handleSetUICustomization(
	_ context.Context,
	in *setUICustomizationInput,
) (*setUICustomizationOutput, error) {
	ui, err := h.Backend.SetUICustomization(in.UserPoolID, in.ClientID, in.CSS)
	if err != nil {
		return nil, err
	}

	return &setUICustomizationOutput{UICustomization: &uiCustomizationType{
		UserPoolID: ui.UserPoolID,
		ClientID:   ui.ClientID,
		CSS:        ui.CSS,
	}}, nil
}

func (h *Handler) brandingOpsA() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateManagedLoginBranding":           service.WrapOp(h.handleCreateManagedLoginBranding),
		"DeleteManagedLoginBranding":           service.WrapOp(h.handleDeleteManagedLoginBranding),
		"DescribeManagedLoginBranding":         service.WrapOp(h.handleDescribeManagedLoginBranding),
		"DescribeManagedLoginBrandingByClient": service.WrapOp(h.handleDescribeManagedLoginBrandingByClient),
		"GetUICustomization":                   service.WrapOp(h.handleGetUICustomization),
		"SetUICustomization":                   service.WrapOp(h.handleSetUICustomization),
		"UpdateManagedLoginBranding":           service.WrapOp(h.handleUpdateManagedLoginBranding),
	}
}

func (h *Handler) brandingOpsB() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opGetUICustomization: wrapAccuracy(h.handleGetUICustomizationFull),
		opSetUICustomization: wrapAccuracy(h.handleSetUICustomizationFull),
	}
}
