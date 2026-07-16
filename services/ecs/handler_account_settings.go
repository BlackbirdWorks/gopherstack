package ecs

import (
	"context"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// ----- Handler: ListAccountSettings -----

type listAccountSettingsInput struct {
	Name         string `json:"name,omitempty"`
	PrincipalArn string `json:"principalArn,omitempty"`
	NextToken    string `json:"nextToken,omitempty"`
	MaxResults   int    `json:"maxResults,omitempty"`
}

type listAccountSettingsOutput struct {
	NextToken string               `json:"nextToken,omitempty"`
	Settings  []accountSettingView `json:"settings"`
}

func (h *Handler) handleListAccountSettings(
	_ context.Context,
	in *listAccountSettingsInput,
) (*listAccountSettingsOutput, error) {
	settings, err := h.Backend.ListAccountSettings(in.Name, in.PrincipalArn)
	if err != nil {
		return nil, err
	}

	views := make([]accountSettingView, 0, len(settings))
	for _, s := range settings {
		views = append(views, accountSettingView(s))
	}

	sort.Slice(views, func(i, j int) bool { return views[i].Name < views[j].Name })

	p := page.New(views, in.NextToken, in.MaxResults, defaultECSMaxResults)

	return &listAccountSettingsOutput{Settings: p.Data, NextToken: p.Next}, nil
}

// ----- Handler: PutAccountSetting -----

type putAccountSettingInput struct {
	Name         string `json:"name"`
	Value        string `json:"value"`
	PrincipalArn string `json:"principalArn,omitempty"`
}

type putAccountSettingOutput struct {
	Setting accountSettingView `json:"setting"`
}

func (h *Handler) handlePutAccountSetting(
	_ context.Context,
	in *putAccountSettingInput,
) (*putAccountSettingOutput, error) {
	setting, err := h.Backend.PutAccountSetting(in.Name, in.Value, in.PrincipalArn)
	if err != nil {
		return nil, err
	}

	return &putAccountSettingOutput{Setting: accountSettingView{
		Name:         setting.Name,
		Value:        setting.Value,
		PrincipalArn: setting.PrincipalArn,
	}}, nil
}

// ----- Handler: PutAccountSettingDefault -----

type putAccountSettingDefaultInput struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type putAccountSettingDefaultOutput struct {
	Setting accountSettingView `json:"setting"`
}

func (h *Handler) handlePutAccountSettingDefault(
	_ context.Context,
	in *putAccountSettingDefaultInput,
) (*putAccountSettingDefaultOutput, error) {
	setting, err := h.Backend.PutAccountSettingDefault(in.Name, in.Value)
	if err != nil {
		return nil, err
	}

	return &putAccountSettingDefaultOutput{Setting: accountSettingView{
		Name:  setting.Name,
		Value: setting.Value,
	}}, nil
}

// ----- Handler: DeleteAccountSetting -----

type deleteAccountSettingInput struct {
	Name         string `json:"name"`
	PrincipalArn string `json:"principalArn,omitempty"`
}

type accountSettingView struct {
	Name         string `json:"name"`
	Value        string `json:"value,omitempty"`
	PrincipalArn string `json:"principalArn,omitempty"`
}

type deleteAccountSettingOutput struct {
	Setting accountSettingView `json:"setting"`
}

func (h *Handler) handleDeleteAccountSetting(
	_ context.Context,
	in *deleteAccountSettingInput,
) (*deleteAccountSettingOutput, error) {
	setting, err := h.Backend.DeleteAccountSetting(in.Name, in.PrincipalArn)
	if err != nil {
		return nil, err
	}

	return &deleteAccountSettingOutput{Setting: accountSettingView{
		Name:         setting.Name,
		Value:        setting.Value,
		PrincipalArn: setting.PrincipalArn,
	}}, nil
}
