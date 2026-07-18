package transfer

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

type createWebAppInput struct {
	Tags []map[string]string `json:"Tags"`
}

type createWebAppOutput struct {
	WebAppID string `json:"WebAppId"`
}

func (h *Handler) handleCreateWebApp(
	_ context.Context,
	in *createWebAppInput,
) (*createWebAppOutput, error) {
	tags := tagsFromList(in.Tags)

	w, err := h.Backend.CreateWebApp(tags)
	if err != nil {
		return nil, err
	}

	return &createWebAppOutput{WebAppID: w.WebAppID}, nil
}

// webAppARN builds the ARN for a Transfer web app.
func webAppARN(accountID, region, webAppID string) string {
	return arn.Build("transfer", region, accountID, "webapp/"+webAppID)
}

type deleteWebAppInput struct {
	WebAppID string `json:"WebAppId"`
}

func (h *Handler) handleDeleteWebApp(_ context.Context, in *deleteWebAppInput) (*struct{}, error) {
	if in.WebAppID == "" {
		return nil, fmt.Errorf("%w: WebAppId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteWebApp(in.WebAppID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type describeWebAppInput struct {
	WebAppID string `json:"WebAppId"`
}

type describeWebAppOutput struct {
	WebApp map[string]any `json:"WebApp"`
}

func (h *Handler) handleDescribeWebApp(
	_ context.Context,
	in *describeWebAppInput,
) (*describeWebAppOutput, error) {
	if in.WebAppID == "" {
		return nil, fmt.Errorf("%w: WebAppId is required", errInvalidRequest)
	}

	w, err := h.Backend.DescribeWebApp(in.WebAppID)
	if err != nil {
		return nil, err
	}

	webAppMap := map[string]any{
		"WebAppId": w.WebAppID,
		keyArn:     webAppARN(w.AccountID, w.Region, w.WebAppID),
		keyTags:    tagsToList(w.Tags),
	}

	if w.IdentityProviderDetails != nil {
		webAppMap["IdentityProviderDetails"] = map[string]any{
			"IdentityProviderType": w.IdentityProviderDetails.IdentityProviderType,
			"InstanceArn":          w.IdentityProviderDetails.InstanceArn,
			keyRole:                w.IdentityProviderDetails.Role,
			keyURL:                 w.IdentityProviderDetails.URL,
			"Directory":            w.IdentityProviderDetails.Directory,
			"Function":             w.IdentityProviderDetails.Function,
		}
	}

	return &describeWebAppOutput{WebApp: webAppMap}, nil
}

type listWebAppsInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listWebAppsOutput struct {
	NextToken string           `json:"NextToken,omitempty"`
	WebApps   []map[string]any `json:"WebApps"`
}

func (h *Handler) handleListWebApps(
	_ context.Context,
	in *listWebAppsInput,
) (*listWebAppsOutput, error) {
	items := h.Backend.ListWebApps()
	page, next := applyNextTokenItems(items, in.NextToken, in.MaxResults)
	out := make([]map[string]any, len(page))

	for i, w := range page {
		out[i] = map[string]any{
			"WebAppId": w.WebAppID,
			keyArn:     webAppARN(w.AccountID, w.Region, w.WebAppID),
		}
	}

	return &listWebAppsOutput{WebApps: out, NextToken: next}, nil
}

type webAppIdentityProviderDetailsInput struct {
	IdentityProviderType string `json:"IdentityProviderType,omitempty"`
	InstanceArn          string `json:"InstanceArn,omitempty"`
	Role                 string `json:"Role,omitempty"`
	URL                  string `json:"Url,omitempty"`
	Directory            string `json:"Directory,omitempty"`
	Function             string `json:"Function,omitempty"`
}

type updateWebAppInput struct {
	IdentityProviderDetails *webAppIdentityProviderDetailsInput `json:"IdentityProviderDetails,omitempty"`
	WebAppID                string                              `json:"WebAppId"`
}

type updateWebAppOutput struct {
	WebAppID string `json:"WebAppId"`
}

func (h *Handler) handleUpdateWebApp(
	_ context.Context,
	in *updateWebAppInput,
) (*updateWebAppOutput, error) {
	if in.WebAppID == "" {
		return nil, fmt.Errorf("%w: WebAppId is required", errInvalidRequest)
	}

	var ipd *WebAppIdentityProviderDetails
	if in.IdentityProviderDetails != nil {
		ipd = &WebAppIdentityProviderDetails{
			IdentityProviderType: in.IdentityProviderDetails.IdentityProviderType,
			InstanceArn:          in.IdentityProviderDetails.InstanceArn,
			Role:                 in.IdentityProviderDetails.Role,
			URL:                  in.IdentityProviderDetails.URL,
			Directory:            in.IdentityProviderDetails.Directory,
			Function:             in.IdentityProviderDetails.Function,
		}
	}

	w, err := h.Backend.UpdateWebApp(in.WebAppID, ipd)
	if err != nil {
		return nil, err
	}

	return &updateWebAppOutput{WebAppID: w.WebAppID}, nil
}

type webAppCustomizationInput struct {
	WebAppID    string `json:"WebAppId"`
	Title       string `json:"Title"`
	LogoFile    string `json:"LogoFile"`
	FaviconFile string `json:"FaviconFile"`
}

type describeWebAppCustomizationOutput struct {
	WebAppCustomization map[string]any `json:"WebAppCustomization"`
}

func (h *Handler) handleDeleteWebAppCustomization(
	_ context.Context,
	in *webAppCustomizationInput,
) (*struct{}, error) {
	if in.WebAppID == "" {
		return nil, fmt.Errorf("%w: WebAppId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteWebAppCustomization(in.WebAppID); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleDescribeWebAppCustomization(
	_ context.Context,
	in *webAppCustomizationInput,
) (*describeWebAppCustomizationOutput, error) {
	if in.WebAppID == "" {
		return nil, fmt.Errorf("%w: WebAppId is required", errInvalidRequest)
	}

	c, err := h.Backend.DescribeWebAppCustomization(in.WebAppID)
	if err != nil {
		return nil, err
	}

	return &describeWebAppCustomizationOutput{
		WebAppCustomization: map[string]any{
			keyWebAppID:   c.WebAppID,
			"Title":       c.Title,
			"LogoFile":    c.LogoFile,
			"FaviconFile": c.FaviconFile,
		},
	}, nil
}

func (h *Handler) handleUpdateWebAppCustomization(
	_ context.Context,
	in *webAppCustomizationInput,
) (*struct{}, error) {
	if in.WebAppID == "" {
		return nil, fmt.Errorf("%w: WebAppId is required", errInvalidRequest)
	}

	if _, err := h.Backend.UpdateWebAppCustomization(in.WebAppID, in.Title, in.LogoFile, in.FaviconFile); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}
