package transfer

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

type webAppIdentityCenterConfigInput struct {
	InstanceArn string `json:"InstanceArn,omitempty"`
	Role        string `json:"Role,omitempty"`
}

type webAppIdentityProviderDetailsInput struct {
	IdentityCenterConfig *webAppIdentityCenterConfigInput `json:"IdentityCenterConfig,omitempty"`
}

type webAppVpcConfigInput struct {
	VpcID            string   `json:"VpcId,omitempty"`
	IPAddressType    string   `json:"IpAddressType,omitempty"`
	SecurityGroupIDs []string `json:"SecurityGroupIds,omitempty"`
	SubnetIDs        []string `json:"SubnetIds,omitempty"`
}

type webAppEndpointDetailsInput struct {
	Vpc *webAppVpcConfigInput `json:"Vpc,omitempty"`
}

type webAppUnitsInput struct {
	Provisioned int32 `json:"Provisioned,omitempty"`
}

type createWebAppInput struct {
	IdentityProviderDetails *webAppIdentityProviderDetailsInput `json:"IdentityProviderDetails"`
	EndpointDetails         *webAppEndpointDetailsInput         `json:"EndpointDetails,omitempty"`
	WebAppUnits             *webAppUnitsInput                   `json:"WebAppUnits,omitempty"`
	AccessEndpoint          string                              `json:"AccessEndpoint,omitempty"`
	WebAppEndpointPolicy    string                              `json:"WebAppEndpointPolicy,omitempty"`
	Tags                    []map[string]string                 `json:"Tags,omitempty"`
}

type createWebAppOutput struct {
	WebAppID string `json:"WebAppId"`
}

func (h *Handler) handleCreateWebApp(
	_ context.Context,
	in *createWebAppInput,
) (*createWebAppOutput, error) {
	backendIn := &CreateWebAppInput{
		AccessEndpoint:       in.AccessEndpoint,
		WebAppEndpointPolicy: in.WebAppEndpointPolicy,
		Tags:                 tagsFromList(in.Tags),
	}

	if in.IdentityProviderDetails != nil && in.IdentityProviderDetails.IdentityCenterConfig != nil {
		backendIn.IdentityCenterConfig = &WebAppIdentityCenterConfig{
			InstanceArn: in.IdentityProviderDetails.IdentityCenterConfig.InstanceArn,
			Role:        in.IdentityProviderDetails.IdentityCenterConfig.Role,
		}
	}

	if in.EndpointDetails != nil && in.EndpointDetails.Vpc != nil {
		backendIn.VpcConfig = &WebAppVpcConfig{
			SecurityGroupIDs: in.EndpointDetails.Vpc.SecurityGroupIDs,
			SubnetIDs:        in.EndpointDetails.Vpc.SubnetIDs,
			VpcID:            in.EndpointDetails.Vpc.VpcID,
			IPAddressType:    in.EndpointDetails.Vpc.IPAddressType,
		}
	}

	if in.WebAppUnits != nil {
		backendIn.WebAppUnits = in.WebAppUnits.Provisioned
	}

	w, err := h.Backend.CreateWebApp(backendIn)
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
		keyWebAppID:            w.WebAppID,
		keyArn:                 webAppARN(w.AccountID, w.Region, w.WebAppID),
		keyTags:                tagsToList(w.Tags),
		"AccessEndpoint":       w.AccessEndpoint,
		"WebAppEndpoint":       w.WebAppEndpoint,
		"WebAppEndpointPolicy": w.WebAppEndpointPolicy,
		"EndpointType":         webAppEndpointType(w),
	}

	if w.IdentityCenterConfig != nil {
		webAppMap["DescribedIdentityProviderDetails"] = map[string]any{
			"IdentityCenterConfig": map[string]any{
				"ApplicationArn": w.IdentityCenterConfig.ApplicationArn,
				"InstanceArn":    w.IdentityCenterConfig.InstanceArn,
				keyRole:          w.IdentityCenterConfig.Role,
			},
		}
	}

	if w.VpcConfig != nil {
		webAppMap["DescribedEndpointDetails"] = map[string]any{
			"Vpc": map[string]any{
				"SubnetIds":     w.VpcConfig.SubnetIDs,
				"VpcEndpointId": w.VpcConfig.VpcEndpointID,
				"VpcId":         w.VpcConfig.VpcID,
			},
		}
	}

	if w.WebAppUnits != 0 {
		webAppMap["WebAppUnits"] = map[string]any{"Provisioned": w.WebAppUnits}
	}

	return &describeWebAppOutput{WebApp: webAppMap}, nil
}

// webAppEndpointType returns the real-AWS EndpointType ("PUBLIC" or "VPC") for a web app.
func webAppEndpointType(w *WebApp) string {
	if w.VpcConfig != nil {
		return "VPC"
	}

	return "PUBLIC"
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
			keyWebAppID:      w.WebAppID,
			keyArn:           webAppARN(w.AccountID, w.Region, w.WebAppID),
			"AccessEndpoint": w.AccessEndpoint,
			"WebAppEndpoint": w.WebAppEndpoint,
			"EndpointType":   webAppEndpointType(w),
		}
	}

	return &listWebAppsOutput{WebApps: out, NextToken: next}, nil
}

type updateWebAppIdentityCenterConfigInput struct {
	Role string `json:"Role,omitempty"`
}

type updateWebAppIdentityProviderDetailsInput struct {
	IdentityCenterConfig *updateWebAppIdentityCenterConfigInput `json:"IdentityCenterConfig,omitempty"`
}

type updateWebAppVpcConfigInput struct {
	IPAddressType string   `json:"IpAddressType,omitempty"`
	SubnetIDs     []string `json:"SubnetIds,omitempty"`
}

type updateWebAppEndpointDetailsInput struct {
	Vpc *updateWebAppVpcConfigInput `json:"Vpc,omitempty"`
}

type updateWebAppInput struct {
	IdentityProviderDetails *updateWebAppIdentityProviderDetailsInput `json:"IdentityProviderDetails,omitempty"`
	EndpointDetails         *updateWebAppEndpointDetailsInput         `json:"EndpointDetails,omitempty"`
	WebAppUnits             *webAppUnitsInput                         `json:"WebAppUnits,omitempty"`
	WebAppID                string                                    `json:"WebAppId"`
	AccessEndpoint          string                                    `json:"AccessEndpoint,omitempty"`
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

	backendIn := &UpdateWebAppInput{
		WebAppID:       in.WebAppID,
		AccessEndpoint: in.AccessEndpoint,
	}

	if in.IdentityProviderDetails != nil && in.IdentityProviderDetails.IdentityCenterConfig != nil {
		role := in.IdentityProviderDetails.IdentityCenterConfig.Role
		backendIn.IdentityCenterRole = &role
	}

	if in.EndpointDetails != nil && in.EndpointDetails.Vpc != nil {
		backendIn.VpcSubnetIDs = in.EndpointDetails.Vpc.SubnetIDs

		if in.EndpointDetails.Vpc.IPAddressType != "" {
			ipType := in.EndpointDetails.Vpc.IPAddressType
			backendIn.VpcIPAddressType = &ipType
		}
	}

	if in.WebAppUnits != nil {
		units := in.WebAppUnits.Provisioned
		backendIn.WebAppUnits = &units
	}

	w, err := h.Backend.UpdateWebApp(backendIn)
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
