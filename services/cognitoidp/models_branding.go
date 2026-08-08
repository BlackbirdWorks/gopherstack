package cognitoidp

import "time"

// UICustomization stores hosted-UI CSS and logo settings for a pool or client.
type UICustomization struct {
	CreatedAt      time.Time `json:"createdAt"`
	LastModifiedAt time.Time `json:"lastModifiedAt"`
	UserPoolID     string    `json:"userPoolID,omitempty"`
	ClientID       string    `json:"clientID,omitempty"`
	CSS            string    `json:"css,omitempty"`
	ImageURL       string    `json:"imageURL,omitempty"`
}

// ManagedLoginBranding stores managed login branding for a pool client.
type ManagedLoginBranding struct {
	CreatedAt                time.Time        `json:"createdAt"`
	LastModifiedAt           time.Time        `json:"lastModifiedAt"`
	ManagedLoginBrandingID   string           `json:"managedLoginBrandingID,omitempty"`
	UserPoolID               string           `json:"userPoolID,omitempty"`
	ClientID                 string           `json:"clientID,omitempty"`
	Settings                 map[string]any   `json:"settings,omitempty"`
	Assets                   []map[string]any `json:"assets,omitempty"`
	UseCognitoProvidedValues bool             `json:"useCognitoProvidedValues,omitempty"`
}

type setUICustomizationFullInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
	CSS        string `json:"CSS,omitempty"`
	ImageData  string `json:"ImageData,omitempty"`
}

type uiCustomizationJSON struct {
	UserPoolID       string  `json:"UserPoolId,omitempty"`
	ClientID         string  `json:"ClientId,omitempty"`
	CSS              string  `json:"CSS,omitempty"`
	ImageURL         string  `json:"ImageUrl,omitempty"`
	CreationDate     float64 `json:"CreationDate,omitempty"`
	LastModifiedDate float64 `json:"LastModifiedDate,omitempty"`
}

type setUICustomizationFullOutput struct {
	UICustomization *uiCustomizationJSON `json:"UICustomization,omitempty"`
}

type getUICustomizationFullInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
}

type getUICustomizationFullOutput struct {
	UICustomization *uiCustomizationJSON `json:"UICustomization,omitempty"`
}

type createManagedLoginBrandingInput struct {
	UserPoolID               string           `json:"UserPoolId,omitempty"`
	ClientID                 string           `json:"ClientId,omitempty"`
	Settings                 map[string]any   `json:"Settings,omitempty"`
	Assets                   []map[string]any `json:"Assets,omitempty"`
	UseCognitoProvidedValues bool             `json:"UseCognitoProvidedValues,omitempty"`
}

type managedLoginBrandingType struct {
	ManagedLoginBrandingID   string           `json:"ManagedLoginBrandingId,omitempty"`
	UserPoolID               string           `json:"UserPoolId,omitempty"`
	Settings                 map[string]any   `json:"Settings,omitempty"`
	Assets                   []map[string]any `json:"Assets,omitempty"`
	UseCognitoProvidedValues bool             `json:"UseCognitoProvidedValues,omitempty"`
	CreationDate             float64          `json:"CreationDate,omitempty"`
	LastModifiedDate         float64          `json:"LastModifiedDate,omitempty"`
}

type createManagedLoginBrandingOutput struct {
	ManagedLoginBranding *managedLoginBrandingType `json:"ManagedLoginBranding,omitempty"`
}

type deleteManagedLoginBrandingInput struct {
	ManagedLoginBrandingID string `json:"ManagedLoginBrandingId,omitempty"`
	UserPoolID             string `json:"UserPoolId,omitempty"`
}

type deleteManagedLoginBrandingOutput struct{}

type describeManagedLoginBrandingInput struct {
	ManagedLoginBrandingID string `json:"ManagedLoginBrandingId,omitempty"`
	UserPoolID             string `json:"UserPoolId,omitempty"`
}

type describeManagedLoginBrandingOutput struct {
	ManagedLoginBranding *managedLoginBrandingType `json:"ManagedLoginBranding,omitempty"`
}

type describeManagedLoginBrandingByClientInput struct {
	ClientID   string `json:"ClientId,omitempty"`
	UserPoolID string `json:"UserPoolId,omitempty"`
}

type describeManagedLoginBrandingByClientOutput struct {
	ManagedLoginBranding *managedLoginBrandingType `json:"ManagedLoginBranding,omitempty"`
}

type updateManagedLoginBrandingInput struct {
	ManagedLoginBrandingID   string           `json:"ManagedLoginBrandingId,omitempty"`
	UserPoolID               string           `json:"UserPoolId,omitempty"`
	Settings                 map[string]any   `json:"Settings,omitempty"`
	Assets                   []map[string]any `json:"Assets,omitempty"`
	UseCognitoProvidedValues bool             `json:"UseCognitoProvidedValues,omitempty"`
}

type updateManagedLoginBrandingOutput struct {
	ManagedLoginBranding *managedLoginBrandingType `json:"ManagedLoginBranding,omitempty"`
}

type uiCustomizationType struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
	CSS        string `json:"CSS,omitempty"`
}

type getUICustomizationInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
}

type getUICustomizationOutput struct {
	UICustomization *uiCustomizationType `json:"UICustomization,omitempty"`
}

type setUICustomizationInput struct {
	UserPoolID string `json:"UserPoolId,omitempty"`
	ClientID   string `json:"ClientId,omitempty"`
	CSS        string `json:"CSS,omitempty"`
}

type setUICustomizationOutput struct {
	UICustomization *uiCustomizationType `json:"UICustomization,omitempty"`
}
