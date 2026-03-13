package pinpoint

import "time"

// App represents an Amazon Pinpoint application.
type App struct {
	Tags         map[string]string
	ARN          string
	ID           string
	Name         string
	CreationDate string
}

// createAppRequest is the request body for creating a Pinpoint app.
type createAppRequest struct {
	Tags map[string]string `json:"tags,omitempty"`
	Name string            `json:"Name"`
}

// appResponse is the JSON representation of an ApplicationResponse.
type appResponse struct {
	Tags         map[string]string `json:"tags,omitempty"`
	ARN          string            `json:"Arn"`
	ID           string            `json:"Id"`
	Name         string            `json:"Name"`
	CreationDate string            `json:"CreationDate,omitempty"`
}

// appsResponse is the JSON representation of ApplicationsResponse (GetApps).
type appsResponse struct {
	NextToken *string       `json:"NextToken,omitempty"`
	Item      []appResponse `json:"Item"`
}

// tagsModel is the JSON representation of TagsModel.
type tagsModel struct {
	Tags map[string]string `json:"tags"`
}

// tagResourceRequest is the request body for TagResource.
type tagResourceRequest struct {
	Tags map[string]string `json:"tags"`
}

// appSettingsResponse is the JSON representation of ApplicationSettingsResource.
// CampaignHook, Limits, and QuietTime must be non-nil empty objects so the
// Terraform provider's flatten helpers do not dereference nil pointers.
type appSettingsResponse struct {
	CampaignHook     map[string]any `json:"CampaignHook"`
	Limits           map[string]any `json:"Limits"`
	QuietTime        map[string]any `json:"QuietTime"`
	ApplicationID    string         `json:"ApplicationId"`
	LastModifiedDate string         `json:"LastModifiedDate,omitempty"`
}

// newAppSettingsResponse builds an ApplicationSettingsResource response with
// empty (non-nil) nested objects for CampaignHook, Limits, and QuietTime.
// The Terraform provider's flatten helpers dereference these pointers directly
// and panic if they are nil, so we must always return non-nil empty structs.
func newAppSettingsResponse(appID string) appSettingsResponse {
	return appSettingsResponse{
		ApplicationID:    appID,
		LastModifiedDate: nowRFC3339(),
		CampaignHook:     map[string]any{},
		Limits:           map[string]any{},
		QuietTime:        map[string]any{},
	}
}

// nowRFC3339 returns the current UTC time formatted as RFC 3339.
func nowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339)
}
