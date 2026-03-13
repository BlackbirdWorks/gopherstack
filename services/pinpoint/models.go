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

// nowRFC3339 returns the current UTC time formatted as RFC 3339.
func nowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339)
}
