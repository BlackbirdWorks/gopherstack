package serverlessrepo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// createApplicationRequest is the request body for CreateApplication.
type createApplicationRequest struct {
	Name                 string   `json:"name"`
	Description          string   `json:"description"`
	Author               string   `json:"author"`
	HomePageURL          string   `json:"homePageUrl"`
	LicenseBody          string   `json:"licenseBody"`
	LicenseURL           string   `json:"licenseUrl"`
	ReadmeBody           string   `json:"readmeBody"`
	ReadmeURL            string   `json:"readmeUrl"`
	SpdxLicenseID        string   `json:"spdxLicenseId"`
	SourceCodeURL        string   `json:"sourceCodeUrl"`
	SourceCodeArchiveURL string   `json:"sourceCodeArchiveUrl"`
	TemplateBody         string   `json:"templateBody"`
	TemplateURL          string   `json:"templateUrl"`
	SemanticVersion      string   `json:"semanticVersion"`
	Labels               []string `json:"labels"`
}

// resolveCreateApplicationBodies validates the licenseBody/licenseUrl, readmeBody/readmeUrl,
// and templateBody/templateUrl mutual-exclusivity constraints documented on the real AWS SAR
// CreateApplicationInput ("You can specify only one of X and Y; otherwise, an error results")
// and, when only the *Body variant is given, synthesizes the *Url the real service would
// return after uploading the inline content to S3.
func resolveCreateApplicationBodies(req *createApplicationRequest) error {
	if req.LicenseBody != "" {
		if req.LicenseURL != "" {
			return fmt.Errorf("%w: only one of licenseBody and licenseUrl may be specified", ErrValidation)
		}

		req.LicenseURL = synthesizeLicenseURL(req.Name)
	}

	if req.ReadmeBody != "" {
		if req.ReadmeURL != "" {
			return fmt.Errorf("%w: only one of readmeBody and readmeUrl may be specified", ErrValidation)
		}

		req.ReadmeURL = synthesizeReadmeURL(req.Name)
	}

	if req.TemplateBody != "" {
		if req.TemplateURL != "" {
			return fmt.Errorf("%w: only one of templateBody and templateUrl may be specified", ErrValidation)
		}

		req.TemplateURL = synthesizeTemplateURL(req.Name, req.SemanticVersion)
	}

	return nil
}

// versionResponse represents the SAR Version type in API responses.
// The botocore SAR model expects "version" to be a struct, not a plain string.
type versionResponse struct {
	ApplicationID        string                `json:"applicationId,omitempty"`
	CreationTime         string                `json:"creationTime,omitempty"`
	SemanticVersion      string                `json:"semanticVersion,omitempty"`
	TemplateURL          string                `json:"templateUrl,omitempty"`
	SourceCodeURL        string                `json:"sourceCodeUrl,omitempty"`
	SourceCodeArchiveURL string                `json:"sourceCodeArchiveUrl,omitempty"`
	ParameterDefinitions []ParameterDefinition `json:"parameterDefinitions"`
	RequiredCapabilities []string              `json:"requiredCapabilities"`
	ResourcesSupported   bool                  `json:"resourcesSupported"`
}

// applicationResponse represents the API response shape for a single application.
// CreateApplicationOutput/GetApplicationOutput/UpdateApplicationOutput
// (aws-sdk-go-v2/service/serverlessapplicationrepository@v1.33.4, api_op_GetApplication.go)
// carry no top-level sourceCodeUrl -- that field exists only nested under Version.
type applicationResponse struct {
	Version           *versionResponse `json:"version,omitempty"`
	HomePageURL       string           `json:"homePageUrl,omitempty"`
	ApplicationID     string           `json:"applicationId"`
	Name              string           `json:"name"`
	Description       string           `json:"description"`
	Author            string           `json:"author"`
	CreationTime      string           `json:"creationTime"`
	LicenseURL        string           `json:"licenseUrl,omitempty"`
	ReadmeURL         string           `json:"readmeUrl,omitempty"`
	SpdxLicenseID     string           `json:"spdxLicenseId,omitempty"`
	VerifiedAuthorURL string           `json:"verifiedAuthorUrl,omitempty"`
	Labels            []string         `json:"labels,omitempty"`
	IsVerifiedAuthor  bool             `json:"isVerifiedAuthor"`
}

// applicationSummary is a summary used in list responses.
type applicationSummary struct {
	CreationTime  string   `json:"creationTime"`
	ApplicationID string   `json:"applicationId"`
	Name          string   `json:"name"`
	Description   string   `json:"description"`
	Author        string   `json:"author"`
	HomePageURL   string   `json:"homePageUrl,omitempty"`
	SpdxLicenseID string   `json:"spdxLicenseId,omitempty"`
	Labels        []string `json:"labels,omitempty"`
}

func toApplicationResponse(a *Application) applicationResponse {
	resp := applicationResponse{
		ApplicationID:     a.ApplicationID,
		Name:              a.Name,
		Description:       a.Description,
		Author:            a.Author,
		HomePageURL:       a.HomePageURL,
		LicenseURL:        a.LicenseURL,
		ReadmeURL:         a.ReadmeURL,
		SpdxLicenseID:     a.SpdxLicenseID,
		CreationTime:      isoTimestamp(a.CreationTime),
		IsVerifiedAuthor:  a.IsVerifiedAuthor,
		VerifiedAuthorURL: a.VerifiedAuthorURL,
		Labels:            a.Labels,
	}

	if a.SemanticVersion != "" {
		resp.Version = &versionResponse{
			ApplicationID:        a.ApplicationID,
			CreationTime:         isoTimestamp(a.CreationTime),
			SemanticVersion:      a.SemanticVersion,
			ParameterDefinitions: []ParameterDefinition{},
			RequiredCapabilities: []string{},
			ResourcesSupported:   true,
		}
	}

	return resp
}

func toEmbeddedVersionResponse(v *ApplicationVersion) *versionResponse {
	return &versionResponse{
		ApplicationID:        v.ApplicationID,
		CreationTime:         isoTimestamp(v.CreationTime),
		SemanticVersion:      v.SemanticVersion,
		TemplateURL:          v.TemplateURL,
		SourceCodeURL:        v.SourceCodeURL,
		SourceCodeArchiveURL: v.SourceCodeArchiveURL,
		ParameterDefinitions: v.ParameterDefinitions,
		RequiredCapabilities: v.RequiredCapabilities,
		ResourcesSupported:   v.ResourcesSupported,
	}
}

func (h *Handler) handleCreateApplication(ctx context.Context, body []byte) ([]byte, error) {
	var req createApplicationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := resolveCreateApplicationBodies(&req); err != nil {
		return nil, err
	}

	a, err := h.Backend.CreateApplication(
		req.Name,
		req.Description,
		req.Author,
		req.SourceCodeURL,
		req.SemanticVersion,
		req.Labels,
		req.HomePageURL,
		req.LicenseURL,
		req.SpdxLicenseID,
	)
	if err != nil {
		return nil, err
	}

	if req.ReadmeURL != "" {
		a, err = h.Backend.SetApplicationReadmeURL(a.Name, req.ReadmeURL)
		if err != nil {
			return nil, err
		}
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "serverlessrepo: created application", "name", a.Name, "id", a.ApplicationID)

	resp := toApplicationResponse(a)
	if req.SemanticVersion != "" &&
		(req.SourceCodeURL != "" || req.SourceCodeArchiveURL != "" || req.TemplateURL != "") {
		v, versionErr := h.Backend.CreateApplicationVersionWithOptions(
			a.Name,
			req.SemanticVersion,
			CreateApplicationVersionOptions{
				SourceCodeURL:        req.SourceCodeURL,
				SourceCodeArchiveURL: req.SourceCodeArchiveURL,
				TemplateURL:          req.TemplateURL,
			},
		)
		if versionErr != nil {
			return nil, versionErr
		}

		resp.Version = toEmbeddedVersionResponse(v)
	}

	b, marshalErr := json.Marshal(resp)
	if marshalErr != nil {
		return nil, marshalErr
	}

	return b, errHTTP201
}

func (h *Handler) handleGetApplication(req *http.Request) ([]byte, error) {
	name, err := extractApplicationName(req)
	if err != nil {
		return nil, err
	}

	a, err := h.Backend.GetApplication(name)
	if err != nil {
		return nil, err
	}

	resp := toApplicationResponse(a)

	// A query parameter selects a version; otherwise GetApplication returns its current version.
	sv := req.URL.Query().Get(keySemanticVersion)
	explicitVersion := sv != ""
	if sv == "" {
		sv = a.SemanticVersion
	}

	if sv != "" {
		v, vErr := h.Backend.GetApplicationVersion(name, sv)
		if vErr != nil {
			if explicitVersion {
				return nil, vErr
			}
		} else {
			resp.Version = toEmbeddedVersionResponse(v)
		}
	}

	return json.Marshal(resp)
}

func (h *Handler) handleListApplications(req *http.Request) ([]byte, error) {
	apps := h.Backend.ListApplications()

	// Apply pagination: nextToken is treated as the last-seen application name (exclusive cursor).
	nextToken := req.URL.Query().Get("nextToken")
	maxItems := parseMaxItems(req.URL.Query().Get("maxItems"), maxItemsDefault)

	start := 0

	if nextToken != "" {
		for i, a := range apps {
			if a.Name == nextToken {
				start = i + 1

				break
			}
		}
	}

	end := min(start+maxItems, len(apps))

	page := apps[start:end]

	summaries := make([]applicationSummary, 0, len(page))

	for _, a := range page {
		summaries = append(summaries, applicationSummary{
			ApplicationID: a.ApplicationID,
			Name:          a.Name,
			Description:   a.Description,
			Author:        a.Author,
			HomePageURL:   a.HomePageURL,
			SpdxLicenseID: a.SpdxLicenseID,
			Labels:        a.Labels,
			CreationTime:  isoTimestamp(a.CreationTime),
		})
	}

	resp := map[string]any{"applications": summaries}

	if end < len(apps) {
		resp["nextToken"] = apps[end-1].Name
	}

	return json.Marshal(resp)
}

// updateApplicationRequest is the request body for UpdateApplication.
type updateApplicationRequest struct {
	Description string   `json:"description"`
	Author      string   `json:"author"`
	HomePageURL string   `json:"homePageUrl"`
	ReadmeBody  string   `json:"readmeBody"`
	ReadmeURL   string   `json:"readmeUrl"`
	Labels      []string `json:"labels"`
}

func (h *Handler) handleUpdateApplication(ctx context.Context, req *http.Request, body []byte) ([]byte, error) {
	name, nameErr := extractApplicationName(req)
	if nameErr != nil {
		return nil, nameErr
	}

	var updateReq updateApplicationRequest
	if err := json.Unmarshal(body, &updateReq); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	// Real AWS SAR accepts only one of readmeBody and readmeUrl on UpdateApplication;
	// specifying both is a BadRequestException. When only readmeBody is given, AWS uploads its
	// content to S3 and returns that generated location as readmeUrl; gopherstack emulates
	// that S3 upload with a deterministic synthesized URL.
	if updateReq.ReadmeBody != "" {
		if updateReq.ReadmeURL != "" {
			return nil, fmt.Errorf("%w: only one of readmeBody and readmeUrl may be specified", ErrValidation)
		}

		updateReq.ReadmeURL = synthesizeReadmeURL(name)
	}

	a, err := h.Backend.UpdateApplication(
		name,
		updateReq.Description,
		updateReq.Author,
		updateReq.HomePageURL,
		updateReq.ReadmeURL,
	)
	if err != nil {
		return nil, err
	}

	if updateReq.Labels != nil {
		a, err = h.Backend.UpdateApplicationLabels(name, updateReq.Labels)
		if err != nil {
			return nil, err
		}
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "serverlessrepo: updated application", "name", a.Name)

	resp := toApplicationResponse(a)

	if a.SemanticVersion != "" {
		if v, vErr := h.Backend.GetApplicationVersion(name, a.SemanticVersion); vErr == nil {
			resp.Version = toEmbeddedVersionResponse(v)
		}
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDeleteApplication(ctx context.Context, req *http.Request) error {
	name, nameErr := extractApplicationName(req)
	if nameErr != nil {
		return nameErr
	}

	if err := h.Backend.DeleteApplication(name); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "serverlessrepo: deleted application", "name", name)

	return nil
}

// unshareApplicationRequest is the request body for UnshareApplication.
type unshareApplicationRequest struct {
	OrganizationID string `json:"organizationId"`
}

func (h *Handler) handleUnshareApplication(ctx context.Context, req *http.Request, body []byte) error {
	appName, err := extractApplicationName(req)
	if err != nil {
		return err
	}

	var unshareReq unshareApplicationRequest
	if jsonErr := json.Unmarshal(body, &unshareReq); jsonErr != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, jsonErr)
	}

	if unshareReq.OrganizationID == "" {
		return fmt.Errorf("%w: organizationId is required", errInvalidRequest)
	}

	if backendErr := h.Backend.UnshareApplication(appName, unshareReq.OrganizationID); backendErr != nil {
		return backendErr
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "serverlessrepo: unshared application",
		"app", appName, "orgId", unshareReq.OrganizationID)

	return nil
}
