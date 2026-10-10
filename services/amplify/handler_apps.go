package amplify

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
)

// JSON response keys used by the app handlers.
const keyApp = "app"

// handleApps handles POST/GET /apps.
func (h *Handler) handleApps(ctx context.Context, c *echo.Context) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.createApp(ctx, c)
	case http.MethodGet:
		return h.listApps(ctx, c)
	default:
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleAppID handles GET/POST/DELETE /apps/{appId}.
func (h *Handler) handleAppID(ctx context.Context, c *echo.Context, appID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getApp(ctx, c, appID)
	case http.MethodDelete:
		return h.deleteApp(ctx, c, appID)
	case http.MethodPost:
		return h.updateApp(ctx, c, appID)
	default:
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// createAppRequest is the wire shape of a CreateApp request body, mirroring
// aws-sdk-go-v2/service/amplify's CreateAppInput field-for-field (minus
// AccessToken/OauthToken, which are write-only secrets real Amplify uses to
// authorize against a Git provider and never stores -- gopherstack has no
// external Git provider to authorize against, so they are accepted but
// intentionally discarded, same as it does today for every other AWS
// service stub's credential-shaped fields).
// appJobConfigInput mirrors aws-sdk-go-v2/service/amplify/types.JobConfig, the
// nested wire shape of CreateAppInput/UpdateAppInput's "jobConfig" member.
type appJobConfigInput struct {
	BuildComputeType string `json:"buildComputeType"`
}

type createAppRequest struct {
	Tags                       map[string]string         `json:"tags"`
	EnvironmentVariables       map[string]string         `json:"environmentVariables"`
	AutoBranchCreationConfig   *AutoBranchCreationConfig `json:"autoBranchCreationConfig"`
	CacheConfig                *CacheConfig              `json:"cacheConfig"`
	JobConfig                  *appJobConfigInput        `json:"jobConfig"`
	EnableBranchAutoBuild      *bool                     `json:"enableBranchAutoBuild"`
	BasicAuthCredentials       string                    `json:"basicAuthCredentials"`
	Repository                 string                    `json:"repository"`
	Platform                   string                    `json:"platform"`
	Description                string                    `json:"description"`
	BuildSpec                  string                    `json:"buildSpec"`
	CustomHeaders              string                    `json:"customHeaders"`
	IAMServiceRoleArn          string                    `json:"iamServiceRoleArn"`
	ComputeRoleArn             string                    `json:"computeRoleArn"`
	Name                       string                    `json:"name"`
	AutoBranchCreationPatterns []string                  `json:"autoBranchCreationPatterns"`
	CustomRules                []CustomRule              `json:"customRules"`
	EnableBasicAuth            bool                      `json:"enableBasicAuth"`
	EnableAutoBranchCreation   bool                      `json:"enableAutoBranchCreation"`
	EnableBranchAutoDeletion   bool                      `json:"enableBranchAutoDeletion"`
}

// toAppOptions converts a CreateApp body into backend options; absent members take Amplify defaults.
func (r createAppRequest) toAppOptions() AppOptions {
	opts := AppOptions{
		EnvironmentVariables:       r.EnvironmentVariables,
		AutoBranchCreationConfig:   r.AutoBranchCreationConfig,
		CacheConfig:                r.CacheConfig,
		AutoBranchCreationPatterns: r.AutoBranchCreationPatterns,
		CustomRules:                r.CustomRules,
		EnableBranchAutoBuild:      r.EnableBranchAutoBuild,
		BasicAuthCredentials:       ptrconv.NilIfEmpty(r.BasicAuthCredentials),
		BuildSpec:                  ptrconv.NilIfEmpty(r.BuildSpec),
		CustomHeaders:              ptrconv.NilIfEmpty(r.CustomHeaders),
		IAMServiceRoleArn:          ptrconv.NilIfEmpty(r.IAMServiceRoleArn),
		ComputeRoleARN:             ptrconv.NilIfEmpty(r.ComputeRoleArn),
	}

	if r.JobConfig != nil {
		opts.JobConfigBuildComputeType = ptrconv.NilIfEmpty(r.JobConfig.BuildComputeType)
	}

	opts.EnableBasicAuth = &r.EnableBasicAuth
	opts.EnableAutoBranchCreation = &r.EnableAutoBranchCreation
	opts.EnableBranchAutoDeletion = &r.EnableBranchAutoDeletion

	return opts
}

// createApp handles POST /apps.
func (h *Handler) createApp(ctx context.Context, c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input createAppRequest

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	if input.Name == "" {
		return amplifyErrorJSON(c, http.StatusBadRequest, "name is required")
	}

	app, createErr := h.Backend.CreateApp(
		input.Name, input.Description, input.Repository, input.Platform, input.Tags,
		input.toAppOptions(),
	)
	if createErr != nil {
		return h.handleBackendError(ctx, c, "CreateApp", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyApp: toAppView(app)})
}

// getApp handles GET /apps/{appId}.
func (h *Handler) getApp(ctx context.Context, c *echo.Context, appID string) error {
	app, err := h.Backend.GetApp(appID)
	if err != nil {
		return h.handleBackendError(ctx, c, "GetApp", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyApp: toAppView(app)})
}

// listApps handles GET /apps.
func (h *Handler) listApps(ctx context.Context, c *echo.Context) error {
	q := c.Request().URL.Query()
	nextToken, maxResults, argErr := listPageArgs(q)
	if argErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, argErr.Error())
	}

	apps, outToken, err := h.Backend.ListApps(nextToken, maxResults)
	if err != nil {
		return h.handleBackendError(ctx, c, "ListApps", err)
	}

	resp := map[string]any{arnResourceApps: toAppViews(apps)}
	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

// deleteApp handles DELETE /apps/{appId}.
func (h *Handler) deleteApp(ctx context.Context, c *echo.Context, appID string) error {
	app, err := h.Backend.DeleteApp(appID)
	if err != nil {
		return h.handleBackendError(ctx, c, "DeleteApp", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyApp: toAppView(app)})
}

// updateApp handles POST /apps/{appId}.
func (h *Handler) updateApp(ctx context.Context, c *echo.Context, appID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input createAppRequest

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	app, updateErr := h.Backend.UpdateApp(
		appID, input.Name, "", "", input.Platform,
		input.toAppUpdateOptions(sentKeys(body)),
	)
	if updateErr != nil {
		return h.handleBackendError(ctx, c, "UpdateApp", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyApp: toAppView(app)})
}

// parseAppsOperation maps the method for /apps to its operation name.
func parseAppsOperation(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateApp"
	case http.MethodGet:
		return "ListApps"
	default:
		return opUnknown
	}
}

// parseAppIDOperation maps the method for /apps/{appId} to its operation name.
func parseAppIDOperation(method string) string {
	switch method {
	case http.MethodGet:
		return "GetApp"
	case http.MethodDelete:
		return "DeleteApp"
	case http.MethodPost:
		return "UpdateApp"
	default:
		return opUnknown
	}
}

// productionBranchView is the JSON representation of a ProductionBranch with
// LastDeployTime as a Unix epoch float64 value.
type productionBranchView struct {
	BranchName     string  `json:"branchName,omitempty"`
	Status         string  `json:"status,omitempty"`
	ThumbnailURL   string  `json:"thumbnailUrl,omitempty"`
	LastDeployTime float64 `json:"lastDeployTime,omitempty"`
}

func toProductionBranchView(pb *ProductionBranch) *productionBranchView {
	if pb == nil {
		return nil
	}

	v := &productionBranchView{
		BranchName:   pb.BranchName,
		Status:       pb.Status,
		ThumbnailURL: pb.ThumbnailURL,
	}

	if !pb.LastDeployTime.IsZero() {
		v.LastDeployTime = float64(pb.LastDeployTime.Unix())
	}

	return v
}

// appJobConfigView mirrors aws-sdk-go-v2/service/amplify/types.JobConfig on
// the response side.
type appJobConfigView struct {
	BuildComputeType string `json:"buildComputeType"`
}

// appView is the JSON representation of an App with timestamps as Unix epoch
// float64 values, as required by the AWS SDK v2 deserialiser.
type appView struct {
	Tags                       map[string]string         `json:"tags,omitempty"`
	EnvironmentVariables       map[string]string         `json:"environmentVariables"`
	AutoBranchCreationConfig   *AutoBranchCreationConfig `json:"autoBranchCreationConfig,omitempty"`
	CacheConfig                *CacheConfig              `json:"cacheConfig,omitempty"`
	ProductionBranch           *productionBranchView     `json:"productionBranch,omitempty"`
	JobConfig                  *appJobConfigView         `json:"jobConfig,omitempty"`
	BuildSpec                  string                    `json:"buildSpec,omitempty"`
	IAMServiceRoleArn          string                    `json:"iamServiceRoleArn,omitempty"`
	Name                       string                    `json:"name"`
	Description                string                    `json:"description"`
	Repository                 string                    `json:"repository"`
	DefaultDomain              string                    `json:"defaultDomain"`
	BasicAuthCredentials       string                    `json:"basicAuthCredentials,omitempty"`
	AppID                      string                    `json:"appId"`
	CustomHeaders              string                    `json:"customHeaders,omitempty"`
	ARN                        string                    `json:"appArn"`
	RepositoryCloneMethod      string                    `json:"repositoryCloneMethod,omitempty"`
	ComputeRoleArn             string                    `json:"computeRoleArn,omitempty"`
	Platform                   Platform                  `json:"platform"`
	CustomRules                []CustomRule              `json:"customRules,omitempty"`
	AutoBranchCreationPatterns []string                  `json:"autoBranchCreationPatterns,omitempty"`
	CreateTime                 float64                   `json:"createTime"`
	UpdateTime                 float64                   `json:"updateTime"`
	EnableBranchAutoBuild      bool                      `json:"enableBranchAutoBuild"`
	EnableBasicAuth            bool                      `json:"enableBasicAuth"`
	EnableAutoBranchCreation   bool                      `json:"enableAutoBranchCreation,omitempty"`
	EnableBranchAutoDeletion   bool                      `json:"enableBranchAutoDeletion,omitempty"`
}

func toAppView(a *App) appView {
	var tagMap map[string]string
	if a.Tags != nil {
		tagMap = a.Tags.Clone()
	}

	envVars := a.EnvironmentVariables
	if envVars == nil {
		envVars = map[string]string{}
	}

	var jobConfig *appJobConfigView
	if a.JobConfigBuildComputeType != "" {
		jobConfig = &appJobConfigView{BuildComputeType: a.JobConfigBuildComputeType}
	}

	return appView{
		Tags:                       tagMap,
		EnvironmentVariables:       envVars,
		AutoBranchCreationConfig:   a.AutoBranchCreationConfig,
		CacheConfig:                a.CacheConfig,
		ProductionBranch:           toProductionBranchView(a.ProductionBranch),
		JobConfig:                  jobConfig,
		CreateTime:                 float64(a.CreateTime.Unix()),
		UpdateTime:                 float64(a.UpdateTime.Unix()),
		AppID:                      a.AppID,
		ARN:                        a.ARN,
		Name:                       a.Name,
		Description:                a.Description,
		Repository:                 a.Repository,
		DefaultDomain:              a.DefaultDomain,
		BasicAuthCredentials:       a.BasicAuthCredentials,
		BuildSpec:                  a.BuildSpec,
		CustomHeaders:              a.CustomHeaders,
		IAMServiceRoleArn:          a.IAMServiceRoleArn,
		ComputeRoleArn:             a.ComputeRoleARN,
		RepositoryCloneMethod:      a.RepositoryCloneMethod,
		AutoBranchCreationPatterns: a.AutoBranchCreationPatterns,
		CustomRules:                a.CustomRules,
		Platform:                   a.Platform,
		EnableBranchAutoBuild:      a.EnableBranchAutoBuild,
		EnableBasicAuth:            a.EnableBasicAuth,
		EnableAutoBranchCreation:   a.EnableAutoBranchCreation,
		EnableBranchAutoDeletion:   a.EnableBranchAutoDeletion,
	}
}

func toAppViews(apps []*App) []appView {
	views := make([]appView, len(apps))
	for i, a := range apps {
		views[i] = toAppView(a)
	}

	return views
}
