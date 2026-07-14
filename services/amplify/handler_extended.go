package amplify

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	subWebhooks            = "webhooks"
	subBackendEnvironments = "backendenvironments"
	subDomains             = "domains"
	subJobs                = "jobs"
	subDeployments         = "deployments"
	subAccessLogs          = "accesslogs"
	subArtifactsRoot       = "artifacts"
	subStop                = "stop"
	subStart               = "start"

	// Path segment counts for extended routes.
	pathSegsAppBranchSub  = 5 // ["apps", "{appId}", "branches", "{branchName}", "jobs"]
	pathSegsAppBranchItem = 6 // ["apps", "{appId}", "branches", "{branchName}", "jobs", "{jobId}"]
	pathSegsJobAction     = 7 // ["apps", "{appId}", "branches", "{branchName}", "jobs", "{jobId}", "action"]

	// JSON response keys used in multiple handler responses.
	keyApp                = "app"
	keyBranch             = "branch"
	keyWebhook            = "webhook"
	keyBackendEnvironment = "backendEnvironment"
	keyDomainAssociation  = "domainAssociation"
	keyJobSummary         = "jobSummary"
)

// ---- View types ----

type webhookView struct {
	WebhookID   string  `json:"webhookId"`
	WebhookARN  string  `json:"webhookArn"`
	AppID       string  `json:"appId"`
	BranchName  string  `json:"branchName"`
	Description string  `json:"description,omitempty"`
	WebhookURL  string  `json:"webhookUrl"`
	CreateTime  float64 `json:"createTime"`
	UpdateTime  float64 `json:"updateTime"`
}

type backendEnvironmentView struct {
	EnvironmentName       string  `json:"environmentName"`
	BackendEnvironmentARN string  `json:"backendEnvironmentArn"`
	AppID                 string  `json:"appId"`
	StackName             string  `json:"stackName,omitempty"`
	DeploymentArtifacts   string  `json:"deploymentArtifacts,omitempty"`
	CreateTime            float64 `json:"createTime"`
	UpdateTime            float64 `json:"updateTime"`
}

type subDomainSettingView struct {
	Prefix     string `json:"prefix"`
	BranchName string `json:"branchName"`
}

type subDomainView struct {
	SubDomainSetting subDomainSettingView `json:"subDomainSetting"`
	DNSRecord        string               `json:"dnsRecord,omitempty"`
	Verified         bool                 `json:"verified"`
}

type domainAssociationView struct {
	AppID                            string          `json:"appId"`
	DomainName                       string          `json:"domainName"`
	ARN                              string          `json:"domainAssociationArn"`
	DomainStatus                     string          `json:"domainStatus"`
	StatusReason                     string          `json:"statusReason,omitempty"`
	CertificateVerificationDNSRecord string          `json:"certificateVerificationDNSRecord,omitempty"`
	SubDomains                       []subDomainView `json:"subDomains"`
	EnableAutoSubDomain              bool            `json:"enableAutoSubDomain"`
}

type jobSummaryView struct {
	JobID     string  `json:"jobId"`
	JobARN    string  `json:"jobArn"`
	CommitID  string  `json:"commitId,omitempty"`
	CommitMsg string  `json:"commitMessage,omitempty"`
	Status    string  `json:"status"`
	Type      string  `json:"jobType"`
	StartTime float64 `json:"startTime"`
	EndTime   float64 `json:"endTime,omitempty"`
}

type artifactView struct {
	ArtifactID       string `json:"artifactId"`
	ArtifactType     string `json:"artifactType"`
	ArtifactFileName string `json:"artifactFileName"`
}

// ---- Converters ----

func toWebhookView(w *Webhook) webhookView {
	return webhookView{
		CreateTime:  float64(w.CreateTime.Unix()),
		UpdateTime:  float64(w.UpdateTime.Unix()),
		WebhookID:   w.WebhookID,
		WebhookARN:  w.WebhookARN,
		AppID:       w.AppID,
		BranchName:  w.BranchName,
		Description: w.Description,
		WebhookURL:  w.WebhookURL,
	}
}

func toWebhookViews(ws []*Webhook) []webhookView {
	views := make([]webhookView, len(ws))
	for i, w := range ws {
		views[i] = toWebhookView(w)
	}

	return views
}

func toBackendEnvironmentView(be *BackendEnvironment) backendEnvironmentView {
	return backendEnvironmentView{
		CreateTime:            float64(be.CreateTime.Unix()),
		UpdateTime:            float64(be.UpdateTime.Unix()),
		EnvironmentName:       be.EnvironmentName,
		BackendEnvironmentARN: be.BackendEnvironmentARN,
		AppID:                 be.AppID,
		StackName:             be.StackName,
		DeploymentArtifacts:   be.DeploymentArtifacts,
	}
}

func toBackendEnvironmentViews(envs []*BackendEnvironment) []backendEnvironmentView {
	views := make([]backendEnvironmentView, len(envs))
	for i, be := range envs {
		views[i] = toBackendEnvironmentView(be)
	}

	return views
}

func toDomainAssociationView(d *DomainAssociation) domainAssociationView {
	subs := make([]subDomainView, len(d.SubDomains))
	for i, sd := range d.SubDomains {
		subs[i] = subDomainView{
			SubDomainSetting: subDomainSettingView{
				Prefix:     sd.SubDomainSetting.Prefix,
				BranchName: sd.SubDomainSetting.BranchName,
			},
			DNSRecord: sd.DNSRecord,
			Verified:  sd.Verified,
		}
	}

	return domainAssociationView{
		SubDomains:                       subs,
		AppID:                            d.AppID,
		DomainName:                       d.DomainName,
		ARN:                              d.ARN,
		DomainStatus:                     string(d.DomainStatus),
		StatusReason:                     d.StatusReason,
		CertificateVerificationDNSRecord: d.CertificateVerificationDNSRecord,
		EnableAutoSubDomain:              d.EnableAutoSubDomain,
	}
}

func toDomainAssociationViews(ds []*DomainAssociation) []domainAssociationView {
	views := make([]domainAssociationView, len(ds))
	for i, d := range ds {
		views[i] = toDomainAssociationView(d)
	}

	return views
}

func toJobSummaryView(j *Job) jobSummaryView {
	v := jobSummaryView{
		StartTime: float64(j.StartTime.Unix()),
		JobID:     j.JobID,
		JobARN:    j.JobARN,
		CommitID:  j.CommitID,
		CommitMsg: j.CommitMsg,
		Status:    string(j.Status),
		Type:      string(j.Type),
	}

	if !j.EndTime.IsZero() {
		v.EndTime = float64(j.EndTime.Unix())
	}

	return v
}

func toJobSummaryViews(jobs []*Job) []jobSummaryView {
	views := make([]jobSummaryView, len(jobs))
	for i, j := range jobs {
		views[i] = toJobSummaryView(j)
	}

	return views
}

func toArtifactViews(arts []*Artifact) []artifactView {
	views := make([]artifactView, len(arts))
	for i, a := range arts {
		views[i] = artifactView{
			ArtifactID:       a.ArtifactID,
			ArtifactType:     a.ArtifactType,
			ArtifactFileName: a.ArtifactFileName,
		}
	}

	return views
}

// ---- Parse helpers ----

func parseWebhookIDOp(method string) string {
	switch method {
	case http.MethodGet:
		return "GetWebhook"
	case http.MethodPost:
		return "UpdateWebhook"
	case http.MethodDelete:
		return "DeleteWebhook"
	default:
		return opUnknown
	}
}

func parseAppsPathOp(method string, segs []string) string {
	switch len(segs) {
	case pathSegsApps:
		return parseAppsOperation(method)
	case pathSegsAppID:
		return parseAppIDOperation(method)
	case pathSegsAppSub:
		return parseAppSubPathOp(method, segs[2])
	case pathSegsAppBranch:
		return parseAppItemPathOp(method, segs[2])
	case pathSegsAppBranchSub:
		if segs[2] != arnResourceBranches {
			return opUnknown
		}

		return parseAppBranchSubPathOp(method, segs[4])
	case pathSegsAppBranchItem:
		if segs[2] != arnResourceBranches {
			return opUnknown
		}

		return parseAppBranchItemPathOp(method, segs[4], segs[5])
	case pathSegsJobAction:
		if segs[2] != arnResourceBranches || segs[4] != subJobs {
			return opUnknown
		}

		return parseJobActionPathOp(method, segs[6])
	default:
		return opUnknown
	}
}

func parseAppSubPathOp(method, sub string) string {
	switch sub {
	case arnResourceBranches:
		return parseBranchesOperation(method)
	case subWebhooks:
		return parseListOrCreateOp(method, "ListWebhooks", "CreateWebhook")
	case subBackendEnvironments:
		return parseListOrCreateOp(method, "ListBackendEnvironments", "CreateBackendEnvironment")
	case subDomains:
		return parseListOrCreateOp(method, "ListDomainAssociations", "CreateDomainAssociation")
	case subAccessLogs:
		if method == http.MethodPost {
			return "GenerateAccessLogs"
		}

		return opUnknown
	default:
		return opUnknown
	}
}

func parseListOrCreateOp(method, listOp, createOp string) string {
	switch method {
	case http.MethodGet:
		return listOp
	case http.MethodPost:
		return createOp
	default:
		return opUnknown
	}
}

func parseAppItemPathOp(method, sub string) string {
	switch {
	case sub == arnResourceBranches:
		return parseBranchOperation(method)
	case sub == subBackendEnvironments && method == http.MethodGet:
		return "GetBackendEnvironment"
	case sub == subBackendEnvironments && method == http.MethodDelete:
		return "DeleteBackendEnvironment"
	case sub == subDomains && method == http.MethodGet:
		return "GetDomainAssociation"
	case sub == subDomains && method == http.MethodDelete:
		return "DeleteDomainAssociation"
	case sub == subDomains && method == http.MethodPost:
		return "UpdateDomainAssociation"
	default:
		return opUnknown
	}
}

func parseAppBranchSubPathOp(method, sub string) string {
	switch {
	case sub == subJobs && method == http.MethodPost:
		return "StartJob"
	case sub == subJobs && method == http.MethodGet:
		return "ListJobs"
	case sub == subDeployments && method == http.MethodPost:
		return "CreateDeployment"
	default:
		return opUnknown
	}
}

func parseAppBranchItemPathOp(method, sub, item string) string {
	switch {
	case sub == subJobs && method == http.MethodGet:
		return "GetJob"
	case sub == subJobs && method == http.MethodDelete:
		return "DeleteJob"
	case sub == subDeployments && item == subStart && method == http.MethodPost:
		return "StartDeployment"
	default:
		return opUnknown
	}
}

func parseJobActionPathOp(method, action string) string {
	switch {
	case action == subStop && method == http.MethodDelete:
		return "StopJob"
	case action == subArtifactsRoot && method == http.MethodGet:
		return "ListArtifacts"
	default:
		return opUnknown
	}
}

// ---- Routing helpers ----

// routeApps dispatches /apps/* paths.
func (h *Handler) routeApps(ctx context.Context, c *echo.Context, segs []string) error {
	switch len(segs) {
	case pathSegsApps:
		return h.handleApps(ctx, c)
	case pathSegsAppID:
		return h.handleAppID(ctx, c, segs[1])
	case pathSegsAppSub:
		return h.routeAppSub(ctx, c, segs)
	case pathSegsAppBranch:
		return h.routeAppItem(ctx, c, segs)
	case pathSegsAppBranchSub:
		return h.routeAppBranchSub(ctx, c, segs)
	case pathSegsAppBranchItem:
		return h.routeAppBranchItem(ctx, c, segs)
	case pathSegsJobAction:
		return h.routeJobAction(ctx, c, segs)
	default:
		return amplifyErrorJSON(c, http.StatusNotFound, "not found")
	}
}

// routeAppSub dispatches /apps/{appId}/{sub}.
func (h *Handler) routeAppSub(ctx context.Context, c *echo.Context, segs []string) error {
	appID, sub := segs[1], segs[2]
	switch sub {
	case arnResourceBranches:
		return h.handleBranches(ctx, c, appID)
	case subWebhooks:
		return h.handleAppWebhooks(ctx, c, appID)
	case subBackendEnvironments:
		return h.handleBackendEnvironments(ctx, c, appID)
	case subDomains:
		return h.handleDomainAssociations(ctx, c, appID)
	case subAccessLogs:
		return h.generateAccessLogs(ctx, c, appID)
	default:
		return amplifyErrorJSON(c, http.StatusNotFound, "not found")
	}
}

// routeAppItem dispatches /apps/{appId}/{sub}/{item}.
func (h *Handler) routeAppItem(ctx context.Context, c *echo.Context, segs []string) error {
	appID, sub, item := segs[1], segs[2], segs[3]
	switch sub {
	case arnResourceBranches:
		return h.handleBranchName(ctx, c, appID, item)
	case subBackendEnvironments:
		return h.handleBackendEnvironmentName(ctx, c, appID, item)
	case subDomains:
		return h.handleDomainAssociationName(ctx, c, appID, item)
	default:
		return amplifyErrorJSON(c, http.StatusNotFound, "not found")
	}
}

// routeAppBranchSub dispatches /apps/{appId}/branches/{branchName}/{sub}.
func (h *Handler) routeAppBranchSub(ctx context.Context, c *echo.Context, segs []string) error {
	if segs[2] != arnResourceBranches {
		return amplifyErrorJSON(c, http.StatusNotFound, "not found")
	}

	appID, branchName, sub := segs[1], segs[3], segs[4]
	switch sub {
	case subJobs:
		return h.handleBranchJobs(ctx, c, appID, branchName)
	case subDeployments:
		return h.createDeployment(ctx, c, appID, branchName)
	default:
		return amplifyErrorJSON(c, http.StatusNotFound, "not found")
	}
}

// routeAppBranchItem dispatches /apps/{appId}/branches/{branchName}/{sub}/{item}.
func (h *Handler) routeAppBranchItem(ctx context.Context, c *echo.Context, segs []string) error {
	if segs[2] != arnResourceBranches {
		return amplifyErrorJSON(c, http.StatusNotFound, "not found")
	}

	appID, branchName, sub, item := segs[1], segs[3], segs[4], segs[5]
	switch sub {
	case subJobs:
		return h.handleBranchJobID(ctx, c, appID, branchName, item)
	case subDeployments:
		if item != subStart {
			return amplifyErrorJSON(c, http.StatusNotFound, "not found")
		}

		return h.startDeployment(ctx, c, appID, branchName)
	default:
		return amplifyErrorJSON(c, http.StatusNotFound, "not found")
	}
}

// routeJobAction dispatches /apps/{appId}/branches/{branchName}/jobs/{jobId}/{action}.
func (h *Handler) routeJobAction(ctx context.Context, c *echo.Context, segs []string) error {
	if segs[2] != arnResourceBranches || segs[4] != subJobs {
		return amplifyErrorJSON(c, http.StatusNotFound, "not found")
	}

	appID, branchName, jobID, action := segs[1], segs[3], segs[5], segs[6]
	switch action {
	case subStop:
		return h.stopJob(ctx, c, appID, branchName, jobID)
	case subArtifactsRoot:
		return h.listArtifacts(ctx, c, appID, branchName, jobID)
	default:
		return amplifyErrorJSON(c, http.StatusNotFound, "not found")
	}
}

// routeWebhooks dispatches /webhooks/{webhookId}.
func (h *Handler) routeWebhooks(ctx context.Context, c *echo.Context, segs []string) error {
	const webhookIDSegs = 2
	if len(segs) != webhookIDSegs {
		return amplifyErrorJSON(c, http.StatusNotFound, "not found")
	}

	return h.handleWebhookID(ctx, c, segs[1])
}

// routeArtifacts dispatches /artifacts/{artifactId}.
func (h *Handler) routeArtifacts(ctx context.Context, c *echo.Context, segs []string) error {
	const artifactIDSegs = 2
	if len(segs) != artifactIDSegs {
		return amplifyErrorJSON(c, http.StatusNotFound, "not found")
	}

	return h.getArtifactURL(ctx, c, segs[1])
}

// ---- App sub-resource dispatchers ----

// handleAppWebhooks handles POST/GET /apps/{appId}/webhooks.
func (h *Handler) handleAppWebhooks(ctx context.Context, c *echo.Context, appID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.createWebhook(ctx, c, appID)
	case http.MethodGet:
		return h.listWebhooks(ctx, c, appID)
	default:
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleWebhookID handles GET/POST/DELETE /webhooks/{webhookId}.
func (h *Handler) handleWebhookID(ctx context.Context, c *echo.Context, webhookID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getWebhook(ctx, c, webhookID)
	case http.MethodPost:
		return h.updateWebhook(ctx, c, webhookID)
	case http.MethodDelete:
		return h.deleteWebhook(ctx, c, webhookID)
	default:
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleBackendEnvironments handles POST/GET /apps/{appId}/backendenvironments.
func (h *Handler) handleBackendEnvironments(ctx context.Context, c *echo.Context, appID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.createBackendEnvironment(ctx, c, appID)
	case http.MethodGet:
		return h.listBackendEnvironments(ctx, c, appID)
	default:
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleBackendEnvironmentName handles GET/DELETE /apps/{appId}/backendenvironments/{environmentName}.
func (h *Handler) handleBackendEnvironmentName(
	ctx context.Context,
	c *echo.Context,
	appID, environmentName string,
) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getBackendEnvironment(ctx, c, appID, environmentName)
	case http.MethodDelete:
		return h.deleteBackendEnvironment(ctx, c, appID, environmentName)
	default:
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleDomainAssociations handles POST/GET /apps/{appId}/domains.
func (h *Handler) handleDomainAssociations(ctx context.Context, c *echo.Context, appID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.createDomainAssociation(ctx, c, appID)
	case http.MethodGet:
		return h.listDomainAssociations(ctx, c, appID)
	default:
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleDomainAssociationName handles GET/DELETE/POST /apps/{appId}/domains/{domainName}.
func (h *Handler) handleDomainAssociationName(
	ctx context.Context,
	c *echo.Context,
	appID, domainName string,
) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getDomainAssociation(ctx, c, appID, domainName)
	case http.MethodDelete:
		return h.deleteDomainAssociation(ctx, c, appID, domainName)
	case http.MethodPost:
		return h.updateDomainAssociation(ctx, c, appID, domainName)
	default:
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleBranchJobs handles POST/GET /apps/{appId}/branches/{branchName}/jobs.
func (h *Handler) handleBranchJobs(ctx context.Context, c *echo.Context, appID, branchName string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.startJob(ctx, c, appID, branchName)
	case http.MethodGet:
		return h.listJobs(ctx, c, appID, branchName)
	default:
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleBranchJobID handles GET/DELETE /apps/{appId}/branches/{branchName}/jobs/{jobId}.
func (h *Handler) handleBranchJobID(
	ctx context.Context,
	c *echo.Context,
	appID, branchName, jobID string,
) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getJob(ctx, c, appID, branchName, jobID)
	case http.MethodDelete:
		return h.deleteJob(ctx, c, appID, branchName, jobID)
	default:
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// ---- App / Branch update ----

// updateApp handles POST /apps/{appId}.
func (h *Handler) updateApp(ctx context.Context, c *echo.Context, appID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Repository  string `json:"repository"`
		Platform    string `json:"platform"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	app, updateErr := h.Backend.UpdateApp(appID, input.Name, input.Description, input.Repository, input.Platform)
	if updateErr != nil {
		return h.handleBackendError(ctx, c, "UpdateApp", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyApp: toAppView(app)})
}

// updateBranch handles POST /apps/{appId}/branches/{branchName}.
func (h *Handler) updateBranch(ctx context.Context, c *echo.Context, appID, branchName string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input struct {
		Description     string `json:"description"`
		Stage           string `json:"stage"`
		EnableAutoBuild bool   `json:"enableAutoBuild"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	branch, updateErr := h.Backend.UpdateBranch(
		appID, branchName, input.Description, input.Stage, input.EnableAutoBuild,
	)
	if updateErr != nil {
		return h.handleBackendError(ctx, c, "UpdateBranch", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyBranch: toBranchView(branch)})
}

// ---- Webhook operations ----

// createWebhook handles POST /apps/{appId}/webhooks.
func (h *Handler) createWebhook(ctx context.Context, c *echo.Context, appID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input struct {
		BranchName  string `json:"branchName"`
		Description string `json:"description"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	wh, createErr := h.Backend.CreateWebhook(appID, input.BranchName, input.Description)
	if createErr != nil {
		return h.handleBackendError(ctx, c, "CreateWebhook", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyWebhook: toWebhookView(wh)})
}

// listWebhooks handles GET /apps/{appId}/webhooks.
func (h *Handler) listWebhooks(ctx context.Context, c *echo.Context, appID string) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")

	maxResults := 0
	if s := q.Get("maxResults"); s != "" {
		if n, convErr := strconv.Atoi(s); convErr == nil && n > 0 {
			maxResults = n
		}
	}

	webhooks, outToken, err := h.Backend.ListWebhooks(appID, nextToken, maxResults)
	if err != nil {
		return h.handleBackendError(ctx, c, "ListWebhooks", err)
	}

	resp := map[string]any{"webhooks": toWebhookViews(webhooks)}
	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

// getWebhook handles GET /webhooks/{webhookId}.
func (h *Handler) getWebhook(ctx context.Context, c *echo.Context, webhookID string) error {
	wh, err := h.Backend.GetWebhook(webhookID)
	if err != nil {
		return h.handleBackendError(ctx, c, "GetWebhook", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyWebhook: toWebhookView(wh)})
}

// updateWebhook handles POST /webhooks/{webhookId}.
func (h *Handler) updateWebhook(ctx context.Context, c *echo.Context, webhookID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input struct {
		BranchName  string `json:"branchName"`
		Description string `json:"description"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	wh, updateErr := h.Backend.UpdateWebhook(webhookID, input.BranchName, input.Description)
	if updateErr != nil {
		return h.handleBackendError(ctx, c, "UpdateWebhook", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyWebhook: toWebhookView(wh)})
}

// deleteWebhook handles DELETE /webhooks/{webhookId}.
func (h *Handler) deleteWebhook(ctx context.Context, c *echo.Context, webhookID string) error {
	wh, err := h.Backend.DeleteWebhook(webhookID)
	if err != nil {
		return h.handleBackendError(ctx, c, "DeleteWebhook", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyWebhook: toWebhookView(wh)})
}

// ---- Backend environment operations ----

// createBackendEnvironment handles POST /apps/{appId}/backendenvironments.
func (h *Handler) createBackendEnvironment(ctx context.Context, c *echo.Context, appID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input struct {
		EnvironmentName     string `json:"environmentName"`
		StackName           string `json:"stackName"`
		DeploymentArtifacts string `json:"deploymentArtifacts"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	be, createErr := h.Backend.CreateBackendEnvironment(
		appID, input.EnvironmentName, input.StackName, input.DeploymentArtifacts,
	)
	if createErr != nil {
		return h.handleBackendError(ctx, c, "CreateBackendEnvironment", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyBackendEnvironment: toBackendEnvironmentView(be)})
}

// listBackendEnvironments handles GET /apps/{appId}/backendenvironments.
func (h *Handler) listBackendEnvironments(ctx context.Context, c *echo.Context, appID string) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")

	maxResults := 0
	if s := q.Get("maxResults"); s != "" {
		if n, convErr := strconv.Atoi(s); convErr == nil && n > 0 {
			maxResults = n
		}
	}

	envs, outToken, err := h.Backend.ListBackendEnvironments(appID, nextToken, maxResults)
	if err != nil {
		return h.handleBackendError(ctx, c, "ListBackendEnvironments", err)
	}

	resp := map[string]any{"backendEnvironments": toBackendEnvironmentViews(envs)}
	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

// getBackendEnvironment handles GET /apps/{appId}/backendenvironments/{environmentName}.
func (h *Handler) getBackendEnvironment(
	ctx context.Context,
	c *echo.Context,
	appID, environmentName string,
) error {
	be, err := h.Backend.GetBackendEnvironment(appID, environmentName)
	if err != nil {
		return h.handleBackendError(ctx, c, "GetBackendEnvironment", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyBackendEnvironment: toBackendEnvironmentView(be)})
}

// deleteBackendEnvironment handles DELETE /apps/{appId}/backendenvironments/{environmentName}.
func (h *Handler) deleteBackendEnvironment(
	ctx context.Context,
	c *echo.Context,
	appID, environmentName string,
) error {
	be, err := h.Backend.DeleteBackendEnvironment(appID, environmentName)
	if err != nil {
		return h.handleBackendError(ctx, c, "DeleteBackendEnvironment", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyBackendEnvironment: toBackendEnvironmentView(be)})
}

// ---- Domain association operations ----

// createDomainAssociation handles POST /apps/{appId}/domains.
func (h *Handler) createDomainAssociation(ctx context.Context, c *echo.Context, appID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input struct {
		DomainName          string             `json:"domainName"`
		SubDomainSettings   []SubDomainSetting `json:"subDomainSettings"`
		EnableAutoSubDomain bool               `json:"enableAutoSubDomain"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	domain, createErr := h.Backend.CreateDomainAssociation(
		appID, input.DomainName, input.SubDomainSettings, input.EnableAutoSubDomain,
	)
	if createErr != nil {
		return h.handleBackendError(ctx, c, "CreateDomainAssociation", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyDomainAssociation: toDomainAssociationView(domain)})
}

// listDomainAssociations handles GET /apps/{appId}/domains.
func (h *Handler) listDomainAssociations(ctx context.Context, c *echo.Context, appID string) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")

	maxResults := 0
	if s := q.Get("maxResults"); s != "" {
		if n, convErr := strconv.Atoi(s); convErr == nil && n > 0 {
			maxResults = n
		}
	}

	domains, outToken, err := h.Backend.ListDomainAssociations(appID, nextToken, maxResults)
	if err != nil {
		return h.handleBackendError(ctx, c, "ListDomainAssociations", err)
	}

	resp := map[string]any{"domainAssociations": toDomainAssociationViews(domains)}
	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

// getDomainAssociation handles GET /apps/{appId}/domains/{domainName}.
func (h *Handler) getDomainAssociation(
	ctx context.Context,
	c *echo.Context,
	appID, domainName string,
) error {
	domain, err := h.Backend.GetDomainAssociation(appID, domainName)
	if err != nil {
		return h.handleBackendError(ctx, c, "GetDomainAssociation", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyDomainAssociation: toDomainAssociationView(domain)})
}

// deleteDomainAssociation handles DELETE /apps/{appId}/domains/{domainName}.
func (h *Handler) deleteDomainAssociation(
	ctx context.Context,
	c *echo.Context,
	appID, domainName string,
) error {
	domain, err := h.Backend.DeleteDomainAssociation(appID, domainName)
	if err != nil {
		return h.handleBackendError(ctx, c, "DeleteDomainAssociation", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyDomainAssociation: toDomainAssociationView(domain)})
}

// updateDomainAssociation handles POST /apps/{appId}/domains/{domainName}.
func (h *Handler) updateDomainAssociation(
	ctx context.Context,
	c *echo.Context,
	appID, domainName string,
) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input struct {
		SubDomainSettings   []SubDomainSetting `json:"subDomainSettings"`
		EnableAutoSubDomain bool               `json:"enableAutoSubDomain"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	domain, updateErr := h.Backend.UpdateDomainAssociation(
		appID, domainName, input.SubDomainSettings, input.EnableAutoSubDomain,
	)
	if updateErr != nil {
		return h.handleBackendError(ctx, c, "UpdateDomainAssociation", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyDomainAssociation: toDomainAssociationView(domain)})
}

// ---- Job operations ----

// startJob handles POST /apps/{appId}/branches/{branchName}/jobs.
func (h *Handler) startJob(ctx context.Context, c *echo.Context, appID, branchName string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input struct {
		CommitID  string `json:"commitId"`
		CommitMsg string `json:"commitMessage"`
		JobType   string `json:"jobType"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	job, startErr := h.Backend.StartJob(appID, branchName, input.JobType, input.CommitID, input.CommitMsg)
	if startErr != nil {
		return h.handleBackendError(ctx, c, "StartJob", startErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyJobSummary: toJobSummaryView(job)})
}

// listJobs handles GET /apps/{appId}/branches/{branchName}/jobs.
func (h *Handler) listJobs(ctx context.Context, c *echo.Context, appID, branchName string) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")

	maxResults := 0
	if s := q.Get("maxResults"); s != "" {
		if n, convErr := strconv.Atoi(s); convErr == nil && n > 0 {
			maxResults = n
		}
	}

	jobs, outToken, err := h.Backend.ListJobs(appID, branchName, nextToken, maxResults)
	if err != nil {
		return h.handleBackendError(ctx, c, "ListJobs", err)
	}

	resp := map[string]any{"jobSummaries": toJobSummaryViews(jobs)}
	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

// getJob handles GET /apps/{appId}/branches/{branchName}/jobs/{jobId}.
func (h *Handler) getJob(ctx context.Context, c *echo.Context, appID, branchName, jobID string) error {
	job, err := h.Backend.GetJob(appID, branchName, jobID)
	if err != nil {
		return h.handleBackendError(ctx, c, "GetJob", err)
	}

	jobResp := map[string]any{
		"summary": toJobSummaryView(job),
		"steps":   []any{},
	}

	return c.JSON(http.StatusOK, map[string]any{"job": jobResp})
}

// deleteJob handles DELETE /apps/{appId}/branches/{branchName}/jobs/{jobId}.
func (h *Handler) deleteJob(ctx context.Context, c *echo.Context, appID, branchName, jobID string) error {
	job, err := h.Backend.DeleteJob(appID, branchName, jobID)
	if err != nil {
		return h.handleBackendError(ctx, c, "DeleteJob", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyJobSummary: toJobSummaryView(job)})
}

// stopJob handles DELETE /apps/{appId}/branches/{branchName}/jobs/{jobId}/stop.
func (h *Handler) stopJob(ctx context.Context, c *echo.Context, appID, branchName, jobID string) error {
	job, err := h.Backend.StopJob(appID, branchName, jobID)
	if err != nil {
		return h.handleBackendError(ctx, c, "StopJob", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyJobSummary: toJobSummaryView(job)})
}

// ---- Deployment operations ----

// createDeployment handles POST /apps/{appId}/branches/{branchName}/deployments.
func (h *Handler) createDeployment(ctx context.Context, c *echo.Context, appID, branchName string) error {
	if c.Request().Method != http.MethodPost {
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}

	jobID, zipUploadURL, err := h.Backend.CreateDeployment(appID, branchName)
	if err != nil {
		return h.handleBackendError(ctx, c, "CreateDeployment", err)
	}

	return c.JSON(http.StatusCreated, map[string]any{
		"jobId":          jobID,
		"zipUploadUrl":   zipUploadURL,
		"fileUploadUrls": map[string]string{},
	})
}

// startDeployment handles POST /apps/{appId}/branches/{branchName}/deployments/start.
func (h *Handler) startDeployment(ctx context.Context, c *echo.Context, appID, branchName string) error {
	if c.Request().Method != http.MethodPost {
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input struct {
		JobID     string `json:"jobId"`
		SourceURL string `json:"sourceUrl"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	job, startErr := h.Backend.StartDeployment(appID, branchName, input.JobID, input.SourceURL)
	if startErr != nil {
		return h.handleBackendError(ctx, c, "StartDeployment", startErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyJobSummary: toJobSummaryView(job)})
}

// ---- Access logs ----

// generateAccessLogs handles POST /apps/{appId}/accesslogs.
func (h *Handler) generateAccessLogs(ctx context.Context, c *echo.Context, appID string) error {
	if c.Request().Method != http.MethodPost {
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return amplifyErrorJSON(c, http.StatusInternalServerError, err.Error())
	}

	var input struct {
		DomainName string `json:"domainName"`
		StartTime  string `json:"startTime"`
		EndTime    string `json:"endTime"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return amplifyErrorJSON(c, http.StatusBadRequest, "invalid request body")
	}

	logURL, logErr := h.Backend.GenerateAccessLogs(appID, input.DomainName, input.StartTime, input.EndTime)
	if logErr != nil {
		return h.handleBackendError(ctx, c, "GenerateAccessLogs", logErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"logUrl": logURL})
}

// ---- Artifacts ----

// getArtifactURL handles GET /artifacts/{artifactId}.
func (h *Handler) getArtifactURL(ctx context.Context, c *echo.Context, artifactID string) error {
	if c.Request().Method != http.MethodGet {
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}

	id, artifactURL, err := h.Backend.GetArtifactURL(artifactID)
	if err != nil {
		return h.handleBackendError(ctx, c, "GetArtifactUrl", err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"artifactId":  id,
		"artifactUrl": artifactURL,
	})
}

// listArtifacts handles GET /apps/{appId}/branches/{branchName}/jobs/{jobId}/artifacts.
func (h *Handler) listArtifacts(ctx context.Context, c *echo.Context, appID, branchName, jobID string) error {
	if c.Request().Method != http.MethodGet {
		return amplifyErrorJSON(c, http.StatusMethodNotAllowed, "method not allowed")
	}

	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")

	maxResults := 0
	if s := q.Get("maxResults"); s != "" {
		if n, convErr := strconv.Atoi(s); convErr == nil && n > 0 {
			maxResults = n
		}
	}

	artifacts, outToken, err := h.Backend.ListArtifacts(appID, branchName, jobID, nextToken, maxResults)
	if err != nil {
		return h.handleBackendError(ctx, c, "ListArtifacts", err)
	}

	resp := map[string]any{"artifacts": toArtifactViews(artifacts)}
	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}
