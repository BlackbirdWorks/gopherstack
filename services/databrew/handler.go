package databrew

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	databrewPathPrefix = "/databrew/v1/"

	segDatasets       = "datasets"
	segRecipes        = "recipes"
	segRecipeVersions = "recipeVersions"
	segProjects       = "projects"
	segProfileJobs    = "profileJobs"
	segRecipeJobs     = "recipeJobs"
	segJobs           = "jobs"
	segRulesets       = "rulesets"
	segSchedules      = "schedules"
	segTags           = "tags"
	segJobRun         = "jobRun"
	nextTokenKey      = "NextToken"

	opCreateDataset    = "CreateDataset"
	opDescribeDataset  = "DescribeDataset"
	opListDatasets     = "ListDatasets"
	opUpdateDataset    = "UpdateDataset"
	opDeleteDataset    = "DeleteDataset"
	opCreateRecipe     = "CreateRecipe"
	opDescribeRecipe   = "DescribeRecipe"
	opListRecipes      = "ListRecipes"
	opPublishRecipe    = "PublishRecipe"
	opUpdateRecipe     = "UpdateRecipe"
	opDeleteRecipe     = "DeleteRecipe"
	opCreateProject    = "CreateProject"
	opDescribeProject  = "DescribeProject"
	opListProjects     = "ListProjects"
	opUpdateProject    = "UpdateProject"
	opDeleteProject    = "DeleteProject"
	opCreateProfileJob = "CreateProfileJob"
	opCreateRecipeJob  = "CreateRecipeJob"
	opDescribeJob      = "DescribeJob"
	opListJobs         = "ListJobs"
	opUpdateProfileJob = "UpdateProfileJob"
	opUpdateRecipeJob  = "UpdateRecipeJob"
	opDeleteJob        = "DeleteJob"
	opStartJobRun      = "StartJobRun"
	opListJobRuns      = "ListJobRuns"
	opDescribeJobRun   = "DescribeJobRun"
	opStopJobRun       = "StopJobRun"

	opCreateRuleset   = "CreateRuleset"
	opDescribeRuleset = "DescribeRuleset"
	opListRulesets    = "ListRulesets"
	opUpdateRuleset   = "UpdateRuleset"
	opDeleteRuleset   = "DeleteRuleset"

	opCreateSchedule   = "CreateSchedule"
	opDescribeSchedule = "DescribeSchedule"
	opListSchedules    = "ListSchedules"
	opUpdateSchedule   = "UpdateSchedule"
	opDeleteSchedule   = "DeleteSchedule"

	opTagResource         = "TagResource"
	opUntagResource       = "UntagResource"
	opListTagsForResource = "ListTagsForResource"

	opBatchDeleteRecipeVersion = "BatchDeleteRecipeVersion"
	opDeleteRecipeVersion      = "DeleteRecipeVersion"
	opListRecipeVersions       = "ListRecipeVersions"

	opSendProjectSessionAction = "SendProjectSessionAction"
	opStartProjectSession      = "StartProjectSession"

	opUnknown = "Unknown"

	minPathSegments = 2

	keyName = "Name"
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request body")
)

// Handler is the HTTP handler for AWS Glue DataBrew operations.
type Handler struct {
	Backend   StorageBackend
	AccountID string
	Region    string
}

// NewHandler creates a new DataBrew handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend:   backend,
		AccountID: backend.AccountID(),
		Region:    backend.Region(),
	}
}

func (h *Handler) Name() string                        { return "DataBrew" }
func (h *Handler) Reset()                              { h.Backend.Reset() }
func (h *Handler) StartWorker(_ context.Context) error { return nil }

// Shutdown implements service.Shutdowner. It cancels in-flight job run
// transition goroutines and waits for them to drain, bounded by ctx.
func (h *Handler) Shutdown(ctx context.Context) {
	if b, ok := h.Backend.(interface{ Shutdown(context.Context) }); ok {
		b.Shutdown(ctx)
	}
}

var _ service.Shutdowner = (*Handler)(nil)

func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateDataset, opDescribeDataset, opListDatasets, opUpdateDataset, opDeleteDataset,
		opCreateRecipe, opDescribeRecipe, opListRecipes, opPublishRecipe, opUpdateRecipe, opDeleteRecipe,
		opCreateProject, opDescribeProject, opListProjects, opUpdateProject, opDeleteProject,
		opCreateProfileJob, opCreateRecipeJob, opDescribeJob, opListJobs,
		opUpdateProfileJob, opUpdateRecipeJob, opDeleteJob, opStartJobRun, opListJobRuns,
		opDescribeJobRun, opStopJobRun,
		opCreateRuleset, opDescribeRuleset, opListRulesets, opUpdateRuleset, opDeleteRuleset,
		opCreateSchedule, opDescribeSchedule, opListSchedules, opUpdateSchedule, opDeleteSchedule,
		opTagResource, opUntagResource, opListTagsForResource,
		opBatchDeleteRecipeVersion, opDeleteRecipeVersion, opListRecipeVersions,
		opSendProjectSessionAction, opStartProjectSession,
	}
}

func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		if strings.HasPrefix(c.Request().Host, "databrew.") ||
			strings.HasPrefix(path, databrewPathPrefix) ||
			path == "/databrew/v1" {
			return true
		}

		// Match paths sent by the SDK. Unambiguous segments match unconditionally;
		// ambiguous segments (datasets, schedules, jobs, tags, recipeVersions)
		// require a SigV4 service check. tags/{ResourceArn} and recipeVersions
		// are top-level real AWS paths (TagResource/UntagResource/
		// ListTagsForResource and ListRecipeVersions), not just the
		// /databrew/v1/ convenience prefix.
		firstSeg, _, _ := strings.Cut(strings.TrimPrefix(path, "/"), "/")
		switch firstSeg {
		case segRecipes, segProfileJobs, segRecipeJobs, segRulesets, segProjects:
			return true
		case segDatasets, segSchedules, segJobs, segTags, segRecipeVersions:
			return httputils.ExtractServiceFromRequest(c.Request()) == "databrew"
		}

		return false
	}
}

// MatchPriority returns PriorityPathVersioned+1 so DataBrew is evaluated before
// IoT Analytics, which also claims /datasets at PriorityPathVersioned.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned + 1 }

func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := parseDataBrewRESTPath(c.Request().Method, c.Request().URL.Path)

	return op
}

func (h *Handler) ExtractResource(c *echo.Context) string {
	_, name := parseDataBrewRESTPath(c.Request().Method, c.Request().URL.Path)

	return name
}

func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		// Resolve the per-request region (from SigV4 / X-Amz-Region) and attach
		// it to the context so backend operations are region-scoped.
		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())
		ctx = context.WithValue(ctx, regionContextKey{}, region)

		action, name := parseDataBrewRESTPath(c.Request().Method, c.Request().URL.Path)
		if action == opUnknown {
			return c.String(http.StatusNotFound, "not found")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "databrew: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		body, err = enrichDataBrewBody(c, action, name, body)
		if err != nil {
			return h.handleError(c, err)
		}

		// For tags, job runs, and recipe versions, the resource identifier
		// travels as an extra path segment beyond resource/name; pull it into
		// the body so handlers can read it uniformly. This must run
		// unconditionally (not gated by a minimum segment count): every
		// DataBrew ARN embeds a "/" in its resource part (e.g.
		// "job/myjob"), so a bare (non-/databrew/v1/-prefixed) tag path like
		// /tags/arn:aws:databrew:us-east-1:111111111111:job/myjob only has 4
		// "/"-separated segments -- a fixed threshold tuned for the
		// convenience prefix would silently skip ResourceArn extraction for
		// real SDK traffic.
		body = enrichDataBrewSubOpBody(c.Request().URL.Path, body)

		result, dispErr := h.dispatch(ctx, action, body)
		if dispErr != nil {
			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.JSON(http.StatusOK, map[string]any{})
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

// databrewErrorResponse is the restjson1 error envelope. The real SDK's error
// deserializer (aws-sdk-go-v2/aws/protocol/restjson.GetErrorInfo) identifies
// the concrete exception type solely from the X-Amzn-ErrorType header or a
// "code"/"__type" JSON field -- HTTP status is NOT consulted. A body of just
// {"Message": "..."} (no code/__type) is silently downgraded by the SDK to a
// generic smithy.GenericAPIError, so callers doing errors.As(err,
// &types.ResourceNotFoundException{}) never match. __type must carry the
// exception name for typed error handling to work.
type databrewErrorResponse struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	status, code := http.StatusInternalServerError, "InternalFailure"

	switch {
	case errors.Is(err, ErrNotFound):
		status, code = http.StatusNotFound, "ResourceNotFoundException"
	case errors.Is(err, ErrAlreadyExists):
		status, code = http.StatusConflict, "ConflictException"
	case errors.Is(err, ErrValidation):
		status, code = http.StatusBadRequest, "ValidationException"
	case errors.Is(err, errUnknownAction):
		status, code = http.StatusNotFound, "ResourceNotFoundException"
	case errors.Is(err, errInvalidRequest):
		status, code = http.StatusBadRequest, "ValidationException"
	}

	return c.JSON(status, databrewErrorResponse{Type: code, Message: err.Error()})
}

func parseDataBrewRESTPath(method, path string) (string, string) {
	after, ok := strings.CutPrefix(path, databrewPathPrefix)
	if !ok {
		after, _ = strings.CutPrefix(path, "/")
	}

	segments := strings.SplitN(after, "/", minPathSegments+1)
	if len(segments) == 0 || segments[0] == "" {
		return opUnknown, ""
	}

	resource := segments[0]
	name := ""
	if len(segments) >= minPathSegments {
		name = segments[1]
	}

	var subOp string
	if len(segments) >= 3 { //nolint:mnd // 3 segments: resource/name/subOp
		subOp = segments[2]
	}

	return mapResourceOp(resource, method, name, subOp)
}

func mapResourceOp(resource, method, name, subOp string) (string, string) {
	switch resource {
	case segDatasets:

		return parseDatasetOp(method, name), name
	case segRecipes:

		return parseRecipeOp(method, name, subOp), name
	case segProjects:

		return parseProjectOp(method, name, subOp), name
	case segProfileJobs:

		return parseProfileJobOp(method, name), name
	case segRecipeJobs:

		return parseRecipeJobOp(method, name), name
	case segJobs:

		return parseJobOp(method, name, subOp), name
	case segRulesets:

		return parseRulesetOp(method, name), name
	case segSchedules:

		return parseScheduleOp(method, name), name
	case segTags:

		return parseTagsOp(method, name), name
	case segRecipeVersions:
		// The real AWS ListRecipeVersions op is GET /recipeVersions?name=...
		// (RecipeName travels as a query param, not a path segment; see
		// enrichDataBrewBody). BatchDeleteRecipeVersion is a recipe sub-op at
		// POST /recipes/{Name}/batchDeleteRecipeVersion (see
		// parseRecipeSubOp) -- there is no real AWS op at POST /recipeVersions.
		if method == http.MethodGet {
			return opListRecipeVersions, name
		}

		return opUnknown, ""
	}

	return opUnknown, ""
}

func parseDatasetOp(method, name string) string {
	switch method {
	case http.MethodPost:
		if name == "" {
			return opCreateDataset
		}
	case http.MethodGet:
		if name == "" {
			return opListDatasets
		}

		return opDescribeDataset
	case http.MethodPut:
		if name != "" {
			return opUpdateDataset
		}
	case http.MethodDelete:
		if name != "" {
			return opDeleteDataset
		}
	}

	return opUnknown
}

func parseRecipeSubOp(method, subOp string) string {
	switch {
	case subOp == "publishRecipe":
		return opPublishRecipe
	// Convenience aliases: /recipes/{Name}/recipeVersions is not a real AWS
	// path (the real ListRecipeVersions op is GET /recipeVersions?name=...,
	// handled by mapResourceOp's segRecipeVersions case, and the real
	// BatchDeleteRecipeVersion op is POST
	// /recipes/{Name}/batchDeleteRecipeVersion below), but both GET and POST
	// forms are kept here so callers using the /databrew/v1/ nested
	// convenience path keep working.
	case subOp == segRecipeVersions && method == http.MethodGet:
		return opListRecipeVersions
	case subOp == segRecipeVersions && method == http.MethodPost:
		return opBatchDeleteRecipeVersion
	// Real AWS path: POST /recipes/{Name}/batchDeleteRecipeVersion.
	case subOp == "batchDeleteRecipeVersion" && method == http.MethodPost:
		return opBatchDeleteRecipeVersion
	case strings.HasPrefix(subOp, "recipeVersion/") && method == http.MethodDelete:
		return opDeleteRecipeVersion
	}

	return ""
}

func parseRecipeOp(method, name, subOp string) string {
	if op := parseRecipeSubOp(method, subOp); op != "" {
		return op
	}
	switch method {
	case http.MethodPost:
		if name == "" {
			return opCreateRecipe
		}
	case http.MethodGet:
		if name == "" {
			return opListRecipes
		}

		return opDescribeRecipe
	case http.MethodPut:
		if name != "" {
			return opUpdateRecipe
		}
	case http.MethodDelete:
		if name != "" {
			return opDeleteRecipe
		}
	}

	return opUnknown
}

func parseProjectOp(method, name, subOp string) string {
	if subOp == "sendProjectSessionAction" && method == http.MethodPut {
		return opSendProjectSessionAction
	}
	if subOp == "startProjectSession" && method == http.MethodPut {
		return opStartProjectSession
	}
	switch method {
	case http.MethodPost:
		if name == "" {
			return opCreateProject
		}
	case http.MethodGet:
		if name == "" {
			return opListProjects
		}

		return opDescribeProject
	case http.MethodPut:
		if name != "" {
			return opUpdateProject
		}
	case http.MethodDelete:
		if name != "" {
			return opDeleteProject
		}
	}

	return opUnknown
}

func parseProfileJobOp(method, name string) string {
	switch method {
	case http.MethodPost:
		if name == "" {
			return opCreateProfileJob
		}
	case http.MethodPut:
		if name != "" {
			return opUpdateProfileJob
		}
	}

	return opUnknown
}

func parseRecipeJobOp(method, name string) string {
	switch method {
	case http.MethodPost:
		if name == "" {
			return opCreateRecipeJob
		}
	case http.MethodPut:
		if name != "" {
			return opUpdateRecipeJob
		}
	}

	return opUnknown
}

func parseJobOp(method, name, subOp string) string {
	switch {
	case subOp == "startJobRun" && method == http.MethodPost:

		return opStartJobRun
	case subOp == "jobRuns" && method == http.MethodGet:

		return opListJobRuns
	case strings.HasPrefix(subOp, "jobRun/") && method == http.MethodGet:
		return opDescribeJobRun
	case strings.HasPrefix(subOp, "jobRun/") && method == http.MethodPost:
		return opStopJobRun
	case method == http.MethodGet && name == "":

		return opListJobs
	case method == http.MethodGet && name != "":

		return opDescribeJob
	case method == http.MethodDelete && name != "":

		return opDeleteJob
	}

	return opUnknown
}

func enrichDataBrewBody(c *echo.Context, _, name string, body []byte) ([]byte, error) {
	m := make(map[string]json.RawMessage)
	if len(body) > 0 {
		if err := json.Unmarshal(body, &m); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	if name != "" {
		if existingName, ok := m["Name"]; ok {
			var parsedName string
			if err := json.Unmarshal(existingName, &parsedName); err == nil && parsedName != name {
				return nil, fmt.Errorf("%w: name in body does not match path", errInvalidRequest)
			}
		}
		nameJSON, _ := json.Marshal(name)
		m["Name"] = nameJSON
	}

	if maxResults := c.QueryParam("maxResults"); maxResults != "" {
		m["MaxResults"], _ = json.Marshal(maxResults)
	}
	if nextToken := c.QueryParam("nextToken"); nextToken != "" {
		m[nextTokenKey], _ = json.Marshal(nextToken)
	}
	if v := c.QueryParam("datasetName"); v != "" {
		m["DatasetName"], _ = json.Marshal(v)
	}
	if v := c.QueryParam("projectName"); v != "" {
		m["ProjectName"], _ = json.Marshal(v)
	}
	if v := c.QueryParam("targetArn"); v != "" {
		m["TargetArn"], _ = json.Marshal(v)
	}
	// UntagResource is DELETE /tags/{ResourceArn}?tagKeys=a&tagKeys=b -- TagKeys
	// travels as a repeated query param, never in the (typically absent) DELETE
	// body. Confirmed against aws-sdk-go-v2's serializer
	// (awsRestjson1_serializeOpHttpBindingsUntagResourceInput calls
	// encoder.AddQuery("tagKeys") per key). Without this, UntagResource always
	// silently no-ops.
	if tagKeys := c.QueryParams()["tagKeys"]; len(tagKeys) > 0 {
		m["TagKeys"], _ = json.Marshal(tagKeys)
	}
	// ListRecipeVersions is GET /recipeVersions?name=... (top-level, no path
	// segment for the recipe name); only set when name isn't already known
	// from a path segment (e.g. the /recipes/{Name}/recipeVersions convenience
	// alias already populated "Name" above).
	if _, ok := m["Name"]; !ok {
		if v := c.QueryParam("name"); v != "" {
			m["Name"], _ = json.Marshal(v)
		}
	}

	result, _ := json.Marshal(m)

	return result, nil
}

func enrichDataBrewSubOpBody(path string, body []byte) []byte {
	segments := strings.Split(path, "/")
	m := make(map[string]json.RawMessage)
	if len(body) > 0 {
		_ = json.Unmarshal(body, &m)
	}

	for i, seg := range segments {
		switch seg {
		case "jobRun":
			// e.g. /jobs/{Name}/jobRun/{RunId} — with or without /databrew/v1/ prefix
			if i+1 < len(segments) {
				runIDJSON, _ := json.Marshal(segments[i+1])
				m["RunId"] = runIDJSON
			}
		case "recipeVersion":
			// e.g. /recipes/{Name}/recipeVersion/{RecipeVersion}
			if i+1 < len(segments) {
				versionJSON, _ := json.Marshal(segments[i+1])
				m["RecipeVersion"] = versionJSON
			}
		case "tags":
			// e.g. /databrew/v1/tags/{resourceArn} or /tags/{resourceArn}
			if i+1 < len(segments) {
				arn := strings.Join(segments[i+1:], "/")
				arnJSON, _ := json.Marshal(arn)
				m["ResourceArn"] = arnJSON
			}
		}
	}

	result, _ := json.Marshal(m)

	return result
}

func parseRulesetOp(method, name string) string {
	switch method {
	case http.MethodPost:
		if name == "" {
			return opCreateRuleset
		}
	case http.MethodGet:
		if name == "" {
			return opListRulesets
		}

		return opDescribeRuleset
	case http.MethodPut:

		return opUpdateRuleset
	case http.MethodDelete:

		return opDeleteRuleset
	}

	return opUnknown
}

func parseScheduleOp(method, name string) string {
	switch method {
	case http.MethodPost:
		if name == "" {
			return opCreateSchedule
		}
	case http.MethodGet:
		if name == "" {
			return opListSchedules
		}

		return opDescribeSchedule
	case http.MethodPut:

		return opUpdateSchedule
	case http.MethodDelete:

		return opDeleteSchedule
	}

	return opUnknown
}

func parseTagsOp(method, _ string) string {
	switch method {
	case http.MethodPost:

		return opTagResource
	case http.MethodDelete:

		return opUntagResource
	case http.MethodGet:

		return opListTagsForResource
	}

	return opUnknown
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	if result, ok, err := h.dispatchDataset(ctx, action, body); ok {
		return result, err
	}

	if result, ok, err := h.dispatchRecipe(ctx, action, body); ok {
		return result, err
	}

	if result, ok, err := h.dispatchProject(ctx, action, body); ok {
		return result, err
	}

	if result, ok, err := h.dispatchJob(ctx, action, body); ok {
		return result, err
	}

	if result, ok, err := h.dispatchRuleset(ctx, action, body); ok {
		return result, err
	}

	if result, ok, err := h.dispatchSchedule(ctx, action, body); ok {
		return result, err
	}

	if result, ok, err := h.dispatchTags(ctx, action, body); ok {
		return result, err
	}

	return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
}

func (h *Handler) dispatchDataset(
	ctx context.Context,
	action string,
	body []byte,
) ([]byte, bool, error) {
	switch action {
	case opCreateDataset:
		r, e := h.handleCreateDataset(ctx, body)

		return r, true, e
	case opDescribeDataset:
		r, e := h.handleDescribeDataset(ctx, body)

		return r, true, e
	case opListDatasets:
		r, e := h.handleListDatasets(ctx, body)

		return r, true, e
	case opUpdateDataset:
		r, e := h.handleUpdateDataset(ctx, body)

		return r, true, e
	case opDeleteDataset:
		r, e := h.handleDeleteDataset(ctx, body)

		return r, true, e
	}

	return nil, false, nil
}

func (h *Handler) dispatchRecipe(
	ctx context.Context,
	action string,
	body []byte,
) ([]byte, bool, error) {
	switch action {
	case opCreateRecipe:
		r, e := h.handleCreateRecipe(ctx, body)

		return r, true, e
	case opDescribeRecipe:
		r, e := h.handleDescribeRecipe(ctx, body)

		return r, true, e
	case opListRecipes:
		r, e := h.handleListRecipes(ctx, body)

		return r, true, e
	case opPublishRecipe:
		r, e := h.handlePublishRecipe(ctx, body)

		return r, true, e
	case opUpdateRecipe:
		r, e := h.handleUpdateRecipe(ctx, body)

		return r, true, e
	case opDeleteRecipe:
		r, e := h.handleDeleteRecipe(ctx, body)

		return r, true, e
	case opBatchDeleteRecipeVersion:
		r, e := h.handleBatchDeleteRecipeVersion(ctx, body)

		return r, true, e
	case opDeleteRecipeVersion:
		r, e := h.handleDeleteRecipeVersion(ctx, body)

		return r, true, e
	case opListRecipeVersions:
		r, e := h.handleListRecipeVersions(ctx, body)

		return r, true, e
	}

	return nil, false, nil
}

func (h *Handler) dispatchProject(
	ctx context.Context,
	action string,
	body []byte,
) ([]byte, bool, error) {
	switch action {
	case opCreateProject:
		r, e := h.handleCreateProject(ctx, body)

		return r, true, e
	case opDescribeProject:
		r, e := h.handleDescribeProject(ctx, body)

		return r, true, e
	case opListProjects:
		r, e := h.handleListProjects(ctx, body)

		return r, true, e
	case opUpdateProject:
		r, e := h.handleUpdateProject(ctx, body)

		return r, true, e
	case opDeleteProject:
		r, e := h.handleDeleteProject(ctx, body)

		return r, true, e
	case opStartProjectSession:
		r, e := h.handleStartProjectSession(ctx, body)

		return r, true, e
	case opSendProjectSessionAction:
		r, e := h.handleSendProjectSessionAction(ctx, body)

		return r, true, e
	}

	return nil, false, nil
}

func (h *Handler) dispatchJob(
	ctx context.Context,
	action string,
	body []byte,
) ([]byte, bool, error) {
	switch action {
	case opCreateProfileJob:
		r, e := h.handleCreateProfileJob(ctx, body)

		return r, true, e
	case opCreateRecipeJob:
		r, e := h.handleCreateRecipeJob(ctx, body)

		return r, true, e
	case opDescribeJob:
		r, e := h.handleDescribeJob(ctx, body)

		return r, true, e
	case opListJobs:
		r, e := h.handleListJobs(ctx, body)

		return r, true, e
	case opUpdateProfileJob:
		r, e := h.handleUpdateProfileJob(ctx, body)

		return r, true, e
	case opUpdateRecipeJob:
		r, e := h.handleUpdateRecipeJob(ctx, body)

		return r, true, e
	case opDeleteJob:
		r, e := h.handleDeleteJob(ctx, body)

		return r, true, e
	case opStartJobRun:
		r, e := h.handleStartJobRun(ctx, body)

		return r, true, e
	case opListJobRuns:
		r, e := h.handleListJobRuns(ctx, body)

		return r, true, e
	case opDescribeJobRun:
		r, e := h.handleDescribeJobRun(ctx, body)

		return r, true, e
	case opStopJobRun:
		r, e := h.handleStopJobRun(ctx, body)

		return r, true, e
	}

	return nil, false, nil
}

func (h *Handler) dispatchRuleset(ctx context.Context, action string, body []byte) ([]byte, bool, error) {
	switch action {
	case opCreateRuleset:
		r, e := h.handleCreateRuleset(ctx, body)

		return r, true, e
	case opDescribeRuleset:
		r, e := h.handleDescribeRuleset(ctx, body)

		return r, true, e
	case opListRulesets:
		r, e := h.handleListRulesets(ctx, body)

		return r, true, e
	case opUpdateRuleset:
		r, e := h.handleUpdateRuleset(ctx, body)

		return r, true, e
	case opDeleteRuleset:
		r, e := h.handleDeleteRuleset(ctx, body)

		return r, true, e
	}

	return nil, false, nil
}

func (h *Handler) dispatchSchedule(ctx context.Context, action string, body []byte) ([]byte, bool, error) {
	switch action {
	case opCreateSchedule:
		r, e := h.handleCreateSchedule(ctx, body)

		return r, true, e
	case opDescribeSchedule:
		r, e := h.handleDescribeSchedule(ctx, body)

		return r, true, e
	case opListSchedules:
		r, e := h.handleListSchedules(ctx, body)

		return r, true, e
	case opUpdateSchedule:
		r, e := h.handleUpdateSchedule(ctx, body)

		return r, true, e
	case opDeleteSchedule:
		r, e := h.handleDeleteSchedule(ctx, body)

		return r, true, e
	}

	return nil, false, nil
}

func (h *Handler) dispatchTags(ctx context.Context, action string, body []byte) ([]byte, bool, error) {
	switch action {
	case opListTagsForResource:
		r, e := h.handleListTagsForResource(ctx, body)

		return r, true, e
	case opTagResource:
		r, e := h.handleTagResource(ctx, body)

		return r, true, e
	case opUntagResource:
		r, e := h.handleUntagResource(ctx, body)

		return r, true, e
	}

	return nil, false, nil
}

func (h *Handler) handleCreateDataset(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		FormatOptions DatasetFormatOptions `json:"FormatOptions"`
		Input         DatasetInput         `json:"Input"`
		Tags          map[string]string    `json:"Tags"`
		Name          string               `json:"Name"`
		Format        string               `json:"Format"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	ds, err := h.Backend.CreateDataset(ctx, req.Name, req.Format, req.Input, req.FormatOptions, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: ds.Name})
}

func (h *Handler) handleDescribeDataset(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	ds, err := h.Backend.DescribeDataset(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(ds)
}

func (h *Handler) handleListDatasets(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		MaxResults string `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}
	_ = json.Unmarshal(body, &req)
	maxResults, _ := strconv.Atoi(req.MaxResults)

	datasets, next := h.Backend.ListDatasets(ctx, maxResults, req.NextToken)

	return json.Marshal(map[string]any{"Datasets": datasets, nextTokenKey: next})
}

func (h *Handler) handleUpdateDataset(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		FormatOptions DatasetFormatOptions `json:"FormatOptions"`
		Input         DatasetInput         `json:"Input"`
		Name          string               `json:"Name"`
		Format        string               `json:"Format"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.UpdateDataset(ctx, req.Name, req.Format, req.Input, req.FormatOptions); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleDeleteDataset(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.DeleteDataset(ctx, req.Name); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleCreateRecipe(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags        map[string]string `json:"Tags"`
		Name        string            `json:"Name"`
		Description string            `json:"Description"`
		Steps       []RecipeStep      `json:"Steps"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	r, err := h.Backend.CreateRecipe(ctx, req.Name, req.Description, req.Steps, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: r.Name})
}

func (h *Handler) handleDescribeRecipe(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	r, err := h.Backend.DescribeRecipe(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(r)
}

func (h *Handler) handleListRecipes(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		MaxResults string `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}
	_ = json.Unmarshal(body, &req)
	maxResults, _ := strconv.Atoi(req.MaxResults)

	recipes, next := h.Backend.ListRecipes(ctx, maxResults, req.NextToken)

	return json.Marshal(map[string]any{"Recipes": recipes, nextTokenKey: next})
}

func (h *Handler) handlePublishRecipe(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name        string `json:"Name"`
		Description string `json:"Description"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.PublishRecipe(ctx, req.Name, req.Description); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleUpdateRecipe(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name        string       `json:"Name"`
		Description string       `json:"Description"`
		Steps       []RecipeStep `json:"Steps"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.UpdateRecipe(ctx, req.Name, req.Description, req.Steps); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleDeleteRecipe(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.DeleteRecipe(ctx, req.Name); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleCreateProject(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags        map[string]string `json:"Tags"`
		Name        string            `json:"Name"`
		DatasetName string            `json:"DatasetName"`
		RecipeName  string            `json:"RecipeName"`
		RoleArn     string            `json:"RoleArn"`
		Sample      Sample            `json:"Sample"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	p, err := h.Backend.CreateProject(
		ctx,
		req.Name,
		req.DatasetName,
		req.RecipeName,
		req.RoleArn,
		req.Sample,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: p.Name})
}

func (h *Handler) handleDescribeProject(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	p, err := h.Backend.DescribeProject(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(p)
}

func (h *Handler) handleListProjects(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		MaxResults string `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}
	_ = json.Unmarshal(body, &req)
	maxResults, _ := strconv.Atoi(req.MaxResults)

	projects, next := h.Backend.ListProjects(ctx, maxResults, req.NextToken)

	return json.Marshal(map[string]any{"Projects": projects, nextTokenKey: next})
}

func (h *Handler) handleUpdateProject(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name        string `json:"Name"`
		DatasetName string `json:"DatasetName"`
		RoleArn     string `json:"RoleArn"`
		Sample      Sample `json:"Sample"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.UpdateProject(ctx, req.Name, req.DatasetName, req.RoleArn, req.Sample); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleDeleteProject(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.DeleteProject(ctx, req.Name); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleCreateProfileJob(ctx context.Context, body []byte) ([]byte, error) {
	// CreateProfileJobInput's output field is a single "OutputLocation"
	// (S3Location), NOT the "Outputs" list CreateRecipeJobInput uses --
	// confirmed against aws-sdk-go-v2/service/databrew's serializer
	// (awsRestjson1_serializeOpDocumentCreateProfileJobInput writes the
	// "OutputLocation" JSON key). The Job entity itself still exposes this as
	// an Outputs list (see backend.Job.Outputs / DescribeJob), so it's
	// converted to a one-element Output slice for storage.
	var req struct {
		Tags           map[string]string `json:"Tags"`
		OutputLocation *S3Location       `json:"OutputLocation"`
		Name           string            `json:"Name"`
		DatasetName    string            `json:"DatasetName"`
		RoleArn        string            `json:"RoleArn"`
		MaxCapacity    int               `json:"MaxCapacity"`
		MaxRetries     int               `json:"MaxRetries"`
		Timeout        int               `json:"Timeout"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	j, err := h.Backend.CreateJob(
		ctx,
		req.Name,
		"PROFILE",
		req.DatasetName,
		"",
		"",
		req.RoleArn,
		outputLocationToOutputs(req.OutputLocation),
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: j.Name})
}

// outputLocationToOutputs converts a CreateProfileJobInput/UpdateProfileJobInput
// "OutputLocation" (a single S3Location) into the one-element Outputs slice
// the Job entity stores/returns, or nil when loc is unset.
func outputLocationToOutputs(loc *S3Location) []Output {
	if loc == nil {
		return nil
	}

	return []Output{{Location: *loc}}
}

func (h *Handler) handleCreateRecipeJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags            map[string]string `json:"Tags"`
		RecipeReference *RecipeRef        `json:"RecipeReference"`
		Name            string            `json:"Name"`
		DatasetName     string            `json:"DatasetName"`
		ProjectName     string            `json:"ProjectName"`
		RoleArn         string            `json:"RoleArn"`
		Outputs         []Output          `json:"Outputs"`
		MaxCapacity     int               `json:"MaxCapacity"`
		MaxRetries      int               `json:"MaxRetries"`
		Timeout         int               `json:"Timeout"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	recipeName := ""
	if req.RecipeReference != nil {
		recipeName = req.RecipeReference.Name
	}
	j, err := h.Backend.CreateJob(
		ctx,
		req.Name,
		"RECIPE",
		req.DatasetName,
		req.ProjectName,
		recipeName,
		req.RoleArn,
		req.Outputs,
		req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: j.Name})
}

func (h *Handler) handleDescribeJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	j, err := h.Backend.DescribeJob(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(j)
}

func (h *Handler) handleListJobs(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		MaxResults  string `json:"MaxResults"`
		NextToken   string `json:"NextToken"`
		DatasetName string `json:"DatasetName"`
		ProjectName string `json:"ProjectName"`
	}
	_ = json.Unmarshal(body, &req)
	maxResults, _ := strconv.Atoi(req.MaxResults)

	jobs, next := h.Backend.ListJobs(ctx, maxResults, req.NextToken, req.DatasetName, req.ProjectName)

	return json.Marshal(map[string]any{"Jobs": jobs, nextTokenKey: next})
}

// handleUpdateProfileJob handles UpdateProfileJob, whose wire field for the
// job's output destination is "OutputLocation" (a single S3Location), not
// "Outputs" -- see the outputLocationToOutputs doc comment.
func (h *Handler) handleUpdateProfileJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		OutputLocation *S3Location `json:"OutputLocation"`
		Name           string      `json:"Name"`
		RoleArn        string      `json:"RoleArn"`
		MaxCapacity    int         `json:"MaxCapacity"`
		MaxRetries     int         `json:"MaxRetries"`
		Timeout        int         `json:"Timeout"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.UpdateJob(
		ctx, req.Name, req.RoleArn, outputLocationToOutputs(req.OutputLocation),
		req.MaxCapacity, req.MaxRetries, req.Timeout,
	); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

// handleUpdateRecipeJob handles UpdateRecipeJob, whose wire field for the
// job's output destinations is "Outputs" (a list), matching CreateRecipeJob.
func (h *Handler) handleUpdateRecipeJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name        string   `json:"Name"`
		RoleArn     string   `json:"RoleArn"`
		Outputs     []Output `json:"Outputs"`
		MaxCapacity int      `json:"MaxCapacity"`
		MaxRetries  int      `json:"MaxRetries"`
		Timeout     int      `json:"Timeout"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.UpdateJob(
		ctx, req.Name, req.RoleArn, req.Outputs, req.MaxCapacity, req.MaxRetries, req.Timeout,
	); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleDeleteJob(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.DeleteJob(ctx, req.Name); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleStartJobRun(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	run, err := h.Backend.StartJobRun(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{"RunId": run.RunID})
}

func (h *Handler) handleListJobRuns(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name       string `json:"Name"`
		MaxResults string `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	maxResults, _ := strconv.Atoi(req.MaxResults)

	runs, next, err := h.Backend.ListJobRuns(ctx, req.Name, maxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"JobRuns": runs, nextTokenKey: next})
}

func (h *Handler) handleDescribeJobRun(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name  string `json:"Name"`
		RunID string `json:"RunId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	run, err := h.Backend.DescribeJobRun(ctx, req.Name, req.RunID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(run)
}

func (h *Handler) handleStopJobRun(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name  string `json:"Name"`
		RunID string `json:"RunId"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	run, err := h.Backend.StopJobRun(ctx, req.Name, req.RunID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{"RunId": run.RunID})
}

func (h *Handler) handleCreateRuleset(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags        map[string]string `json:"Tags"`
		Name        string            `json:"Name"`
		Description string            `json:"Description"`
		TargetArn   string            `json:"TargetArn"`
		Rules       []Rule            `json:"Rules"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	rs, err := h.Backend.CreateRuleset(ctx, req.Name, req.Description, req.TargetArn, req.Rules, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: rs.Name})
}

func (h *Handler) handleDescribeRuleset(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	rs, err := h.Backend.DescribeRuleset(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(rs)
}

func (h *Handler) handleListRulesets(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		MaxResults string `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
		TargetArn  string `json:"TargetArn"`
	}
	_ = json.Unmarshal(body, &req)
	maxResults, _ := strconv.Atoi(req.MaxResults)

	rulesets, next := h.Backend.ListRulesets(ctx, maxResults, req.NextToken, req.TargetArn)

	return json.Marshal(map[string]any{"Rulesets": rulesets, nextTokenKey: next})
}

func (h *Handler) handleUpdateRuleset(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name        string `json:"Name"`
		Description string `json:"Description"`
		Rules       []Rule `json:"Rules"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.UpdateRuleset(ctx, req.Name, req.Description, req.Rules); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleDeleteRuleset(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.DeleteRuleset(ctx, req.Name); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleCreateSchedule(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags           map[string]string `json:"Tags"`
		Name           string            `json:"Name"`
		CronExpression string            `json:"CronExpression"`
		JobNames       []string          `json:"JobNames"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	sc, err := h.Backend.CreateSchedule(ctx, req.Name, req.JobNames, req.CronExpression, req.Tags)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: sc.Name})
}

func (h *Handler) handleDescribeSchedule(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	sc, err := h.Backend.DescribeSchedule(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(sc)
}

func (h *Handler) handleListSchedules(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		MaxResults string `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}
	_ = json.Unmarshal(body, &req)
	maxResults, _ := strconv.Atoi(req.MaxResults)

	schedules, next := h.Backend.ListSchedules(ctx, maxResults, req.NextToken)

	return json.Marshal(map[string]any{"Schedules": schedules, nextTokenKey: next})
}

func (h *Handler) handleUpdateSchedule(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name           string   `json:"Name"`
		CronExpression string   `json:"CronExpression"`
		JobNames       []string `json:"JobNames"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.UpdateSchedule(ctx, req.Name, req.JobNames, req.CronExpression); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleDeleteSchedule(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.DeleteSchedule(ctx, req.Name); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleTagResource(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags        map[string]string `json:"Tags"`
		ResourceArn string            `json:"ResourceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.UpdateTagsByArn(ctx, req.ResourceArn, req.Tags, nil); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleUntagResource(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ResourceArn string   `json:"ResourceArn"`
		TagKeys     []string `json:"TagKeys"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.UpdateTagsByArn(ctx, req.ResourceArn, nil, req.TagKeys); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleListTagsForResource(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ResourceArn string `json:"ResourceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	tags, err := h.Backend.FindTagsByArn(ctx, req.ResourceArn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Tags": tags})
}

func (h *Handler) handleBatchDeleteRecipeVersion(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name           string   `json:"Name"`
		RecipeVersions []string `json:"RecipeVersions"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if _, err := h.Backend.DescribeRecipe(ctx, req.Name); err != nil {
		return nil, err
	}
	// We only emulate a single version "1.0", so any batch deletion for emulation
	// will either do nothing or delete the recipe itself if it was the only version.
	// For simplicity, return success with no per-version errors.

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleDeleteRecipeVersion(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name          string `json:"Name"`
		RecipeVersion string `json:"RecipeVersion"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if _, err := h.Backend.DescribeRecipe(ctx, req.Name); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyName: req.Name, "RecipeVersion": req.RecipeVersion})
}

func (h *Handler) handleListRecipeVersions(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name string `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	r, err := h.Backend.DescribeRecipe(ctx, req.Name)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Recipes": []Recipe{*r}})
}

func (h *Handler) handleStartProjectSession(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Name          string `json:"Name"`
		AssumeControl bool   `json:"AssumeControl"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}

func (h *Handler) handleSendProjectSessionAction(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Action map[string]any `json:"Action"`
		Name   string         `json:"Name"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	return json.Marshal(map[string]string{keyName: req.Name})
}
