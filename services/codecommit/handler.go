package codecommit

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyRepositoryID     = "repositoryId"
	keyRepositoryName   = "repositoryName"
	keyCreationDate     = "creationDate"
	keyErrors           = "errors"
	keyMessage          = "message"
	keyCommitID         = "commitId"
	keyTreeID           = "treeId"
	keyLastModifiedDate = "lastModifiedDate"
	keyApprovalRuleTmpl = "approvalRuleTemplate"
	keyPullRequest      = "pullRequest"
	keyComment          = "comment"
	keySourceCommitID   = "sourceCommitId"
	keyDestCommitID     = "destinationCommitId"
	keyBlobID           = "blobId"
	keyFilePath         = "filePath"
	keyFileMode         = "fileMode"
	prStatusMerged      = "MERGED"
	fileModeNormal      = "NORMAL"
)

const codecommitTargetPrefix = "CodeCommit_20150413."

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// paginateStrings slices a sorted string slice using the nextToken cursor and maxResults limit.
// The nextToken is an opaque decimal index into the slice.
// Returns the page and the next token (empty string if no more pages).
func paginateStrings(items []string, nextToken string, maxResults int) ([]string, string) {
	start := 0
	if nextToken != "" {
		if idx, err := strconv.Atoi(nextToken); err == nil && idx >= 0 {
			start = idx
		}
	}
	if start > len(items) {
		start = len(items)
	}
	end := len(items)
	if maxResults > 0 && start+maxResults < end {
		end = start + maxResults
	}
	page := items[start:end]
	token := ""
	if end < len(items) {
		token = strconv.Itoa(end)
	}
	return page, token
}

// Handler is the Echo HTTP handler for AWS CodeCommit operations.
type Handler struct {
	Backend *InMemoryBackend
	ops     map[string]func([]byte) (any, error)
}

// NewHandler creates a new CodeCommit handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears all handler and backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// buildOps returns the dispatch table mapping action name to handler function.
func (h *Handler) buildOps() map[string]func([]byte) (any, error) {
	batchDisassoc := h.handleBatchDisassociateApprovalRuleTemplateFromRepositories

	return map[string]func([]byte) (any, error){
		"AssociateApprovalRuleTemplateWithRepository":           h.handleAssociateApprovalRuleTemplateWithRepository,
		"BatchAssociateApprovalRuleTemplateWithRepositories":    h.handleBatchAssociateApprovalRuleTemplateWithRepositories,
		"BatchDescribeMergeConflicts":                           h.handleBatchDescribeMergeConflicts,
		"BatchDisassociateApprovalRuleTemplateFromRepositories": batchDisassoc,
		"BatchGetCommits":            h.handleBatchGetCommits,
		"BatchGetRepositories":       h.handleBatchGetRepositories,
		"CreateApprovalRuleTemplate": h.handleCreateApprovalRuleTemplate,
		"CreateBranch":               h.handleCreateBranch,
		"CreateCommit":               h.handleCreateCommit,
		"CreatePullRequest":          h.handleCreatePullRequest,
		"CreateRepository":           h.handleCreateRepository,
		"GetRepository":              h.handleGetRepository,
		"DeleteRepository":           h.handleDeleteRepository,
		"ListRepositories":           h.handleListRepositories,
		"TagResource":                h.handleTagResource,
		"UntagResource":              h.handleUntagResource,
		"ListTagsForResource":        h.handleListTagsForResource,
		// Implemented ops
		"CreatePullRequestApprovalRule":                    h.handleCreatePullRequestApprovalRule,
		"CreateUnreferencedMergeCommit":                    h.handleCreateUnreferencedMergeCommit,
		"DeleteApprovalRuleTemplate":                       h.handleDeleteApprovalRuleTemplate,
		"DeleteBranch":                                     h.handleDeleteBranch,
		"DeleteCommentContent":                             h.handleDeleteCommentContent,
		"DeleteFile":                                       h.handleDeleteFile,
		"DeletePullRequestApprovalRule":                    h.handleDeletePullRequestApprovalRule,
		"DescribeMergeConflicts":                           h.handleDescribeMergeConflicts,
		"DescribePullRequestEvents":                        h.handleDescribePullRequestEvents,
		"DisassociateApprovalRuleTemplateFromRepository":   h.handleDisassociateApprovalRuleTemplateFromRepository,
		"EvaluatePullRequestApprovalRules":                 h.handleEvaluatePullRequestApprovalRules,
		"GetApprovalRuleTemplate":                          h.handleGetApprovalRuleTemplate,
		"GetBlob":                                          h.handleGetBlob,
		"GetBranch":                                        h.handleGetBranch,
		"GetComment":                                       h.handleGetComment,
		"GetCommentReactions":                              h.handleGetCommentReactions,
		"GetCommentsForComparedCommit":                     h.handleGetCommentsForComparedCommit,
		"GetCommentsForPullRequest":                        h.handleGetCommentsForPullRequest,
		"GetCommit":                                        h.handleGetCommit,
		"GetDifferences":                                   h.handleGetDifferences,
		"GetFile":                                          h.handleGetFile,
		"GetFolder":                                        h.handleGetFolder,
		"GetMergeCommit":                                   h.handleGetMergeCommit,
		"GetMergeConflicts":                                h.handleGetMergeConflicts,
		"GetMergeOptions":                                  h.handleGetMergeOptions,
		"GetPullRequest":                                   h.handleGetPullRequest,
		"GetPullRequestApprovalStates":                     h.handleGetPullRequestApprovalStates,
		"GetPullRequestOverrideState":                      h.handleGetPullRequestOverrideState,
		"GetRepositoryTriggers":                            h.handleGetRepositoryTriggers,
		"ListApprovalRuleTemplates":                        h.handleListApprovalRuleTemplates,
		"ListAssociatedApprovalRuleTemplatesForRepository": h.handleListAssociatedApprovalRuleTemplatesForRepository,
		"ListBranches":                                     h.handleListBranches,
		"ListFileCommitHistory":                            h.handleListFileCommitHistory,
		"ListPullRequests":                                 h.handleListPullRequests,
		"ListRepositoriesForApprovalRuleTemplate":          h.handleListRepositoriesForApprovalRuleTemplate,
		"MergeBranchesByFastForward":                       h.handleMergeBranchesByFastForward,
		"MergeBranchesBySquash":                            h.handleMergeBranchesBySquash,
		"MergeBranchesByThreeWay":                          h.handleMergeBranchesByThreeWay,
		"MergePullRequestByFastForward":                    h.handleMergePullRequestByFastForward,
		"MergePullRequestBySquash":                         h.handleMergePullRequestBySquash,
		"MergePullRequestByThreeWay":                       h.handleMergePullRequestByThreeWay,
		"OverridePullRequestApprovalRules":                 h.handleOverridePullRequestApprovalRules,
		"PostCommentForComparedCommit":                     h.handlePostCommentForComparedCommit,
		"PostCommentForPullRequest":                        h.handlePostCommentForPullRequest,
		"PostCommentReply":                                 h.handlePostCommentReply,
		"PutCommentReaction":                               h.handlePutCommentReaction,
		"PutFile":                                          h.handlePutFile,
		"PutRepositoryTriggers":                            h.handlePutRepositoryTriggers,
		"TestRepositoryTriggers":                           h.handleTestRepositoryTriggers,
		"UpdateApprovalRuleTemplateContent":                h.handleUpdateApprovalRuleTemplateContent,
		"UpdateApprovalRuleTemplateDescription":            h.handleUpdateApprovalRuleTemplateDescription,
		"UpdateApprovalRuleTemplateName":                   h.handleUpdateApprovalRuleTemplateName,
		"UpdateComment":                                    h.handleUpdateComment,
		"UpdateDefaultBranch":                              h.handleUpdateDefaultBranch,
		"UpdatePullRequestApprovalRuleContent":             h.handleUpdatePullRequestApprovalRuleContent,
		"UpdatePullRequestApprovalState":                   h.handleUpdatePullRequestApprovalState,
		"UpdatePullRequestDescription":                     h.handleUpdatePullRequestDescription,
		"UpdatePullRequestStatus":                          h.handleUpdatePullRequestStatus,
		"UpdatePullRequestTitle":                           h.handleUpdatePullRequestTitle,
		"UpdateRepositoryDescription":                      h.handleUpdateRepositoryDescription,
		"UpdateRepositoryEncryptionKey":                    h.handleUpdateRepositoryEncryptionKey,
		"UpdateRepositoryName":                             h.handleUpdateRepositoryName,
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "CodeCommit" }

// GetSupportedOperations returns the list of supported CodeCommit operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AssociateApprovalRuleTemplateWithRepository",
		"BatchAssociateApprovalRuleTemplateWithRepositories",
		"BatchDescribeMergeConflicts",
		"BatchDisassociateApprovalRuleTemplateFromRepositories",
		"BatchGetCommits",
		"BatchGetRepositories",
		"CreateApprovalRuleTemplate",
		"CreateBranch",
		"CreateCommit",
		"CreatePullRequest",
		"CreatePullRequestApprovalRule",
		"CreateRepository",
		"CreateUnreferencedMergeCommit",
		"DeleteApprovalRuleTemplate",
		"DeleteBranch",
		"DeleteCommentContent",
		"DeleteFile",
		"DeletePullRequestApprovalRule",
		"DeleteRepository",
		"DescribeMergeConflicts",
		"DescribePullRequestEvents",
		"DisassociateApprovalRuleTemplateFromRepository",
		"EvaluatePullRequestApprovalRules",
		"GetApprovalRuleTemplate",
		"GetBlob",
		"GetBranch",
		"GetComment",
		"GetCommentReactions",
		"GetCommentsForComparedCommit",
		"GetCommentsForPullRequest",
		"GetCommit",
		"GetDifferences",
		"GetFile",
		"GetFolder",
		"GetMergeCommit",
		"GetMergeConflicts",
		"GetMergeOptions",
		"GetPullRequest",
		"GetPullRequestApprovalStates",
		"GetPullRequestOverrideState",
		"GetRepository",
		"GetRepositoryTriggers",
		"ListApprovalRuleTemplates",
		"ListAssociatedApprovalRuleTemplatesForRepository",
		"ListBranches",
		"ListFileCommitHistory",
		"ListPullRequests",
		"ListRepositories",
		"ListRepositoriesForApprovalRuleTemplate",
		"ListTagsForResource",
		"MergeBranchesByFastForward",
		"MergeBranchesBySquash",
		"MergeBranchesByThreeWay",
		"MergePullRequestByFastForward",
		"MergePullRequestBySquash",
		"MergePullRequestByThreeWay",
		"OverridePullRequestApprovalRules",
		"PostCommentForComparedCommit",
		"PostCommentForPullRequest",
		"PostCommentReply",
		"PutCommentReaction",
		"PutFile",
		"PutRepositoryTriggers",
		"TagResource",
		"TestRepositoryTriggers",
		"UntagResource",
		"UpdateApprovalRuleTemplateContent",
		"UpdateApprovalRuleTemplateDescription",
		"UpdateApprovalRuleTemplateName",
		"UpdateComment",
		"UpdateDefaultBranch",
		"UpdatePullRequestApprovalRuleContent",
		"UpdatePullRequestApprovalState",
		"UpdatePullRequestDescription",
		"UpdatePullRequestStatus",
		"UpdatePullRequestTitle",
		"UpdateRepositoryDescription",
		"UpdateRepositoryEncryptionKey",
		"UpdateRepositoryName",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "codecommit" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this CodeCommit instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches AWS CodeCommit requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), codecommitTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the CodeCommit operation from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, codecommitTargetPrefix)
	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

// ExtractResource extracts the repository name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, readErr := httputils.ReadBody(c.Request())
	if readErr != nil {
		return ""
	}

	var input struct {
		RepositoryName string `json:"repositoryName"`
	}
	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return ""
	}

	return input.RepositoryName
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"CodeCommit", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

// dispatch routes the operation to the appropriate handler and marshals the response.
func (h *Handler) dispatch(_ context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	resp, err := fn(body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(resp)
}

// handleError maps backend errors to HTTP error responses.
func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	code := http.StatusBadRequest
	errType := "ValidationException"

	switch {
	case errors.Is(err, ErrNotFound):
		code = http.StatusNotFound
		errType = "RepositoryDoesNotExistException"
	case errors.Is(err, ErrAlreadyExists):
		code = http.StatusBadRequest
		errType = "RepositoryNameExistsException"
	case errors.Is(err, ErrApprovalRuleTemplateNotFound):
		code = http.StatusNotFound
		errType = "ApprovalRuleTemplateDoesNotExistException"
	case errors.Is(err, ErrApprovalRuleTemplateAlreadyExists):
		code = http.StatusBadRequest
		errType = "ApprovalRuleTemplateNameAlreadyExistsException"
	case errors.Is(err, ErrBranchNotFound):
		code = http.StatusNotFound
		errType = "BranchDoesNotExistException"
	case errors.Is(err, ErrBranchAlreadyExists):
		code = http.StatusBadRequest
		errType = "BranchNameExistsException"
	case errors.Is(err, ErrCommitNotFound):
		code = http.StatusNotFound
		errType = "CommitDoesNotExistException"
	case errors.Is(err, ErrPullRequestNotFound):
		code = http.StatusNotFound
		errType = "PullRequestDoesNotExistException"
	case errors.Is(err, ErrPullRequestAlreadyMerged):
		code = http.StatusBadRequest
		errType = "PullRequestAlreadyClosedException"
	case errors.Is(err, ErrInvalidRepositoryName):
		code = http.StatusBadRequest
		errType = "InvalidRepositoryNameException"
	case errors.Is(err, ErrMaxRepositoriesExceeded):
		code = http.StatusBadRequest
		errType = "MaximumRepositoryNamesExceededException"
	case errors.Is(err, ErrValidation):
		code = http.StatusBadRequest
		errType = "InvalidParameterException"
	case errors.Is(err, errInvalidRequest):
		code = http.StatusBadRequest
		errType = "ValidationException"
	}

	return c.JSON(code, map[string]string{
		"__type":   errType,
		keyMessage: err.Error(),
	})
}

// --- Request body types ---

type createRepositoryInput struct {
	Tags                  map[string]string `json:"tags"`
	RepositoryName        string            `json:"repositoryName"`
	RepositoryDescription string            `json:"repositoryDescription"`
}

type getRepositoryInput struct {
	RepositoryName string `json:"repositoryName"`
}

type deleteRepositoryInput struct {
	RepositoryName string `json:"repositoryName"`
}

type tagResourceInput struct {
	Tags        map[string]string `json:"tags"`
	ResourceARN string            `json:"resourceArn"`
}

type untagResourceInput struct {
	ResourceARN string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

type listTagsForResourceInput struct {
	ResourceARN string `json:"resourceArn"`
}

// --- Response helpers ---

func repoMetadata(r *Repository) map[string]any {
	m := map[string]any{
		keyRepositoryID:     r.RepositoryID,
		keyRepositoryName:   r.RepositoryName,
		"Arn":               r.ARN,
		"accountId":         r.AccountID,
		"cloneUrlHttp":      r.CloneURLHTTP,
		"cloneUrlSsh":       r.CloneURLSSH,
		keyCreationDate:     r.CreationDate.Unix(),
		keyLastModifiedDate: r.LastModifiedDate.Unix(),
	}
	if r.Description != "" {
		m["repositoryDescription"] = r.Description
	}
	if r.DefaultBranch != "" {
		m["defaultBranch"] = r.DefaultBranch
	}
	if r.KmsKeyID != "" {
		m["kmsKeyId"] = r.KmsKeyID
	}

	return m
}

// --- Operation handlers ---

func (h *Handler) handleCreateRepository(body []byte) (any, error) {
	var in createRepositoryInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	r, err := h.Backend.CreateRepository(in.RepositoryName, in.RepositoryDescription, in.Tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"repositoryMetadata": repoMetadata(r),
	}, nil
}

func (h *Handler) handleGetRepository(body []byte) (any, error) {
	var in getRepositoryInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	r, err := h.Backend.GetRepository(in.RepositoryName)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"repositoryMetadata": repoMetadata(r),
	}, nil
}

func (h *Handler) handleDeleteRepository(body []byte) (any, error) {
	var in deleteRepositoryInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	r, err := h.Backend.DeleteRepository(in.RepositoryName)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyRepositoryID: r.RepositoryID,
	}, nil
}

func (h *Handler) handleListRepositories(body []byte) (any, error) {
	var in struct {
		SortBy     string `json:"sortBy"` // "repositoryName" (default) or "lastModifiedDate"
		Order      string `json:"order"`  // "ASCENDING" (default) or "DESCENDING"
		NextToken  string `json:"nextToken"`
		MaxResults int    `json:"maxResults"`
	}
	// Ignore parse errors — all fields are optional.
	_ = json.Unmarshal(body, &in)

	repos := h.Backend.ListRepositories()

	// Apply sort.
	switch in.SortBy {
	case "lastModifiedDate":
		sort.Slice(repos, func(i, j int) bool {
			if strings.EqualFold(in.Order, "DESCENDING") {
				return repos[i].LastModifiedDate.After(repos[j].LastModifiedDate)
			}
			return repos[i].LastModifiedDate.Before(repos[j].LastModifiedDate)
		})
	default:
		// Default: sort by repositoryName ascending (already sorted by backend).
		if strings.EqualFold(in.Order, "DESCENDING") {
			sort.Slice(repos, func(i, j int) bool {
				return repos[i].RepositoryName > repos[j].RepositoryName
			})
		}
	}

	// Apply pagination.
	start := 0
	if in.NextToken != "" {
		if idx, err := strconv.Atoi(in.NextToken); err == nil && idx >= 0 {
			start = idx
		}
	}
	if start > len(repos) {
		start = len(repos)
	}
	end := len(repos)
	if in.MaxResults > 0 && start+in.MaxResults < end {
		end = start + in.MaxResults
	}
	page := repos[start:end]

	items := make([]map[string]any, 0, len(page))
	for _, r := range page {
		items = append(items, map[string]any{
			keyRepositoryID:   r.RepositoryID,
			keyRepositoryName: r.RepositoryName,
		})
	}

	resp := map[string]any{
		"repositories": items,
	}
	if end < len(repos) {
		resp["nextToken"] = strconv.Itoa(end)
	}

	return resp, nil
}

func (h *Handler) handleTagResource(body []byte) (any, error) {
	var in tagResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(in.ResourceARN, in.Tags); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) handleUntagResource(body []byte) (any, error) {
	var in untagResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.ResourceARN, in.TagKeys); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) handleListTagsForResource(body []byte) (any, error) {
	var in listTagsForResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	kv, err := h.Backend.ListTagsForResource(in.ResourceARN)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"tags": kv,
	}, nil
}

// --- New operation input types ---

type createApprovalRuleTemplateInput struct {
	ApprovalRuleTemplateName        string `json:"approvalRuleTemplateName"`
	ApprovalRuleTemplateContent     string `json:"approvalRuleTemplateContent"`
	ApprovalRuleTemplateDescription string `json:"approvalRuleTemplateDescription"`
}

type associateApprovalRuleTemplateWithRepositoryInput struct {
	ApprovalRuleTemplateName string `json:"approvalRuleTemplateName"`
	RepositoryName           string `json:"repositoryName"`
}

type batchAssociateApprovalRuleTemplateInput struct {
	ApprovalRuleTemplateName string   `json:"approvalRuleTemplateName"`
	RepositoryNames          []string `json:"repositoryNames"`
}

type batchDisassociateApprovalRuleTemplateInput struct {
	ApprovalRuleTemplateName string   `json:"approvalRuleTemplateName"`
	RepositoryNames          []string `json:"repositoryNames"`
}

type batchDescribeMergeConflictsInput struct {
	RepositoryName             string   `json:"repositoryName"`
	DestinationCommitSpecifier string   `json:"destinationCommitSpecifier"`
	SourceCommitSpecifier      string   `json:"sourceCommitSpecifier"`
	MergeOption                string   `json:"mergeOption"`
	FilePaths                  []string `json:"filePaths"`
}

type batchGetCommitsInput struct {
	RepositoryName string   `json:"repositoryName"`
	CommitIDs      []string `json:"commitIds"`
}

type batchGetRepositoriesInput struct {
	RepositoryNames []string `json:"repositoryNames"`
}

type createBranchInput struct {
	RepositoryName string `json:"repositoryName"`
	BranchName     string `json:"branchName"`
	CommitID       string `json:"commitId"`
}

type createCommitPutFileEntry struct {
	FilePath    string `json:"filePath"`
	FileContent string `json:"fileContent"` // base64-encoded
	FileMode    string `json:"fileMode"`
}

type createCommitDeleteFileEntry struct {
	FilePath string `json:"filePath"`
}

type createCommitInput struct {
	RepositoryName string                        `json:"repositoryName"`
	BranchName     string                        `json:"branchName"`
	AuthorName     string                        `json:"authorName"`
	Email          string                        `json:"email"`
	CommitMessage  string                        `json:"commitMessage"`
	ParentCommitId string                        `json:"parentCommitId"`
	PutFiles       []createCommitPutFileEntry    `json:"putFiles"`
	DeleteFiles    []createCommitDeleteFileEntry `json:"deleteFiles"`
}

type pullRequestTargetInput struct {
	RepositoryName       string `json:"repositoryName"`
	SourceReference      string `json:"sourceReference"`
	DestinationReference string `json:"destinationReference"`
}

type createPullRequestInput struct {
	Title              string                   `json:"title"`
	Description        string                   `json:"description"`
	ClientRequestToken string                   `json:"clientRequestToken"`
	Targets            []pullRequestTargetInput `json:"targets"`
}

// --- New operation handlers ---

func approvalRuleTemplateToMap(t *ApprovalRuleTemplate) map[string]any {
	m := map[string]any{
		"approvalRuleTemplateId":          t.ApprovalRuleTemplateID,
		"approvalRuleTemplateName":        t.ApprovalRuleTemplateName,
		"approvalRuleTemplateArn":         t.ApprovalRuleTemplateARN,
		"approvalRuleTemplateContent":     t.ApprovalRuleTemplateContent,
		"approvalRuleTemplateDescription": t.ApprovalRuleTemplateDescription,
		keyCreationDate:                   t.CreationDate.Unix(),
		keyLastModifiedDate:               t.LastModifiedDate.Unix(),
		"ruleContentSha256":               t.RuleContentSha256,
	}
	if t.LastModifiedUser != "" {
		m["lastModifiedUser"] = t.LastModifiedUser
	}

	return m
}

func (h *Handler) handleCreateApprovalRuleTemplate(body []byte) (any, error) {
	var in createApprovalRuleTemplateInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.ApprovalRuleTemplateName == "" {
		return nil, fmt.Errorf("%w: approvalRuleTemplateName is required", errInvalidRequest)
	}

	if in.ApprovalRuleTemplateContent == "" {
		return nil, fmt.Errorf("%w: approvalRuleTemplateContent is required", errInvalidRequest)
	}

	t, err := h.Backend.CreateApprovalRuleTemplate(
		in.ApprovalRuleTemplateName,
		in.ApprovalRuleTemplateDescription,
		in.ApprovalRuleTemplateContent,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyApprovalRuleTmpl: approvalRuleTemplateToMap(t),
	}, nil
}

func (h *Handler) handleAssociateApprovalRuleTemplateWithRepository(body []byte) (any, error) {
	var in associateApprovalRuleTemplateWithRepositoryInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.ApprovalRuleTemplateName == "" {
		return nil, fmt.Errorf("%w: approvalRuleTemplateName is required", errInvalidRequest)
	}

	if in.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	if err := h.Backend.AssociateApprovalRuleTemplateWithRepository(
		in.ApprovalRuleTemplateName,
		in.RepositoryName,
	); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) handleBatchAssociateApprovalRuleTemplateWithRepositories(body []byte) (any, error) {
	var in batchAssociateApprovalRuleTemplateInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.ApprovalRuleTemplateName == "" {
		return nil, fmt.Errorf("%w: approvalRuleTemplateName is required", errInvalidRequest)
	}

	associated, batchErrors := h.Backend.BatchAssociateApprovalRuleTemplateWithRepositories(
		in.ApprovalRuleTemplateName,
		in.RepositoryNames,
	)

	if associated == nil {
		associated = []string{}
	}

	if batchErrors == nil {
		batchErrors = []BatchAssociationError{}
	}

	return map[string]any{
		"associatedRepositoryNames": associated,
		keyErrors:                   batchErrors,
	}, nil
}

func (h *Handler) handleBatchDisassociateApprovalRuleTemplateFromRepositories(body []byte) (any, error) {
	var in batchDisassociateApprovalRuleTemplateInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.ApprovalRuleTemplateName == "" {
		return nil, fmt.Errorf("%w: approvalRuleTemplateName is required", errInvalidRequest)
	}

	disassociated, batchErrors := h.Backend.BatchDisassociateApprovalRuleTemplateFromRepositories(
		in.ApprovalRuleTemplateName,
		in.RepositoryNames,
	)

	if disassociated == nil {
		disassociated = []string{}
	}

	if batchErrors == nil {
		batchErrors = []BatchAssociationError{}
	}

	return map[string]any{
		"disassociatedRepositoryNames": disassociated,
		keyErrors:                      batchErrors,
	}, nil
}

// validMergeOptions are the AWS-accepted values for the mergeOption parameter.
func isValidMergeOption(opt string) bool {
	switch opt {
	case "FAST_FORWARD_MERGE", "SQUASH_MERGE", "THREE_WAY_MERGE":
		return true
	}

	return false
}

func (h *Handler) handleBatchDescribeMergeConflicts(body []byte) (any, error) {
	var in batchDescribeMergeConflictsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	if in.DestinationCommitSpecifier == "" {
		return nil, fmt.Errorf("%w: destinationCommitSpecifier is required", errInvalidRequest)
	}

	if in.SourceCommitSpecifier == "" {
		return nil, fmt.Errorf("%w: sourceCommitSpecifier is required", errInvalidRequest)
	}

	if in.MergeOption == "" {
		return nil, fmt.Errorf("%w: mergeOption is required", errInvalidRequest)
	}

	if !isValidMergeOption(in.MergeOption) {
		return nil, fmt.Errorf(
			"%w: mergeOption must be FAST_FORWARD_MERGE, SQUASH_MERGE, or THREE_WAY_MERGE",
			ErrValidation,
		)
	}

	result, err := h.Backend.BatchDescribeMergeConflicts(
		in.RepositoryName,
		in.DestinationCommitSpecifier,
		in.SourceCommitSpecifier,
		in.MergeOption,
		in.FilePaths,
	)
	if err != nil {
		return nil, err
	}

	errs := result.Errors
	if errs == nil {
		errs = []ConflictError{}
	}

	return map[string]any{
		"conflicts":       result.Conflicts,
		keyDestCommitID:   result.DestinationCommitID,
		keySourceCommitID: result.SourceCommitID,
		keyErrors:         errs,
	}, nil
}

func (h *Handler) handleBatchGetCommits(body []byte) (any, error) {
	var in batchGetCommitsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	if len(in.CommitIDs) == 0 {
		return nil, fmt.Errorf("%w: commitIds must contain at least one commit ID", errInvalidRequest)
	}

	found, batchErrors, err := h.Backend.BatchGetCommits(in.RepositoryName, in.CommitIDs)
	if err != nil {
		return nil, err
	}

	commits := make([]map[string]any, 0, len(found))
	for _, c := range found {
		commits = append(commits, commitToMap(c))
	}

	return map[string]any{
		"commits": commits,
		keyErrors: batchErrors,
	}, nil
}

// commitToMap converts a Commit to the AWS-accurate JSON map representation.
// The author/committer date is returned as a Unix timestamp string, matching the real AWS API.
func commitToMap(c *Commit) map[string]any {
	parents := c.Parents
	if parents == nil {
		parents = []string{}
	}

	// AWS returns the commit date as a Unix epoch integer formatted as a decimal string.
	date := ""
	if !c.CreatedAt.IsZero() {
		date = strconv.FormatInt(c.CreatedAt.Unix(), 10)
	}

	return map[string]any{
		keyCommitID: c.CommitID,
		keyTreeID:   c.TreeID,
		keyMessage:  c.Message,
		"parents":   parents,
		"author": map[string]any{
			"name":  c.AuthorName,
			"email": c.AuthorEmail,
			"date":  date,
		},
		"committer": map[string]any{
			"name":  c.CommitterName,
			"email": c.CommitterEmail,
			"date":  date,
		},
		"additionalData": c.AdditionalData,
	}
}

func (h *Handler) handleBatchGetRepositories(body []byte) (any, error) {
	var in batchGetRepositoriesInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	found, notFound, err := h.Backend.BatchGetRepositories(in.RepositoryNames)
	if err != nil {
		return nil, err
	}

	repos := make([]map[string]any, 0, len(found))
	for _, r := range found {
		repos = append(repos, repoMetadata(r))
	}

	if notFound == nil {
		notFound = []string{}
	}

	return map[string]any{
		"repositories":         repos,
		"repositoriesNotFound": notFound,
	}, nil
}

func (h *Handler) handleCreateBranch(body []byte) (any, error) {
	var in createBranchInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	if in.BranchName == "" {
		return nil, fmt.Errorf("%w: branchName is required", errInvalidRequest)
	}

	if in.CommitID == "" {
		return nil, fmt.Errorf("%w: commitId is required", errInvalidRequest)
	}

	if err := h.Backend.CreateBranch(in.RepositoryName, in.BranchName, in.CommitID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) handleCreateCommit(body []byte) (any, error) {
	var in createCommitInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	if in.BranchName == "" {
		return nil, fmt.Errorf("%w: branchName is required", errInvalidRequest)
	}

	// Decode putFiles entries.
	putFiles := make([]PutFileEntry, 0, len(in.PutFiles))
	filesAdded := make([]any, 0, len(in.PutFiles))
	for _, pf := range in.PutFiles {
		content, err := base64.StdEncoding.DecodeString(pf.FileContent)
		if err != nil {
			content = []byte(pf.FileContent)
		}
		fileMode := pf.FileMode
		if fileMode == "" {
			fileMode = fileModeNormal
		}
		putFiles = append(putFiles, PutFileEntry{
			FilePath:    pf.FilePath,
			FileContent: content,
			FileMode:    fileMode,
		})
		filesAdded = append(filesAdded, map[string]any{
			keyFilePath:    pf.FilePath,
			"blobId":       "",
			keyFileMode:    fileMode,
			"absolutePath": pf.FilePath,
		})
	}

	deleteFiles := make([]string, 0, len(in.DeleteFiles))
	filesDeleted := make([]any, 0, len(in.DeleteFiles))
	for _, df := range in.DeleteFiles {
		deleteFiles = append(deleteFiles, df.FilePath)
		filesDeleted = append(filesDeleted, map[string]any{keyFilePath: df.FilePath})
	}

	commit, err := h.Backend.CreateCommit(
		in.RepositoryName, in.BranchName,
		in.AuthorName, in.Email, in.CommitMessage,
		in.ParentCommitId, putFiles, deleteFiles,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyCommitID:    commit.CommitID,
		keyTreeID:      commit.TreeID,
		"filesAdded":   filesAdded,
		"filesUpdated": []any{},
		"filesDeleted": filesDeleted,
	}, nil
}

func pullRequestToMap(pr *PullRequest) map[string]any {
	targets := make([]map[string]any, 0, len(pr.PullRequestTargets))
	for _, t := range pr.PullRequestTargets {
		targets = append(targets, map[string]any{
			keyRepositoryName:      t.RepositoryName,
			"sourceReference":      t.SourceReference,
			"destinationReference": t.DestinationReference,
			"sourceCommit":         t.SourceCommit,
			"destinationCommit":    t.DestinationCommit,
			"mergeBase":            t.MergeBase,
		})
	}

	return map[string]any{
		"pullRequestId":      pr.PullRequestID,
		"title":              pr.Title,
		"description":        pr.Description,
		"authorArn":          pr.AuthorARN,
		"pullRequestStatus":  pr.PullRequestStatus,
		keyCreationDate:      pr.CreationDate.Unix(),
		"lastActivityDate":   pr.LastActivityDate.Unix(),
		"revisionId":         pr.RevisionID,
		"pullRequestTargets": targets,
		"clientRequestToken": pr.ClientRequestToken,
	}
}

func (h *Handler) handleCreatePullRequest(body []byte) (any, error) {
	var in createPullRequestInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.Title == "" {
		return nil, fmt.Errorf("%w: title is required", errInvalidRequest)
	}

	if len(in.Targets) == 0 {
		return nil, fmt.Errorf("%w: at least one target is required", errInvalidRequest)
	}

	targets := make([]PullRequestTarget, 0, len(in.Targets))
	for i, t := range in.Targets {
		if t.RepositoryName == "" {
			return nil, fmt.Errorf("%w: targets[%d].repositoryName is required", errInvalidRequest, i)
		}

		if t.SourceReference == "" {
			return nil, fmt.Errorf("%w: targets[%d].sourceReference is required", errInvalidRequest, i)
		}

		targets = append(targets, PullRequestTarget{
			RepositoryName:       t.RepositoryName,
			SourceReference:      t.SourceReference,
			DestinationReference: t.DestinationReference,
		})
	}

	pr, err := h.Backend.CreatePullRequest(in.Title, in.Description, in.ClientRequestToken, targets)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyPullRequest: pullRequestToMap(pr),
	}, nil
}

func (h *Handler) handleDeleteBranch(body []byte) (any, error) {
	var in struct {
		RepositoryName string `json:"repositoryName"`
		BranchName     string `json:"branchName"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.RepositoryName == "" || in.BranchName == "" {
		return nil, fmt.Errorf("%w: repositoryName and branchName are required", errInvalidRequest)
	}

	br, err := h.Backend.DeleteBranch(in.RepositoryName, in.BranchName)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"deletedBranch": map[string]any{
			"branchName": br.BranchName,
			keyCommitID:  br.CommitID,
		},
	}, nil
}

func (h *Handler) handleGetBranch(body []byte) (any, error) {
	var in struct {
		RepositoryName string `json:"repositoryName"`
		BranchName     string `json:"branchName"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	br, err := h.Backend.GetBranch(in.RepositoryName, in.BranchName)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"branch": map[string]any{
			"branchName": br.BranchName,
			keyCommitID:  br.CommitID,
		},
	}, nil
}

func (h *Handler) handleGetCommit(body []byte) (any, error) {
	var in struct {
		RepositoryName string `json:"repositoryName"`
		CommitID       string `json:"commitId"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	c, err := h.Backend.GetCommit(in.RepositoryName, in.CommitID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"commit": commitToMap(c),
	}, nil
}

func (h *Handler) handleListBranches(body []byte) (any, error) {
	var in struct {
		RepositoryName string `json:"repositoryName"`
		NextToken      string `json:"nextToken"`
		MaxResults     int    `json:"maxResults"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	branches, err := h.Backend.ListBranches(in.RepositoryName)
	if err != nil {
		return nil, err
	}

	// Apply pagination.
	page, nextToken := paginateStrings(branches, in.NextToken, in.MaxResults)

	resp := map[string]any{
		"branches": page,
	}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, nil
}

func (h *Handler) handleGetPullRequest(body []byte) (any, error) {
	var in struct {
		PullRequestID string `json:"pullRequestId"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.PullRequestID == "" {
		return nil, fmt.Errorf("%w: pullRequestId is required", errInvalidRequest)
	}

	pr, err := h.Backend.GetPullRequest(in.PullRequestID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyPullRequest: pullRequestToMap(pr),
	}, nil
}

func (h *Handler) handleListPullRequests(body []byte) (any, error) {
	var in struct {
		RepositoryName    string `json:"repositoryName"`
		PullRequestStatus string `json:"pullRequestStatus"`
		AuthorARN         string `json:"authorArn"`
		NextToken         string `json:"nextToken"`
		MaxResults        int    `json:"maxResults"`
	}
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.RepositoryName == "" {
		return nil, fmt.Errorf("%w: repositoryName is required", errInvalidRequest)
	}

	if in.PullRequestStatus != "" &&
		in.PullRequestStatus != prStatusOpen &&
		in.PullRequestStatus != prStatusClosed &&
		in.PullRequestStatus != prStatusMerged {
		return nil, fmt.Errorf("%w: pullRequestStatus must be OPEN, CLOSED, or MERGED", ErrValidation)
	}

	ids, err := h.Backend.ListPullRequests(in.RepositoryName, in.PullRequestStatus, in.AuthorARN)
	if err != nil {
		return nil, err
	}

	// Apply pagination.
	ids, nextToken := paginateStrings(ids, in.NextToken, in.MaxResults)

	resp := map[string]any{
		"pullRequestIds": ids,
	}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, nil
}
