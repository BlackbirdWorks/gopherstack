package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	backupTargetPrefix = "AmazonBackupService."
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the Echo HTTP handler for AWS Backup operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new Backup handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Backup" }

// GetSupportedOperations returns the list of supported Backup operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateBackupVault",
		"DescribeBackupVault",
		"ListBackupVaults",
		"DeleteBackupVault",
		"CreateBackupPlan",
		"GetBackupPlan",
		"ListBackupPlans",
		"UpdateBackupPlan",
		"DeleteBackupPlan",
		"StartBackupJob",
		"DescribeBackupJob",
		"ListBackupJobs",
		"TagResource",
		"ListTags",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "backup" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Backup instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Backup requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), backupTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Backup action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, backupTargetPrefix)
}

// ExtractResource extracts the primary resource name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, readErr := httputils.ReadBody(c.Request())
	if readErr != nil {
		return ""
	}

	var m map[string]json.RawMessage
	if unmarshalErr := json.Unmarshal(body, &m); unmarshalErr != nil {
		return ""
	}

	for _, key := range []string{"BackupVaultName", "BackupPlanId", "BackupJobId", "ResourceArn"} {
		if v, ok := m[key]; ok {
			var s string
			if json.Unmarshal(v, &s) == nil {
				return s
			}
		}
	}

	return ""
}

// Handler returns the Echo handler function for Backup requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"Backup", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateBackupVault":   service.WrapOp(h.handleCreateBackupVault),
		"DescribeBackupVault": service.WrapOp(h.handleDescribeBackupVault),
		"ListBackupVaults":    service.WrapOp(h.handleListBackupVaults),
		"DeleteBackupVault":   service.WrapOp(h.handleDeleteBackupVault),
		"CreateBackupPlan":    service.WrapOp(h.handleCreateBackupPlan),
		"GetBackupPlan":       service.WrapOp(h.handleGetBackupPlan),
		"ListBackupPlans":     service.WrapOp(h.handleListBackupPlans),
		"UpdateBackupPlan":    service.WrapOp(h.handleUpdateBackupPlan),
		"DeleteBackupPlan":    service.WrapOp(h.handleDeleteBackupPlan),
		"StartBackupJob":      service.WrapOp(h.handleStartBackupJob),
		"DescribeBackupJob":   service.WrapOp(h.handleDescribeBackupJob),
		"ListBackupJobs":      service.WrapOp(h.handleListBackupJobs),
		"TagResource":         service.WrapOp(h.handleTagResource),
		"ListTags":            service.WrapOp(h.handleListTags),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable()[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, ErrNotFound):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "ResourceNotFoundException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusNotFound, payload)
	case errors.Is(err, ErrAlreadyExists):
		payload, _ := json.Marshal(service.JSONErrorResponse{
			Type:    "AlreadyExistsException",
			Message: err.Error(),
		})

		return c.JSONBlob(http.StatusConflict, payload)
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}
}

// --- Vault handlers ---

type createBackupVaultInput struct {
	BackupVaultTags  map[string]string `json:"BackupVaultTags"`
	BackupVaultName  string            `json:"BackupVaultName"`
	EncryptionKeyArn string            `json:"EncryptionKeyArn"`
	CreatorRequestID string            `json:"CreatorRequestId"`
}

type createBackupVaultOutput struct {
	BackupVaultArn  string `json:"BackupVaultArn"`
	BackupVaultName string `json:"BackupVaultName"`
	CreationDate    string `json:"CreationDate"`
}

func (h *Handler) handleCreateBackupVault(
	_ context.Context,
	in *createBackupVaultInput,
) (*createBackupVaultOutput, error) {
	if in.BackupVaultName == "" {
		return nil, fmt.Errorf("%w: BackupVaultName is required", errInvalidRequest)
	}

	v, err := h.Backend.CreateBackupVault(
		in.BackupVaultName,
		in.EncryptionKeyArn,
		in.CreatorRequestID,
		in.BackupVaultTags,
	)
	if err != nil {
		return nil, err
	}

	return &createBackupVaultOutput{
		BackupVaultArn:  v.BackupVaultArn,
		BackupVaultName: v.BackupVaultName,
		CreationDate:    v.CreationTime.Format("2006-01-02T15:04:05Z"),
	}, nil
}

type describeBackupVaultInput struct {
	BackupVaultName string `json:"BackupVaultName"`
}

type describeBackupVaultOutput struct {
	BackupVaultName        string `json:"BackupVaultName"`
	BackupVaultArn         string `json:"BackupVaultArn"`
	EncryptionKeyArn       string `json:"EncryptionKeyArn,omitempty"`
	CreationDate           string `json:"CreationDate"`
	NumberOfRecoveryPoints int64  `json:"NumberOfRecoveryPoints"`
}

func (h *Handler) handleDescribeBackupVault(
	_ context.Context,
	in *describeBackupVaultInput,
) (*describeBackupVaultOutput, error) {
	v, err := h.Backend.DescribeBackupVault(in.BackupVaultName)
	if err != nil {
		return nil, err
	}

	return &describeBackupVaultOutput{
		BackupVaultName:        v.BackupVaultName,
		BackupVaultArn:         v.BackupVaultArn,
		EncryptionKeyArn:       v.EncryptionKeyArn,
		NumberOfRecoveryPoints: v.NumberOfRecoveryPoints,
		CreationDate:           v.CreationTime.Format("2006-01-02T15:04:05Z"),
	}, nil
}

type listBackupVaultsInput struct{}

type vaultSummary struct {
	BackupVaultName        string `json:"BackupVaultName"`
	BackupVaultArn         string `json:"BackupVaultArn"`
	NumberOfRecoveryPoints int64  `json:"NumberOfRecoveryPoints"`
}

type listBackupVaultsOutput struct {
	BackupVaultList []vaultSummary `json:"BackupVaultList"`
}

func (h *Handler) handleListBackupVaults(_ context.Context, _ *listBackupVaultsInput) (*listBackupVaultsOutput, error) {
	vaults := h.Backend.ListBackupVaults()
	items := make([]vaultSummary, 0, len(vaults))
	for _, v := range vaults {
		items = append(items, vaultSummary{
			BackupVaultName:        v.BackupVaultName,
			BackupVaultArn:         v.BackupVaultArn,
			NumberOfRecoveryPoints: v.NumberOfRecoveryPoints,
		})
	}

	return &listBackupVaultsOutput{BackupVaultList: items}, nil
}

type deleteBackupVaultInput struct {
	BackupVaultName string `json:"BackupVaultName"`
}

type deleteBackupVaultOutput struct{}

func (h *Handler) handleDeleteBackupVault(
	_ context.Context,
	in *deleteBackupVaultInput,
) (*deleteBackupVaultOutput, error) {
	if err := h.Backend.DeleteBackupVault(in.BackupVaultName); err != nil {
		return nil, err
	}

	return &deleteBackupVaultOutput{}, nil
}

// --- Plan handlers ---

type backupRuleInput struct {
	RuleName                string `json:"RuleName"`
	TargetBackupVaultName   string `json:"TargetBackupVaultName"`
	ScheduleExpression      string `json:"ScheduleExpression"`
	StartWindowMinutes      int64  `json:"StartWindowMinutes"`
	CompletionWindowMinutes int64  `json:"CompletionWindowMinutes"`
}

type backupPlanInput struct {
	BackupPlanTags map[string]string `json:"BackupPlanTags"`
	BackupPlanName string            `json:"BackupPlanName"`
	Rules          []backupRuleInput `json:"Rules"`
}

type createBackupPlanInput struct {
	BackupPlan backupPlanInput `json:"BackupPlan"`
}

type createBackupPlanOutput struct {
	BackupPlanArn string `json:"BackupPlanArn"`
	BackupPlanID  string `json:"BackupPlanId"`
	VersionID     string `json:"VersionId"`
	CreationDate  string `json:"CreationDate"`
}

func (h *Handler) handleCreateBackupPlan(
	_ context.Context,
	in *createBackupPlanInput,
) (*createBackupPlanOutput, error) {
	if in.BackupPlan.BackupPlanName == "" {
		return nil, fmt.Errorf("%w: BackupPlanName is required", errInvalidRequest)
	}

	rules := make([]Rule, 0, len(in.BackupPlan.Rules))
	for _, r := range in.BackupPlan.Rules {
		rules = append(rules, Rule{
			RuleName:                r.RuleName,
			TargetVaultName:         r.TargetBackupVaultName,
			ScheduleExpression:      r.ScheduleExpression,
			StartWindowMinutes:      r.StartWindowMinutes,
			CompletionWindowMinutes: r.CompletionWindowMinutes,
		})
	}

	p, err := h.Backend.CreateBackupPlan(in.BackupPlan.BackupPlanName, rules, in.BackupPlan.BackupPlanTags)
	if err != nil {
		return nil, err
	}

	return &createBackupPlanOutput{
		BackupPlanArn: p.BackupPlanArn,
		BackupPlanID:  p.BackupPlanID,
		VersionID:     p.VersionID,
		CreationDate:  p.CreationTime.Format("2006-01-02T15:04:05Z"),
	}, nil
}

type getBackupPlanInput struct {
	BackupPlanID string `json:"BackupPlanId"`
}

type backupRuleOutput struct {
	RuleName              string `json:"RuleName"`
	TargetBackupVaultName string `json:"TargetBackupVaultName"`
	ScheduleExpression    string `json:"ScheduleExpression,omitempty"`
}

type backupPlanOutput struct {
	BackupPlanName string             `json:"BackupPlanName"`
	Rules          []backupRuleOutput `json:"Rules"`
}

type getBackupPlanOutput struct {
	BackupPlanArn string           `json:"BackupPlanArn"`
	BackupPlanID  string           `json:"BackupPlanId"`
	VersionID     string           `json:"VersionId"`
	CreationDate  string           `json:"CreationDate"`
	BackupPlan    backupPlanOutput `json:"BackupPlan"`
}

func (h *Handler) handleGetBackupPlan(_ context.Context, in *getBackupPlanInput) (*getBackupPlanOutput, error) {
	p, err := h.Backend.GetBackupPlan(in.BackupPlanID)
	if err != nil {
		return nil, err
	}

	rules := make([]backupRuleOutput, 0, len(p.Rules))
	for _, r := range p.Rules {
		rules = append(rules, backupRuleOutput{
			RuleName:              r.RuleName,
			TargetBackupVaultName: r.TargetVaultName,
			ScheduleExpression:    r.ScheduleExpression,
		})
	}

	return &getBackupPlanOutput{
		BackupPlan: backupPlanOutput{
			BackupPlanName: p.BackupPlanName,
			Rules:          rules,
		},
		BackupPlanArn: p.BackupPlanArn,
		BackupPlanID:  p.BackupPlanID,
		VersionID:     p.VersionID,
		CreationDate:  p.CreationTime.Format("2006-01-02T15:04:05Z"),
	}, nil
}

type listBackupPlansInput struct{}

type planSummary struct {
	BackupPlanName string `json:"BackupPlanName"`
	BackupPlanArn  string `json:"BackupPlanArn"`
	BackupPlanID   string `json:"BackupPlanId"`
	VersionID      string `json:"VersionId"`
}

type listBackupPlansOutput struct {
	BackupPlansList []planSummary `json:"BackupPlansList"`
}

func (h *Handler) handleListBackupPlans(_ context.Context, _ *listBackupPlansInput) (*listBackupPlansOutput, error) {
	plans := h.Backend.ListBackupPlans()
	items := make([]planSummary, 0, len(plans))
	for _, p := range plans {
		items = append(items, planSummary{
			BackupPlanName: p.BackupPlanName,
			BackupPlanArn:  p.BackupPlanArn,
			BackupPlanID:   p.BackupPlanID,
			VersionID:      p.VersionID,
		})
	}

	return &listBackupPlansOutput{BackupPlansList: items}, nil
}

type updateBackupPlanInput struct {
	BackupPlanID string          `json:"BackupPlanId"`
	BackupPlan   backupPlanInput `json:"BackupPlan"`
}

type updateBackupPlanOutput struct {
	BackupPlanArn string `json:"BackupPlanArn"`
	BackupPlanID  string `json:"BackupPlanId"`
	VersionID     string `json:"VersionId"`
}

func (h *Handler) handleUpdateBackupPlan(
	_ context.Context,
	in *updateBackupPlanInput,
) (*updateBackupPlanOutput, error) {
	rules := make([]Rule, 0, len(in.BackupPlan.Rules))
	for _, r := range in.BackupPlan.Rules {
		rules = append(rules, Rule{
			RuleName:                r.RuleName,
			TargetVaultName:         r.TargetBackupVaultName,
			ScheduleExpression:      r.ScheduleExpression,
			StartWindowMinutes:      r.StartWindowMinutes,
			CompletionWindowMinutes: r.CompletionWindowMinutes,
		})
	}

	p, err := h.Backend.UpdateBackupPlan(in.BackupPlanID, rules)
	if err != nil {
		return nil, err
	}

	return &updateBackupPlanOutput{
		BackupPlanArn: p.BackupPlanArn,
		BackupPlanID:  p.BackupPlanID,
		VersionID:     p.VersionID,
	}, nil
}

type deleteBackupPlanInput struct {
	BackupPlanID string `json:"BackupPlanId"`
}

type deleteBackupPlanOutput struct {
	BackupPlanArn string `json:"BackupPlanArn"`
	BackupPlanID  string `json:"BackupPlanId"`
	VersionID     string `json:"VersionId"`
}

func (h *Handler) handleDeleteBackupPlan(
	_ context.Context,
	in *deleteBackupPlanInput,
) (*deleteBackupPlanOutput, error) {
	p, err := h.Backend.GetBackupPlan(in.BackupPlanID)
	if err != nil {
		return nil, err
	}

	if delErr := h.Backend.DeleteBackupPlan(in.BackupPlanID); delErr != nil {
		return nil, delErr
	}

	return &deleteBackupPlanOutput{
		BackupPlanArn: p.BackupPlanArn,
		BackupPlanID:  p.BackupPlanID,
		VersionID:     p.VersionID,
	}, nil
}

// --- Job handlers ---

type startBackupJobInput struct {
	BackupVaultName string `json:"BackupVaultName"`
	ResourceArn     string `json:"ResourceArn"`
	IamRoleArn      string `json:"IamRoleArn"`
	ResourceType    string `json:"ResourceType"`
}

type startBackupJobOutput struct {
	BackupJobID    string `json:"BackupJobId"`
	BackupVaultArn string `json:"BackupVaultArn"`
	CreationDate   string `json:"CreationDate"`
}

func (h *Handler) handleStartBackupJob(_ context.Context, in *startBackupJobInput) (*startBackupJobOutput, error) {
	if in.BackupVaultName == "" {
		return nil, fmt.Errorf("%w: BackupVaultName is required", errInvalidRequest)
	}

	j, err := h.Backend.StartBackupJob(in.BackupVaultName, in.ResourceArn, in.IamRoleArn, in.ResourceType)
	if err != nil {
		return nil, err
	}

	return &startBackupJobOutput{
		BackupJobID:    j.BackupJobID,
		BackupVaultArn: j.BackupVaultArn,
		CreationDate:   j.CreationTime.Format("2006-01-02T15:04:05Z"),
	}, nil
}

type describeBackupJobInput struct {
	BackupJobID string `json:"BackupJobId"`
}

type describeBackupJobOutput struct {
	BackupJobID     string `json:"BackupJobId"`
	BackupVaultName string `json:"BackupVaultName"`
	BackupVaultArn  string `json:"BackupVaultArn"`
	ResourceArn     string `json:"ResourceArn,omitempty"`
	ResourceType    string `json:"ResourceType,omitempty"`
	IamRoleArn      string `json:"IamRoleArn,omitempty"`
	State           string `json:"State"`
	CreationDate    string `json:"CreationDate"`
}

func (h *Handler) handleDescribeBackupJob(
	_ context.Context,
	in *describeBackupJobInput,
) (*describeBackupJobOutput, error) {
	j, err := h.Backend.DescribeBackupJob(in.BackupJobID)
	if err != nil {
		return nil, err
	}

	return &describeBackupJobOutput{
		BackupJobID:     j.BackupJobID,
		BackupVaultName: j.BackupVaultName,
		BackupVaultArn:  j.BackupVaultArn,
		ResourceArn:     j.ResourceArn,
		ResourceType:    j.ResourceType,
		IamRoleArn:      j.IAMRoleArn,
		State:           j.State,
		CreationDate:    j.CreationTime.Format("2006-01-02T15:04:05Z"),
	}, nil
}

type listBackupJobsInput struct {
	ByBackupVaultName string `json:"ByBackupVaultName"`
}

type jobSummary struct {
	BackupJobID     string `json:"BackupJobId"`
	BackupVaultName string `json:"BackupVaultName"`
	BackupVaultArn  string `json:"BackupVaultArn"`
	ResourceArn     string `json:"ResourceArn,omitempty"`
	State           string `json:"State"`
}

type listBackupJobsOutput struct {
	BackupJobs []jobSummary `json:"BackupJobs"`
}

func (h *Handler) handleListBackupJobs(_ context.Context, in *listBackupJobsInput) (*listBackupJobsOutput, error) {
	jobs := h.Backend.ListBackupJobs(in.ByBackupVaultName)
	items := make([]jobSummary, 0, len(jobs))
	for _, j := range jobs {
		items = append(items, jobSummary{
			BackupJobID:     j.BackupJobID,
			BackupVaultName: j.BackupVaultName,
			BackupVaultArn:  j.BackupVaultArn,
			ResourceArn:     j.ResourceArn,
			State:           j.State,
		})
	}

	return &listBackupJobsOutput{BackupJobs: items}, nil
}

// --- Tag handlers ---

type tagResourceInput struct {
	Tags        map[string]string `json:"Tags"`
	ResourceArn string            `json:"ResourceArn"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(_ context.Context, in *tagResourceInput) (*tagResourceOutput, error) {
	if in.Tags == nil {
		in.Tags = make(map[string]string)
	}

	if err := h.Backend.TagResource(in.ResourceArn, in.Tags); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

type listTagsInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsOutput struct {
	Tags map[string]string `json:"Tags"`
}

func (h *Handler) handleListTags(_ context.Context, in *listTagsInput) (*listTagsOutput, error) {
	t, err := h.Backend.ListTags(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsOutput{Tags: t}, nil
}
