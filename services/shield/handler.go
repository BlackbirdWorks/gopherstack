package shield

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	shieldService       = "shield"
	shieldTargetPrefix  = "AWSShield_20160616."
	shieldMatchPriority = service.PriorityHeaderExact
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler is the HTTP handler for the AWS Shield Advanced API.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new Shield handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{
		Backend: backend,
	}
}

// Reset clears all handler state by delegating to the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "Shield" }

// GetSupportedOperations returns the list of supported Shield operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AssociateDRTLogBucket",
		"AssociateDRTRole",
		"AssociateHealthCheck",
		"AssociateProactiveEngagementDetails",
		"CreateProtection",
		"CreateProtectionGroup",
		"CreateSubscription",
		"DeleteProtection",
		"DeleteProtectionGroup",
		"DeleteSubscription",
		"DescribeAttack",
		"DescribeAttackStatistics",
		"DescribeDRTAccess",
		"DescribeProtection",
		"DescribeSubscription",
		"GetSubscriptionState",
		"ListProtections",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return shieldService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Shield API requests.
// Requests are identified by the X-Amz-Target header prefix "AWSShield_20160616.".
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), shieldTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return shieldMatchPriority }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, shieldTargetPrefix)
}

// ExtractResource extracts the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return h.ExtractOperation(c)
}

// Handler returns the Echo handler function for Shield requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "shield: failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		op := h.ExtractOperation(c)

		result, dispErr := h.dispatch(ctx, op, body)
		if dispErr != nil {
			return h.handleError(c, dispErr)
		}

		if result == nil {
			return c.JSONBlob(http.StatusOK, []byte("{}"))
		}

		return c.JSONBlob(http.StatusOK, result)
	}
}

func (h *Handler) dispatch(ctx context.Context, op string, body []byte) ([]byte, error) {
	if result, ok, err := h.dispatchSubscriptionAndProtectionOps(ctx, op, body); ok {
		return result, err
	}

	if result, ok, err := h.dispatchTagOps(op, body); ok {
		return result, err
	}

	if result, ok, err := h.dispatchDRTAndEngagementOps(op, body); ok {
		return result, err
	}

	if result, ok, err := h.dispatchProtectionGroupAndAttackOps(op, body); ok {
		return result, err
	}

	return nil, fmt.Errorf("%w: %s", errUnknownAction, op)
}

func (h *Handler) dispatchSubscriptionAndProtectionOps(
	ctx context.Context, op string, body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateSubscription":
		return nil, true, h.handleCreateSubscription(ctx)
	case "DeleteSubscription":
		return nil, true, h.handleDeleteSubscription()
	case "DescribeSubscription":
		res, err := h.handleDescribeSubscription()

		return res, true, err
	case "GetSubscriptionState":
		res, err := h.handleGetSubscriptionState()

		return res, true, err
	case "CreateProtection":
		res, err := h.handleCreateProtection(ctx, body)

		return res, true, err
	case "DescribeProtection":
		res, err := h.handleDescribeProtection(body)

		return res, true, err
	case "DeleteProtection":
		return nil, true, h.handleDeleteProtection(ctx, body)
	case "ListProtections":
		res, err := h.handleListProtections()

		return res, true, err
	}

	return nil, false, nil
}

func (h *Handler) dispatchTagOps(op string, body []byte) ([]byte, bool, error) {
	switch op {
	case "TagResource":
		return nil, true, h.handleTagResource(body)
	case "ListTagsForResource":
		res, err := h.handleListTagsForResource(body)

		return res, true, err
	case "UntagResource":
		return nil, true, h.handleUntagResource(body)
	}

	return nil, false, nil
}

func (h *Handler) dispatchDRTAndEngagementOps(op string, body []byte) ([]byte, bool, error) {
	switch op {
	case "AssociateDRTLogBucket":
		return nil, true, h.handleAssociateDRTLogBucket(body)
	case "AssociateDRTRole":
		return nil, true, h.handleAssociateDRTRole(body)
	case "DescribeDRTAccess":
		res, err := h.handleDescribeDRTAccess()

		return res, true, err
	case "AssociateHealthCheck":
		return nil, true, h.handleAssociateHealthCheck(body)
	case "AssociateProactiveEngagementDetails":
		return nil, true, h.handleAssociateProactiveEngagementDetails(body)
	}

	return nil, false, nil
}

func (h *Handler) dispatchProtectionGroupAndAttackOps(op string, body []byte) ([]byte, bool, error) {
	switch op {
	case "CreateProtectionGroup":
		return nil, true, h.handleCreateProtectionGroup(body)
	case "DeleteProtectionGroup":
		return nil, true, h.handleDeleteProtectionGroup(body)
	case "DescribeAttack":
		res, err := h.handleDescribeAttack(body)

		return res, true, err
	case "DescribeAttackStatistics":
		res, err := h.handleDescribeAttackStatistics()

		return res, true, err
	}

	return nil, false, nil
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "ResourceNotFoundException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, awserr.ErrConflict):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "ResourceAlreadyExistsException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, awserr.ErrInvalidParameter),
		errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		payload, _ := json.Marshal(map[string]string{
			"__type":  "InvalidParameterException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	default:
		payload, _ := json.Marshal(map[string]string{
			"__type":  "InternalErrorException",
			"message": err.Error(),
		})

		return c.JSONBlob(http.StatusInternalServerError, payload)
	}
}

func (h *Handler) handleCreateSubscription(ctx context.Context) error {
	if err := h.Backend.CreateSubscription(); err != nil {
		// Shield returns empty body on success; ignore "already exists" per AWS behavior
		if errors.Is(err, awserr.ErrConflict) {
			return nil
		}

		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "shield: created subscription")

	return nil
}

func (h *Handler) handleDescribeSubscription() ([]byte, error) {
	sub, err := h.Backend.DescribeSubscription()
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Subscription": map[string]any{
			"StartTime":            sub.StartTime.Unix(),
			"EndTime":              sub.EndTime.Unix(),
			"AutoRenew":            sub.AutoRenew,
			"TimeCommitmentInDays": sub.TimeCommitmentInDays,
		},
	})
}

func (h *Handler) handleGetSubscriptionState() ([]byte, error) {
	state := h.Backend.GetSubscriptionState()

	return json.Marshal(map[string]string{
		"SubscriptionState": state,
	})
}

// createProtectionRequest is the request body for CreateProtection.
type createProtectionRequest struct {
	Name        string    `json:"Name"`
	ResourceArn string    `json:"ResourceArn"`
	Tags        []tagItem `json:"Tags"`
}

// tagItem represents a key/value pair for the Tags field in Shield API requests.
type tagItem struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

func tagsFromItems(items []tagItem) map[string]string {
	m := make(map[string]string, len(items))

	for _, t := range items {
		m[t.Key] = t.Value
	}

	return m
}

func tagsToItems(tags map[string]string) []tagItem {
	items := make([]tagItem, 0, len(tags))

	for k, v := range tags {
		items = append(items, tagItem{Key: k, Value: v})
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Key < items[j].Key
	})

	return items
}

func (h *Handler) handleCreateProtection(ctx context.Context, body []byte) ([]byte, error) {
	var req createProtectionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.Name == "" {
		return nil, fmt.Errorf("%w: Name is required", errInvalidRequest)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	tags := tagsFromItems(req.Tags)

	p, err := h.Backend.CreateProtection(req.Name, req.ResourceArn, tags)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "shield: created protection", "name", p.Name, "id", p.ID)

	return json.Marshal(map[string]string{
		"ProtectionId": p.ID,
	})
}

// describeProtectionRequest is the request body for DescribeProtection.
type describeProtectionRequest struct {
	ProtectionID string `json:"ProtectionId"`
	ResourceArn  string `json:"ResourceArn"`
}

func (h *Handler) handleDescribeProtection(body []byte) ([]byte, error) {
	var req describeProtectionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProtectionID == "" && req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ProtectionId or ResourceArn is required", errInvalidRequest)
	}

	p, err := h.Backend.DescribeProtection(req.ProtectionID, req.ResourceArn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Protection": protectionToMap(p),
	})
}

// deleteProtectionRequest is the request body for DeleteProtection.
type deleteProtectionRequest struct {
	ProtectionID string `json:"ProtectionId"`
}

func (h *Handler) handleDeleteProtection(ctx context.Context, body []byte) error {
	var req deleteProtectionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProtectionID == "" {
		return fmt.Errorf("%w: ProtectionId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteProtection(req.ProtectionID); err != nil {
		return err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "shield: deleted protection", "id", req.ProtectionID)

	return nil
}

func (h *Handler) handleListProtections() ([]byte, error) {
	protections := h.Backend.ListProtections()
	items := make([]map[string]any, 0, len(protections))

	for _, p := range protections {
		items = append(items, protectionToMap(p))
	}

	return json.Marshal(map[string]any{
		"Protections": items,
	})
}

// tagResourceRequest is the request body for TagResource.
type tagResourceRequest struct {
	ResourceARN string    `json:"ResourceARN"`
	Tags        []tagItem `json:"Tags"`
}

func (h *Handler) handleTagResource(body []byte) error {
	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(req.ResourceARN, tagsFromItems(req.Tags)); err != nil {
		return err
	}

	return nil
}

// listTagsForResourceRequest is the request body for ListTagsForResource.
type listTagsForResourceRequest struct {
	ResourceARN string `json:"ResourceARN"`
}

func (h *Handler) handleListTagsForResource(body []byte) ([]byte, error) {
	var req listTagsForResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(req.ResourceARN)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Tags": tagsToItems(tags),
	})
}

// untagResourceRequest is the request body for UntagResource.
type untagResourceRequest struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(body []byte) error {
	var req untagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(req.ResourceARN, req.TagKeys); err != nil {
		return err
	}

	return nil
}

func protectionToMap(p *Protection) map[string]any {
	return map[string]any{
		"Id":             p.ID,
		"Name":           p.Name,
		"ResourceArn":    p.ResourceARN,
		"HealthCheckIds": p.HealthCheckIDs,
	}
}

// associateDRTLogBucketRequest is the request body for AssociateDRTLogBucket.
type associateDRTLogBucketRequest struct {
	LogBucket string `json:"LogBucket"`
}

func (h *Handler) handleAssociateDRTLogBucket(body []byte) error {
	var req associateDRTLogBucketRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.LogBucket == "" {
		return fmt.Errorf("%w: LogBucket is required", errInvalidRequest)
	}

	if err := h.Backend.AssociateDRTLogBucket(req.LogBucket); err != nil {
		return err
	}

	return nil
}

// associateDRTRoleRequest is the request body for AssociateDRTRole.
type associateDRTRoleRequest struct {
	RoleArn string `json:"RoleArn"`
}

func (h *Handler) handleAssociateDRTRole(body []byte) error {
	var req associateDRTRoleRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.RoleArn == "" {
		return fmt.Errorf("%w: RoleArn is required", errInvalidRequest)
	}

	if err := h.Backend.AssociateDRTRole(req.RoleArn); err != nil {
		return err
	}

	return nil
}

func (h *Handler) handleDescribeDRTAccess() ([]byte, error) {
	access := h.Backend.DescribeDRTAccess()

	return json.Marshal(map[string]any{
		"LogBucketList": access.LogBucketList,
		"RoleArn":       access.RoleArn,
	})
}

// associateHealthCheckRequest is the request body for AssociateHealthCheck.
type associateHealthCheckRequest struct {
	ProtectionID   string `json:"ProtectionId"`
	HealthCheckArn string `json:"HealthCheckArn"`
}

func (h *Handler) handleAssociateHealthCheck(body []byte) error {
	var req associateHealthCheckRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProtectionID == "" {
		return fmt.Errorf("%w: ProtectionId is required", errInvalidRequest)
	}

	if req.HealthCheckArn == "" {
		return fmt.Errorf("%w: HealthCheckArn is required", errInvalidRequest)
	}

	if err := h.Backend.AssociateHealthCheck(req.ProtectionID, req.HealthCheckArn); err != nil {
		return err
	}

	return nil
}

// emergencyContactItem represents a single emergency contact in the API request/response.
type emergencyContactItem struct {
	EmailAddress string `json:"EmailAddress"`
	PhoneNumber  string `json:"PhoneNumber,omitempty"`
	ContactNotes string `json:"ContactNotes,omitempty"`
}

// associateProactiveEngagementRequest is the request body for AssociateProactiveEngagementDetails.
type associateProactiveEngagementRequest struct {
	EmergencyContactList []emergencyContactItem `json:"EmergencyContactList"`
}

func (h *Handler) handleAssociateProactiveEngagementDetails(body []byte) error {
	var req associateProactiveEngagementRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if len(req.EmergencyContactList) == 0 {
		return fmt.Errorf("%w: EmergencyContactList must have at least one entry", errInvalidRequest)
	}

	contacts := make([]EmergencyContact, 0, len(req.EmergencyContactList))

	for _, c := range req.EmergencyContactList {
		if c.EmailAddress == "" {
			return fmt.Errorf("%w: EmailAddress is required in each emergency contact", errInvalidRequest)
		}

		contacts = append(contacts, EmergencyContact(c))
	}

	if err := h.Backend.AssociateProactiveEngagementDetails(contacts); err != nil {
		return err
	}

	return nil
}

// createProtectionGroupRequest is the request body for CreateProtectionGroup.
type createProtectionGroupRequest struct {
	ProtectionGroupID string   `json:"ProtectionGroupId"`
	Aggregation       string   `json:"Aggregation"`
	Pattern           string   `json:"Pattern"`
	ResourceType      string   `json:"ResourceType"`
	Members           []string `json:"Members"`
}

func (h *Handler) handleCreateProtectionGroup(body []byte) error {
	var req createProtectionGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProtectionGroupID == "" {
		return fmt.Errorf("%w: ProtectionGroupId is required", errInvalidRequest)
	}

	if req.Aggregation == "" {
		return fmt.Errorf("%w: Aggregation is required", errInvalidRequest)
	}

	if req.Pattern == "" {
		return fmt.Errorf("%w: Pattern is required", errInvalidRequest)
	}

	if _, err := h.Backend.CreateProtectionGroup(
		req.ProtectionGroupID, req.Aggregation, req.Pattern, req.ResourceType, req.Members,
	); err != nil {
		return err
	}

	return nil
}

// deleteProtectionGroupRequest is the request body for DeleteProtectionGroup.
type deleteProtectionGroupRequest struct {
	ProtectionGroupID string `json:"ProtectionGroupId"`
}

func (h *Handler) handleDeleteProtectionGroup(body []byte) error {
	var req deleteProtectionGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProtectionGroupID == "" {
		return fmt.Errorf("%w: ProtectionGroupId is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteProtectionGroup(req.ProtectionGroupID); err != nil {
		return err
	}

	return nil
}

func (h *Handler) handleDeleteSubscription() error {
	if err := h.Backend.DeleteSubscription(); err != nil {
		return err
	}

	return nil
}

// describeAttackRequest is the request body for DescribeAttack.
type describeAttackRequest struct {
	AttackID string `json:"AttackId"`
}

func (h *Handler) handleDescribeAttack(body []byte) ([]byte, error) {
	var req describeAttackRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AttackID == "" {
		return nil, fmt.Errorf("%w: AttackId is required", errInvalidRequest)
	}

	attack, err := h.Backend.DescribeAttack(req.AttackID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Attack": map[string]any{
			"AttackId":    attack.AttackID,
			"ResourceArn": attack.ResourceARN,
			"StartTime":   attack.StartTime.Unix(),
			"EndTime":     attack.EndTime.Unix(),
		},
	})
}

func (h *Handler) handleDescribeAttackStatistics() ([]byte, error) {
	stats := h.Backend.DescribeAttackStatistics()

	return json.Marshal(map[string]any{
		"AttackStatistics": map[string]any{
			"TimeRange": map[string]any{
				"FromInclusive": stats.TimeRange.FromInclusive,
				"ToExclusive":   stats.TimeRange.ToExclusive,
			},
			"DataItems": stats.DataItems,
		},
	})
}
