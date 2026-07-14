package shield

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	shieldService       = "shield"
	shieldTargetPrefix  = "AWSShield_20160616."
	shieldMatchPriority = service.PriorityHeaderExact

	keyTypeField    = "__type"
	keyMessageField = "message"
	keyStartTime    = "StartTime"
	keyEndTime      = "EndTime"
	keyResourceArn  = "ResourceArn"
	keyAttackID     = "AttackId"
	keyMax          = "Max"
	keyType         = "Type"

	// nanosPerSecond is used to convert UnixNano to fractional seconds (AWS timestamp format).
	nanosPerSecond = 1e9

	// secondsPerDay converts a day-granularity commitment period into the
	// seconds-granularity value the wire format expects (see subscriptionLimits
	// callers of TimeCommitmentInSeconds below).
	secondsPerDay = 86400

	// maxProtectionsPerPage is the upper bound for ListProtections pagination.
	maxProtectionsPerPage = 1000
	// maxProtectionGroupsPerPage is the upper bound for ListProtectionGroups pagination.
	maxProtectionGroupsPerPage = 1000
	// maxAttacksPerPage is the upper bound for ListAttacks pagination.
	maxAttacksPerPage = 10000

	// subscriptionMaxProtections is the Shield Advanced limit for total protections.
	subscriptionMaxProtections = 1000
	// subscriptionMaxProtectionsPerType is the per-resource-type protection limit.
	subscriptionMaxProtectionsPerType = 100
	// subscriptionMaxProtectionGroups is the Shield Advanced limit for protection groups.
	subscriptionMaxProtectionGroups = 100
	// subscriptionMaxMembersPerGroup is the limit for ARBITRARY pattern members.
	subscriptionMaxMembersPerGroup = 10000
)

var (
	errUnknownAction          = errors.New("unknown action")
	errInvalidRequest         = errors.New("invalid request")
	errInvalidPaginationToken = errors.New("invalid pagination token")
)

// floatSeconds converts t to a float64 seconds-since-epoch value (AWS timestamp protocol).
func floatSeconds(t interface {
	Unix() int64
	UnixNano() int64
},
) float64 {
	return float64(t.UnixNano()) / nanosPerSecond
}

// protectedARNEntry describes an ARN service/resource fragment for Shield-supported resource types.
type protectedARNEntry struct {
	service  string
	resource string
}

// shieldSupportedARNs returns the ARN patterns for Shield-supported resource types.
func shieldSupportedARNs() []protectedARNEntry {
	return []protectedARNEntry{
		{"cloudfront", ""},
		{"route53", "hostedzone"},
		{"elasticloadbalancing", "loadbalancer/app/"},
		{"elasticloadbalancing", "loadbalancer/"},
		{"ec2", "eip"},
		{"globalaccelerator", ""},
	}
}

// validateProtectedResourceARN checks that the ARN refers to a Shield-supported resource type.
func validateProtectedResourceARN(arn string) error {
	lower := strings.ToLower(arn)

	for _, entry := range shieldSupportedARNs() {
		if strings.Contains(lower, entry.service) {
			if entry.resource == "" || strings.Contains(lower, strings.ToLower(entry.resource)) {
				return nil
			}
		}
	}

	return fmt.Errorf(
		"%w: ResourceArn %q does not refer to a Shield-supported resource type "+
			"(CloudFront, Route 53 Hosted Zone, ALB, CLB, EIP, Global Accelerator)",
		errInvalidRequest,
		arn,
	)
}

// validateHealthCheckARN checks that the ARN is a Route 53 health check ARN.
func validateHealthCheckARN(arn string) error {
	if !strings.HasPrefix(arn, "arn:aws:route53:::healthcheck/") {
		return fmt.Errorf(
			"%w: HealthCheckArn %q must be a Route 53 health check ARN (arn:aws:route53:::healthcheck/<id>)",
			errInvalidRequest,
			arn,
		)
	}

	return nil
}

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
		"DescribeEmergencyContactSettings",
		"DescribeProtection",
		"DescribeProtectionGroup",
		"DescribeSubscription",
		"DisableApplicationLayerAutomaticResponse",
		"DisableProactiveEngagement",
		"DisassociateDRTLogBucket",
		"DisassociateDRTRole",
		"DisassociateHealthCheck",
		"EnableApplicationLayerAutomaticResponse",
		"EnableProactiveEngagement",
		"GetAttackVectorDefinitionVersion",
		"GetSubscriptionState",
		"ListAttacks",
		"ListProtectionGroups",
		"ListProtections",
		"ListResourcesInProtectionGroup",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
		"UpdateApplicationLayerAutomaticResponse",
		"UpdateEmergencyContactSettings",
		"UpdateProtectionGroup",
		"UpdateSubscription",
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
	case "UpdateSubscription":
		return nil, true, h.handleUpdateSubscription(body)
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
		res, err := h.handleListProtections(body)

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
	case "DisassociateDRTLogBucket":
		return nil, true, h.handleDisassociateDRTLogBucket(body)
	case "AssociateDRTRole":
		return nil, true, h.handleAssociateDRTRole(body)
	case "DisassociateDRTRole":
		return nil, true, h.handleDisassociateDRTRole()
	case "DescribeDRTAccess":
		res, err := h.handleDescribeDRTAccess()

		return res, true, err
	case "AssociateHealthCheck":
		return nil, true, h.handleAssociateHealthCheck(body)
	case "DisassociateHealthCheck":
		return nil, true, h.handleDisassociateHealthCheck(body)
	case "AssociateProactiveEngagementDetails":
		return nil, true, h.handleAssociateProactiveEngagementDetails(body)
	case "UpdateEmergencyContactSettings":
		return nil, true, h.handleUpdateEmergencyContactSettings(body)
	case "DescribeEmergencyContactSettings":
		res, err := h.handleDescribeEmergencyContactSettings()

		return res, true, err
	case "EnableProactiveEngagement":
		return nil, true, h.handleEnableProactiveEngagement()
	case "DisableProactiveEngagement":
		return nil, true, h.handleDisableProactiveEngagement()
	}

	return nil, false, nil
}

func (h *Handler) dispatchProtectionGroupAndAttackOps(
	op string,
	body []byte,
) ([]byte, bool, error) {
	switch op {
	case "CreateProtectionGroup":
		return nil, true, h.handleCreateProtectionGroup(body)
	case "DescribeProtectionGroup":
		res, err := h.handleDescribeProtectionGroup(body)

		return res, true, err
	case "ListProtectionGroups":
		res, err := h.handleListProtectionGroups(body)

		return res, true, err
	case "UpdateProtectionGroup":
		return nil, true, h.handleUpdateProtectionGroup(body)
	case "DeleteProtectionGroup":
		return nil, true, h.handleDeleteProtectionGroup(body)
	case "ListAttacks":
		res, err := h.handleListAttacks(body)

		return res, true, err
	case "DescribeAttack":
		res, err := h.handleDescribeAttack(body)

		return res, true, err
	case "DescribeAttackStatistics":
		res, err := h.handleDescribeAttackStatistics()

		return res, true, err
	case "GetAttackVectorDefinitionVersion":
		res, err := h.handleGetAttackVectorDefinitionVersion()

		return res, true, err
	case "EnableApplicationLayerAutomaticResponse":
		return nil, true, h.handleEnableApplicationLayerAutomaticResponse(body)
	case "DisableApplicationLayerAutomaticResponse":
		return nil, true, h.handleDisableApplicationLayerAutomaticResponse(body)
	case "UpdateApplicationLayerAutomaticResponse":
		return nil, true, h.handleUpdateApplicationLayerAutomaticResponse(body)
	case "ListResourcesInProtectionGroup":
		res, err := h.handleListResourcesInProtectionGroup(body)

		return res, true, err
	case "__SimulateAttack":
		res, err := h.handleSimulateAttack(body)

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
			keyTypeField:    "ResourceNotFoundException",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, awserr.ErrConflict):
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "ResourceAlreadyExistsException",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	case errors.Is(err, awserr.ErrInvalidParameter),
		errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "InvalidParameterException",
			keyMessageField: err.Error(),
		})

		return c.JSONBlob(http.StatusBadRequest, payload)
	default:
		payload, _ := json.Marshal(map[string]string{
			keyTypeField:    "InternalErrorException",
			keyMessageField: err.Error(),
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

// subscriptionLimits returns the standard Shield Advanced subscription limits.
func subscriptionLimits() map[string]any {
	const maxPerType = int64(subscriptionMaxProtectionsPerType)

	return map[string]any{
		"ProtectionLimits": map[string]any{
			"MaxProtections": int64(subscriptionMaxProtections),
			"ProtectedResourceTypeLimits": []map[string]any{
				{keyType: ResourceTypeCloudFrontDistribution, keyMax: maxPerType},
				{keyType: ResourceTypeRoute53HostedZone, keyMax: maxPerType},
				{keyType: ResourceTypeApplicationLoadBalancer, keyMax: maxPerType},
				{keyType: ResourceTypeClassicLoadBalancer, keyMax: maxPerType},
				{keyType: ResourceTypeElasticIPAllocation, keyMax: maxPerType},
				{keyType: ResourceTypeGlobalAccelerator, keyMax: maxPerType},
			},
		},
		"ProtectionGroupLimits": map[string]any{
			"MaxProtectionGroups": int64(subscriptionMaxProtectionGroups),
			"PatternTypeLimits": map[string]any{
				"ArbitraryPatternLimits": map[string]any{
					"MaxMembers": int64(subscriptionMaxMembersPerGroup),
				},
			},
		},
	}
}

// subscriptionResourceLimits returns per-resource-type max count limits.
func subscriptionResourceLimits() []map[string]any {
	const maxInt = int64(100)

	return []map[string]any{
		{keyType: ResourceTypeCloudFrontDistribution, keyMax: maxInt},
		{keyType: ResourceTypeRoute53HostedZone, keyMax: maxInt},
		{keyType: ResourceTypeApplicationLoadBalancer, keyMax: maxInt},
		{keyType: ResourceTypeClassicLoadBalancer, keyMax: maxInt},
		{keyType: ResourceTypeElasticIPAllocation, keyMax: maxInt},
		{keyType: ResourceTypeGlobalAccelerator, keyMax: maxInt},
	}
}

func (h *Handler) handleDescribeSubscription() ([]byte, error) {
	sub, err := h.Backend.DescribeSubscription()
	if err != nil {
		return nil, err
	}

	// Gap 22: correct SubscriptionArn format — no trailing path segment.
	subscriptionArn := arn.Build("shield", "", h.Backend.AccountID(), "subscription")

	return json.Marshal(map[string]any{
		"Subscription": map[string]any{
			keyStartTime: floatSeconds(sub.StartTime),
			keyEndTime:   floatSeconds(sub.EndTime),
			"AutoRenew":  sub.AutoRenew,
			// AWS wire field is TimeCommitmentInSeconds (seconds), not days -- see
			// types.Subscription.TimeCommitmentInSeconds in aws-sdk-go-v2.
			"TimeCommitmentInSeconds":   sub.TimeCommitmentInDays * secondsPerDay,
			"SubscriptionArn":           subscriptionArn,
			"ProactiveEngagementStatus": h.Backend.GetProactiveEngagementStatus(),
			"Limits":                    subscriptionResourceLimits(),
			"SubscriptionLimits":        subscriptionLimits(),
		},
	})
}

// updateSubscriptionRequest is the request body for UpdateSubscription.
type updateSubscriptionRequest struct {
	AutoRenew string `json:"AutoRenew"`
}

func (h *Handler) handleUpdateSubscription(body []byte) error {
	var req updateSubscriptionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.AutoRenew != AutoRenewEnabled && req.AutoRenew != AutoRenewDisabled {
		return fmt.Errorf("%w: AutoRenew must be ENABLED or DISABLED", errInvalidRequest)
	}

	return h.Backend.UpdateSubscription(req.AutoRenew)
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

	if err := validateProtectedResourceARN(req.ResourceArn); err != nil {
		return nil, err
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

	alarCfg := h.Backend.GetALARConfig(p.ResourceARN)

	return json.Marshal(map[string]any{
		"Protection": protectionToMap(p, alarCfg),
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

// listProtectionsRequest is the request body for ListProtections.
type listProtectionsRequest struct {
	InclusionFilters *struct {
		ResourceArns    []string `json:"ResourceArns"`
		ProtectionNames []string `json:"ProtectionNames"`
		ResourceTypes   []string `json:"ResourceTypes"`
	} `json:"InclusionFilters,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

func (h *Handler) handleListProtections(body []byte) ([]byte, error) {
	var req listProtectionsRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	protections := h.Backend.ListProtections()

	if f := req.InclusionFilters; f != nil {
		protections = applyProtectionFilters(
			protections,
			sliceToSet(f.ResourceArns),
			sliceToSet(f.ProtectionNames),
			sliceToSet(f.ResourceTypes),
		)
	}

	maxResults := clampMaxResults(req.MaxResults, maxProtectionsPerPage)

	start, err := decodeOffsetToken(req.NextToken)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", errInvalidRequest, err.Error())
	}

	if start >= len(protections) {
		return json.Marshal(map[string]any{"Protections": []map[string]any{}})
	}

	end := start + maxResults

	var nextToken string

	if end < len(protections) {
		nextToken = encodeOffsetToken(end)
		protections = protections[start:end]
	} else {
		protections = protections[start:]
	}

	items := make([]map[string]any, 0, len(protections))

	for _, p := range protections {
		alarCfg := h.Backend.GetALARConfig(p.ResourceARN)
		items = append(items, protectionToMap(p, alarCfg))
	}

	resp := map[string]any{"Protections": items}

	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// sliceToSet builds a string set from a slice.
func sliceToSet(ss []string) map[string]struct{} {
	if len(ss) == 0 {
		return nil
	}

	m := make(map[string]struct{}, len(ss))

	for _, s := range ss {
		m[s] = struct{}{}
	}

	return m
}

type offsetToken struct {
	O int `json:"o"`
}

// decodeOffsetToken decodes an opaque base64url(JSON) cursor token, returning 0 for an empty token.
// Returns an error on any malformed input so callers can surface InvalidPaginationTokenException.
func decodeOffsetToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}

	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, errInvalidPaginationToken
	}

	var t offsetToken
	if unmarshalErr := json.Unmarshal(raw, &t); unmarshalErr != nil || t.O < 0 {
		return 0, errInvalidPaginationToken
	}

	return t.O, nil
}

// encodeOffsetToken encodes an integer cursor as an opaque base64url(JSON) string.
func encodeOffsetToken(offset int) string {
	data, _ := json.Marshal(offsetToken{O: offset})

	return base64.RawURLEncoding.EncodeToString(data)
}

// clampMaxResults clamps maxResults to [1, maxCap].
func clampMaxResults(v, maxCap int) int {
	if v <= 0 || v > maxCap {
		return maxCap
	}

	return v
}

// protectionMatchesFilters returns true if p passes all inclusion filter sets.
func protectionMatchesFilters(
	p *Protection,
	arnSet, nameSet, typeSet map[string]struct{},
) bool {
	if len(arnSet) > 0 {
		if _, ok := arnSet[p.ResourceARN]; !ok {
			return false
		}
	}

	if len(nameSet) > 0 {
		if _, ok := nameSet[p.Name]; !ok {
			return false
		}
	}

	if len(typeSet) > 0 {
		for rt := range typeSet {
			if resourceARNMatchesType(p.ResourceARN, rt) {
				return true
			}
		}

		return false
	}

	return true
}

// applyProtectionFilters filters protections by the given inclusion filter sets.
func applyProtectionFilters(
	protections []*Protection,
	arnSet, nameSet, typeSet map[string]struct{},
) []*Protection {
	if len(arnSet) == 0 && len(nameSet) == 0 && len(typeSet) == 0 {
		return protections
	}

	out := make([]*Protection, 0, len(protections))

	for _, p := range protections {
		if protectionMatchesFilters(p, arnSet, nameSet, typeSet) {
			out = append(out, p)
		}
	}

	return out
}

// protectionGroupMatchesFilters returns true if pg passes all filter sets.
func protectionGroupMatchesFilters(
	pg *ProtectionGroup,
	idSet, patternSet, typeSet, aggSet map[string]struct{},
) bool {
	if len(idSet) > 0 {
		if _, ok := idSet[pg.ID]; !ok {
			return false
		}
	}

	if len(patternSet) > 0 {
		if _, ok := patternSet[pg.Pattern]; !ok {
			return false
		}
	}

	if len(typeSet) > 0 {
		if _, ok := typeSet[pg.ResourceType]; !ok {
			return false
		}
	}

	if len(aggSet) > 0 {
		if _, ok := aggSet[pg.Aggregation]; !ok {
			return false
		}
	}

	return true
}

// applyProtectionGroupFilters filters protection groups by the given inclusion filter sets.
func applyProtectionGroupFilters(
	groups []*ProtectionGroup,
	idSet, patternSet, typeSet, aggSet map[string]struct{},
) []*ProtectionGroup {
	if len(idSet) == 0 && len(patternSet) == 0 && len(typeSet) == 0 && len(aggSet) == 0 {
		return groups
	}

	out := groups[:0]

	for _, pg := range groups {
		if protectionGroupMatchesFilters(pg, idSet, patternSet, typeSet, aggSet) {
			out = append(out, pg)
		}
	}

	return out
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

func protectionToMap(p *Protection, alarCfg *ALARConfig) map[string]any {
	healthChecks := p.HealthCheckIDs
	if healthChecks == nil {
		healthChecks = []string{}
	}

	m := map[string]any{
		"Id":             p.ID,
		"ProtectionArn":  p.ProtectionArn,
		"Name":           p.Name,
		keyResourceArn:   p.ResourceARN,
		"HealthCheckIds": healthChecks,
		"CreationTime":   floatSeconds(p.CreationTime),
	}

	// Gap 4: include ALAR config when present.
	if alarCfg != nil {
		status := "DISABLED"
		if alarCfg.Enabled {
			status = "ENABLED"
		}

		action := map[string]any{}
		if alarCfg.Action == "BLOCK" {
			action["Block"] = map[string]any{}
		} else {
			action["Count"] = map[string]any{}
		}

		m["ApplicationLayerAutomaticResponseConfiguration"] = map[string]any{
			"Status": status,
			"Action": action,
		}
	}

	return m
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

// disassociateDRTLogBucketRequest is the request body for DisassociateDRTLogBucket.
type disassociateDRTLogBucketRequest struct {
	LogBucket string `json:"LogBucket"`
}

func (h *Handler) handleDisassociateDRTLogBucket(body []byte) error {
	var req disassociateDRTLogBucketRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.LogBucket == "" {
		return fmt.Errorf("%w: LogBucket is required", errInvalidRequest)
	}

	return h.Backend.DisassociateDRTLogBucket(req.LogBucket)
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

func (h *Handler) handleDisassociateDRTRole() error {
	return h.Backend.DisassociateDRTRole()
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

	if err := validateHealthCheckARN(req.HealthCheckArn); err != nil {
		return err
	}

	return h.Backend.AssociateHealthCheck(req.ProtectionID, req.HealthCheckArn)
}

// disassociateHealthCheckRequest is the request body for DisassociateHealthCheck.
type disassociateHealthCheckRequest struct {
	ProtectionID   string `json:"ProtectionId"`
	HealthCheckArn string `json:"HealthCheckArn"`
}

func (h *Handler) handleDisassociateHealthCheck(body []byte) error {
	var req disassociateHealthCheckRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProtectionID == "" {
		return fmt.Errorf("%w: ProtectionId is required", errInvalidRequest)
	}

	if req.HealthCheckArn == "" {
		return fmt.Errorf("%w: HealthCheckArn is required", errInvalidRequest)
	}

	return h.Backend.DisassociateHealthCheck(req.ProtectionID, req.HealthCheckArn)
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
		return fmt.Errorf(
			"%w: EmergencyContactList must have at least one entry",
			errInvalidRequest,
		)
	}

	contacts := make([]EmergencyContact, 0, len(req.EmergencyContactList))

	for _, c := range req.EmergencyContactList {
		if c.EmailAddress == "" {
			return fmt.Errorf(
				"%w: EmailAddress is required in each emergency contact",
				errInvalidRequest,
			)
		}

		contacts = append(contacts, EmergencyContact(c))
	}

	return h.Backend.AssociateProactiveEngagementDetails(contacts)
}

// updateEmergencyContactSettingsRequest is the request body for UpdateEmergencyContactSettings.
type updateEmergencyContactSettingsRequest struct {
	EmergencyContactList []emergencyContactItem `json:"EmergencyContactList"`
}

func (h *Handler) handleUpdateEmergencyContactSettings(body []byte) error {
	var req updateEmergencyContactSettingsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	contacts := make([]EmergencyContact, 0, len(req.EmergencyContactList))

	for _, c := range req.EmergencyContactList {
		contacts = append(contacts, EmergencyContact(c))
	}

	return h.Backend.UpdateEmergencyContactSettings(contacts)
}

func (h *Handler) handleDescribeEmergencyContactSettings() ([]byte, error) {
	contacts := h.Backend.DescribeEmergencyContactSettings()
	items := make([]emergencyContactItem, 0, len(contacts))

	for _, c := range contacts {
		items = append(items, emergencyContactItem(c))
	}

	return json.Marshal(map[string]any{
		"EmergencyContactList": items,
	})
}

func (h *Handler) handleEnableProactiveEngagement() error {
	return h.Backend.EnableProactiveEngagement()
}

func (h *Handler) handleDisableProactiveEngagement() error {
	return h.Backend.DisableProactiveEngagement()
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

	_, err := h.Backend.CreateProtectionGroup(
		req.ProtectionGroupID, req.Aggregation, req.Pattern, req.ResourceType, req.Members,
	)

	return err
}

// describeProtectionGroupRequest is the request body for DescribeProtectionGroup.
type describeProtectionGroupRequest struct {
	ProtectionGroupID string `json:"ProtectionGroupId"`
}

func (h *Handler) handleDescribeProtectionGroup(body []byte) ([]byte, error) {
	var req describeProtectionGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProtectionGroupID == "" {
		return nil, fmt.Errorf("%w: ProtectionGroupId is required", errInvalidRequest)
	}

	pg, err := h.Backend.DescribeProtectionGroup(req.ProtectionGroupID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"ProtectionGroup": protectionGroupToMap(pg),
	})
}

// listProtectionGroupsRequest is the request body for ListProtectionGroups.
type listProtectionGroupsRequest struct {
	InclusionFilters *struct {
		ProtectionGroupIDs []string `json:"ProtectionGroupIds"`
		Patterns           []string `json:"Patterns"`
		ResourceTypes      []string `json:"ResourceTypes"`
		Aggregations       []string `json:"Aggregations"`
	} `json:"InclusionFilters,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

func (h *Handler) handleListProtectionGroups(body []byte) ([]byte, error) {
	var req listProtectionGroupsRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	groups := h.Backend.ListProtectionGroups()

	if f := req.InclusionFilters; f != nil {
		groups = applyProtectionGroupFilters(
			groups,
			sliceToSet(f.ProtectionGroupIDs),
			sliceToSet(f.Patterns),
			sliceToSet(f.ResourceTypes),
			sliceToSet(f.Aggregations),
		)
	}

	maxResults := clampMaxResults(req.MaxResults, maxProtectionGroupsPerPage)

	start, err := decodeOffsetToken(req.NextToken)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", errInvalidRequest, err.Error())
	}

	if start >= len(groups) {
		return json.Marshal(map[string]any{"ProtectionGroups": []map[string]any{}})
	}

	end := start + maxResults

	var nextToken string

	if end < len(groups) {
		nextToken = encodeOffsetToken(end)
		groups = groups[start:end]
	} else {
		groups = groups[start:]
	}

	items := make([]map[string]any, 0, len(groups))

	for _, pg := range groups {
		items = append(items, protectionGroupToMap(pg))
	}

	resp := map[string]any{"ProtectionGroups": items}

	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// updateProtectionGroupRequest is the request body for UpdateProtectionGroup.
type updateProtectionGroupRequest struct {
	ProtectionGroupID string   `json:"ProtectionGroupId"`
	Aggregation       string   `json:"Aggregation"`
	Pattern           string   `json:"Pattern"`
	ResourceType      string   `json:"ResourceType"`
	Members           []string `json:"Members"`
}

func (h *Handler) handleUpdateProtectionGroup(body []byte) error {
	var req updateProtectionGroupRequest
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

	return h.Backend.UpdateProtectionGroup(
		req.ProtectionGroupID, req.Aggregation, req.Pattern, req.ResourceType, req.Members,
	)
}

// listAttacksTimeRange mirrors the AWS Shield TimeRange wire format for ListAttacks.
// AWS sends StartTime/EndTime as {"FromInclusive": float64} / {"ToExclusive": float64}.
type listAttacksTimeRange struct {
	FromInclusive *float64 `json:"FromInclusive,omitempty"`
	ToExclusive   *float64 `json:"ToExclusive,omitempty"`
}

// listAttacksRequest is the request body for ListAttacks.
type listAttacksRequest struct {
	StartTime    *listAttacksTimeRange `json:"StartTime,omitempty"`
	EndTime      *listAttacksTimeRange `json:"EndTime,omitempty"`
	NextToken    string                `json:"NextToken,omitempty"`
	ResourceARNs []string              `json:"ResourceArns"`
	MaxResults   int                   `json:"MaxResults,omitempty"`
}

func (h *Handler) handleListAttacks(body []byte) ([]byte, error) {
	var req listAttacksRequest
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	var startTime, endTime int64
	if req.StartTime != nil && req.StartTime.FromInclusive != nil {
		startTime = int64(*req.StartTime.FromInclusive)
	}

	if req.EndTime != nil && req.EndTime.ToExclusive != nil {
		endTime = int64(*req.EndTime.ToExclusive)
	}

	attacks := h.Backend.ListAttacks(req.ResourceARNs, startTime, endTime)

	maxResults := clampMaxResults(req.MaxResults, maxAttacksPerPage)

	start, err := decodeOffsetToken(req.NextToken)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", errInvalidRequest, err.Error())
	}

	var nextToken string

	if start < len(attacks) {
		end := start + maxResults
		if end < len(attacks) {
			nextToken = encodeOffsetToken(end)
			attacks = attacks[start:end]
		} else {
			attacks = attacks[start:]
		}
	} else {
		attacks = nil
	}

	items := make([]map[string]any, 0, len(attacks))

	for _, a := range attacks {
		vectors := make([]map[string]any, 0, len(a.AttackVectors))
		for _, v := range a.AttackVectors {
			vectors = append(vectors, map[string]any{"VectorType": v.VectorType})
		}

		items = append(items, map[string]any{
			keyAttackID:     a.AttackID,
			keyResourceArn:  a.ResourceARN,
			keyStartTime:    floatSeconds(a.StartTime),
			keyEndTime:      floatSeconds(a.EndTime),
			"AttackVectors": vectors,
		})
	}

	resp := map[string]any{"AttackSummaries": items}

	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

func protectionGroupToMap(pg *ProtectionGroup) map[string]any {
	members := pg.Members
	if members == nil {
		members = []string{}
	}

	return map[string]any{
		"ProtectionGroupId":  pg.ID,
		"ProtectionGroupArn": pg.ProtectionGroupArn,
		"Aggregation":        pg.Aggregation,
		"Pattern":            pg.Pattern,
		"ResourceType":       pg.ResourceType,
		"Members":            members,
		"CreationTime":       floatSeconds(pg.CreationTime),
	}
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

	return h.Backend.DeleteProtectionGroup(req.ProtectionGroupID)
}

func (h *Handler) handleDeleteSubscription() error {
	return h.Backend.DeleteSubscription()
}

// attackVectorDefinitionVersion is the taxonomy version returned by GetAttackVectorDefinitionVersion.
// AWS Shield returns a date-stamped semantic version; we pin to the 2022-11-29 release which
// introduced the current set of vector types used in this emulator.
const attackVectorDefinitionVersion = "20221129.1"

// handleGetAttackVectorDefinitionVersion returns the current attack-vector taxonomy version.
func (h *Handler) handleGetAttackVectorDefinitionVersion() ([]byte, error) {
	return json.Marshal(map[string]any{
		"AttackVectorDefinitionVersion": attackVectorDefinitionVersion,
	})
}

// simulateAttackRequest is the request body for the __SimulateAttack endpoint (gap 25).
type simulateAttackRequest struct {
	ResourceArn       string   `json:"ResourceArn"`
	AttackVectorTypes []string `json:"AttackVectorTypes,omitempty"`
}

// handleSimulateAttack creates a synthetic attack record via the chaos endpoint (gap 25/10).
func (h *Handler) handleSimulateAttack(body []byte) ([]byte, error) {
	var req simulateAttackRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	attack, err := h.Backend.SimulateAttack(req.ResourceArn, req.AttackVectorTypes)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		keyAttackID:    attack.AttackID,
		keyResourceArn: attack.ResourceARN,
		keyStartTime:   floatSeconds(attack.StartTime),
		keyEndTime:     floatSeconds(attack.EndTime),
	})
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

	vectors := make([]map[string]any, 0, len(attack.AttackVectors))
	for _, v := range attack.AttackVectors {
		vectors = append(vectors, map[string]any{"VectorType": v.VectorType})
	}

	counters := make([]map[string]any, 0, len(attack.AttackCounters))
	for _, c := range attack.AttackCounters {
		counters = append(counters, map[string]any{
			"Name":    c.Name,
			keyMax:    c.Max,
			"Average": c.Average,
			"Sum":     c.Sum,
			"N":       c.N,
			"Unit":    c.Unit,
		})
	}

	mitigations := make([]map[string]any, 0, len(attack.Mitigations))
	for _, m := range attack.Mitigations {
		mitigations = append(mitigations, map[string]any{"MitigationName": m.MitigationName})
	}

	return json.Marshal(map[string]any{
		"Attack": map[string]any{
			keyAttackID:      attack.AttackID,
			keyResourceArn:   attack.ResourceARN,
			keyStartTime:     floatSeconds(attack.StartTime),
			keyEndTime:       floatSeconds(attack.EndTime),
			"AttackVectors":  vectors,
			"AttackCounters": counters,
			"Mitigations":    mitigations,
		},
	})
}

func (h *Handler) handleDescribeAttackStatistics() ([]byte, error) {
	stats := h.Backend.DescribeAttackStatistics()

	// AWS returns DataItems and TimeRange at the top level (no "AttackStatistics" wrapper).
	return json.Marshal(map[string]any{
		"TimeRange": map[string]any{
			"FromInclusive": float64(stats.TimeRange.FromInclusive),
			"ToExclusive":   float64(stats.TimeRange.ToExclusive),
		},
		"DataItems": stats.DataItems,
	})
}

// alarAction is the nested action object in ALAR requests.
type alarAction struct {
	Block *struct{} `json:"Block"`
	Count *struct{} `json:"Count"`
}

// alarRequest is the shared request body for ALAR enable/update operations.
type alarRequest struct {
	Action      alarAction `json:"Action"`
	ResourceArn string     `json:"ResourceArn"`
}

// alarActionString extracts "BLOCK" or "COUNT" from an alarAction, or returns an error.
func alarActionString(a alarAction) (string, error) {
	switch {
	case a.Block != nil:
		return "BLOCK", nil
	case a.Count != nil:
		return "COUNT", nil
	default:
		return "", fmt.Errorf("%w: Action must specify Block or Count", errInvalidRequest)
	}
}

func (h *Handler) handleEnableApplicationLayerAutomaticResponse(body []byte) error {
	var req alarRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	action, err := alarActionString(req.Action)
	if err != nil {
		return err
	}

	return h.Backend.EnableApplicationLayerAutomaticResponse(req.ResourceArn, action)
}

// disableALARRequest is the request body for DisableApplicationLayerAutomaticResponse.
type disableALARRequest struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleDisableApplicationLayerAutomaticResponse(body []byte) error {
	var req disableALARRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	return h.Backend.DisableApplicationLayerAutomaticResponse(req.ResourceArn)
}

func (h *Handler) handleUpdateApplicationLayerAutomaticResponse(body []byte) error {
	var req alarRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceArn == "" {
		return fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	action, err := alarActionString(req.Action)
	if err != nil {
		return err
	}

	return h.Backend.UpdateApplicationLayerAutomaticResponse(req.ResourceArn, action)
}

// listResourcesInProtectionGroupRequest is the request body for ListResourcesInProtectionGroup.
type listResourcesInProtectionGroupRequest struct {
	ProtectionGroupID string `json:"ProtectionGroupId"`
}

func (h *Handler) handleListResourcesInProtectionGroup(body []byte) ([]byte, error) {
	var req listResourcesInProtectionGroupRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ProtectionGroupID == "" {
		return nil, fmt.Errorf("%w: ProtectionGroupId is required", errInvalidRequest)
	}

	arns, err := h.Backend.ListResourcesInProtectionGroup(req.ProtectionGroupID)
	if err != nil {
		return nil, err
	}

	if arns == nil {
		arns = []string{}
	}

	return json.Marshal(map[string]any{
		"ResourceArns": arns,
	})
}
