package shield

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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

	keyTypeField    = "__type"
	keyMessageField = "message"
	keyStartTime    = "StartTime"
	keyEndTime      = "EndTime"
	keyResourceArn  = "ResourceArn"
	keyAttackID     = "AttackId"
	keyMax          = "Max"
	keyType         = "Type"

	// codeInvalidParameterException is the wire __type for malformed/invalid request parameters.
	// Named as a constant since classifyShieldError maps three distinct sentinel errors
	// (awserr.ErrInvalidParameter, errInvalidRequest, errUnknownAction) to it.
	codeInvalidParameterException = "InvalidParameterException"

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

			return h.handleError(c, err)
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

// shieldErrorRule maps a sentinel error (matched via errors.Is) to a wire error code.
type shieldErrorRule struct {
	sentinel error
	code     string
}

// shieldErrorRules is checked in order -- more specific sentinels MUST precede any generic
// sentinel they wrap, or the generic rule would shadow them. ErrSubscriptionRequired,
// ErrProtectionAlreadyExists, ErrSubscriptionAlreadyExists, and ErrProtectionGroupAlreadyExists
// all wrap awserr.ErrConflict, so ErrSubscriptionRequired (whose real wire code is
// InvalidOperationException, not ResourceAlreadyExistsException) is listed first.
func shieldErrorRules() []shieldErrorRule {
	return []shieldErrorRule{
		{ErrSubscriptionRequired, "InvalidOperationException"},
		{ErrLimitExceeded, "LimitsExceededException"},
		{ErrNoAssociatedRole, "NoAssociatedRoleException"},
		{errInvalidPaginationToken, "InvalidPaginationTokenException"},
		{awserr.ErrNotFound, "ResourceNotFoundException"},
		{awserr.ErrConflict, "ResourceAlreadyExistsException"},
		{awserr.ErrInvalidParameter, codeInvalidParameterException},
		{errInvalidRequest, codeInvalidParameterException},
		{errUnknownAction, codeInvalidParameterException},
	}
}

// classifyShieldError maps err to a wire error code and HTTP status, matching the real Shield
// AWSShield_20160616 error catalog. Malformed request bodies (JSON syntax/type errors) are
// treated the same as InvalidParameterException since they never reach a specific handler's own
// validation. Unrecognized errors fall back to InternalErrorException/500.
func classifyShieldError(err error) (string, int) {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	if errors.As(err, &syntaxErr) || errors.As(err, &typeErr) {
		return codeInvalidParameterException, http.StatusBadRequest
	}

	for _, rule := range shieldErrorRules() {
		if errors.Is(err, rule.sentinel) {
			return rule.code, http.StatusBadRequest
		}
	}

	return "InternalErrorException", http.StatusInternalServerError
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	code, status := classifyShieldError(err)
	payload, _ := json.Marshal(map[string]string{
		keyTypeField:    code,
		keyMessageField: err.Error(),
	})

	return c.JSONBlob(status, payload)
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
