package directconnect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// directConnectTargetPrefix is the literal X-Amz-Target prefix every one of
// this service's 63 operations carries -- "OvertureService", Direct
// Connect's real internal AWS codename (confirmed by grepping every op's
// own X-Amz-Target header literal in the SDK's serializers.go -- see
// PARITY.md). Unlike outposts/resiliencehub (restjson1, path-routed), this
// service is awsjson1.1: every request is a literal POST / dispatched
// purely by this header, with zero path-based routing at all.
const directConnectTargetPrefix = "OvertureService."

var errUnknownOperation = errors.New("unknown Direct Connect operation")

// Handler is the HTTP handler for the AWS Direct Connect API.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new Direct Connect handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "DirectConnect" }

// GetSupportedOperations returns every operation this handler routes -- all
// 63 operations on the aws-sdk-go-v2/service/directconnect client, per
// sdk_completeness_test.go's empty exception list.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AcceptDirectConnectGatewayAssociationProposal",
		"AllocateConnectionOnInterconnect",
		"AllocateHostedConnection",
		"AllocatePrivateVirtualInterface",
		"AllocatePublicVirtualInterface",
		"AllocateTransitVirtualInterface",
		"AssociateConnectionWithLag",
		"AssociateHostedConnection",
		"AssociateMacSecKey",
		"AssociateVirtualInterface",
		"ConfirmConnection",
		"ConfirmCustomerAgreement",
		"ConfirmPrivateVirtualInterface",
		"ConfirmPublicVirtualInterface",
		"ConfirmTransitVirtualInterface",
		"CreateBGPPeer",
		"CreateConnection",
		"CreateDirectConnectGateway",
		"CreateDirectConnectGatewayAssociation",
		"CreateDirectConnectGatewayAssociationProposal",
		"CreateInterconnect",
		"CreateLag",
		"CreatePrivateVirtualInterface",
		"CreatePublicVirtualInterface",
		"CreateTransitVirtualInterface",
		"DeleteBGPPeer",
		"DeleteConnection",
		"DeleteDirectConnectGateway",
		"DeleteDirectConnectGatewayAssociation",
		"DeleteDirectConnectGatewayAssociationProposal",
		"DeleteInterconnect",
		"DeleteLag",
		"DeleteVirtualInterface",
		"DescribeConnectionLoa",
		"DescribeConnections",
		"DescribeConnectionsOnInterconnect",
		"DescribeCustomerMetadata",
		"DescribeDirectConnectGatewayAssociationProposals",
		"DescribeDirectConnectGatewayAssociations",
		"DescribeDirectConnectGatewayAttachments",
		"DescribeDirectConnectGateways",
		"DescribeHostedConnections",
		"DescribeInterconnectLoa",
		"DescribeInterconnects",
		"DescribeLags",
		"DescribeLoa",
		"DescribeLocations",
		"DescribeRouterConfiguration",
		"DescribeTags",
		"DescribeVirtualGateways",
		"DescribeVirtualInterfaces",
		"DisassociateConnectionFromLag",
		"DisassociateMacSecKey",
		"ListVirtualInterfaceRoutes",
		"ListVirtualInterfaceTestHistory",
		"StartBgpFailoverTest",
		"StopBgpFailoverTest",
		"TagResource",
		"UntagResource",
		"UpdateConnection",
		"UpdateDirectConnectGateway",
		"UpdateDirectConnectGatewayAssociation",
		"UpdateLag",
		"UpdateVirtualInterfaceAttributes",
	}
}

// Reset clears all stored state in the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "directconnect" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a matcher that reports true iff the request carries
// this service's exact X-Amz-Target prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, directConnectTargetPrefix)
	}
}

// MatchPriority returns the routing priority. This service is matched by an
// exact X-Amz-Target prefix (every op carries the literal
// "OvertureService." prefix), the same tier DynamoDB/Kinesis-style
// header-exact services use.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), directConnectTargetPrefix)
}

// ExtractResource extracts the primary resource identifier from the
// request body -- whichever of this service's common id/ARN field names is
// present, matching services/acmpca's extractFirstResourceByKeys pattern.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var m map[string]json.RawMessage
	if unmarshalErr := json.Unmarshal(body, &m); unmarshalErr != nil {
		return ""
	}

	return extractFirstResourceByKeys(
		m,
		"resourceArn", "connectionId", "virtualInterfaceId", "lagId",
		"interconnectId", "directConnectGatewayId", "associationId", "proposalId",
	)
}

// extractFirstResourceByKeys returns the first string resource found for the provided JSON field names.
func extractFirstResourceByKeys(body map[string]json.RawMessage, keys ...string) string {
	for _, key := range keys {
		if raw, ok := body[key]; ok {
			var resourceID string
			if err := json.Unmarshal(raw, &resourceID); err == nil && resourceID != "" {
				return resourceID
			}
		}
	}

	return ""
}

// Handler returns the Echo handler function for Direct Connect requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		return service.HandleTarget(
			c, log,
			"DirectConnect", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			func(dispatchCtx context.Context, action string, body []byte) ([]byte, error) {
				return h.dispatch(dispatchCtx, action, body)
			},
			h.handleError,
		)
	}
}

// dispatch routes action to its handler via the flat opTable() map, keeping
// this function itself a single map lookup (cyclomatic complexity 1)
// instead of one giant switch over all 63 operations.
func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.opTable()[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownOperation, action)
	}

	return fn(ctx, body)
}

type opFunc func(ctx context.Context, body []byte) ([]byte, error)

// opTable merges every op-family's own handler map -- see handler_*.go,
// each contributing one family (connections, lags, interconnects, virtual
// interfaces, BGP/failover-test, gateways/associations, tags, static
// reference data).
func (h *Handler) opTable() map[string]opFunc {
	table := make(map[string]opFunc, len(h.GetSupportedOperations()))

	maps.Copy(table, h.connectionOps())
	maps.Copy(table, h.lagAndInterconnectOps())
	maps.Copy(table, h.vifOps())
	maps.Copy(table, h.bgpOps())
	maps.Copy(table, h.gatewayOps())
	maps.Copy(table, h.staticAndTagOps())

	return table
}

// handleError renders err as one of Direct Connect's 5 awsjson1.1 exception
// shapes: {"__type": "...Exception", "message": "..."}. Every one of the 5
// carries only a bare message -- there is no ResourceId/ResourceType wire
// field anywhere in this service (unlike outposts'/resiliencehub's richer
// error models) -- see errors.go/PARITY.md.
func (h *Handler) handleError(ctx context.Context, c *echo.Context, action string, err error) error {
	status, errType := classifyDirectConnectError(err)

	if status == http.StatusInternalServerError {
		logger.Load(ctx).ErrorContext(ctx, "directconnect: operation failed", "op", action, "error", err)
	}

	body := map[string]string{"__type": errType, "message": err.Error()}
	payload, _ := json.Marshal(body)

	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

	return c.JSONBlob(status, payload)
}

// paginate applies pkgs/page's opaque-token pagination to items, using
// maxResults (nil or <=0 falls back to defaultPageLimit) and token. This
// service's own SDK generates no Paginator type for any of its ten
// MaxResults/NextToken-bearing Describe/List ops (PARITY.md wire-trap #4);
// pkgs/page is used here instead of leaving MaxResults/NextToken silently
// ignored, matching the pkgs-catalog.md guidance to prefer it over
// hand-rolled cursor logic.
func paginate[T any](items []T, token string, maxResults *int32) ([]T, string) {
	limit := 0
	if maxResults != nil {
		limit = int(*maxResults)
	}

	pg := page.New(items, token, limit, defaultPageLimit)

	return pg.Data, pg.Next
}

// decodeBody unmarshals body into a fresh *T, treating an empty body as a
// no-op (leaving the result at its zero value) rather than a JSON error --
// several operations in this service (e.g. DescribeLocations,
// DescribeVirtualGateways, DescribeCustomerMetadata) take no input fields
// at all.
func decodeBody[T any](body []byte) (*T, error) {
	var v T

	if len(body) > 0 {
		if err := json.Unmarshal(body, &v); err != nil {
			return nil, clientError("malformed request body: " + err.Error())
		}
	}

	return &v, nil
}

// marshalResponse marshals v, wrapping any error as a server-fault
// exception (marshaling our own well-typed wire structs should never
// fail).
func marshalResponse(v any) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, serverError("directconnect: marshal response: " + err.Error())
	}

	return data, nil
}

// classifyDirectConnectError maps err to its HTTP status and wire exception
// type. A JSON-decode failure (not an *apiError at all) folds into
// DirectConnectClientException too, since this service has no dedicated
// ValidationException (PARITY.md wire-trap #2).
func classifyDirectConnectError(err error) (int, string) {
	switch {
	case errors.Is(err, errServerSentinel):
		return http.StatusInternalServerError, "DirectConnectServerException"
	case errors.Is(err, errDuplicateTagKeys):
		return http.StatusBadRequest, "DuplicateTagKeysException"
	case errors.Is(err, errLimitExceeded):
		return http.StatusBadRequest, "LimitExceededException"
	case errors.Is(err, errTooManyTags):
		return http.StatusBadRequest, "TooManyTagsException"
	default:
		return http.StatusBadRequest, "DirectConnectClientException"
	}
}
