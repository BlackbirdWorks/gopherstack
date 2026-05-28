package ec2

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	ec2APIVersion = "2016-11-15"
	ec2XMLNS      = "http://ec2.amazonaws.com/doc/2016-11-15/"
	unknownOp     = "Unknown"
)

// Handler is the Echo HTTP handler for EC2 operations.
type Handler struct {
	Backend Backend
	ops     map[string]ec2ActionFn
	janitor *Janitor
	// svcCtx is the service-lifetime context derived from the root service
	// context via StartWorker. It is used for detached background work
	// (compute launch/terminate hooks) that must outlive the per-request HTTP
	// context but should still be cancelled at service shutdown.
	// Falls back to context.Background until StartWorker has run.
	svcCtx    context.Context
	AccountID string
	Region    string
}

// NewHandler creates a new EC2 handler with the given backend.
// The dispatch table is built once and cached in h.ops.
func NewHandler(backend Backend) *Handler {
	h := &Handler{Backend: backend, svcCtx: context.Background()}
	h.ops = h.buildOps()

	return h
}

// Reset clears all backend resource state and re-caches the dispatch table.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// WithJanitor attaches a background janitor to the handler.
// If the backend is not an *InMemoryBackend, this is a no-op.
// The optional taskTimeout bounds each sweep; 0 means no per-task timeout.
func (h *Handler) WithJanitor(
	interval, terminatedTTL, cancelledSpotTTL time.Duration,
	taskTimeout ...time.Duration,
) *Handler {
	if mem, ok := h.Backend.(*InMemoryBackend); ok {
		j := NewJanitor(mem, interval, terminatedTTL, cancelledSpotTTL)
		if len(taskTimeout) > 0 {
			j.TaskTimeout = taskTimeout[0]
		}

		h.janitor = j
	}

	return h
}

// StartWorker starts the background janitor if configured and captures the
// service-lifetime context for detached compute hook calls.
func (h *Handler) StartWorker(ctx context.Context) error {
	// Capture the root service ctx so detached fire-and-forget compute hooks
	// (launchOnCompute, terminateOnCompute, computeStartOrStop) can run with
	// a context that outlives any individual HTTP request but is cancelled
	// when the service shuts down. Use WithoutCancel-like semantics: just
	// reuse the parent so cancellation propagates from the root.
	h.svcCtx = ctx

	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
}

// Name returns the service name.
func (h *Handler) Name() string {
	return "EC2"
}

// GetSupportedOperations returns the list of supported EC2 operations.
//
//nolint:funlen
func (h *Handler) GetSupportedOperations() []string {
	extOps := append(deepDiveSupportedOperations(), refinement2SupportedOperations()...)
	extOps = append(extOps, refinement3SupportedOperations()...)
	extOps = append(extOps, networking1SupportedOperations()...)
	extOps = append(extOps, advancedNetworkingSupportedOperations()...)
	extOps = append(extOps, ec2CoreSupportedOperations()...)
	extOps = append(extOps, spotFleetSupportedOperations()...)
	extOps = append(extOps, batch1SupportedOperations()...)
	extOps = append(extOps, batch2SupportedOperations()...)
	extOps = append(extOps, batch3SupportedOperations()...)
	extOps = append(extOps, batch4SupportedOperations()...)
	extOps = append(extOps, stubSupportedOperations()...)

	return append([]string{
		"RunInstances",
		"DescribeInstances",
		"TerminateInstances",
		"StartInstances",
		"StopInstances",
		"RebootInstances",
		"DescribeInstanceStatus",
		"DescribeImages",
		"DescribeRegions",
		"DescribeAvailabilityZones",
		"DescribeSecurityGroups",
		"CreateSecurityGroup",
		"DeleteSecurityGroup",
		"AuthorizeSecurityGroupIngress",
		"AuthorizeSecurityGroupEgress",
		"RevokeSecurityGroupIngress",
		"DescribeVpcs",
		"DescribeVpcAttribute",
		"DescribeSubnets",
		"CreateVpc",
		"DeleteVpc",
		"CreateSubnet",
		"DeleteSubnet",
		"CreateKeyPair",
		"DescribeKeyPairs",
		"DeleteKeyPair",
		"ImportKeyPair",
		"CreateVolume",
		"DescribeVolumes",
		"DeleteVolume",
		"AttachVolume",
		"DetachVolume",
		"AllocateAddress",
		"AssociateAddress",
		"DisassociateAddress",
		"ReleaseAddress",
		"DescribeAddresses",
		"CreateInternetGateway",
		"DeleteInternetGateway",
		"DescribeInternetGateways",
		"AttachInternetGateway",
		"DetachInternetGateway",
		"CreateRouteTable",
		"DeleteRouteTable",
		"DescribeRouteTables",
		"CreateRoute",
		"DeleteRoute",
		"AssociateRouteTable",
		"DisassociateRouteTable",
		"CreateNatGateway",
		"DeleteNatGateway",
		"DescribeNatGateways",
		"DescribeNetworkInterfaces",
		"CreateNetworkInterface",
		"DeleteNetworkInterface",
		"AttachNetworkInterface",
		"DetachNetworkInterface",
		"AssignPrivateIpAddresses",
		"UnassignPrivateIpAddresses",
		"ModifyNetworkInterfaceAttribute",
		"RevokeSecurityGroupEgress",
		"DescribeInstanceTypes",
		"DescribeTags",
		"CreateTags",
		"DeleteTags",
		"DescribeInstanceAttribute",
		"ModifyInstanceAttribute",
		"ResetInstanceAttribute",
		"DescribeImageAttribute",
		"DescribeLaunchTemplates",
		"RequestSpotInstances",
		"DescribeSpotInstanceRequests",
		"CancelSpotInstanceRequests",
		"DescribeSpotPriceHistory",
		"CreatePlacementGroup",
		"DescribePlacementGroups",
		"DeletePlacementGroup",
		"DescribeVolumeAttribute",
		"ModifyVolumeAttribute",
		"DescribeSnapshotAttribute",
		"ModifySnapshotAttribute",
		"AcceptAddressTransfer",
		"AcceptCapacityReservationBillingOwnership",
		"AcceptReservedInstancesExchangeQuote",
		"AcceptTransitGatewayMulticastDomainAssociations",
		"AcceptTransitGatewayPeeringAttachment",
		"AcceptTransitGatewayVpcAttachment",
		"AcceptVpcEndpointConnections",
		"AcceptVpcPeeringConnection",
		"AdvertiseByoipCidr",
		"AllocateHosts",
		"DescribeCapacityReservations",
		"DescribeByoipCidrs",
		"DescribeHosts",
		"DescribeVpcPeeringConnections",
	}, extOps...)
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "ec2" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this EC2 instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Region} }

// RouteMatcher returns a function that matches EC2 requests.
// EC2 requests are form-encoded POSTs containing the EC2 API version.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if r.Method != http.MethodPost {
			return false
		}

		if strings.HasPrefix(r.URL.Path, "/dashboard/") {
			return false
		}

		ct := r.Header.Get("Content-Type")
		if !strings.Contains(ct, "application/x-www-form-urlencoded") {
			return false
		}

		body, err := httputils.ReadBody(r)
		if err != nil {
			return false
		}

		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return false
		}

		return vals.Get("Version") == ec2APIVersion
	}
}

// MatchPriority returns the routing priority for the EC2 handler.
func (h *Handler) MatchPriority() int {
	return service.PriorityFormStandard
}

// ExtractOperation extracts the EC2 action from the request form.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return unknownOp
	}

	action := r.Form.Get("Action")
	if action == "" {
		return unknownOp
	}

	return action
}

// ExtractResource returns the primary resource identifier from the EC2 request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return ""
	}

	resourceKeys := []string{
		"InstanceId.1", "GroupId.1", "GroupId",
		"VpcId.1", "VpcId", "SubnetId.1", "SubnetId",
		"ResourceId.1", "ResourceId",
	}

	for _, key := range resourceKeys {
		if v := r.Form.Get(key); v != "" {
			return v
		}
	}

	return ""
}

// Handler returns the Echo handler function for EC2 requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		reqID := newRequestID()

		r := c.Request()
		if err := r.ParseForm(); err != nil {
			log.ErrorContext(ctx, "failed to parse EC2 request form", "error", err)

			return h.writeError(
				c,
				reqID,
				http.StatusBadRequest,
				"InvalidParameterValue",
				"failed to parse request body",
			)
		}

		action := r.Form.Get("Action")
		if action == "" {
			return h.writeError(
				c,
				reqID,
				http.StatusBadRequest,
				"MissingAction",
				"missing Action parameter",
			)
		}

		log.DebugContext(ctx, "EC2 request", "action", action)

		resp, opErr := h.dispatch(action, r.Form, reqID)
		if opErr != nil {
			return h.handleOpError(c, reqID, action, opErr)
		}

		xmlBytes, marshalErr := marshalXML(resp)
		if marshalErr != nil {
			log.ErrorContext(
				ctx,
				"failed to marshal EC2 response",
				"action",
				action,
				"error",
				marshalErr,
			)

			return h.writeError(
				c,
				reqID,
				http.StatusInternalServerError,
				"InternalFailure",
				"internal server error",
			)
		}

		return c.Blob(http.StatusOK, "text/xml", xmlBytes)
	}
}

type ec2ActionFn func(vals url.Values, reqID string) (any, error)

func (h *Handler) buildOps() map[string]ec2ActionFn {
	ops := h.buildCoreOps()

	registerDeepDiveOps(h, ops)
	registerAcceptAndAdvancedOps(h, ops)
	registerRefinement2Ops(h, ops)
	registerRefinement3Ops(h, ops)
	registerNetworking1Ops(h, ops)
	registerEC2CoreOps(h, ops)
	registerBatch1Ops(h, ops)
	registerBatch2Ops(h, ops)
	registerBatch3Ops(h, ops)
	registerBatch4Ops(h, ops)
	registerBatch5Ops(h, ops)
	registerStubOps(h, ops)
	// registerAdvancedNetworkingOps must run last to override stub entries.
	registerAdvancedNetworkingOps(h, ops)
	// registerSpotFleetOps overrides stub spot fleet handlers with real implementations.
	registerSpotFleetOps(h, ops)

	return ops
}

func (h *Handler) buildCoreOps() map[string]ec2ActionFn {
	return map[string]ec2ActionFn{
		"RunInstances":                    h.handleRunInstances,
		"DescribeInstances":               h.handleDescribeInstances,
		"TerminateInstances":              h.handleTerminateInstances,
		"DescribeSecurityGroups":          h.handleDescribeSecurityGroups,
		"CreateSecurityGroup":             h.handleCreateSecurityGroup,
		"DeleteSecurityGroup":             h.handleDeleteSecurityGroup,
		"RevokeSecurityGroupEgress":       h.handleRevokeSecurityGroupEgress,
		"DescribeVpcs":                    h.handleDescribeVpcs,
		"DescribeVpcAttribute":            h.handleDescribeVpcAttribute,
		"DescribeSubnets":                 h.handleDescribeSubnets,
		"CreateVpc":                       h.handleCreateVpc,
		"DeleteVpc":                       h.handleDeleteVpc,
		"CreateSubnet":                    h.handleCreateSubnet,
		"DeleteSubnet":                    h.handleDeleteSubnet,
		"DescribeInstanceTypes":           h.handleDescribeInstanceTypes,
		"DescribeTags":                    h.handleDescribeTags,
		"CreateTags":                      h.handleCreateTags,
		"DeleteTags":                      h.handleDeleteTags,
		"DescribeInstanceAttribute":       h.handleDescribeInstanceAttribute,
		"ModifyInstanceAttribute":         h.handleModifyInstanceAttribute,
		"ResetInstanceAttribute":          h.handleResetInstanceAttribute,
		"StartInstances":                  h.handleStartInstances,
		"StopInstances":                   h.handleStopInstances,
		"RebootInstances":                 h.handleRebootInstances,
		"DescribeInstanceStatus":          h.handleDescribeInstanceStatus,
		"DescribeImages":                  h.handleDescribeImages,
		"DescribeRegions":                 h.handleDescribeRegions,
		"DescribeAvailabilityZones":       h.handleDescribeAvailabilityZones,
		"CreateKeyPair":                   h.handleCreateKeyPair,
		"DescribeKeyPairs":                h.handleDescribeKeyPairs,
		"DeleteKeyPair":                   h.handleDeleteKeyPair,
		"ImportKeyPair":                   h.handleImportKeyPair,
		"CreateVolume":                    h.handleCreateVolume,
		"DescribeVolumes":                 h.handleDescribeVolumes,
		"DeleteVolume":                    h.handleDeleteVolume,
		"AttachVolume":                    h.handleAttachVolume,
		"DetachVolume":                    h.handleDetachVolume,
		"DescribeVolumeAttribute":         h.handleDescribeVolumeAttribute,
		"ModifyVolumeAttribute":           h.handleModifyVolumeAttribute,
		"DescribeSnapshotAttribute":       h.handleDescribeSnapshotAttribute,
		"ModifySnapshotAttribute":         h.handleModifySnapshotAttribute,
		"AllocateAddress":                 h.handleAllocateAddress,
		"AssociateAddress":                h.handleAssociateAddress,
		"DisassociateAddress":             h.handleDisassociateAddress,
		"ReleaseAddress":                  h.handleReleaseAddress,
		"DescribeAddresses":               h.handleDescribeAddresses,
		"CreateInternetGateway":           h.handleCreateInternetGateway,
		"DeleteInternetGateway":           h.handleDeleteInternetGateway,
		"DescribeInternetGateways":        h.handleDescribeInternetGateways,
		"AttachInternetGateway":           h.handleAttachInternetGateway,
		"DetachInternetGateway":           h.handleDetachInternetGateway,
		"CreateRouteTable":                h.handleCreateRouteTable,
		"DeleteRouteTable":                h.handleDeleteRouteTable,
		"DescribeRouteTables":             h.handleDescribeRouteTables,
		"CreateRoute":                     h.handleCreateRoute,
		"DeleteRoute":                     h.handleDeleteRoute,
		"AssociateRouteTable":             h.handleAssociateRouteTable,
		"DisassociateRouteTable":          h.handleDisassociateRouteTable,
		"CreateNatGateway":                h.handleCreateNatGateway,
		"DeleteNatGateway":                h.handleDeleteNatGateway,
		"DescribeNatGateways":             h.handleDescribeNatGateways,
		"DescribeNetworkInterfaces":       h.handleDescribeNetworkInterfaces,
		"CreateNetworkInterface":          h.handleCreateNetworkInterface,
		"DeleteNetworkInterface":          h.handleDeleteNetworkInterface,
		"AttachNetworkInterface":          h.handleAttachNetworkInterface,
		"DetachNetworkInterface":          h.handleDetachNetworkInterface,
		"AssignPrivateIpAddresses":        h.handleAssignPrivateIPAddresses,
		"UnassignPrivateIpAddresses":      h.handleUnassignPrivateIPAddresses,
		"ModifyNetworkInterfaceAttribute": h.handleModifyNetworkInterfaceAttribute,
		"AuthorizeSecurityGroupIngress":   h.handleAuthorizeSecurityGroupIngress,
		"AuthorizeSecurityGroupEgress":    h.handleAuthorizeSecurityGroupEgress,
		"RevokeSecurityGroupIngress":      h.handleRevokeSecurityGroupIngress,
		"DescribeImageAttribute":          h.handleDescribeImageAttribute,
		"DescribeLaunchTemplates":         h.handleDescribeLaunchTemplates,
		"RequestSpotInstances":            h.handleRequestSpotInstances,
		"DescribeSpotInstanceRequests":    h.handleDescribeSpotInstanceRequests,
		"CancelSpotInstanceRequests":      h.handleCancelSpotInstanceRequests,
		"DescribeSpotPriceHistory":        h.handleDescribeSpotPriceHistory,
		"CreatePlacementGroup":            h.handleCreatePlacementGroup,
		"DescribePlacementGroups":         h.handleDescribePlacementGroups,
		"DeletePlacementGroup":            h.handleDeletePlacementGroup,
		"DescribeVpcPeeringConnections":   h.handleDescribeVpcPeeringConnections,
	}
}

// dispatch routes the EC2 action to the appropriate handler function.
// If DryRun=true is present in vals, the request is validated and then
// rejected with ErrDryRunOperation (HTTP 412) as real AWS does.
func (h *Handler) dispatch(action string, vals url.Values, reqID string) (any, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s is not a supported EC2 action", ErrInvalidParameter, action)
	}

	if vals.Get("DryRun") == ec2BooleanTrue {
		return nil, ErrDryRunOperation
	}

	return fn(vals, reqID)
}

// ---- action handlers ----

func (h *Handler) handleRunInstances(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	instanceType := vals.Get("InstanceType")
	subnetID := vals.Get("SubnetId")
	userData := vals.Get("UserData")
	keyName := vals.Get("KeyName")

	minCount, maxCount, err := parseRunInstancesCounts(vals)
	if err != nil {
		return nil, err
	}

	_ = maxCount // AWS uses MaxCount for capacity planning; mock always launches minCount

	sgIDs, err := h.validateSecurityGroupIDs(vals)
	if err != nil {
		return nil, err
	}

	instances, err := h.Backend.RunInstances(imageID, instanceType, subnetID, minCount)
	if err != nil {
		return nil, err
	}

	for _, inst := range instances {
		if userData != "" {
			// Store as-is; DescribeInstanceAttribute returns the raw (base64) form.
			if attrErr := h.Backend.SetInstanceAttribute(inst.ID, attrUserData, userData); attrErr != nil {
				return nil, attrErr
			}
		}

		if keyName != "" {
			inst.KeyName = keyName
		}

		if len(sgIDs) > 0 {
			inst.SecurityGroups = sgIDs
		}
	}

	if cb, c := h.computeBackend(); c != nil {
		h.launchOnCompute(h.svcCtx, cb, c, instances, keyName, userData)
	}

	if tags := parseTagSpecification(vals, "instance"); len(tags) > 0 {
		ids := make([]string, 0, len(instances))
		for _, inst := range instances {
			ids = append(ids, inst.ID)
		}

		if err = h.Backend.CreateTags(ids, tags); err != nil {
			return nil, err
		}
	}

	items := make([]instanceItem, 0, len(instances))
	for _, inst := range instances {
		items = append(items, toInstanceItem(inst, h.Backend.TagsForResource(inst.ID)))
	}

	return &runInstancesResponse{
		Xmlns:         ec2XMLNS,
		RequestID:     reqID,
		ReservationID: "r-" + uuid.New().String()[:17],
		OwnerID:       h.AccountID,
		InstancesSet:  instanceItemSet{Items: items},
	}, nil
}

// describeInstancesMaxResults is the maximum MaxResults for DescribeInstances.
const describeInstancesMaxResults = 1000

// describeInstancesMinResults is the minimum MaxResults for DescribeInstances.
const describeInstancesMinResults = 5

func (h *Handler) handleDescribeInstances(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InstanceId")

	// Parse named EC2 filters: Filter.N.Name / Filter.N.Value.M
	filters := parseEC2Filters(vals)

	// Fetch all instances matching the IDs (state filter applied post-fetch so
	// that multi-value OR semantics work: e.g. state=running OR state=stopped).
	instances := h.Backend.DescribeInstances(ids, "")

	// Apply all filters post-fetch (AND across filter names, OR within values).
	instances = applyInstanceFilters(instances, filters, h.Backend)

	// Pagination: MaxResults / NextToken.
	maxResults := 0
	if v := vals.Get("MaxResults"); v != "" {
		if _, scanErr := fmt.Sscan(v, &maxResults); scanErr != nil || maxResults < 1 {
			return nil, fmt.Errorf("%w: MaxResults must be a positive integer", ErrInvalidParameter)
		}
		if maxResults < describeInstancesMinResults || maxResults > describeInstancesMaxResults {
			return nil, fmt.Errorf(
				"%w: MaxResults must be between %d and %d",
				ErrInvalidParameter,
				describeInstancesMinResults,
				describeInstancesMaxResults,
			)
		}
	}

	offset := 0
	if tok := vals.Get("NextToken"); tok != "" {
		_, _ = fmt.Sscan(tok, &offset)
	}

	var nextToken string

	if maxResults > 0 {
		if offset > len(instances) {
			offset = len(instances)
		}

		instances = instances[offset:]

		if len(instances) > maxResults {
			nextToken = strconv.Itoa(offset + maxResults)
			instances = instances[:maxResults]
		}
	}

	items := make([]instanceItem, 0, len(instances))
	for _, inst := range instances {
		items = append(items, toInstanceItem(inst, h.Backend.TagsForResource(inst.ID)))
	}

	reservation := reservationItem{
		ReservationID: "r-" + uuid.New().String()[:17],
		OwnerID:       h.AccountID,
		InstancesSet:  instanceItemSet{Items: items},
	}

	return &describeInstancesResponse{
		Xmlns:          ec2XMLNS,
		RequestID:      reqID,
		ReservationSet: reservationSet{Items: []reservationItem{reservation}},
		NextToken:      nextToken,
	}, nil
}

func (h *Handler) handleTerminateInstances(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InstanceId")
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: at least one InstanceId is required", ErrInvalidParameter)
	}

	cb, c := h.computeBackend()

	var (
		providerIDs map[string]string
		dnsNames    map[string]string
	)

	if c != nil {
		providerIDs = snapshotProviderIDs(cb, ids)

		if lookup, ok := h.Backend.(instanceLookup); ok {
			dnsNames = snapshotPublicDNSNames(lookup, ids)
		}
	}

	changes, err := h.Backend.TerminateInstances(ids)
	if err != nil {
		return nil, err
	}

	if c != nil {
		h.terminateOnCompute(h.svcCtx, cb, c, providerIDs, dnsNames)
	}

	items := make([]instanceStateChangeItem, 0, len(changes))
	for _, ch := range changes {
		items = append(items, instanceStateChangeItem{
			InstanceID:    ch.InstanceID,
			CurrentState:  stateItem{Code: ch.CurrentState.Code, Name: ch.CurrentState.Name},
			PreviousState: stateItem{Code: ch.PreviousState.Code, Name: ch.PreviousState.Name},
		})
	}

	return &terminateInstancesResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		InstancesSet: instanceStateChangeSet{Items: items},
	}, nil
}

func (h *Handler) handleDescribeSecurityGroups(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "GroupId")
	groups := h.Backend.DescribeSecurityGroups(ids)

	// Apply named filters: vpc-id, group-name, group-id.
	filters := parseEC2Filters(vals)
	groups = applySecurityGroupFilters(groups, filters)

	items := make([]sgItem, 0, len(groups))
	for _, sg := range groups {
		items = append(items, toSGItem(sg))
	}

	return &describeSecurityGroupsResponse{
		Xmlns:             ec2XMLNS,
		RequestID:         reqID,
		SecurityGroupInfo: sgItemSet{Items: items},
	}, nil
}

func (h *Handler) handleCreateSecurityGroup(vals url.Values, reqID string) (any, error) {
	name := vals.Get("GroupName")
	desc := vals.Get("GroupDescription")
	vpcID := vals.Get("VpcId")

	sg, err := h.Backend.CreateSecurityGroup(name, desc, vpcID)
	if err != nil {
		return nil, err
	}

	if tags := parseTagSpecification(vals, "security-group"); len(tags) > 0 {
		if err = h.Backend.CreateTags([]string{sg.ID}, tags); err != nil {
			return nil, err
		}
	}

	return &createSecurityGroupResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		GroupID:   sg.ID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDeleteSecurityGroup(vals url.Values, reqID string) (any, error) {
	id := vals.Get("GroupId")
	if id == "" {
		return nil, fmt.Errorf("%w: GroupId is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteSecurityGroup(id); err != nil {
		return nil, err
	}

	return &deleteSecurityGroupResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeVpcs(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VpcId")
	vpcs := h.Backend.DescribeVpcs(ids)

	items := make([]vpcItem, 0, len(vpcs))
	for _, v := range vpcs {
		items = append(items, toVPCItem(v))
	}

	return &describeVpcsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		VpcSet:    vpcItemSet{Items: items},
	}, nil
}

type describeVpcAttributeResponse struct {
	XMLName   xml.Name `xml:"DescribeVpcAttributeResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	VpcID     string   `xml:"vpcId"`
	// Attribute has no XML tag; encoding/xml uses the namedBoolAttr.XMLName field (set at runtime)
	// to produce a dynamic element name such as <enableDnsHostnames> or <enableDnsSupport>.
	Attribute namedBoolAttr
}

// namedBoolAttr is a boolean attribute element whose XML element name is set dynamically.
type namedBoolAttr struct {
	XMLName xml.Name
	Value   string `xml:"value"`
}

func (h *Handler) handleDescribeVpcAttribute(vals url.Values, reqID string) (any, error) {
	vpcID := vals.Get("VpcId")
	attr := vals.Get("Attribute")

	// Return false for all VPC boolean attributes (enableDnsHostnames, enableDnsSupport, etc.).
	// Terraform reads these to set up VPC configuration. The attribute name is used as the
	// XML element name to match the AWS EC2 API response format.
	return &describeVpcAttributeResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		VpcID:     vpcID,
		Attribute: namedBoolAttr{XMLName: xml.Name{Local: attr}, Value: ec2BooleanFalse},
	}, nil
}

func (h *Handler) handleCreateVpc(vals url.Values, reqID string) (any, error) {
	cidr := vals.Get("CidrBlock")

	v, err := h.Backend.CreateVpc(cidr)
	if err != nil {
		return nil, err
	}

	if tags := parseTagSpecification(vals, resourceTypeVPC); len(tags) > 0 {
		if err = h.Backend.CreateTags([]string{v.ID}, tags); err != nil {
			return nil, err
		}
	}

	return &createVpcResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Vpc:       toVPCItem(v),
	}, nil
}

func (h *Handler) handleDescribeSubnets(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "SubnetId")
	subnets := h.Backend.DescribeSubnets(ids)

	items := make([]subnetItem, 0, len(subnets))
	for _, s := range subnets {
		items = append(items, toSubnetItem(s))
	}

	return &describeSubnetsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		SubnetSet: subnetItemSet{Items: items},
	}, nil
}

func (h *Handler) handleCreateSubnet(vals url.Values, reqID string) (any, error) {
	vpcID := vals.Get("VpcId")
	cidr := vals.Get("CidrBlock")
	az := vals.Get("AvailabilityZone")

	s, err := h.Backend.CreateSubnet(vpcID, cidr, az)
	if err != nil {
		return nil, err
	}

	if tags := parseTagSpecification(vals, "subnet"); len(tags) > 0 {
		if err = h.Backend.CreateTags([]string{s.ID}, tags); err != nil {
			return nil, err
		}
	}

	return &createSubnetResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Subnet:    toSubnetItem(s),
	}, nil
}

func (h *Handler) handleDeleteVpc(vals url.Values, reqID string) (any, error) {
	id := vals.Get("VpcId")
	if id == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteVpc(id); err != nil {
		return nil, err
	}

	return &deleteVpcResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDeleteSubnet(vals url.Values, reqID string) (any, error) {
	id := vals.Get("SubnetId")
	if id == "" {
		return nil, fmt.Errorf("%w: SubnetId is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteSubnet(id); err != nil {
		return nil, err
	}

	return &deleteSubnetResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

// handleRevokeSecurityGroupEgress is a no-op stub.
// Terraform calls this to revoke the default egress rule when creating a security group.
func (h *Handler) handleRevokeSecurityGroupEgress(_ url.Values, reqID string) (any, error) {
	return &revokeSecurityGroupEgressResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    ec2BooleanTrue,
	}, nil
}

// handleDescribeLaunchTemplates returns launch templates.
func (h *Handler) handleDescribeLaunchTemplates(vals url.Values, reqID string) (any, error) {
	names := parseMemberList(vals, "LaunchTemplateName")
	templates := h.Backend.DescribeLaunchTemplates(names)
	items := make([]launchTemplateItem, 0, len(templates))
	for _, template := range templates {
		items = append(items, launchTemplateItem{
			ID:                   template.ID,
			Name:                 template.Name,
			CreateTime:           template.CreateTime.Format(time.RFC3339),
			CreatedBy:            template.CreatedBy,
			DefaultVersionNumber: template.DefaultVersionNumber,
			LatestVersionNumber:  template.LatestVersionNumber,
		})
	}

	return &describeLaunchTemplatesResponse{
		Xmlns:             ec2XMLNS,
		RequestID:         reqID,
		LaunchTemplateSet: launchTemplateSet{Items: items},
	}, nil
}

// ec2DescribeInstanceTypesMaxPageSize is the AWS-documented upper bound for
// MaxResults on DescribeInstanceTypes. The minimum is 5.
const (
	ec2DescribeInstanceTypesMaxPageSize = 100
	ec2DescribeInstanceTypesMinPageSize = 5
	ec2DefaultInstanceTypeFallback      = "t2.micro"
)

// handleDescribeInstanceTypes returns a stub response for the requested instance
// types. Multiple `InstanceType.N` values are echoed back. `MaxResults` and
// `NextToken` are honored so that callers iterating over instance-type catalogs
// see AWS-shaped pagination, with NextToken representing an opaque integer
// offset into the requested set.
func (h *Handler) handleDescribeInstanceTypes(vals url.Values, reqID string) (any, error) {
	requested := parseMemberList(vals, "InstanceType")

	// Backwards-compat: when a Filter.1.Value.1 is supplied (older callers), use it.
	if len(requested) == 0 {
		if v := vals.Get("Filter.1.Value.1"); v != "" {
			requested = []string{v}
		}
	}

	if len(requested) == 0 {
		requested = []string{ec2DefaultInstanceTypeFallback}
	}

	maxResults, nextToken, err := parseInstanceTypesPagination(vals)
	if err != nil {
		return nil, err
	}

	page, outToken := paginateInstanceTypes(requested, nextToken, maxResults)

	items := make([]instanceTypeItem, 0, len(page))
	for _, t := range page {
		items = append(items, instanceTypeItem{InstanceType: t})
	}

	return &describeInstanceTypesResponse{
		Xmlns:         ec2XMLNS,
		RequestID:     reqID,
		NextToken:     outToken,
		InstanceTypes: instanceTypeSet{Items: items},
	}, nil
}

// parseInstanceTypesPagination validates MaxResults bounds and decodes
// NextToken (which we serialize as a base-10 offset into the result set).
func parseInstanceTypesPagination(vals url.Values) (int, int, error) {
	maxResults := 0

	if v := vals.Get("MaxResults"); v != "" {
		n, perr := strconv.Atoi(v)
		if perr != nil || n < ec2DescribeInstanceTypesMinPageSize ||
			n > ec2DescribeInstanceTypesMaxPageSize {
			return 0, 0, fmt.Errorf(
				"%w: MaxResults=%q must be between %d and %d",
				ErrInvalidParameter, v,
				ec2DescribeInstanceTypesMinPageSize, ec2DescribeInstanceTypesMaxPageSize,
			)
		}

		maxResults = n
	}

	offset := 0

	if tok := vals.Get("NextToken"); tok != "" {
		n, perr := strconv.Atoi(tok)
		if perr != nil || n < 0 {
			return 0, 0, fmt.Errorf("%w: NextToken %q is not valid", ErrInvalidParameter, tok)
		}

		offset = n
	}

	return maxResults, offset, nil
}

// paginateInstanceTypes slices the instance-type catalog and returns the next
// pagination token (empty when fully consumed).
func paginateInstanceTypes(items []string, offset, maxResults int) ([]string, string) {
	if offset >= len(items) {
		return nil, ""
	}

	end := len(items)
	if maxResults > 0 && offset+maxResults < end {
		end = offset + maxResults
	}

	page := items[offset:end]

	var token string
	if end < len(items) {
		token = strconv.Itoa(end)
	}

	return page, token
}

// validDescribeTagsFilters is the set of filter names accepted by DescribeTags.
//
//nolint:gochecknoglobals // lookup set
var validDescribeTagsFilters = map[string]bool{
	"key":           true,
	"resource-id":   true,
	"resource-type": true,
	"value":         true,
}

// handleDescribeTags returns tags for EC2 resources, supporting Filter.N.Name / Filter.N.Value.* semantics.
// If a filter with Name=resource-id is present, only tags for those resource IDs are returned.
// Unknown filter names are rejected with InvalidParameterValue per AWS behaviour.
func (h *Handler) handleDescribeTags(vals url.Values, reqID string) (any, error) {
	var resourceIDs []string

	for i := 1; i <= maxFiltersPerRequest; i++ {
		name := vals.Get(fmt.Sprintf("Filter.%d.Name", i))
		if name == "" {
			break
		}

		if !validDescribeTagsFilters[name] {
			return nil, fmt.Errorf(
				"%w: unknown filter name %q for DescribeTags",
				ErrInvalidParameter,
				name,
			)
		}

		if name == "resource-id" {
			resourceIDs = parseMemberList(vals, fmt.Sprintf("Filter.%d.Value", i))
		}
	}

	entries := h.Backend.DescribeTags(resourceIDs)

	items := make([]tagItem, 0, len(entries))
	for _, e := range entries {
		items = append(items, tagItem(e))
	}

	return &describeTagsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		TagSet:    tagItemSet{Items: items},
	}, nil
}

// handleCreateTags applies tags to one or more resources.
func (h *Handler) handleCreateTags(vals url.Values, reqID string) (any, error) {
	resourceIDs := parseMemberList(vals, "ResourceId")
	tags := parseEC2Tags(vals)

	if err := h.Backend.CreateTags(resourceIDs, tags); err != nil {
		return nil, err
	}

	return &createTagsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    ec2BooleanTrue,
	}, nil
}

// handleDeleteTags removes tags from one or more resources.
func (h *Handler) handleDeleteTags(vals url.Values, reqID string) (any, error) {
	resourceIDs := parseMemberList(vals, "ResourceId")
	keys := parseEC2TagKeys(vals)

	if err := h.Backend.DeleteTags(resourceIDs, keys); err != nil {
		return nil, err
	}

	return &deleteTagsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    ec2BooleanTrue,
	}, nil
}

// handleDescribeInstanceAttribute returns the current value for the requested instance attribute.
// Terraform calls this after RunInstances to read instanceInitiatedShutdownBehavior.
func (h *Handler) handleDescribeInstanceAttribute(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("InstanceId")
	attr := vals.Get("Attribute")

	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceId is required", ErrInvalidParameter)
	}

	instances := h.Backend.DescribeInstances([]string{instanceID}, "")
	if len(instances) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}

	inst := instances[0]

	// Build the attribute value from stored instance state when possible;
	// fall back to AWS defaults for unmodelled attributes.
	var attrValue string

	switch attr {
	case attrUserData:
		attrValue = inst.UserData
	case attrInstanceType:
		attrValue = inst.InstanceType
	case attrEnaSupport:
		if inst.EnaSupport {
			attrValue = ec2BooleanTrue
		} else {
			attrValue = ec2BooleanFalse
		}
	case attrSriovNetSupport:
		if inst.SriovNetSupport != "" {
			attrValue = inst.SriovNetSupport
		} else {
			attrValue = "simple"
		}
	case attrDisableAPIStop, attrDisableAPITermination, attrEBSOptimized:
		attrValue = ec2BooleanFalse
	case attrSourceDest:
		attrValue = ec2BooleanFalse
	case attrInstanceInitiatedShutdownBehavior, attrKernel, attrRamdisk:
		attrValue = "stop"
	default:
		attrValue = ""
	}

	return &describeInstanceAttributeResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		InstanceID: instanceID,
		Attribute:  namedStringAttr{XMLName: xml.Name{Local: attr}, Value: attrValue},
	}, nil
}

// ---- error handling ----

// errCodeLookup maps sentinel errors to their EC2 API error codes.
//
//nolint:gochecknoglobals // package-level mapping, analogous to a lookup table
var errCodeLookup = []struct {
	err  error
	code string
}{
	{ErrInstanceNotFound, "InvalidInstanceID.NotFound"},
	{ErrSecurityGroupNotFound, "InvalidGroup.NotFound"},
	{ErrVPCNotFound, "InvalidVpcID.NotFound"},
	{ErrSubnetNotFound, "InvalidSubnetID.NotFound"},
	{ErrDuplicateSGName, "InvalidGroup.Duplicate"},
	{ErrKeyPairNotFound, "InvalidKeyPair.NotFound"},
	{ErrDuplicateKeyPairName, "InvalidKeyPair.Duplicate"},
	{ErrVolumeNotFound, "InvalidVolume.NotFound"},
	{ErrVolumeInUse, "VolumeInUse"},
	{ErrAddressNotFound, "InvalidAllocationID.NotFound"},
	{ErrInternetGatewayNotFound, "InvalidInternetGatewayID.NotFound"},
	{ErrRouteTableNotFound, "InvalidRouteTableID.NotFound"},
	{ErrNatGatewayNotFound, "InvalidNatGatewayID.NotFound"},
	{ErrRouteNotFound, "InvalidRoute.NotFound"},
	{ErrAssociationNotFound, "InvalidAssociationID.NotFound"},
	{ErrNetworkInterfaceNotFound, "InvalidNetworkInterfaceID.NotFound"},
	{ErrNetworkInterfaceInUse, "InvalidParameterValue"},
	{ErrAttachmentNotFound, "InvalidAttachmentID.NotFound"},
	{ErrSpotRequestNotFound, "InvalidSpotInstanceRequestID.NotFound"},
	{ErrPlacementGroupNotFound, "InvalidPlacementGroup.NotFound"},
	{ErrDuplicatePlacementGroupName, "InvalidPlacementGroup.Duplicate"},
	{ErrInvalidInstanceState, "IncorrectInstanceState"},
	{ErrAddressTransferNotFound, "InvalidAddressTransfer.NotFound"},
	{ErrCapacityReservationNotFound, "InvalidCapacityReservationId.NotFound"},
	{ErrReservedInstancesNotFound, "InvalidReservedInstancesId.NotFound"},
	{ErrTransitGatewayAttachmentNotFound, "InvalidTransitGatewayAttachmentID.NotFound"},
	{ErrVpcPeeringConnectionNotFound, "InvalidVpcPeeringConnectionID.NotFound"},
	{ErrVpcEndpointNotFound, "InvalidVpcEndpointService.NotFound"},
	{ErrByoipCidrNotFound, "InvalidByoipCidr.NotFound"},
	{ErrHostNotFound, "InvalidHostID.NotFound"},
	{ErrCIDRConflict, "InvalidVpc.Conflict"},
	{ErrInvalidParameter, "InvalidParameterValue"},
}

// opErrCode resolves an error to its EC2 API error code and HTTP status code.
func opErrCode(opErr error) (string, int) {
	if errors.Is(opErr, ErrDryRunOperation) {
		return "DryRunOperation", http.StatusPreconditionFailed
	}

	for _, entry := range errCodeLookup {
		if errors.Is(opErr, entry.err) {
			return entry.code, http.StatusBadRequest
		}
	}

	return "InternalFailure", http.StatusInternalServerError
}

func (h *Handler) handleOpError(c *echo.Context, reqID, action string, opErr error) error {
	code, statusCode := opErrCode(opErr)

	if statusCode == http.StatusInternalServerError {
		logger.Load(c.Request().Context()).
			Error("EC2 internal error", "error", opErr, "action", action)
	}

	return h.writeError(c, reqID, statusCode, code, opErr.Error())
}

func (h *Handler) writeError(
	c *echo.Context,
	reqID string,
	statusCode int,
	code, message string,
) error {
	errResp := &ec2ErrorResponse{
		XMLName:   xml.Name{Local: "Response"},
		Errors:    ec2ErrorsWrapper{Error: ec2Error{Code: code, Message: message}},
		RequestID: reqID,
	}

	xmlBytes, err := marshalXML(errResp)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	return c.Blob(statusCode, "text/xml", xmlBytes)
}

// ---- helpers ----

// parseMemberList extracts ordered list parameters like "InstanceId.1", "InstanceId.2", ...
func parseMemberList(vals url.Values, prefix string) []string {
	var result []string

	for i := 1; ; i++ {
		v := vals.Get(fmt.Sprintf("%s.%d", prefix, i))
		if v == "" {
			return result
		}
		result = append(result, v)
	}
}

// maxTagsPerRequest is the maximum number of tags accepted in a single EC2 request.
// AWS allows up to 50 tags per resource; we use 1000 as a generous but bounded limit.
const maxTagsPerRequest = 1000

// maxFiltersPerRequest is the maximum number of filters accepted in a single EC2 DescribeTags request.
const maxFiltersPerRequest = 100

// parseEC2Tags extracts Tag.N.Key / Tag.N.Value from EC2 form values.
func parseEC2Tags(vals url.Values) map[string]string {
	tags := make(map[string]string)

	for i := 1; i <= maxTagsPerRequest; i++ {
		key := vals.Get(fmt.Sprintf("Tag.%d.Key", i))
		if key == "" {
			return tags
		}

		tags[key] = vals.Get(fmt.Sprintf("Tag.%d.Value", i))
	}

	return tags
}

// parseEC2TagKeys extracts Tag.N.Key from EC2 DeleteTags form values.
func parseEC2TagKeys(vals url.Values) []string {
	var keys []string

	for i := 1; i <= maxTagsPerRequest; i++ {
		key := vals.Get(fmt.Sprintf("Tag.%d.Key", i))
		if key == "" {
			return keys
		}

		keys = append(keys, key)
	}

	return keys
}

// parseTagSpecification extracts tags from TagSpecification.N.Tag.M.Key/Value form values
// for a specific resourceType (e.g. resourceTypeVPC, "subnet", "instance", "security-group").
// Terraform and the AWS SDK send inline tags this way during resource creation.
// Returns a map of tag keys to values for the matched resource type, or an empty map if none found.
func parseTagSpecification(vals url.Values, resourceType string) map[string]string {
	tags := make(map[string]string)

	for i := 1; i <= maxTagsPerRequest; i++ {
		rt := vals.Get(fmt.Sprintf("TagSpecification.%d.ResourceType", i))
		if rt == "" {
			break
		}

		if rt != resourceType {
			continue
		}

		for j := 1; j <= maxTagsPerRequest; j++ {
			key := vals.Get(fmt.Sprintf("TagSpecification.%d.Tag.%d.Key", i, j))
			if key == "" {
				break
			}

			tags[key] = vals.Get(fmt.Sprintf("TagSpecification.%d.Tag.%d.Value", i, j))
		}
	}

	return tags
}

// marshalXML encodes the payload with the XML declaration header.
func marshalXML(v any) ([]byte, error) {
	raw, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}

	return append([]byte(xml.Header), raw...), nil
}

// newRequestID generates a unique request ID.
func newRequestID() string {
	return "gopherstack-ec2-" + uuid.New().String()
}

// ---- XML conversion helpers ----

func toInstanceItem(inst *Instance, instanceTags map[string]string) instanceItem {
	tagItems := make([]instanceTagItem, 0, len(instanceTags))
	for k, v := range instanceTags {
		tagItems = append(tagItems, instanceTagItem{Key: k, Value: v})
	}

	sort.Slice(tagItems, func(i, j int) bool { return tagItems[i].Key < tagItems[j].Key })

	groupItems := make([]instanceGroupItem, 0, len(inst.SecurityGroups))
	for _, sgID := range inst.SecurityGroups {
		groupItems = append(groupItems, instanceGroupItem{GroupID: sgID})
	}

	return instanceItem{
		InstanceID:       inst.ID,
		ImageID:          inst.ImageID,
		InstanceType:     inst.InstanceType,
		StateItem:        stateItem{Code: inst.State.Code, Name: inst.State.Name},
		VPCID:            inst.VPCID,
		SubnetID:         inst.SubnetID,
		LaunchTime:       inst.LaunchTime.Format("2006-01-02T15:04:05.000Z"),
		PrivateIPAddress: inst.PrivateIP,
		PublicIPAddress:  inst.PublicIPAddress,
		PublicDNSName:    inst.PublicDNSName,
		KeyName:          inst.KeyName,
		GroupSet:         instanceGroupSet{Items: groupItems},
		TagSet:           instanceTagItemSet{Items: tagItems},
	}
}

func toSGItem(sg *SecurityGroup) sgItem {
	return sgItem{
		GroupID:          sg.ID,
		GroupName:        sg.Name,
		GroupDescription: sg.Description,
		VPCID:            sg.VPCID,
	}
}

func toVPCItem(v *VPC) vpcItem {
	isDefault := ec2BooleanFalse
	if v.IsDefault {
		isDefault = ec2BooleanTrue
	}

	return vpcItem{
		VpcID:     v.ID,
		CIDRBlock: v.CIDRBlock,
		IsDefault: isDefault,
		State:     stateAvailable,
	}
}

func toSubnetItem(s *Subnet) subnetItem {
	return subnetItem{
		SubnetID:         s.ID,
		VPCID:            s.VPCID,
		CIDRBlock:        s.CIDRBlock,
		AvailabilityZone: s.AvailabilityZone,
		State:            stateAvailable,
	}
}

// ---- XML response types ----

type ec2Error struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

type ec2ErrorsWrapper struct {
	Error ec2Error `xml:"Error"`
}

type ec2ErrorResponse struct {
	XMLName   xml.Name         `xml:"Response"`
	Errors    ec2ErrorsWrapper `xml:"Errors"`
	RequestID string           `xml:"RequestID"`
}

type stateItem struct {
	Name string `xml:"name"`
	Code int    `xml:"code"`
}

type instanceGroupItem struct {
	GroupID   string `xml:"groupId"`
	GroupName string `xml:"groupName"`
}

type instanceGroupSet struct {
	Items []instanceGroupItem `xml:"item"`
}

type instanceItem struct {
	LaunchTime       string             `xml:"launchTime"`
	InstanceID       string             `xml:"instanceId"`
	ImageID          string             `xml:"imageId"`
	InstanceType     string             `xml:"instanceType"`
	VPCID            string             `xml:"vpcId,omitempty"`
	SubnetID         string             `xml:"subnetId,omitempty"`
	PrivateIPAddress string             `xml:"privateIpAddress,omitempty"`
	PublicIPAddress  string             `xml:"ipAddress,omitempty"`
	PublicDNSName    string             `xml:"dnsName,omitempty"`
	KeyName          string             `xml:"keyName,omitempty"`
	StateItem        stateItem          `xml:"instanceState"`
	GroupSet         instanceGroupSet   `xml:"groupSet"`
	TagSet           instanceTagItemSet `xml:"tagSet"`
}

// instanceTagItem is the embedded per-instance tag entry in DescribeInstances
// XML (no resourceId/resourceType fields, only key/value).
type instanceTagItem struct {
	Key   string `xml:"key"`
	Value string `xml:"value"`
}

type instanceTagItemSet struct {
	Items []instanceTagItem `xml:"item"`
}

type instanceItemSet struct {
	Items []instanceItem `xml:"item"`
}

type runInstancesResponse struct {
	XMLName       xml.Name        `xml:"RunInstancesResponse"`
	Xmlns         string          `xml:"xmlns,attr"`
	RequestID     string          `xml:"requestId"`
	ReservationID string          `xml:"reservationId"`
	OwnerID       string          `xml:"ownerId"`
	InstancesSet  instanceItemSet `xml:"instancesSet"`
}

type reservationItem struct {
	ReservationID string          `xml:"reservationId"`
	OwnerID       string          `xml:"ownerId"`
	InstancesSet  instanceItemSet `xml:"instancesSet"`
}

type reservationSet struct {
	Items []reservationItem `xml:"item"`
}

type describeInstancesResponse struct {
	XMLName        xml.Name       `xml:"DescribeInstancesResponse"`
	Xmlns          string         `xml:"xmlns,attr"`
	RequestID      string         `xml:"requestId"`
	NextToken      string         `xml:"nextToken,omitempty"`
	ReservationSet reservationSet `xml:"reservationSet"`
}

type instanceStateChangeItem struct {
	InstanceID    string    `xml:"instanceId"`
	CurrentState  stateItem `xml:"currentState"`
	PreviousState stateItem `xml:"previousState"`
}

type instanceStateChangeSet struct {
	Items []instanceStateChangeItem `xml:"item"`
}

type terminateInstancesResponse struct {
	XMLName      xml.Name               `xml:"TerminateInstancesResponse"`
	Xmlns        string                 `xml:"xmlns,attr"`
	RequestID    string                 `xml:"requestId"`
	InstancesSet instanceStateChangeSet `xml:"instancesSet"`
}

type sgItem struct {
	GroupID          string `xml:"groupId"`
	GroupName        string `xml:"groupName"`
	GroupDescription string `xml:"groupDescription"`
	VPCID            string `xml:"vpcId,omitempty"`
}

type sgItemSet struct {
	Items []sgItem `xml:"item"`
}

type describeSecurityGroupsResponse struct {
	XMLName           xml.Name  `xml:"DescribeSecurityGroupsResponse"`
	Xmlns             string    `xml:"xmlns,attr"`
	RequestID         string    `xml:"requestId"`
	SecurityGroupInfo sgItemSet `xml:"securityGroupInfo"`
}

type createSecurityGroupResponse struct {
	XMLName   xml.Name `xml:"CreateSecurityGroupResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	GroupID   string   `xml:"groupId"`
	Return    bool     `xml:"return"`
}

type deleteSecurityGroupResponse struct {
	XMLName   xml.Name `xml:"DeleteSecurityGroupResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type vpcItem struct {
	VpcID     string `xml:"vpcId"`
	CIDRBlock string `xml:"cidrBlock"`
	IsDefault string `xml:"isDefault"`
	State     string `xml:"state"`
}

type vpcItemSet struct {
	Items []vpcItem `xml:"item"`
}

type describeVpcsResponse struct {
	XMLName   xml.Name   `xml:"DescribeVpcsResponse"`
	Xmlns     string     `xml:"xmlns,attr"`
	RequestID string     `xml:"requestId"`
	VpcSet    vpcItemSet `xml:"vpcSet"`
}

type createVpcResponse struct {
	XMLName   xml.Name `xml:"CreateVpcResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Vpc       vpcItem  `xml:"vpc"`
}

type subnetItem struct {
	SubnetID         string `xml:"subnetId"`
	VPCID            string `xml:"vpcId"`
	CIDRBlock        string `xml:"cidrBlock"`
	AvailabilityZone string `xml:"availabilityZone"`
	State            string `xml:"state"`
}

type subnetItemSet struct {
	Items []subnetItem `xml:"item"`
}

type describeSubnetsResponse struct {
	XMLName   xml.Name      `xml:"DescribeSubnetsResponse"`
	Xmlns     string        `xml:"xmlns,attr"`
	RequestID string        `xml:"requestId"`
	SubnetSet subnetItemSet `xml:"subnetSet"`
}

type createSubnetResponse struct {
	XMLName   xml.Name   `xml:"CreateSubnetResponse"`
	Xmlns     string     `xml:"xmlns,attr"`
	RequestID string     `xml:"requestId"`
	Subnet    subnetItem `xml:"subnet"`
}

type deleteVpcResponse struct {
	XMLName   xml.Name `xml:"DeleteVpcResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type deleteSubnetResponse struct {
	XMLName   xml.Name `xml:"DeleteSubnetResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type revokeSecurityGroupEgressResponse struct {
	XMLName   xml.Name `xml:"RevokeSecurityGroupEgressResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    string   `xml:"return"`
}

type instanceTypeItem struct {
	InstanceType string `xml:"instanceType"`
}

type instanceTypeSet struct {
	Items []instanceTypeItem `xml:"item"`
}

type describeInstanceTypesResponse struct {
	XMLName       xml.Name        `xml:"DescribeInstanceTypesResponse"`
	Xmlns         string          `xml:"xmlns,attr"`
	RequestID     string          `xml:"requestId"`
	NextToken     string          `xml:"nextToken,omitempty"`
	InstanceTypes instanceTypeSet `xml:"instanceTypeSet"`
}

type tagItem struct {
	ResourceID   string `xml:"resourceId"`
	ResourceType string `xml:"resourceType"`
	Key          string `xml:"key"`
	Value        string `xml:"value"`
}

type tagItemSet struct {
	Items []tagItem `xml:"item"`
}

type describeTagsResponse struct {
	XMLName   xml.Name   `xml:"DescribeTagsResponse"`
	Xmlns     string     `xml:"xmlns,attr"`
	RequestID string     `xml:"requestId"`
	TagSet    tagItemSet `xml:"tagSet"`
}

type createTagsResponse struct {
	XMLName   xml.Name `xml:"CreateTagsResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    string   `xml:"return"`
}

type deleteTagsResponse struct {
	XMLName   xml.Name `xml:"DeleteTagsResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    string   `xml:"return"`
}

type launchTemplateItem struct {
	ID                   string `xml:"launchTemplateId"`
	Name                 string `xml:"launchTemplateName"`
	CreateTime           string `xml:"createTime"`
	CreatedBy            string `xml:"createdBy"`
	DefaultVersionNumber int64  `xml:"defaultVersionNumber"`
	LatestVersionNumber  int64  `xml:"latestVersionNumber"`
}

type launchTemplateSet struct {
	Items []launchTemplateItem `xml:"item"`
}

type describeLaunchTemplatesResponse struct {
	XMLName           xml.Name          `xml:"DescribeLaunchTemplatesResponse"`
	Xmlns             string            `xml:"xmlns,attr"`
	RequestID         string            `xml:"requestId"`
	LaunchTemplateSet launchTemplateSet `xml:"launchTemplates"`
}

// namedStringAttr is a string attribute element whose XML element name is set dynamically.
// Used for DescribeInstanceAttribute where the attribute name becomes the element name.
type namedStringAttr struct {
	XMLName xml.Name
	Value   string `xml:"value"`
}

type describeInstanceAttributeResponse struct {
	XMLName    xml.Name `xml:"DescribeInstanceAttributeResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	RequestID  string   `xml:"requestId"`
	InstanceID string   `xml:"instanceId"`
	Attribute  namedStringAttr
}
