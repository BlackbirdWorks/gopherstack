package ec2

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

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
	Backend   Backend
	AccountID string
	Region    string
}

// NewHandler creates a new EC2 handler with the given backend.
func NewHandler(backend Backend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string {
	return "EC2"
}

// GetSupportedOperations returns the list of supported EC2 operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
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
		"CreateSubnet",
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
		"RevokeSecurityGroupEgress",
		"DescribeInstanceTypes",
		"DescribeTags",
		"CreateTags",
		"DeleteTags",
		"DescribeInstanceAttribute",
		"DescribeImageAttribute",
	}
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
			return h.writeError(c, reqID, http.StatusBadRequest, "MissingAction", "missing Action parameter")
		}

		log.DebugContext(ctx, "EC2 request", "action", action)

		resp, opErr := h.dispatch(action, r.Form, reqID)
		if opErr != nil {
			return h.handleOpError(c, reqID, action, opErr)
		}

		xmlBytes, marshalErr := marshalXML(resp)
		if marshalErr != nil {
			log.ErrorContext(ctx, "failed to marshal EC2 response", "action", action, "error", marshalErr)

			return h.writeError(c, reqID, http.StatusInternalServerError, "InternalFailure", "internal server error")
		}

		return c.Blob(http.StatusOK, "text/xml", xmlBytes)
	}
}

type ec2ActionFn func(vals url.Values, reqID string) (any, error)

func (h *Handler) dispatchTable() map[string]ec2ActionFn {
	return map[string]ec2ActionFn{
		"RunInstances":                  h.handleRunInstances,
		"DescribeInstances":             h.handleDescribeInstances,
		"TerminateInstances":            h.handleTerminateInstances,
		"DescribeSecurityGroups":        h.handleDescribeSecurityGroups,
		"CreateSecurityGroup":           h.handleCreateSecurityGroup,
		"DeleteSecurityGroup":           h.handleDeleteSecurityGroup,
		"RevokeSecurityGroupEgress":     h.handleRevokeSecurityGroupEgress,
		"DescribeVpcs":                  h.handleDescribeVpcs,
		"DescribeVpcAttribute":          h.handleDescribeVpcAttribute,
		"DescribeSubnets":               h.handleDescribeSubnets,
		"CreateVpc":                     h.handleCreateVpc,
		"CreateSubnet":                  h.handleCreateSubnet,
		"DescribeInstanceTypes":         h.handleDescribeInstanceTypes,
		"DescribeTags":                  h.handleDescribeTags,
		"CreateTags":                    h.handleCreateTags,
		"DeleteTags":                    h.handleDeleteTags,
		"DescribeInstanceAttribute":     h.handleDescribeInstanceAttribute,
		"StartInstances":                h.handleStartInstances,
		"StopInstances":                 h.handleStopInstances,
		"RebootInstances":               h.handleRebootInstances,
		"DescribeInstanceStatus":        h.handleDescribeInstanceStatus,
		"DescribeImages":                h.handleDescribeImages,
		"DescribeRegions":               h.handleDescribeRegions,
		"DescribeAvailabilityZones":     h.handleDescribeAvailabilityZones,
		"CreateKeyPair":                 h.handleCreateKeyPair,
		"DescribeKeyPairs":              h.handleDescribeKeyPairs,
		"DeleteKeyPair":                 h.handleDeleteKeyPair,
		"ImportKeyPair":                 h.handleImportKeyPair,
		"CreateVolume":                  h.handleCreateVolume,
		"DescribeVolumes":               h.handleDescribeVolumes,
		"DeleteVolume":                  h.handleDeleteVolume,
		"AttachVolume":                  h.handleAttachVolume,
		"DetachVolume":                  h.handleDetachVolume,
		"AllocateAddress":               h.handleAllocateAddress,
		"AssociateAddress":              h.handleAssociateAddress,
		"DisassociateAddress":           h.handleDisassociateAddress,
		"ReleaseAddress":                h.handleReleaseAddress,
		"DescribeAddresses":             h.handleDescribeAddresses,
		"CreateInternetGateway":         h.handleCreateInternetGateway,
		"DeleteInternetGateway":         h.handleDeleteInternetGateway,
		"DescribeInternetGateways":      h.handleDescribeInternetGateways,
		"AttachInternetGateway":         h.handleAttachInternetGateway,
		"DetachInternetGateway":         h.handleDetachInternetGateway,
		"CreateRouteTable":              h.handleCreateRouteTable,
		"DeleteRouteTable":              h.handleDeleteRouteTable,
		"DescribeRouteTables":           h.handleDescribeRouteTables,
		"CreateRoute":                   h.handleCreateRoute,
		"DeleteRoute":                   h.handleDeleteRoute,
		"AssociateRouteTable":           h.handleAssociateRouteTable,
		"DisassociateRouteTable":        h.handleDisassociateRouteTable,
		"CreateNatGateway":              h.handleCreateNatGateway,
		"DeleteNatGateway":              h.handleDeleteNatGateway,
		"DescribeNatGateways":           h.handleDescribeNatGateways,
		"DescribeNetworkInterfaces":     h.handleDescribeNetworkInterfaces,
		"AuthorizeSecurityGroupIngress": h.handleAuthorizeSecurityGroupIngress,
		"AuthorizeSecurityGroupEgress":  h.handleAuthorizeSecurityGroupEgress,
		"RevokeSecurityGroupIngress":    h.handleRevokeSecurityGroupIngress,
		"DescribeImageAttribute":        h.handleDescribeImageAttribute,
	}
}

// dispatch routes the EC2 action to the appropriate handler function.
func (h *Handler) dispatch(action string, vals url.Values, reqID string) (any, error) {
	fn, ok := h.dispatchTable()[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s is not a supported EC2 action", ErrInvalidParameter, action)
	}

	return fn(vals, reqID)
}

// ---- action handlers ----

func (h *Handler) handleRunInstances(vals url.Values, reqID string) (any, error) {
	imageID := vals.Get("ImageId")
	instanceType := vals.Get("InstanceType")
	subnetID := vals.Get("SubnetId")

	count := 1
	if v := vals.Get("MinCount"); v != "" {
		fmt.Sscan(v, &count) //nolint:errcheck,gosec // parse best-effort; invalid values fall back to 1
	}

	instances, err := h.Backend.RunInstances(imageID, instanceType, subnetID, count)
	if err != nil {
		return nil, err
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
		items = append(items, toInstanceItem(inst))
	}

	return &runInstancesResponse{
		Xmlns:         ec2XMLNS,
		RequestID:     reqID,
		ReservationID: "r-" + uuid.New().String()[:17],
		OwnerID:       h.AccountID,
		InstancesSet:  instanceItemSet{Items: items},
	}, nil
}

func (h *Handler) handleDescribeInstances(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InstanceId")
	state := vals.Get("Filter.1.Value.1")

	instances := h.Backend.DescribeInstances(ids, state)

	items := make([]instanceItem, 0, len(instances))
	for _, inst := range instances {
		items = append(items, toInstanceItem(inst))
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
	}, nil
}

func (h *Handler) handleTerminateInstances(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "InstanceId")
	if len(ids) == 0 {
		return nil, fmt.Errorf("%w: at least one InstanceId is required", ErrInvalidParameter)
	}

	changes, err := h.Backend.TerminateInstances(ids)
	if err != nil {
		return nil, err
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
		Attribute: namedBoolAttr{XMLName: xml.Name{Local: attr}, Value: "false"},
	}, nil
}

func (h *Handler) handleCreateVpc(vals url.Values, reqID string) (any, error) {
	cidr := vals.Get("CidrBlock")

	v, err := h.Backend.CreateVpc(cidr)
	if err != nil {
		return nil, err
	}

	if tags := parseTagSpecification(vals, "vpc"); len(tags) > 0 {
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

// handleRevokeSecurityGroupEgress is a no-op stub.
// Terraform calls this to revoke the default egress rule when creating a security group.
func (h *Handler) handleRevokeSecurityGroupEgress(_ url.Values, reqID string) (any, error) {
	return &revokeSecurityGroupEgressResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    "true",
	}, nil
}

// handleDescribeInstanceTypes returns a minimal stub for the requested instance types.
// Terraform calls this to validate the instance type before launching an instance.
func (h *Handler) handleDescribeInstanceTypes(vals url.Values, reqID string) (any, error) {
	// Return a stub for whatever instance type was requested (e.g., t2.micro).
	instanceType := vals.Get("Filter.1.Value.1")
	if instanceType == "" {
		instanceType = vals.Get("InstanceType.1")
	}
	if instanceType == "" {
		instanceType = "t2.micro"
	}

	return &describeInstanceTypesResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		InstanceTypes: instanceTypeSet{Items: []instanceTypeItem{
			{InstanceType: instanceType},
		}},
	}, nil
}

// handleDescribeTags returns tags for EC2 resources, supporting Filter.N.Name / Filter.N.Value.* semantics.
// If a filter with Name=resource-id is present, only tags for those resource IDs are returned.
// Other filter names are accepted but ignored (returns all tags when no resource-id filter is present).
func (h *Handler) handleDescribeTags(vals url.Values, reqID string) (any, error) {
	var resourceIDs []string

	for i := 1; i <= maxFiltersPerRequest; i++ {
		name := vals.Get(fmt.Sprintf("Filter.%d.Name", i))
		if name == "" {
			break
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
		Return:    "true",
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
		Return:    "true",
	}, nil
}

// handleDescribeInstanceAttribute returns a default value for the requested instance attribute.
// Terraform calls this after RunInstances to read instanceInitiatedShutdownBehavior.
func (h *Handler) handleDescribeInstanceAttribute(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("InstanceId")
	attr := vals.Get("Attribute")

	// Default values match common AWS defaults; the attribute name is the XML element name.
	// Boolean attributes (AttributeBooleanValue) must return "true" or "false" so that
	// strconv.ParseBool succeeds in the SDK deserializer.
	attrValue := "stop"
	switch attr {
	case "disableApiStop", "disableApiTermination", "sourceDestCheck", "ebsOptimized", "enaSupport":
		attrValue = "false"
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
	{ErrInvalidInstanceState, "IncorrectInstanceState"},
	{ErrInvalidParameter, "InvalidParameterValue"},
}

// opErrCode resolves an error to its EC2 API error code and HTTP status code.
func opErrCode(opErr error) (string, int) {
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
		logger.Load(c.Request().Context()).Error("EC2 internal error", "error", opErr, "action", action)
	}

	return h.writeError(c, reqID, statusCode, code, opErr.Error())
}

func (h *Handler) writeError(c *echo.Context, reqID string, statusCode int, code, message string) error {
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
// for a specific resourceType (e.g. "vpc", "subnet", "instance", "security-group").
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
	return fmt.Sprintf("gopherstack-ec2-%s", uuid.New().String())
}

// ---- XML conversion helpers ----

func toInstanceItem(inst *Instance) instanceItem {
	return instanceItem{
		InstanceID:       inst.ID,
		ImageID:          inst.ImageID,
		InstanceType:     inst.InstanceType,
		StateItem:        stateItem{Code: inst.State.Code, Name: inst.State.Name},
		VPCID:            inst.VPCID,
		SubnetID:         inst.SubnetID,
		LaunchTime:       inst.LaunchTime.Format("2006-01-02T15:04:05.000Z"),
		PrivateIPAddress: inst.PrivateIP,
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
	isDefault := "false"
	if v.IsDefault {
		isDefault = "true"
	}

	return vpcItem{
		VpcID:     v.ID,
		CIDRBlock: v.CIDRBlock,
		IsDefault: isDefault,
		State:     "available",
	}
}

func toSubnetItem(s *Subnet) subnetItem {
	return subnetItem{
		SubnetID:         s.ID,
		VPCID:            s.VPCID,
		CIDRBlock:        s.CIDRBlock,
		AvailabilityZone: s.AvailabilityZone,
		State:            "available",
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

type instanceItem struct {
	LaunchTime       string    `xml:"launchTime"`
	InstanceID       string    `xml:"instanceId"`
	ImageID          string    `xml:"imageId"`
	InstanceType     string    `xml:"instanceType"`
	VPCID            string    `xml:"vpcId,omitempty"`
	SubnetID         string    `xml:"subnetId,omitempty"`
	PrivateIPAddress string    `xml:"privateIpAddress,omitempty"`
	StateItem        stateItem `xml:"instanceState"`
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
