package ec2

import (
	"encoding/xml"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
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
	Backend   *InMemoryBackend
	Logger    *slog.Logger
	AccountID string
	Region    string
}

// NewHandler creates a new EC2 handler with the given backend and logger.
func NewHandler(backend *InMemoryBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log}
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
		"DescribeSecurityGroups",
		"CreateSecurityGroup",
		"DeleteSecurityGroup",
		"DescribeVpcs",
		"DescribeSubnets",
		"CreateVpc",
		"CreateSubnet",
	}
}

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

		body, err := httputil.ReadBody(r)
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

	for _, key := range []string{"InstanceId.1", "GroupId.1", "GroupId", "VpcId.1", "VpcId", "SubnetId.1", "SubnetId"} {
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

// dispatch routes the EC2 action to the appropriate handler function.
func (h *Handler) dispatch(action string, vals url.Values, reqID string) (any, error) {
	switch action {
	case "RunInstances":
		return h.handleRunInstances(vals, reqID)
	case "DescribeInstances":
		return h.handleDescribeInstances(vals, reqID)
	case "TerminateInstances":
		return h.handleTerminateInstances(vals, reqID)
	case "DescribeSecurityGroups":
		return h.handleDescribeSecurityGroups(vals, reqID)
	case "CreateSecurityGroup":
		return h.handleCreateSecurityGroup(vals, reqID)
	case "DeleteSecurityGroup":
		return h.handleDeleteSecurityGroup(vals, reqID)
	case "DescribeVpcs":
		return h.handleDescribeVpcs(vals, reqID)
	case "DescribeSubnets":
		return h.handleDescribeSubnets(vals, reqID)
	case "CreateVpc":
		return h.handleCreateVpc(vals, reqID)
	case "CreateSubnet":
		return h.handleCreateSubnet(vals, reqID)
	default:
		return nil, fmt.Errorf("%w: %s is not a supported EC2 action", ErrInvalidParameter, action)
	}
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

	instances, err := h.Backend.TerminateInstances(ids)
	if err != nil {
		return nil, err
	}

	items := make([]instanceStateChangeItem, 0, len(instances))
	for _, inst := range instances {
		items = append(items, instanceStateChangeItem{
			InstanceID:    inst.ID,
			CurrentState:  stateItem{Code: inst.State.Code, Name: inst.State.Name},
			PreviousState: stateItem(StateRunning),
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

func (h *Handler) handleCreateVpc(vals url.Values, reqID string) (any, error) {
	cidr := vals.Get("CidrBlock")

	v, err := h.Backend.CreateVpc(cidr)
	if err != nil {
		return nil, err
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

	return &createSubnetResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Subnet:    toSubnetItem(s),
	}, nil
}

// ---- error handling ----

func (h *Handler) handleOpError(c *echo.Context, reqID, action string, opErr error) error {
	statusCode := http.StatusBadRequest

	var code string

	switch {
	case errors.Is(opErr, ErrInstanceNotFound):
		code = "InvalidInstanceID.NotFound"
	case errors.Is(opErr, ErrSecurityGroupNotFound):
		code = "InvalidGroup.NotFound"
	case errors.Is(opErr, ErrVPCNotFound):
		code = "InvalidVpcID.NotFound"
	case errors.Is(opErr, ErrSubnetNotFound):
		code = "InvalidSubnetID.NotFound"
	case errors.Is(opErr, ErrDuplicateSGName):
		code = "InvalidGroup.Duplicate"
	case errors.Is(opErr, ErrInvalidParameter):
		code = "InvalidParameterValue"
	default:
		code = "InternalFailure"
		statusCode = http.StatusInternalServerError
		h.Logger.Error("EC2 internal error", "error", opErr, "action", action)
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
		InstanceID:   inst.ID,
		ImageID:      inst.ImageID,
		InstanceType: inst.InstanceType,
		StateItem:    stateItem{Code: inst.State.Code, Name: inst.State.Name},
		VPCID:        inst.VPCID,
		SubnetID:     inst.SubnetID,
		LaunchTime:   inst.LaunchTime.Format("2006-01-02T15:04:05.000Z"),
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
	}
}

func toSubnetItem(s *Subnet) subnetItem {
	return subnetItem{
		SubnetID:         s.ID,
		VPCID:            s.VPCID,
		CIDRBlock:        s.CIDRBlock,
		AvailabilityZone: s.AvailabilityZone,
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
	LaunchTime   string    `xml:"launchTime"`
	InstanceID   string    `xml:"instanceId"`
	ImageID      string    `xml:"imageId"`
	InstanceType string    `xml:"instanceType"`
	VPCID        string    `xml:"vpcId,omitempty"`
	SubnetID     string    `xml:"subnetId,omitempty"`
	StateItem    stateItem `xml:"instanceState"`
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
