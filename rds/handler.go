package rds

import (
	"encoding/xml"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	rdsVersion       = "2014-10-31"
	rdsXMLNS         = "http://rds.amazonaws.com/doc/2014-10-31/"
	rdsMatchPriority = 84
)

// Handler is the Echo HTTP handler for RDS operations.
type Handler struct {
	Backend *InMemoryBackend
	Logger  *slog.Logger
}

// NewHandler creates a new RDS handler.
func NewHandler(backend *InMemoryBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log}
}

// Name returns the service name.
func (h *Handler) Name() string { return "RDS" }

// GetSupportedOperations returns supported RDS operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateDBInstance",
		"DeleteDBInstance",
		"DescribeDBInstances",
		"ModifyDBInstance",
		"CreateDBSnapshot",
		"DescribeDBSnapshots",
		"DeleteDBSnapshot",
		"CreateDBSubnetGroup",
		"DescribeDBSubnetGroups",
		"DeleteDBSubnetGroup",
	}
}

// RouteMatcher returns a function that matches RDS requests.
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

		return vals.Get("Version") == rdsVersion
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return rdsMatchPriority }

// ExtractOperation extracts the RDS action from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return "Unknown"
	}

	action := r.Form.Get("Action")
	if action == "" {
		return "Unknown"
	}

	return action
}

// ExtractResource returns the DB instance identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return ""
	}

	return r.Form.Get("DBInstanceIdentifier")
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		if err := r.ParseForm(); err != nil {
			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "failed to read request body")
		}

		vals := r.Form
		action := vals.Get("Action")
		if action == "" {
			return h.writeError(c, http.StatusBadRequest, "MissingAction", "missing Action parameter")
		}

		resp, opErr := h.dispatch(action, vals)
		if opErr != nil {
			return h.handleOpError(c, action, opErr)
		}

		xmlBytes, err := marshalXML(resp)
		if err != nil {
			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "internal server error")
		}

		return c.Blob(http.StatusOK, "text/xml", xmlBytes)
	}
}

// dispatch routes the RDS action to the appropriate handler.
func (h *Handler) dispatch(action string, vals url.Values) (any, error) {
	switch action {
	case "CreateDBInstance":
		return h.handleCreateDBInstance(vals)
	case "DeleteDBInstance":
		return h.handleDeleteDBInstance(vals)
	case "DescribeDBInstances":
		return h.handleDescribeDBInstances(vals)
	case "ModifyDBInstance":
		return h.handleModifyDBInstance(vals)
	case "CreateDBSnapshot":
		return h.handleCreateDBSnapshot(vals)
	case "DescribeDBSnapshots":
		return h.handleDescribeDBSnapshots(vals)
	case "DeleteDBSnapshot":
		return h.handleDeleteDBSnapshot(vals)
	case "CreateDBSubnetGroup":
		return h.handleCreateDBSubnetGroup(vals)
	case "DescribeDBSubnetGroups":
		return h.handleDescribeDBSubnetGroups(vals)
	case "DeleteDBSubnetGroup":
		return h.handleDeleteDBSubnetGroup(vals)
	default:
		return nil, fmt.Errorf("%w: %s is not a valid RDS action", ErrUnknownAction, action)
	}
}

func (h *Handler) handleCreateDBInstance(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	engine := vals.Get("Engine")
	instanceClass := vals.Get("DBInstanceClass")
	dbName := vals.Get("DBName")
	masterUser := vals.Get("MasterUsername")

	allocatedStorage, _ := strconv.Atoi(vals.Get("AllocatedStorage"))

	inst, err := h.Backend.CreateDBInstance(id, engine, instanceClass, dbName, masterUser, allocatedStorage)
	if err != nil {
		return nil, err
	}

	return &createDBInstanceResponse{
		Xmlns:      rdsXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleDeleteDBInstance(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")

	inst, err := h.Backend.DeleteDBInstance(id)
	if err != nil {
		return nil, err
	}

	return &deleteDBInstanceResponse{
		Xmlns:      rdsXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleDescribeDBInstances(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")

	instances, err := h.Backend.DescribeDBInstances(id)
	if err != nil {
		return nil, err
	}

	members := make([]xmlDBInstance, 0, len(instances))
	for _, inst := range instances {
		cp := inst
		members = append(members, toXMLInstance(&cp))
	}

	return &describeDBInstancesResponse{
		Xmlns:       rdsXMLNS,
		DBInstances: xmlDBInstanceList{Members: members},
	}, nil
}

func (h *Handler) handleModifyDBInstance(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	instanceClass := vals.Get("DBInstanceClass")
	allocatedStorage, _ := strconv.Atoi(vals.Get("AllocatedStorage"))

	inst, err := h.Backend.ModifyDBInstance(id, instanceClass, allocatedStorage)
	if err != nil {
		return nil, err
	}

	return &modifyDBInstanceResponse{
		Xmlns:      rdsXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleCreateDBSnapshot(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBSnapshotIdentifier")
	instanceID := vals.Get("DBInstanceIdentifier")

	snap, err := h.Backend.CreateDBSnapshot(snapshotID, instanceID)
	if err != nil {
		return nil, err
	}

	return &createDBSnapshotResponse{
		Xmlns:      rdsXMLNS,
		DBSnapshot: toXMLSnapshot(snap),
	}, nil
}

func (h *Handler) handleDescribeDBSnapshots(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBSnapshotIdentifier")

	snaps, err := h.Backend.DescribeDBSnapshots(snapshotID)
	if err != nil {
		return nil, err
	}

	members := make([]xmlDBSnapshot, 0, len(snaps))
	for _, snap := range snaps {
		cp := snap
		members = append(members, toXMLSnapshot(&cp))
	}

	return &describeDBSnapshotsResponse{
		Xmlns:       rdsXMLNS,
		DBSnapshots: xmlDBSnapshotList{Members: members},
	}, nil
}

func (h *Handler) handleDeleteDBSnapshot(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBSnapshotIdentifier")

	snap, err := h.Backend.DeleteDBSnapshot(snapshotID)
	if err != nil {
		return nil, err
	}

	return &deleteDBSnapshotResponse{
		Xmlns:      rdsXMLNS,
		DBSnapshot: toXMLSnapshot(snap),
	}, nil
}

func (h *Handler) handleCreateDBSubnetGroup(vals url.Values) (any, error) {
	name := vals.Get("DBSubnetGroupName")
	description := vals.Get("DBSubnetGroupDescription")
	vpcID := vals.Get("VpcId")

	var subnetIDs []string

	for i := 1; ; i++ {
		sid := vals.Get(fmt.Sprintf("SubnetIds.member.%d", i))
		if sid == "" {
			break
		}
		subnetIDs = append(subnetIDs, sid)
	}

	sg, err := h.Backend.CreateDBSubnetGroup(name, description, vpcID, subnetIDs)
	if err != nil {
		return nil, err
	}

	return &createDBSubnetGroupResponse{
		Xmlns:         rdsXMLNS,
		DBSubnetGroup: toXMLSubnetGroup(sg),
	}, nil
}

func (h *Handler) handleDescribeDBSubnetGroups(vals url.Values) (any, error) {
	name := vals.Get("DBSubnetGroupName")

	sgs, err := h.Backend.DescribeDBSubnetGroups(name)
	if err != nil {
		return nil, err
	}

	members := make([]xmlDBSubnetGroup, 0, len(sgs))
	for _, sg := range sgs {
		cp := sg
		members = append(members, toXMLSubnetGroup(&cp))
	}

	return &describeDBSubnetGroupsResponse{
		Xmlns:          rdsXMLNS,
		DBSubnetGroups: xmlDBSubnetGroupList{Members: members},
	}, nil
}

func (h *Handler) handleDeleteDBSubnetGroup(vals url.Values) (any, error) {
	name := vals.Get("DBSubnetGroupName")

	if err := h.Backend.DeleteDBSubnetGroup(name); err != nil {
		return nil, err
	}

	return &deleteDBSubnetGroupResponse{
		Xmlns: rdsXMLNS,
	}, nil
}

func toXMLInstance(inst *DBInstance) xmlDBInstance {
	return xmlDBInstance{
		DBInstanceIdentifier: inst.DBInstanceIdentifier,
		DBInstanceClass:      inst.DBInstanceClass,
		Engine:               inst.Engine,
		DBInstanceStatus:     inst.DBInstanceStatus,
		MasterUsername:       inst.MasterUsername,
		DBName:               inst.DBName,
		Endpoint:             inst.Endpoint,
		Port:                 inst.Port,
		AllocatedStorage:     inst.AllocatedStorage,
		VpcID:                inst.VpcID,
		DBSubnetGroupName:    inst.DBSubnetGroupName,
	}
}

func toXMLSnapshot(snap *DBSnapshot) xmlDBSnapshot {
	return xmlDBSnapshot{
		DBSnapshotIdentifier: snap.DBSnapshotIdentifier,
		DBInstanceIdentifier: snap.DBInstanceIdentifier,
		Engine:               snap.Engine,
		Status:               snap.Status,
	}
}

func toXMLSubnetGroup(sg *DBSubnetGroup) xmlDBSubnetGroup {
	subnetMembers := make([]xmlSubnet, 0, len(sg.SubnetIDs))
	for _, id := range sg.SubnetIDs {
		subnetMembers = append(subnetMembers, xmlSubnet{SubnetIdentifier: id})
	}

	return xmlDBSubnetGroup{
		DBSubnetGroupName:        sg.DBSubnetGroupName,
		DBSubnetGroupDescription: sg.DBSubnetGroupDescription,
		VpcID:                    sg.VpcID,
		SubnetGroupStatus:        sg.Status,
		Subnets:                  xmlSubnetList{Members: subnetMembers},
	}
}

func (h *Handler) handleOpError(c *echo.Context, action string, opErr error) error {
	statusCode := http.StatusBadRequest

	var code string

	switch {
	case errors.Is(opErr, ErrInstanceNotFound):
		code = "DBInstanceNotFound"
	case errors.Is(opErr, ErrInstanceAlreadyExists):
		code = "DBInstanceAlreadyExists"
	case errors.Is(opErr, ErrSnapshotNotFound):
		code = "DBSnapshotNotFound"
	case errors.Is(opErr, ErrSnapshotAlreadyExists):
		code = "DBSnapshotAlreadyExists"
	case errors.Is(opErr, ErrSubnetGroupNotFound):
		code = "DBSubnetGroupNotFound"
	case errors.Is(opErr, ErrSubnetGroupAlreadyExists):
		code = "DBSubnetGroupAlreadyExists"
	case errors.Is(opErr, ErrInvalidParameter):
		code = "InvalidParameterValue"
	case errors.Is(opErr, ErrUnknownAction):
		code = "InvalidAction"
	default:
		code = "InternalFailure"
		statusCode = http.StatusInternalServerError
		h.Logger.Error("RDS internal error", "error", opErr, "action", action)
	}

	return h.writeError(c, statusCode, code, opErr.Error())
}

func (h *Handler) writeError(c *echo.Context, statusCode int, code, message string) error {
	errResp := &rdsErrorResponse{
		Xmlns: rdsXMLNS,
		Error: rdsError{Code: code, Message: message, Type: "Sender"},
	}

	xmlBytes, err := marshalXML(errResp)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	return c.Blob(statusCode, "text/xml", xmlBytes)
}

func marshalXML(v any) ([]byte, error) {
	raw, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}

	return append([]byte(xml.Header), raw...), nil
}

// ---- XML response types ----

type rdsError struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
	Type    string `xml:"Type"`
}

type rdsErrorResponse struct {
	XMLName xml.Name `xml:"ErrorResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Error   rdsError `xml:"Error"`
}

type xmlDBInstance struct {
	DBInstanceIdentifier string `xml:"DBInstanceIdentifier"`
	DBInstanceClass      string `xml:"DBInstanceClass"`
	Engine               string `xml:"Engine"`
	DBInstanceStatus     string `xml:"DBInstanceStatus"`
	MasterUsername       string `xml:"MasterUsername"`
	DBName               string `xml:"DBName"`
	Endpoint             string `xml:"Endpoint>Address"`
	VpcID                string `xml:"DBSubnetGroup>VpcId,omitempty"`
	DBSubnetGroupName    string `xml:"DBSubnetGroup>DBSubnetGroupName,omitempty"`
	Port                 int    `xml:"Endpoint>Port"`
	AllocatedStorage     int    `xml:"AllocatedStorage"`
}

type xmlDBInstanceList struct {
	Members []xmlDBInstance `xml:"DBInstance"`
}

type createDBInstanceResponse struct {
	XMLName    xml.Name      `xml:"CreateDBInstanceResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"CreateDBInstanceResult>DBInstance"`
}

type deleteDBInstanceResponse struct {
	XMLName    xml.Name      `xml:"DeleteDBInstanceResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"DeleteDBInstanceResult>DBInstance"`
}

type describeDBInstancesResponse struct {
	XMLName     xml.Name          `xml:"DescribeDBInstancesResponse"`
	Xmlns       string            `xml:"xmlns,attr"`
	DBInstances xmlDBInstanceList `xml:"DescribeDBInstancesResult>DBInstances"`
}

type modifyDBInstanceResponse struct {
	XMLName    xml.Name      `xml:"ModifyDBInstanceResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"ModifyDBInstanceResult>DBInstance"`
}

type xmlDBSnapshot struct {
	DBSnapshotIdentifier string `xml:"DBSnapshotIdentifier"`
	DBInstanceIdentifier string `xml:"DBInstanceIdentifier"`
	Engine               string `xml:"Engine"`
	Status               string `xml:"Status"`
}

type xmlDBSnapshotList struct {
	Members []xmlDBSnapshot `xml:"DBSnapshot"`
}

type createDBSnapshotResponse struct {
	XMLName    xml.Name      `xml:"CreateDBSnapshotResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBSnapshot xmlDBSnapshot `xml:"CreateDBSnapshotResult>DBSnapshot"`
}

type deleteDBSnapshotResponse struct {
	XMLName    xml.Name      `xml:"DeleteDBSnapshotResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBSnapshot xmlDBSnapshot `xml:"DeleteDBSnapshotResult>DBSnapshot"`
}

type describeDBSnapshotsResponse struct {
	XMLName     xml.Name          `xml:"DescribeDBSnapshotsResponse"`
	Xmlns       string            `xml:"xmlns,attr"`
	DBSnapshots xmlDBSnapshotList `xml:"DescribeDBSnapshotsResult>DBSnapshots"`
}

type xmlSubnet struct {
	SubnetIdentifier string `xml:"SubnetIdentifier"`
}

type xmlSubnetList struct {
	Members []xmlSubnet `xml:"Subnet"`
}

type xmlDBSubnetGroup struct {
	DBSubnetGroupName        string        `xml:"DBSubnetGroupName"`
	DBSubnetGroupDescription string        `xml:"DBSubnetGroupDescription"`
	VpcID                    string        `xml:"VpcId"`
	SubnetGroupStatus        string        `xml:"SubnetGroupStatus"`
	Subnets                  xmlSubnetList `xml:"Subnets"`
}

type xmlDBSubnetGroupList struct {
	Members []xmlDBSubnetGroup `xml:"DBSubnetGroup"`
}

type createDBSubnetGroupResponse struct {
	XMLName       xml.Name         `xml:"CreateDBSubnetGroupResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	DBSubnetGroup xmlDBSubnetGroup `xml:"CreateDBSubnetGroupResult>DBSubnetGroup"`
}

type deleteDBSubnetGroupResponse struct {
	XMLName xml.Name `xml:"DeleteDBSubnetGroupResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type describeDBSubnetGroupsResponse struct {
	XMLName        xml.Name             `xml:"DescribeDBSubnetGroupsResponse"`
	Xmlns          string               `xml:"xmlns,attr"`
	DBSubnetGroups xmlDBSubnetGroupList `xml:"DescribeDBSubnetGroupsResult>DBSubnetGroups"`
}
