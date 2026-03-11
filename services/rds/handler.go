package rds

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	rdsVersion = "2014-10-31"
	rdsXMLNS   = "http://rds.amazonaws.com/doc/2014-10-31/"
)

// Handler is the Echo HTTP handler for RDS operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new RDS handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
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
		"ListTagsForResource",
		"AddTagsToResource",
		"RemoveTagsFromResource",
		"CreateDBParameterGroup",
		"DescribeDBParameterGroups",
		"DeleteDBParameterGroup",
		"ModifyDBParameterGroup",
		"DescribeDBParameters",
		"ResetDBParameterGroup",
		"CreateOptionGroup",
		"DescribeOptionGroups",
		"DeleteOptionGroup",
		"ModifyOptionGroup",
		"DescribeOptionGroupOptions",
		"CreateDBCluster",
		"DescribeDBClusters",
		"DeleteDBCluster",
		"ModifyDBCluster",
		"CreateDBClusterParameterGroup",
		"DescribeDBClusterParameterGroups",
		"CreateDBClusterSnapshot",
		"DescribeDBClusterSnapshots",
		"CreateDBInstanceReadReplica",
		"PromoteReadReplica",
		"RebootDBInstance",
		"DescribeDBEngineVersions",
		"DescribeOrderableDBInstanceOptions",
		"DescribeDBLogFiles",
		"DownloadDBLogFilePortion",
		"DescribeGlobalClusters",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "rds" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this RDS instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

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

		body, err := httputils.ReadBody(r)
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
func (h *Handler) MatchPriority() int { return service.PriorityFormRDS }

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
	case "ListTagsForResource":
		return h.handleListTagsForResource(vals)
	case "AddTagsToResource":
		return h.handleAddTagsToResource(vals)
	case "RemoveTagsFromResource":
		return h.handleRemoveTagsFromResource(vals)
	default:
		return h.dispatchExtended(action, vals)
	}
}

// dispatchExtended routes the first subset of extended RDS actions (parameter groups, option groups,
// and cluster CRUD) to the appropriate handler. See dispatchExtended2 for the remaining actions.
func (h *Handler) dispatchExtended(action string, vals url.Values) (any, error) {
	switch action {
	case "CreateDBParameterGroup":
		return h.handleCreateDBParameterGroup(vals)
	case "DescribeDBParameterGroups":
		return h.handleDescribeDBParameterGroups(vals)
	case "DeleteDBParameterGroup":
		return h.handleDeleteDBParameterGroup(vals)
	case "ModifyDBParameterGroup":
		return h.handleModifyDBParameterGroup(vals)
	case "DescribeDBParameters":
		return h.handleDescribeDBParameters(vals)
	case "ResetDBParameterGroup":
		return h.handleResetDBParameterGroup(vals)
	case "CreateOptionGroup":
		return h.handleCreateOptionGroup(vals)
	case "DescribeOptionGroups":
		return h.handleDescribeOptionGroups(vals)
	case "DeleteOptionGroup":
		return h.handleDeleteOptionGroup(vals)
	case "ModifyOptionGroup":
		return h.handleModifyOptionGroup(vals)
	case "DescribeOptionGroupOptions":
		return h.handleDescribeOptionGroupOptions(vals)
	case "CreateDBCluster":
		return h.handleCreateDBCluster(vals)
	case "DescribeDBClusters":
		return h.handleDescribeDBClusters(vals)
	default:
		return h.dispatchExtended2(action, vals)
	}
}

// dispatchExtended2 routes the remaining extended RDS actions (cluster modifications, snapshots,
// replicas, and misc operations) to the appropriate handler. Split from dispatchExtended to keep
// cyclomatic complexity within limits.
func (h *Handler) dispatchExtended2(action string, vals url.Values) (any, error) {
	switch action {
	case "DeleteDBCluster":
		return h.handleDeleteDBCluster(vals)
	case "ModifyDBCluster":
		return h.handleModifyDBCluster(vals)
	case "CreateDBClusterParameterGroup":
		return h.handleCreateDBClusterParameterGroup(vals)
	case "DescribeDBClusterParameterGroups":
		return h.handleDescribeDBClusterParameterGroups(vals)
	case "CreateDBClusterSnapshot":
		return h.handleCreateDBClusterSnapshot(vals)
	case "DescribeDBClusterSnapshots":
		return h.handleDescribeDBClusterSnapshots(vals)
	case "CreateDBInstanceReadReplica":
		return h.handleCreateDBInstanceReadReplica(vals)
	case "PromoteReadReplica":
		return h.handlePromoteReadReplica(vals)
	case "RebootDBInstance":
		return h.handleRebootDBInstance(vals)
	case "DescribeDBEngineVersions":
		return h.handleDescribeDBEngineVersions(vals)
	case "DescribeOrderableDBInstanceOptions":
		return h.handleDescribeOrderableDBInstanceOptions(vals)
	case "DescribeDBLogFiles":
		return h.handleDescribeDBLogFiles(vals)
	case "DownloadDBLogFilePortion":
		return h.handleDownloadDBLogFilePortion(vals)
	default:
		return h.dispatchExtended3(action, vals)
	}
}

// dispatchExtended3 routes the final set of extended RDS actions. Split from dispatchExtended2 to
// keep cyclomatic complexity within limits.
func (h *Handler) dispatchExtended3(action string, vals url.Values) (any, error) {
	switch action {
	case "DescribeGlobalClusters":
		return h.handleDescribeGlobalClusters(vals)
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
	paramGroupName := vals.Get("DBParameterGroupName")

	rawStorage := vals.Get("AllocatedStorage")
	allocatedStorage := 0

	if rawStorage != "" {
		var err error

		allocatedStorage, err = strconv.Atoi(rawStorage)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid AllocatedStorage %q", ErrInvalidParameter, rawStorage)
		}
	}

	inst, err := h.Backend.CreateDBInstance(
		id,
		engine,
		instanceClass,
		dbName,
		masterUser,
		paramGroupName,
		allocatedStorage,
	)
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

	rawStorage := vals.Get("AllocatedStorage")
	allocatedStorage := 0

	if rawStorage != "" {
		var err error

		allocatedStorage, err = strconv.Atoi(rawStorage)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid AllocatedStorage %q", ErrInvalidParameter, rawStorage)
		}
	}

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
	subnetIDs := parseSubnetIDMembers(vals)

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

func (h *Handler) handleListTagsForResource(vals url.Values) (any, error) {
	arn := vals.Get("ResourceName")
	tags := h.Backend.ListTagsForResource(arn)

	members := make([]svcTags.KV, 0, len(tags))
	for _, t := range tags {
		members = append(members, svcTags.KV(t))
	}

	return &listTagsForResourceResponse{
		Xmlns:   rdsXMLNS,
		TagList: xmlTagList{Members: members},
	}, nil
}

func (h *Handler) handleAddTagsToResource(vals url.Values) (any, error) {
	arn := vals.Get("ResourceName")
	tags := parseTagEntries(vals)
	h.Backend.AddTagsToResource(arn, tags)

	return &addTagsToResourceResponse{Xmlns: rdsXMLNS}, nil
}

func (h *Handler) handleRemoveTagsFromResource(vals url.Values) (any, error) {
	arn := vals.Get("ResourceName")
	keys := parseTagKeyMembers(vals)
	h.Backend.RemoveTagsFromResource(arn, keys)

	return &removeTagsFromResourceResponse{Xmlns: rdsXMLNS}, nil
}

func toXMLInstance(inst *DBInstance) xmlDBInstance {
	result := xmlDBInstance{
		DBInstanceIdentifier:              inst.DBInstanceIdentifier,
		DbiResourceID:                     inst.DbiResourceID,
		DBInstanceClass:                   inst.DBInstanceClass,
		Engine:                            inst.Engine,
		DBInstanceStatus:                  inst.DBInstanceStatus,
		MasterUsername:                    inst.MasterUsername,
		DBName:                            inst.DBName,
		Endpoint:                          inst.Endpoint,
		Port:                              inst.Port,
		AllocatedStorage:                  inst.AllocatedStorage,
		VpcID:                             inst.VpcID,
		DBSubnetGroupName:                 inst.DBSubnetGroupName,
		ReplicaSourceDBInstanceIdentifier: inst.ReplicaSourceDBInstanceIdentifier,
	}

	if inst.DBParameterGroupName != "" {
		result.DBParameterGroups = &xmlDBParamGroupsWrapper{
			Status: &xmlDBParamGroupStatus{DBParameterGroupName: inst.DBParameterGroupName},
		}
	}

	return result
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
	code := rdsErrorCode(opErr)

	if code == "" {
		code = "InternalFailure"
		statusCode = http.StatusInternalServerError
		logger.Load(c.Request().Context()).Error("RDS internal error", "error", opErr, "action", action)
	}

	return h.writeError(c, statusCode, code, opErr.Error())
}

func rdsErrorCode(opErr error) string {
	type errorMapping struct {
		sentinel error
		code     string
	}

	mappings := []errorMapping{
		{ErrInstanceNotFound, "DBInstanceNotFound"},
		{ErrInstanceAlreadyExists, "DBInstanceAlreadyExists"},
		{ErrSnapshotNotFound, "DBSnapshotNotFound"},
		{ErrSnapshotAlreadyExists, "DBSnapshotAlreadyExists"},
		{ErrSubnetGroupNotFound, "DBSubnetGroupNotFoundFault"},
		{ErrSubnetGroupAlreadyExists, "DBSubnetGroupAlreadyExists"},
		{ErrInvalidParameter, "InvalidParameterValue"},
		{ErrUnknownAction, "InvalidAction"},
		{ErrParameterGroupNotFound, "DBParameterGroupNotFound"},
		{ErrParameterGroupAlreadyExists, "DBParameterGroupAlreadyExists"},
		{ErrOptionGroupNotFound, "OptionGroupNotFound"},
		{ErrOptionGroupAlreadyExists, "OptionGroupAlreadyExists"},
		{ErrClusterNotFound, "DBClusterNotFound"},
		{ErrClusterAlreadyExists, "DBClusterAlreadyExists"},
		{ErrClusterSnapshotNotFound, "DBClusterSnapshotNotFound"},
		{ErrClusterSnapshotAlreadyExists, "DBClusterSnapshotAlreadyExists"},
	}

	for _, m := range mappings {
		if errors.Is(opErr, m.sentinel) {
			return m.code
		}
	}

	return ""
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

type xmlDBParamGroupStatus struct {
	DBParameterGroupName string `xml:"DBParameterGroupName,omitempty"`
}

type xmlDBParamGroupsWrapper struct {
	Status *xmlDBParamGroupStatus `xml:"DBParameterGroupStatus,omitempty"`
}

type xmlDBInstance struct {
	DBInstanceIdentifier              string                   `xml:"DBInstanceIdentifier"`
	DbiResourceID                     string                   `xml:"DbiResourceId,omitempty"`
	DBInstanceClass                   string                   `xml:"DBInstanceClass"`
	Engine                            string                   `xml:"Engine"`
	DBInstanceStatus                  string                   `xml:"DBInstanceStatus"`
	MasterUsername                    string                   `xml:"MasterUsername"`
	DBName                            string                   `xml:"DBName"`
	Endpoint                          string                   `xml:"Endpoint>Address"`
	VpcID                             string                   `xml:"DBSubnetGroup>VpcId,omitempty"`
	DBSubnetGroupName                 string                   `xml:"DBSubnetGroup>DBSubnetGroupName,omitempty"`
	DBParameterGroups                 *xmlDBParamGroupsWrapper `xml:"DBParameterGroups,omitempty"`
	ReplicaSourceDBInstanceIdentifier string                   `xml:"ReadReplicaSourceDBInstanceIdentifier,omitempty"`
	Port                              int                      `xml:"Endpoint>Port"`
	AllocatedStorage                  int                      `xml:"AllocatedStorage"`
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

type listTagsForResourceResponse struct {
	XMLName xml.Name   `xml:"ListTagsForResourceResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	TagList xmlTagList `xml:"ListTagsForResourceResult>TagList"`
}

type xmlTagList struct {
	Members []svcTags.KV `xml:"Tag"`
}

type addTagsToResourceResponse struct {
	XMLName xml.Name `xml:"AddTagsToResourceResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type removeTagsFromResourceResponse struct {
	XMLName xml.Name `xml:"RemoveTagsFromResourceResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

// parseSubnetIDMembers parses SubnetIds.member.N form values.
func parseSubnetIDMembers(vals url.Values) []string {
	var ids []string
	for i := 1; ; i++ {
		sid := vals.Get(fmt.Sprintf("SubnetIds.member.%d", i))
		if sid == "" {
			return ids
		}
		ids = append(ids, sid)
	}
}

// parseTagEntries parses Tags.Tag.N.Key/Value form values.
func parseTagEntries(vals url.Values) []Tag {
	var tags []Tag
	for i := 1; ; i++ {
		key := vals.Get(fmt.Sprintf("Tags.Tag.%d.Key", i))
		if key == "" {
			return tags
		}
		tags = append(tags, Tag{Key: key, Value: vals.Get(fmt.Sprintf("Tags.Tag.%d.Value", i))})
	}
}

// parseTagKeyMembers parses TagKeys.member.N form values.
func parseTagKeyMembers(vals url.Values) []string {
	var keys []string
	for i := 1; ; i++ {
		k := vals.Get(fmt.Sprintf("TagKeys.member.%d", i))
		if k == "" {
			return keys
		}
		keys = append(keys, k)
	}
}

// parseParameterMembers parses Parameters.Parameter.N.ParameterName/ParameterValue form values.
func parseParameterMembers(vals url.Values) []DBParameter {
	var params []DBParameter
	for i := 1; ; i++ {
		name := vals.Get(fmt.Sprintf("Parameters.Parameter.%d.ParameterName", i))
		if name == "" {
			return params
		}
		params = append(params, DBParameter{
			ParameterName:  name,
			ParameterValue: vals.Get(fmt.Sprintf("Parameters.Parameter.%d.ParameterValue", i)),
		})
	}
}

// ---- Parameter Group handlers ----

func (h *Handler) handleCreateDBParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	family := vals.Get("DBParameterGroupFamily")
	description := vals.Get("Description")
	pg, err := h.Backend.CreateDBParameterGroup(name, family, description)
	if err != nil {
		return nil, err
	}

	return &createDBParameterGroupResponse{
		Xmlns:            rdsXMLNS,
		DBParameterGroup: toXMLParameterGroup(pg),
	}, nil
}

func (h *Handler) handleDescribeDBParameterGroups(vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	groups, err := h.Backend.DescribeDBParameterGroups(name)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBParameterGroup, 0, len(groups))
	for _, pg := range groups {
		cp := pg
		members = append(members, toXMLParameterGroup(&cp))
	}

	return &describeDBParameterGroupsResponse{
		Xmlns:             rdsXMLNS,
		DBParameterGroups: xmlDBParameterGroupList{Members: members},
	}, nil
}

func (h *Handler) handleDeleteDBParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	if err := h.Backend.DeleteDBParameterGroup(name); err != nil {
		return nil, err
	}

	return &deleteDBParameterGroupResponse{Xmlns: rdsXMLNS}, nil
}

func (h *Handler) handleModifyDBParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	params := parseParameterMembers(vals)
	pg, err := h.Backend.ModifyDBParameterGroup(name, params)
	if err != nil {
		return nil, err
	}

	return &modifyDBParameterGroupResponse{
		Xmlns:                rdsXMLNS,
		DBParameterGroupName: pg.DBParameterGroupName,
	}, nil
}

func (h *Handler) handleDescribeDBParameters(vals url.Values) (any, error) {
	groupName := vals.Get("DBParameterGroupName")
	params, err := h.Backend.DescribeDBParameters(groupName)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBParameter, 0, len(params))
	for _, p := range params {
		members = append(members, xmlDBParameter(p))
	}

	return &describeDBParametersResponse{
		Xmlns:      rdsXMLNS,
		Parameters: xmlDBParameterList{Members: members},
	}, nil
}

func (h *Handler) handleResetDBParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBParameterGroupName")
	resetAll := vals.Get("ResetAllParameters") == "true"
	var paramNames []string
	for i := 1; ; i++ {
		pName := vals.Get(fmt.Sprintf("Parameters.Parameter.%d.ParameterName", i))
		if pName == "" {
			break
		}
		paramNames = append(paramNames, pName)
	}
	pg, err := h.Backend.ResetDBParameterGroup(name, resetAll, paramNames)
	if err != nil {
		return nil, err
	}

	return &resetDBParameterGroupResponse{
		Xmlns:                rdsXMLNS,
		DBParameterGroupName: pg.DBParameterGroupName,
	}, nil
}

// ---- Option Group handlers ----

func (h *Handler) handleCreateOptionGroup(vals url.Values) (any, error) {
	name := vals.Get("OptionGroupName")
	engine := vals.Get("EngineName")
	majorVersion := vals.Get("MajorEngineVersion")
	description := vals.Get("OptionGroupDescription")
	og, err := h.Backend.CreateOptionGroup(name, engine, majorVersion, description)
	if err != nil {
		return nil, err
	}

	return &createOptionGroupResponse{
		Xmlns:       rdsXMLNS,
		OptionGroup: toXMLOptionGroup(og),
	}, nil
}

func (h *Handler) handleDescribeOptionGroups(vals url.Values) (any, error) {
	name := vals.Get("OptionGroupName")
	groups, err := h.Backend.DescribeOptionGroups(name)
	if err != nil {
		return nil, err
	}
	members := make([]xmlOptionGroup, 0, len(groups))
	for _, og := range groups {
		cp := og
		members = append(members, toXMLOptionGroup(&cp))
	}

	return &describeOptionGroupsResponse{
		Xmlns:            rdsXMLNS,
		OptionGroupsList: xmlOptionGroupList{Members: members},
	}, nil
}

func (h *Handler) handleDeleteOptionGroup(vals url.Values) (any, error) {
	name := vals.Get("OptionGroupName")
	if err := h.Backend.DeleteOptionGroup(name); err != nil {
		return nil, err
	}

	return &deleteOptionGroupResponse{Xmlns: rdsXMLNS}, nil
}

func (h *Handler) handleModifyOptionGroup(vals url.Values) (any, error) {
	name := vals.Get("OptionGroupName")
	var optionsToAdd []OptionGroupOption
	for i := 1; ; i++ {
		optName := vals.Get(fmt.Sprintf("OptionsToInclude.OptionConfiguration.%d.OptionName", i))
		if optName == "" {
			break
		}
		optionsToAdd = append(optionsToAdd, OptionGroupOption{
			OptionName:    optName,
			OptionVersion: vals.Get(fmt.Sprintf("OptionsToInclude.OptionConfiguration.%d.OptionVersion", i)),
		})
	}
	var optionsToRemove []string
	for i := 1; ; i++ {
		optName := vals.Get(fmt.Sprintf("OptionsToRemove.member.%d", i))
		if optName == "" {
			break
		}
		optionsToRemove = append(optionsToRemove, optName)
	}
	og, err := h.Backend.ModifyOptionGroup(name, optionsToAdd, optionsToRemove)
	if err != nil {
		return nil, err
	}

	return &modifyOptionGroupResponse{
		Xmlns:       rdsXMLNS,
		OptionGroup: toXMLOptionGroup(og),
	}, nil
}

func (h *Handler) handleDescribeOptionGroupOptions(_ url.Values) (any, error) {
	return &describeOptionGroupOptionsResponse{Xmlns: rdsXMLNS}, nil
}

// ---- Cluster handlers ----

func (h *Handler) handleCreateDBCluster(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	engine := vals.Get("Engine")
	masterUser := vals.Get("MasterUsername")
	dbName := vals.Get("DatabaseName")
	paramGroupName := vals.Get("DBClusterParameterGroupName")
	rawPort := vals.Get("Port")
	port := 0
	if rawPort != "" {
		var err error
		port, err = strconv.Atoi(rawPort)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid Port %q", ErrInvalidParameter, rawPort)
		}
	}
	cluster, err := h.Backend.CreateDBCluster(id, engine, masterUser, dbName, paramGroupName, port)
	if err != nil {
		return nil, err
	}

	return &createDBClusterResponse{
		Xmlns:     rdsXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleDescribeDBClusters(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	clusters, err := h.Backend.DescribeDBClusters(id)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBCluster, 0, len(clusters))
	for _, c := range clusters {
		cp := c
		members = append(members, toXMLCluster(&cp))
	}

	return &describeDBClustersResponse{
		Xmlns:      rdsXMLNS,
		DBClusters: xmlDBClusterList{Members: members},
	}, nil
}

func (h *Handler) handleDeleteDBCluster(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.DeleteDBCluster(id)
	if err != nil {
		return nil, err
	}

	return &deleteDBClusterResponse{
		Xmlns:     rdsXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleModifyDBCluster(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	paramGroupName := vals.Get("DBClusterParameterGroupName")
	cluster, err := h.Backend.ModifyDBCluster(id, paramGroupName)
	if err != nil {
		return nil, err
	}

	return &modifyDBClusterResponse{
		Xmlns:     rdsXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleCreateDBClusterParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	family := vals.Get("DBParameterGroupFamily")
	description := vals.Get("Description")
	pg, err := h.Backend.CreateDBClusterParameterGroup(name, family, description)
	if err != nil {
		return nil, err
	}

	return &createDBClusterParameterGroupResponse{
		Xmlns:            rdsXMLNS,
		DBParameterGroup: toXMLParameterGroup(pg),
	}, nil
}

func (h *Handler) handleDescribeDBClusterParameterGroups(vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	groups, err := h.Backend.DescribeDBClusterParameterGroups(name)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBParameterGroup, 0, len(groups))
	for _, pg := range groups {
		cp := pg
		members = append(members, toXMLParameterGroup(&cp))
	}

	return &describeDBClusterParameterGroupsResponse{
		Xmlns: rdsXMLNS,
		Result: describeDBClusterParameterGroupsResult{
			DBClusterParameterGroups: xmlDBParameterGroupList{Members: members},
		},
	}, nil
}

func (h *Handler) handleCreateDBClusterSnapshot(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	snap, err := h.Backend.CreateDBClusterSnapshot(snapshotID, clusterID)
	if err != nil {
		return nil, err
	}

	return &createDBClusterSnapshotResponse{
		Xmlns:             rdsXMLNS,
		DBClusterSnapshot: toXMLClusterSnapshot(snap),
	}, nil
}

func (h *Handler) handleDescribeDBClusterSnapshots(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	snaps, err := h.Backend.DescribeDBClusterSnapshots(snapshotID)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBClusterSnapshot, 0, len(snaps))
	for _, snap := range snaps {
		cp := snap
		members = append(members, toXMLClusterSnapshot(&cp))
	}

	return &describeDBClusterSnapshotsResponse{
		Xmlns:              rdsXMLNS,
		DBClusterSnapshots: xmlDBClusterSnapshotList{Members: members},
	}, nil
}

// ---- Read Replica handlers ----

func (h *Handler) handleCreateDBInstanceReadReplica(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	sourceID := vals.Get("SourceDBInstanceIdentifier")
	inst, err := h.Backend.CreateDBInstanceReadReplica(id, sourceID)
	if err != nil {
		return nil, err
	}

	return &createDBInstanceReadReplicaResponse{
		Xmlns:      rdsXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handlePromoteReadReplica(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	inst, err := h.Backend.PromoteReadReplica(id)
	if err != nil {
		return nil, err
	}

	return &promoteReadReplicaResponse{
		Xmlns:      rdsXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

// ---- Misc handlers ----

func (h *Handler) handleRebootDBInstance(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	inst, err := h.Backend.RebootDBInstance(id)
	if err != nil {
		return nil, err
	}

	return &rebootDBInstanceResponse{
		Xmlns:      rdsXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleDescribeDBEngineVersions(vals url.Values) (any, error) {
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")
	versions := h.Backend.DescribeDBEngineVersions(engine, engineVersion)
	members := make([]xmlDBEngineVersion, 0, len(versions))
	for _, v := range versions {
		members = append(members, xmlDBEngineVersion(v))
	}

	return &describeDBEngineVersionsResponse{
		Xmlns:            rdsXMLNS,
		DBEngineVersions: xmlDBEngineVersionList{Members: members},
	}, nil
}

func (h *Handler) handleDescribeOrderableDBInstanceOptions(vals url.Values) (any, error) {
	engine := vals.Get("Engine")
	engineVersion := vals.Get("EngineVersion")
	options := h.Backend.DescribeOrderableDBInstanceOptions(engine, engineVersion)
	members := make([]xmlOrderableDBInstanceOption, 0, len(options))
	for _, o := range options {
		members = append(members, xmlOrderableDBInstanceOption(o))
	}

	return &describeOrderableDBInstanceOptionsResponse{
		Xmlns: rdsXMLNS,
		Result: describeOrderableDBInstanceOptionsResult{
			OrderableDBInstanceOptions: xmlOrderableDBInstanceOptionList{Members: members},
		},
	}, nil
}

func (h *Handler) handleDescribeDBLogFiles(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")
	files, err := h.Backend.DescribeDBLogFiles(instanceID)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBLogFile, 0, len(files))
	for _, f := range files {
		members = append(members, xmlDBLogFile(f))
	}

	return &describeDBLogFilesResponse{
		Xmlns:              rdsXMLNS,
		DescribeDBLogFiles: xmlDBLogFileList{Members: members},
	}, nil
}

func (h *Handler) handleDownloadDBLogFilePortion(vals url.Values) (any, error) {
	instanceID := vals.Get("DBInstanceIdentifier")
	logFileName := vals.Get("LogFileName")
	data, err := h.Backend.DownloadDBLogFilePortion(instanceID, logFileName)
	if err != nil {
		return nil, err
	}

	return &downloadDBLogFilePortionResponse{
		Xmlns:                 rdsXMLNS,
		LogFileData:           data,
		AdditionalDataPending: false,
		Marker:                "",
	}, nil
}

func (h *Handler) handleDescribeGlobalClusters(_ url.Values) (any, error) {
	return &describeGlobalClustersResponse{
		Xmlns:          rdsXMLNS,
		GlobalClusters: xmlGlobalClusterList{},
	}, nil
}

func toXMLParameterGroup(pg *DBParameterGroup) xmlDBParameterGroup {
	return xmlDBParameterGroup{
		DBParameterGroupName:   pg.DBParameterGroupName,
		DBParameterGroupFamily: pg.DBParameterGroupFamily,
		Description:            pg.Description,
	}
}

func toXMLOptionGroup(og *OptionGroup) xmlOptionGroup {
	opts := make([]xmlOptionGroupOption, 0, len(og.Options))
	for _, o := range og.Options {
		opts = append(opts, xmlOptionGroupOption(o))
	}

	return xmlOptionGroup{
		OptionGroupName:        og.OptionGroupName,
		OptionGroupDescription: og.OptionGroupDescription,
		EngineName:             og.EngineName,
		MajorEngineVersion:     og.MajorEngineVersion,
		Options:                xmlOptionGroupOptionList{Members: opts},
	}
}

func toXMLCluster(c *DBCluster) xmlDBCluster {
	return xmlDBCluster{
		DBClusterIdentifier:         c.DBClusterIdentifier,
		Engine:                      c.Engine,
		Status:                      c.Status,
		MasterUsername:              c.MasterUsername,
		DatabaseName:                c.DatabaseName,
		DBClusterParameterGroupName: c.DBClusterParameterGroupName,
		Endpoint:                    c.Endpoint,
		Port:                        c.Port,
	}
}

func toXMLClusterSnapshot(s *DBClusterSnapshot) xmlDBClusterSnapshot {
	return xmlDBClusterSnapshot{
		DBClusterSnapshotIdentifier: s.DBClusterSnapshotIdentifier,
		DBClusterIdentifier:         s.DBClusterIdentifier,
		Engine:                      s.Engine,
		Status:                      s.Status,
	}
}

// ---- Parameter Group XML types ----

type xmlDBParameterGroup struct {
	DBParameterGroupName   string `xml:"DBParameterGroupName"`
	DBParameterGroupFamily string `xml:"DBParameterGroupFamily"`
	Description            string `xml:"Description"`
}

type xmlDBParameterGroupList struct {
	Members []xmlDBParameterGroup `xml:"DBParameterGroup"`
}

type xmlDBParameter struct {
	ParameterName  string `xml:"ParameterName"`
	ParameterValue string `xml:"ParameterValue,omitempty"`
	Description    string `xml:"Description,omitempty"`
	ApplyType      string `xml:"ApplyType,omitempty"`
	DataType       string `xml:"DataType,omitempty"`
	IsModifiable   bool   `xml:"IsModifiable"`
}

type xmlDBParameterList struct {
	Members []xmlDBParameter `xml:"Parameter"`
}

type createDBParameterGroupResponse struct {
	XMLName          xml.Name            `xml:"CreateDBParameterGroupResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	DBParameterGroup xmlDBParameterGroup `xml:"CreateDBParameterGroupResult>DBParameterGroup"`
}

type describeDBParameterGroupsResponse struct {
	XMLName           xml.Name                `xml:"DescribeDBParameterGroupsResponse"`
	Xmlns             string                  `xml:"xmlns,attr"`
	DBParameterGroups xmlDBParameterGroupList `xml:"DescribeDBParameterGroupsResult>DBParameterGroups"`
}

type deleteDBParameterGroupResponse struct {
	XMLName xml.Name `xml:"DeleteDBParameterGroupResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type modifyDBParameterGroupResponse struct {
	XMLName              xml.Name `xml:"ModifyDBParameterGroupResponse"`
	Xmlns                string   `xml:"xmlns,attr"`
	DBParameterGroupName string   `xml:"ModifyDBParameterGroupResult>DBParameterGroupName"`
}

type resetDBParameterGroupResponse struct {
	XMLName              xml.Name `xml:"ResetDBParameterGroupResponse"`
	Xmlns                string   `xml:"xmlns,attr"`
	DBParameterGroupName string   `xml:"ResetDBParameterGroupResult>DBParameterGroupName"`
}

type describeDBParametersResponse struct {
	XMLName    xml.Name           `xml:"DescribeDBParametersResponse"`
	Xmlns      string             `xml:"xmlns,attr"`
	Parameters xmlDBParameterList `xml:"DescribeDBParametersResult>Parameters"`
}

// ---- Option Group XML types ----

type xmlOptionGroupOption struct {
	OptionName    string `xml:"OptionName"`
	OptionVersion string `xml:"OptionVersion,omitempty"`
}

type xmlOptionGroupOptionList struct {
	Members []xmlOptionGroupOption `xml:"Option"`
}

type xmlOptionGroup struct {
	OptionGroupName        string                   `xml:"OptionGroupName"`
	OptionGroupDescription string                   `xml:"OptionGroupDescription"`
	EngineName             string                   `xml:"EngineName"`
	MajorEngineVersion     string                   `xml:"MajorEngineVersion"`
	Options                xmlOptionGroupOptionList `xml:"Options"`
}

type xmlOptionGroupList struct {
	Members []xmlOptionGroup `xml:"OptionGroup"`
}

type createOptionGroupResponse struct {
	XMLName     xml.Name       `xml:"CreateOptionGroupResponse"`
	Xmlns       string         `xml:"xmlns,attr"`
	OptionGroup xmlOptionGroup `xml:"CreateOptionGroupResult>OptionGroup"`
}

type describeOptionGroupsResponse struct {
	XMLName          xml.Name           `xml:"DescribeOptionGroupsResponse"`
	Xmlns            string             `xml:"xmlns,attr"`
	OptionGroupsList xmlOptionGroupList `xml:"DescribeOptionGroupsResult>OptionGroupsList"`
}

type deleteOptionGroupResponse struct {
	XMLName xml.Name `xml:"DeleteOptionGroupResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type modifyOptionGroupResponse struct {
	XMLName     xml.Name       `xml:"ModifyOptionGroupResponse"`
	Xmlns       string         `xml:"xmlns,attr"`
	OptionGroup xmlOptionGroup `xml:"ModifyOptionGroupResult>OptionGroup"`
}

type describeOptionGroupOptionsResponse struct {
	XMLName xml.Name `xml:"DescribeOptionGroupOptionsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

// ---- Cluster XML types ----

type xmlDBCluster struct {
	DBClusterIdentifier         string `xml:"DBClusterIdentifier"`
	Engine                      string `xml:"Engine"`
	Status                      string `xml:"Status"`
	MasterUsername              string `xml:"MasterUsername"`
	DatabaseName                string `xml:"DatabaseName,omitempty"`
	DBClusterParameterGroupName string `xml:"DBClusterParameterGroup"`
	Endpoint                    string `xml:"Endpoint,omitempty"`
	Port                        int    `xml:"Port"`
}

type xmlDBClusterList struct {
	Members []xmlDBCluster `xml:"DBCluster"`
}

type createDBClusterResponse struct {
	XMLName   xml.Name     `xml:"CreateDBClusterResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	DBCluster xmlDBCluster `xml:"CreateDBClusterResult>DBCluster"`
}

type describeDBClustersResponse struct {
	XMLName    xml.Name         `xml:"DescribeDBClustersResponse"`
	Xmlns      string           `xml:"xmlns,attr"`
	DBClusters xmlDBClusterList `xml:"DescribeDBClustersResult>DBClusters"`
}

type deleteDBClusterResponse struct {
	XMLName   xml.Name     `xml:"DeleteDBClusterResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	DBCluster xmlDBCluster `xml:"DeleteDBClusterResult>DBCluster"`
}

type modifyDBClusterResponse struct {
	XMLName   xml.Name     `xml:"ModifyDBClusterResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	DBCluster xmlDBCluster `xml:"ModifyDBClusterResult>DBCluster"`
}

type createDBClusterParameterGroupResponse struct {
	XMLName          xml.Name            `xml:"CreateDBClusterParameterGroupResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	DBParameterGroup xmlDBParameterGroup `xml:"CreateDBClusterParameterGroupResult>DBClusterParameterGroup"`
}

type describeDBClusterParameterGroupsResult struct {
	DBClusterParameterGroups xmlDBParameterGroupList `xml:"DBClusterParameterGroups"`
}

type describeDBClusterParameterGroupsResponse struct {
	XMLName xml.Name                               `xml:"DescribeDBClusterParameterGroupsResponse"`
	Xmlns   string                                 `xml:"xmlns,attr"`
	Result  describeDBClusterParameterGroupsResult `xml:"DescribeDBClusterParameterGroupsResult"`
}

// ---- Cluster Snapshot XML types ----

type xmlDBClusterSnapshot struct {
	DBClusterSnapshotIdentifier string `xml:"DBClusterSnapshotIdentifier"`
	DBClusterIdentifier         string `xml:"DBClusterIdentifier"`
	Engine                      string `xml:"Engine"`
	Status                      string `xml:"Status"`
}

type xmlDBClusterSnapshotList struct {
	Members []xmlDBClusterSnapshot `xml:"DBClusterSnapshot"`
}

type createDBClusterSnapshotResponse struct {
	XMLName           xml.Name             `xml:"CreateDBClusterSnapshotResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	DBClusterSnapshot xmlDBClusterSnapshot `xml:"CreateDBClusterSnapshotResult>DBClusterSnapshot"`
}

type describeDBClusterSnapshotsResponse struct {
	XMLName            xml.Name                 `xml:"DescribeDBClusterSnapshotsResponse"`
	Xmlns              string                   `xml:"xmlns,attr"`
	DBClusterSnapshots xmlDBClusterSnapshotList `xml:"DescribeDBClusterSnapshotsResult>DBClusterSnapshots"`
}

// ---- Read Replica XML types ----

type createDBInstanceReadReplicaResponse struct {
	XMLName    xml.Name      `xml:"CreateDBInstanceReadReplicaResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"CreateDBInstanceReadReplicaResult>DBInstance"`
}

type promoteReadReplicaResponse struct {
	XMLName    xml.Name      `xml:"PromoteReadReplicaResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"PromoteReadReplicaResult>DBInstance"`
}

// ---- Misc XML types ----

type rebootDBInstanceResponse struct {
	XMLName    xml.Name      `xml:"RebootDBInstanceResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"RebootDBInstanceResult>DBInstance"`
}

type xmlDBEngineVersion struct {
	Engine              string `xml:"Engine"`
	EngineVersion       string `xml:"EngineVersion"`
	DBEngineDescription string `xml:"DBEngineDescription"`
}

type xmlDBEngineVersionList struct {
	Members []xmlDBEngineVersion `xml:"DBEngineVersion"`
}

type describeDBEngineVersionsResponse struct {
	XMLName          xml.Name               `xml:"DescribeDBEngineVersionsResponse"`
	Xmlns            string                 `xml:"xmlns,attr"`
	DBEngineVersions xmlDBEngineVersionList `xml:"DescribeDBEngineVersionsResult>DBEngineVersions"`
}

type xmlOrderableDBInstanceOption struct {
	Engine          string `xml:"Engine"`
	EngineVersion   string `xml:"EngineVersion"`
	DBInstanceClass string `xml:"DBInstanceClass"`
	MultiAZCapable  bool   `xml:"MultiAZCapable"`
}

type xmlOrderableDBInstanceOptionList struct {
	Members []xmlOrderableDBInstanceOption `xml:"OrderableDBInstanceOption"`
}

type describeOrderableDBInstanceOptionsResult struct {
	OrderableDBInstanceOptions xmlOrderableDBInstanceOptionList `xml:"OrderableDBInstanceOptions"`
}

type describeOrderableDBInstanceOptionsResponse struct {
	XMLName xml.Name                                 `xml:"DescribeOrderableDBInstanceOptionsResponse"`
	Xmlns   string                                   `xml:"xmlns,attr"`
	Result  describeOrderableDBInstanceOptionsResult `xml:"DescribeOrderableDBInstanceOptionsResult"`
}

type xmlDBLogFile struct {
	LogFileName string `xml:"LogFileName"`
	Size        int64  `xml:"Size"`
}

type xmlDBLogFileList struct {
	Members []xmlDBLogFile `xml:"DescribeDBLogFilesDetails"`
}

type describeDBLogFilesResponse struct {
	XMLName            xml.Name         `xml:"DescribeDBLogFilesResponse"`
	Xmlns              string           `xml:"xmlns,attr"`
	DescribeDBLogFiles xmlDBLogFileList `xml:"DescribeDBLogFilesResult>DescribeDBLogFiles"`
}

type downloadDBLogFilePortionResponse struct {
	XMLName               xml.Name `xml:"DownloadDBLogFilePortionResponse"`
	Xmlns                 string   `xml:"xmlns,attr"`
	LogFileData           string   `xml:"DownloadDBLogFilePortionResult>LogFileData"`
	Marker                string   `xml:"DownloadDBLogFilePortionResult>Marker,omitempty"`
	AdditionalDataPending bool     `xml:"DownloadDBLogFilePortionResult>AdditionalDataPending"`
}

type xmlGlobalClusterList struct {
	Members []struct{} `xml:"GlobalCluster"`
}

type describeGlobalClustersResponse struct {
	XMLName        xml.Name             `xml:"DescribeGlobalClustersResponse"`
	Xmlns          string               `xml:"xmlns,attr"`
	GlobalClusters xmlGlobalClusterList `xml:"DescribeGlobalClustersResult>GlobalClusters"`
}
