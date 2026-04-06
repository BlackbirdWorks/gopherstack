package docdb

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
	docdbVersion = "2014-10-31"
	docdbXMLNS   = "http://rds.amazonaws.com/doc/2014-10-31/"
)

// Handler is the Echo HTTP handler for DocDB operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new DocDB handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Reset clears all backend state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Name returns the service name.
func (h *Handler) Name() string { return "DocDB" }

// GetSupportedOperations returns supported DocDB operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateDBCluster",
		"DescribeDBClusters",
		"DeleteDBCluster",
		"ModifyDBCluster",
		"StopDBCluster",
		"StartDBCluster",
		"FailoverDBCluster",
		"CreateDBInstance",
		"DescribeDBInstances",
		"DeleteDBInstance",
		"ModifyDBInstance",
		"RebootDBInstance",
		"CreateDBSubnetGroup",
		"DescribeDBSubnetGroups",
		"DeleteDBSubnetGroup",
		"CreateDBClusterParameterGroup",
		"DescribeDBClusterParameterGroups",
		"DeleteDBClusterParameterGroup",
		"ModifyDBClusterParameterGroup",
		"CreateDBClusterSnapshot",
		"DescribeDBClusterSnapshots",
		"DeleteDBClusterSnapshot",
		"ListTagsForResource",
		"AddTagsToResource",
		"RemoveTagsFromResource",
		"DescribeDBEngineVersions",
		"DescribeOrderableDBInstanceOptions",
		"DescribeGlobalClusters",
		"AddSourceIdentifierToSubscription",
		"ApplyPendingMaintenanceAction",
		"CopyDBClusterParameterGroup",
		"CopyDBClusterSnapshot",
		"CreateEventSubscription",
		"CreateGlobalCluster",
		"DeleteEventSubscription",
		"DeleteGlobalCluster",
		"DescribeCertificates",
		"DescribeDBClusterParameters",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "docdb" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this DocDB instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches DocDB requests.
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
		ua := r.Header.Get("User-Agent")
		if !strings.Contains(ua, "api/docdb") {
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

		return vals.Get("Version") == docdbVersion
	}
}

// MatchPriority returns the routing priority for DocDB (higher than RDS to intercept DocDB requests first).
func (h *Handler) MatchPriority() int { return service.PriorityFormDocDB }

// ExtractOperation extracts the DocDB action from the request.
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

// ExtractResource returns the DB cluster identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return ""
	}

	return r.Form.Get("DBClusterIdentifier")
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

func (h *Handler) dispatch(action string, vals url.Values) (any, error) {
	switch action {
	case "CreateDBCluster":
		return h.handleCreateDBCluster(vals)
	case "DescribeDBClusters":
		return h.handleDescribeDBClusters(vals)
	case "DeleteDBCluster":
		return h.handleDeleteDBCluster(vals)
	case "ModifyDBCluster":
		return h.handleModifyDBCluster(vals)
	case "StopDBCluster":
		return h.handleStopDBCluster(vals)
	case "StartDBCluster":
		return h.handleStartDBCluster(vals)
	case "FailoverDBCluster":
		return h.handleFailoverDBCluster(vals)
	case "CreateDBInstance":
		return h.handleCreateDBInstance(vals)
	case "DescribeDBInstances":
		return h.handleDescribeDBInstances(vals)
	case "DeleteDBInstance":
		return h.handleDeleteDBInstance(vals)
	case "ModifyDBInstance":
		return h.handleModifyDBInstance(vals)
	case "RebootDBInstance":
		return h.handleRebootDBInstance(vals)
	default:
		return h.dispatchExtended(action, vals)
	}
}

func (h *Handler) dispatchExtended(action string, vals url.Values) (any, error) {
	switch action {
	case "CreateDBSubnetGroup":
		return h.handleCreateDBSubnetGroup(vals)
	case "DescribeDBSubnetGroups":
		return h.handleDescribeDBSubnetGroups(vals)
	case "DeleteDBSubnetGroup":
		return h.handleDeleteDBSubnetGroup(vals)
	case "CreateDBClusterParameterGroup":
		return h.handleCreateDBClusterParameterGroup(vals)
	case "DescribeDBClusterParameterGroups":
		return h.handleDescribeDBClusterParameterGroups(vals)
	case "DeleteDBClusterParameterGroup":
		return h.handleDeleteDBClusterParameterGroup(vals)
	case "ModifyDBClusterParameterGroup":
		return h.handleModifyDBClusterParameterGroup(vals)
	default:
		return h.dispatchExtended2(action, vals)
	}
}

func (h *Handler) dispatchExtended2(action string, vals url.Values) (any, error) {
	switch action {
	case "CreateDBClusterSnapshot":
		return h.handleCreateDBClusterSnapshot(vals)
	case "DescribeDBClusterSnapshots":
		return h.handleDescribeDBClusterSnapshots(vals)
	case "DeleteDBClusterSnapshot":
		return h.handleDeleteDBClusterSnapshot(vals)
	case "ListTagsForResource":
		return h.handleListTagsForResource(vals)
	case "AddTagsToResource":
		return h.handleAddTagsToResource(vals)
	case "RemoveTagsFromResource":
		return h.handleRemoveTagsFromResource(vals)
	case "DescribeDBEngineVersions":
		return h.handleDescribeDBEngineVersions(vals)
	case "DescribeOrderableDBInstanceOptions":
		return h.handleDescribeOrderableDBInstanceOptions(vals)
	case "DescribeGlobalClusters":
		return h.handleDescribeGlobalClusters(vals)
	default:
		return h.dispatchExtended3(action, vals)
	}
}

func (h *Handler) dispatchExtended3(action string, vals url.Values) (any, error) {
	switch action {
	case "AddSourceIdentifierToSubscription":
		return h.handleAddSourceIdentifierToSubscription(vals)
	case "ApplyPendingMaintenanceAction":
		return h.handleApplyPendingMaintenanceAction(vals)
	case "CopyDBClusterParameterGroup":
		return h.handleCopyDBClusterParameterGroup(vals)
	case "CopyDBClusterSnapshot":
		return h.handleCopyDBClusterSnapshot(vals)
	case "CreateEventSubscription":
		return h.handleCreateEventSubscription(vals)
	case "CreateGlobalCluster":
		return h.handleCreateGlobalCluster(vals)
	case "DeleteEventSubscription":
		return h.handleDeleteEventSubscription(vals)
	case "DeleteGlobalCluster":
		return h.handleDeleteGlobalCluster(vals)
	case "DescribeCertificates":
		return h.handleDescribeCertificates(vals)
	case "DescribeDBClusterParameters":
		return h.handleDescribeDBClusterParameters(vals)
	default:
		return nil, fmt.Errorf("%w: %s is not a valid DocDB action", ErrUnknownAction, action)
	}
}

func (h *Handler) handleCreateDBCluster(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	engine := vals.Get("Engine")
	masterUser := vals.Get("MasterUsername")
	dbName := vals.Get("DatabaseName")
	paramGroupName := vals.Get("DBClusterParameterGroupName")
	tags := tagsToMap(parseTagEntries(vals))
	cluster, err := h.Backend.CreateDBCluster(id, engine, masterUser, dbName, paramGroupName, 0, tags)
	if err != nil {
		return nil, err
	}

	return &createDBClusterResponse{
		Xmlns:     docdbXMLNS,
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

	members, nextMarker := applyDocDBMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBClustersResponse{
		Xmlns: docdbXMLNS,
		Result: describeDBClustersResult{
			DBClusters: xmlDBClusterList{Members: members},
			Marker:     nextMarker,
		},
	}, nil
}

func (h *Handler) handleDeleteDBCluster(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.DeleteDBCluster(id)
	if err != nil {
		return nil, err
	}

	return &deleteDBClusterResponse{
		Xmlns:     docdbXMLNS,
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
		Xmlns:     docdbXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleStopDBCluster(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.StopDBCluster(id)
	if err != nil {
		return nil, err
	}

	return &stopDBClusterResponse{
		Xmlns:     docdbXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleStartDBCluster(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.StartDBCluster(id)
	if err != nil {
		return nil, err
	}

	return &startDBClusterResponse{
		Xmlns:     docdbXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleFailoverDBCluster(vals url.Values) (any, error) {
	id := vals.Get("DBClusterIdentifier")
	cluster, err := h.Backend.FailoverDBCluster(id)
	if err != nil {
		return nil, err
	}

	return &failoverDBClusterResponse{
		Xmlns:     docdbXMLNS,
		DBCluster: toXMLCluster(cluster),
	}, nil
}

func (h *Handler) handleCreateDBInstance(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	instanceClass := vals.Get("DBInstanceClass")
	engine := vals.Get("Engine")
	tags := tagsToMap(parseTagEntries(vals))
	inst, err := h.Backend.CreateDBInstance(id, clusterID, instanceClass, engine, tags)
	if err != nil {
		return nil, err
	}

	return &createDBInstanceResponse{
		Xmlns:      docdbXMLNS,
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

	members, nextMarker := applyDocDBMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBInstancesResponse{
		Xmlns: docdbXMLNS,
		Result: describeDBInstancesResult{
			DBInstances: xmlDBInstanceList{Members: members},
			Marker:      nextMarker,
		},
	}, nil
}

func (h *Handler) handleDeleteDBInstance(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	inst, err := h.Backend.DeleteDBInstance(id)
	if err != nil {
		return nil, err
	}

	return &deleteDBInstanceResponse{
		Xmlns:      docdbXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleModifyDBInstance(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	instanceClass := vals.Get("DBInstanceClass")
	inst, err := h.Backend.ModifyDBInstance(id, instanceClass)
	if err != nil {
		return nil, err
	}

	return &modifyDBInstanceResponse{
		Xmlns:      docdbXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleRebootDBInstance(vals url.Values) (any, error) {
	id := vals.Get("DBInstanceIdentifier")
	inst, err := h.Backend.RebootDBInstance(id)
	if err != nil {
		return nil, err
	}

	return &rebootDBInstanceResponse{
		Xmlns:      docdbXMLNS,
		DBInstance: toXMLInstance(inst),
	}, nil
}

func (h *Handler) handleCreateDBSubnetGroup(vals url.Values) (any, error) {
	name := vals.Get("DBSubnetGroupName")
	description := vals.Get("DBSubnetGroupDescription")
	vpcID := vals.Get("VpcId")
	subnetIDs := parseSubnetIDMembers(vals)
	tags := tagsToMap(parseTagEntries(vals))
	sg, err := h.Backend.CreateDBSubnetGroup(name, description, vpcID, subnetIDs, tags)
	if err != nil {
		return nil, err
	}

	return &createDBSubnetGroupResponse{
		Xmlns:         docdbXMLNS,
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

	members, nextMarker := applyDocDBMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBSubnetGroupsResponse{
		Xmlns: docdbXMLNS,
		Result: describeDBSubnetGroupsResult{
			DBSubnetGroups: xmlDBSubnetGroupList{Members: members},
			Marker:         nextMarker,
		},
	}, nil
}

func (h *Handler) handleDeleteDBSubnetGroup(vals url.Values) (any, error) {
	name := vals.Get("DBSubnetGroupName")
	if err := h.Backend.DeleteDBSubnetGroup(name); err != nil {
		return nil, err
	}

	return &deleteDBSubnetGroupResponse{Xmlns: docdbXMLNS}, nil
}

func (h *Handler) handleCreateDBClusterParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	family := vals.Get("DBParameterGroupFamily")
	description := vals.Get("Description")
	tags := tagsToMap(parseTagEntries(vals))
	pg, err := h.Backend.CreateDBClusterParameterGroup(name, family, description, tags)
	if err != nil {
		return nil, err
	}

	return &createDBClusterParameterGroupResponse{
		Xmlns:                   docdbXMLNS,
		DBClusterParameterGroup: toXMLParameterGroup(pg),
	}, nil
}

func (h *Handler) handleDescribeDBClusterParameterGroups(vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	groups, err := h.Backend.DescribeDBClusterParameterGroups(name)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBClusterParameterGroup, 0, len(groups))
	for _, pg := range groups {
		cp := pg
		members = append(members, toXMLParameterGroup(&cp))
	}

	return &describeDBClusterParameterGroupsResponse{
		Xmlns: docdbXMLNS,
		Result: describeDBClusterParameterGroupsResult{
			DBClusterParameterGroups: xmlDBClusterParameterGroupList{Members: members},
		},
	}, nil
}

func (h *Handler) handleDeleteDBClusterParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	if err := h.Backend.DeleteDBClusterParameterGroup(name); err != nil {
		return nil, err
	}

	return &deleteDBClusterParameterGroupResponse{Xmlns: docdbXMLNS}, nil
}

func (h *Handler) handleModifyDBClusterParameterGroup(vals url.Values) (any, error) {
	name := vals.Get("DBClusterParameterGroupName")
	pg, err := h.Backend.ModifyDBClusterParameterGroup(name)
	if err != nil {
		return nil, err
	}

	return &modifyDBClusterParameterGroupResponse{
		Xmlns:                       docdbXMLNS,
		DBClusterParameterGroupName: pg.DBClusterParameterGroupName,
	}, nil
}

func (h *Handler) handleCreateDBClusterSnapshot(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	tags := tagsToMap(parseTagEntries(vals))
	snap, err := h.Backend.CreateDBClusterSnapshot(snapshotID, clusterID, tags)
	if err != nil {
		return nil, err
	}

	return &createDBClusterSnapshotResponse{
		Xmlns:             docdbXMLNS,
		DBClusterSnapshot: toXMLClusterSnapshot(snap),
	}, nil
}

func (h *Handler) handleDescribeDBClusterSnapshots(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	clusterID := vals.Get("DBClusterIdentifier")
	snaps, err := h.Backend.DescribeDBClusterSnapshots(snapshotID, clusterID)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBClusterSnapshot, 0, len(snaps))
	for _, snap := range snaps {
		cp := snap
		members = append(members, toXMLClusterSnapshot(&cp))
	}

	members, nextMarker := applyDocDBMarker(members, vals.Get("Marker"), vals.Get("MaxRecords"))

	return &describeDBClusterSnapshotsResponse{
		Xmlns: docdbXMLNS,
		Result: describeDBClusterSnapshotsResult{
			DBClusterSnapshots: xmlDBClusterSnapshotList{Members: members},
			Marker:             nextMarker,
		},
	}, nil
}

func (h *Handler) handleDeleteDBClusterSnapshot(vals url.Values) (any, error) {
	snapshotID := vals.Get("DBClusterSnapshotIdentifier")
	snap, err := h.Backend.DeleteDBClusterSnapshot(snapshotID)
	if err != nil {
		return nil, err
	}

	return &deleteDBClusterSnapshotResponse{
		Xmlns:             docdbXMLNS,
		DBClusterSnapshot: toXMLClusterSnapshot(snap),
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
		Xmlns:   docdbXMLNS,
		TagList: xmlTagList{Members: members},
	}, nil
}

func (h *Handler) handleAddTagsToResource(vals url.Values) (any, error) {
	arn := vals.Get("ResourceName")
	tags := parseTagEntries(vals)
	h.Backend.AddTagsToResource(arn, tags)

	return &addTagsToResourceResponse{Xmlns: docdbXMLNS}, nil
}

func (h *Handler) handleRemoveTagsFromResource(vals url.Values) (any, error) {
	arn := vals.Get("ResourceName")
	keys := parseTagKeyMembers(vals)
	h.Backend.RemoveTagsFromResource(arn, keys)

	return &removeTagsFromResourceResponse{Xmlns: docdbXMLNS}, nil
}

func (h *Handler) handleDescribeDBEngineVersions(_ url.Values) (any, error) {
	members := []xmlDBEngineVersion{
		{Engine: docDBEngine, EngineVersion: "4.0.0", DBEngineDescription: "Amazon DocumentDB"},
		{Engine: docDBEngine, EngineVersion: "5.0.0", DBEngineDescription: "Amazon DocumentDB"},
	}

	return &describeDBEngineVersionsResponse{
		Xmlns:            docdbXMLNS,
		DBEngineVersions: xmlDBEngineVersionList{Members: members},
	}, nil
}

func (h *Handler) handleDescribeOrderableDBInstanceOptions(_ url.Values) (any, error) {
	members := []xmlOrderableDBInstanceOption{
		{Engine: docDBEngine, EngineVersion: "4.0.0", DBInstanceClass: "db.t3.medium"},
		{Engine: docDBEngine, EngineVersion: "4.0.0", DBInstanceClass: "db.r5.large"},
		{Engine: docDBEngine, EngineVersion: "5.0.0", DBInstanceClass: "db.t3.medium"},
		{Engine: docDBEngine, EngineVersion: "5.0.0", DBInstanceClass: "db.r5.large"},
	}

	return &describeOrderableDBInstanceOptionsResponse{
		Xmlns: docdbXMLNS,
		Result: describeOrderableDBInstanceOptionsResult{
			OrderableDBInstanceOptions: xmlOrderableDBInstanceOptionList{Members: members},
		},
	}, nil
}

func (h *Handler) handleDescribeGlobalClusters(vals url.Values) (any, error) {
	gcs := h.Backend.DescribeGlobalClusters(vals.Get("GlobalClusterIdentifier"))
	members := make([]xmlGlobalCluster, 0, len(gcs))
	for _, gc := range gcs {
		cp := gc
		members = append(members, toXMLGlobalCluster(&cp))
	}

	return &describeGlobalClustersResponse{
		Xmlns:          docdbXMLNS,
		GlobalClusters: xmlGlobalClusterList{Members: members},
	}, nil
}

func (h *Handler) handleAddSourceIdentifierToSubscription(vals url.Values) (any, error) {
	subscriptionName := vals.Get("SubscriptionName")
	sourceID := vals.Get("SourceIdentifier")
	sub, err := h.Backend.AddSourceIdentifierToSubscription(subscriptionName, sourceID)
	if err != nil {
		return nil, err
	}

	return &addSourceIdentifierToSubscriptionResponse{
		Xmlns:             docdbXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleApplyPendingMaintenanceAction(vals url.Values) (any, error) {
	resourceARN := vals.Get("ResourceIdentifier")
	action := vals.Get("ApplyAction")
	optInType := vals.Get("OptInType")
	if err := h.Backend.ApplyPendingMaintenanceAction(resourceARN, action, optInType); err != nil {
		return nil, err
	}

	return &applyPendingMaintenanceActionResponse{
		Xmlns: docdbXMLNS,
		Result: applyPendingMaintenanceActionResult{
			ResourcePendingMaintenanceActions: xmlResourcePendingMaintenanceActions{
				ResourceIdentifier:              resourceARN,
				PendingMaintenanceActionDetails: xmlPendingMaintenanceActionList{},
			},
		},
	}, nil
}

func (h *Handler) handleCopyDBClusterParameterGroup(vals url.Values) (any, error) {
	sourceGroupName := vals.Get("SourceDBClusterParameterGroupIdentifier")
	targetName := vals.Get("TargetDBClusterParameterGroupIdentifier")
	targetDescription := vals.Get("TargetDBClusterParameterGroupDescription")
	pg, err := h.Backend.CopyDBClusterParameterGroup(sourceGroupName, targetName, targetDescription)
	if err != nil {
		return nil, err
	}

	return &copyDBClusterParameterGroupResponse{
		Xmlns:                   docdbXMLNS,
		DBClusterParameterGroup: toXMLParameterGroup(pg),
	}, nil
}

func (h *Handler) handleCopyDBClusterSnapshot(vals url.Values) (any, error) {
	sourceSnapshotID := vals.Get("SourceDBClusterSnapshotIdentifier")
	targetSnapshotID := vals.Get("TargetDBClusterSnapshotIdentifier")
	snap, err := h.Backend.CopyDBClusterSnapshot(sourceSnapshotID, targetSnapshotID)
	if err != nil {
		return nil, err
	}

	return &copyDBClusterSnapshotResponse{
		Xmlns:             docdbXMLNS,
		DBClusterSnapshot: toXMLClusterSnapshot(snap),
	}, nil
}

func (h *Handler) handleCreateEventSubscription(vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	snsTopicARN := vals.Get("SnsTopicArn")
	sourceIDs := parseSourceIDMembers(vals)
	sub, err := h.Backend.CreateEventSubscription(name, snsTopicARN, sourceIDs)
	if err != nil {
		return nil, err
	}

	return &createEventSubscriptionResponse{
		Xmlns:             docdbXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleCreateGlobalCluster(vals url.Values) (any, error) {
	id := vals.Get("GlobalClusterIdentifier")
	sourceDBClusterID := vals.Get("SourceDBClusterIdentifier")
	gc, err := h.Backend.CreateGlobalCluster(id, sourceDBClusterID)
	if err != nil {
		return nil, err
	}

	return &createGlobalClusterResponse{
		Xmlns:         docdbXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleDeleteEventSubscription(vals url.Values) (any, error) {
	name := vals.Get("SubscriptionName")
	sub, err := h.Backend.DeleteEventSubscription(name)
	if err != nil {
		return nil, err
	}

	return &deleteEventSubscriptionResponse{
		Xmlns:             docdbXMLNS,
		EventSubscription: toXMLEventSubscription(sub),
	}, nil
}

func (h *Handler) handleDeleteGlobalCluster(vals url.Values) (any, error) {
	id := vals.Get("GlobalClusterIdentifier")
	gc, err := h.Backend.DeleteGlobalCluster(id)
	if err != nil {
		return nil, err
	}

	return &deleteGlobalClusterResponse{
		Xmlns:         docdbXMLNS,
		GlobalCluster: toXMLGlobalCluster(gc),
	}, nil
}

func (h *Handler) handleDescribeCertificates(vals url.Values) (any, error) {
	certificateID := vals.Get("CertificateIdentifier")
	certs := h.Backend.DescribeCertificates(certificateID)
	members := make([]xmlCertificate, 0, len(certs))
	for _, c := range certs {
		cp := c
		members = append(members, toXMLCertificate(&cp))
	}

	return &describeCertificatesResponse{
		Xmlns: docdbXMLNS,
		Result: describeCertificatesResult{
			Certificates: xmlCertificateList{Members: members},
		},
	}, nil
}

func (h *Handler) handleDescribeDBClusterParameters(vals url.Values) (any, error) {
	groupName := vals.Get("DBClusterParameterGroupName")
	params, err := h.Backend.DescribeDBClusterParameters(groupName)
	if err != nil {
		return nil, err
	}
	members := make([]xmlDBClusterParameter, 0, len(params))
	for _, p := range params {
		cp := p
		members = append(members, toXMLDBClusterParameter(&cp))
	}

	return &describeDBClusterParametersResponse{
		Xmlns: docdbXMLNS,
		Result: describeDBClusterParametersResult{
			Parameters: xmlDBClusterParameterList{Members: members},
		},
	}, nil
}

func (h *Handler) handleOpError(c *echo.Context, action string, opErr error) error {
	statusCode := http.StatusBadRequest
	code := docdbErrorCode(opErr)
	if code == "" {
		code = "InternalFailure"
		statusCode = http.StatusInternalServerError
		logger.Load(c.Request().Context()).Error("DocDB internal error", "error", opErr, "action", action)
	}

	return h.writeError(c, statusCode, code, opErr.Error())
}

func docdbErrorCode(opErr error) string {
	sentinels := []error{
		ErrClusterNotFound, ErrClusterAlreadyExists,
		ErrInstanceNotFound, ErrInstanceAlreadyExists,
		ErrSubnetGroupNotFound, ErrSubnetGroupAlreadyExists,
		ErrClusterParameterGroupNotFound, ErrClusterParameterGroupAlreadyExists,
		ErrClusterSnapshotNotFound, ErrClusterSnapshotAlreadyExists,
		ErrEventSubscriptionNotFound, ErrEventSubscriptionAlreadyExists,
		ErrGlobalClusterNotFound, ErrGlobalClusterAlreadyExists,
		ErrInvalidParameter, ErrUnknownAction,
	}
	for _, s := range sentinels {
		if errors.Is(opErr, s) {
			return s.Error()
		}
	}

	return ""
}

func (h *Handler) writeError(c *echo.Context, statusCode int, code, message string) error {
	errResp := &docdbErrorResponse{
		Xmlns: docdbXMLNS,
		Error: docdbError{Code: code, Message: message, Type: "Sender"},
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
		DBClusterArn:                c.DBClusterArn,
		EngineVersion:               c.EngineVersion,
	}
}

func toXMLInstance(inst *DBInstance) xmlDBInstance {
	return xmlDBInstance{
		DBInstanceIdentifier: inst.DBInstanceIdentifier,
		DBClusterIdentifier:  inst.DBClusterIdentifier,
		DBInstanceClass:      inst.DBInstanceClass,
		Engine:               inst.Engine,
		DBInstanceStatus:     inst.DBInstanceStatus,
		Endpoint:             inst.Endpoint,
		Port:                 inst.Port,
		DBInstanceArn:        inst.DBInstanceArn,
		EngineVersion:        inst.EngineVersion,
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

func toXMLParameterGroup(pg *DBClusterParameterGroup) xmlDBClusterParameterGroup {
	return xmlDBClusterParameterGroup{
		DBClusterParameterGroupName: pg.DBClusterParameterGroupName,
		DBParameterGroupFamily:      pg.DBParameterGroupFamily,
		Description:                 pg.Description,
	}
}

func toXMLClusterSnapshot(snap *DBClusterSnapshot) xmlDBClusterSnapshot {
	return xmlDBClusterSnapshot{
		DBClusterSnapshotIdentifier: snap.DBClusterSnapshotIdentifier,
		DBClusterIdentifier:         snap.DBClusterIdentifier,
		Engine:                      snap.Engine,
		Status:                      snap.Status,
		EngineVersion:               snap.EngineVersion,
	}
}

type docdbError struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
	Type    string `xml:"Type"`
}

type docdbErrorResponse struct {
	XMLName xml.Name   `xml:"ErrorResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	Error   docdbError `xml:"Error"`
}

type xmlDBCluster struct {
	DBClusterIdentifier         string `xml:"DBClusterIdentifier"`
	Engine                      string `xml:"Engine"`
	Status                      string `xml:"Status"`
	MasterUsername              string `xml:"MasterUsername,omitempty"`
	DatabaseName                string `xml:"DatabaseName,omitempty"`
	DBClusterParameterGroupName string `xml:"DBClusterParameterGroup,omitempty"`
	Endpoint                    string `xml:"Endpoint,omitempty"`
	DBClusterArn                string `xml:"DBClusterArn,omitempty"`
	EngineVersion               string `xml:"EngineVersion,omitempty"`
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

type describeDBClustersResult struct {
	Marker     string           `xml:"Marker,omitempty"`
	DBClusters xmlDBClusterList `xml:"DBClusters"`
}

type describeDBClustersResponse struct {
	XMLName xml.Name                 `xml:"DescribeDBClustersResponse"`
	Xmlns   string                   `xml:"xmlns,attr"`
	Result  describeDBClustersResult `xml:"DescribeDBClustersResult"`
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

type stopDBClusterResponse struct {
	XMLName   xml.Name     `xml:"StopDBClusterResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	DBCluster xmlDBCluster `xml:"StopDBClusterResult>DBCluster"`
}

type startDBClusterResponse struct {
	XMLName   xml.Name     `xml:"StartDBClusterResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	DBCluster xmlDBCluster `xml:"StartDBClusterResult>DBCluster"`
}

type failoverDBClusterResponse struct {
	XMLName   xml.Name     `xml:"FailoverDBClusterResponse"`
	Xmlns     string       `xml:"xmlns,attr"`
	DBCluster xmlDBCluster `xml:"FailoverDBClusterResult>DBCluster"`
}

type xmlDBInstance struct {
	DBInstanceIdentifier string `xml:"DBInstanceIdentifier"`
	DBClusterIdentifier  string `xml:"DBClusterIdentifier,omitempty"`
	DBInstanceClass      string `xml:"DBInstanceClass"`
	Engine               string `xml:"Engine"`
	DBInstanceStatus     string `xml:"DBInstanceStatus"`
	Endpoint             string `xml:"Endpoint>Address,omitempty"`
	DBInstanceArn        string `xml:"DBInstanceArn,omitempty"`
	EngineVersion        string `xml:"EngineVersion,omitempty"`
	Port                 int    `xml:"Endpoint>Port"`
}

type xmlDBInstanceList struct {
	Members []xmlDBInstance `xml:"DBInstance"`
}

type createDBInstanceResponse struct {
	XMLName    xml.Name      `xml:"CreateDBInstanceResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"CreateDBInstanceResult>DBInstance"`
}

type describeDBInstancesResult struct {
	Marker      string            `xml:"Marker,omitempty"`
	DBInstances xmlDBInstanceList `xml:"DBInstances"`
}

type describeDBInstancesResponse struct {
	XMLName xml.Name                  `xml:"DescribeDBInstancesResponse"`
	Xmlns   string                    `xml:"xmlns,attr"`
	Result  describeDBInstancesResult `xml:"DescribeDBInstancesResult"`
}

type deleteDBInstanceResponse struct {
	XMLName    xml.Name      `xml:"DeleteDBInstanceResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"DeleteDBInstanceResult>DBInstance"`
}

type modifyDBInstanceResponse struct {
	XMLName    xml.Name      `xml:"ModifyDBInstanceResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"ModifyDBInstanceResult>DBInstance"`
}

type rebootDBInstanceResponse struct {
	XMLName    xml.Name      `xml:"RebootDBInstanceResponse"`
	Xmlns      string        `xml:"xmlns,attr"`
	DBInstance xmlDBInstance `xml:"RebootDBInstanceResult>DBInstance"`
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
	VpcID                    string        `xml:"VpcId,omitempty"`
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

type describeDBSubnetGroupsResult struct {
	Marker         string               `xml:"Marker,omitempty"`
	DBSubnetGroups xmlDBSubnetGroupList `xml:"DBSubnetGroups"`
}

type describeDBSubnetGroupsResponse struct {
	XMLName xml.Name                     `xml:"DescribeDBSubnetGroupsResponse"`
	Xmlns   string                       `xml:"xmlns,attr"`
	Result  describeDBSubnetGroupsResult `xml:"DescribeDBSubnetGroupsResult"`
}

type deleteDBSubnetGroupResponse struct {
	XMLName xml.Name `xml:"DeleteDBSubnetGroupResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type xmlDBClusterParameterGroup struct {
	DBClusterParameterGroupName string `xml:"DBClusterParameterGroupName"`
	DBParameterGroupFamily      string `xml:"DBParameterGroupFamily"`
	Description                 string `xml:"Description"`
}

type xmlDBClusterParameterGroupList struct {
	Members []xmlDBClusterParameterGroup `xml:"DBClusterParameterGroup"`
}

type createDBClusterParameterGroupResponse struct {
	XMLName                 xml.Name                   `xml:"CreateDBClusterParameterGroupResponse"`
	Xmlns                   string                     `xml:"xmlns,attr"`
	DBClusterParameterGroup xmlDBClusterParameterGroup `xml:"CreateDBClusterParameterGroupResult>DBClusterParameterGroup"`
}

type describeDBClusterParameterGroupsResult struct {
	DBClusterParameterGroups xmlDBClusterParameterGroupList `xml:"DBClusterParameterGroups"`
}

type describeDBClusterParameterGroupsResponse struct {
	XMLName xml.Name                               `xml:"DescribeDBClusterParameterGroupsResponse"`
	Xmlns   string                                 `xml:"xmlns,attr"`
	Result  describeDBClusterParameterGroupsResult `xml:"DescribeDBClusterParameterGroupsResult"`
}

type deleteDBClusterParameterGroupResponse struct {
	XMLName xml.Name `xml:"DeleteDBClusterParameterGroupResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type modifyDBClusterParameterGroupResponse struct {
	XMLName                     xml.Name `xml:"ModifyDBClusterParameterGroupResponse"`
	Xmlns                       string   `xml:"xmlns,attr"`
	DBClusterParameterGroupName string   `xml:"ModifyDBClusterParameterGroupResult>DBClusterParameterGroupName"`
}

type xmlDBClusterSnapshot struct {
	DBClusterSnapshotIdentifier string `xml:"DBClusterSnapshotIdentifier"`
	DBClusterIdentifier         string `xml:"DBClusterIdentifier"`
	Engine                      string `xml:"Engine"`
	Status                      string `xml:"Status"`
	EngineVersion               string `xml:"EngineVersion,omitempty"`
}

type xmlDBClusterSnapshotList struct {
	Members []xmlDBClusterSnapshot `xml:"DBClusterSnapshot"`
}

type createDBClusterSnapshotResponse struct {
	XMLName           xml.Name             `xml:"CreateDBClusterSnapshotResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	DBClusterSnapshot xmlDBClusterSnapshot `xml:"CreateDBClusterSnapshotResult>DBClusterSnapshot"`
}

type describeDBClusterSnapshotsResult struct {
	Marker             string                   `xml:"Marker,omitempty"`
	DBClusterSnapshots xmlDBClusterSnapshotList `xml:"DBClusterSnapshots"`
}

type describeDBClusterSnapshotsResponse struct {
	XMLName xml.Name                         `xml:"DescribeDBClusterSnapshotsResponse"`
	Xmlns   string                           `xml:"xmlns,attr"`
	Result  describeDBClusterSnapshotsResult `xml:"DescribeDBClusterSnapshotsResult"`
}

type deleteDBClusterSnapshotResponse struct {
	XMLName           xml.Name             `xml:"DeleteDBClusterSnapshotResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	DBClusterSnapshot xmlDBClusterSnapshot `xml:"DeleteDBClusterSnapshotResult>DBClusterSnapshot"`
}

type xmlTagList struct {
	Members []svcTags.KV `xml:"Tag"`
}

type listTagsForResourceResponse struct {
	XMLName xml.Name   `xml:"ListTagsForResourceResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	TagList xmlTagList `xml:"ListTagsForResourceResult>TagList"`
}

type addTagsToResourceResponse struct {
	XMLName xml.Name `xml:"AddTagsToResourceResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type removeTagsFromResourceResponse struct {
	XMLName xml.Name `xml:"RemoveTagsFromResourceResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
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

type xmlGlobalClusterList struct {
	Members []xmlGlobalCluster `xml:"GlobalCluster"`
}

type describeGlobalClustersResponse struct {
	XMLName        xml.Name             `xml:"DescribeGlobalClustersResponse"`
	Xmlns          string               `xml:"xmlns,attr"`
	GlobalClusters xmlGlobalClusterList `xml:"DescribeGlobalClustersResult>GlobalClusters"`
}

type xmlSourceIDList struct {
	Members []string `xml:"SourceId"`
}

type xmlEventSubscription struct {
	SubscriptionName string          `xml:"CustSubscriptionId"`
	SnsTopicARN      string          `xml:"SnsTopicArn,omitempty"`
	Status           string          `xml:"Status"`
	SourceIDsList    xmlSourceIDList `xml:"SourceIdsList"`
}

type addSourceIdentifierToSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"AddSourceIdentifierToSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"AddSourceIdentifierToSubscriptionResult>EventSubscription"`
}

type xmlPendingMaintenanceAction struct {
	Action      string `xml:"Action"`
	OptInStatus string `xml:"OptInStatus"`
}

type xmlPendingMaintenanceActionList struct {
	Members []xmlPendingMaintenanceAction `xml:"PendingMaintenanceAction"`
}

type xmlResourcePendingMaintenanceActions struct {
	ResourceIdentifier              string                          `xml:"ResourceIdentifier"`
	PendingMaintenanceActionDetails xmlPendingMaintenanceActionList `xml:"PendingMaintenanceActionDetails"`
}

type applyPendingMaintenanceActionResult struct {
	ResourcePendingMaintenanceActions xmlResourcePendingMaintenanceActions `xml:"ResourcePendingMaintenanceActions"`
}

type applyPendingMaintenanceActionResponse struct {
	XMLName xml.Name                            `xml:"ApplyPendingMaintenanceActionResponse"`
	Xmlns   string                              `xml:"xmlns,attr"`
	Result  applyPendingMaintenanceActionResult `xml:"ApplyPendingMaintenanceActionResult"`
}

type copyDBClusterParameterGroupResponse struct {
	XMLName                 xml.Name                   `xml:"CopyDBClusterParameterGroupResponse"`
	Xmlns                   string                     `xml:"xmlns,attr"`
	DBClusterParameterGroup xmlDBClusterParameterGroup `xml:"CopyDBClusterParameterGroupResult>DBClusterParameterGroup"`
}

type copyDBClusterSnapshotResponse struct {
	XMLName           xml.Name             `xml:"CopyDBClusterSnapshotResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	DBClusterSnapshot xmlDBClusterSnapshot `xml:"CopyDBClusterSnapshotResult>DBClusterSnapshot"`
}

type createEventSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"CreateEventSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"CreateEventSubscriptionResult>EventSubscription"`
}

type xmlGlobalCluster struct {
	GlobalClusterIdentifier   string `xml:"GlobalClusterIdentifier"`
	SourceDBClusterIdentifier string `xml:"SourceDBClusterIdentifier,omitempty"`
	Status                    string `xml:"Status"`
}

type createGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"CreateGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"CreateGlobalClusterResult>GlobalCluster"`
}

type deleteEventSubscriptionResponse struct {
	XMLName           xml.Name             `xml:"DeleteEventSubscriptionResponse"`
	Xmlns             string               `xml:"xmlns,attr"`
	EventSubscription xmlEventSubscription `xml:"DeleteEventSubscriptionResult>EventSubscription"`
}

type deleteGlobalClusterResponse struct {
	XMLName       xml.Name         `xml:"DeleteGlobalClusterResponse"`
	Xmlns         string           `xml:"xmlns,attr"`
	GlobalCluster xmlGlobalCluster `xml:"DeleteGlobalClusterResult>GlobalCluster"`
}

type xmlCertificate struct {
	CertificateIdentifier string `xml:"CertificateIdentifier"`
	CertificateType       string `xml:"CertificateType"`
	Thumbprint            string `xml:"Thumbprint,omitempty"`
	ValidFrom             string `xml:"ValidFrom,omitempty"`
	ValidTill             string `xml:"ValidTill,omitempty"`
}

type xmlCertificateList struct {
	Members []xmlCertificate `xml:"Certificate"`
}

type describeCertificatesResult struct {
	Certificates xmlCertificateList `xml:"Certificates"`
}

type describeCertificatesResponse struct {
	XMLName xml.Name                   `xml:"DescribeCertificatesResponse"`
	Xmlns   string                     `xml:"xmlns,attr"`
	Result  describeCertificatesResult `xml:"DescribeCertificatesResult"`
}

type xmlDBClusterParameter struct {
	ParameterName  string `xml:"ParameterName"`
	ParameterValue string `xml:"ParameterValue,omitempty"`
	Description    string `xml:"Description,omitempty"`
	Source         string `xml:"Source,omitempty"`
	ApplyType      string `xml:"ApplyType,omitempty"`
	DataType       string `xml:"DataType,omitempty"`
	IsModifiable   bool   `xml:"IsModifiable"`
}

type xmlDBClusterParameterList struct {
	Members []xmlDBClusterParameter `xml:"Parameter"`
}

type describeDBClusterParametersResult struct {
	Parameters xmlDBClusterParameterList `xml:"Parameters"`
}

type describeDBClusterParametersResponse struct {
	XMLName xml.Name                          `xml:"DescribeDBClusterParametersResponse"`
	Xmlns   string                            `xml:"xmlns,attr"`
	Result  describeDBClusterParametersResult `xml:"DescribeDBClusterParametersResult"`
}

func toXMLEventSubscription(sub *EventSubscription) xmlEventSubscription {
	ids := make([]string, len(sub.SourceIDs))
	copy(ids, sub.SourceIDs)

	return xmlEventSubscription{
		SubscriptionName: sub.SubscriptionName,
		SnsTopicARN:      sub.SnsTopicARN,
		Status:           sub.Status,
		SourceIDsList:    xmlSourceIDList{Members: ids},
	}
}

func toXMLGlobalCluster(gc *GlobalCluster) xmlGlobalCluster {
	return xmlGlobalCluster{
		GlobalClusterIdentifier:   gc.GlobalClusterIdentifier,
		SourceDBClusterIdentifier: gc.SourceDBClusterID,
		Status:                    gc.Status,
	}
}

func toXMLCertificate(c *Certificate) xmlCertificate {
	return xmlCertificate{
		CertificateIdentifier: c.CertificateIdentifier,
		CertificateType:       c.CertificateType,
		Thumbprint:            c.Thumbprint,
		ValidFrom:             c.ValidFrom,
		ValidTill:             c.ValidTill,
	}
}

func toXMLDBClusterParameter(p *DBClusterParameter) xmlDBClusterParameter {
	return xmlDBClusterParameter{
		ParameterName:  p.ParameterName,
		ParameterValue: p.ParameterValue,
		Description:    p.Description,
		Source:         p.Source,
		ApplyType:      p.ApplyType,
		DataType:       p.DataType,
		IsModifiable:   p.IsModifiable,
	}
}

func parseSourceIDMembers(vals url.Values) []string {
	var ids []string
	for i := 1; ; i++ {
		id := vals.Get(fmt.Sprintf("SourceIds.SourceId.%d", i))
		if id == "" {
			return ids
		}
		ids = append(ids, id)
	}
}

func tagsToMap(tags []Tag) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

const defaultDocDBMaxRecords = 100

// applyDocDBMarker applies Marker/MaxRecords-based pagination to a slice.
// marker is the starting index as a string, maxRecordsStr is the limit as a string.
func applyDocDBMarker[T any](items []T, marker, maxRecordsStr string) ([]T, string) {
	start := 0
	if marker != "" {
		idx, err := strconv.Atoi(marker)
		if err == nil && idx > 0 {
			start = idx
		}
	}

	if start >= len(items) {
		return []T{}, ""
	}

	items = items[start:]

	limit := defaultDocDBMaxRecords
	if maxRecordsStr != "" {
		if n, err := strconv.Atoi(maxRecordsStr); err == nil && n > 0 {
			limit = n
		}
	}

	if len(items) <= limit {
		return items, ""
	}

	return items[:limit], strconv.Itoa(start + limit)
}
