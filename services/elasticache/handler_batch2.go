package elasticache

import (
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

// ----------------------------------------
// Log delivery config XML (gap #9)
// ----------------------------------------

type logDeliveryDestinationDetailsXML struct {
	LogGroup       string `xml:"CloudWatchLogsDetails>LogGroup,omitempty"`
	DeliveryStream string `xml:"KinesisFirehoseDetails>DeliveryStream,omitempty"`
}

type logDeliveryConfigXML struct {
	DestinationDetails logDeliveryDestinationDetailsXML `xml:"DestinationDetails"`
	LogType            string                           `xml:"LogType,omitempty"`
	DestinationType    string                           `xml:"DestinationType,omitempty"`
	LogFormat          string                           `xml:"LogFormat,omitempty"`
	Status             string                           `xml:"Status,omitempty"`
	Message            string                           `xml:"Message,omitempty"`
}

type logDeliveryConfigsXML struct {
	LogDeliveryConfiguration []logDeliveryConfigXML `xml:"LogDeliveryConfiguration"`
}

func logDeliveryConfigsToXML(configs []LogDeliveryConfig) *logDeliveryConfigsXML {
	if len(configs) == 0 {
		return nil
	}

	items := make([]logDeliveryConfigXML, 0, len(configs))

	for _, c := range configs {
		item := logDeliveryConfigXML{
			LogType:         c.LogType,
			DestinationType: c.DestinationType,
			LogFormat:       c.LogFormat,
			Status:          c.Status,
			Message:         c.Message,
		}

		switch c.DestinationType {
		case "kinesis-firehose":
			item.DestinationDetails = logDeliveryDestinationDetailsXML{
				DeliveryStream: c.DestinationDetails,
			}
		default:
			item.DestinationDetails = logDeliveryDestinationDetailsXML{
				LogGroup: c.DestinationDetails,
			}
		}

		items = append(items, item)
	}

	return &logDeliveryConfigsXML{LogDeliveryConfiguration: items}
}

// ----------------------------------------
// Extended replicationGroupXML with log delivery
// ----------------------------------------

// replicationGroupWithLogDeliveryXML extends the base XML with log delivery configs.
// Used by create/modify/describe responses to include LogDeliveryConfigurations.
type replicationGroupWithLogDeliveryXML struct {
	PendingModifiedValues      *rgPendingModifiedXML  `xml:"PendingModifiedValues,omitempty"`
	NodeGroups                 *nodeGroupsListXML     `xml:"NodeGroups,omitempty"`
	UserGroupIDs               *rgUserGroupIDsXML     `xml:"UserGroupIds,omitempty"`
	LogDeliveryConfigurations  *logDeliveryConfigsXML `xml:"LogDeliveryConfigurations,omitempty"`
	ReplicationGroupID         string                 `xml:"ReplicationGroupId"`
	Description                string                 `xml:"Description"`
	Status                     string                 `xml:"Status"`
	ARN                        string                 `xml:"ARN"`
	Engine                     string                 `xml:"Engine,omitempty"`
	CacheParameterGroupName    string                 `xml:"CacheParameterGroupName,omitempty"`
	AutomaticFailover          string                 `xml:"AutomaticFailover,omitempty"`
	MultiAZ                    string                 `xml:"MultiAZ,omitempty"`
	CacheNodeType              string                 `xml:"CacheNodeType,omitempty"`
	SnapshotWindow             string                 `xml:"SnapshotWindow,omitempty"`
	PreferredMaintenanceWindow string                 `xml:"PreferredMaintenanceWindow,omitempty"`
	EngineVersion              string                 `xml:"EngineVersion,omitempty"`
	CreatedAt                  string                 `xml:"CreatingDate,omitempty"`
	KmsKeyID                   string                 `xml:"KmsKeyId,omitempty"`
	NotificationTopicArn       string                 `xml:"NotificationTopicArn,omitempty"`
	TransitEncryptionMode      string                 `xml:"TransitEncryptionMode,omitempty"`
	DataTiering                string                 `xml:"DataTiering,omitempty"`
	SnapshotRetentionLimit     int                    `xml:"SnapshotRetentionLimit,omitempty"`
	NumCacheClusters           int                    `xml:"NumCacheClusters,omitempty"`
	ClusterEnabled             bool                   `xml:"ClusterEnabled,omitempty"`
	AuthTokenEnabled           bool                   `xml:"AuthTokenEnabled,omitempty"`
	AtRestEncryptionEnabled    bool                   `xml:"AtRestEncryptionEnabled,omitempty"`
	TransitEncryptionEnabled   bool                   `xml:"TransitEncryptionEnabled,omitempty"`
}

func rgToXMLWithLogDelivery(rg ReplicationGroup) replicationGroupWithLogDeliveryXML {
	multiAZ := statusDisabled
	if rg.MultiAZEnabled {
		multiAZ = statusEnabled
	}

	autoFailover := rg.AutomaticFailover
	if autoFailover == "" {
		autoFailover = statusDisabled
	}

	numCacheClusters := int(rg.ReplicaCount) + 1
	if numCacheClusters <= 1 && !rg.ClusterModeEnabled {
		numCacheClusters = 1
	}

	var userGroupIDs *rgUserGroupIDsXML
	if len(rg.UserGroupIDs) > 0 {
		userGroupIDs = &rgUserGroupIDsXML{UserGroupID: rg.UserGroupIDs}
	}

	return replicationGroupWithLogDeliveryXML{
		ReplicationGroupID:         rg.ReplicationGroupID,
		Description:                rg.Description,
		Status:                     rg.Status,
		ARN:                        rg.ARN,
		Engine:                     rg.Engine,
		CacheParameterGroupName:    rg.CacheParameterGroupName,
		AutomaticFailover:          autoFailover,
		MultiAZ:                    multiAZ,
		CacheNodeType:              rg.CacheNodeType,
		SnapshotWindow:             rg.SnapshotWindow,
		PreferredMaintenanceWindow: rg.PreferredMaintenanceWindow,
		EngineVersion:              rg.EngineVersion,
		KmsKeyID:                   rg.KmsKeyID,
		NotificationTopicArn:       rg.NotificationTopicArn,
		TransitEncryptionMode:      rg.TransitEncryptionMode,
		SnapshotRetentionLimit:     rg.SnapshotRetentionLimit,
		NumCacheClusters:           numCacheClusters,
		ClusterEnabled:             rg.ClusterModeEnabled,
		AuthTokenEnabled:           rg.AuthTokenEnabled,
		AtRestEncryptionEnabled:    rg.AtRestEncryptionEnabled,
		TransitEncryptionEnabled:   rg.TransitEncryptionEnabled,
		DataTiering:                dataTieringStatus(rg.DataTieringEnabled),
		NodeGroups:                 nodeGroupsToXML(rg.NodeGroups),
		PendingModifiedValues:      pendingToXML(rg.PendingModifiedValues),
		UserGroupIDs:               userGroupIDs,
		LogDeliveryConfigurations:  logDeliveryConfigsToXML(rg.LogDeliveryConfigurations),
	}
}

// ----------------------------------------
// Extended serverlessCache XML with new fields
// ----------------------------------------

type serverlessCacheFullXML struct {
	Endpoint               *serverlessCacheEndpointXML `xml:"Endpoint,omitempty"`
	ReaderEndpoint         *serverlessCacheEndpointXML `xml:"ReaderEndpoint,omitempty"`
	ARN                    string                      `xml:"ARN"`
	Name                   string                      `xml:"ServerlessCacheName"`
	Description            string                      `xml:"Description,omitempty"`
	Status                 string                      `xml:"Status"`
	Engine                 string                      `xml:"Engine,omitempty"`
	KmsKeyId               string                      `xml:"KmsKeyId,omitempty"`
	UserGroupId            string                      `xml:"UserGroupId,omitempty"`
	SubnetGroupName        string                      `xml:"SubnetGroupName,omitempty"`
	DailySnapshotTime      string                      `xml:"DailySnapshotTime,omitempty"`
	MajorEngineVersion     string                      `xml:"MajorEngineVersion,omitempty"`
	SnapshotRetentionLimit int32                       `xml:"SnapshotRetentionLimit,omitempty"`
}

func serverlessCacheFullToXML(sc *ServerlessCache) serverlessCacheFullXML {
	x := serverlessCacheFullXML{
		ARN:                    sc.ARN,
		Name:                   sc.Name,
		Description:            sc.Description,
		Status:                 sc.Status,
		Engine:                 sc.Engine,
		KmsKeyId:               sc.KmsKeyId,
		UserGroupId:            sc.UserGroupId,
		SubnetGroupName:        sc.SubnetGroupName,
		DailySnapshotTime:      sc.DailySnapshotTime,
		MajorEngineVersion:     sc.MajorEngineVersion,
		SnapshotRetentionLimit: sc.SnapshotRetentionLimit,
	}

	if sc.Endpoint != nil {
		x.Endpoint = &serverlessCacheEndpointXML{Address: sc.Endpoint.Address, Port: sc.Endpoint.Port}
	}

	if sc.ReaderEndpoint != nil {
		x.ReaderEndpoint = &serverlessCacheEndpointXML{Address: sc.ReaderEndpoint.Address, Port: sc.ReaderEndpoint.Port}
	}

	return x
}

// ----------------------------------------
// createServerlessCacheFull handler
// ----------------------------------------

func (h *Handler) createServerlessCacheFull(c *echo.Context, form url.Values) error {
	name := form.Get("ServerlessCacheName")
	description := form.Get("Description")
	engine := form.Get("Engine")
	kmsKeyId := form.Get("KmsKeyId")
	userGroupId := form.Get("UserGroupId")
	subnetGroupName := form.Get("SubnetGroupName")
	dailySnapshotTime := form.Get("DailySnapshotTime")
	majorEngineVersion := form.Get("MajorEngineVersion")
	securityGroupIds := parseRepeatedField(form, "SecurityGroupIds.member")
	subnetIds := parseRepeatedField(form, "SubnetIds.member")

	snapshotRetention, _ := strconv.ParseInt(form.Get("SnapshotRetentionLimit"), 10, 32)

	opts := ServerlessCreateOpts{
		Name:                   name,
		Description:            description,
		Engine:                 engine,
		KmsKeyId:               kmsKeyId,
		UserGroupId:            userGroupId,
		SubnetGroupName:        subnetGroupName,
		DailySnapshotTime:      dailySnapshotTime,
		MajorEngineVersion:     majorEngineVersion,
		SecurityGroupIds:       securityGroupIds,
		SubnetIds:              subnetIds,
		SnapshotRetentionLimit: int32(snapshotRetention),
	}

	if initialTags := parseFormTags(form); len(initialTags) > 0 {
		opts.Tags = initialTags
	}

	sc, err := h.Backend.CreateServerlessCacheFull(opts)
	if err != nil {
		if errors.Is(err, ErrServerlessCacheAlreadyExists) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"ServerlessCacheAlreadyExistsFault",
				"Serverless cache already exists",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName         xml.Name               `xml:"CreateServerlessCacheResponse"`
		Xmlns           string                 `xml:"xmlns,attr"`
		ServerlessCache serverlessCacheFullXML `xml:"CreateServerlessCacheResult>ServerlessCache"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:           elasticacheNS,
		ServerlessCache: serverlessCacheFullToXML(sc),
	})
}

// ----------------------------------------
// modifyServerlessCacheFull handler
// ----------------------------------------

func (h *Handler) modifyServerlessCacheFull(c *echo.Context, form url.Values) error {
	name := form.Get("ServerlessCacheName")
	description := form.Get("Description")
	userGroupId := form.Get("UserGroupId")
	dailySnapshotTime := form.Get("DailySnapshotTime")
	securityGroupIds := parseRepeatedField(form, "SecurityGroupIds.member")
	removeUserGroup := strings.EqualFold(form.Get("RemoveUserGroup"), "true")

	var snapshotRetentionLimit *int32

	if raw := form.Get("SnapshotRetentionLimit"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 32)
		if err == nil {
			v32 := int32(v)
			snapshotRetentionLimit = &v32
		}
	}

	opts := ServerlessModifyOpts{
		Description:            description,
		UserGroupId:            userGroupId,
		DailySnapshotTime:      dailySnapshotTime,
		SecurityGroupIds:       securityGroupIds,
		RemoveUserGroup:        removeUserGroup,
		SnapshotRetentionLimit: snapshotRetentionLimit,
	}

	sc, err := h.Backend.ModifyServerlessCacheFull(name, opts)
	if err != nil {
		if errors.Is(err, ErrServerlessCacheNotFound) {
			return xmlError(c, http.StatusBadRequest, "ServerlessCacheNotFound", "Serverless cache not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName         xml.Name               `xml:"ModifyServerlessCacheResponse"`
		Xmlns           string                 `xml:"xmlns,attr"`
		ServerlessCache serverlessCacheFullXML `xml:"ModifyServerlessCacheResult>ServerlessCache"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:           elasticacheNS,
		ServerlessCache: serverlessCacheFullToXML(sc),
	})
}

// ----------------------------------------
// createCacheSubnetGroupFull handler (with VpcId)
// ----------------------------------------

func (h *Handler) createCacheSubnetGroupFull(c *echo.Context, form url.Values) error {
	name := form.Get("CacheSubnetGroupName")
	desc := form.Get("CacheSubnetGroupDescription")
	vpcId := form.Get("VpcId")
	subnetIDs := parseSubnetIDs(form)

	sg, err := h.Backend.CreateSubnetGroupFull(name, desc, vpcId, subnetIDs)
	if err != nil {
		if errors.Is(err, ErrSubnetGroupAlreadyExists) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"CacheSubnetGroupAlreadyExists",
				"Cache subnet group already exists",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	if initialTags := parseFormTags(form); len(initialTags) > 0 {
		_ = h.Backend.AddTagsToResource(sg.ARN, initialTags)
	}

	type result struct {
		XMLName          xml.Name            `xml:"CreateCacheSubnetGroupResponse"`
		Xmlns            string              `xml:"xmlns,attr"`
		CacheSubnetGroup cacheSubnetGroupXML `xml:"CreateCacheSubnetGroupResult>CacheSubnetGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:            elasticacheNS,
		CacheSubnetGroup: subnetGroupToXML(sg),
	})
}

// ----------------------------------------
// copySnapshotFull handler (with KmsKeyId)
// ----------------------------------------

func (h *Handler) copySnapshotFull(c *echo.Context, form url.Values) error {
	sourceSnapshotName := form.Get("SourceSnapshotName")
	targetSnapshotName := form.Get("TargetSnapshotName")
	kmsKeyId := form.Get("KmsKeyId")

	var snap *CacheSnapshot
	var err error

	if kmsKeyId != "" {
		snap, err = h.Backend.CopySnapshotFull(sourceSnapshotName, targetSnapshotName, kmsKeyId)
	} else {
		snap, err = h.Backend.CopySnapshot(sourceSnapshotName, targetSnapshotName)
	}

	if err != nil {
		if errors.Is(err, ErrSnapshotNotFound) {
			return xmlError(c, http.StatusBadRequest, "SnapshotNotFound", "Source snapshot not found")
		}

		if errors.Is(err, ErrSnapshotAlreadyExists) {
			return xmlError(c, http.StatusBadRequest, "SnapshotAlreadyExistsFault", "Target snapshot already exists")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName  xml.Name    `xml:"CopySnapshotResponse"`
		Xmlns    string      `xml:"xmlns,attr"`
		Snapshot snapshotXML `xml:"CopySnapshotResult>Snapshot"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:    elasticacheNS,
		Snapshot: snapshotToXML(snap),
	})
}
