package elasticache

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"
)

// ----------------------------------------
// CreateCacheSecurityGroup
// ----------------------------------------

type cacheSecurityGroupXML struct {
	ARN                    string `xml:"ARN"`
	CacheSecurityGroupName string `xml:"CacheSecurityGroupName"`
	Description            string `xml:"Description"`
	OwnerID                string `xml:"OwnerId"`
}

func cacheSecurityGroupToXML(sg *CacheSecurityGroup) cacheSecurityGroupXML {
	return cacheSecurityGroupXML{
		ARN:                    sg.ARN,
		CacheSecurityGroupName: sg.Name,
		Description:            sg.Description,
		OwnerID:                sg.OwnerID,
	}
}

func (h *Handler) createCacheSecurityGroup(ctx context.Context, c *echo.Context, form url.Values) error {
	name := form.Get("CacheSecurityGroupName")
	description := form.Get("Description")

	sg, err := h.Backend.CreateCacheSecurityGroup(ctx, name, description)
	if err != nil {
		if errors.Is(err, ErrCacheSecurityGroupAlreadyExists) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"CacheSecurityGroupAlreadyExists",
				"Cache security group already exists",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName            xml.Name              `xml:"CreateCacheSecurityGroupResponse"`
		Xmlns              string                `xml:"xmlns,attr"`
		CacheSecurityGroup cacheSecurityGroupXML `xml:"CreateCacheSecurityGroupResult>CacheSecurityGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:              elasticacheNS,
		CacheSecurityGroup: cacheSecurityGroupToXML(sg),
	})
}

// ----------------------------------------
// AuthorizeCacheSecurityGroupIngress
// ----------------------------------------

func (h *Handler) authorizeCacheSecurityGroupIngress(ctx context.Context, c *echo.Context, form url.Values) error {
	name := form.Get("CacheSecurityGroupName")
	ec2SecurityGroupName := form.Get("EC2SecurityGroupName")
	ec2SecurityGroupOwnerID := form.Get("EC2SecurityGroupOwnerId")

	sg, err := h.Backend.AuthorizeCacheSecurityGroupIngress(ctx, name, ec2SecurityGroupName, ec2SecurityGroupOwnerID)
	if err != nil {
		if errors.Is(err, ErrCacheSecurityGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheSecurityGroupNotFound", "Cache security group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName            xml.Name              `xml:"AuthorizeCacheSecurityGroupIngressResponse"`
		Xmlns              string                `xml:"xmlns,attr"`
		CacheSecurityGroup cacheSecurityGroupXML `xml:"AuthorizeCacheSecurityGroupIngressResult>CacheSecurityGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:              elasticacheNS,
		CacheSecurityGroup: cacheSecurityGroupToXML(sg),
	})
}

// ----------------------------------------
// CreateGlobalReplicationGroup
// ----------------------------------------

type globalReplicationGroupXML struct {
	GlobalReplicationGroupID          string `xml:"GlobalReplicationGroupId"`
	GlobalReplicationGroupDescription string `xml:"GlobalReplicationGroupDescription,omitempty"`
	Status                            string `xml:"Status"`
	ARN                               string `xml:"ARN"`
	Engine                            string `xml:"Engine,omitempty"`
	EngineVersion                     string `xml:"EngineVersion,omitempty"`
	NodeGroupCount                    int32  `xml:"NodeGroupCount,omitempty"`
}

func globalRGToXML(grg *GlobalReplicationGroup) globalReplicationGroupXML {
	return globalReplicationGroupXML{
		GlobalReplicationGroupID:          grg.GlobalReplicationGroupID,
		GlobalReplicationGroupDescription: grg.Description,
		Status:                            grg.Status,
		ARN:                               grg.ARN,
		Engine:                            grg.Engine,
		EngineVersion:                     grg.EngineVersion,
		NodeGroupCount:                    grg.NodeGroupCount,
	}
}

func (h *Handler) createGlobalReplicationGroup(ctx context.Context, c *echo.Context, form url.Values) error {
	suffix := form.Get("GlobalReplicationGroupIdSuffix")
	description := form.Get("GlobalReplicationGroupDescription")
	primaryReplicationGroupID := form.Get("PrimaryReplicationGroupId")

	grg, err := h.Backend.CreateGlobalReplicationGroup(ctx, suffix, description, primaryReplicationGroupID)
	if err != nil {
		if errors.Is(err, ErrGlobalReplicationGroupExists) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"GlobalReplicationGroupAlreadyExistsFault",
				"Global replication group already exists",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName                xml.Name                  `xml:"CreateGlobalReplicationGroupResponse"`
		Xmlns                  string                    `xml:"xmlns,attr"`
		GlobalReplicationGroup globalReplicationGroupXML `xml:"CreateGlobalReplicationGroupResult>GlobalReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                  elasticacheNS,
		GlobalReplicationGroup: globalRGToXML(grg),
	})
}

// ----------------------------------------
// CreateServerlessCache
// ----------------------------------------

type serverlessCacheEndpointXML struct {
	Address string `xml:"Address"`
	Port    int    `xml:"Port"`
}

type serverlessCacheXML struct {
	Endpoint       *serverlessCacheEndpointXML `xml:"Endpoint,omitempty"`
	ReaderEndpoint *serverlessCacheEndpointXML `xml:"ReaderEndpoint,omitempty"`
	ARN            string                      `xml:"ARN"`
	Name           string                      `xml:"ServerlessCacheName"`
	Description    string                      `xml:"Description,omitempty"`
	Status         string                      `xml:"Status"`
	Engine         string                      `xml:"Engine,omitempty"`
}

func serverlessCacheToXML(sc *ServerlessCache) serverlessCacheXML {
	x := serverlessCacheXML{
		ARN:         sc.ARN,
		Name:        sc.Name,
		Description: sc.Description,
		Status:      sc.Status,
		Engine:      sc.Engine,
	}
	if sc.Endpoint != nil {
		x.Endpoint = &serverlessCacheEndpointXML{Address: sc.Endpoint.Address, Port: sc.Endpoint.Port}
	}
	if sc.ReaderEndpoint != nil {
		x.ReaderEndpoint = &serverlessCacheEndpointXML{Address: sc.ReaderEndpoint.Address, Port: sc.ReaderEndpoint.Port}
	}

	return x
}

func (h *Handler) createServerlessCache(ctx context.Context, c *echo.Context, form url.Values) error {
	name := form.Get("ServerlessCacheName")
	description := form.Get("Description")
	engine := form.Get("Engine")

	sc, err := h.Backend.CreateServerlessCache(ctx, name, description, engine)
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
		XMLName         xml.Name           `xml:"CreateServerlessCacheResponse"`
		Xmlns           string             `xml:"xmlns,attr"`
		ServerlessCache serverlessCacheXML `xml:"CreateServerlessCacheResult>ServerlessCache"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:           elasticacheNS,
		ServerlessCache: serverlessCacheToXML(sc),
	})
}

// ----------------------------------------
// CreateServerlessCacheSnapshot
// ----------------------------------------

type serverlessCacheSnapshotXML struct {
	ARN                 string `xml:"ARN"`
	Name                string `xml:"ServerlessCacheSnapshotName"`
	Status              string `xml:"Status"`
	ServerlessCacheName string `xml:"ServerlessCacheName,omitempty"`
	SnapshotType        string `xml:"SnapshotType,omitempty"`
}

func serverlessCacheSnapshotToXML(snap *ServerlessCacheSnapshot) serverlessCacheSnapshotXML {
	return serverlessCacheSnapshotXML{
		ARN:                 snap.ARN,
		Name:                snap.Name,
		Status:              snap.Status,
		ServerlessCacheName: snap.ServerlessCacheName,
		SnapshotType:        snap.SnapshotType,
	}
}

func (h *Handler) createServerlessCacheSnapshot(ctx context.Context, c *echo.Context, form url.Values) error {
	snapshotName := form.Get("ServerlessCacheSnapshotName")
	serverlessCacheName := form.Get("ServerlessCacheName")

	snap, err := h.Backend.CreateServerlessCacheSnapshot(ctx, snapshotName, serverlessCacheName)
	if err != nil {
		if errors.Is(err, ErrServerlessCacheSnapshotExists) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"ServerlessCacheSnapshotAlreadyExistsFault",
				"Serverless cache snapshot already exists",
			)
		}
		if errors.Is(err, ErrServerlessCacheNotFound) {
			return xmlError(c, http.StatusBadRequest, "ServerlessCacheNotFound", "Serverless cache not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName                 xml.Name                   `xml:"CreateServerlessCacheSnapshotResponse"`
		Xmlns                   string                     `xml:"xmlns,attr"`
		ServerlessCacheSnapshot serverlessCacheSnapshotXML `xml:"CreateServerlessCacheSnapshotResult>ServerlessCacheSnapshot"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                   elasticacheNS,
		ServerlessCacheSnapshot: serverlessCacheSnapshotToXML(snap),
	})
}

// ----------------------------------------
// CopyServerlessCacheSnapshot
// ----------------------------------------

func (h *Handler) copyServerlessCacheSnapshot(ctx context.Context, c *echo.Context, form url.Values) error {
	sourceSnapshotName := form.Get("SourceServerlessCacheSnapshotName")
	targetSnapshotName := form.Get("TargetServerlessCacheSnapshotName")

	snap, err := h.Backend.CopyServerlessCacheSnapshot(ctx, sourceSnapshotName, targetSnapshotName)
	if err != nil {
		if errors.Is(err, ErrServerlessCacheSnapshotNotFound) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"ServerlessCacheSnapshotNotFoundFault",
				"Source serverless cache snapshot not found",
			)
		}
		if errors.Is(err, ErrServerlessCacheSnapshotExists) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"ServerlessCacheSnapshotAlreadyExistsFault",
				"Target serverless cache snapshot already exists",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName                 xml.Name                   `xml:"CopyServerlessCacheSnapshotResponse"`
		Xmlns                   string                     `xml:"xmlns,attr"`
		ServerlessCacheSnapshot serverlessCacheSnapshotXML `xml:"CopyServerlessCacheSnapshotResult>ServerlessCacheSnapshot"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                   elasticacheNS,
		ServerlessCacheSnapshot: serverlessCacheSnapshotToXML(snap),
	})
}

// ----------------------------------------
// CreateUser
// ----------------------------------------

func (h *Handler) createUser(ctx context.Context, c *echo.Context, form url.Values) error {
	userID := form.Get("UserId")
	userName := form.Get("UserName")
	accessString := form.Get("AccessString")
	engine := form.Get("Engine")
	noPasswordRequired := strings.EqualFold(form.Get("NoPasswordRequired"), "true")

	u, err := h.Backend.CreateUser(ctx, userID, userName, accessString, engine, noPasswordRequired)
	if err != nil {
		if errors.Is(err, ErrUserAlreadyExists) {
			return xmlError(c, http.StatusBadRequest, "UserAlreadyExists", "User already exists")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	// The SDK deserializer reads the user fields directly from CreateUserResult (not under a User element).
	type result struct {
		XMLName            xml.Name `xml:"CreateUserResponse"`
		Xmlns              string   `xml:"xmlns,attr"`
		ARN                string   `xml:"CreateUserResult>ARN"`
		UserID             string   `xml:"CreateUserResult>UserId"`
		UserName           string   `xml:"CreateUserResult>UserName"`
		Status             string   `xml:"CreateUserResult>Status"`
		Engine             string   `xml:"CreateUserResult>Engine,omitempty"`
		AccessString       string   `xml:"CreateUserResult>AccessString,omitempty"`
		NoPasswordRequired bool     `xml:"CreateUserResult>NoPasswordRequired"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:              elasticacheNS,
		ARN:                u.ARN,
		UserID:             u.UserID,
		UserName:           u.UserName,
		Status:             u.Status,
		Engine:             u.Engine,
		AccessString:       u.AccessString,
		NoPasswordRequired: u.NoPasswordRequired,
	})
}

// ----------------------------------------
// Shared batch update action types and helpers
// ----------------------------------------

type updateActionResultXML struct {
	ReplicationGroupID string `xml:"ReplicationGroupId,omitempty"`
	CacheClusterID     string `xml:"CacheClusterId,omitempty"`
	ServiceUpdateName  string `xml:"ServiceUpdateName"`
	UpdateActionStatus string `xml:"UpdateActionStatus"`
}

type processedUpdateActionsXML struct {
	ProcessedUpdateAction []updateActionResultXML `xml:"ProcessedUpdateAction"`
}

type unprocessedUpdateActionsXML struct {
	UnprocessedUpdateAction []updateActionResultXML `xml:"UnprocessedUpdateAction"`
}

// toBatchUpdateActionXMLLists converts a BatchUpdateResult to the XML list types used in responses.
func toBatchUpdateActionXMLLists(result *BatchUpdateResult) (processedUpdateActionsXML, unprocessedUpdateActionsXML) {
	processed := make([]updateActionResultXML, 0, len(result.ProcessedUpdateActions))
	for _, a := range result.ProcessedUpdateActions {
		processed = append(processed, updateActionResultXML(a))
	}

	unprocessed := make([]updateActionResultXML, 0, len(result.UnprocessedUpdateActions))
	for _, a := range result.UnprocessedUpdateActions {
		unprocessed = append(unprocessed, updateActionResultXML(a))
	}

	return processedUpdateActionsXML{ProcessedUpdateAction: processed},
		unprocessedUpdateActionsXML{UnprocessedUpdateAction: unprocessed}
}

// ----------------------------------------
// BatchApplyUpdateAction
// ----------------------------------------

func (h *Handler) batchApplyUpdateAction(ctx context.Context, c *echo.Context, form url.Values) error {
	serviceUpdateName := form.Get("ServiceUpdateName")
	replicationGroupIDs := parseRepeatedField(form, "ReplicationGroupIds.member")
	cacheClusterIDs := parseRepeatedField(form, "CacheClusterIds.member")

	result, err := h.Backend.BatchApplyUpdateAction(ctx, replicationGroupIDs, cacheClusterIDs, serviceUpdateName)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	processed, unprocessed := toBatchUpdateActionXMLLists(result)

	type resp struct {
		XMLName                  xml.Name                    `xml:"BatchApplyUpdateActionResponse"`
		Xmlns                    string                      `xml:"xmlns,attr"`
		ProcessedUpdateActions   processedUpdateActionsXML   `xml:"BatchApplyUpdateActionResult>ProcessedUpdateActions"`
		UnprocessedUpdateActions unprocessedUpdateActionsXML `xml:"BatchApplyUpdateActionResult>UnprocessedUpdateActions"`
	}

	return xmlResp(c, http.StatusOK, resp{
		Xmlns:                    elasticacheNS,
		ProcessedUpdateActions:   processed,
		UnprocessedUpdateActions: unprocessed,
	})
}

// ----------------------------------------
// BatchStopUpdateAction
// ----------------------------------------

func (h *Handler) batchStopUpdateAction(ctx context.Context, c *echo.Context, form url.Values) error {
	serviceUpdateName := form.Get("ServiceUpdateName")
	replicationGroupIDs := parseRepeatedField(form, "ReplicationGroupIds.member")
	cacheClusterIDs := parseRepeatedField(form, "CacheClusterIds.member")

	result, err := h.Backend.BatchStopUpdateAction(ctx, replicationGroupIDs, cacheClusterIDs, serviceUpdateName)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	processed, unprocessed := toBatchUpdateActionXMLLists(result)

	type resp struct {
		XMLName                  xml.Name                    `xml:"BatchStopUpdateActionResponse"`
		Xmlns                    string                      `xml:"xmlns,attr"`
		ProcessedUpdateActions   processedUpdateActionsXML   `xml:"BatchStopUpdateActionResult>ProcessedUpdateActions"`
		UnprocessedUpdateActions unprocessedUpdateActionsXML `xml:"BatchStopUpdateActionResult>UnprocessedUpdateActions"`
	}

	return xmlResp(c, http.StatusOK, resp{
		Xmlns:                    elasticacheNS,
		ProcessedUpdateActions:   processed,
		UnprocessedUpdateActions: unprocessed,
	})
}

// ----------------------------------------
// CompleteMigration
// ----------------------------------------

func (h *Handler) completeMigration(ctx context.Context, c *echo.Context, form url.Values) error {
	replicationGroupID := form.Get("ReplicationGroupId")
	force := strings.EqualFold(form.Get("Force"), "true")

	rg, err := h.Backend.CompleteMigration(ctx, replicationGroupID, force)
	if err != nil {
		if errors.Is(err, ErrReplicationGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "ReplicationGroupNotFound", "Replication group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName          xml.Name            `xml:"CompleteMigrationResponse"`
		Xmlns            string              `xml:"xmlns,attr"`
		ReplicationGroup replicationGroupXML `xml:"CompleteMigrationResult>ReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:            elasticacheNS,
		ReplicationGroup: rgToXML(*rg),
	})
}

// ----------------------------------------
// Helpers
// ----------------------------------------

// parseRepeatedField extracts a list of values from form fields with numeric suffixes.
// e.g., "ReplicationGroupIds.member.1", "ReplicationGroupIds.member.2", etc.
func parseRepeatedField(form url.Values, prefix string) []string {
	var items []string
	for i := 1; ; i++ {
		v := form.Get(fmt.Sprintf("%s.%d", prefix, i))
		if v == "" {
			break
		}
		items = append(items, v)
	}

	return items
}
