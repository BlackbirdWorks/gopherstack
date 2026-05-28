package elasticache

import (
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

// describeUsersResultXML is the XML envelope for DescribeUsers responses.
type describeUsersResultXML struct {
	XMLName xml.Name `xml:"DescribeUsersResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Marker  string   `xml:"DescribeUsersResult>Marker,omitempty"`
	Users   struct {
		Member []userXML `xml:"member"`
	} `xml:"DescribeUsersResult>Users"`
}

// describeUserGroupsResultXML is the XML envelope for DescribeUserGroups responses.
type describeUserGroupsResultXML struct {
	XMLName    xml.Name `xml:"DescribeUserGroupsResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	Marker     string   `xml:"DescribeUserGroupsResult>Marker,omitempty"`
	UserGroups struct {
		Member []userGroupXML `xml:"member"`
	} `xml:"DescribeUserGroupsResult>UserGroups"`
}

// describeGlobalRGsResultXML is the XML envelope for DescribeGlobalReplicationGroups responses.
type describeGlobalRGsResultXML struct {
	XMLName                 xml.Name `xml:"DescribeGlobalReplicationGroupsResponse"`
	Xmlns                   string   `xml:"xmlns,attr"`
	Marker                  string   `xml:"DescribeGlobalReplicationGroupsResult>Marker,omitempty"`
	GlobalReplicationGroups struct {
		GlobalReplicationGroup []globalReplicationGroupXML `xml:"GlobalReplicationGroup"`
	} `xml:"DescribeGlobalReplicationGroupsResult>GlobalReplicationGroups"`
}

// describeRCNsResultXML is the XML envelope for DescribeReservedCacheNodes responses.
type describeRCNsResultXML struct {
	XMLName            xml.Name `xml:"DescribeReservedCacheNodesResponse"`
	Xmlns              string   `xml:"xmlns,attr"`
	Marker             string   `xml:"DescribeReservedCacheNodesResult>Marker,omitempty"`
	ReservedCacheNodes struct {
		ReservedCacheNode []reservedCacheNodeXML `xml:"ReservedCacheNode"`
	} `xml:"DescribeReservedCacheNodesResult>ReservedCacheNodes"`
}

// describeServerlessCachesResultXML is the XML envelope for DescribeServerlessCaches responses.
type describeServerlessCachesResultXML struct {
	XMLName          xml.Name `xml:"DescribeServerlessCachesResponse"`
	Xmlns            string   `xml:"xmlns,attr"`
	Marker           string   `xml:"DescribeServerlessCachesResult>Marker,omitempty"`
	ServerlessCaches struct {
		Member []serverlessCacheXML `xml:"member"`
	} `xml:"DescribeServerlessCachesResult>ServerlessCaches"`
}

// describeSGsResultXML is the XML envelope for DescribeCacheSecurityGroups responses.
type describeSGsResultXML struct {
	XMLName             xml.Name `xml:"DescribeCacheSecurityGroupsResponse"`
	Xmlns               string   `xml:"xmlns,attr"`
	Marker              string   `xml:"DescribeCacheSecurityGroupsResult>Marker,omitempty"`
	CacheSecurityGroups struct {
		CacheSecurityGroup []cacheSecurityGroupXML `xml:"CacheSecurityGroup"`
	} `xml:"DescribeCacheSecurityGroupsResult>CacheSecurityGroups"`
}

type userXML struct {
	ARN                string `xml:"ARN"`
	UserID             string `xml:"UserId"`
	UserName           string `xml:"UserName"`
	Status             string `xml:"Status"`
	Engine             string `xml:"Engine,omitempty"`
	AccessString       string `xml:"AccessString,omitempty"`
	NoPasswordRequired bool   `xml:"NoPasswordRequired"`
}

func userToXML(u *User) userXML {
	return userXML{
		ARN:                u.ARN,
		UserID:             u.UserID,
		UserName:           u.UserName,
		Status:             u.Status,
		Engine:             u.Engine,
		AccessString:       u.AccessString,
		NoPasswordRequired: u.NoPasswordRequired,
	}
}

type userGroupXML struct {
	ARN         string `xml:"ARN"`
	UserGroupID string `xml:"UserGroupId"`
	Description string `xml:"Description,omitempty"`
	Status      string `xml:"Status"`
	Engine      string `xml:"Engine,omitempty"`
	UserIDs     struct {
		Member []string `xml:"member"`
	} `xml:"UserIds"`
}

func userGroupToXML(ug *UserGroup) userGroupXML {
	x := userGroupXML{
		ARN:         ug.ARN,
		UserGroupID: ug.UserGroupID,
		Description: ug.Description,
		Status:      ug.Status,
		Engine:      ug.Engine,
	}
	x.UserIDs.Member = ug.UserIDs

	return x
}

type reservedCacheNodeXML struct {
	ReservationID       string  `xml:"ReservationId,omitempty"`
	ReservedCacheNodeID string  `xml:"ReservedCacheNodeId"`
	ARN                 string  `xml:"ReservationARN,omitempty"`
	CacheNodeType       string  `xml:"CacheNodeType"`
	ProductDescription  string  `xml:"ProductDescription"`
	OfferingType        string  `xml:"OfferingType"`
	State               string  `xml:"State"`
	OfferingID          string  `xml:"ReservedCacheNodesOfferingId"`
	StartTime           string  `xml:"StartTime,omitempty"`
	FixedPrice          float64 `xml:"FixedPrice"`
	UsagePrice          float64 `xml:"UsagePrice"`
	Duration            int32   `xml:"Duration"`
	CacheNodeCount      int32   `xml:"CacheNodeCount"`
}

func reservedCacheNodeToXML(rcn *ReservedCacheNode) reservedCacheNodeXML {
	startTime := ""
	if !rcn.StartTime.IsZero() {
		startTime = rcn.StartTime.UTC().Format(time.RFC3339)
	}

	return reservedCacheNodeXML{
		ReservationID:       rcn.ReservationID,
		ReservedCacheNodeID: rcn.ReservedCacheNodeID,
		ARN:                 rcn.ARN,
		CacheNodeType:       rcn.CacheNodeType,
		Duration:            rcn.Duration,
		FixedPrice:          rcn.FixedPrice,
		UsagePrice:          rcn.UsagePrice,
		CacheNodeCount:      rcn.CacheNodeCount,
		ProductDescription:  rcn.ProductDescription,
		OfferingType:        rcn.OfferingType,
		State:               rcn.State,
		OfferingID:          rcn.OfferingID,
		StartTime:           startTime,
	}
}

type reservedCacheNodesOfferingXML struct {
	OfferingID         string  `xml:"ReservedCacheNodesOfferingId"`
	CacheNodeType      string  `xml:"CacheNodeType"`
	ProductDescription string  `xml:"ProductDescription"`
	OfferingType       string  `xml:"OfferingType"`
	FixedPrice         float64 `xml:"FixedPrice"`
	UsagePrice         float64 `xml:"UsagePrice"`
	Duration           int32   `xml:"Duration"`
}

type cacheEngineVersionXML struct {
	Engine                        string `xml:"Engine"`
	EngineVersion                 string `xml:"EngineVersion"`
	CacheParameterGroupFamily     string `xml:"CacheParameterGroupFamily"`
	CacheEngineDescription        string `xml:"CacheEngineDescription"`
	CacheEngineVersionDescription string `xml:"CacheEngineVersionDescription"`
}

type serviceUpdateXML struct {
	ServiceUpdateName   string `xml:"ServiceUpdateName"`
	ServiceUpdateStatus string `xml:"ServiceUpdateStatus"`
}

type updateActionXML struct {
	ReplicationGroupID string `xml:"ReplicationGroupId,omitempty"`
	CacheClusterID     string `xml:"CacheClusterId,omitempty"`
	ServiceUpdateName  string `xml:"ServiceUpdateName"`
	UpdateActionStatus string `xml:"UpdateActionStatus"`
}

func (h *Handler) deleteUser(c *echo.Context, form url.Values) error {
	userID := form.Get("UserId")

	u, err := h.Backend.DeleteUser(userID)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return xmlError(c, http.StatusBadRequest, "UserNotFound", "User not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName            xml.Name `xml:"DeleteUserResponse"`
		Xmlns              string   `xml:"xmlns,attr"`
		ARN                string   `xml:"DeleteUserResult>ARN"`
		UserID             string   `xml:"DeleteUserResult>UserId"`
		UserName           string   `xml:"DeleteUserResult>UserName"`
		Status             string   `xml:"DeleteUserResult>Status"`
		Engine             string   `xml:"DeleteUserResult>Engine,omitempty"`
		AccessString       string   `xml:"DeleteUserResult>AccessString,omitempty"`
		NoPasswordRequired bool     `xml:"DeleteUserResult>NoPasswordRequired"`
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

func (h *Handler) describeUsers(c *echo.Context, form url.Values) error {
	userID := form.Get("UserId")
	marker, maxRecords := parsePagination(form)

	p, err := h.Backend.DescribeUsers(userID, marker, maxRecords)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return xmlError(c, http.StatusBadRequest, "UserNotFound", "User not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	var res describeUsersResultXML
	res.Xmlns = elasticacheNS
	res.Marker = p.Next

	for i := range p.Data {
		res.Users.Member = append(res.Users.Member, userToXML(&p.Data[i]))
	}

	return xmlResp(c, http.StatusOK, res)
}

func (h *Handler) modifyUser(c *echo.Context, form url.Values) error {
	userID := form.Get("UserId")
	accessString := form.Get("AccessString")
	noPasswordRequired := strings.EqualFold(form.Get("NoPasswordRequired"), "true")

	u, err := h.Backend.ModifyUser(userID, accessString, noPasswordRequired)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return xmlError(c, http.StatusBadRequest, "UserNotFound", "User not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName            xml.Name `xml:"ModifyUserResponse"`
		Xmlns              string   `xml:"xmlns,attr"`
		ARN                string   `xml:"ModifyUserResult>ARN"`
		UserID             string   `xml:"ModifyUserResult>UserId"`
		UserName           string   `xml:"ModifyUserResult>UserName"`
		Status             string   `xml:"ModifyUserResult>Status"`
		Engine             string   `xml:"ModifyUserResult>Engine,omitempty"`
		AccessString       string   `xml:"ModifyUserResult>AccessString,omitempty"`
		NoPasswordRequired bool     `xml:"ModifyUserResult>NoPasswordRequired"`
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

func (h *Handler) createUserGroup(c *echo.Context, form url.Values) error {
	groupID := form.Get("UserGroupId")
	description := form.Get("Description")
	engine := form.Get("Engine")
	userIDs := parseRepeatedField(form, "UserIds.member")

	ug, err := h.Backend.CreateUserGroup(groupID, description, engine, userIDs)
	if err != nil {
		if errors.Is(err, ErrUserGroupAlreadyExists) {
			return xmlError(c, http.StatusBadRequest, "UserGroupAlreadyExistsFault", "User group already exists")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName     xml.Name `xml:"CreateUserGroupResponse"`
		Xmlns       string   `xml:"xmlns,attr"`
		ARN         string   `xml:"CreateUserGroupResult>ARN"`
		UserGroupID string   `xml:"CreateUserGroupResult>UserGroupId"`
		Description string   `xml:"CreateUserGroupResult>Description,omitempty"`
		Status      string   `xml:"CreateUserGroupResult>Status"`
		Engine      string   `xml:"CreateUserGroupResult>Engine,omitempty"`
		UserIDs     struct {
			Member []string `xml:"member"`
		} `xml:"CreateUserGroupResult>UserIds"`
	}

	x := userGroupToXML(ug)
	r := result{
		Xmlns:       elasticacheNS,
		ARN:         x.ARN,
		UserGroupID: x.UserGroupID,
		Description: x.Description,
		Status:      x.Status,
		Engine:      x.Engine,
	}
	r.UserIDs.Member = x.UserIDs.Member

	return xmlResp(c, http.StatusOK, r)
}

func (h *Handler) deleteUserGroup(c *echo.Context, form url.Values) error {
	groupID := form.Get("UserGroupId")

	ug, err := h.Backend.DeleteUserGroup(groupID)
	if err != nil {
		if errors.Is(err, ErrUserGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "UserGroupNotFound", "User group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName     xml.Name `xml:"DeleteUserGroupResponse"`
		Xmlns       string   `xml:"xmlns,attr"`
		ARN         string   `xml:"DeleteUserGroupResult>ARN"`
		UserGroupID string   `xml:"DeleteUserGroupResult>UserGroupId"`
		Description string   `xml:"DeleteUserGroupResult>Description,omitempty"`
		Status      string   `xml:"DeleteUserGroupResult>Status"`
		Engine      string   `xml:"DeleteUserGroupResult>Engine,omitempty"`
		UserIDs     struct {
			Member []string `xml:"member"`
		} `xml:"DeleteUserGroupResult>UserIds"`
	}

	x := userGroupToXML(ug)
	r := result{
		Xmlns:       elasticacheNS,
		ARN:         x.ARN,
		UserGroupID: x.UserGroupID,
		Description: x.Description,
		Status:      x.Status,
		Engine:      x.Engine,
	}
	r.UserIDs.Member = x.UserIDs.Member

	return xmlResp(c, http.StatusOK, r)
}

func (h *Handler) describeUserGroups(c *echo.Context, form url.Values) error {
	groupID := form.Get("UserGroupId")
	marker, maxRecords := parsePagination(form)

	p, err := h.Backend.DescribeUserGroups(groupID, marker, maxRecords)
	if err != nil {
		if errors.Is(err, ErrUserGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "UserGroupNotFound", "User group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	var res describeUserGroupsResultXML
	res.Xmlns = elasticacheNS
	res.Marker = p.Next

	for i := range p.Data {
		res.UserGroups.Member = append(res.UserGroups.Member, userGroupToXML(&p.Data[i]))
	}

	return xmlResp(c, http.StatusOK, res)
}

func (h *Handler) modifyUserGroup(c *echo.Context, form url.Values) error {
	groupID := form.Get("UserGroupId")
	userIDsToAdd := parseRepeatedField(form, "UserIdsToAdd.member")
	userIDsToRemove := parseRepeatedField(form, "UserIdsToRemove.member")

	ug, err := h.Backend.ModifyUserGroup(groupID, userIDsToAdd, userIDsToRemove)
	if err != nil {
		if errors.Is(err, ErrUserGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "UserGroupNotFound", "User group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName     xml.Name `xml:"ModifyUserGroupResponse"`
		Xmlns       string   `xml:"xmlns,attr"`
		ARN         string   `xml:"ModifyUserGroupResult>ARN"`
		UserGroupID string   `xml:"ModifyUserGroupResult>UserGroupId"`
		Description string   `xml:"ModifyUserGroupResult>Description,omitempty"`
		Status      string   `xml:"ModifyUserGroupResult>Status"`
		Engine      string   `xml:"ModifyUserGroupResult>Engine,omitempty"`
		UserIDs     struct {
			Member []string `xml:"member"`
		} `xml:"ModifyUserGroupResult>UserIds"`
	}

	x := userGroupToXML(ug)
	r := result{
		Xmlns:       elasticacheNS,
		ARN:         x.ARN,
		UserGroupID: x.UserGroupID,
		Description: x.Description,
		Status:      x.Status,
		Engine:      x.Engine,
	}
	r.UserIDs.Member = x.UserIDs.Member

	return xmlResp(c, http.StatusOK, r)
}

func (h *Handler) deleteGlobalReplicationGroup(c *echo.Context, form url.Values) error {
	id := form.Get("GlobalReplicationGroupId")
	retainPrimary := strings.EqualFold(form.Get("RetainPrimaryReplicationGroup"), "true")

	grg, err := h.Backend.DeleteGlobalReplicationGroup(id, retainPrimary)
	if err != nil {
		if errors.Is(err, ErrGlobalReplicationGroupNotFound) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"GlobalReplicationGroupNotFoundFault",
				"Global replication group not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName                xml.Name                  `xml:"DeleteGlobalReplicationGroupResponse"`
		Xmlns                  string                    `xml:"xmlns,attr"`
		GlobalReplicationGroup globalReplicationGroupXML `xml:"DeleteGlobalReplicationGroupResult>GlobalReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                  elasticacheNS,
		GlobalReplicationGroup: globalRGToXML(grg),
	})
}

func (h *Handler) describeGlobalReplicationGroups(c *echo.Context, form url.Values) error {
	id := form.Get("GlobalReplicationGroupId")
	marker, maxRecords := parsePagination(form)

	p, err := h.Backend.DescribeGlobalReplicationGroups(id, marker, maxRecords)
	if err != nil {
		if errors.Is(err, ErrGlobalReplicationGroupNotFound) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"GlobalReplicationGroupNotFoundFault",
				"Global replication group not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	var res describeGlobalRGsResultXML
	res.Xmlns = elasticacheNS
	res.Marker = p.Next

	for i := range p.Data {
		res.GlobalReplicationGroups.GlobalReplicationGroup = append(
			res.GlobalReplicationGroups.GlobalReplicationGroup,
			globalRGToXML(&p.Data[i]),
		)
	}

	return xmlResp(c, http.StatusOK, res)
}

func (h *Handler) disassociateGlobalReplicationGroup(c *echo.Context, form url.Values) error {
	id := form.Get("GlobalReplicationGroupId")
	replicationGroupID := form.Get("ReplicationGroupId")
	replicationGroupRegion := form.Get("ReplicationGroupRegion")

	grg, err := h.Backend.DisassociateGlobalReplicationGroup(id, replicationGroupID, replicationGroupRegion)
	if err != nil {
		if errors.Is(err, ErrGlobalReplicationGroupNotFound) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"GlobalReplicationGroupNotFoundFault",
				"Global replication group not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName xml.Name                  `xml:"DisassociateGlobalReplicationGroupResponse"`
		Xmlns   string                    `xml:"xmlns,attr"`
		GRG     globalReplicationGroupXML `xml:"DisassociateGlobalReplicationGroupResult>GlobalReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns: elasticacheNS,
		GRG:   globalRGToXML(grg),
	})
}

func (h *Handler) failoverGlobalReplicationGroup(c *echo.Context, form url.Values) error {
	id := form.Get("GlobalReplicationGroupId")
	primaryRegion := form.Get("PrimaryRegion")
	primaryReplicationGroupID := form.Get("PrimaryReplicationGroupId")

	grg, err := h.Backend.FailoverGlobalReplicationGroup(id, primaryRegion, primaryReplicationGroupID)
	if err != nil {
		if errors.Is(err, ErrGlobalReplicationGroupNotFound) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"GlobalReplicationGroupNotFoundFault",
				"Global replication group not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName                xml.Name                  `xml:"FailoverGlobalReplicationGroupResponse"`
		Xmlns                  string                    `xml:"xmlns,attr"`
		GlobalReplicationGroup globalReplicationGroupXML `xml:"FailoverGlobalReplicationGroupResult>GlobalReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                  elasticacheNS,
		GlobalReplicationGroup: globalRGToXML(grg),
	})
}

func (h *Handler) increaseNodeGroupsInGlobalReplicationGroup(c *echo.Context, form url.Values) error {
	id := form.Get("GlobalReplicationGroupId")
	nodeGroupCount, _ := strconv.ParseInt(form.Get("NodeGroupCount"), 10, 32)

	grg, err := h.Backend.IncreaseNodeGroupsInGlobalReplicationGroup(id, int32(nodeGroupCount))
	if err != nil {
		if errors.Is(err, ErrGlobalReplicationGroupNotFound) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"GlobalReplicationGroupNotFoundFault",
				"Global replication group not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName xml.Name                  `xml:"IncreaseNodeGroupsInGlobalReplicationGroupResponse"`
		Xmlns   string                    `xml:"xmlns,attr"`
		GRG     globalReplicationGroupXML `xml:"IncreaseNodeGroupsInGlobalReplicationGroupResult>GlobalReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns: elasticacheNS,
		GRG:   globalRGToXML(grg),
	})
}

func (h *Handler) decreaseNodeGroupsInGlobalReplicationGroup(c *echo.Context, form url.Values) error {
	id := form.Get("GlobalReplicationGroupId")
	nodeGroupCount, _ := strconv.ParseInt(form.Get("NodeGroupCount"), 10, 32)

	grg, err := h.Backend.DecreaseNodeGroupsInGlobalReplicationGroup(id, int32(nodeGroupCount))
	if err != nil {
		if errors.Is(err, ErrGlobalReplicationGroupNotFound) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"GlobalReplicationGroupNotFoundFault",
				"Global replication group not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName xml.Name                  `xml:"DecreaseNodeGroupsInGlobalReplicationGroupResponse"`
		Xmlns   string                    `xml:"xmlns,attr"`
		GRG     globalReplicationGroupXML `xml:"DecreaseNodeGroupsInGlobalReplicationGroupResult>GlobalReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns: elasticacheNS,
		GRG:   globalRGToXML(grg),
	})
}

func (h *Handler) modifyGlobalReplicationGroup(c *echo.Context, form url.Values) error {
	id := form.Get("GlobalReplicationGroupId")
	description := form.Get("GlobalReplicationGroupDescription")
	engineVersion := form.Get("EngineVersion")
	automaticFailoverEnabled := strings.EqualFold(form.Get("AutomaticFailoverEnabled"), "true")

	grg, err := h.Backend.ModifyGlobalReplicationGroup(id, description, engineVersion, automaticFailoverEnabled)
	if err != nil {
		if errors.Is(err, ErrGlobalReplicationGroupNotFound) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"GlobalReplicationGroupNotFoundFault",
				"Global replication group not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName                xml.Name                  `xml:"ModifyGlobalReplicationGroupResponse"`
		Xmlns                  string                    `xml:"xmlns,attr"`
		GlobalReplicationGroup globalReplicationGroupXML `xml:"ModifyGlobalReplicationGroupResult>GlobalReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                  elasticacheNS,
		GlobalReplicationGroup: globalRGToXML(grg),
	})
}

func (h *Handler) rebalanceSlotsInGlobalReplicationGroup(c *echo.Context, form url.Values) error {
	id := form.Get("GlobalReplicationGroupId")

	grg, err := h.Backend.RebalanceSlotsInGlobalReplicationGroup(id)
	if err != nil {
		if errors.Is(err, ErrGlobalReplicationGroupNotFound) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"GlobalReplicationGroupNotFoundFault",
				"Global replication group not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName xml.Name                  `xml:"RebalanceSlotsInGlobalReplicationGroupResponse"`
		Xmlns   string                    `xml:"xmlns,attr"`
		GRG     globalReplicationGroupXML `xml:"RebalanceSlotsInGlobalReplicationGroupResult>GlobalReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns: elasticacheNS,
		GRG:   globalRGToXML(grg),
	})
}

func (h *Handler) describeReservedCacheNodes(c *echo.Context, form url.Values) error {
	id := form.Get("ReservedCacheNodeId")
	cacheNodeType := form.Get("CacheNodeType")
	offeringType := form.Get("OfferingType")
	marker, maxRecords := parsePagination(form)

	p, err := h.Backend.DescribeReservedCacheNodes(id, cacheNodeType, offeringType, marker, maxRecords)
	if err != nil {
		if errors.Is(err, ErrReservedCacheNodeNotFound) {
			return xmlError(c, http.StatusBadRequest, "ReservedCacheNodeNotFound", "Reserved cache node not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	var res describeRCNsResultXML
	res.Xmlns = elasticacheNS
	res.Marker = p.Next

	for i := range p.Data {
		res.ReservedCacheNodes.ReservedCacheNode = append(
			res.ReservedCacheNodes.ReservedCacheNode,
			reservedCacheNodeToXML(&p.Data[i]),
		)
	}

	return xmlResp(c, http.StatusOK, res)
}

func (h *Handler) describeReservedCacheNodesOfferings(c *echo.Context, form url.Values) error {
	offeringID := form.Get("ReservedCacheNodesOfferingId")
	cacheNodeType := form.Get("CacheNodeType")
	offeringType := form.Get("OfferingType")
	marker, maxRecords := parsePagination(form)

	p, err := h.Backend.DescribeReservedCacheNodesOfferings(offeringID, cacheNodeType, offeringType, marker, maxRecords)
	if err != nil {
		if errors.Is(err, ErrReservedCacheNodesOfferingNotFound) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"ReservedCacheNodesOfferingNotFound",
				"Reserved cache nodes offering not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	items := make([]reservedCacheNodesOfferingXML, 0, len(p.Data))
	for _, o := range p.Data {
		items = append(items, reservedCacheNodesOfferingXML(o))
	}

	type offeringsListXML struct {
		ReservedCacheNodesOffering []reservedCacheNodesOfferingXML `xml:"ReservedCacheNodesOffering"`
	}

	type result struct {
		XMLName   xml.Name         `xml:"DescribeReservedCacheNodesOfferingsResponse"`
		Xmlns     string           `xml:"xmlns,attr"`
		Marker    string           `xml:"DescribeReservedCacheNodesOfferingsResult>Marker,omitempty"`
		Offerings offeringsListXML `xml:"DescribeReservedCacheNodesOfferingsResult>ReservedCacheNodesOfferings"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:     elasticacheNS,
		Marker:    p.Next,
		Offerings: offeringsListXML{ReservedCacheNodesOffering: items},
	})
}

func (h *Handler) purchaseReservedCacheNodesOffering(c *echo.Context, form url.Values) error {
	offeringID := form.Get("ReservedCacheNodesOfferingId")
	reservedCacheNodeID := form.Get("ReservedCacheNodeId")
	count, _ := strconv.ParseInt(form.Get("CacheNodeCount"), 10, 32)

	rcn, err := h.Backend.PurchaseReservedCacheNodesOffering(offeringID, reservedCacheNodeID, int32(count))
	if err != nil {
		if errors.Is(err, ErrReservedCacheNodesOfferingNotFound) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"ReservedCacheNodesOfferingNotFound",
				"Reserved cache nodes offering not found",
			)
		}

		if errors.Is(err, ErrReservedCacheNodeAlreadyExists) {
			return xmlError(c, http.StatusConflict, "ReservedCacheNodeAlreadyExists", err.Error())
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName           xml.Name             `xml:"PurchaseReservedCacheNodesOfferingResponse"`
		Xmlns             string               `xml:"xmlns,attr"`
		ReservedCacheNode reservedCacheNodeXML `xml:"PurchaseReservedCacheNodesOfferingResult>ReservedCacheNode"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:             elasticacheNS,
		ReservedCacheNode: reservedCacheNodeToXML(rcn),
	})
}

func (h *Handler) deleteServerlessCache(c *echo.Context, form url.Values) error {
	name := form.Get("ServerlessCacheName")

	sc, err := h.Backend.DeleteServerlessCache(name)
	if err != nil {
		if errors.Is(err, ErrServerlessCacheNotFound) {
			return xmlError(c, http.StatusBadRequest, "ServerlessCacheNotFound", "Serverless cache not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName         xml.Name           `xml:"DeleteServerlessCacheResponse"`
		Xmlns           string             `xml:"xmlns,attr"`
		ServerlessCache serverlessCacheXML `xml:"DeleteServerlessCacheResult>ServerlessCache"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:           elasticacheNS,
		ServerlessCache: serverlessCacheToXML(sc),
	})
}

func (h *Handler) deleteServerlessCacheSnapshot(c *echo.Context, form url.Values) error {
	name := form.Get("ServerlessCacheSnapshotName")

	snap, err := h.Backend.DeleteServerlessCacheSnapshot(name)
	if err != nil {
		if errors.Is(err, ErrServerlessCacheSnapshotNotFound) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"ServerlessCacheSnapshotNotFoundFault",
				"Serverless cache snapshot not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName                 xml.Name                   `xml:"DeleteServerlessCacheSnapshotResponse"`
		Xmlns                   string                     `xml:"xmlns,attr"`
		ServerlessCacheSnapshot serverlessCacheSnapshotXML `xml:"DeleteServerlessCacheSnapshotResult>ServerlessCacheSnapshot"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                   elasticacheNS,
		ServerlessCacheSnapshot: serverlessCacheSnapshotToXML(snap),
	})
}

func (h *Handler) describeServerlessCaches(c *echo.Context, form url.Values) error {
	name := form.Get("ServerlessCacheName")
	marker, maxRecords := parsePagination(form)

	p, err := h.Backend.DescribeServerlessCaches(name, marker, maxRecords)
	if err != nil {
		if errors.Is(err, ErrServerlessCacheNotFound) {
			return xmlError(c, http.StatusBadRequest, "ServerlessCacheNotFound", "Serverless cache not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	var res describeServerlessCachesResultXML
	res.Xmlns = elasticacheNS
	res.Marker = p.Next

	for i := range p.Data {
		res.ServerlessCaches.Member = append(res.ServerlessCaches.Member, serverlessCacheToXML(&p.Data[i]))
	}

	return xmlResp(c, http.StatusOK, res)
}

func (h *Handler) describeServerlessCacheSnapshots(c *echo.Context, form url.Values) error {
	serverlessCacheName := form.Get("ServerlessCacheName")
	snapshotName := form.Get("ServerlessCacheSnapshotName")
	marker, maxRecords := parsePagination(form)

	p, err := h.Backend.DescribeServerlessCacheSnapshots(serverlessCacheName, snapshotName, marker, maxRecords)
	if err != nil {
		if errors.Is(err, ErrServerlessCacheSnapshotNotFound) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"ServerlessCacheSnapshotNotFoundFault",
				"Serverless cache snapshot not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	items := make([]serverlessCacheSnapshotXML, 0, len(p.Data))
	for i := range p.Data {
		items = append(items, serverlessCacheSnapshotToXML(&p.Data[i]))
	}

	type scsListXML struct {
		ServerlessCacheSnapshot []serverlessCacheSnapshotXML `xml:"ServerlessCacheSnapshot"`
	}

	type result struct {
		XMLName                  xml.Name   `xml:"DescribeServerlessCacheSnapshotsResponse"`
		Xmlns                    string     `xml:"xmlns,attr"`
		Marker                   string     `xml:"DescribeServerlessCacheSnapshotsResult>Marker,omitempty"`
		ServerlessCacheSnapshots scsListXML `xml:"DescribeServerlessCacheSnapshotsResult>ServerlessCacheSnapshots"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                    elasticacheNS,
		Marker:                   p.Next,
		ServerlessCacheSnapshots: scsListXML{ServerlessCacheSnapshot: items},
	})
}

func (h *Handler) exportServerlessCacheSnapshot(c *echo.Context, form url.Values) error {
	snapshotName := form.Get("ServerlessCacheSnapshotName")
	s3BucketName := form.Get("S3BucketName")

	snap, err := h.Backend.ExportServerlessCacheSnapshot(snapshotName, s3BucketName)
	if err != nil {
		if errors.Is(err, ErrServerlessCacheSnapshotNotFound) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"ServerlessCacheSnapshotNotFoundFault",
				"Serverless cache snapshot not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName                 xml.Name                   `xml:"ExportServerlessCacheSnapshotResponse"`
		Xmlns                   string                     `xml:"xmlns,attr"`
		ServerlessCacheSnapshot serverlessCacheSnapshotXML `xml:"ExportServerlessCacheSnapshotResult>ServerlessCacheSnapshot"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                   elasticacheNS,
		ServerlessCacheSnapshot: serverlessCacheSnapshotToXML(snap),
	})
}

func (h *Handler) modifyServerlessCache(c *echo.Context, form url.Values) error {
	name := form.Get("ServerlessCacheName")
	description := form.Get("Description")

	sc, err := h.Backend.ModifyServerlessCache(name, description)
	if err != nil {
		if errors.Is(err, ErrServerlessCacheNotFound) {
			return xmlError(c, http.StatusBadRequest, "ServerlessCacheNotFound", "Serverless cache not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName         xml.Name           `xml:"ModifyServerlessCacheResponse"`
		Xmlns           string             `xml:"xmlns,attr"`
		ServerlessCache serverlessCacheXML `xml:"ModifyServerlessCacheResult>ServerlessCache"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:           elasticacheNS,
		ServerlessCache: serverlessCacheToXML(sc),
	})
}

func (h *Handler) startMigration(c *echo.Context, form url.Values) error {
	replicationGroupID := form.Get("ReplicationGroupId")

	rg, err := h.Backend.StartMigration(replicationGroupID)
	if err != nil {
		if errors.Is(err, ErrReplicationGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "ReplicationGroupNotFound", "Replication group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName          xml.Name            `xml:"StartMigrationResponse"`
		Xmlns            string              `xml:"xmlns,attr"`
		ReplicationGroup replicationGroupXML `xml:"StartMigrationResult>ReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:            elasticacheNS,
		ReplicationGroup: rgToXML(*rg),
	})
}

func (h *Handler) testMigration(c *echo.Context, form url.Values) error {
	replicationGroupID := form.Get("ReplicationGroupId")

	rg, err := h.Backend.TestMigration(replicationGroupID)
	if err != nil {
		if errors.Is(err, ErrReplicationGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "ReplicationGroupNotFound", "Replication group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName          xml.Name            `xml:"TestMigrationResponse"`
		Xmlns            string              `xml:"xmlns,attr"`
		ReplicationGroup replicationGroupXML `xml:"TestMigrationResult>ReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:            elasticacheNS,
		ReplicationGroup: rgToXML(*rg),
	})
}

func (h *Handler) increaseReplicaCount(c *echo.Context, form url.Values) error {
	replicationGroupID := form.Get("ReplicationGroupId")
	newReplicaCount, _ := strconv.ParseInt(form.Get("NewReplicaCount"), 10, 32)

	rg, err := h.Backend.IncreaseReplicaCount(replicationGroupID, int32(newReplicaCount))
	if err != nil {
		if errors.Is(err, ErrReplicationGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "ReplicationGroupNotFound", "Replication group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName          xml.Name            `xml:"IncreaseReplicaCountResponse"`
		Xmlns            string              `xml:"xmlns,attr"`
		ReplicationGroup replicationGroupXML `xml:"IncreaseReplicaCountResult>ReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:            elasticacheNS,
		ReplicationGroup: rgToXML(*rg),
	})
}

func (h *Handler) decreaseReplicaCount(c *echo.Context, form url.Values) error {
	replicationGroupID := form.Get("ReplicationGroupId")
	newReplicaCount, _ := strconv.ParseInt(form.Get("NewReplicaCount"), 10, 32)

	rg, err := h.Backend.DecreaseReplicaCount(replicationGroupID, int32(newReplicaCount))
	if err != nil {
		if errors.Is(err, ErrReplicationGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "ReplicationGroupNotFound", "Replication group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName          xml.Name            `xml:"DecreaseReplicaCountResponse"`
		Xmlns            string              `xml:"xmlns,attr"`
		ReplicationGroup replicationGroupXML `xml:"DecreaseReplicaCountResult>ReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:            elasticacheNS,
		ReplicationGroup: rgToXML(*rg),
	})
}

func (h *Handler) modifyReplicationGroupShardConfiguration(c *echo.Context, form url.Values) error {
	replicationGroupID := form.Get("ReplicationGroupId")
	nodeGroupCount, _ := strconv.ParseInt(form.Get("NodeGroupCount"), 10, 32)

	rg, err := h.Backend.ModifyReplicationGroupShardConfiguration(replicationGroupID, int32(nodeGroupCount))
	if err != nil {
		if errors.Is(err, ErrReplicationGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "ReplicationGroupNotFound", "Replication group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName          xml.Name            `xml:"ModifyReplicationGroupShardConfigurationResponse"`
		Xmlns            string              `xml:"xmlns,attr"`
		ReplicationGroup replicationGroupXML `xml:"ModifyReplicationGroupShardConfigurationResult>ReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:            elasticacheNS,
		ReplicationGroup: rgToXML(*rg),
	})
}

func (h *Handler) describeCacheEngineVersions(c *echo.Context, form url.Values) error {
	engine := form.Get("Engine")
	family := form.Get("CacheParameterGroupFamily")
	engineVersion := form.Get("EngineVersion")
	marker, maxRecords := parsePagination(form)

	p, err := h.Backend.DescribeCacheEngineVersions(engine, family, engineVersion, marker, maxRecords)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	items := make([]cacheEngineVersionXML, 0, len(p.Data))
	for _, v := range p.Data {
		items = append(items, cacheEngineVersionXML(v))
	}

	type cevListXML struct {
		CacheEngineVersion []cacheEngineVersionXML `xml:"CacheEngineVersion"`
	}

	type result struct {
		XMLName             xml.Name   `xml:"DescribeCacheEngineVersionsResponse"`
		Xmlns               string     `xml:"xmlns,attr"`
		Marker              string     `xml:"DescribeCacheEngineVersionsResult>Marker,omitempty"`
		CacheEngineVersions cevListXML `xml:"DescribeCacheEngineVersionsResult>CacheEngineVersions"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:               elasticacheNS,
		Marker:              p.Next,
		CacheEngineVersions: cevListXML{CacheEngineVersion: items},
	})
}

func (h *Handler) rebootCacheCluster(c *echo.Context, form url.Values) error {
	clusterID := form.Get("CacheClusterId")
	nodeIDs := parseRepeatedField(form, "CacheNodeIdsToReboot.CacheNodeId")

	cl, err := h.Backend.RebootCacheCluster(clusterID, nodeIDs)
	if err != nil {
		if errors.Is(err, ErrClusterNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheClusterNotFound", "Cache cluster not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName      xml.Name        `xml:"RebootCacheClusterResponse"`
		Xmlns        string          `xml:"xmlns,attr"`
		CacheCluster cacheClusterXML `xml:"RebootCacheClusterResult>CacheCluster"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:        elasticacheNS,
		CacheCluster: clusterToXML(cl, cl.Status),
	})
}

func (h *Handler) deleteCacheSecurityGroup(c *echo.Context, form url.Values) error {
	name := form.Get("CacheSecurityGroupName")

	err := h.Backend.DeleteCacheSecurityGroup(name)
	if err != nil {
		if errors.Is(err, ErrCacheSecurityGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheSecurityGroupNotFound", "Cache security group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName xml.Name `xml:"DeleteCacheSecurityGroupResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
	}

	return xmlResp(c, http.StatusOK, result{Xmlns: elasticacheNS})
}

func (h *Handler) describeCacheSecurityGroups(c *echo.Context, form url.Values) error {
	name := form.Get("CacheSecurityGroupName")
	marker, maxRecords := parsePagination(form)

	p, err := h.Backend.DescribeCacheSecurityGroups(name, marker, maxRecords)
	if err != nil {
		if errors.Is(err, ErrCacheSecurityGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheSecurityGroupNotFound", "Cache security group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	var res describeSGsResultXML
	res.Xmlns = elasticacheNS
	res.Marker = p.Next

	for i := range p.Data {
		res.CacheSecurityGroups.CacheSecurityGroup = append(
			res.CacheSecurityGroups.CacheSecurityGroup,
			cacheSecurityGroupToXML(&p.Data[i]),
		)
	}

	return xmlResp(c, http.StatusOK, res)
}

func (h *Handler) revokeCacheSecurityGroupIngress(c *echo.Context, form url.Values) error {
	name := form.Get("CacheSecurityGroupName")
	ec2SecurityGroupName := form.Get("EC2SecurityGroupName")
	ec2SecurityGroupOwnerID := form.Get("EC2SecurityGroupOwnerId")

	sg, err := h.Backend.RevokeCacheSecurityGroupIngress(name, ec2SecurityGroupName, ec2SecurityGroupOwnerID)
	if err != nil {
		if errors.Is(err, ErrCacheSecurityGroupNotFound) {
			return xmlError(c, http.StatusBadRequest, "CacheSecurityGroupNotFound", "Cache security group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName            xml.Name              `xml:"RevokeCacheSecurityGroupIngressResponse"`
		Xmlns              string                `xml:"xmlns,attr"`
		CacheSecurityGroup cacheSecurityGroupXML `xml:"RevokeCacheSecurityGroupIngressResult>CacheSecurityGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:              elasticacheNS,
		CacheSecurityGroup: cacheSecurityGroupToXML(sg),
	})
}

func (h *Handler) describeEngineDefaultParameters(c *echo.Context, form url.Values) error {
	family := form.Get("CacheParameterGroupFamily")
	marker, maxRecords := parsePagination(form)

	p, err := h.Backend.DescribeEngineDefaultParameters(family, marker, maxRecords)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type paramsListXML struct {
		Parameter []parameterXML `xml:"Parameter"`
	}

	type result struct {
		XMLName xml.Name      `xml:"DescribeEngineDefaultParametersResponse"`
		Xmlns   string        `xml:"xmlns,attr"`
		Family  string        `xml:"DescribeEngineDefaultParametersResult>EngineDefaults>CacheParameterGroupFamily"`
		Marker  string        `xml:"DescribeEngineDefaultParametersResult>EngineDefaults>Marker,omitempty"`
		Params  paramsListXML `xml:"DescribeEngineDefaultParametersResult>EngineDefaults>Parameters"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:  elasticacheNS,
		Family: family,
		Marker: p.Next,
		Params: paramsListXML{Parameter: buildParameterItems(p.Data)},
	})
}

func (h *Handler) describeServiceUpdates(c *echo.Context, form url.Values) error {
	serviceUpdateName := form.Get("ServiceUpdateName")
	marker, maxRecords := parsePagination(form)
	statusList := parseRepeatedField(form, "ServiceUpdateStatus.member")

	p, err := h.Backend.DescribeServiceUpdates(serviceUpdateName, marker, maxRecords, statusList)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	items := make([]serviceUpdateXML, 0, len(p.Data))
	for _, su := range p.Data {
		items = append(items, serviceUpdateXML{
			ServiceUpdateName:   su.ServiceUpdateName,
			ServiceUpdateStatus: su.Status,
		})
	}

	type suListXML struct {
		ServiceUpdate []serviceUpdateXML `xml:"ServiceUpdate"`
	}

	type result struct {
		XMLName        xml.Name  `xml:"DescribeServiceUpdatesResponse"`
		Xmlns          string    `xml:"xmlns,attr"`
		Marker         string    `xml:"DescribeServiceUpdatesResult>Marker,omitempty"`
		ServiceUpdates suListXML `xml:"DescribeServiceUpdatesResult>ServiceUpdates"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:          elasticacheNS,
		Marker:         p.Next,
		ServiceUpdates: suListXML{ServiceUpdate: items},
	})
}

func (h *Handler) describeUpdateActions(c *echo.Context, form url.Values) error {
	serviceUpdateName := form.Get("ServiceUpdateName")
	marker, maxRecords := parsePagination(form)

	p, err := h.Backend.DescribeUpdateActions(serviceUpdateName, marker, maxRecords)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	items := make([]updateActionXML, 0, len(p.Data))
	for _, ua := range p.Data {
		items = append(items, updateActionXML(ua))
	}

	type uaListXML struct {
		UpdateAction []updateActionXML `xml:"UpdateAction"`
	}

	type result struct {
		XMLName       xml.Name  `xml:"DescribeUpdateActionsResponse"`
		Xmlns         string    `xml:"xmlns,attr"`
		Marker        string    `xml:"DescribeUpdateActionsResult>Marker,omitempty"`
		UpdateActions uaListXML `xml:"DescribeUpdateActionsResult>UpdateActions"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:         elasticacheNS,
		Marker:        p.Next,
		UpdateActions: uaListXML{UpdateAction: items},
	})
}

func (h *Handler) listAllowedNodeTypeModifications(c *echo.Context, form url.Values) error {
	clusterID := form.Get("CacheClusterId")
	replicationGroupID := form.Get("ReplicationGroupId")

	mods, err := h.Backend.ListAllowedNodeTypeModifications(clusterID, replicationGroupID)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type scaleModsXML struct {
		Member []string `xml:"member"`
	}

	type result struct {
		XMLName                xml.Name     `xml:"ListAllowedNodeTypeModificationsResponse"`
		Xmlns                  string       `xml:"xmlns,attr"`
		ScaleUpModifications   scaleModsXML `xml:"ListAllowedNodeTypeModificationsResult>ScaleUpModifications"`
		ScaleDownModifications scaleModsXML `xml:"ListAllowedNodeTypeModificationsResult>ScaleDownModifications"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                elasticacheNS,
		ScaleUpModifications: scaleModsXML{Member: mods},
	})
}
