package elasticache

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/labstack/echo/v5"
)

// describeUserGroupsResultXML is the XML envelope for DescribeUserGroups responses.
type describeUserGroupsResultXML struct {
	XMLName    xml.Name `xml:"DescribeUserGroupsResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	Marker     string   `xml:"DescribeUserGroupsResult>Marker,omitempty"`
	UserGroups struct {
		Member []userGroupXML `xml:"member"`
	} `xml:"DescribeUserGroupsResult>UserGroups"`
}

// userGroupXML is the wire shape of types.UserGroup. There is no Description
// field on the real SDK type -- a prior pass invented one and serialized it
// on the wire; do not re-add it. ReplicationGroups is the reverse of a
// ReplicationGroup's UserGroupIds, computed fresh on every response (see
// userGroupReplicationGroupIDsLocked).
type userGroupXML struct {
	ARN         string `xml:"ARN"`
	UserGroupID string `xml:"UserGroupId"`
	Status      string `xml:"Status"`
	Engine      string `xml:"Engine,omitempty"`
	UserIDs     struct {
		Member []string `xml:"member"`
	} `xml:"UserIds"`
	ReplicationGroups struct {
		Member []string `xml:"member"`
	} `xml:"ReplicationGroups"`
}

func userGroupToXML(ug *UserGroup) userGroupXML {
	x := userGroupXML{
		ARN:         ug.ARN,
		UserGroupID: ug.UserGroupID,
		Status:      ug.Status,
		Engine:      ug.Engine,
	}
	x.UserIDs.Member = ug.UserIDs
	x.ReplicationGroups.Member = ug.AssignedReplicationGroupIDs

	return x
}

func (h *Handler) createUserGroup(ctx context.Context, c *echo.Context, form url.Values) error {
	groupID := form.Get("UserGroupId")
	engine := form.Get("Engine")
	userIDs := parseRepeatedField(form, "UserIds.member")

	ug, err := h.Backend.CreateUserGroupValidated(ctx, groupID, engine, userIDs)
	if err != nil {
		if errors.Is(err, ErrUserGroupAlreadyExists) {
			return xmlError(c, http.StatusBadRequest, "UserGroupAlreadyExists", "User group already exists")
		}

		if errors.Is(err, ErrGroupUserNotFound) {
			return xmlError(c, http.StatusNotFound, "UserNotFound", err.Error())
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	h.applyCreateTimeTags(ctx, form, ug.ARN)

	type result struct {
		XMLName     xml.Name `xml:"CreateUserGroupResponse"`
		Xmlns       string   `xml:"xmlns,attr"`
		ARN         string   `xml:"CreateUserGroupResult>ARN"`
		UserGroupID string   `xml:"CreateUserGroupResult>UserGroupId"`
		Status      string   `xml:"CreateUserGroupResult>Status"`
		Engine      string   `xml:"CreateUserGroupResult>Engine,omitempty"`
		UserIDs     struct {
			Member []string `xml:"member"`
		} `xml:"CreateUserGroupResult>UserIds"`
		ReplicationGroups struct {
			Member []string `xml:"member"`
		} `xml:"CreateUserGroupResult>ReplicationGroups"`
	}

	x := userGroupToXML(ug)
	r := result{
		Xmlns:       elasticacheNS,
		ARN:         x.ARN,
		UserGroupID: x.UserGroupID,
		Status:      x.Status,
		Engine:      x.Engine,
	}
	r.UserIDs.Member = x.UserIDs.Member
	r.ReplicationGroups.Member = x.ReplicationGroups.Member

	return xmlResp(c, http.StatusOK, r)
}

func (h *Handler) deleteUserGroup(ctx context.Context, c *echo.Context, form url.Values) error {
	groupID := form.Get("UserGroupId")

	ug, err := h.Backend.DeleteUserGroup(ctx, groupID)
	if err != nil {
		if errors.Is(err, ErrUserGroupNotFound) {
			return xmlError(c, http.StatusNotFound, "UserGroupNotFound", "User group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName     xml.Name `xml:"DeleteUserGroupResponse"`
		Xmlns       string   `xml:"xmlns,attr"`
		ARN         string   `xml:"DeleteUserGroupResult>ARN"`
		UserGroupID string   `xml:"DeleteUserGroupResult>UserGroupId"`
		Status      string   `xml:"DeleteUserGroupResult>Status"`
		Engine      string   `xml:"DeleteUserGroupResult>Engine,omitempty"`
		UserIDs     struct {
			Member []string `xml:"member"`
		} `xml:"DeleteUserGroupResult>UserIds"`
		ReplicationGroups struct {
			Member []string `xml:"member"`
		} `xml:"DeleteUserGroupResult>ReplicationGroups"`
	}

	x := userGroupToXML(ug)
	r := result{
		Xmlns:       elasticacheNS,
		ARN:         x.ARN,
		UserGroupID: x.UserGroupID,
		Status:      x.Status,
		Engine:      x.Engine,
	}
	r.UserIDs.Member = x.UserIDs.Member
	r.ReplicationGroups.Member = x.ReplicationGroups.Member

	return xmlResp(c, http.StatusOK, r)
}

func (h *Handler) describeUserGroups(ctx context.Context, c *echo.Context, form url.Values) error {
	groupID := form.Get("UserGroupId")

	p, err := describeListChecked(c, form,
		func(marker string, maxRecords int) (page.Page[UserGroup], error) {
			return h.Backend.DescribeUserGroups(ctx, groupID, marker, maxRecords)
		},
		ErrUserGroupNotFound, http.StatusNotFound, "UserGroupNotFound", "User group not found")
	if err != nil {
		return err
	}

	var res describeUserGroupsResultXML
	res.Xmlns = elasticacheNS
	res.Marker = p.Next

	for i := range p.Data {
		res.UserGroups.Member = append(res.UserGroups.Member, userGroupToXML(&p.Data[i]))
	}

	return xmlResp(c, http.StatusOK, res)
}

func (h *Handler) modifyUserGroup(ctx context.Context, c *echo.Context, form url.Values) error {
	groupID := form.Get("UserGroupId")
	userIDsToAdd := parseRepeatedField(form, "UserIdsToAdd.member")
	userIDsToRemove := parseRepeatedField(form, "UserIdsToRemove.member")

	ug, err := h.Backend.ModifyUserGroup(ctx, groupID, userIDsToAdd, userIDsToRemove)
	if err != nil {
		if errors.Is(err, ErrUserGroupNotFound) {
			return xmlError(c, http.StatusNotFound, "UserGroupNotFound", "User group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName     xml.Name `xml:"ModifyUserGroupResponse"`
		Xmlns       string   `xml:"xmlns,attr"`
		ARN         string   `xml:"ModifyUserGroupResult>ARN"`
		UserGroupID string   `xml:"ModifyUserGroupResult>UserGroupId"`
		Status      string   `xml:"ModifyUserGroupResult>Status"`
		Engine      string   `xml:"ModifyUserGroupResult>Engine,omitempty"`
		UserIDs     struct {
			Member []string `xml:"member"`
		} `xml:"ModifyUserGroupResult>UserIds"`
		ReplicationGroups struct {
			Member []string `xml:"member"`
		} `xml:"ModifyUserGroupResult>ReplicationGroups"`
	}

	x := userGroupToXML(ug)
	r := result{
		Xmlns:       elasticacheNS,
		ARN:         x.ARN,
		UserGroupID: x.UserGroupID,
		Status:      x.Status,
		Engine:      x.Engine,
	}
	r.UserIDs.Member = x.UserIDs.Member
	r.ReplicationGroups.Member = x.ReplicationGroups.Member

	return xmlResp(c, http.StatusOK, r)
}
