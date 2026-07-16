package elasticache

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"

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

func (h *Handler) createUserGroup(ctx context.Context, c *echo.Context, form url.Values) error {
	groupID := form.Get("UserGroupId")
	description := form.Get("Description")
	engine := form.Get("Engine")
	userIDs := parseRepeatedField(form, "UserIds.member")

	ug, err := h.Backend.CreateUserGroupValidated(ctx, groupID, description, engine, userIDs)
	if err != nil {
		if errors.Is(err, ErrUserGroupAlreadyExists) {
			return xmlError(c, http.StatusBadRequest, "UserGroupAlreadyExists", "User group already exists")
		}

		if errors.Is(err, ErrGroupUserNotFound) {
			return xmlError(c, http.StatusNotFound, "UserNotFound", err.Error())
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

func (h *Handler) describeUserGroups(ctx context.Context, c *echo.Context, form url.Values) error {
	groupID := form.Get("UserGroupId")
	marker, maxRecords := parsePagination(form)

	p, err := h.Backend.DescribeUserGroups(ctx, groupID, marker, maxRecords)
	if err != nil {
		if errors.Is(err, ErrUserGroupNotFound) {
			return xmlError(c, http.StatusNotFound, "UserGroupNotFound", "User group not found")
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
