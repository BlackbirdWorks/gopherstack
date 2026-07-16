package elasticache

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"
)

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

// describeUsersResultXML is the XML envelope for DescribeUsers responses.
type describeUsersResultXML struct {
	XMLName xml.Name `xml:"DescribeUsersResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Marker  string   `xml:"DescribeUsersResult>Marker,omitempty"`
	Users   struct {
		Member []userXML `xml:"member"`
	} `xml:"DescribeUsersResult>Users"`
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

func (h *Handler) deleteUser(ctx context.Context, c *echo.Context, form url.Values) error {
	userID := form.Get("UserId")

	u, err := h.Backend.DeleteUser(ctx, userID)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return xmlError(c, http.StatusNotFound, "UserNotFound", "User not found")
		}

		if errors.Is(err, ErrUserNotInGroup) {
			return xmlError(c, http.StatusBadRequest, "InvalidParameterValue", err.Error())
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

func (h *Handler) describeUsers(ctx context.Context, c *echo.Context, form url.Values) error {
	userID := form.Get("UserId")
	marker, maxRecords := parsePagination(form)

	p, err := h.Backend.DescribeUsers(ctx, userID, marker, maxRecords)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return xmlError(c, http.StatusNotFound, "UserNotFound", "User not found")
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

func (h *Handler) modifyUser(ctx context.Context, c *echo.Context, form url.Values) error {
	userID := form.Get("UserId")
	accessString := form.Get("AccessString")
	noPasswordRequired := strings.EqualFold(form.Get("NoPasswordRequired"), "true")

	u, err := h.Backend.ModifyUser(ctx, userID, accessString, noPasswordRequired)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			return xmlError(c, http.StatusNotFound, "UserNotFound", "User not found")
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
