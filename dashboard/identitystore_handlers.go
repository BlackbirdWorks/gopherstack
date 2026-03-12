package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"

	identitystorebackend "github.com/blackbirdworks/gopherstack/services/identitystore"
)

// ----------------------------------------
// View models
// ----------------------------------------

// identityStoreUserView is the view model for a single identity store user row.
type identityStoreUserView struct {
	UserID      string
	UserName    string
	DisplayName string
	GivenName   string
	FamilyName  string
	Emails      string
}

// identityStoreGroupView is the view model for a single identity store group row.
type identityStoreGroupView struct {
	GroupID     string
	DisplayName string
	Description string
	Members     int
}

// identityStoreMembershipView is the view model for a membership row.
type identityStoreMembershipView struct {
	MembershipID string
	GroupDisplay string
	UserDisplay  string
}

// identityStoreIndexData is the template data for the Identity Store dashboard page.
type identityStoreIndexData struct {
	PageData

	IdentityStoreID string
	Users           []identityStoreUserView
	Groups          []identityStoreGroupView
	Memberships     []identityStoreMembershipView
}

// ----------------------------------------
// Route setup
// ----------------------------------------

// setupIdentityStoreRoutes registers all Identity Store dashboard routes.
func (h *DashboardHandler) setupIdentityStoreRoutes() {
	h.SubRouter.GET("/dashboard/identitystore", h.identityStoreIndex)
	h.SubRouter.POST("/dashboard/identitystore/user/create", h.identityStoreCreateUser)
	h.SubRouter.POST("/dashboard/identitystore/user/delete", h.identityStoreDeleteUser)
	h.SubRouter.POST("/dashboard/identitystore/group/create", h.identityStoreCreateGroup)
	h.SubRouter.POST("/dashboard/identitystore/group/delete", h.identityStoreDeleteGroup)
}

// ----------------------------------------
// Handlers
// ----------------------------------------

// defaultIdentityStoreID is the fixed identity store ID used by the dashboard.
const defaultIdentityStoreID = "d-0000000000"

// identityStoreIndex renders the main Identity Store dashboard page.
func (h *DashboardHandler) identityStoreIndex(c *echo.Context) error {
	if h.IdentityStoreOps == nil {
		h.renderTemplate(c.Response(), "identitystore/index.html", identityStoreIndexData{
			PageData:        PageData{Title: "Identity Store", ActiveTab: "identitystore"},
			IdentityStoreID: defaultIdentityStoreID,
		})

		return nil
	}

	storeID := defaultIdentityStoreID
	users := h.IdentityStoreOps.Backend.ListUsers(storeID)
	groups := h.IdentityStoreOps.Backend.ListGroups(storeID)

	userMap := make(map[string]*identitystorebackend.User, len(users))
	for _, u := range users {
		userMap[u.UserID] = u
	}

	userViews := make([]identityStoreUserView, 0, len(users))
	for _, u := range users {
		v := identityStoreUserView{
			UserID:      u.UserID,
			UserName:    u.UserName,
			DisplayName: u.DisplayName,
		}

		if u.Name != nil {
			v.GivenName = u.Name.GivenName
			v.FamilyName = u.Name.FamilyName
		}

		if len(u.Emails) > 0 {
			v.Emails = u.Emails[0].Value
		}

		userViews = append(userViews, v)
	}

	groupViews := make([]identityStoreGroupView, 0, len(groups))
	for _, g := range groups {
		memberships := h.IdentityStoreOps.Backend.ListGroupMemberships(storeID, g.GroupID)
		groupViews = append(groupViews, identityStoreGroupView{
			GroupID:     g.GroupID,
			DisplayName: g.DisplayName,
			Description: g.Description,
			Members:     len(memberships),
		})
	}

	memberships := make([]identityStoreMembershipView, 0)

	for _, g := range groups {
		for _, m := range h.IdentityStoreOps.Backend.ListGroupMemberships(storeID, g.GroupID) {
			mv := identityStoreMembershipView{
				MembershipID: m.MembershipID,
				GroupDisplay: g.DisplayName,
			}

			if u, ok := userMap[m.MemberID.UserID]; ok {
				mv.UserDisplay = u.DisplayName
				if mv.UserDisplay == "" {
					mv.UserDisplay = u.UserName
				}
			} else {
				mv.UserDisplay = m.MemberID.UserID
			}

			memberships = append(memberships, mv)
		}
	}

	h.renderTemplate(c.Response(), "identitystore/index.html", identityStoreIndexData{
		PageData:        PageData{Title: "Identity Store", ActiveTab: "identitystore"},
		IdentityStoreID: storeID,
		Users:           userViews,
		Groups:          groupViews,
		Memberships:     memberships,
	})

	return nil
}

// identityStoreCreateUser handles user creation from the dashboard.
func (h *DashboardHandler) identityStoreCreateUser(c *echo.Context) error {
	if h.IdentityStoreOps == nil {
		return c.Redirect(http.StatusFound, "/dashboard/identitystore")
	}

	userName := c.FormValue("user_name")
	displayName := c.FormValue("display_name")
	givenName := c.FormValue("given_name")
	familyName := c.FormValue("family_name")
	email := c.FormValue("email")

	if userName == "" {
		return c.Redirect(http.StatusFound, "/dashboard/identitystore")
	}

	req := &identitystorebackend.CreateUserRequest{
		UserName:    userName,
		DisplayName: displayName,
	}

	if givenName != "" || familyName != "" {
		req.Name = &identitystorebackend.Name{
			GivenName:  givenName,
			FamilyName: familyName,
		}
	}

	if email != "" {
		req.Emails = []identitystorebackend.Email{
			{Value: email, Type: "work", Primary: true},
		}
	}

	_, _ = h.IdentityStoreOps.Backend.CreateUser(defaultIdentityStoreID, req)

	return c.Redirect(http.StatusFound, "/dashboard/identitystore")
}

// identityStoreDeleteUser handles user deletion from the dashboard.
func (h *DashboardHandler) identityStoreDeleteUser(c *echo.Context) error {
	if h.IdentityStoreOps == nil {
		return c.Redirect(http.StatusFound, "/dashboard/identitystore")
	}

	userID := c.FormValue("user_id")
	if userID != "" {
		_ = h.IdentityStoreOps.Backend.DeleteUser(defaultIdentityStoreID, userID)
	}

	return c.Redirect(http.StatusFound, "/dashboard/identitystore")
}

// identityStoreCreateGroup handles group creation from the dashboard.
func (h *DashboardHandler) identityStoreCreateGroup(c *echo.Context) error {
	if h.IdentityStoreOps == nil {
		return c.Redirect(http.StatusFound, "/dashboard/identitystore")
	}

	displayName := c.FormValue("display_name")
	description := c.FormValue("description")

	if displayName == "" {
		return c.Redirect(http.StatusFound, "/dashboard/identitystore")
	}

	req := &identitystorebackend.CreateGroupRequest{
		DisplayName: displayName,
		Description: description,
	}

	_, _ = h.IdentityStoreOps.Backend.CreateGroup(defaultIdentityStoreID, req)

	return c.Redirect(http.StatusFound, "/dashboard/identitystore")
}

// identityStoreDeleteGroup handles group deletion from the dashboard.
func (h *DashboardHandler) identityStoreDeleteGroup(c *echo.Context) error {
	if h.IdentityStoreOps == nil {
		return c.Redirect(http.StatusFound, "/dashboard/identitystore")
	}

	groupID := c.FormValue("group_id")
	if groupID != "" {
		_ = h.IdentityStoreOps.Backend.DeleteGroup(defaultIdentityStoreID, groupID)
	}

	return c.Redirect(http.StatusFound, "/dashboard/identitystore")
}
