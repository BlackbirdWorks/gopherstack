package dashboard

import (
	"net/http"
	"sort"

	"github.com/labstack/echo/v5"
)

// ssoadminPermissionSetView is the view model for a single permission set row.
type ssoadminPermissionSetView struct {
	PermissionSetArn string
	Name             string
	Description      string
	SessionDuration  string
}

// ssoadminInstanceView is the view model for a single SSO instance row.
type ssoadminInstanceView struct {
	InstanceArn     string
	Name            string
	Status          string
	OwnerAccountID  string
	IdentityStoreID string
}

// ssoadminIndexData is the template data for the SSO Admin dashboard page.
type ssoadminIndexData struct {
	PageData

	Instances      []ssoadminInstanceView
	PermissionSets []ssoadminPermissionSetView
}

// ssoadminSnippet returns the shared SnippetData for the SSO Admin dashboard.
func ssoadminSnippet() *SnippetData {
	return &SnippetData{
		ID:    "ssoadmin-operations",
		Title: "Using SSO Admin",
		Cli:   `aws sso-admin list-instances --endpoint-url http://localhost:8000`,
		Go: `import (
    "context"
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/ssoadmin"
)

cfg, _ := config.LoadDefaultConfig(context.TODO(),
    config.WithRegion("us-east-1"),
)
client := ssoadmin.NewFromConfig(cfg, func(o *ssoadmin.Options) {
    o.BaseEndpoint = aws.String("http://localhost:8000")
})
out, _ := client.ListInstances(context.TODO(), &ssoadmin.ListInstancesInput{})`,
		Python: `import boto3

client = boto3.client('sso-admin', endpoint_url='http://localhost:8000',
    region_name='us-east-1')
response = client.list_instances()`,
	}
}

// setupSsoAdminRoutes registers all SSO Admin dashboard routes.
func (h *DashboardHandler) setupSsoAdminRoutes() {
	h.SubRouter.GET("/dashboard/ssoadmin", h.ssoadminIndex)
	h.SubRouter.POST("/dashboard/ssoadmin/create-instance", h.ssoadminCreateInstance)
	h.SubRouter.POST("/dashboard/ssoadmin/delete-instance", h.ssoadminDeleteInstance)
	h.SubRouter.POST("/dashboard/ssoadmin/create-permission-set", h.ssoadminCreatePermissionSet)
	h.SubRouter.POST("/dashboard/ssoadmin/delete-permission-set", h.ssoadminDeletePermissionSet)
}

// ssoadminIndex renders the main SSO Admin dashboard page.
func (h *DashboardHandler) ssoadminIndex(c *echo.Context) error {
	w := c.Response()

	if h.SsoAdminOps == nil {
		h.renderTemplate(w, "ssoadmin/index.html", ssoadminIndexData{
			PageData: PageData{
				Title:     "SSO Admin",
				ActiveTab: "ssoadmin",
				Snippet:   ssoadminSnippet(),
			},
			Instances:      []ssoadminInstanceView{},
			PermissionSets: []ssoadminPermissionSetView{},
		})

		return nil
	}

	instances := h.SsoAdminOps.Backend.ListInstances()
	instanceViews := make([]ssoadminInstanceView, 0, len(instances))
	for _, inst := range instances {
		instanceViews = append(instanceViews, ssoadminInstanceView{
			InstanceArn:     inst.InstanceArn,
			Name:            inst.Name,
			Status:          inst.Status,
			OwnerAccountID:  inst.OwnerAccountID,
			IdentityStoreID: inst.IdentityStoreID,
		})
	}
	sort.Slice(instanceViews, func(i, j int) bool {
		return instanceViews[i].InstanceArn < instanceViews[j].InstanceArn
	})

	var psViews []ssoadminPermissionSetView
	for _, inst := range instances {
		for _, ps := range h.SsoAdminOps.Backend.ListPermissionSets(inst.InstanceArn) {
			psViews = append(psViews, ssoadminPermissionSetView{
				PermissionSetArn: ps.PermissionSetArn,
				Name:             ps.Name,
				Description:      ps.Description,
				SessionDuration:  ps.SessionDuration,
			})
		}
	}
	sort.Slice(psViews, func(i, j int) bool {
		return psViews[i].PermissionSetArn < psViews[j].PermissionSetArn
	})

	h.renderTemplate(w, "ssoadmin/index.html", ssoadminIndexData{
		PageData: PageData{
			Title:     "SSO Admin",
			ActiveTab: "ssoadmin",
			Snippet:   ssoadminSnippet(),
		},
		Instances:      instanceViews,
		PermissionSets: psViews,
	})

	return nil
}

// ssoadminCreateInstance handles POST /dashboard/ssoadmin/create-instance.
func (h *DashboardHandler) ssoadminCreateInstance(c *echo.Context) error {
	if h.SsoAdminOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	ctx := c.Request().Context()

	if _, err := h.SsoAdminOps.Backend.CreateInstance(name, "", ""); err != nil {
		h.Logger.ErrorContext(ctx, "ssoadmin: failed to create instance", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/ssoadmin")
}

// ssoadminDeleteInstance handles POST /dashboard/ssoadmin/delete-instance.
func (h *DashboardHandler) ssoadminDeleteInstance(c *echo.Context) error {
	if h.SsoAdminOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	instanceArn := c.Request().FormValue("instance_arn")
	if instanceArn == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	ctx := c.Request().Context()

	if err := h.SsoAdminOps.Backend.DeleteInstance(instanceArn); err != nil {
		h.Logger.ErrorContext(ctx, "ssoadmin: failed to delete instance", "instance_arn", instanceArn, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/ssoadmin")
}

// ssoadminCreatePermissionSet handles POST /dashboard/ssoadmin/create-permission-set.
func (h *DashboardHandler) ssoadminCreatePermissionSet(c *echo.Context) error {
	if h.SsoAdminOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	instanceArn := c.Request().FormValue("instance_arn")
	name := c.Request().FormValue("name")
	if instanceArn == "" || name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	description := c.Request().FormValue("description")
	sessionDuration := c.Request().FormValue("session_duration")
	ctx := c.Request().Context()

	if _, err := h.SsoAdminOps.Backend.CreatePermissionSet(
		instanceArn,
		name,
		description,
		sessionDuration,
		"",
		nil,
	); err != nil {
		h.Logger.ErrorContext(ctx, "ssoadmin: failed to create permission set", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/ssoadmin")
}

// ssoadminDeletePermissionSet handles POST /dashboard/ssoadmin/delete-permission-set.
func (h *DashboardHandler) ssoadminDeletePermissionSet(c *echo.Context) error {
	if h.SsoAdminOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	instanceArn := c.Request().FormValue("instance_arn")
	permissionSetArn := c.Request().FormValue("permission_set_arn")
	if instanceArn == "" || permissionSetArn == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	ctx := c.Request().Context()

	if err := h.SsoAdminOps.Backend.DeletePermissionSet(instanceArn, permissionSetArn); err != nil {
		h.Logger.ErrorContext(
			ctx,
			"ssoadmin: failed to delete permission set",
			"permission_set_arn",
			permissionSetArn,
			"error",
			err,
		)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/ssoadmin")
}
