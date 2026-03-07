package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// iamPageData is the template data for the IAM overview page.
type iamPageData struct {
	Users             any
	Roles             any
	Policies          any
	Groups            any
	AccessKeys        any
	InstanceProfiles  any
	PageData          //nolint:embeddedstructfieldcheck // must be last for backward compatibility
	EnforcementActive bool
}

// iamIndex renders the IAM resource overview page.
func (h *DashboardHandler) iamIndex(c *echo.Context) error {
	w := c.Response()

	data := iamPageData{
		EnforcementActive: h.GlobalConfig.EnforceIAM,
		PageData: PageData{
			Title:     "IAM",
			ActiveTab: "iam",
			Snippet: &SnippetData{
				ID:    "iam-operations",
				Title: "Using Iam",
				Cli:   `aws iam help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Using Iam
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithEndpointResolverWithOptions(
        aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
            return aws.Endpoint{URL: "http://localhost:8000"}, nil
        }),
    ),
)
if err != nil {
    log.Fatal(err)
}
client := iam.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Using Iam
import boto3

client = boto3.client('iam', endpoint_url='http://localhost:8000')`,
			},
		},
	}

	if h.IAMOps != nil {
		data.Users = h.IAMOps.Backend.ListAllUsers()
		data.Roles = h.IAMOps.Backend.ListAllRoles()
		data.Policies = h.IAMOps.Backend.ListAllPolicies()
		data.Groups = h.IAMOps.Backend.ListAllGroups()
		data.AccessKeys = h.IAMOps.Backend.ListAllAccessKeys()
		data.InstanceProfiles = h.IAMOps.Backend.ListAllInstanceProfiles()
	}

	h.renderTemplate(w, "iam/index.html", data)

	return nil
}

// iamCreateUser handles user creation via the dashboard form.
func (h *DashboardHandler) iamCreateUser(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	if err := r.ParseForm(); err != nil {
		return c.String(http.StatusBadRequest, "Invalid request")
	}

	userName := r.FormValue("userName")
	path := r.FormValue("path")

	if h.IAMOps != nil {
		if _, err := h.IAMOps.Backend.CreateUser(userName, path, ""); err != nil {
			h.Logger.Error("failed to create IAM user", "error", err)

			return c.String(http.StatusInternalServerError, "Failed to create user: "+err.Error())
		}
	}

	w.Header().Set("Hx-Redirect", "/dashboard/iam")

	return c.NoContent(http.StatusOK)
}

// iamDeleteUser handles user deletion via the dashboard.
func (h *DashboardHandler) iamDeleteUser(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	userName := r.URL.Query().Get("name")
	if userName == "" {
		return c.String(http.StatusBadRequest, "Missing name")
	}

	if h.IAMOps != nil {
		if err := h.IAMOps.Backend.DeleteUser(userName); err != nil {
			h.Logger.Error("failed to delete IAM user", "error", err)

			return c.String(http.StatusInternalServerError, "Failed to delete user")
		}
	}

	w.Header().Set("Hx-Redirect", "/dashboard/iam")

	return c.NoContent(http.StatusOK)
}

// iamCreateRole handles role creation via the dashboard form.
func (h *DashboardHandler) iamCreateRole(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	if err := r.ParseForm(); err != nil {
		return c.String(http.StatusBadRequest, "Invalid request")
	}

	roleName := r.FormValue("roleName")
	path := r.FormValue("path")
	doc := r.FormValue("assumeRolePolicyDocument")

	if h.IAMOps != nil {
		if _, err := h.IAMOps.Backend.CreateRole(roleName, path, doc, ""); err != nil {
			h.Logger.Error("failed to create IAM role", "error", err)

			return c.String(http.StatusInternalServerError, "Failed to create role: "+err.Error())
		}
	}

	w.Header().Set("Hx-Redirect", "/dashboard/iam")

	return c.NoContent(http.StatusOK)
}

// iamDeleteRole handles role deletion via the dashboard.
func (h *DashboardHandler) iamDeleteRole(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	roleName := r.URL.Query().Get("name")
	if roleName == "" {
		return c.String(http.StatusBadRequest, "Missing name")
	}

	if h.IAMOps != nil {
		if err := h.IAMOps.Backend.DeleteRole(roleName); err != nil {
			h.Logger.Error("failed to delete IAM role", "error", err)

			return c.String(http.StatusInternalServerError, "Failed to delete role")
		}
	}

	w.Header().Set("Hx-Redirect", "/dashboard/iam")

	return c.NoContent(http.StatusOK)
}

// iamCreatePolicy handles policy creation via the dashboard form.
func (h *DashboardHandler) iamCreatePolicy(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	if err := r.ParseForm(); err != nil {
		return c.String(http.StatusBadRequest, "Invalid request")
	}

	policyName := r.FormValue("policyName")
	path := r.FormValue("path")
	doc := r.FormValue("policyDocument")

	if h.IAMOps != nil {
		if _, err := h.IAMOps.Backend.CreatePolicy(policyName, path, doc); err != nil {
			h.Logger.Error("failed to create IAM policy", "error", err)

			return c.String(http.StatusInternalServerError, "Failed to create policy: "+err.Error())
		}
	}

	w.Header().Set("Hx-Redirect", "/dashboard/iam")

	return c.NoContent(http.StatusOK)
}

// iamDeletePolicy handles policy deletion via the dashboard.
func (h *DashboardHandler) iamDeletePolicy(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	policyArn := r.URL.Query().Get("arn")
	if policyArn == "" {
		return c.String(http.StatusBadRequest, "Missing arn")
	}

	if h.IAMOps != nil {
		if err := h.IAMOps.Backend.DeletePolicy(policyArn); err != nil {
			h.Logger.Error("failed to delete IAM policy", "error", err)

			return c.String(http.StatusInternalServerError, "Failed to delete policy")
		}
	}

	w.Header().Set("Hx-Redirect", "/dashboard/iam")

	return c.NoContent(http.StatusOK)
}

// iamCreateGroup handles group creation via the dashboard form.
func (h *DashboardHandler) iamCreateGroup(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	if err := r.ParseForm(); err != nil {
		return c.String(http.StatusBadRequest, "Invalid request")
	}

	groupName := r.FormValue("groupName")
	path := r.FormValue("path")

	if h.IAMOps != nil {
		if _, err := h.IAMOps.Backend.CreateGroup(groupName, path); err != nil {
			h.Logger.Error("failed to create IAM group", "error", err)

			return c.String(http.StatusInternalServerError, "Failed to create group: "+err.Error())
		}
	}

	w.Header().Set("Hx-Redirect", "/dashboard/iam")

	return c.NoContent(http.StatusOK)
}

// iamDeleteGroup handles group deletion via the dashboard.
func (h *DashboardHandler) iamDeleteGroup(c *echo.Context) error {
	r := c.Request()
	w := c.Response()

	groupName := r.URL.Query().Get("name")
	if groupName == "" {
		return c.String(http.StatusBadRequest, "Missing name")
	}

	if h.IAMOps != nil {
		if err := h.IAMOps.Backend.DeleteGroup(groupName); err != nil {
			h.Logger.Error("failed to delete IAM group", "error", err)

			return c.String(http.StatusInternalServerError, "Failed to delete group")
		}
	}

	w.Header().Set("Hx-Redirect", "/dashboard/iam")

	return c.NoContent(http.StatusOK)
}
