package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// verifiedPermissionsPolicyStoreView is the view model for a single Verified Permissions policy store.
type verifiedPermissionsPolicyStoreView struct {
	PolicyStoreID string
	Arn           string
	Description   string
}

// verifiedPermissionsIndexData is the template data for the Verified Permissions index page.
type verifiedPermissionsIndexData struct {
	PageData

	PolicyStores []verifiedPermissionsPolicyStoreView
}

func verifiedPermissionsSnippet() *SnippetData {
	return &SnippetData{
		ID:    "verifiedpermissions-operations",
		Title: "Using Verified Permissions",
		Cli: `# Create a Verified Permissions policy store
aws verifiedpermissions create-policy-store \
  --validation-settings "mode=OFF" \
  --endpoint-url http://localhost:8000

# List policy stores
aws verifiedpermissions list-policy-stores \
  --endpoint-url http://localhost:8000`,
		Go: `// Initialize AWS SDK v2 for Verified Permissions
cfg, err := config.LoadDefaultConfig(context.TODO(),
    config.WithBaseEndpoint("http://localhost:8000"),
)
if err != nil {
    log.Fatal(err)
}
client := verifiedpermissions.NewFromConfig(cfg)`,
		Python: `# Initialize boto3 client for Verified Permissions
import boto3

client = boto3.client('verifiedpermissions', endpoint_url='http://localhost:8000')
response = client.create_policy_store(
    validationSettings={'mode': 'OFF'}
)`,
	}
}

// setupVerifiedPermissionsRoutes registers the Verified Permissions dashboard routes.
func (h *DashboardHandler) setupVerifiedPermissionsRoutes() {
	h.SubRouter.GET("/dashboard/verifiedpermissions", h.verifiedPermissionsIndex)
	h.SubRouter.POST("/dashboard/verifiedpermissions/create-policy-store", h.verifiedPermissionsCreatePolicyStore)
	h.SubRouter.POST("/dashboard/verifiedpermissions/delete-policy-store", h.verifiedPermissionsDeletePolicyStore)
}

// verifiedPermissionsIndex renders the Verified Permissions dashboard index.
func (h *DashboardHandler) verifiedPermissionsIndex(c *echo.Context) error {
	w := c.Response()

	pageData := PageData{
		Title:     "Verified Permissions",
		ActiveTab: "verifiedpermissions",
		Snippet:   verifiedPermissionsSnippet(),
	}

	if h.VerifiedPermissionsOps == nil {
		h.renderTemplate(w, "verifiedpermissions/index.html", verifiedPermissionsIndexData{
			PageData:     pageData,
			PolicyStores: []verifiedPermissionsPolicyStoreView{},
		})

		return nil
	}

	stores := h.VerifiedPermissionsOps.Backend.ListPolicyStores()
	views := make([]verifiedPermissionsPolicyStoreView, 0, len(stores))

	for i := range stores {
		ps := &stores[i]
		views = append(views, verifiedPermissionsPolicyStoreView{
			PolicyStoreID: ps.PolicyStoreID,
			Arn:           ps.Arn,
			Description:   ps.Description,
		})
	}

	h.renderTemplate(w, "verifiedpermissions/index.html", verifiedPermissionsIndexData{
		PageData:     pageData,
		PolicyStores: views,
	})

	return nil
}

// verifiedPermissionsCreatePolicyStore handles POST /dashboard/verifiedpermissions/create-policy-store.
func (h *DashboardHandler) verifiedPermissionsCreatePolicyStore(c *echo.Context) error {
	if h.VerifiedPermissionsOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	description := c.Request().FormValue("description")

	if _, err := h.VerifiedPermissionsOps.Backend.CreatePolicyStore(description, nil); err != nil {
		h.Logger.Error("failed to create verified permissions policy store", "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/verifiedpermissions")
}

// verifiedPermissionsDeletePolicyStore handles POST /dashboard/verifiedpermissions/delete-policy-store.
func (h *DashboardHandler) verifiedPermissionsDeletePolicyStore(c *echo.Context) error {
	if h.VerifiedPermissionsOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	policyStoreID := c.Request().FormValue("policy_store_id")
	if policyStoreID == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.VerifiedPermissionsOps.Backend.DeletePolicyStore(policyStoreID); err != nil {
		h.Logger.Error(
			"failed to delete verified permissions policy store",
			"policy_store_id",
			policyStoreID,
			"error",
			err,
		)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusSeeOther, "/dashboard/verifiedpermissions")
}
