package dashboard

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// backupVaultView is the view model for a single AWS Backup vault.
type backupVaultView struct {
	Name string
	ARN  string
}

// backupIndexData is the template data for the Backup index page.
type backupIndexData struct {
	PageData

	Vaults []backupVaultView
}

// backupIndex renders the Backup dashboard index.
//

func (h *DashboardHandler) backupIndex(c *echo.Context) error {
	w := c.Response()

	if h.BackupOps == nil {
		h.renderTemplate(w, "backup/index.html", backupIndexData{
			PageData: PageData{Title: "Backup Vaults", ActiveTab: "backup",
				Snippet: &SnippetData{
					ID:    "backup-operations",
					Title: "Using Backup",
					Cli:   `aws backup help --endpoint-url http://localhost:8000`,
					Go: `// Initialize AWS SDK v2 for Backup
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
client := backup.NewFromConfig(cfg)`,
					Python: `# Initialize boto3 client for Backup
import boto3

client = boto3.client('backup', endpoint_url='http://localhost:8000')`,
				}},
			Vaults: []backupVaultView{},
		})

		return nil
	}

	vaults := h.BackupOps.Backend.ListBackupVaults()
	views := make([]backupVaultView, 0, len(vaults))

	for _, v := range vaults {
		views = append(views, backupVaultView{
			Name: v.BackupVaultName,
			ARN:  v.BackupVaultArn,
		})
	}

	h.renderTemplate(w, "backup/index.html", backupIndexData{
		PageData: PageData{Title: "Backup Vaults", ActiveTab: "backup",
			Snippet: &SnippetData{
				ID:    "backup-operations",
				Title: "Using Backup",
				Cli:   `aws backup help --endpoint-url http://localhost:8000`,
				Go: `// Initialize AWS SDK v2 for Backup
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
client := backup.NewFromConfig(cfg)`,
				Python: `# Initialize boto3 client for Backup
import boto3

client = boto3.client('backup', endpoint_url='http://localhost:8000')`,
			}},
		Vaults: views,
	})

	return nil
}

// backupCreateVault handles POST /dashboard/backup/vault/create.
func (h *DashboardHandler) backupCreateVault(c *echo.Context) error {
	if h.BackupOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	_, err := h.BackupOps.Backend.CreateBackupVault(name, "", "", nil)
	if err != nil {
		h.Logger.Error("failed to create backup vault", "name", name, "error", err)

		return c.NoContent(http.StatusBadRequest)
	}

	return c.Redirect(http.StatusFound, "/dashboard/backup")
}

// backupDeleteVault handles POST /dashboard/backup/vault/delete.
func (h *DashboardHandler) backupDeleteVault(c *echo.Context) error {
	if h.BackupOps == nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}

	if err := c.Request().ParseForm(); err != nil {
		return c.NoContent(http.StatusBadRequest)
	}

	name := c.Request().FormValue("name")
	if name == "" {
		return c.NoContent(http.StatusBadRequest)
	}

	if err := h.BackupOps.Backend.DeleteBackupVault(name); err != nil {
		h.Logger.Error("failed to delete backup vault", "name", name, "error", err)

		return c.NoContent(http.StatusNotFound)
	}

	return c.Redirect(http.StatusFound, "/dashboard/backup")
}
