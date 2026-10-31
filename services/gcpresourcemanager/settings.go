package gcpresourcemanager

import "time"

const (
	// DefaultPort is the dedicated gRPC and REST listener's default TCP port.
	DefaultPort = 8148
	// DefaultOrganizationID is the id of the single emulated organization.
	DefaultOrganizationID = "123456789012"
	// DefaultDomain is the display name (the domain) of the emulated organization.
	DefaultDomain = "gopherstack.local"
	// DefaultSeedProjects are the project ids created at startup.
	DefaultSeedProjects = "gopherstack-test"
	// DefaultDeleteRetention is how long a DELETE_REQUESTED project or folder stays restorable.
	DefaultDeleteRetention = 30 * 24 * time.Hour
)

// Settings holds service-level configuration, embedded in the root CLI command as CLI.GCPResourceManager.
type Settings struct {
	OrganizationID  string        `json:"organizationId"  env:"GCP_RESOURCEMANAGER_ORGANIZATION_ID"  default:"123456789012"       name:"organization-id"  help:"Numeric id of the one emulated organization (organizations/ID)."`                                                                             //nolint:lll // config struct tags are intentionally verbose
	Domain          string        `json:"domain"          env:"GCP_RESOURCEMANAGER_DOMAIN"           default:"gopherstack.local"  name:"domain"           help:"Display name (domain) of the emulated organization."`                                                                                        //nolint:lll // config struct tags are intentionally verbose
	SeedProjects    string        `json:"seedProjects"    env:"GCP_RESOURCEMANAGER_SEED_PROJECTS"    default:"gopherstack-test"   name:"seed-projects"    help:"Comma-separated project ids created at startup, with no parent, so they can be read and used for IAM and Service Usage."`                                  //nolint:lll // config struct tags are intentionally verbose
	DeleteRetention time.Duration `json:"deleteRetention" env:"GCP_RESOURCEMANAGER_DELETE_RETENTION" default:"720h"               name:"delete-retention" help:"How long a DELETE_REQUESTED project or folder can be undeleted before it is purged (GCP: about 30 days). 0 purges at once and frees the id."` //nolint:lll // config struct tags are intentionally verbose
	Port            int           `json:"port"            env:"GCP_RESOURCEMANAGER_PORT"             default:"8148"               name:"port"             help:"Fixed TCP port for the dedicated Resource Manager and Service Usage gRPC and REST listener; startup fails if it is unavailable."`                         //nolint:lll // config struct tags are intentionally verbose
}

// DefaultSettings returns the settings used when no ConfigProvider is available.
func DefaultSettings() Settings {
	return Settings{
		OrganizationID: DefaultOrganizationID, Domain: DefaultDomain, SeedProjects: DefaultSeedProjects,
		DeleteRetention: DefaultDeleteRetention, Port: DefaultPort,
	}
}
