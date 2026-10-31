package gcpresourcemanager

import (
	"slices"
	"strings"
)

// catalogEntry is one Google API a project can enable.
type catalogEntry struct {
	name    string
	title   string
	summary string
	depends []string
	// defaultOn marks the services every API-created project starts with.
	defaultOn bool
}

// catalog lists the Google APIs the emulator knows: one per GCP emulator service plus the common ones Terraform
// enables. Names and titles follow the public Service Usage listing; dependencies are the few that gate other
// services here (container needs compute, functions need build and registry, ...).
//
//nolint:gochecknoglobals // immutable catalog
var catalog = []catalogEntry{
	{name: "apigateway.googleapis.com", title: "API Gateway API", summary: "Create and manage API gateways.", depends: []string{"servicemanagement.googleapis.com", "servicecontrol.googleapis.com"}},
	{name: "artifactregistry.googleapis.com", title: "Artifact Registry API", summary: "Store and manage build artifacts and container images."},
	{name: "batch.googleapis.com", title: "Batch API", summary: "Run batch jobs on Google Cloud.", depends: []string{"compute.googleapis.com"}},
	{name: "bigquery.googleapis.com", title: "BigQuery API", summary: "A data platform for customers to create, manage, share and query data.", defaultOn: true},
	{name: "bigquerymigration.googleapis.com", title: "BigQuery Migration API", summary: "Migrate data warehouses to BigQuery.", defaultOn: true},
	{name: "bigquerystorage.googleapis.com", title: "BigQuery Storage API", summary: "Read and write BigQuery table data at scale.", defaultOn: true},
	{name: "bigtable.googleapis.com", title: "Cloud Bigtable API", summary: "Data API for Cloud Bigtable."},
	{name: "bigtableadmin.googleapis.com", title: "Cloud Bigtable Admin API", summary: "Administer Cloud Bigtable tables and instances."},
	{name: "cloudapis.googleapis.com", title: "Google Cloud APIs", summary: "Umbrella for Google Cloud APIs.", defaultOn: true},
	{name: "cloudasset.googleapis.com", title: "Cloud Asset API", summary: "Inventory and history of Google Cloud resources."},
	{name: "cloudbilling.googleapis.com", title: "Cloud Billing API", summary: "Manage billing for Google Cloud projects."},
	{name: "cloudbuild.googleapis.com", title: "Cloud Build API", summary: "Build, test and deploy software.", depends: []string{"artifactregistry.googleapis.com"}},
	{name: "cloudfunctions.googleapis.com", title: "Cloud Functions API", summary: "Manage lightweight user-provided functions.", depends: []string{"cloudbuild.googleapis.com", "artifactregistry.googleapis.com"}},
	{name: "cloudkms.googleapis.com", title: "Cloud Key Management Service (KMS) API", summary: "Manage encryption keys on Google Cloud."},
	{name: "cloudresourcemanager.googleapis.com", title: "Cloud Resource Manager API", summary: "Create, read and update metadata for Google Cloud resource containers."},
	{name: "cloudscheduler.googleapis.com", title: "Cloud Scheduler API", summary: "Create and manage cron jobs."},
	{name: "cloudtasks.googleapis.com", title: "Cloud Tasks API", summary: "Manage distributed task queues."},
	{name: "cloudtrace.googleapis.com", title: "Cloud Trace API", summary: "Send and retrieve latency data.", defaultOn: true},
	{name: "clouddebugger.googleapis.com", title: "Cloud Debugger API", summary: "Examine the call stack and variables of a running application.", defaultOn: true},
	{name: "compute.googleapis.com", title: "Compute Engine API", summary: "Create and run virtual machines on Google Cloud."},
	{name: "container.googleapis.com", title: "Kubernetes Engine API", summary: "Build and manage container-based applications.", depends: []string{"compute.googleapis.com"}},
	{name: "containerregistry.googleapis.com", title: "Container Registry API", summary: "Store and manage container images."},
	{name: "datastore.googleapis.com", title: "Cloud Datastore API", summary: "Store schemaless NoSQL data.", defaultOn: true},
	{name: "dns.googleapis.com", title: "Cloud DNS API", summary: "Manage DNS zones and records."},
	{name: "eventarc.googleapis.com", title: "Eventarc API", summary: "Build event-driven applications."},
	{name: "firestore.googleapis.com", title: "Cloud Firestore API", summary: "Store and sync data in a document database."},
	{name: "iam.googleapis.com", title: "Identity and Access Management (IAM) API", summary: "Manage identity and access control for Google Cloud resources."},
	{name: "iamcredentials.googleapis.com", title: "IAM Service Account Credentials API", summary: "Create short-lived credentials for service accounts.", depends: []string{"iam.googleapis.com"}},
	{name: "identitytoolkit.googleapis.com", title: "Identity Toolkit API", summary: "Toolkit for authenticating users of an app."},
	{name: "logging.googleapis.com", title: "Cloud Logging API", summary: "Write log entries and manage logging configuration.", defaultOn: true},
	{name: "monitoring.googleapis.com", title: "Cloud Monitoring API", summary: "Manage monitoring data and configuration.", defaultOn: true},
	{name: "networkservices.googleapis.com", title: "Network Services API", summary: "Configure network services."},
	{name: "orgpolicy.googleapis.com", title: "Organization Policy API", summary: "Manage organization policies."},
	{name: "pubsub.googleapis.com", title: "Cloud Pub/Sub API", summary: "Ingest event streams and deliver them to subscribers."},
	{name: "redis.googleapis.com", title: "Google Cloud Memorystore for Redis API", summary: "Manage Redis instances."},
	{name: "run.googleapis.com", title: "Cloud Run Admin API", summary: "Deploy and manage user-provided container images."},
	{name: "secretmanager.googleapis.com", title: "Secret Manager API", summary: "Store sensitive data such as API keys and passwords."},
	{name: "servicecontrol.googleapis.com", title: "Service Control API", summary: "Report and check service usage."},
	{name: "servicemanagement.googleapis.com", title: "Service Management API", summary: "Manage Google API service configurations.", defaultOn: true},
	{name: "servicenetworking.googleapis.com", title: "Service Networking API", summary: "Provide private connectivity to managed services."},
	{name: "serviceusage.googleapis.com", title: "Service Usage API", summary: "Enable and disable Google services on a project.", defaultOn: true},
	{name: "spanner.googleapis.com", title: "Cloud Spanner API", summary: "Manage Cloud Spanner instances and databases."},
	{name: "sql-component.googleapis.com", title: "Cloud SQL", summary: "Cloud SQL component.", defaultOn: true},
	{name: "sqladmin.googleapis.com", title: "Cloud SQL Admin API", summary: "Manage Cloud SQL databases.", depends: []string{"sql-component.googleapis.com"}},
	{name: "storage-api.googleapis.com", title: "Google Cloud Storage JSON API", summary: "Lets you store and retrieve potentially-large, immutable data objects.", defaultOn: true},
	{name: "storage-component.googleapis.com", title: "Cloud Storage", summary: "Google Cloud Storage component.", defaultOn: true},
	{name: "storage.googleapis.com", title: "Cloud Storage API", summary: "Store and retrieve data objects.", defaultOn: true},
	{name: "sts.googleapis.com", title: "Security Token Service API", summary: "Exchange credentials for short-lived access tokens."},
	{name: "vpcaccess.googleapis.com", title: "Serverless VPC Access API", summary: "Connect serverless services to a VPC network."},
	{name: "workflows.googleapis.com", title: "Workflows API", summary: "Orchestrate and automate Google Cloud and HTTP-based API services."},
	{name: "workflowexecutions.googleapis.com", title: "Workflow Executions API", summary: "Execute workflows."},
}

func lookupService(name string) (catalogEntry, bool) {
	i := slices.IndexFunc(catalog, func(e catalogEntry) bool { return e.name == name })
	if i < 0 {
		return catalogEntry{}, false
	}

	return catalog[i], true
}

// dependentsOf lists the entries that declare name as a dependency.
func dependentsOf(name string) []string {
	var out []string

	for _, e := range catalog {
		if slices.Contains(e.depends, name) {
			out = append(out, e.name)
		}
	}

	return out
}

// CatalogServices returns the googleapis.com names of every service the emulator can enable, sorted.
func CatalogServices() []string {
	out := make([]string, 0, len(catalog))
	for _, e := range catalog {
		out = append(out, e.name)
	}

	slices.Sort(out)

	return out
}

func shortServiceName(name string) string { return strings.TrimSuffix(name, ".googleapis.com") }
