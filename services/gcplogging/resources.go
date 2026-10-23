package gcplogging

import (
	"google.golang.org/genproto/googleapis/api/label"
	"google.golang.org/genproto/googleapis/api/monitoredres"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	labelProject  = "project_id"
	labelLocation = "location"
)

type resourceDef struct {
	typ, display, desc string
	labels             []string
}

//nolint:gochecknoglobals // immutable catalog of the monitored resource types
var resourceCatalog = []resourceDef{
	{"global", "Global", "A resource type that is not associated with any specific resource.", []string{labelProject}},
	{"gce_instance", "VM Instance", "A virtual machine instance hosted in Compute Engine.",
		[]string{labelProject, "instance_id", "zone"}},
	{"gce_firewall_rule", "Compute Engine firewall rule", "A firewall rule hosted in Compute Engine.",
		[]string{labelProject, "firewall_rule_id"}},
	{
		"gce_network",
		"Compute Engine network",
		"A network hosted in Compute Engine.",
		[]string{labelProject, "network_id"},
	},
	{"gae_app", "GAE Application", "An application running in Google App Engine.",
		[]string{labelProject, "module_id", "version_id"}},
	{"k8s_container", "Kubernetes Container", "A Kubernetes container instance.",
		[]string{labelProject, labelLocation, "cluster_name", "namespace_name", "pod_name", "container_name"}},
	{
		"k8s_cluster",
		"Kubernetes Cluster",
		"A Kubernetes cluster.",
		[]string{labelProject, labelLocation, "cluster_name"},
	},
	{"cloud_run_revision", "Cloud Run Revision", "A revision of a Cloud Run service.",
		[]string{labelProject, "service_name", "revision_name", labelLocation, "configuration_name"}},
	{"cloud_run_job", "Cloud Run Job", "A Cloud Run job.", []string{labelProject, "job_name", labelLocation}},
	{"cloud_function", "Cloud Function", "A Cloud Function.", []string{labelProject, "function_name", "region"}},
	{
		"cloud_scheduler_job",
		"Cloud Scheduler Job",
		"A Cloud Scheduler job.",
		[]string{labelProject, "job_id", labelLocation},
	},
	{
		"cloud_tasks_queue",
		"Cloud Tasks Queue",
		"A Cloud Tasks queue.",
		[]string{labelProject, "queue_id", "target_type"},
	},
	{
		"gcs_bucket",
		"Cloud Storage Bucket",
		"A Cloud Storage bucket.",
		[]string{labelProject, "bucket_name", labelLocation},
	},
	{"pubsub_topic", "Pub/Sub Topic", "A Pub/Sub topic.", []string{labelProject, "topic_id"}},
	{
		"pubsub_subscription",
		"Pub/Sub Subscription",
		"A Pub/Sub subscription.",
		[]string{labelProject, "subscription_id"},
	},
	{"bigquery_resource", "BigQuery Resource", "A BigQuery resource.", []string{labelProject}},
	{"bigquery_dataset", "BigQuery Dataset", "A BigQuery dataset.", []string{labelProject, "dataset_id"}},
	{
		"cloudsql_database",
		"Cloud SQL Database",
		"A Cloud SQL database.",
		[]string{labelProject, "database_id", "region"},
	},
	{"audited_resource", "Audited Resource", "An audited resource.", []string{"service", "method"}},
	{"project", "Project", "A Google Cloud project.", []string{labelProject}},
	{"logging_sink", "Logging Sink", "A Cloud Logging sink.", []string{labelProject, "name", pathDestination}},
	{"secretmanager.googleapis.com/Secret", "Secret Manager Secret", "A Secret Manager secret.",
		[]string{"resource_container", labelLocation, "secret_id"}},
	{"cloudkms_cryptokey", "Cloud KMS CryptoKey", "A Cloud KMS crypto key.",
		[]string{labelProject, labelLocation, "key_ring_id", "crypto_key_id"}},
}

func descriptorOf(d resourceDef) *monitoredres.MonitoredResourceDescriptor {
	out := &monitoredres.MonitoredResourceDescriptor{
		Name:        "projects/-/monitoredResourceDescriptors/" + d.typ,
		Type:        d.typ,
		DisplayName: d.display,
		Description: d.desc,
	}

	for _, l := range d.labels {
		out.Labels = append(
			out.Labels,
			&label.LabelDescriptor{Key: l, ValueType: label.LabelDescriptor_STRING, Description: l},
		)
	}

	return out
}

// ListMonitoredResourceDescriptors pages the static catalog of monitored resource types.
func (b *InMemoryBackend) ListMonitoredResourceDescriptors(
	token string, size int32,
) ([]*monitoredres.MonitoredResourceDescriptor, string, error) {
	if err := pageTokenOK(token); err != nil {
		return nil, "", err
	}

	all := make([]*monitoredres.MonitoredResourceDescriptor, 0, len(resourceCatalog))
	for _, d := range resourceCatalog {
		all = append(all, descriptorOf(d))
	}

	pg := page.New(all, token, clampPage(size), defaultPageSize)

	return pg.Data, pg.Next, nil
}
