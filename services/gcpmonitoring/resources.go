package gcpmonitoring

import (
	"slices"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/genproto/googleapis/api/label"
	"google.golang.org/genproto/googleapis/api/monitoredres"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	labelProject  = "project_id"
	labelLocation = "location"
	labelZone     = "zone"
	labelRegion   = "region"
	labelHost     = "host"
	typeGlobal    = "global"
	typeUptimeURL = "uptime_url"
	defaultPage   = 100
	maxPage       = 100000
)

type resourceDef struct {
	typ, display, desc string
	labels             []string
}

//nolint:gochecknoglobals // immutable catalog of the monitored resource types
var resourceCatalog = []resourceDef{
	{
		typeGlobal,
		"Global",
		"A resource type that is not associated with any specific resource.",
		[]string{labelProject},
	},
	{typeGCEInstance, "VM Instance", "A virtual machine instance hosted in Compute Engine.",
		[]string{labelProject, "instance_id", labelZone}},
	{"gce_instance_group", "Instance Group", "A Compute Engine instance group.",
		[]string{labelProject, "instance_group_id", labelZone}},
	{"gce_disk", "Compute Engine disk", "A Compute Engine disk.", []string{labelProject, "disk_id", labelZone}},
	{"k8s_container", "Kubernetes Container", "A Kubernetes container instance.",
		[]string{labelProject, labelLocation, "cluster_name", "namespace_name", "pod_name", "container_name"}},
	{
		"k8s_cluster",
		"Kubernetes Cluster",
		"A Kubernetes cluster.",
		[]string{labelProject, labelLocation, "cluster_name"},
	},
	{typeCloudRunRevision, "Cloud Run Revision", "A revision of a Cloud Run service.",
		[]string{labelProject, "service_name", "revision_name", labelLocation, "configuration_name"}},
	{"cloud_run_job", "Cloud Run Job", "A Cloud Run job.", []string{labelProject, "job_name", labelLocation}},
	{"cloud_function", "Cloud Function", "A Cloud Function.", []string{labelProject, "function_name", labelRegion}},
	{"cloud_scheduler_job", "Cloud Scheduler Job", "A Cloud Scheduler job.",
		[]string{labelProject, "job_id", labelLocation}},
	{"cloud_tasks_queue", "Cloud Tasks Queue", "A Cloud Tasks queue.",
		[]string{labelProject, "queue_id", "target_type"}},
	{"gcs_bucket", "Cloud Storage Bucket", "A Cloud Storage bucket.",
		[]string{labelProject, "bucket_name", labelLocation}},
	{"pubsub_topic", "Pub/Sub Topic", "A Pub/Sub topic.", []string{labelProject, "topic_id"}},
	{"pubsub_subscription", "Pub/Sub Subscription", "A Pub/Sub subscription.",
		[]string{labelProject, "subscription_id"}},
	{"bigquery_dataset", "BigQuery Dataset", "A BigQuery dataset.", []string{labelProject, "dataset_id"}},
	{"cloudsql_database", "Cloud SQL Database", "A Cloud SQL database.",
		[]string{labelProject, "database_id", labelRegion}},
	{"cloudkms_cryptokey", "Cloud KMS CryptoKey", "A Cloud KMS crypto key.",
		[]string{labelProject, labelLocation, "key_ring_id", "crypto_key_id"}},
	{"consumed_api", "Consumed API", "A consumed Google API.",
		[]string{labelProject, "service", "method", "version", "credential_id"}},
	{"generic_node", "Generic Node", "A generic node.",
		[]string{labelProject, labelLocation, "namespace", "node_id"}},
	{"generic_task", "Generic Task", "A generic task.",
		[]string{labelProject, labelLocation, "namespace", "job", "task_id"}},
	{typeUptimeURL, "Uptime Check URL", "A URL checked by an uptime check.", []string{labelProject, labelHost}},
	{"project", "Project", "A Google Cloud project.", []string{labelProject}},
}

func catalogDef(typ string) (resourceDef, bool) {
	i := slices.IndexFunc(resourceCatalog, func(d resourceDef) bool { return d.typ == typ })
	if i < 0 {
		return resourceDef{}, false
	}

	return resourceCatalog[i], true
}

func resourceDescriptor(d resourceDef) *monitoredres.MonitoredResourceDescriptor {
	out := &monitoredres.MonitoredResourceDescriptor{
		Name:        "projects/-/monitoredResourceDescriptors/" + d.typ,
		Type:        d.typ,
		DisplayName: d.display,
		Description: d.desc,
	}

	for _, l := range d.labels {
		out.Labels = append(out.Labels, &label.LabelDescriptor{
			Key: l, ValueType: label.LabelDescriptor_STRING, Description: l,
		})
	}

	return out
}

func resourceDescriptorName(project, typ string) string {
	return projectScope + project + "/" + collResourceDescriptors + "/" + typ
}

// ListMonitoredResourceDescriptors pages the static catalog, narrowed by a resource.type filter.
func (b *InMemoryBackend) ListMonitoredResourceDescriptors(
	req *monitoringpb.ListMonitoredResourceDescriptorsRequest,
) ([]*monitoredres.MonitoredResourceDescriptor, string, error) {
	project, err := projectOf(req.GetName())
	if err != nil {
		return nil, "", err
	}

	if err = pageTokenOK(req.GetPageToken()); err != nil {
		return nil, "", err
	}

	f, err := parseOptionalFilter(req.GetFilter())
	if err != nil {
		return nil, "", err
	}

	all := make([]*monitoredres.MonitoredResourceDescriptor, 0, len(resourceCatalog))

	for _, d := range resourceCatalog {
		env := mapEnv{pathResourceTyp: {d.typ}, "resource.display_name": {d.display}}
		if f.matches(env) {
			rd := resourceDescriptor(d)
			rd.Name = resourceDescriptorName(project, d.typ)
			all = append(all, rd)
		}
	}

	pg := page.New(all, req.GetPageToken(), clampPage(req.GetPageSize()), defaultPage)

	return pg.Data, pg.Next, nil
}

// GetMonitoredResourceDescriptor returns one catalog entry.
func (b *InMemoryBackend) GetMonitoredResourceDescriptor(
	name string,
) (*monitoredres.MonitoredResourceDescriptor, error) {
	project, typ, err := splitName(name, collResourceDescriptors)
	if err != nil {
		return nil, err
	}

	d, ok := catalogDef(typ)
	if !ok {
		return nil, errNotFound("Monitored resource descriptor %s does not exist.", name)
	}

	rd := resourceDescriptor(d)
	rd.Name = resourceDescriptorName(project, typ)

	return rd, nil
}

func clampPage(size int32) int {
	switch {
	case size <= 0:
		return defaultPage
	case size > maxPage:
		return maxPage
	default:
		return int(size)
	}
}

func pageTokenOK(token string) error {
	if err := page.ValidateToken(token); err != nil {
		return errInvalid("Invalid page token.")
	}

	return nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	return keys
}
