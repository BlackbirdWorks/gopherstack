package gcpstorage

import (
	"context"

	"google.golang.org/genproto/googleapis/api/monitoredres"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpaudit"
)

const auditService = "storage.googleapis.com"

func (b *InMemoryBackend) bucketOwner(name string) (string, string) {
	b.mu.RLock("bucketOwner")
	defer b.mu.RUnlock()

	if bk, ok := b.buckets[name]; ok {
		return bk.Project, bk.Location
	}

	return "", ""
}

func auditBucketResource(project, bucket, location string) *monitoredres.MonitoredResource {
	return &monitoredres.MonitoredResource{
		Type:   "gcs_bucket",
		Labels: map[string]string{"bucket_name": bucket, "project_id": project, "location": location},
	}
}

func (b *InMemoryBackend) auditBucket(method, project string, bk *Bucket) {
	b.Audit.Emit(context.Background(), gcpaudit.Record{
		Time: b.now(), Project: project, ServiceName: auditService, MethodName: method,
		ResourceName: "projects/_/buckets/" + bk.Name, Permission: method, Kind: gcpaudit.Activity,
		Resource: auditBucketResource(project, bk.Name, bk.Location),
	})
}

func (b *InMemoryBackend) auditObject(ev ObjectEvent) {
	var method string

	switch ev.Type {
	case ObjectFinalized:
		method = "storage.objects.create"
	case ObjectDeleted:
		method = "storage.objects.delete"
	case ObjectMetadataUpdated:
		method = "storage.objects.update"
	case ObjectArchived:
		return
	}

	if !b.Audit.Enabled() {
		return
	}

	project, loc := b.bucketOwner(ev.Object.Bucket)

	b.Audit.Emit(context.Background(), gcpaudit.Record{
		Time: ev.Time, Project: project, ServiceName: auditService, MethodName: method,
		ResourceName: "projects/_/buckets/" + ev.Object.Bucket + "/objects/" + ev.Object.Name,
		Permission:   method, Kind: gcpaudit.DataWrite, Resource: auditBucketResource(project, ev.Object.Bucket, loc),
	})
}
