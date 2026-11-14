package gcpbigquery

import (
	"context"
	"time"

	"google.golang.org/genproto/googleapis/api/monitoredres"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpaudit"
)

func (b *InMemoryBackend) auditCall(method, resource, project string) {
	b.Audit.Emit(context.Background(), gcpaudit.Record{
		Time: time.Now(), Project: project, ServiceName: "bigquery.googleapis.com", MethodName: method,
		ResourceName: resource, Kind: gcpaudit.Activity,
		Resource: &monitoredres.MonitoredResource{
			Type: "bigquery_resource", Labels: map[string]string{"project_id": project},
		},
	})
}
