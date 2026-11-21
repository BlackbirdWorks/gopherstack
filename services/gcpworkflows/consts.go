package gcpworkflows

import "google.golang.org/genproto/googleapis/api/monitoredres"

const (
	resourceWorkflow = "workflows.googleapis.com/Workflow"
	labelExecutionID = "workflows.googleapis.com/execution_id"
	keyLocation      = "location"
	keyRevisionID    = "revision_id"
	keyStep          = "step"
	fieldCreateTime  = "createTime"
	fieldUpdateTime  = "updateTime"
)

func (r runInfo) resource() *monitoredres.MonitoredResource {
	return &monitoredres.MonitoredResource{
		Type: resourceWorkflow,
		Labels: map[string]string{
			"project_id": r.project, keyLocation: r.location, "workflow_id": r.workflow, keyRevisionID: r.revision,
		},
	}
}
