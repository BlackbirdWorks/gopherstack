package gcpresourcemanager_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuditLogs(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	sink := &auditSink{}
	e.backend.Audit.SetSink(sink)

	e.grpc.createProject(t, "audited-proj", "")

	logs := sink.logs(t)
	require.NotEmpty(t, logs)

	first := logs[0]
	assert.Equal(t, "google.cloud.resourcemanager.v3.Projects.CreateProject", first.GetMethodName())
	assert.Equal(t, "cloudresourcemanager.googleapis.com", first.GetServiceName())
	assert.Equal(t, "projects/audited-proj", first.GetResourceName())
	assert.Contains(t, sink.logNames()[0], "projects/audited-proj/logs/cloudaudit.googleapis.com%2Factivity")
}
