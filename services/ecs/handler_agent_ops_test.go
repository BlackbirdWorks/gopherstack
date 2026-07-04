package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----- DescribeServiceRevisions -----

func TestECS_DescribeServiceRevisions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tdRec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "revfam",
		"containerDefinitions": []map[string]any{
			{"name": "web", "image": "nginx", "memory": 128},
		},
	})
	require.Equal(t, http.StatusOK, tdRec.Code)

	svcRec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "rev-svc",
		"taskDefinition": "revfam",
		"desiredCount":   1,
	})
	require.Equal(t, http.StatusOK, svcRec.Code)

	var svcResp map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))

	svc := svcResp["service"].(map[string]any)
	deployments := svc["deployments"].([]any)
	require.Len(t, deployments, 1)

	// ServiceRevisionArn is tracked internally (mirroring how real AWS surfaces it
	// via ListServiceDeployments' TargetServiceRevision) rather than on the plain
	// Deployment wire shape, so fetch it directly from backend state.
	backendSvcs, _, err := h.Backend.DescribeServices("default", []string{"rev-svc"})
	require.NoError(t, err)
	require.Len(t, backendSvcs, 1)
	require.Len(t, backendSvcs[0].Deployments, 1)

	revisionArn := backendSvcs[0].Deployments[0].ServiceRevisionArn
	require.NotEmpty(t, revisionArn, "deployment must carry a serviceRevisionArn")
	assert.Contains(t, revisionArn, ":service-revision/default/rev-svc/")

	tests := []struct {
		name         string
		arns         []string
		wantRevCount int
		wantFailures int
	}{
		{
			name:         "known revision arn returns the revision",
			arns:         []string{revisionArn},
			wantRevCount: 1,
			wantFailures: 0,
		},
		{
			name: "unknown arn returns a failure",
			arns: []string{
				"arn:aws:ecs:us-east-1:000000000000:service-revision/default/rev-svc/does-not-exist",
			},
			wantRevCount: 0,
			wantFailures: 1,
		},
		{
			name: "mixed known and unknown",
			arns: []string{
				revisionArn,
				"arn:aws:ecs:us-east-1:000000000000:service-revision/default/rev-svc/missing",
			},
			wantRevCount: 1,
			wantFailures: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doECSRequest(t, h, "DescribeServiceRevisions", map[string]any{
				"serviceRevisionArns": tt.arns,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			revs, _ := resp["serviceRevisions"].([]any)
			failures, _ := resp["failures"].([]any)
			assert.Len(t, revs, tt.wantRevCount)
			assert.Len(t, failures, tt.wantFailures)

			if tt.wantRevCount > 0 {
				rev := revs[0].(map[string]any)
				assert.Equal(t, svc["serviceArn"], rev["serviceArn"])
				assert.Equal(t, svc["taskDefinition"], rev["taskDefinition"])
				assert.Equal(t, revisionArn, rev["serviceRevisionArn"])
			}
		})
	}
}

func TestECS_DescribeServiceRevisions_TracksUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "revfam2",
		"containerDefinitions": []map[string]any{{"name": "web", "image": "nginx", "memory": 128}},
	})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "revfam2",
		"containerDefinitions": []map[string]any{{"name": "web", "image": "nginx:2", "memory": 128}},
	})

	svcRec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "rev-svc-2",
		"taskDefinition": "revfam2:1",
		"desiredCount":   1,
	})
	require.Equal(t, http.StatusOK, svcRec.Code)

	backendSvcs, _, err := h.Backend.DescribeServices("default", []string{"rev-svc-2"})
	require.NoError(t, err)
	require.Len(t, backendSvcs, 1)
	require.Len(t, backendSvcs[0].Deployments, 1)
	firstArn := backendSvcs[0].Deployments[0].ServiceRevisionArn
	require.NotEmpty(t, firstArn)

	updRec := doECSRequest(t, h, "UpdateService", map[string]any{
		"service":        "rev-svc-2",
		"taskDefinition": "revfam2:2",
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	backendSvcs, _, err = h.Backend.DescribeServices("default", []string{"rev-svc-2"})
	require.NoError(t, err)
	require.Len(t, backendSvcs, 1)
	require.Len(t, backendSvcs[0].Deployments, 2)

	secondArn := backendSvcs[0].Deployments[0].ServiceRevisionArn
	require.NotEmpty(t, secondArn)
	assert.NotEqual(t, firstArn, secondArn, "update must mint a new service revision")

	rec := doECSRequest(t, h, "DescribeServiceRevisions", map[string]any{
		"serviceRevisionArns": []string{firstArn, secondArn},
	})

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	revs, _ := resp["serviceRevisions"].([]any)
	assert.Len(t, revs, 2, "both the original and rotated revision remain describable")
}

// ----- DiscoverPollEndpoint -----

func TestECS_DiscoverPollEndpoint(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECSRequest(t, h, "DiscoverPollEndpoint", map[string]any{
		"clusterArn":           "default",
		"containerInstanceArn": "arn:aws:ecs:us-east-1:000000000000:container-instance/default/abc",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Contains(t, resp["endpoint"], testRegion)
	assert.Contains(t, resp["serviceConnectEndpoint"], testRegion)
	assert.Contains(t, resp["telemetryEndpoint"], testRegion)
}

// ----- Submit*StateChange -----

func TestECS_SubmitTaskStateChange(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "statefam",
		"containerDefinitions": []map[string]any{{"name": "web", "image": "nginx", "memory": 128}},
	})

	runRec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": "statefam",
	})
	require.Equal(t, http.StatusOK, runRec.Code)

	var runResp map[string]any
	require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &runResp))
	tasks := runResp["tasks"].([]any)
	require.Len(t, tasks, 1)
	task := tasks[0].(map[string]any)
	taskArn := task["taskArn"].(string)

	rec := doECSRequest(t, h, "SubmitTaskStateChange", map[string]any{
		"cluster": "default",
		"task":    taskArn,
		"status":  "STOPPED",
		"reason":  "essential container exited",
		"containers": []map[string]any{
			{"containerName": "web", "exitCode": 137, "reason": "OOMKilled", "runtimeId": "rt-123"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ACK", resp["acknowledgment"])

	descRec := doECSRequest(t, h, "DescribeTasks", map[string]any{
		"cluster": "default",
		"tasks":   []string{taskArn},
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	descTasks := descResp["tasks"].([]any)
	require.Len(t, descTasks, 1)

	updated := descTasks[0].(map[string]any)
	assert.Equal(t, "STOPPED", updated["lastStatus"])
	assert.Equal(t, "essential container exited", updated["stoppedReason"])

	containers := updated["containers"].([]any)
	require.Len(t, containers, 1)
	container := containers[0].(map[string]any)
	assert.InDelta(t, float64(137), container["exitCode"], 0)
	assert.Equal(t, "OOMKilled", container["reason"])
	assert.Equal(t, "rt-123", container["runtimeId"])
}

func TestECS_SubmitTaskStateChange_UnknownTaskIsNoop(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECSRequest(t, h, "SubmitTaskStateChange", map[string]any{
		"cluster": "default",
		"task":    "does-not-exist",
		"status":  "STOPPED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ACK", resp["acknowledgment"])
}

func TestECS_SubmitContainerStateChange(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "cstatefam",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "app:1", "memory": 128}},
	})

	runRec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": "cstatefam",
	})

	var runResp map[string]any
	require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &runResp))
	taskArn := runResp["tasks"].([]any)[0].(map[string]any)["taskArn"].(string)

	rec := doECSRequest(t, h, "SubmitContainerStateChange", map[string]any{
		"cluster":       "default",
		"task":          taskArn,
		"containerName": "app",
		"status":        "RUNNING",
		"runtimeId":     "rt-999",
		"networkBindings": []map[string]any{
			{"bindIP": "0.0.0.0", "containerPort": 80, "hostPort": 8080, "protocol": "tcp"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doECSRequest(t, h, "DescribeTasks", map[string]any{
		"cluster": "default",
		"tasks":   []string{taskArn},
	})

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	updated := descResp["tasks"].([]any)[0].(map[string]any)
	container := updated["containers"].([]any)[0].(map[string]any)

	assert.Equal(t, "RUNNING", container["lastStatus"])
	assert.Equal(t, "rt-999", container["runtimeId"])
	bindings := container["networkBindings"].([]any)
	require.Len(t, bindings, 1)
	assert.InDelta(t, float64(8080), bindings[0].(map[string]any)["hostPort"], 0)
}

func TestECS_SubmitAttachmentStateChanges(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "attachfam",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "app:1", "memory": 128}},
	})

	runRec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": "attachfam",
		"launchType":     "FARGATE",
	})

	var runResp map[string]any
	require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &runResp))
	task := runResp["tasks"].([]any)[0].(map[string]any)
	taskArn := task["taskArn"].(string)
	attachments, _ := task["attachments"].([]any)
	require.NotEmpty(t, attachments, "FARGATE tasks are created with an ENI attachment")
	attachmentID := attachments[0].(map[string]any)["id"].(string)

	rec := doECSRequest(t, h, "SubmitAttachmentStateChanges", map[string]any{
		"cluster": "default",
		"attachments": []map[string]any{
			{"attachmentArn": attachmentID, "status": "DETACHED"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doECSRequest(t, h, "DescribeTasks", map[string]any{
		"cluster": "default",
		"tasks":   []string{taskArn},
	})

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	updated := descResp["tasks"].([]any)[0].(map[string]any)
	updatedAttachments := updated["attachments"].([]any)
	require.NotEmpty(t, updatedAttachments)
	assert.Equal(t, "DETACHED", updatedAttachments[0].(map[string]any)["status"])
}
