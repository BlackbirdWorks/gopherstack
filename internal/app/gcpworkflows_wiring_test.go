package app

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/workflows/apiv1/workflowspb"
	"cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpeventarcbackend "github.com/blackbirdworks/gopherstack/services/gcpeventarc"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
	gcpsecretmanagerbackend "github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
	gcpworkflowsbackend "github.com/blackbirdworks/gopherstack/services/gcpworkflows"
)

const wfParent = "projects/p/locations/us-central1"

func deployAndRun(t *testing.T, wf *gcpworkflowsbackend.Handler, id, source string) *executionspb.Execution {
	t.Helper()

	op, err := wf.Backend.CreateWorkflow(&workflowspb.CreateWorkflowRequest{
		Parent: wfParent, WorkflowId: id,
		Workflow: &workflowspb.Workflow{SourceCode: &workflowspb.Workflow_SourceContents{SourceContents: source}},
	})
	require.NoError(t, err)
	require.True(t, op.GetDone())
	require.Nil(t, op.GetError(), "%v", op.GetError())

	ex, err := wf.Backend.CreateExecution(&executionspb.CreateExecutionRequest{Parent: wfParent + "/workflows/" + id})
	require.NoError(t, err)

	return awaitExecution(t, wf, ex.GetName())
}

func awaitExecution(t *testing.T, wf *gcpworkflowsbackend.Handler, name string) *executionspb.Execution {
	t.Helper()

	var got *executionspb.Execution

	require.Eventually(t, func() bool {
		var err error

		got, err = wf.Backend.GetExecution(&executionspb.GetExecutionRequest{Name: name})

		return err == nil && got.GetState() != executionspb.Execution_ACTIVE
	}, 30*time.Second, 5*time.Millisecond)

	return got
}

func TestWireGCPWorkflowsOptionalPeers(t *testing.T) {
	t.Parallel()

	wf := gcpworkflowsbackend.NewHandler(gcpworkflowsbackend.NewInMemoryBackend())
	t.Cleanup(func() { wf.Shutdown(context.Background()) })

	assert.NotPanics(t, func() { wireGCPWorkflows(map[string]service.Registerable{}) })
	assert.NotPanics(t, func() { wireGCPWorkflows(map[string]service.Registerable{"GCPWorkflows": wf}) })
}

func TestWorkflowEndpoints(t *testing.T) {
	t.Parallel()

	ps := gcppubsubbackend.NewHandler(gcppubsubbackend.NewInMemoryBackend())
	ps.Port = 0
	require.NoError(t, ps.StartWorker(t.Context()))
	t.Cleanup(func() { ps.Shutdown(context.Background()) })

	tests := []struct {
		name    string
		service string
		eps     workflowEndpoints
		want    string
	}{
		{"unwired service", "pubsub", workflowEndpoints{}, ""},
		{"unknown service", "nope", workflowEndpoints{pubsub: ps}, ""},
		{"wired pubsub", "pubsub", workflowEndpoints{pubsub: ps}, loopbackBase(ps.Addr())},
		{"storage port unset", "storage", workflowEndpoints{}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.eps.BaseURL(tt.service))
		})
	}

	assert.Contains(t, loopbackBase(ps.Addr()), "http://127.0.0.1:")
	assert.Empty(t, loopbackBase("nonsense"))
}

func TestWireGCPWorkflowsConnectorsReachPubSub(t *testing.T) {
	t.Parallel()

	wf := gcpworkflowsbackend.NewHandler(gcpworkflowsbackend.NewInMemoryBackend())
	t.Cleanup(func() { wf.Shutdown(context.Background()) })

	ps := gcppubsubbackend.NewHandler(gcppubsubbackend.NewInMemoryBackend())
	ps.Port = 0
	sm := gcpsecretmanagerbackend.NewHandler(gcpsecretmanagerbackend.NewInMemoryBackend())
	sm.Port = 0

	wireGCPWorkflows(map[string]service.Registerable{"GCPWorkflows": wf, "GCPPubSub": ps, "GCPSecretManager": sm})

	require.NoError(t, ps.StartWorker(t.Context()))
	t.Cleanup(func() { ps.Shutdown(context.Background()) })
	require.NoError(t, sm.StartWorker(t.Context()))
	t.Cleanup(func() { sm.Shutdown(context.Background()) })

	got := deployAndRun(t, wf, "wired", `
main:
  steps:
    - mk:
        call: googleapis.pubsub.v1.projects.topics.create
        args: {name: projects/p/topics/wired-topic, body: {}}
    - pub:
        call: googleapis.pubsub.v1.projects.topics.publish
        args:
          topic: projects/p/topics/wired-topic
          body: {messages: [{data: aGk=}]}
        result: res
    - r:
        return: ${len(res.messageIds)}
`)

	require.Equal(t, executionspb.Execution_SUCCEEDED, got.GetState(), got.GetError().GetPayload())
	assert.Equal(t, "1", got.GetResult())

	topic, err := ps.Backend.GetTopic("projects/p/topics/wired-topic")
	require.NoError(t, err)
	assert.NotNil(t, topic)
}

func TestWireGCPEventarcStartsWorkflowExecutions(t *testing.T) {
	t.Parallel()

	ea := gcpeventarcbackend.NewHandler(gcpeventarcbackend.NewInMemoryBackend())
	ea.Port = 0
	ps := gcppubsubbackend.NewHandler(gcppubsubbackend.NewInMemoryBackend())
	ps.Port = 0
	wf := gcpworkflowsbackend.NewHandler(gcpworkflowsbackend.NewInMemoryBackend())
	t.Cleanup(func() { wf.Shutdown(context.Background()) })

	wireGCPEventarc(map[string]service.Registerable{"GCPEventarc": ea, "GCPPubSub": ps, "GCPWorkflows": wf})

	require.NoError(t, ps.StartWorker(t.Context()))
	t.Cleanup(func() { ps.Shutdown(context.Background()) })
	require.NoError(t, ea.StartWorker(t.Context()))
	t.Cleanup(func() { ea.Shutdown(context.Background()) })

	op, err := wf.Backend.CreateWorkflow(&workflowspb.CreateWorkflowRequest{
		Parent: wfParent, WorkflowId: "receiver",
		Workflow: &workflowspb.Workflow{SourceCode: &workflowspb.Workflow_SourceContents{
			SourceContents: "main:\n  params: [event]\n  steps:\n    - r:\n        return: ${event.type}\n",
		}},
	})
	require.NoError(t, err)
	require.Nil(t, op.GetError())

	tr, err := ea.Backend.CreateTrigger(&eventarcpb.CreateTriggerRequest{
		Parent: wfParent, TriggerId: "to-workflow",
		Trigger: &eventarcpb.Trigger{
			EventFilters: []*eventarcpb.EventFilter{
				{Attribute: "type", Value: "google.cloud.pubsub.topic.v1.messagePublished"},
			},
			Destination: &eventarcpb.Destination{Descriptor_: &eventarcpb.Destination_Workflow{
				Workflow: wfParent + "/workflows/receiver",
			}},
		},
	})
	require.NoError(t, err)
	require.True(t, tr.GetDone())

	created, err := ea.Backend.GetTrigger(wfParent + "/triggers/to-workflow")
	require.NoError(t, err)

	_, err = ps.Backend.Publish(&pubsubpb.PublishRequest{
		Topic:    created.GetTransport().GetPubsub().GetTopic(),
		Messages: []*pubsubpb.PubsubMessage{{Data: []byte("wired")}},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		resp, listErr := wf.Backend.ListExecutions(&executionspb.ListExecutionsRequest{
			Parent: wfParent + "/workflows/receiver", View: executionspb.ExecutionView_FULL,
		})

		return listErr == nil && len(resp.GetExecutions()) == 1 && resp.GetExecutions()[0].GetResult() != ""
	}, 30*time.Second, 5*time.Millisecond)
}
