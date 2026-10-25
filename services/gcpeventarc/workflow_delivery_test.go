package gcpeventarc_test

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/workflows/apiv1/workflowspb"
	"cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const eventWorkflow = `
main:
  params: [event]
  steps:
    - r:
        return:
          specversion: ${event.specversion}
          type: ${event.type}
          source: ${event.source}
          id: ${event.id}
          contenttype: ${event.datacontenttype}
          data: ${event.data}
`

func workflowDestination(name string) *eventarcpb.Destination {
	return &eventarcpb.Destination{Descriptor_: &eventarcpb.Destination_Workflow{Workflow: name}}
}

func (e *env) deployWorkflow(t *testing.T, id string) string {
	t.Helper()

	name := parent + "/workflows/" + id

	_, err := e.wf.CreateWorkflow(&workflowspb.CreateWorkflowRequest{
		Parent:     parent,
		WorkflowId: id,
		Workflow: &workflowspb.Workflow{
			SourceCode: &workflowspb.Workflow_SourceContents{SourceContents: eventWorkflow},
		},
	})
	require.NoError(t, err)

	return name
}

// finishedExecutions waits for n executions of the workflow to finish and returns them.
func (e *env) finishedExecutions(t *testing.T, workflow string, n int) []*executionspb.Execution {
	t.Helper()

	var done []*executionspb.Execution

	require.Eventually(t, func() bool {
		resp, err := e.wf.ListExecutions(&executionspb.ListExecutionsRequest{
			Parent: workflow, View: executionspb.ExecutionView_FULL,
		})
		if err != nil {
			return false
		}

		done = done[:0]

		for _, ex := range resp.GetExecutions() {
			if ex.GetState() != executionspb.Execution_ACTIVE {
				done = append(done, ex)
			}
		}

		return len(done) >= n
	}, waitFor, tick)

	return done
}

func TestPubsubEventStartsWorkflowExecution(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	wfName := e.deployWorkflow(t, "on-event")

	created := e.createTrigger(t, "to-workflow", pubsubTrigger("", workflowDestination(wfName)))
	topic := created.GetTransport().GetPubsub().GetTopic()

	e.publish(t, topic, &pubsub.Message{Data: []byte("hello workflow")})

	execs := e.finishedExecutions(t, wfName, 1)
	require.Len(t, execs, 1)
	require.Equal(t, executionspb.Execution_SUCCEEDED, execs[0].GetState(), execs[0].GetError().GetPayload())

	var arg map[string]any
	require.NoError(t, json.Unmarshal([]byte(execs[0].GetArgument()), &arg))
	assert.Equal(t, "1.0", arg["specversion"])
	assert.Equal(t, "google.cloud.pubsub.topic.v1.messagePublished", arg["type"])

	var res struct {
		Specversion string `json:"specversion"`
		Type        string `json:"type"`
		Source      string `json:"source"`
		ID          string `json:"id"`
		Contenttype string `json:"contenttype"`
		Data        struct {
			Message struct {
				Data string `json:"data"`
			} `json:"message"`
			Subscription string `json:"subscription"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal([]byte(execs[0].GetResult()), &res))

	assert.Equal(t, "1.0", res.Specversion)
	assert.Equal(t, "//pubsub.googleapis.com/"+topic, res.Source)
	assert.Equal(t, "application/json", res.Contenttype)
	assert.NotEmpty(t, res.ID)
	assert.Equal(t, created.GetTransport().GetPubsub().GetSubscription(), res.Data.Subscription)

	raw, err := base64.StdEncoding.DecodeString(res.Data.Message.Data)
	require.NoError(t, err)
	assert.Equal(t, "hello workflow", string(raw))
}

func TestStorageEventStartsWorkflowExecution(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	wfName := e.deployWorkflow(t, "on-object")

	e.createTrigger(t, "obj-to-workflow", storageTrigger("finalized", "events-bkt", workflowDestination(wfName)))

	require.NoError(t, e.gcs.Bucket("events-bkt").Create(t.Context(), project, nil))

	w := e.gcs.Bucket("events-bkt").Object("dir/a.txt").NewWriter(t.Context())
	_, err := w.Write([]byte("data"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	execs := e.finishedExecutions(t, wfName, 1)

	var res struct {
		Type string `json:"type"`
		Data struct {
			Bucket string `json:"bucket"`
			Name   string `json:"name"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal([]byte(execs[0].GetResult()), &res))
	assert.Equal(t, "google.cloud.storage.object.v1.finalized", res.Type)
	assert.Equal(t, "events-bkt", res.Data.Bucket)
	assert.Equal(t, "dir/a.txt", res.Data.Name)
}

func TestWorkflowDestinationUpdate(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	wfName := e.deployWorkflow(t, "first-wf")
	created := e.createTrigger(t, "retarget", pubsubTrigger("", runDestination("receiver", "/x")))

	update := func(dest string) error {
		op, err := e.client.UpdateTrigger(t.Context(), &eventarcpb.UpdateTriggerRequest{
			Trigger:    &eventarcpb.Trigger{Name: created.GetName(), Destination: workflowDestination(dest)},
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"destination"}},
		})
		if err != nil {
			return err
		}

		_, err = op.Wait(t.Context())

		return err
	}

	assert.Equal(t, codes.FailedPrecondition, code(update(parent+"/workflows/ghost")))
	require.NoError(t, update(wfName))

	got, err := e.client.GetTrigger(t.Context(), &eventarcpb.GetTriggerRequest{Name: created.GetName()})
	require.NoError(t, err)
	assert.Equal(t, wfName, got.GetDestination().GetWorkflow())

	e.publish(t, got.GetTransport().GetPubsub().GetTopic(), &pubsub.Message{Data: []byte("x")})
	assert.Len(t, e.finishedExecutions(t, wfName, 1), 1)
}
