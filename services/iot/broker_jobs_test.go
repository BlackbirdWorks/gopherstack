package iot_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

const (
	jobsThing    = "dev1"
	jobsTopic    = "$aws/things/dev1/jobs/"
	jobsDeadline = 5 * time.Second
	jobsDocument = `{"operation":"reboot"}`
)

type jobsMQTTEnv struct {
	t       *testing.T
	backend *iot.InMemoryBackend
	client  pahomqtt.Client
	msgs    chan pahomqtt.Message
}

func newJobsMQTTEnv(t *testing.T) *jobsMQTTEnv {
	t.Helper()

	backend := iot.NewInMemoryBackend()
	broker := iot.NewBroker(backend, 0)
	iot.NewHandler(backend, broker)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})

	go func() {
		_ = broker.Start(ctx)
		close(done)
	}()

	t.Cleanup(func() {
		cancel()
		<-done
		backend.DrainBackground(context.Background())
	})

	select {
	case <-broker.Ready():
	case <-time.After(jobsDeadline):
		require.FailNow(t, "broker not ready")
	}

	env := &jobsMQTTEnv{t: t, backend: backend, msgs: make(chan pahomqtt.Message, 64)}
	opts := pahomqtt.NewClientOptions().
		AddBroker(fmt.Sprintf("tcp://127.0.0.1:%d", broker.Port())).
		SetClientID("jobs-device").
		SetConnectTimeout(jobsDeadline).
		SetDefaultPublishHandler(func(_ pahomqtt.Client, m pahomqtt.Message) { env.msgs <- m })

	env.client = pahomqtt.NewClient(opts)
	tok := env.client.Connect()
	require.True(t, tok.WaitTimeout(jobsDeadline))
	require.NoError(t, tok.Error())
	t.Cleanup(func() { env.client.Disconnect(100) })

	sub := env.client.Subscribe(jobsTopic+"#", 1, nil)
	require.True(t, sub.WaitTimeout(jobsDeadline))
	require.NoError(t, sub.Error())

	_, err := backend.CreateThing(&iot.CreateThingInput{ThingName: jobsThing})
	require.NoError(t, err)

	return env
}

func (e *jobsMQTTEnv) createJob(t *testing.T) {
	t.Helper()

	const id = "job-1"

	_, err := e.backend.CreateJob(&iot.CreateJobInput{
		JobID: id, Document: jobsDocument,
		Targets: []string{"arn:aws:iot:us-east-1:000000000000:thing/" + jobsThing},
	})
	require.NoError(t, err)
}

func (e *jobsMQTTEnv) publish(t *testing.T, suffix, payload string) {
	t.Helper()

	tok := e.client.Publish(jobsTopic+suffix, 1, false, payload)
	require.True(t, tok.WaitTimeout(jobsDeadline))
	require.NoError(t, tok.Error())
}

func (e *jobsMQTTEnv) expect(t *testing.T, suffix string) map[string]any {
	t.Helper()

	timer := time.NewTimer(jobsDeadline)
	defer timer.Stop()

	for {
		select {
		case m := <-e.msgs:
			if m.Topic() != jobsTopic+suffix {
				continue
			}

			var out map[string]any
			require.NoError(t, json.Unmarshal(m.Payload(), &out))

			return out
		case <-timer.C:
			require.FailNow(t, "no message on "+jobsTopic+suffix)
		}
	}
}

func TestBrokerJobsTopics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(e *jobsMQTTEnv)
		name string
	}{
		{name: "notify_on_job_creation", run: func(e *jobsMQTTEnv) {
			e.createJob(e.t)

			notify := e.expect(e.t, "notify")
			jobs, ok := notify["jobs"].(map[string]any)
			require.True(e.t, ok)
			queued, ok := jobs["QUEUED"].([]any)
			require.True(e.t, ok)
			assert.Equal(e.t, "job-1", queued[0].(map[string]any)["jobId"])
			assert.NotZero(e.t, notify["timestamp"])

			next := e.expect(e.t, "notify-next")
			exec, ok := next["execution"].(map[string]any)
			require.True(e.t, ok)
			assert.Equal(e.t, "job-1", exec["jobId"])
			assert.Equal(e.t, "QUEUED", exec["status"])
			assert.Equal(e.t, map[string]any{"operation": "reboot"}, exec["jobDocument"])
		}},
		{name: "get_lists_pending", run: func(e *jobsMQTTEnv) {
			e.createJob(e.t)
			e.publish(e.t, "get", `{"clientToken":"tok-1"}`)

			out := e.expect(e.t, "get/accepted")
			assert.Equal(e.t, "tok-1", out["clientToken"])
			queued, ok := out["queuedJobs"].([]any)
			require.True(e.t, ok)
			assert.Equal(e.t, "job-1", queued[0].(map[string]any)["jobId"])
		}},
		{name: "start_next_then_update", run: func(e *jobsMQTTEnv) {
			e.createJob(e.t)
			e.publish(e.t, "start-next", `{"statusDetails":{"step":"1"}}`)

			started := e.expect(e.t, "start-next/accepted")
			exec, ok := started["execution"].(map[string]any)
			require.True(e.t, ok)
			assert.Equal(e.t, "IN_PROGRESS", exec["status"])
			assert.EqualValues(e.t, 2, exec["versionNumber"])

			e.publish(e.t, "job-1/update", `{"status":"SUCCEEDED","expectedVersion":2,"includeJobExecutionState":true}`)

			updated := e.expect(e.t, "job-1/update/accepted")
			state, ok := updated["executionState"].(map[string]any)
			require.True(e.t, ok)
			assert.Equal(e.t, "SUCCEEDED", state["status"])
			assert.EqualValues(e.t, 3, state["versionNumber"])
		}},
		{name: "get_next_job", run: func(e *jobsMQTTEnv) {
			e.createJob(e.t)
			e.publish(e.t, "$next/get", `{}`)

			out := e.expect(e.t, "$next/get/accepted")
			exec, ok := out["execution"].(map[string]any)
			require.True(e.t, ok)
			assert.Equal(e.t, "job-1", exec["jobId"])
		}},
		{name: "update_version_mismatch", run: func(e *jobsMQTTEnv) {
			e.createJob(e.t)
			e.publish(e.t, "job-1/update", `{"status":"IN_PROGRESS","expectedVersion":99,"clientToken":"t"}`)

			out := e.expect(e.t, "job-1/update/rejected")
			assert.Equal(e.t, "VersionMismatch", out["code"])
			assert.Equal(e.t, "t", out["clientToken"])
			assert.Contains(e.t, out, "executionState")
		}},
		{name: "unknown_job_rejected", run: func(e *jobsMQTTEnv) {
			e.publish(e.t, "nope/get", `{}`)

			assert.Equal(e.t, "ResourceNotFound", e.expect(e.t, "nope/get/rejected")["code"])
		}},
		{name: "invalid_json_rejected", run: func(e *jobsMQTTEnv) {
			e.publish(e.t, "get", `{not json`)

			assert.Equal(e.t, "InvalidJson", e.expect(e.t, "get/rejected")["code"])
		}},
		{name: "notify_on_cancel", run: func(e *jobsMQTTEnv) {
			e.createJob(e.t)
			e.expect(e.t, "notify-next")

			_, err := e.backend.CancelJob("job-1", "", "", false)
			require.NoError(e.t, err)

			next := e.expect(e.t, "notify-next")
			assert.NotContains(e.t, next, "execution")
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(newJobsMQTTEnv(t))
		})
	}
}
