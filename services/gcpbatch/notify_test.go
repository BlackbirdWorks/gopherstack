package gcpbatch_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/batch/apiv1/batchpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/services/gcpbatch"
)

var errNoTopic = errors.New("no such topic")

const notifyTopic = "projects/" + project + "/topics/jobs"

type sentNote struct {
	attrs map[string]string
	topic string
}

type fakeNotifier struct {
	err  error
	sent []sentNote
	mu   sync.Mutex
}

func (f *fakeNotifier) PublishNotification(topic string, _ []byte, attrs map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.sent = append(f.sent, sentNote{topic: topic, attrs: attrs})

	return f.err
}

func (f *fakeNotifier) states(typ, key string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []string

	for _, s := range f.sent {
		if s.attrs["Type"] == typ {
			out = append(out, s.attrs[key])
		}
	}

	return out
}

func notifyOn(msgs ...*batchpb.JobNotification_Message) []*batchpb.JobNotification {
	out := make([]*batchpb.JobNotification, len(msgs))
	for i, m := range msgs {
		out[i] = &batchpb.JobNotification{PubsubTopic: notifyTopic, Message: m}
	}

	return out
}

func TestJobNotificationsPublished(t *testing.T) {
	t.Parallel()

	jobMsg := &batchpb.JobNotification_Message{Type: batchpb.JobNotification_JOB_STATE_CHANGED}
	jobOnly := &batchpb.JobNotification_Message{
		Type: batchpb.JobNotification_JOB_STATE_CHANGED, NewJobState: batchpb.JobStatus_SUCCEEDED,
	}
	taskMsg := &batchpb.JobNotification_Message{Type: batchpb.JobNotification_TASK_STATE_CHANGED}

	tests := []struct {
		name      string
		msgs      []*batchpb.JobNotification_Message
		wantJob   []string
		wantTasks []string
	}{
		{
			name: "every job state", msgs: []*batchpb.JobNotification_Message{jobMsg},
			wantJob: []string{"SCHEDULED", "RUNNING", "SUCCEEDED"},
		},
		{name: "one job state", msgs: []*batchpb.JobNotification_Message{jobOnly}, wantJob: []string{"SUCCEEDED"}},
		{
			name: "every task state", msgs: []*batchpb.JobNotification_Message{taskMsg},
			wantTasks: []string{"ASSIGNED", "RUNNING", "SUCCEEDED"},
		},
		{name: "no notification", msgs: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			n := &fakeNotifier{}
			e := newEnv(t, withTiming(gcpbatch.Timing{
				Schedule: time.Second, Start: time.Second, TaskRun: time.Second,
			}), func(b *gcpbatch.InMemoryBackend) { b.SetNotifier(n) })

			job := basicJob(1)
			job.Notifications = notifyOn(tt.msgs...)
			e.create(t, "n1", job)
			e.clock.Advance(10 * time.Second)
			e.get(t, "n1")

			assert.Equal(t, tt.wantJob, n.states("JOB_STATE_CHANGED", "NewJobState"))
			assert.Equal(t, tt.wantTasks, n.states("TASK_STATE_CHANGED", "NewTaskState"))

			for _, s := range n.sent {
				assert.Equal(t, notifyTopic, s.topic)
				if s.attrs["Type"] == "JOB_STATE_CHANGED" {
					assert.Equal(t, parent+"/jobs/n1", s.attrs["JobName"])
				}
				assert.Equal(t, region, s.attrs["Region"])
				assert.NotEmpty(t, s.attrs["JobUID"])
			}
		})
	}
}

func TestJobNotificationFailureDoesNotBreakJob(t *testing.T) {
	t.Parallel()

	n := &fakeNotifier{err: errNoTopic}
	e := newEnv(t, withTiming(gcpbatch.Timing{Schedule: time.Second}), func(b *gcpbatch.InMemoryBackend) {
		b.SetNotifier(n)
	})

	job := basicJob(1)
	job.Notifications = notifyOn(&batchpb.JobNotification_Message{Type: batchpb.JobNotification_JOB_STATE_CHANGED})
	e.create(t, "n2", job)
	e.clock.Advance(time.Hour)

	assert.Equal(t, batchpb.JobStatus_SUCCEEDED, e.get(t, "n2").GetStatus().GetState())
	assert.NotEmpty(t, n.sent)
}

func TestJobNotificationValidation(t *testing.T) {
	t.Parallel()

	good := &batchpb.JobNotification_Message{Type: batchpb.JobNotification_JOB_STATE_CHANGED}

	tests := []struct {
		name string
		ns   []*batchpb.JobNotification
		want codes.Code
	}{
		{name: "valid", ns: notifyOn(good), want: codes.OK},
		{
			name: "bad topic",
			ns:   []*batchpb.JobNotification{{PubsubTopic: "jobs", Message: good}},
			want: codes.InvalidArgument,
		},
		{name: "no type", ns: notifyOn(&batchpb.JobNotification_Message{}), want: codes.InvalidArgument},
		{
			name: "task state on job type", want: codes.InvalidArgument,
			ns: notifyOn(&batchpb.JobNotification_Message{
				Type: batchpb.JobNotification_JOB_STATE_CHANGED, NewTaskState: batchpb.TaskStatus_FAILED,
			}),
		},
		{
			name: "job state on task type", want: codes.InvalidArgument,
			ns: notifyOn(&batchpb.JobNotification_Message{
				Type: batchpb.JobNotification_TASK_STATE_CHANGED, NewJobState: batchpb.JobStatus_FAILED,
			}),
		},
		{name: "too many", ns: notifyOn(make11(good)...), want: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			job := basicJob(1)
			job.Notifications = tt.ns

			_, err := e.client.CreateJob(t.Context(), &batchpb.CreateJobRequest{Parent: parent, JobId: "v1", Job: job})
			if tt.want == codes.OK {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Equal(t, tt.want, status.Code(err))
		})
	}
}

func make11(m *batchpb.JobNotification_Message) []*batchpb.JobNotification_Message {
	out := make([]*batchpb.JobNotification_Message, 11)
	for i := range out {
		out[i] = m
	}

	return out
}

func TestNotificationsConcurrentReadsPublishOnce(t *testing.T) {
	t.Parallel()

	n := &fakeNotifier{}
	e := newEnv(t, withTiming(gcpbatch.Timing{
		Schedule: time.Second, Start: time.Second, TaskRun: time.Second,
	}), func(b *gcpbatch.InMemoryBackend) { b.SetNotifier(n) })

	job := basicJob(3)
	job.Notifications = notifyOn(
		&batchpb.JobNotification_Message{Type: batchpb.JobNotification_JOB_STATE_CHANGED},
		&batchpb.JobNotification_Message{Type: batchpb.JobNotification_TASK_STATE_CHANGED},
	)
	e.create(t, "cc", job)
	e.clock.Advance(time.Minute)

	var wg sync.WaitGroup

	errs := make(chan error, 16)

	for range 8 {
		wg.Go(func() {
			_, err := e.client.GetJob(t.Context(), &batchpb.GetJobRequest{Name: parent + "/jobs/cc"})
			errs <- err

			_, err = e.client.ListJobs(t.Context(), &batchpb.ListJobsRequest{Parent: parent}).Next()
			errs <- err
		})
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	assert.ElementsMatch(t, []string{"SCHEDULED", "RUNNING", "SUCCEEDED"}, n.states("JOB_STATE_CHANGED", "NewJobState"))
	assert.ElementsMatch(
		t,
		[]string{
			"ASSIGNED",
			"ASSIGNED",
			"ASSIGNED",
			"RUNNING",
			"RUNNING",
			"RUNNING",
			"SUCCEEDED",
			"SUCCEEDED",
			"SUCCEEDED",
		},
		n.states("TASK_STATE_CHANGED", "NewTaskState"),
	)
}

type reentrantNotifier struct {
	reenter func()
	mu      sync.Mutex
	calls   int
}

func (r *reentrantNotifier) PublishNotification(string, []byte, map[string]string) error {
	r.mu.Lock()
	r.calls++
	first := r.calls == 1
	r.mu.Unlock()

	if first {
		r.reenter()
	}

	return nil
}

func TestNotificationPublishedOutsideBackendLock(t *testing.T) {
	t.Parallel()

	n := &reentrantNotifier{}
	e := newEnv(t, withTiming(gcpbatch.Timing{Schedule: time.Second}), func(b *gcpbatch.InMemoryBackend) {
		b.SetNotifier(n)
	})

	n.reenter = func() { e.get(t, "lk") }

	job := basicJob(1)
	job.Notifications = notifyOn(&batchpb.JobNotification_Message{Type: batchpb.JobNotification_JOB_STATE_CHANGED})
	e.create(t, "lk", job)
	e.clock.Advance(time.Hour)

	e.get(t, "lk")

	assert.Positive(t, n.calls)
}
