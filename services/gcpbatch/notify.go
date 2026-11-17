package gcpbatch

import (
	"encoding/json/v2"
	"regexp"

	"cloud.google.com/go/batch/apiv1/batchpb"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	maxNotifications = 10
	maxPendingNotes  = 1000
	attrType         = "Type"
	attrJobUID       = "JobUID"
	attrJobName      = "JobName"
	attrRegion       = "Region"
	attrNewJobState  = "NewJobState"
	attrNewTaskState = "NewTaskState"
	attrTaskName     = "TaskName"
	attrTaskUID      = "TaskUID"
)

var topicRe = regexp.MustCompile(`^projects/[^/]+/topics/[^/]+$`)

// Notifier publishes a job notification to a Pub/Sub topic ("projects/p/topics/t").
type Notifier interface {
	PublishNotification(topic string, data []byte, attrs map[string]string) error
}

type note struct {
	attrs map[string]string
	topic string
	data  []byte
}

func validateNotifications(ns []*batchpb.JobNotification) error {
	if len(ns) > maxNotifications {
		return errInvalid("job.notifications can hold at most %d entries.", maxNotifications)
	}

	for i, n := range ns {
		if !topicRe.MatchString(n.GetPubsubTopic()) {
			return errInvalid("job.notifications[%d].pubsub_topic must be projects/{project}/topics/{topic}.", i)
		}

		m := n.GetMessage()

		switch m.GetType() {
		case batchpb.JobNotification_JOB_STATE_CHANGED:
			if m.GetNewTaskState() != batchpb.TaskStatus_STATE_UNSPECIFIED {
				return errInvalid("job.notifications[%d].message sets a task state on a job notification.", i)
			}
		case batchpb.JobNotification_TASK_STATE_CHANGED:
			if m.GetNewJobState() != batchpb.JobStatus_STATE_UNSPECIFIED {
				return errInvalid("job.notifications[%d].message sets a job state on a task notification.", i)
			}
		case batchpb.JobNotification_TYPE_UNSPECIFIED:
			return errInvalid("job.notifications[%d].message.type is required.", i)
		}
	}

	return nil
}

func (r *jobRec) queueNote(typ, newState string, task int) {
	if len(r.pending) >= maxPendingNotes {
		return
	}

	_, region, _ := parseJobName(r.pb.GetName())
	attrs := map[string]string{
		attrType: typ, attrJobUID: r.pb.GetUid(), attrRegion: region,
	}

	if task < 0 {
		attrs[attrJobName], attrs[attrNewJobState] = r.pb.GetName(), newState
	} else {
		attrs[attrNewTaskState] = newState
		attrs[attrTaskName] = taskName(r.pb.GetName(), task)
		attrs[attrTaskUID] = r.pb.GetUid() + "-" + groupName + "-" + itoa(task)
	}

	data, _ := json.Marshal(attrs)

	for _, n := range r.pb.GetNotifications() {
		if n.GetMessage().GetType().String() == typ && stateWanted(n.GetMessage(), task, newState) {
			r.pending = append(r.pending, note{topic: n.GetPubsubTopic(), attrs: attrs, data: data})
		}
	}
}

func stateWanted(m *batchpb.JobNotification_Message, task int, state string) bool {
	if task < 0 {
		return m.GetNewJobState() == batchpb.JobStatus_STATE_UNSPECIFIED || m.GetNewJobState().String() == state
	}

	return m.GetNewTaskState() == batchpb.TaskStatus_STATE_UNSPECIFIED || m.GetNewTaskState().String() == state
}

func (r *jobRec) noteJobState(to int32) {
	if len(r.pb.GetNotifications()) > 0 {
		r.queueNote(batchpb.JobNotification_JOB_STATE_CHANGED.String(), stateName(to), -1)
	}
}

func (r *jobRec) noteTaskState(i int, to int32) {
	if len(r.pb.GetNotifications()) > 0 {
		r.queueNote(batchpb.JobNotification_TASK_STATE_CHANGED.String(), taskStateName(to), i)
	}
}

// SetNotifier connects the Pub/Sub emulator that job notifications are published to.
func (b *InMemoryBackend) SetNotifier(n Notifier) {
	b.mu.Lock("SetNotifier")
	defer b.mu.Unlock()

	b.notifier = n
}

// flushNotes publishes the notifications queued by state changes; it never holds the backend lock while publishing.
func (b *InMemoryBackend) flushNotes() {
	b.mu.Lock("flushNotes")

	var out []note

	for _, r := range b.jobs {
		out = append(out, r.pending...)
		r.pending = nil
	}

	n := b.notifier
	b.mu.Unlock()

	if n == nil {
		return
	}

	for _, m := range out {
		if err := n.PublishNotification(m.topic, m.data, m.attrs); err != nil {
			logger.Load(b.ctx).Warn("gcpbatch: job notification not delivered", "topic", m.topic, "error", err)
		}
	}
}
