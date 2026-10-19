package gcpcloudtasks

import (
	"net/url"
	"slices"
	"strconv"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxScheduleAhead = 30 * 24 * time.Hour
	minDeadline      = 15 * time.Second
	maxDeadline      = 30 * time.Minute
	maxBodyBytes     = 1 << 20
	autoTaskBase     = 10_000_000_000_000_000
)

func cloneTask(t *cloudtaskspb.Task) *cloudtaskspb.Task {
	out, _ := proto.Clone(t).(*cloudtaskspb.Task)

	return out
}

// viewOf returns a copy of t for the requested view; BASIC (the default) omits request bodies.
func viewOf(t *cloudtaskspb.Task, v cloudtaskspb.Task_View) *cloudtaskspb.Task {
	out := cloneTask(t)
	if v == cloudtaskspb.Task_FULL {
		out.View = cloudtaskspb.Task_FULL

		return out
	}

	out.View = cloudtaskspb.Task_BASIC
	if r := out.GetHttpRequest(); r != nil {
		r.Body = nil
	}

	if r := out.GetAppEngineHttpRequest(); r != nil {
		r.Body = nil
	}

	return out
}

func errTaskMissing() error { return errNotFound("Task does not exist.") }

func validateTask(t *cloudtaskspb.Task, now time.Time) error {
	switch m := t.GetMessageType().(type) {
	case *cloudtaskspb.Task_HttpRequest:
		if err := validateHTTPRequest(m.HttpRequest); err != nil {
			return err
		}
	case *cloudtaskspb.Task_AppEngineHttpRequest:
		if err := validateAppEngineRequest(m.AppEngineHttpRequest); err != nil {
			return err
		}
	default:
		return errInvalid("Task must have either http_request or app_engine_http_request set.")
	}

	if d := t.GetDispatchDeadline(); d != nil && (d.AsDuration() < minDeadline || d.AsDuration() > maxDeadline) {
		return errInvalid("dispatch_deadline must be between 15s and 30m.")
	}

	if s := t.GetScheduleTime(); s != nil && s.AsTime().After(now.Add(maxScheduleAhead)) {
		return errInvalid("schedule_time cannot be more than 30 days in the future.")
	}

	if t.GetRetryConfig() != nil {
		if _, err := defaultRetry(t.GetRetryConfig()); err != nil {
			return err
		}
	}

	return nil
}

func bodyAllowed(m cloudtaskspb.HttpMethod) bool {
	return m != cloudtaskspb.HttpMethod_GET && m != cloudtaskspb.HttpMethod_HEAD && m != cloudtaskspb.HttpMethod_OPTIONS
}

func validateHTTPRequest(r *cloudtaskspb.HttpRequest) error {
	u, err := url.Parse(r.GetUrl())
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errInvalid("HttpRequest url %q must be an absolute http or https URL.", r.GetUrl())
	}

	if len(r.GetBody()) > maxBodyBytes {
		return errInvalid("Task body exceeds the 1 MiB limit.")
	}

	if len(r.GetBody()) > 0 && !bodyAllowed(r.GetHttpMethod()) {
		return errInvalid("HTTP method %s cannot have a body.", r.GetHttpMethod())
	}

	if o := r.GetOauthToken(); o != nil && o.GetServiceAccountEmail() == "" {
		return errInvalid("oauth_token.service_account_email is required.")
	}

	if o := r.GetOidcToken(); o != nil && o.GetServiceAccountEmail() == "" {
		return errInvalid("oidc_token.service_account_email is required.")
	}

	return nil
}

func validateAppEngineRequest(r *cloudtaskspb.AppEngineHttpRequest) error {
	if u := r.GetRelativeUri(); u != "" && u[0] != '/' {
		return errInvalid("relative_uri must start with '/'.")
	}

	if len(r.GetBody()) > maxBodyBytes {
		return errInvalid("Task body exceeds the 1 MiB limit.")
	}

	if len(r.GetBody()) > 0 && !bodyAllowed(r.GetHttpMethod()) {
		return errInvalid("HTTP method %s cannot have a body.", r.GetHttpMethod())
	}

	return nil
}

func (b *InMemoryBackend) newTaskLocked(q *queueRec, in *cloudtaskspb.Task, now time.Time) (*taskRec, error) {
	if err := validateTask(in, now); err != nil {
		return nil, err
	}

	name := in.GetName()
	if name == "" {
		b.taskSeq++
		name = taskNameOf(q.pb.GetName(), strconv.FormatUint(autoTaskBase+b.taskSeq, 10))
	} else {
		parent, err := parseTaskName(name)
		if err != nil {
			return nil, err
		}

		if parent != q.pb.GetName() {
			return nil, errInvalid("Task name %q must be under queue %q.", name, q.pb.GetName())
		}
	}

	if _, ok := q.tasks[name]; ok {
		return nil, errExists("Requested entity already exists")
	}

	if exp, ok := b.tombstones[name]; ok && exp.After(now) {
		return nil, errExists("The task cannot be created because a task with this name existed recently.")
	}

	t := cloneTask(in)
	t.Name, t.CreateTime, t.View = name, tsOf(now), cloudtaskspb.Task_VIEW_UNSPECIFIED
	t.DispatchCount, t.ResponseCount, t.FirstAttempt, t.LastAttempt = 0, 0, nil, nil

	if t.GetScheduleTime() == nil || t.GetScheduleTime().AsTime().Before(now) {
		t.ScheduleTime = tsOf(now)
	}

	rec := &taskRec{pb: t}
	q.tasks[name] = rec

	return rec, nil
}

// CreateTask adds a task to a queue.
func (b *InMemoryBackend) CreateTask(req *cloudtaskspb.CreateTaskRequest) (*cloudtaskspb.Task, error) {
	if _, err := parseQueueName(req.GetParent()); err != nil {
		return nil, err
	}

	if req.GetTask() == nil {
		return nil, errInvalid("task must be specified.")
	}

	b.mu.Lock("CreateTask")
	defer b.mu.Unlock()

	q, ok := b.queues[req.GetParent()]
	if !ok {
		return nil, errQueueMissing()
	}

	t, err := b.newTaskLocked(q, req.GetTask(), b.now())
	if err != nil {
		return nil, err
	}

	return viewOf(t.pb, req.GetResponseView()), nil
}

// GetTask returns a task.
func (b *InMemoryBackend) GetTask(req *cloudtaskspb.GetTaskRequest) (*cloudtaskspb.Task, error) {
	t, err := b.lookup(req.GetName(), "GetTask", func(t *taskRec) *cloudtaskspb.Task {
		return viewOf(t.pb, req.GetResponseView())
	})

	return t, err
}

func (b *InMemoryBackend) lookup(
	name, op string, fn func(*taskRec) *cloudtaskspb.Task,
) (*cloudtaskspb.Task, error) {
	queue, err := parseTaskName(name)
	if err != nil {
		return nil, err
	}

	b.mu.RLock(op)
	defer b.mu.RUnlock()

	q, ok := b.queues[queue]
	if !ok {
		return nil, errQueueMissing()
	}

	t, ok := q.tasks[name]
	if !ok {
		return nil, errTaskMissing()
	}

	return fn(t), nil
}

// ListTasks lists a queue's tasks ordered by schedule time, then name.
func (b *InMemoryBackend) ListTasks(req *cloudtaskspb.ListTasksRequest) ([]*cloudtaskspb.Task, string, error) {
	if _, err := parseQueueName(req.GetParent()); err != nil {
		return nil, "", err
	}

	size, err := pageSizeOf(req.GetPageSize())
	if err != nil {
		return nil, "", err
	}

	b.mu.RLock("ListTasks")
	defer b.mu.RUnlock()

	q, ok := b.queues[req.GetParent()]
	if !ok {
		return nil, "", errQueueMissing()
	}

	recs := sortedTasks(q)
	all := make([]*cloudtaskspb.Task, 0, len(recs))

	for _, t := range recs {
		all = append(all, viewOf(t.pb, req.GetResponseView()))
	}

	pg := page.New(all, req.GetPageToken(), size, defaultPageSize)

	return pg.Data, pg.Next, nil
}

func sortedTasks(q *queueRec) []*taskRec {
	out := make([]*taskRec, 0, len(q.tasks))
	for _, t := range q.tasks {
		out = append(out, t)
	}

	slices.SortFunc(out, func(a, b *taskRec) int {
		if c := a.pb.GetScheduleTime().AsTime().Compare(b.pb.GetScheduleTime().AsTime()); c != 0 {
			return c
		}

		switch {
		case a.pb.GetName() < b.pb.GetName():
			return -1
		case a.pb.GetName() > b.pb.GetName():
			return 1
		}

		return 0
	})

	return out
}

// DeleteTask removes a task; its name stays reserved for an hour.
func (b *InMemoryBackend) DeleteTask(name string) error {
	queue, err := parseTaskName(name)
	if err != nil {
		return err
	}

	b.mu.Lock("DeleteTask")
	defer b.mu.Unlock()

	q, ok := b.queues[queue]
	if !ok {
		return errQueueMissing()
	}

	return b.deleteTaskLocked(q, name)
}
