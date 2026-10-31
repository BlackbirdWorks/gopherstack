package gcpbatch

import (
	"strings"

	"cloud.google.com/go/batch/apiv1/batchpb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const filterStatePrefix = "state="

func taskFilter(f string) (int32, bool, error) {
	f = strings.TrimSpace(f)
	if f == "" {
		return 0, false, nil
	}

	v, ok := strings.CutPrefix(strings.ReplaceAll(f, " ", ""), filterStatePrefix)
	if !ok {
		v, ok = strings.CutPrefix(strings.ReplaceAll(f, " ", ""), "State=")
	}

	st, known := batchpb.TaskStatus_State_value[strings.Trim(v, `"`)]
	if !ok || !known {
		return 0, false, errInvalid("Invalid filter %q: use State=TASK_STATE, for example State=RUNNING.", f)
	}

	return st, true, nil
}

func (b *InMemoryBackend) groupLocked(parent string) (*jobRec, error) {
	job, group, err := parseGroupName(parent)
	if err != nil {
		return nil, err
	}

	r, err := b.liveJobLocked(job)
	if err != nil {
		return nil, err
	}

	if group != groupName {
		return nil, errNotFound("TaskGroup %q not found.", parent)
	}

	return r, nil
}

// ListTasks lists the tasks of a task group, optionally filtered by State=....
func (b *InMemoryBackend) ListTasks(in *batchpb.ListTasksRequest) (*batchpb.ListTasksResponse, error) {
	if in.GetPageSize() < 0 {
		return nil, errInvalid("page_size must not be negative.")
	}

	want, filtered, err := taskFilter(in.GetFilter())
	if err != nil {
		return nil, err
	}

	b.mu.Lock("ListTasks")
	b.advanceLocked()

	r, err := b.groupLocked(in.GetParent())
	if err != nil {
		b.mu.Unlock()

		return nil, err
	}

	all := make([]*batchpb.Task, 0, len(r.Tasks))

	for i, t := range r.Tasks {
		if !filtered || t.State == want {
			all = append(all, r.taskView(i))
		}
	}

	b.mu.Unlock()

	pg := page.New(all, in.GetPageToken(), int(in.GetPageSize()), defaultListPage)

	return &batchpb.ListTasksResponse{Tasks: pg.Data, NextPageToken: pg.Next}, nil
}

// GetTask returns one task.
func (b *InMemoryBackend) GetTask(name string) (*batchpb.Task, error) {
	job, group, idx, err := parseTaskName(name)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("GetTask")
	defer b.mu.Unlock()

	b.advanceLocked()

	r, err := b.liveJobLocked(job)
	if err != nil {
		return nil, err
	}

	if group != groupName || idx >= len(r.Tasks) {
		return nil, errNotFound("Task %q not found.", name)
	}

	return r.taskView(idx), nil
}
