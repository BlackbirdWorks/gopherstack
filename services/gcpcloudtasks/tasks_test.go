package gcpcloudtasks_test

import (
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCreateTask(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	q := e.queue(t, "tasks", func(q *cloudtaskspb.Queue) { q.State = cloudtaskspb.Queue_PAUSED }).GetName()

	future := timestamppb.New(e.clk.now().Add(time.Hour))
	far := timestamppb.New(e.clk.now().Add(31 * 24 * time.Hour))

	tests := []struct {
		mutate func(*cloudtaskspb.Task)
		name   string
		code   codes.Code
	}{
		{name: "auto name", mutate: httpTask("https://example.com/h")},
		{name: "explicit name", mutate: func(task *cloudtaskspb.Task) {
			httpTask("https://example.com/h")(task)
			task.Name = q + "/tasks/my-task_1"
		}},
		{name: "scheduled", mutate: func(task *cloudtaskspb.Task) {
			httpTask("https://example.com/h")(task)
			task.ScheduleTime = future
		}},
		{name: "app engine", mutate: func(task *cloudtaskspb.Task) {
			task.MessageType = &cloudtaskspb.Task_AppEngineHttpRequest{
				AppEngineHttpRequest: &cloudtaskspb.AppEngineHttpRequest{RelativeUri: "/work"},
			}
		}},
		{name: "no message", mutate: func(*cloudtaskspb.Task) {}, code: codes.InvalidArgument},
		{name: "bad url", mutate: httpTask("ftp://example.com"), code: codes.InvalidArgument},
		{name: "relative url", mutate: httpTask("/just/a/path"), code: codes.InvalidArgument},
		{name: "get with body", mutate: func(task *cloudtaskspb.Task) {
			task.MessageType = &cloudtaskspb.Task_HttpRequest{HttpRequest: &cloudtaskspb.HttpRequest{
				Url: "https://example.com", HttpMethod: cloudtaskspb.HttpMethod_GET, Body: []byte("x"),
			}}
		}, code: codes.InvalidArgument},
		{name: "oidc without account", mutate: func(task *cloudtaskspb.Task) {
			task.MessageType = &cloudtaskspb.Task_HttpRequest{HttpRequest: &cloudtaskspb.HttpRequest{
				Url:                 "https://example.com",
				AuthorizationHeader: &cloudtaskspb.HttpRequest_OidcToken{OidcToken: &cloudtaskspb.OidcToken{}},
			}}
		}, code: codes.InvalidArgument},
		{name: "deadline too short", mutate: func(task *cloudtaskspb.Task) {
			httpTask("https://example.com")(task)
			task.DispatchDeadline = durationpb.New(time.Second)
		}, code: codes.InvalidArgument},
		{name: "too far ahead", mutate: func(task *cloudtaskspb.Task) {
			httpTask("https://example.com")(task)
			task.ScheduleTime = far
		}, code: codes.InvalidArgument},
		{name: "foreign queue name", mutate: func(task *cloudtaskspb.Task) {
			httpTask("https://example.com")(task)
			task.Name = location + "/queues/other/tasks/x"
		}, code: codes.InvalidArgument},
		{name: "bad task id", mutate: func(task *cloudtaskspb.Task) {
			httpTask("https://example.com")(task)
			task.Name = q + "/tasks/has space"
		}, code: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			task := &cloudtaskspb.Task{}
			tt.mutate(task)

			out, err := e.client.CreateTask(t.Context(), &cloudtaskspb.CreateTaskRequest{Parent: q, Task: task})
			if tt.code != codes.OK {
				assert.Equal(t, tt.code, status.Code(err))

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, out.GetName())
			assert.NotNil(t, out.GetCreateTime())
			assert.Equal(t, cloudtaskspb.Task_BASIC, out.GetView())
		})
	}

	_, err := e.client.CreateTask(t.Context(), &cloudtaskspb.CreateTaskRequest{
		Parent: location + "/queues/none", Task: &cloudtaskspb.Task{},
	})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestTaskDedupAndTombstone(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	q := e.queue(t, "dedup").GetName()
	name := q + "/tasks/same"
	named := func(task *cloudtaskspb.Task) {
		httpTask("https://example.com")(task)
		task.Name = name
	}

	e.task(t, q, named)

	_, err := e.client.CreateTask(t.Context(), &cloudtaskspb.CreateTaskRequest{
		Parent: q, Task: &cloudtaskspb.Task{Name: name, MessageType: &cloudtaskspb.Task_HttpRequest{
			HttpRequest: &cloudtaskspb.HttpRequest{Url: "https://example.com"},
		}},
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err))

	require.NoError(t, e.client.DeleteTask(t.Context(), &cloudtaskspb.DeleteTaskRequest{Name: name}))

	_, err = e.getTask(t, name)
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.client.CreateTask(t.Context(), &cloudtaskspb.CreateTaskRequest{
		Parent: q, Task: &cloudtaskspb.Task{Name: name, MessageType: &cloudtaskspb.Task_HttpRequest{
			HttpRequest: &cloudtaskspb.HttpRequest{Url: "https://example.com"},
		}},
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err), "deleted names stay reserved")

	e.clk.advance(time.Hour + time.Second)
	e.task(t, q, named)
}

func TestTaskViewsAndList(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	q := e.queue(t, "views").GetName()

	for i := range 3 {
		e.task(t, q, func(task *cloudtaskspb.Task) {
			task.MessageType = &cloudtaskspb.Task_HttpRequest{HttpRequest: &cloudtaskspb.HttpRequest{
				Url: "https://example.com", Body: []byte("payload"),
			}}
			task.ScheduleTime = timestamppb.New(e.clk.now().Add(time.Duration(3-i) * time.Minute))
		})
	}

	tests := []struct {
		name     string
		view     cloudtaskspb.Task_View
		wantBody bool
	}{
		{"default is basic", cloudtaskspb.Task_VIEW_UNSPECIFIED, false},
		{"basic", cloudtaskspb.Task_BASIC, false},
		{"full", cloudtaskspb.Task_FULL, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			it := e.client.ListTasks(t.Context(), &cloudtaskspb.ListTasksRequest{
				Parent: q, ResponseView: tt.view, PageSize: 2,
			})

			var times []time.Time

			for {
				task, err := it.Next()
				if errors.Is(err, iterator.Done) {
					break
				}

				require.NoError(t, err)
				assert.Equal(t, tt.wantBody, len(task.GetHttpRequest().GetBody()) > 0)

				times = append(times, task.GetScheduleTime().AsTime())
			}

			require.Len(t, times, 3)
			assert.True(t, times[0].Before(times[1]) && times[1].Before(times[2]), "ordered by schedule time")
		})
	}
}
