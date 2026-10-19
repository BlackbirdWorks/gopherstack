package gcpcloudtasks

import (
	"context"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

type server struct {
	cloudtaskspb.UnimplementedCloudTasksServer
	b *InMemoryBackend
}

func (s *server) ListQueues(
	_ context.Context, in *cloudtaskspb.ListQueuesRequest,
) (*cloudtaskspb.ListQueuesResponse, error) {
	items, next, err := s.b.ListQueues(in)
	if err != nil {
		return nil, err
	}

	return &cloudtaskspb.ListQueuesResponse{Queues: items, NextPageToken: next}, nil
}

func (s *server) GetQueue(_ context.Context, in *cloudtaskspb.GetQueueRequest) (*cloudtaskspb.Queue, error) {
	return s.b.GetQueue(in.GetName())
}

func (s *server) CreateQueue(_ context.Context, in *cloudtaskspb.CreateQueueRequest) (*cloudtaskspb.Queue, error) {
	return s.b.CreateQueue(in)
}

func (s *server) UpdateQueue(_ context.Context, in *cloudtaskspb.UpdateQueueRequest) (*cloudtaskspb.Queue, error) {
	return s.b.UpdateQueue(in)
}

func (s *server) DeleteQueue(_ context.Context, in *cloudtaskspb.DeleteQueueRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteQueue(in.GetName())
}

func (s *server) PurgeQueue(_ context.Context, in *cloudtaskspb.PurgeQueueRequest) (*cloudtaskspb.Queue, error) {
	return s.b.PurgeQueue(in.GetName())
}

func (s *server) PauseQueue(_ context.Context, in *cloudtaskspb.PauseQueueRequest) (*cloudtaskspb.Queue, error) {
	return s.b.PauseQueue(in.GetName())
}

func (s *server) ResumeQueue(_ context.Context, in *cloudtaskspb.ResumeQueueRequest) (*cloudtaskspb.Queue, error) {
	return s.b.ResumeQueue(in.GetName())
}

func (s *server) GetIamPolicy(_ context.Context, in *iampb.GetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.GetIamPolicy(in.GetResource())
}

func (s *server) SetIamPolicy(_ context.Context, in *iampb.SetIamPolicyRequest) (*iampb.Policy, error) {
	return s.b.SetIamPolicy(in)
}

func (s *server) TestIamPermissions(
	_ context.Context, in *iampb.TestIamPermissionsRequest,
) (*iampb.TestIamPermissionsResponse, error) {
	return s.b.TestIamPermissions(in)
}

func (s *server) ListTasks(
	_ context.Context, in *cloudtaskspb.ListTasksRequest,
) (*cloudtaskspb.ListTasksResponse, error) {
	items, next, err := s.b.ListTasks(in)
	if err != nil {
		return nil, err
	}

	return &cloudtaskspb.ListTasksResponse{Tasks: items, NextPageToken: next}, nil
}

func (s *server) GetTask(_ context.Context, in *cloudtaskspb.GetTaskRequest) (*cloudtaskspb.Task, error) {
	return s.b.GetTask(in)
}

func (s *server) CreateTask(_ context.Context, in *cloudtaskspb.CreateTaskRequest) (*cloudtaskspb.Task, error) {
	return s.b.CreateTask(in)
}

func (s *server) DeleteTask(_ context.Context, in *cloudtaskspb.DeleteTaskRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, s.b.DeleteTask(in.GetName())
}

func (s *server) RunTask(_ context.Context, in *cloudtaskspb.RunTaskRequest) (*cloudtaskspb.Task, error) {
	return s.b.RunTask(in)
}

func (s *server) BatchCreateTasks(
	_ context.Context, in *cloudtaskspb.BatchCreateTasksRequest,
) (*longrunningpb.Operation, error) {
	return s.b.BatchCreateTasks(in)
}

func (s *server) BatchDeleteTasks(
	_ context.Context, in *cloudtaskspb.BatchDeleteTasksRequest,
) (*longrunningpb.Operation, error) {
	return s.b.BatchDeleteTasks(in)
}

func (s *server) GetCmekConfig(
	_ context.Context, in *cloudtaskspb.GetCmekConfigRequest,
) (*cloudtaskspb.CmekConfig, error) {
	return s.b.GetCmekConfig(in.GetName())
}

func (s *server) UpdateCmekConfig(
	_ context.Context, in *cloudtaskspb.UpdateCmekConfigRequest,
) (*cloudtaskspb.CmekConfig, error) {
	return s.b.UpdateCmekConfig(in)
}
