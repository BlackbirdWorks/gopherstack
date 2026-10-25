package gcpworkflows

import (
	"context"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/workflows/apiv1/workflowspb"
	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"
	"google.golang.org/genproto/googleapis/cloud/location"
)

type workflowsServer struct {
	workflowspb.UnimplementedWorkflowsServer
	b *InMemoryBackend
}

func (s *workflowsServer) ListWorkflows(
	_ context.Context, in *workflowspb.ListWorkflowsRequest,
) (*workflowspb.ListWorkflowsResponse, error) {
	return s.b.ListWorkflows(in)
}

func (s *workflowsServer) GetWorkflow(
	_ context.Context,
	in *workflowspb.GetWorkflowRequest,
) (*workflowspb.Workflow, error) {
	return s.b.GetWorkflow(in)
}

func (s *workflowsServer) CreateWorkflow(
	_ context.Context, in *workflowspb.CreateWorkflowRequest,
) (*longrunningpb.Operation, error) {
	return s.b.CreateWorkflow(in)
}

func (s *workflowsServer) DeleteWorkflow(
	_ context.Context, in *workflowspb.DeleteWorkflowRequest,
) (*longrunningpb.Operation, error) {
	return s.b.DeleteWorkflow(in)
}

func (s *workflowsServer) UpdateWorkflow(
	_ context.Context, in *workflowspb.UpdateWorkflowRequest,
) (*longrunningpb.Operation, error) {
	return s.b.UpdateWorkflow(in)
}

func (s *workflowsServer) ListWorkflowRevisions(
	_ context.Context, in *workflowspb.ListWorkflowRevisionsRequest,
) (*workflowspb.ListWorkflowRevisionsResponse, error) {
	return s.b.ListWorkflowRevisions(in)
}

type executionsServer struct {
	executionspb.UnimplementedExecutionsServer
	b *InMemoryBackend
}

func (s *executionsServer) ListExecutions(
	_ context.Context, in *executionspb.ListExecutionsRequest,
) (*executionspb.ListExecutionsResponse, error) {
	return s.b.ListExecutions(in)
}

func (s *executionsServer) CreateExecution(
	_ context.Context, in *executionspb.CreateExecutionRequest,
) (*executionspb.Execution, error) {
	return s.b.CreateExecution(in)
}

func (s *executionsServer) GetExecution(
	_ context.Context,
	in *executionspb.GetExecutionRequest,
) (*executionspb.Execution, error) {
	return s.b.GetExecution(in)
}

func (s *executionsServer) CancelExecution(
	_ context.Context, in *executionspb.CancelExecutionRequest,
) (*executionspb.Execution, error) {
	return s.b.CancelExecution(in)
}

type locationsServer struct {
	location.UnimplementedLocationsServer
	b *InMemoryBackend
}

func (s locationsServer) ListLocations(
	_ context.Context, in *location.ListLocationsRequest,
) (*location.ListLocationsResponse, error) {
	return s.b.ListLocations(in)
}

func (s locationsServer) GetLocation(_ context.Context, in *location.GetLocationRequest) (*location.Location, error) {
	return s.b.GetLocation(in.GetName())
}
