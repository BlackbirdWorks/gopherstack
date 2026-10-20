package gcpcloudtasks

import (
	"strings"
	"time"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
)

const (
	maxBatchCreate = 100
	maxBatchDelete = 1000
	opIDLen        = 16
)

func (b *InMemoryBackend) opNameFor(parent, requestID string) string {
	if requestID == "" {
		requestID = awsid.NewN("", opIDLen)
	}

	return parent + "/operations/" + requestID
}

func failureOf(err error) *rpcstatus.Status { return status.Convert(err).Proto() }

func (b *InMemoryBackend) finishOp(op *longrunningpb.Operation, err error) (*longrunningpb.Operation, error) {
	if err != nil {
		return nil, errInvalid("%v", err)
	}

	return b.ops.Put(op), nil
}

func validateBatchCreate(req *cloudtaskspb.BatchCreateTasksRequest) error {
	if _, err := parseQueueName(req.GetParent()); err != nil {
		return err
	}

	n := len(req.GetRequests())
	if n == 0 {
		return errInvalid("requests must not be empty.")
	}

	if n > maxBatchCreate {
		return errInvalid("A maximum of %d tasks can be created in a batch.", maxBatchCreate)
	}

	for _, r := range req.GetRequests() {
		if r.GetParent() != req.GetParent() {
			return errInvalid("Every CreateTaskRequest.parent must equal BatchCreateTasksRequest.parent.")
		}
	}

	return nil
}

// BatchCreateTasks creates up to 100 tasks and records a finished operation listing per-task failures.
func (b *InMemoryBackend) BatchCreateTasks(
	req *cloudtaskspb.BatchCreateTasksRequest,
) (*longrunningpb.Operation, error) {
	if err := validateBatchCreate(req); err != nil {
		return nil, err
	}

	name := b.opNameFor(req.GetParent(), req.GetRequestId())

	b.mu.Lock("BatchCreateTasks")
	defer b.mu.Unlock()

	if op, ok := b.ops.Lookup(name); ok && req.GetRequestId() != "" {
		return op, nil
	}

	q, ok := b.queues[req.GetParent()]
	if !ok {
		return nil, errQueueMissing()
	}

	now := b.now()
	meta := &cloudtaskspb.BatchCreateTasksMetadata{
		StartTime: tsOf(now), FailedRequests: make(map[int32]*rpcstatus.Status),
	}
	resp := &cloudtaskspb.BatchCreateTasksResponse{}

	for i, r := range req.GetRequests() {
		t, err := b.batchCreateOne(q, r, now)
		if err != nil {
			meta.FailedRequests[int32(i)] = failureOf(err)

			continue
		}

		resp.Tasks = append(resp.Tasks, viewOf(t.pb, r.GetResponseView()))
	}

	meta.EndTime = tsOf(now)

	op, err := gcplro.Done(name, meta, resp)

	return b.finishOp(op, err)
}

func (b *InMemoryBackend) batchCreateOne(
	q *queueRec, r *cloudtaskspb.CreateTaskRequest, now time.Time,
) (*taskRec, error) {
	if r.GetTask() == nil {
		return nil, errInvalid("task must be specified.")
	}

	return b.newTaskLocked(q, r.GetTask(), now)
}

func validateBatchDelete(req *cloudtaskspb.BatchDeleteTasksRequest) error {
	if _, err := parseQueueName(req.GetParent()); err != nil {
		return err
	}

	n := len(req.GetNames())
	if n == 0 {
		return errInvalid("names must not be empty.")
	}

	if n > maxBatchDelete {
		return errInvalid("A maximum of %d tasks can be deleted in a batch.", maxBatchDelete)
	}

	return nil
}

// BatchDeleteTasks deletes up to 1000 tasks and records a finished operation listing per-task failures.
func (b *InMemoryBackend) BatchDeleteTasks(
	req *cloudtaskspb.BatchDeleteTasksRequest,
) (*longrunningpb.Operation, error) {
	if err := validateBatchDelete(req); err != nil {
		return nil, err
	}

	name := b.opNameFor(req.GetParent(), req.GetRequestId())

	b.mu.Lock("BatchDeleteTasks")
	defer b.mu.Unlock()

	if op, ok := b.ops.Lookup(name); ok && req.GetRequestId() != "" {
		return op, nil
	}

	q, ok := b.queues[req.GetParent()]
	if !ok {
		return nil, errQueueMissing()
	}

	now := b.now()
	meta := &cloudtaskspb.BatchDeleteTasksMetadata{
		StartTime: tsOf(now), FailedRequests: make(map[int32]*rpcstatus.Status),
	}

	for i, n := range req.GetNames() {
		if err := b.deleteTaskLocked(q, n); err != nil {
			meta.FailedRequests[int32(i)] = failureOf(err)
		}
	}

	meta.EndTime = tsOf(now)

	op, err := gcplro.Done(name, meta, &emptypb.Empty{})

	return b.finishOp(op, err)
}

func (b *InMemoryBackend) deleteTaskLocked(q *queueRec, name string) error {
	if !strings.HasPrefix(name, q.pb.GetName()+"/tasks/") {
		return errInvalid("Task name %q must be under queue %q.", name, q.pb.GetName())
	}

	if _, ok := q.tasks[name]; !ok {
		return errTaskMissing()
	}

	delete(q.tasks, name)
	b.tombstones[name] = b.now().Add(taskTombstoneTTL)

	return nil
}
