package gcplogging

import (
	"fmt"
	"strings"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
)

// finishBucketOp records an already-finished bucket operation; the bucket change has been applied synchronously.
func (b *InMemoryBackend) finishBucketOp(
	location string, bucket *loggingpb.LogBucket, meta *loggingpb.BucketMetadata,
) (*longrunningpb.Operation, error) {
	b.mu.Lock("bucketOp")
	n := b.nextSeq()
	b.mu.Unlock()

	now := timestamppb.New(b.now())
	meta.StartTime, meta.EndTime = now, now
	meta.State = loggingpb.OperationState_OPERATION_STATE_SUCCEEDED

	op, err := gcplro.Done(fmt.Sprintf("%s/operations/bucket-%d", location, n), meta, bucket)
	if err != nil {
		return nil, errInvalid("operation: %v", err)
	}

	return b.ops.Put(op), nil
}

func (b *InMemoryBackend) createBucketOp(req *loggingpb.CreateBucketRequest) (*longrunningpb.Operation, error) {
	bucket, err := b.CreateBucket(req)
	if err != nil {
		return nil, err
	}

	meta := &loggingpb.BucketMetadata{Request: &loggingpb.BucketMetadata_CreateBucketRequest{CreateBucketRequest: req}}

	return b.finishBucketOp(req.GetParent(), bucket, meta)
}

func (b *InMemoryBackend) updateBucketOp(req *loggingpb.UpdateBucketRequest) (*longrunningpb.Operation, error) {
	bucket, err := b.UpdateBucket(req)
	if err != nil {
		return nil, err
	}

	meta := &loggingpb.BucketMetadata{Request: &loggingpb.BucketMetadata_UpdateBucketRequest{UpdateBucketRequest: req}}
	location := bucket.GetName()[:strings.LastIndex(bucket.GetName(), "/buckets/")]

	return b.finishBucketOp(location, bucket, meta)
}
