package gcplogging

import (
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcplro"
)

const (
	maxCopyFilterLen = 20000
	copyProgressDone = 100
)

func copyDestinationBucket(dest string) (string, bool) {
	name, ok := strings.CutPrefix(dest, destStorage)

	return name, ok && name != "" && !strings.Contains(name, "/")
}

// CopyLogEntries writes the matching entries of a bucket to a Cloud Storage bucket in the sink export layout.
func (b *InMemoryBackend) CopyLogEntries(req *loggingpb.CopyLogEntriesRequest) (*longrunningpb.Operation, error) {
	if err := checkBucketName(req.GetName()); err != nil {
		return nil, err
	}

	if len(req.GetFilter()) > maxCopyFilterLen {
		return nil, errInvalid("filter must be no more than %d characters.", maxCopyFilterLen)
	}

	f, err := compileFilter(req.GetFilter())
	if err != nil {
		return nil, err
	}

	dest, ok := copyDestinationBucket(req.GetDestination())
	if !ok {
		return nil, errInvalid(
			"Invalid destination [%s]: expected storage.googleapis.com/[BUCKET].",
			req.GetDestination(),
		)
	}

	ex := b.exporters()
	if ex.objs == nil {
		return nil, errPrecondition("No Cloud Storage emulator is wired to receive copied log entries.")
	}

	entries, err := b.entriesToCopy(req.GetName(), f)
	if err != nil {
		return nil, err
	}

	batch := exportBatch{objects: map[objectRef][]byte{}}

	for _, e := range entries {
		line, merr := protojson.Marshal(e)
		if merr != nil {
			return nil, errInvalid("encode entry: %v", merr)
		}

		ref := objectRef{bucket: dest, name: objectNameFor(e)}
		batch.objects[ref] = append(append(batch.objects[ref], line...), '\n')
	}

	if ferr := b.writeCopy(ex, &batch); ferr != nil {
		return nil, ferr
	}

	return b.finishCopyOp(req, int64(len(entries)))
}

func (b *InMemoryBackend) entriesToCopy(bucket string, f *filter) ([]*loggingpb.LogEntry, error) {
	b.mu.Lock("CopyLogEntries")
	defer b.mu.Unlock()

	bd, err := b.liveBucketLocked(bucket)
	if err != nil {
		return nil, err
	}

	now := b.now()
	days := time.Duration(bd.pb.GetRetentionDays()) * day

	var out []*loggingpb.LogEntry

	for _, e := range bd.entries {
		if days > 0 && e.GetReceiveTimestamp().AsTime().Before(now.Add(-days)) {
			continue
		}

		if f.matches(newDoc(e)) {
			out = append(out, e)
		}
	}

	sortEntries(out, false)

	return out, nil
}

func (b *InMemoryBackend) writeCopy(ex exporters, batch *exportBatch) error {
	b.exportMu.Lock("copy")
	defer b.exportMu.Unlock()

	for ref, data := range batch.objects {
		if err := appendObject(ex.objs, ref, data); err != nil {
			return errPrecondition("Cannot write to Cloud Storage bucket %s: %v", ref.bucket, err)
		}
	}

	return nil
}

func (b *InMemoryBackend) finishCopyOp(
	req *loggingpb.CopyLogEntriesRequest,
	copied int64,
) (*longrunningpb.Operation, error) {
	b.mu.Lock("copyOp")
	n := b.nextSeq()
	b.mu.Unlock()

	now := timestamppb.New(b.now())
	meta := &loggingpb.CopyLogEntriesMetadata{
		StartTime:      now,
		EndTime:        now,
		State:          loggingpb.OperationState_OPERATION_STATE_SUCCEEDED,
		Progress:       copyProgressDone,
		Request:        req,
		WriterIdentity: writerIdentity(parentOf(req.GetName()), req.GetName(), true),
	}

	op, err := gcplro.Done(
		fmt.Sprintf("%s/operations/copy-%d", parentLocation(req.GetName()), n), meta,
		&loggingpb.CopyLogEntriesResponse{LogEntriesCopiedCount: copied})
	if err != nil {
		return nil, errInvalid("operation: %v", err)
	}

	return b.ops.Put(op), nil
}
