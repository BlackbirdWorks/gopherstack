package gcplogging

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maxEntriesPerWrite = 10000
	maxEntryBytes      = 256 << 10
	maxLabelsPerEntry  = 64
)

func newInsertID() string {
	var buf [10]byte

	_, _ = rand.Read(buf[:])

	return hex.EncodeToString(buf[:])
}

// normalizeEntry applies request defaults and validates one entry; it returns a private copy.
func (b *InMemoryBackend) normalizeEntry(
	req *loggingpb.WriteLogEntriesRequest,
	in *loggingpb.LogEntry,
) (*loggingpb.LogEntry, error) {
	e, _ := proto.Clone(in).(*loggingpb.LogEntry)

	if e.GetLogName() == "" {
		e.LogName = req.GetLogName()
	}

	if _, err := splitLogName(e.GetLogName()); err != nil {
		return nil, err
	}

	if e.GetResource() == nil {
		e.Resource = req.GetResource()
	}

	if e.GetResource().GetType() == "" {
		return nil, errInvalid("Log entry has no resource type: set resource on the entry or the request.")
	}

	e.Resource, _ = proto.Clone(e.GetResource()).(*monitoredres.MonitoredResource)

	if err := mergeLabels(e, req.GetLabels()); err != nil {
		return nil, err
	}

	if proto.Size(e) > maxEntryBytes {
		return nil, errInvalid("Log entry exceeds the maximum size of %d bytes.", maxEntryBytes)
	}

	now := b.now()
	if e.GetTimestamp() == nil {
		e.Timestamp = timestamppb.New(now)
	}

	if e.GetInsertId() == "" {
		e.InsertId = newInsertID()
	}

	e.ReceiveTimestamp = timestamppb.New(now)

	return e, nil
}

func mergeLabels(e *loggingpb.LogEntry, defaults map[string]string) error {
	if len(defaults) == 0 {
		return nil
	}

	merged := make(map[string]string, len(defaults)+len(e.GetLabels()))

	maps.Copy(merged, defaults)
	maps.Copy(merged, e.GetLabels())

	if len(merged) > maxLabelsPerEntry {
		return errInvalid("Log entry has more than %d labels.", maxLabelsPerEntry)
	}

	e.Labels = merged

	return nil
}

// WriteLogEntries stores entries, routes them through sinks, updates log-based metrics and feeds tails.
func (b *InMemoryBackend) WriteLogEntries(ctx context.Context, req *loggingpb.WriteLogEntriesRequest) error {
	in := req.GetEntries()
	if len(in) > maxEntriesPerWrite {
		return errInvalid("Too many log entries: %d, the maximum is %d per request.", len(in), maxEntriesPerWrite)
	}

	valid, failures := b.normalizeAll(req)

	if len(failures) > 0 && !req.GetPartialSuccess() {
		return status.Convert(failures[firstFailure(failures)]).Err()
	}

	if !req.GetDryRun() && len(valid) > 0 {
		b.ingest(ctx, valid)
	}

	if len(failures) > 0 {
		return partialError(failures)
	}

	return nil
}

func firstFailure(f map[int32]error) int32 {
	return slices.Min(slices.Collect(maps.Keys(f)))
}

func (b *InMemoryBackend) normalizeAll(req *loggingpb.WriteLogEntriesRequest) ([]*loggingpb.LogEntry, map[int32]error) {
	valid := make([]*loggingpb.LogEntry, 0, len(req.GetEntries()))
	failures := map[int32]error{}

	for i, in := range req.GetEntries() {
		e, err := b.normalizeEntry(req, in)
		if err != nil {
			failures[int32(i)] = err

			continue
		}

		valid = append(valid, e)
	}

	return valid, failures
}

func partialError(failures map[int32]error) error {
	detail := &loggingpb.WriteLogEntriesPartialErrors{LogEntryErrors: make(map[int32]*rpcstatus.Status, len(failures))}

	for i, err := range failures {
		st := status.Convert(err)
		detail.LogEntryErrors[i] = &rpcstatus.Status{Code: codeInt32(st.Code()), Message: st.Message()}
	}

	st, err := status.New(codes.InvalidArgument,
		fmt.Sprintf("%d log entries could not be written", len(failures))).WithDetails(detail)
	if err != nil {
		return status.Error(codes.InvalidArgument, "log entries could not be written")
	}

	return st.Err()
}

func codeInt32(c codes.Code) int32 {
	return int32(c) //nolint:gosec // gRPC codes fit int32
}
