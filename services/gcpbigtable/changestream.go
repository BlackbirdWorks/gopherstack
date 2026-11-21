package gcpbigtable

import (
	"context"
	"encoding/base64"
	"strconv"
	"time"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const defaultHeartbeat = 5 * time.Second

type changeRec struct {
	key  string
	muts []*btpb.Mutation
	at   int64
	seq  uint64
}

// recordChange appends one committed row change to the table's change stream when the stream is enabled.
func (b *InMemoryBackend) recordChange(t *tableRec, key string, muts []*btpb.Mutation, now, serverTS int64) {
	cfg := t.pb.GetChangeStreamConfig()
	if cfg == nil || len(muts) == 0 {
		return
	}

	resolved := make([]*btpb.Mutation, len(muts))

	for i, m := range muts {
		resolved[i] = clone(m)
		if sc := resolved[i].GetSetCell(); sc != nil && sc.GetTimestampMicros() == serverTime {
			sc.TimestampMicros = serverTS
		}
	}

	t.changeN++
	t.changes = append(t.changes, changeRec{seq: t.changeN, at: now, key: key, muts: resolved})

	cutoff := now - cfg.GetRetentionPeriod().AsDuration().Microseconds()
	drop := 0

	for drop < len(t.changes) && t.changes[drop].at < cutoff {
		drop++
	}

	t.changes = t.changes[drop:]

	close(b.notify)
	b.notify = make(chan struct{})
}

func setCellMutation(fam, qual string, ts int64, val []byte) *btpb.Mutation {
	return &btpb.Mutation{Mutation: &btpb.Mutation_SetCell_{SetCell: &btpb.Mutation_SetCell{
		FamilyName: fam, ColumnQualifier: []byte(qual), TimestampMicros: ts, Value: val,
	}}}
}

type partitionRange struct {
	start string
	end   string
	open  bool
}

func (p partitionRange) contains(key string) bool {
	return key >= p.start && (!p.open || key < p.end)
}

func partitionOf(sp *btpb.StreamPartition) (partitionRange, error) {
	rr := sp.GetRowRange()
	if rr == nil {
		return partitionRange{}, errInvalid("partition.row_range is required.")
	}

	var p partitionRange

	switch s := rr.GetStartKey().(type) {
	case nil:
	case *btpb.RowRange_StartKeyClosed:
		p.start = string(s.StartKeyClosed)
	default:
		return partitionRange{}, errInvalid("partition.row_range must use start_key_closed.")
	}

	switch e := rr.GetEndKey().(type) {
	case nil:
	case *btpb.RowRange_EndKeyOpen:
		p.end, p.open = string(e.EndKeyOpen), true
	default:
		return partitionRange{}, errInvalid("partition.row_range must use end_key_open.")
	}

	if p.open && p.end != "" && p.start >= p.end {
		return partitionRange{}, errInvalid("partition.row_range start must be less than end.")
	}

	return p, nil
}

func encodeToken(seq uint64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatUint(seq, 10)))
}

func decodeToken(tok string) (uint64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return 0, errInvalid("Invalid continuation token.")
	}

	n, err := strconv.ParseUint(string(raw), 10, 64)
	if err != nil {
		return 0, errInvalid("Invalid continuation token.")
	}

	return n, nil
}

type changeQuery struct {
	part      partitionRange
	hb        time.Duration
	startAt   int64
	endAt     int64
	after     uint64
	fromToken bool
}

func parseChangeQuery(req *btpb.ReadChangeStreamRequest) (*changeQuery, error) {
	part, err := partitionOf(req.GetPartition())
	if err != nil {
		return nil, err
	}

	q := &changeQuery{part: part, hb: defaultHeartbeat}

	if hb := req.GetHeartbeatDuration(); hb != nil {
		if q.hb = hb.AsDuration(); q.hb <= 0 {
			return nil, errInvalid("heartbeat_duration must be positive.")
		}
	}

	if req.GetEndTime() != nil {
		q.endAt = req.GetEndTime().AsTime().UnixMicro()
	}

	switch from := req.GetStartFrom().(type) {
	case *btpb.ReadChangeStreamRequest_StartTime:
		q.startAt = from.StartTime.AsTime().UnixMicro()
	case *btpb.ReadChangeStreamRequest_ContinuationTokens:
		for _, tk := range from.ContinuationTokens.GetTokens() {
			if !proto.Equal(tk.GetPartition(), req.GetPartition()) {
				continue
			}

			if q.after, err = decodeToken(tk.GetToken()); err != nil {
				return nil, err
			}

			q.fromToken = true
		}

		if !q.fromToken {
			return nil, errInvalid("continuation_tokens must include a token for the requested partition.")
		}
	default:
		return nil, errInvalid("One of start_time or continuation_tokens is required.")
	}

	return q, nil
}

type changeBatch struct {
	notify  <-chan struct{}
	cluster string
	records []changeRec
	pos     uint64
}

func (b *InMemoryBackend) changeTable(req *btpb.ReadChangeStreamRequest) (*instRec, *tableRec, error) {
	rec, t, _, err := b.tableLocked(req.GetTableName())
	if err != nil {
		return nil, nil, err
	}

	if t.pb.GetChangeStreamConfig() == nil {
		return nil, nil, errPrecondition("Change streaming is not enabled on table %q.", req.GetTableName())
	}

	return rec, t, nil
}

func (b *InMemoryBackend) sourceCluster(rec *instRec, appProfile string) (string, error) {
	if appProfile != "" && appProfile != defaultApp {
		pr, ok := rec.profiles[appProfile]
		if !ok {
			return "", errNotFound("App profile %q not found.", profileName(rec.pb.GetName(), appProfile))
		}

		sc := pr.GetSingleClusterRouting()
		if sc == nil {
			return "", errPrecondition("App profile %q must use single cluster routing for change streams.", appProfile)
		}

		return sc.GetClusterId(), nil
	}

	for _, id := range sortedKeys(rec.clusters) {
		return id, nil
	}

	return "", nil
}

func (b *InMemoryBackend) readChanges(req *btpb.ReadChangeStreamRequest, q *changeQuery) (*changeBatch, error) {
	b.mu.RLock("ReadChangeStream")
	defer b.mu.RUnlock()

	rec, t, err := b.changeTable(req)
	if err != nil {
		return nil, err
	}

	cluster, err := b.sourceCluster(rec, req.GetAppProfileId())
	if err != nil {
		return nil, err
	}

	if q.fromToken && q.after > t.changeN {
		return nil, errInvalid("Continuation token does not belong to this table's change stream.")
	}

	retention := t.pb.GetChangeStreamConfig().GetRetentionPeriod().AsDuration().Microseconds()
	if !q.fromToken && q.startAt < b.nowMicros()-retention {
		return nil, errInvalid("start_time is older than the change stream retention period.")
	}

	out := &changeBatch{notify: b.notify, cluster: cluster, pos: t.changeN}

	for _, c := range t.changes {
		switch {
		case c.seq <= q.after, !q.fromToken && c.at < q.startAt:
		case q.endAt > 0 && c.at > q.endAt:
		case q.part.contains(c.key):
			out.records = append(out.records, c)
		}
	}

	return out, nil
}

func dataChange(c changeRec, cluster string) *btpb.ReadChangeStreamResponse {
	chunks := make([]*btpb.ReadChangeStreamResponse_MutationChunk, len(c.muts))
	for i, m := range c.muts {
		chunks[i] = &btpb.ReadChangeStreamResponse_MutationChunk{Mutation: clone(m)}
	}

	ts := timestamppb.New(time.UnixMicro(c.at))

	return &btpb.ReadChangeStreamResponse{StreamRecord: &btpb.ReadChangeStreamResponse_DataChange_{
		DataChange: &btpb.ReadChangeStreamResponse_DataChange{
			Type:                  btpb.ReadChangeStreamResponse_DataChange_USER,
			SourceClusterId:       cluster,
			RowKey:                []byte(c.key),
			CommitTimestamp:       ts,
			Chunks:                chunks,
			Done:                  true,
			Token:                 encodeToken(c.seq),
			EstimatedLowWatermark: ts,
		},
	}}
}

func (b *InMemoryBackend) heartbeat(part *btpb.StreamPartition, pos uint64) *btpb.ReadChangeStreamResponse {
	return &btpb.ReadChangeStreamResponse{StreamRecord: &btpb.ReadChangeStreamResponse_Heartbeat_{
		Heartbeat: &btpb.ReadChangeStreamResponse_Heartbeat{
			ContinuationToken:     &btpb.StreamContinuationToken{Partition: clone(part), Token: encodeToken(pos)},
			EstimatedLowWatermark: b.ts(),
		},
	}}
}

// StreamChanges delivers the committed changes of a partition from the requested position. With an end_time it
// returns once that time has passed; otherwise it keeps delivering until the context ends, with a heartbeat
// every heartbeat_duration.
func (b *InMemoryBackend) StreamChanges(
	ctx context.Context, req *btpb.ReadChangeStreamRequest, send func(*btpb.ReadChangeStreamResponse) error,
) error {
	q, err := parseChangeQuery(req)
	if err != nil {
		return err
	}

	tick := time.NewTicker(q.hb)
	defer tick.Stop()

	var endC <-chan time.Time

	if q.endAt > 0 {
		timer := time.NewTimer(time.Duration(q.endAt-b.clock().UnixMicro()) * time.Microsecond)
		defer timer.Stop()

		endC = timer.C
	}

	ended := false

	for {
		batch, rerr := b.readChanges(req, q)
		if rerr != nil {
			return rerr
		}

		for _, c := range batch.records {
			if err = send(dataChange(c, batch.cluster)); err != nil {
				return err
			}
		}

		q.after, q.fromToken = batch.pos, true

		if ended || (q.endAt > 0 && b.clock().UnixMicro() >= q.endAt) {
			return nil
		}

		select {
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case <-batch.notify:
		case <-endC:
			ended = true
		case <-tick.C:
			if err = send(b.heartbeat(req.GetPartition(), batch.pos)); err != nil {
				return err
			}
		}
	}
}

// InitialChangeStreamPartitions splits the key space of a change-stream table along its split points.
func (b *InMemoryBackend) InitialChangeStreamPartitions(
	req *btpb.GenerateInitialChangeStreamPartitionsRequest,
) ([]*btpb.StreamPartition, error) {
	b.mu.RLock("GenerateInitialChangeStreamPartitions")
	defer b.mu.RUnlock()

	rec, t, _, err := b.tableLocked(req.GetTableName())
	if err != nil {
		return nil, err
	}

	if t.pb.GetChangeStreamConfig() == nil {
		return nil, errPrecondition("Change streaming is not enabled on table %q.", req.GetTableName())
	}

	if _, err = b.sourceCluster(rec, req.GetAppProfileId()); err != nil {
		return nil, err
	}

	bounds := append([]string{""}, t.splits...)
	out := make([]*btpb.StreamPartition, len(bounds))

	for i, start := range bounds {
		rr := &btpb.RowRange{StartKey: &btpb.RowRange_StartKeyClosed{StartKeyClosed: []byte(start)}}
		if i+1 < len(bounds) {
			rr.EndKey = &btpb.RowRange_EndKeyOpen{EndKeyOpen: []byte(bounds[i+1])}
		}

		out[i] = &btpb.StreamPartition{RowRange: rr}
	}

	return out, nil
}
