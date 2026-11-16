package gcpfirestore

import (
	"errors"
	"io"
	"strconv"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
)

const (
	maxWriteStreams   = 1000
	maxStreamReplay   = 64
	maxWritesPerFrame = 500
)

// writeStream is the server side of one Write stream: its token sequence and the responses a resume may replay.
type writeStream struct {
	id       string
	database string
	acked    []*firestorepb.WriteResponse
	seq      uint64
}

func seqToken(n uint64) []byte { return []byte(strconv.FormatUint(n, 10)) }

func parseSeq(tok []byte) (uint64, bool) {
	if len(tok) == 0 {
		return 0, true
	}

	n, err := strconv.ParseUint(string(tok), 10, 64)

	return n, err == nil
}

func (b *InMemoryBackend) openWriteStream(
	first *firestorepb.WriteRequest,
) ([]*firestorepb.WriteResponse, *writeStream, error) {
	if err := validateDatabase(first.GetDatabase()); err != nil {
		return nil, nil, err
	}

	b.mu.Lock("OpenWriteStream")
	defer b.mu.Unlock()

	if id := first.GetStreamId(); id != "" {
		ws, ok := b.wstreams[id]
		if !ok || ws.database != first.GetDatabase() {
			return nil, nil, errNotFound("Write stream %q does not exist.", id)
		}

		after, valid := parseSeq(first.GetStreamToken())
		if !valid || after > ws.seq {
			return nil, nil, errInvalid("Invalid stream token.")
		}

		var replay []*firestorepb.WriteResponse

		for _, r := range ws.acked {
			if n, _ := parseSeq(r.GetStreamToken()); n > after {
				replay = append(replay, r)
			}
		}

		if len(replay) == 0 {
			replay = []*firestorepb.WriteResponse{{StreamId: ws.id, StreamToken: seqToken(ws.seq)}}
		}

		return replay, ws, nil
	}

	if len(first.GetWrites()) > 0 {
		return nil, nil, errInvalid("The first request of a Write stream must be a handshake without writes.")
	}

	if len(b.wstreams) >= maxWriteStreams {
		b.evictWriteStreamLocked()
	}

	idRaw, err := newTxnID()
	if err != nil {
		return nil, nil, err
	}

	ws := &writeStream{id: idRaw, database: first.GetDatabase()}
	b.wstreams[ws.id] = ws
	b.wstreamOrder = append(b.wstreamOrder, ws.id)

	return []*firestorepb.WriteResponse{{StreamId: ws.id, StreamToken: seqToken(0)}}, ws, nil
}

func (b *InMemoryBackend) evictWriteStreamLocked() {
	for len(b.wstreamOrder) > 0 && len(b.wstreams) >= maxWriteStreams {
		delete(b.wstreams, b.wstreamOrder[0])
		b.wstreamOrder = b.wstreamOrder[1:]
	}
}

// applyStreamWrites commits one request of a Write stream and records the response for replay.
func (b *InMemoryBackend) applyStreamWrites(
	ws *writeStream,
	req *firestorepb.WriteRequest,
) (*firestorepb.WriteResponse, error) {
	if len(req.GetWrites()) > maxWritesPerFrame {
		return nil, errInvalid("A Write request may carry at most %d writes.", maxWritesPerFrame)
	}

	resp, err := b.Commit(&firestorepb.CommitRequest{Database: ws.database, Writes: req.GetWrites()})
	if err != nil {
		return nil, err
	}

	b.mu.Lock("WriteStream")
	defer b.mu.Unlock()

	ws.seq++
	out := &firestorepb.WriteResponse{
		StreamId:     ws.id,
		StreamToken:  seqToken(ws.seq),
		WriteResults: resp.GetWriteResults(),
		CommitTime:   resp.GetCommitTime(),
	}

	ws.acked = append(ws.acked, out)
	if len(ws.acked) > maxStreamReplay {
		ws.acked = ws.acked[len(ws.acked)-maxStreamReplay:]
	}

	return out, nil
}

// Write serves the bidirectional write stream: a handshake, then one atomic commit per request.
func (s *server) Write(stream firestorepb.Firestore_WriteServer) error {
	first, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return nil
	}

	if err != nil {
		return err
	}

	initial, ws, err := s.b.openWriteStream(first)
	if err != nil {
		return err
	}

	for _, r := range initial {
		if err = stream.Send(r); err != nil {
			return err
		}
	}

	for {
		req, rerr := stream.Recv()
		if errors.Is(rerr, io.EOF) {
			return nil
		}

		if rerr != nil {
			return rerr
		}

		if len(req.GetWrites()) == 0 {
			continue
		}

		resp, aerr := s.b.applyStreamWrites(ws, req)
		if aerr != nil {
			return aerr
		}

		if err = stream.Send(resp); err != nil {
			return err
		}
	}
}
