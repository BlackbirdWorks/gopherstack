package sparql

import (
	"bytes"

	"github.com/blackbirdworks/gopherstack/services/neptune/streamlog"
)

// Stream returns the store's Neptune Streams commit log.
func (s *Store) Stream() *streamlog.Log { return s.stream }

// begin starts collecting changes for one transaction when streams are enabled; callers hold s.mu.
func (s *Store) begin() {
	s.pending, s.recording = nil, s.stream.Enabled()
}

func (s *Store) note(op string, q Quad) {
	if s.recording {
		var buf bytes.Buffer

		writeQuad(&buf, q, true)
		s.pending = append(s.pending, streamlog.Change{Op: op, Data: streamlog.Data{Stmt: buf.String()}})
	}
}

func (s *Store) commit() {
	if s.recording {
		s.stream.Commit(s.pending)
	}

	s.pending, s.recording = nil, false
}

// Wipe empties the store and its stream log, as the fast reset does.
func (s *Store) Wipe() {
	s.mu.Lock("Wipe")
	defer s.mu.Unlock()

	s.reset()
	s.stream.Reset()
}
