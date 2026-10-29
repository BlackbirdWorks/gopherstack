package ocilogging

import (
	"encoding/json"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

const (
	specVersion       = "1.0"
	maxEntryBytes     = 1 << 20
	maxDataChars      = 10000
	maxEntriesPerLog  = 50000
	maxBatchesPerCall = 1000
)

// PutLogsIn is the PutLogs request body.
type PutLogsIn struct {
	SpecVersion string     `json:"specversion"`
	Batches     []LogBatch `json:"logEntryBatches"`
}

// LogBatch is a group of entries sharing a source, type and subject.
type LogBatch struct {
	DefaultTime *time.Time `json:"defaultlogentrytime"`
	Source      *string    `json:"source"`
	Type        *string    `json:"type"`
	Subject     string     `json:"subject"`
	Entries     []LogEntry `json:"entries"`
}

// LogEntry is one entry of a batch.
type LogEntry struct {
	Time *time.Time `json:"time"`
	Data *string    `json:"data"`
	ID   *string    `json:"id"`
}

func validatePut(in PutLogsIn) *APIError {
	if in.SpecVersion != specVersion {
		return errParam("specversion must be 1.0")
	}

	if len(in.Batches) == 0 || len(in.Batches) > maxBatchesPerCall {
		return errParam("logEntryBatches must contain between 1 and 1000 batches")
	}

	for _, bt := range in.Batches {
		if bt.Source == nil || bt.Type == nil || bt.DefaultTime == nil {
			return errMissing("every batch needs source, type and defaultlogentrytime")
		}

		if len(bt.Entries) == 0 {
			return errParam("every batch needs at least one entry")
		}

		for _, e := range bt.Entries {
			if e.Data == nil || e.ID == nil || *e.ID == "" {
				return errMissing("every entry needs data and id")
			}

			if len(*e.Data) > maxEntryBytes {
				return errParam("an entry must be smaller than 1 MB")
			}
		}
	}

	return nil
}

func truncateChars(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}

	return string([]rune(s)[:n])
}

// PutLogs stores the batches in an ACTIVE, enabled log.
func (b *InMemoryBackend) PutLogs(logID string, in PutLogsIn) error {
	if aerr := validatePut(in); aerr != nil {
		return aerr
	}

	b.mu.Lock("PutLogs")
	defer b.mu.Unlock()

	b.settleAll()

	l, aerr := b.liveLog("", logID)
	if aerr != nil {
		return aerr
	}

	if l.State != stateActive {
		return errState("Log " + logID + " is in state " + l.State + "; ingestion requires ACTIVE")
	}

	now := b.nowUTC()

	for _, bt := range in.Batches {
		for _, e := range bt.Entries {
			t := bt.DefaultTime.UTC()
			if e.Time != nil {
				t = e.Time.UTC()
			}

			b.append(l.ID, &Entry{
				ID: *e.ID, Time: t, Ingested: now, Data: truncateChars(*e.Data, maxDataChars),
				Source: *bt.Source, Type: *bt.Type, Subject: bt.Subject,
			})
		}
	}

	return nil
}

func (b *InMemoryBackend) append(logID string, e *Entry) {
	e.Seq = b.nextSeq()
	es := b.entries[logID]
	es = append(es, e)

	if len(es) > maxEntriesPerLog {
		es = es[len(es)-maxEntriesPerLog:]
	}

	b.entries[logID] = es
}

// ServiceEvent is a log record emitted by another emulated OCI service.
type ServiceEvent struct {
	Time    time.Time
	Data    map[string]any
	Type    string
	Subject string
}

// EmitServiceLog stores the event in every ACTIVE SERVICE log whose source matches; it returns how many logs took it.
func (b *InMemoryBackend) EmitServiceLog(service, resource, category string, ev ServiceEvent) int {
	data, err := json.Marshal(ev.Data)
	if err != nil {
		return 0
	}

	b.mu.Lock("EmitServiceLog")
	defer b.mu.Unlock()

	b.settleAll()

	now, n := b.nowUTC(), 0

	for _, l := range b.logs {
		s := l.Config
		if l.LogType != logTypeService || l.State != stateActive || s == nil || s.Source == nil ||
			s.Source.Service != service || s.Source.Resource != resource || s.Source.Category != category {
			continue
		}

		t := ev.Time.UTC()
		if ev.Time.IsZero() {
			t = now
		}

		b.append(l.ID, &Entry{
			ID: uuid.NewString(), Time: t, Ingested: now, Data: string(data),
			Source: service + "." + resource, Type: ev.Type, Subject: ev.Subject,
		})

		n++
	}

	return n
}

func (h *Handler) putLogs(q *req) error {
	var in PutLogsIn
	if err := q.decode(&in); err != nil {
		return err
	}

	if err := h.Backend.PutLogs(q.param("logId"), in); err != nil {
		return err
	}

	q.w.WriteHeader(http.StatusOK)

	return nil
}
