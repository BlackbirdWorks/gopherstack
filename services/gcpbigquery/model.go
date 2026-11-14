package gcpbigquery

import (
	"fmt"
	"strconv"

	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

const (
	defaultLocation = "US"
	typeTable       = "TABLE"
	typeView        = "VIEW"
)

// Dataset is a BigQuery dataset. Meta holds the writable passthrough fields (labels, access, description, ...).
type Dataset struct {
	Meta     map[string]any    `json:"meta"`
	Tables   map[string]*Table `json:"tables"`
	Location string            `json:"location"`
	Created  int64             `json:"created"`
	Modified int64             `json:"modified"`
	Hidden   bool              `json:"hidden,omitempty"`
}

// live returns the table unless its expirationTime has passed; an expired table behaves as deleted.
func (d *Dataset) live(id string) (*Table, bool) {
	t, ok := d.Tables[id]
	if !ok || t.expired(nowMs()) {
		return nil, false
	}

	return t, true
}

func (d *Dataset) liveCount() int {
	n := 0

	for id := range d.Tables {
		if _, ok := d.live(id); ok {
			n++
		}
	}

	return n
}

func (t *Table) expired(now int64) bool {
	v, ok := t.Meta["expirationTime"]
	if !ok {
		return false
	}

	ms, err := strconv.ParseInt(fmt.Sprint(v), 10, 64)

	return err == nil && ms > 0 && ms <= now
}

// Table is a table or view with its rows in the canonical stored encoding.
type Table struct {
	Meta      map[string]any   `json:"meta"`
	Seen      map[string]int64 `json:"seen,omitempty"`
	ViewQuery string           `json:"viewQuery,omitempty"`
	Schema    []bqsql.Field    `json:"schema"`
	Rows      [][]any          `json:"rows"`
	Created   int64            `json:"created"`
	Modified  int64            `json:"modified"`
	Seq       int64            `json:"seq"`
}

func (t *Table) kind() string {
	if t.ViewQuery != "" {
		return typeView
	}

	return typeTable
}

// Job is a BigQuery job. Query jobs keep their result rows so getQueryResults can page them.
type Job struct {
	Config      map[string]any `json:"config"`
	Stats       map[string]any `json:"stats"`
	ErrorResult *jobError      `json:"errorResult,omitempty"`
	Dest        *tableRef      `json:"dest,omitempty"`
	Project     string         `json:"project"`
	ID          string         `json:"id"`
	Location    string         `json:"location"`
	RequestID   string         `json:"requestId,omitempty"`
	Type        string         `json:"type"`
	State       string         `json:"state"`
	User        string         `json:"user"`
	Schema      []bqsql.Field  `json:"schema,omitempty"`
	Rows        [][]any        `json:"rows,omitempty"`
	Created     int64          `json:"created"`
	Started     int64          `json:"started"`
	Ended       int64          `json:"ended"`
	NumDML      int64          `json:"numDml"`
	DryRun      bool           `json:"dryRun,omitempty"`
	HasResult   bool           `json:"hasResult,omitempty"`
}

type jobError struct {
	Reason   string `json:"reason"`
	Message  string `json:"message"`
	Location string `json:"location,omitempty"`
}

type tableRef struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
	TableID   string `json:"tableId"`
}
