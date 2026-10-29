// Package deltalog replays a Delta Lake transaction log (JSON commits and
// checkpoint Parquet files) into the table's active file list and schema
// (https://github.com/delta-io/delta/blob/master/PROTOCOL.md).
package deltalog

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

const (
	logDir               = "_delta_log/"
	lastCheckpoint       = "_last_checkpoint"
	maxReaderVersion     = 3
	columnMappingModeKey = "delta.columnMapping.mode"
	physicalNameKey      = "delta.columnMapping.physicalName"
	columnMappingIDKey   = "delta.columnMapping.id"
	jsonSuffix           = ".json"
	checkpointMarker     = ".checkpoint."
	parquetSuffix        = ".parquet"
	maxLogLineBytes      = 64 << 20
	scanBufBytes         = 64 << 10
)

var (
	// ErrNotDeltaTable is returned when the location has no transaction log.
	ErrNotDeltaTable = errors.New("not a Delta Lake table")
	// ErrBadLog is returned for a transaction log that cannot be replayed.
	ErrBadLog = errors.New("invalid Delta Lake transaction log")
	// ErrUnsupported is returned for protocol features this reader does not implement.
	ErrUnsupported = errors.New("unsupported Delta Lake feature")
)

// Storage reads objects under the table location; keys are relative to it.
type Storage interface {
	List(ctx context.Context, prefix string) ([]string, error)
	Get(ctx context.Context, key string) ([]byte, error)
}

// Column is one logical table column.
type Column struct {
	Name         string
	Type         string
	PhysicalName string
	ID           int
}

// AddFile is an active data file.
type AddFile struct {
	PartitionValues map[string]*string
	Path            string
	Size            int64
}

// Snapshot is the table state at a log version.
type Snapshot struct {
	ColumnMapping    string
	Columns          []Column
	PartitionColumns []string
	Files            []AddFile
	Version          int64
}

type action struct {
	Add      *addAction      `json:"add"`
	Remove   *removeAction   `json:"remove"`
	MetaData *metaDataAction `json:"metaData"`
	Protocol *protocolAction `json:"protocol"`
}

type addAction struct {
	PartitionValues map[string]*string `json:"partitionValues"`
	Path            string             `json:"path"`
	DeletionVector  json.RawMessage    `json:"deletionVector"`
	Size            int64              `json:"size"`
}

type removeAction struct {
	Path string `json:"path"`
}

type metaDataAction struct {
	Configuration    map[string]string `json:"configuration"`
	SchemaString     string            `json:"schemaString"`
	PartitionColumns []string          `json:"partitionColumns"`
}

type protocolAction struct {
	ReaderFeatures   []string `json:"readerFeatures"`
	MinReaderVersion int      `json:"minReaderVersion"`
}

type state struct {
	files   map[string]AddFile
	meta    *metaDataAction
	order   []string
	version int64
}

// Load replays the log to its newest version.
func Load(ctx context.Context, st Storage) (*Snapshot, error) {
	keys, err := st.List(ctx, logDir)
	if err != nil {
		return nil, err
	}

	commits := map[int64]string{}
	checkpoints := map[int64][]string{}

	for _, k := range keys {
		name := strings.TrimPrefix(k, logDir)
		if strings.Contains(name, "/") {
			continue
		}

		v, kind := classify(name)

		switch kind {
		case kindCommit:
			commits[v] = k
		case kindCheckpoint:
			checkpoints[v] = append(checkpoints[v], k)
		}
	}

	if len(commits) == 0 && len(checkpoints) == 0 {
		return nil, fmt.Errorf("%w: no _delta_log commits found", ErrNotDeltaTable)
	}

	s := &state{files: map[string]AddFile{}, version: -1}

	if err = s.loadCheckpoint(ctx, st, checkpoints); err != nil {
		return nil, err
	}

	versions := make([]int64, 0, len(commits))

	for v := range commits {
		if v > s.version {
			versions = append(versions, v)
		}
	}

	slices.Sort(versions)

	for _, v := range versions {
		if v != s.version+1 {
			return nil, fmt.Errorf("%w: missing commit %d", ErrBadLog, s.version+1)
		}

		body, gerr := st.Get(ctx, commits[v])
		if gerr != nil {
			return nil, gerr
		}

		if err = s.applyCommit(body); err != nil {
			return nil, fmt.Errorf("commit %d: %w", v, err)
		}

		s.version = v
	}

	return s.snapshot()
}

const (
	kindOther = iota
	kindCommit
	kindCheckpoint
)

func classify(name string) (int64, int) {
	if rest, ok := strings.CutSuffix(name, jsonSuffix); ok {
		if v, err := strconv.ParseInt(rest, 10, 64); err == nil {
			return v, kindCommit
		}

		return 0, kindOther
	}

	head, _, ok := strings.Cut(name, checkpointMarker)
	if !ok || !strings.HasSuffix(name, parquetSuffix) {
		return 0, kindOther
	}

	v, err := strconv.ParseInt(head, 10, 64)
	if err != nil {
		return 0, kindOther
	}

	return v, kindCheckpoint
}

func (s *state) loadCheckpoint(ctx context.Context, st Storage, cps map[int64][]string) error {
	var best int64 = -1

	if raw, err := st.Get(ctx, logDir+lastCheckpoint); err == nil {
		var lc struct {
			Version int64 `json:"version"`
		}

		if json.Unmarshal(raw, &lc) == nil {
			if _, ok := cps[lc.Version]; ok {
				best = lc.Version
			}
		}
	}

	if best < 0 {
		for v := range cps {
			best = max(best, v)
		}
	}

	if best < 0 {
		return nil
	}

	files := cps[best]
	sort.Strings(files)

	for _, k := range files {
		body, err := st.Get(ctx, k)
		if err != nil {
			return err
		}

		if err = s.applyCheckpoint(body); err != nil {
			return fmt.Errorf("checkpoint %d: %w", best, err)
		}
	}

	s.version = best

	return nil
}

func (s *state) applyCheckpoint(body []byte) error {
	pf, err := parquetio.Open(body)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrBadLog, err)
	}

	err = pf.ForEach([]string{"add", "metaData", "protocol", "remove"}, func(vals []any) error {
		var a action

		for i, v := range vals {
			if v == nil {
				continue
			}

			raw, merr := json.Marshal(v)
			if merr != nil {
				return merr
			}

			part := map[int]string{0: "add", 1: "metaData", 2: "protocol", 3: "remove"}[i]
			if uerr := json.Unmarshal([]byte(`{"`+part+`":`+string(raw)+`}`), &a); uerr != nil {
				return fmt.Errorf("%w: %w", ErrBadLog, uerr)
			}
		}

		return s.apply(a)
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrBadLog, err)
	}

	return nil
}

func (s *state) applyCommit(body []byte) error {
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 0, scanBufBytes), maxLogLineBytes)

	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}

		var a action
		if err := json.Unmarshal(line, &a); err != nil {
			return fmt.Errorf("%w: %w", ErrBadLog, err)
		}

		if err := s.apply(a); err != nil {
			return err
		}
	}

	return sc.Err()
}

func (s *state) apply(a action) error {
	switch {
	case a.Protocol != nil:
		if a.Protocol.MinReaderVersion > maxReaderVersion {
			return fmt.Errorf("%w: reader version %d", ErrUnsupported, a.Protocol.MinReaderVersion)
		}
	case a.MetaData != nil:
		s.meta = a.MetaData
	case a.Add != nil:
		if len(a.Add.DeletionVector) > 0 && string(a.Add.DeletionVector) != "null" {
			return fmt.Errorf("%w: deletion vectors", ErrUnsupported)
		}

		p := decodePath(a.Add.Path)
		if _, seen := s.files[p]; !seen {
			s.order = append(s.order, p)
		}

		s.files[p] = AddFile{Path: p, Size: a.Add.Size, PartitionValues: a.Add.PartitionValues}
	case a.Remove != nil:
		delete(s.files, decodePath(a.Remove.Path))
	}

	return nil
}

func decodePath(p string) string {
	if u, err := url.PathUnescape(p); err == nil {
		return u
	}

	return p
}

func (s *state) snapshot() (*Snapshot, error) {
	if s.meta == nil {
		return nil, fmt.Errorf("%w: no metaData action", ErrBadLog)
	}

	cols, err := parseSchema(s.meta.SchemaString, s.meta.Configuration[columnMappingModeKey])
	if err != nil {
		return nil, err
	}

	snap := &Snapshot{
		Version: s.version, Columns: cols, PartitionColumns: s.meta.PartitionColumns,
		ColumnMapping: s.meta.Configuration[columnMappingModeKey],
	}

	for _, p := range s.order {
		if f, ok := s.files[p]; ok {
			snap.Files = append(snap.Files, f)
		}
	}

	return snap, nil
}

type schemaField struct {
	Metadata map[string]any  `json:"metadata"`
	Name     string          `json:"name"`
	Type     json.RawMessage `json:"type"`
}

func parseSchema(s, mode string) ([]Column, error) {
	var st struct {
		Fields []schemaField `json:"fields"`
	}

	if err := json.Unmarshal([]byte(s), &st); err != nil {
		return nil, fmt.Errorf("%w: schemaString: %w", ErrBadLog, err)
	}

	cols := make([]Column, 0, len(st.Fields))

	for _, f := range st.Fields {
		t, err := hiveType(f.Type)
		if err != nil {
			return nil, err
		}

		c := Column{Name: f.Name, Type: t, PhysicalName: f.Name}

		if mode == "name" || mode == "id" {
			if pn, ok := f.Metadata[physicalNameKey].(string); ok {
				c.PhysicalName = pn
			}

			if id, ok := f.Metadata[columnMappingIDKey].(float64); ok {
				c.ID = int(id)
			}
		}

		cols = append(cols, c)
	}

	return cols, nil
}

func hiveType(raw json.RawMessage) (string, error) {
	var prim string
	if err := json.Unmarshal(raw, &prim); err == nil {
		return hivePrimitive(prim)
	}

	var n struct {
		Type        string          `json:"type"`
		ElementType json.RawMessage `json:"elementType"`
		KeyType     json.RawMessage `json:"keyType"`
		ValueType   json.RawMessage `json:"valueType"`
		Fields      []schemaField   `json:"fields"`
	}

	if err := json.Unmarshal(raw, &n); err != nil {
		return "", fmt.Errorf("%w: type %s", ErrBadLog, raw)
	}

	switch n.Type {
	case "array":
		e, err := hiveType(n.ElementType)

		return "array<" + e + ">", err
	case "map":
		k, err := hiveType(n.KeyType)
		if err != nil {
			return "", err
		}

		v, err := hiveType(n.ValueType)

		return "map<" + k + "," + v + ">", err
	case "struct":
		parts := make([]string, 0, len(n.Fields))

		for _, f := range n.Fields {
			t, err := hiveType(f.Type)
			if err != nil {
				return "", err
			}

			parts = append(parts, f.Name+":"+t)
		}

		return "struct<" + strings.Join(parts, ",") + ">", nil
	default:
		return "", fmt.Errorf("%w: type %s", ErrUnsupported, n.Type)
	}
}

func hivePrimitive(prim string) (string, error) {
	switch prim {
	case "long":
		return "bigint", nil
	case "integer":
		return "int", nil
	case "short":
		return "smallint", nil
	case "byte":
		return "tinyint", nil
	case "timestamp_ntz":
		return "timestamp", nil
	case "string", "float", "double", "boolean", "binary", "date", "timestamp":
		return prim, nil
	default:
		if strings.HasPrefix(prim, "decimal") {
			return prim, nil
		}

		return "", fmt.Errorf("%w: type %s", ErrUnsupported, prim)
	}
}
