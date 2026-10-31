package dmsengine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var errNoStreamARN = errors.New("KinesisSettings.StreamArn is required")

type kinesisOptions struct {
	StreamArn                   string `json:"StreamArn"`
	MessageFormat               string `json:"MessageFormat"`
	IncludeControlDetails       bool   `json:"IncludeControlDetails"`
	IncludeNullAndEmpty         bool   `json:"IncludeNullAndEmpty"`
	IncludePartitionValue       bool   `json:"IncludePartitionValue"`
	PartitionIncludeSchemaTable bool   `json:"PartitionIncludeSchemaTable"`
}

type kinesisTarget struct {
	store  StreamStore
	now    func() time.Time
	stream string
	opts   kinesisOptions
}

func newKinesisTarget(ep EndpointConfig, store StreamStore) (Target, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: Kinesis", ErrNotConfigured)
	}

	var o kinesisOptions
	if ep.KinesisSettings != "" {
		if err := json.Unmarshal([]byte(ep.KinesisSettings), &o); err != nil {
			return nil, fmt.Errorf("invalid KinesisSettings: %w", err)
		}
	}

	if o.StreamArn == "" {
		return nil, errNoStreamARN
	}

	name := o.StreamArn[strings.LastIndex(o.StreamArn, "/")+1:]

	return &kinesisTarget{store: store, opts: o, stream: name, now: time.Now}, nil
}

func (t *kinesisTarget) Ping(ctx context.Context) error { return t.store.CheckStream(ctx, t.stream) }

func (t *kinesisTarget) Close() error { return nil }

func (t *kinesisTarget) EndLoad(context.Context, TargetTable) error { return nil }

func (t *kinesisTarget) Commit(context.Context) error { return nil }

func (t *kinesisTarget) Prepare(ctx context.Context, tt TargetTable, _ PrepMode) error {
	if !t.opts.IncludeControlDetails {
		return nil
	}

	cols := map[string]any{}
	for i, c := range tt.Columns {
		cols[c.Name] = map[string]any{"type": strings.ToUpper(c.Native), "nullable": c.Nullable, "position": i + 1}
	}

	def := map[string]any{"columns": cols, "primary-key": tt.PK}

	return t.send(ctx, tt, "control", "create-table", map[string]any{"table-def": def}, t.now())
}

func (t *kinesisTarget) value(v any, k Kind) any {
	if b, ok := v.([]byte); ok {
		return fmt.Sprintf("%X", b)
	}

	x, ok := v.(string)
	if !ok {
		return v
	}

	if isNumeric(k) {
		if _, err := strconv.ParseFloat(x, 64); err == nil {
			return json.Number(x)
		}
	}

	if k == KindBool {
		return x == "1" || x == "t" || strings.EqualFold(x, "true")
	}

	return x
}

func (t *kinesisTarget) dataOf(tt TargetTable, row Row) map[string]any {
	out := make(map[string]any, len(row))

	for i, c := range tt.Columns {
		v := row[i]
		if !t.opts.IncludeNullAndEmpty && (v == nil || v == "") {
			continue
		}

		out[c.Name] = t.value(v, c.Kind)
	}

	return out
}

func (t *kinesisTarget) Load(ctx context.Context, tt TargetTable, rows []Row) error {
	for _, r := range rows {
		if err := t.send(ctx, tt, "data", "load", t.dataOf(tt, r), t.now()); err != nil {
			return err
		}
	}

	return nil
}

func (t *kinesisTarget) Apply(ctx context.Context, tt TargetTable, c Change) error {
	row := c.After
	if c.Op == OpDelete {
		row = c.Before
	}

	return t.send(ctx, tt, "data", string(c.Op), t.dataOf(tt, row), c.Time)
}

func (t *kinesisTarget) send(
	ctx context.Context, tt TargetTable, recType, op string, data map[string]any, ts time.Time,
) error {
	if ts.IsZero() {
		ts = t.now()
	}

	pk := tt.Schema + "." + tt.Name
	meta := map[string]any{
		tTimestamp:           ts.UTC().Format("2006-01-02T15:04:05.000000Z"),
		"record-type":        recType,
		"operation":          op,
		"partition-key-type": "schema-table",
		"schema-name":        tt.Schema,
		"table-name":         tt.Name,
	}

	body, err := t.encode(data, meta)
	if err != nil {
		return err
	}

	return t.store.PutRecord(ctx, t.stream, pk, body)
}

type recordOut struct {
	Data     map[string]any `json:"data"`
	Metadata map[string]any `json:"metadata"`
}

func (t *kinesisTarget) encode(data, meta map[string]any) ([]byte, error) {
	var buf bytes.Buffer

	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)

	if !strings.EqualFold(t.opts.MessageFormat, "json-unformatted") {
		enc.SetIndent("", "  ")
	}

	if err := enc.Encode(recordOut{Data: data, Metadata: meta}); err != nil {
		return nil, err
	}

	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}
