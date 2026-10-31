package dmsengine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDecoded(t *testing.T) {
	t.Parallel()

	table := Table{Schema: "public", Name: "items", Columns: []Column{
		{Name: "id", Native: "integer", Kind: KindInt}, {Name: "name", Native: "text"}, {Name: "blob", Native: "bytea", Kind: KindBytes},
	}}
	lookup := func(s, n string) (Table, bool) { return table, s == "public" && n == "items" }

	tests := []struct {
		name   string
		data   string
		op     Op
		before Row
		after  Row
		commit bool
		skip   bool
	}{
		{name: "insert", data: `table public.items: INSERT: id[integer]:1 name[text]:'it''s alpha' blob[bytea]:'\x0102'`,
			op: OpInsert, after: Row{"1", "it's alpha", []byte{1, 2}}},
		{name: "insert null", data: `table public.items: INSERT: id[integer]:2 name[text]:null`, op: OpInsert, after: Row{"2", nil, nil}},
		{name: "update with old key", data: `table public.items: UPDATE: old-key: id[integer]:1 new-tuple: id[integer]:5 name[text]:'x'`,
			op: OpUpdate, before: Row{"1", nil, nil}, after: Row{"5", "x", nil}},
		{name: "update without old key", data: `table public.items: UPDATE: id[integer]:1 name[text]:'y'`,
			op: OpUpdate, after: Row{"1", "y", nil}},
		{name: "delete", data: `table public.items: DELETE: id[integer]:1`, op: OpDelete, before: Row{"1", nil, nil}},
		{name: "commit", data: `COMMIT 745 (at 2024-01-02 03:04:05.123456+00)`, commit: true},
		{name: "begin skipped", data: `BEGIN 745`, skip: true},
		{name: "untracked table skipped", data: `table public.other: INSERT: id[integer]:1`, skip: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d, ok := parseDecoded("0/16B3748", tt.data, lookup)
			if tt.skip {
				assert.False(t, ok)

				return
			}

			require.True(t, ok)
			assert.Equal(t, tt.commit, d.commit)

			if tt.commit {
				assert.Equal(t, 2024, d.when.Year())

				return
			}

			assert.Equal(t, tt.op, d.change.Op)
			assert.Equal(t, tt.before, d.change.Before)
			assert.Equal(t, tt.after, d.change.After)
		})
	}
}

func TestParseTupleQuotedNames(t *testing.T) {
	t.Parallel()

	got := parseTuple(`"my col"[text]:'a b' tags[text[]]:'{x,y}' n[integer]:7`)
	assert.Equal(t, map[string]any{"my col": "a b", "tags": "{x,y}", "n": "7"}, got)
}

func TestBinlogValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   any
		col  Column
		want any
	}{
		{name: "signed tinyint", in: int8(-3), col: Column{Native: "tinyint"}, want: "-3"},
		{name: "unsigned tinyint wraps", in: int8(-1), col: Column{Native: "tinyint unsigned"}, want: "255"},
		{name: "unsigned int wraps", in: int32(-1), col: Column{Native: "int unsigned"}, want: "4294967295"},
		{name: "enum ordinal", in: int64(2), col: Column{Native: "enum('a','b','c')"}, want: "b"},
		{name: "set mask", in: int64(5), col: Column{Native: "set('x','y','z')"}, want: "x,z"},
		{name: "text bytes", in: []byte("hi"), col: Column{Native: "varchar(5)"}, want: "hi"},
		{name: "blob stays bytes", in: []byte{1}, col: Column{Native: "blob", Kind: KindBytes}, want: []byte{1}},
		{name: "float", in: float32(1.5), col: Column{Native: "float"}, want: "1.5"},
		{name: "null", in: nil, col: Column{Native: "int"}, want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, binlogValue(tt.in, tt.col))
		})
	}
}
