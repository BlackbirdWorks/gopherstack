package dmsengine_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/dmsengine"
)

func intTable() *fakeSource {
	items := dmsengine.Table{Schema: "app", Name: "items", PK: []string{"id"}, Columns: []dmsengine.Column{
		{Name: "id", Native: "int", Kind: dmsengine.KindInt},
		{Name: "name", Native: "varchar(20)", Kind: dmsengine.KindString, Nullable: true},
	}}

	return &fakeSource{
		tables: []dmsengine.Table{items},
		rows: map[string][]dmsengine.Row{
			"app.items": {{"1", "alpha"}, {"2", "beta"}, {"30", "gamma"}},
		},
		changes: make(chan dmsengine.Change, 16),
	}
}

const filteredWithColumn = `{"rules":[
	{"rule-type":"selection","rule-id":"1","rule-name":"1","object-locator":{"schema-name":"app","table-name":"items"},
		"rule-action":"include","filters":[{"filter-type":"source","column-name":"id",
		"filter-conditions":[{"filter-operator":"ste","value":"10"}]}]},
	{"rule-type":"transformation","rule-id":"2","rule-name":"2","rule-action":"add-column","rule-target":"column",
		"object-locator":{"schema-name":"app","table-name":"items"},"value":"label",
		"expression":"upper($name)||'-'||$id","data-type":{"type":"string","length":30}}]}`

func TestTaskFilterAndAddColumnOnLoadAndCDC(t *testing.T) {
	t.Parallel()

	src, tgt, log := intTable(), newFakeTarget(), newProgressLog()
	mappings, err := dmsengine.ParseMappings(filteredWithColumn)
	require.NoError(t, err)

	task := dmsengine.NewTask(dmsengine.TaskConfig{
		Source: src, Target: tgt, MigrationType: dmsengine.MigrationFullLoadAndCDC, Mappings: mappings,
		Settings: defaultSettings(t), SourceFamily: dmsengine.FamilyMySQL, OnProgress: log.push,
	})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan dmsengine.Result, 1)

	go func() { done <- task.Run(ctx) }()

	ins := func(id, name string) dmsengine.Change {
		return dmsengine.Change{Schema: "app", Table: "items", Op: dmsengine.OpInsert, After: dmsengine.Row{id, name}}
	}
	upd := func(bid, bname, aid, aname string) dmsengine.Change {
		return dmsengine.Change{
			Schema: "app", Table: "items", Op: dmsengine.OpUpdate,
			Before: dmsengine.Row{bid, bname}, After: dmsengine.Row{aid, aname},
		}
	}

	src.changes <- ins("5", "five")
	src.changes <- ins("50", "fifty")
	src.changes <- upd("5", "five", "5", "FIVE")
	src.changes <- upd("5", "five", "99", "moved-out")
	src.changes <- upd("60", "x", "3", "moved-in")
	src.changes <- upd("70", "x", "71", "never")
	src.changes <- dmsengine.Change{
		Schema: "app", Table: "items", Op: dmsengine.OpDelete, Before: dmsengine.Row{"80", "x"},
	}
	src.changes <- dmsengine.Change{Commit: true, Position: "p"}
	log.waitFor(t, func(p dmsengine.Progress) bool { return p.Checkpoint == "p" })
	cancel()
	<-done

	assert.Equal(t, []dmsengine.Row{{"1", "alpha", "ALPHA-1"}, {"2", "beta", "BETA-2"}}, tgt.loaded["app.items"])

	type applied struct {
		op  dmsengine.Op
		row dmsengine.Row
	}

	got := make([]applied, 0, len(tgt.applied))

	for _, c := range tgt.applied {
		img := c.After
		if c.Op == dmsengine.OpDelete {
			img = c.Before
		}

		got = append(got, applied{c.Op, img})
	}

	assert.Equal(t, []applied{
		{dmsengine.OpInsert, dmsengine.Row{"5", "five", "FIVE-5"}},
		{dmsengine.OpUpdate, dmsengine.Row{"5", "FIVE", "FIVE-5"}},
		{dmsengine.OpDelete, dmsengine.Row{"5", "five", "FIVE-5"}},
		{dmsengine.OpInsert, dmsengine.Row{"3", "moved-in", "MOVED-IN-3"}},
	}, got)
}

func TestTaskRuleErrorsFailTheTask(t *testing.T) {
	t.Parallel()

	addCol := func(expr string) string {
		return `{"rules":[{"rule-type":"selection","rule-id":"1","rule-name":"1",` +
			`"object-locator":{"schema-name":"app","table-name":"items"},"rule-action":"include"},
		{"rule-type":"transformation","rule-id":"2","rule-name":"2","rule-action":"add-column","rule-target":"column",
		"object-locator":{"schema-name":"app","table-name":"items"},"value":"x","expression":"` + expr + `",
		"data-type":{"type":"string"}}]}`
	}

	tests := []struct {
		name   string
		doc    string
		want   string
		target dmsengine.Family
	}{
		{name: "unsupported function", doc: addCol("soundex($name)"), target: dmsengine.FamilyOther, want: "soundex"},
		{name: "filter column missing", doc: `{"rules":[{"rule-type":"selection","rule-id":"1","rule-name":"1",` +
			`"object-locator":{"schema-name":"app","table-name":"items"},"rule-action":"include",` +
			`"filters":[{"filter-type":"source","column-name":"nope",` +
			`"filter-conditions":[{"filter-operator":"null"}]}]}]}`, target: dmsengine.FamilyOther, want: "nope"},
		{name: "before image on a SQL target", doc: `{"rules":[{"rule-type":"selection","rule-id":"1",` +
			`"rule-name":"1","object-locator":{"schema-name":"app","table-name":"items"},"rule-action":"include"},
			{"rule-type":"transformation","rule-id":"2","rule-name":"2","rule-action":"add-before-image-columns",
			"rule-target":"column","object-locator":{"schema-name":"app","table-name":"items"}}]}`,
			target: dmsengine.FamilyPostgres, want: "Kinesis and S3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mappings, err := dmsengine.ParseMappings(tt.doc)
			require.NoError(t, err)

			task := dmsengine.NewTask(dmsengine.TaskConfig{
				Source: intTable(), Target: newFakeTarget(), MigrationType: dmsengine.MigrationFullLoad,
				Mappings: mappings, Settings: defaultSettings(t), SourceFamily: dmsengine.FamilyMySQL,
				TargetFamily: tt.target,
			})

			res := task.Run(t.Context())
			require.ErrorIs(t, res.Err, dmsengine.ErrMapping)
			assert.Equal(t, dmsengine.StopFatalError, res.Reason)
			assert.Contains(t, res.Err.Error(), tt.want)
		})
	}
}

func TestTaskBeforeImageOnKinesisLikeTarget(t *testing.T) {
	t.Parallel()

	mappings, err := dmsengine.ParseMappings(
		`{"rules":[{"rule-type":"selection","rule-id":"1","rule-name":"1","object-locator":{"schema-name":"app",` +
			`"table-name":"items"},"rule-action":"include"},
		{"rule-type":"transformation","rule-id":"2","rule-name":"2","rule-action":"add-before-image-columns",
		"rule-target":"column","object-locator":{"schema-name":"app","table-name":"items"}}]}`,
	)
	require.NoError(t, err)

	src, tgt, log := intTable(), newFakeTarget(), newProgressLog()
	task := dmsengine.NewTask(dmsengine.TaskConfig{
		Source: src, Target: tgt, MigrationType: dmsengine.MigrationFullLoadAndCDC, Mappings: mappings,
		Settings: defaultSettings(t), SourceFamily: dmsengine.FamilyMySQL, TargetFamily: dmsengine.FamilyKinesis,
		OnProgress: log.push,
	})

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan dmsengine.Result, 1)

	go func() { done <- task.Run(ctx) }()

	src.changes <- dmsengine.Change{
		Schema: "app", Table: "items", Op: dmsengine.OpUpdate,
		Before: dmsengine.Row{"1", "alpha"}, After: dmsengine.Row{"1", "ALPHA"},
	}
	src.changes <- dmsengine.Change{Commit: true, Position: "p"}
	log.waitFor(t, func(p dmsengine.Progress) bool { return p.Checkpoint == "p" })
	cancel()
	require.NoError(t, (<-done).Err)

	assert.Equal(t, dmsengine.Row{"1", "alpha", nil}, tgt.loaded["app.items"][0])
	require.Len(t, tgt.applied, 1)
	assert.Equal(t, dmsengine.Row{"1", "ALPHA", "1"}, tgt.applied[0].After)
}
