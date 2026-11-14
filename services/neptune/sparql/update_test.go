package sparql_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/sparql"
)

var errFetch = errors.New("fetch failed")

func TestUpdateOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		updates      string
		check        string
		wantIns      int
		wantDel      int
		wantAsk      bool
		wantAskValue bool
	}{
		{"insert data", `INSERT DATA { <urn:a> <urn:p> "x" }`, `ASK { <urn:a> <urn:p> "x" }`, 1, 0, true, true},
		{
			"insert data duplicate counts once",
			`INSERT DATA { <urn:a> <urn:p> "x", "x" }`,
			`ASK { ?s ?p ?o }`,
			1,
			0,
			true,
			true,
		},
		{
			"delete data",
			`INSERT DATA { <urn:a> <urn:p> 1 } ; DELETE DATA { <urn:a> <urn:p> 1 }`,
			`ASK { ?s ?p ?o }`,
			1,
			1,
			true,
			false,
		},
		{"delete where", `INSERT DATA { <urn:a> <urn:p> 1 . <urn:a> <urn:q> 2 } ; DELETE WHERE { <urn:a> <urn:p> ?o }`,
			`ASK { <urn:a> <urn:q> 2 }`, 2, 1, true, true},
		{
			"modify with",
			`INSERT DATA { GRAPH <urn:g> { <urn:a> <urn:p> 1 } } ; WITH <urn:g> DELETE ` +
				`{ ?s <urn:p> ?o } INSERT { ?s <urn:p> 2 } WHERE { ?s <urn:p> ?o }`,
			`ASK { GRAPH <urn:g> { <urn:a> <urn:p> 2 } }`,
			2,
			1,
			true,
			true,
		},
		{
			"modify using",
			`INSERT DATA { GRAPH <urn:g> { <urn:a> <urn:p> 1 } } ; INSERT { <urn:b> ` +
				`<urn:p> ?o } USING <urn:g> WHERE { <urn:a> <urn:p> ?o }`,
			`ASK { <urn:b> <urn:p> 1 }`,
			2,
			0,
			true,
			true,
		},
		{
			"insert with fresh blank per solution",
			`INSERT DATA { <urn:a> <urn:p> 1, 2 } ; INSERT { _:n <urn:from> ?o } WHERE { <urn:a> <urn:p> ?o }`,
			`ASK { ?x <urn:from> 1 . ?y <urn:from> 2 FILTER(?x != ?y) }`,
			4,
			0,
			true,
			true,
		},
		{"clear default", `INSERT DATA { <urn:a> <urn:p> 1 . GRAPH <urn:g> { <urn:b> <urn:p> 1 } } ; CLEAR DEFAULT`,
			`ASK { <urn:a> <urn:p> 1 }`, 2, 1, true, false},
		{
			"clear named keeps default",
			`INSERT DATA { <urn:a> <urn:p> 1 . GRAPH <urn:g> { <urn:b> <urn:p> 1 } } ; CLEAR NAMED`,
			`ASK { <urn:a> <urn:p> 1 }`,
			2,
			1,
			true,
			true,
		},
		{"drop all", `INSERT DATA { <urn:a> <urn:p> 1 . GRAPH <urn:g> { <urn:b> <urn:p> 1 } } ; DROP ALL`,
			`ASK { ?s ?p ?o }`, 2, 2, true, false},
		{"create graph is accepted", `CREATE GRAPH <urn:new>`, `ASK { ?s ?p ?o }`, 0, 0, true, false},
		{"move", `INSERT DATA { GRAPH <urn:a> { <urn:s> <urn:p> 1 } } ; MOVE <urn:a> TO <urn:b>`,
			`ASK { GRAPH <urn:b> { <urn:s> <urn:p> 1 } }`, 2, 1, true, true},
		{"add keeps source", `INSERT DATA { GRAPH <urn:a> { <urn:s> <urn:p> 1 } } ; ADD <urn:a> TO <urn:b>`,
			`ASK { GRAPH <urn:a> { <urn:s> <urn:p> 1 } }`, 2, 0, true, true},
		{
			"datetime stored as utc",
			`INSERT DATA { <urn:a> <urn:p> "2020-01-01T10:00:00+02:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> }`,
			`ASK { <urn:a> <urn:p> "2020-01-01T08:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime> }`,
			1,
			0,
			true,
			true,
		},
		{
			"load",
			`LOAD <s3://b/k.nt> INTO GRAPH <urn:l>`,
			`ASK { GRAPH <urn:l> { <urn:a> <urn:p> <urn:b> } }`,
			1,
			0,
			true,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := sparql.NewStore()
			res, err := s.Update(context.Background(), tt.updates, sparql.UpdateOptions{
				Fetch: func(context.Context, string) ([]byte, error) { return []byte("<urn:a> <urn:p> <urn:b> .\n"), nil },
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantIns, res.Inserted, "inserted")
			assert.Equal(t, tt.wantDel, res.Deleted, "deleted")

			r, err := s.Query(context.Background(), tt.check)
			require.NoError(t, err)
			assert.Equal(t, tt.wantAskValue, r.Bool)
		})
	}
}

func TestUpdateRollsBackOnFailure(t *testing.T) {
	t.Parallel()

	s := sparql.NewStore()
	_, err := s.Update(context.Background(), `INSERT DATA { <urn:a> <urn:p> 1 }`, sparql.UpdateOptions{})
	require.NoError(t, err)

	_, err = s.Update(context.Background(),
		`DELETE WHERE { <urn:a> <urn:p> 1 } ; INSERT DATA { <urn:b> <urn:p> 2 } ; LOAD <s3://b/k>`,
		sparql.UpdateOptions{Fetch: func(context.Context, string) ([]byte, error) { return nil, errFetch }})
	require.ErrorIs(t, err, errFetch)

	assert.Equal(t, 1, s.Len())

	r, err := s.Query(context.Background(), `ASK { <urn:a> <urn:p> 1 }`)
	require.NoError(t, err)
	assert.True(t, r.Bool)
}

func TestUpdateAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		update     string
		write, del bool
	}{
		{"insert data", `INSERT DATA { <urn:a> <urn:p> 1 }`, true, false},
		{"delete data", `DELETE DATA { <urn:a> <urn:p> 1 }`, false, true},
		{"delete insert", `DELETE { ?s ?p ?o } INSERT { ?s ?p 1 } WHERE { ?s ?p ?o }`, true, true},
		{"insert only where", `INSERT { ?s <urn:q> 1 } WHERE { ?s ?p ?o }`, true, false},
		{"clear", `CLEAR ALL`, false, true},
		{"load", `LOAD <s3://b/k>`, true, false},
		{"move", `MOVE <urn:a> TO <urn:b>`, true, true},
		{"copy", `COPY <urn:a> TO <urn:b>`, true, true},
		{"add", `ADD <urn:a> TO <urn:b>`, true, false},
		{"create", `CREATE GRAPH <urn:a>`, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			u, err := sparql.ParseUpdate(tt.update)
			require.NoError(t, err)

			w, d := u.Access()
			assert.Equal(t, tt.write, w, "write")
			assert.Equal(t, tt.del, d, "delete")
		})
	}
}

func TestUpdateSyntaxErrors(t *testing.T) {
	t.Parallel()

	tests := []struct{ name, update string }{
		{"empty", ``},
		{"variable in insert data", `INSERT DATA { ?s <urn:p> 1 }`},
		{"unknown verb", `UPSERT { }`},
		{"missing where", `DELETE { ?s ?p ?o }`},
		{"trailing", `CLEAR ALL garbage`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := sparql.ParseUpdate(tt.update)
			require.Error(t, err)
		})
	}
}
