package athena_test

import (
	"testing"

	athenasdk "github.com/aws/aws-sdk-go-v2/service/athena"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

const flagsData = `{"id":1,"ok":true,"n":5,"name":"ann"}
{"id":2,"ok":false,"n":null,"name":"bob"}
`

func flagsClient(t *testing.T) *athenasdk.Client {
	t.Helper()

	return sdkClientFor(t, map[string][]byte{"lake/flags/a.json": []byte(flagsData)},
		map[string]*athena.GlueTableDef{"shop/flags": {
			Name: "flags", Location: "s3://lake/flags/", SerdeLibrary: serdeJSON,
			Columns: []athena.Column{
				{Name: "id", Type: "int"}, {Name: "ok", Type: "boolean"},
				{Name: "n", Type: "int"}, {Name: "name", Type: "string"},
			},
		}})
}

func TestSDK_BooleanResults(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		sql   string
		types []string
		want  [][]string
	}{
		{"comparison", "SELECT 1 < 2, 2 < 1", []string{"boolean", "boolean"}, [][]string{{"true", "false"}}},
		{"literals", "SELECT true, false", []string{"boolean", "boolean"}, [][]string{{"true", "false"}}},
		{"logical", "SELECT true AND false AS a, true OR false AS o, NOT true AS n",
			[]string{"boolean", "boolean", "boolean"}, [][]string{{"false", "true", "false"}}},
		{"boolean column", "SELECT ok FROM flags ORDER BY id", []string{"boolean"}, [][]string{{"true"}, {"false"}}},
		{
			"column comparison",
			"SELECT n > 3 AS big FROM flags ORDER BY id",
			[]string{"boolean"},
			[][]string{{"true"}, {""}},
		},
		{"is null", "SELECT n IS NULL AS missing, id IN (1, 3) AS hit FROM flags ORDER BY id",
			[]string{"boolean", "boolean"}, [][]string{{"false", "true"}, {"true", "false"}}},
		{"between like", "SELECT id BETWEEN 1 AND 1 AS b, name LIKE 'a%' AS l FROM flags ORDER BY id",
			[]string{"boolean", "boolean"}, [][]string{{"true", "true"}, {"false", "false"}}},
		{"case boolean", "SELECT CASE WHEN id = 1 THEN true ELSE false END AS c FROM flags ORDER BY id",
			[]string{"boolean"}, [][]string{{"true"}, {"false"}}},
		{"case not boolean", "SELECT CASE WHEN id = 1 THEN 10 ELSE 20 END AS c FROM flags ORDER BY id",
			[]string{"bigint"}, [][]string{{"10"}, {"20"}}},
		{"function", "SELECT starts_with(name, 'a') AS s, regexp_like(name, 'b') AS r FROM flags ORDER BY id",
			[]string{"boolean", "boolean"}, [][]string{{"true", "false"}, {"false", "true"}}},
		{"cast", "SELECT CAST(1 AS boolean) AS c", []string{"boolean"}, [][]string{{"true"}}},
		{"cte alias", "WITH t AS (SELECT id, n > 3 AS big FROM flags) SELECT big FROM t ORDER BY id",
			[]string{"boolean"}, [][]string{{"true"}, {""}}},
		{"subquery", "SELECT x FROM (SELECT ok AND true AS x FROM flags) ORDER BY x",
			[]string{"boolean"}, [][]string{{"false"}, {"true"}}},
		{"aggregate", "SELECT bool_and(ok) AS a, bool_or(ok) AS o FROM flags",
			[]string{"boolean", "boolean"}, [][]string{{"false", "true"}}},
		{"not boolean", "SELECT id + 1 AS i, name FROM flags ORDER BY id",
			[]string{"bigint", "varchar"}, [][]string{{"2", "ann"}, {"3", "bob"}}},
		{"filtered", "SELECT id FROM flags WHERE ok = true", []string{"integer"}, [][]string{{"1"}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := sdkRun(t, flagsClient(t), tc.sql)
			assert.Equal(t, tc.want, got.rows)
			assert.Equal(t, tc.types, got.types)
		})
	}
}
