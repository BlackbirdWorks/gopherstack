package cosmosdb_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

func TestSearchFunctions_Scalar(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{name: "cosine_identical", query: "SELECT VALUE VectorDistance([1,2,3],[1,2,3]) FROM c", want: "1"},
		{name: "cosine_orthogonal", query: "SELECT VALUE VectorDistance([1,0],[0,1]) FROM c", want: "0"},
		{
			name:  "dot",
			query: "SELECT VALUE VectorDistance([1,2],[3,4], true, {distanceFunction:'dotproduct'}) FROM c",
			want:  "11",
		},
		{
			name:  "euclid",
			query: "SELECT VALUE VectorDistance([0,0],[3,4], false, {distanceFunction:'euclidean'}) FROM c",
			want:  "5",
		},
		{name: "dim_mismatch", query: "SELECT VALUE IS_DEFINED(VectorDistance([1,2],[1,2,3])) FROM c", want: "false"},
		{
			name:  "bad_function",
			query: "SELECT VALUE IS_DEFINED(VectorDistance([1],[1], false, {distanceFunction:'x'})) FROM c",
			want:  "false",
		},
		{
			name:  "zero_vector_cosine",
			query: "SELECT VALUE IS_DEFINED(VectorDistance([0,0],[1,1])) FROM c",
			want:  "false",
		},
		{name: "non_numeric", query: "SELECT VALUE IS_DEFINED(VectorDistance(['a'],[1])) FROM c", want: "false"},
		{name: "ft_contains", query: "SELECT VALUE FullTextContains('The Quick fox!', 'QUICK') FROM c", want: "true"},
		{name: "ft_absent", query: "SELECT VALUE FullTextContains('The Quick fox!', 'dog') FROM c", want: "false"},
		{name: "ft_all", query: "SELECT VALUE FullTextContainsAll('a b c', 'a', 'c') FROM c", want: "true"},
		{name: "ft_all_missing", query: "SELECT VALUE FullTextContainsAll('a b c', 'a', 'z') FROM c", want: "false"},
		{name: "ft_any", query: "SELECT VALUE FullTextContainsAny('a b c', 'z', 'b') FROM c", want: "true"},
		{name: "ft_non_string", query: "SELECT VALUE IS_DEFINED(FullTextContains(5, 'a')) FROM c", want: "false"},
		{name: "ft_score_zero", query: "SELECT VALUE FullTextScore('hello world', 'zzz') FROM c", want: "0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows, err := cosmosdb.ExecuteQuery(
				tt.query,
				nil,
				[]cosmosdb.DocumentInfo{{ID: "x", Body: map[string]any{"id": "x"}}},
			)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			assert.Equal(t, tt.want, jsonString(t, rows[0]))
		})
	}
}

func TestSearchFunctions_ParseErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
	}{
		{name: "rank_plain_property", query: "SELECT * FROM c ORDER BY RANK c.x"},
		{name: "rank_with_other_key", query: "SELECT * FROM c ORDER BY RANK FullTextScore(c.t, 'a'), c.id"},
		{name: "rrf_bad_operand", query: "SELECT * FROM c ORDER BY RANK RRF(c.a, c.b)"},
		{
			name:  "rrf_weights_mismatch",
			query: "SELECT * FROM c ORDER BY RANK RRF(FullTextScore(c.t,'a'), FullTextScore(c.u,'b'), [1])",
		},
		{name: "vector_arity", query: "SELECT VectorDistance([1]) FROM c"},
		{name: "contains_arity", query: "SELECT FullTextContains(c.t) FROM c"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := cosmosdb.ExecuteQuery(tt.query, nil, nil)
			require.ErrorIs(t, err, cosmosdb.ErrQueryParse)
		})
	}
}

func jsonString(t *testing.T, v any) string {
	t.Helper()

	b, err := json.Marshal(v)
	require.NoError(t, err)

	return string(b)
}
