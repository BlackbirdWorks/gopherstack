package expr_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/expr"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsReservedWord(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		word string
		want bool
	}{
		{name: "uppercase reserved", word: "STATUS", want: true},
		{name: "lowercase reserved", word: "status", want: true},
		{name: "mixed case reserved", word: "StAtUs", want: true},
		{name: "size is reserved", word: "size", want: true},
		{name: "not reserved", word: "phase", want: false},
		{name: "empty string", word: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, expr.IsReservedWord(tt.word))
		})
	}
}

// parseCondition is a small test helper mirroring the real ConditionExpression
// parse path: lex, parse, then CheckReservedWords -- the same sequence
// services/dynamodb/expressions.go runs for ConditionExpression, FilterExpression,
// and KeyConditionExpression.
func parseCondition(t *testing.T, input string) (expr.Node, error) {
	t.Helper()

	p := expr.NewParser(expr.NewLexer(input))

	node, err := p.ParseCondition()
	if err != nil {
		return nil, err
	}

	return node, expr.CheckReservedWords(node)
}

func TestCheckReservedWords_Condition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		wantWord string
		wantErr  bool
	}{
		{
			name:    "bare non-reserved name",
			input:   "phase = :v",
			wantErr: false,
		},
		{
			name:     "bare reserved word rejected",
			input:    "status = :v",
			wantErr:  true,
			wantWord: "status",
		},
		{
			name:    "escaped reserved word via placeholder is allowed",
			input:   "#status = :v",
			wantErr: false,
		},
		{
			name:     "reserved word in nested document path segment",
			input:    "a.SIZE = :v",
			wantErr:  true,
			wantWord: "SIZE",
		},
		{
			name:    "nested path with placeholder segment is allowed",
			input:   "a.#sz = :v",
			wantErr: false,
		},
		{
			name:     "reserved word as function argument",
			input:    "attribute_exists(status)",
			wantErr:  true,
			wantWord: "status",
		},
		{
			name:     "reserved word on right side of AND",
			input:    "pk = :v AND total > :n",
			wantErr:  true,
			wantWord: "total",
		},
		{
			name:     "reserved word inside BETWEEN",
			input:    "count BETWEEN :lo AND :hi",
			wantErr:  true,
			wantWord: "count",
		},
		{
			name:     "reserved word inside IN candidate list target",
			input:    "name IN (:a, :b)",
			wantErr:  true,
			wantWord: "name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseCondition(t, tt.input)
			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, expr.ErrReservedWord)

			var rwErr *expr.ReservedWordError
			require.ErrorAs(t, err, &rwErr)
			assert.Equal(t, tt.wantWord, rwErr.Word)
			assert.Equal(t,
				"Attribute name is a reserved keyword; reserved keyword: "+tt.wantWord,
				err.Error(),
			)
		})
	}
}

func TestCheckReservedWords_Update(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		wantWord string
		wantErr  bool
	}{
		{
			name:    "SET with non-reserved target",
			input:   "SET phase = :v",
			wantErr: false,
		},
		{
			name:     "SET target is reserved word",
			input:    "SET status = :v",
			wantErr:  true,
			wantWord: "status",
		},
		{
			name:    "SET target escaped via placeholder",
			input:   "SET #status = :v",
			wantErr: false,
		},
		{
			name:     "REMOVE target is reserved word",
			input:    "REMOVE missing",
			wantErr:  true,
			wantWord: "missing",
		},
		{
			name:     "ADD target is reserved word",
			input:    "ADD total :v",
			wantErr:  true,
			wantWord: "total",
		},
		{
			name:     "reserved word on RHS path (SET a = b)",
			input:    "SET a = data",
			wantErr:  true,
			wantWord: "data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := expr.NewParser(expr.NewLexer(tt.input))
			node, err := p.ParseUpdate()
			require.NoError(t, err)

			err = expr.CheckReservedWords(node)
			if !tt.wantErr {
				assert.NoError(t, err)

				return
			}

			require.Error(t, err)

			var rwErr *expr.ReservedWordError
			require.ErrorAs(t, err, &rwErr)
			assert.Equal(t, tt.wantWord, rwErr.Word)
		})
	}
}

func TestCheckReservedWords_Projection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		wantWord string
		wantErr  bool
	}{
		{
			name:    "non-reserved projection list",
			input:   "pk, phase",
			wantErr: false,
		},
		{
			name:     "reserved word in projection list",
			input:    "pk, status",
			wantErr:  true,
			wantWord: "status",
		},
		{
			name:    "projection list escaped via placeholder",
			input:   "pk, #status",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := expr.NewParser(expr.NewLexer(tt.input))
			proj, err := p.ParseProjection()
			require.NoError(t, err)

			err = expr.CheckReservedWords(proj)
			if !tt.wantErr {
				assert.NoError(t, err)

				return
			}

			require.Error(t, err)

			var rwErr *expr.ReservedWordError
			require.ErrorAs(t, err, &rwErr)
			assert.Equal(t, tt.wantWord, rwErr.Word)
		})
	}
}
