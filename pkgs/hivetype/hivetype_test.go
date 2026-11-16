package hivetype_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/hivetype"
)

func TestParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		kind hivetype.Kind
		bad  bool
	}{
		{name: "scalar", in: "bigint", kind: hivetype.Bigint},
		{name: "decimal", in: "decimal(12,3)", kind: hivetype.Decimal},
		{name: "varchar", in: "varchar(10)", kind: hivetype.Varchar},
		{name: "array", in: "array<int>", kind: hivetype.Array},
		{name: "map", in: "map<string,array<double>>", kind: hivetype.Map},
		{name: "struct", in: "struct<a:int,b:struct<c:string>>", kind: hivetype.Struct},
		{name: "row", in: "row(a int, b varchar)", kind: hivetype.Struct},
		{name: "unknown", in: "geometry", bad: true},
		{name: "trailing", in: "int)", bad: true},
		{name: "open", in: "array<int", bad: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ty, err := hivetype.Parse(tc.in)
			if tc.bad {
				require.ErrorIs(t, err, hivetype.ErrSyntax)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.kind, ty.Kind)
		})
	}

	ty, err := hivetype.Parse("struct<a:int,b:map<string,decimal(5,2)>>")
	require.NoError(t, err)
	require.Len(t, ty.Fields, 2)
	assert.Equal(t, "b", ty.Fields[1].Name)
	assert.Equal(t, 2, ty.Fields[1].Type.Value.Scale)
	assert.True(t, hivetype.IsNested("array<int>"))
	assert.False(t, hivetype.IsNested("int"))
}
