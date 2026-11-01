package jsonx_test

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func TestV1Semantics(t *testing.T) {
	t.Parallel()

	type in struct {
		H string         `json:"h"`
		M map[string]int `json:"m"`
		S []int          `json:"s"`
	}

	b, err := json.Marshal(in{H: "<&>"}, jsonx.V1())
	require.NoError(t, err)
	assert.JSONEq(t, `{"s":null,"m":null,"h":"<&>"}`, string(b))
	assert.Contains(t, string(b), "\\u003c")

	var out in

	require.NoError(t, json.Unmarshal([]byte(`{"S":[1],"H":"x","dup":1,"dup":2}`), &out, jsonx.V1()))
	assert.Equal(t, []int{1}, out.S)
	assert.Equal(t, "x", out.H)

	err = json.Unmarshal([]byte(`{"s":`), &out, jsonx.V1())
	require.EqualError(t, err, "unexpected end of JSON input")
}

func TestUseNumber(t *testing.T) {
	t.Parallel()

	var got any

	doc := `{"a":1.50,"b":[2,"x",true,null,{"c":3}],"d":"s"}`
	err := json.Unmarshal([]byte(doc), &got, jsonx.V1(), jsonx.UseNumber())
	require.NoError(t, err)

	m, ok := got.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, jsonx.Number("1.50"), m["a"])
	assert.Equal(t, "s", m["d"])
	assert.Equal(t, []any{jsonx.Number("2"), "x", true, nil, map[string]any{"c": jsonx.Number("3")}}, m["b"])

	var typed struct{ N int }

	require.NoError(t, json.Unmarshal([]byte(`{"N":7}`), &typed, jsonx.V1(), jsonx.UseNumber()))
	assert.Equal(t, 7, typed.N)
}

func TestNumber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "number", in: `12.5`, want: "12.5"},
		{name: "quoted number", in: `"7"`, want: "7"},
		{name: "quoted junk", in: `"x"`, wantErr: true},
		{name: "bool", in: `true`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var n jsonx.Number

			err := json.Unmarshal([]byte(tt.in), &n, jsonx.V1())
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, n.String())

			out, err := json.Marshal(n, jsonx.V1())
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(out))
		})
	}

	out, err := json.Marshal(jsonx.Number(""), jsonx.V1())
	require.NoError(t, err)
	assert.Equal(t, "0", string(out))
}

func TestDecoderStreamTolerantOfTrailingData(t *testing.T) {
	t.Parallel()

	var v map[string]int

	dec := jsontext.NewDecoder(strings.NewReader(`{"a":1} trailing`))
	require.NoError(t, json.UnmarshalDecode(dec, &v, jsonx.V1()))
	assert.Equal(t, map[string]int{"a": 1}, v)
}
