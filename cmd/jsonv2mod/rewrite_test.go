package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const header = `package p

import (
	"encoding/json"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

var _ = http.StatusOK
var _ = arn.X

`

func TestRewrite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		want         []string
		wantFindings int
	}{
		{
			name: "marshal unmarshal",
			body: `func f(b []byte) {
	_, _ = json.Marshal(b)
	_ = json.Unmarshal(b, &b)
}`,
			want: []string{
				"json.Marshal(b, jsonx.V1())", "json.Unmarshal(b, &b, jsonx.V1())",
				`"encoding/json/v2"`, `pkgs/jsonx"`,
			},
		},
		{
			name: "multiline trailing comma",
			body: `func f(b []byte) {
	_, _ = json.Marshal(
		b,
	)
}`,
			want: []string{"b, jsonx.V1(),"},
		},
		{
			name: "marshal indent",
			body: `func f(v any) { _, _ = json.MarshalIndent(v, "", "  ") }`,
			want: []string{
				`json.Marshal(v, jsonx.V1(), jsontext.Multiline(true), jsontext.WithIndent("  "))`,
				"encoding/json/jsontext",
			},
		},
		{
			name: "decode chain",
			body: `func f(r *http.Request, v any) error { return json.NewDecoder(r.Body).Decode(&v) }`,
			want: []string{"json.UnmarshalDecode(jsontext.NewDecoder(r.Body), &v, jsonx.V1())"},
		},
		{
			name: "encode chain",
			body: `func f(w http.ResponseWriter, v any) { _ = json.NewEncoder(w).Encode(v) }`,
			want: []string{"json.MarshalEncode(jsontext.NewEncoder(w, jsonx.V1()), v)"},
		},
		{
			name: "decoder var",
			body: `func f(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()
	dec.DisallowUnknownFields()

	return dec.Decode(&v)
}`,
			want: []string{
				"dec := jsontext.NewDecoder(r.Body)",
				"json.UnmarshalDecode(dec, &v, jsonx.V1(), jsonx.UseNumber(), json.RejectUnknownMembers(true))",
			},
		},
		{
			name: "encoder var",
			body: `func f(w http.ResponseWriter, v any) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "\t")

	return enc.Encode(v)
}`,
			want: []string{
				`jsontext.NewEncoder(w, jsonx.V1(), jsontext.EscapeForHTML(false), jsontext.Multiline(true), ` +
					`jsontext.WithIndent("\t"))`,
				"json.MarshalEncode(enc, v)",
			},
		},
		{
			name: "types",
			body: `type T struct {
	R json.RawMessage
	N json.Number
}

var _ json.Marshaler

func f(b []byte) bool { return json.Valid(b) }`,
			want: []string{
				"R jsontext.Value",
				"N jsonx.Number",
				"json.Marshaler",
				"jsontext.Value(b).IsValid(jsonx.V1())",
			},
		},
		{
			name: "bad stream stays untouched",
			body: `func f(r *http.Request, v any) {
	dec := json.NewDecoder(r.Body)
	_ = dec.Decode(&v)
	_ = dec.Buffered()
}`,
			want:         []string{"_ = dec.Decode(&v)", "json.NewDecoder(r.Body)"},
			wantFindings: 1,
		},
		{
			name: "conditional setindent is not hoisted",
			body: `func f(w http.ResponseWriter, v any, c bool) {
	enc := json.NewEncoder(w)
	if c {
		enc.SetIndent("", " ")
	}
	_ = enc.Encode(v)
}`,
			want:         []string{"enc.SetIndent"},
			wantFindings: 1,
		},
		{
			name: "leftovers",
			body: `func f(r *http.Request) {
	d := json.NewDecoder(r.Body)
	_ = d.More()
	var e *json.SyntaxError
	_ = e
}`,
			want:         []string{"json.NewDecoder(r.Body)"},
			wantFindings: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := rewrite("x.go", []byte(header+tt.body+"\n"))
			require.NoError(t, err)
			assert.Len(t, res.findings, tt.wantFindings)
			require.NotNil(t, res.out)

			for _, w := range tt.want {
				assert.Contains(t, string(res.out), w)
			}
		})
	}
}

func TestRewriteSingleLineImport(t *testing.T) {
	t.Parallel()

	src := "package p\n\nimport \"encoding/json\"\n\nfunc f(b []byte) bool { return json.Valid(b) }\n"
	res, err := rewrite("x.go", []byte(src))
	require.NoError(t, err)
	require.NotNil(t, res.out)
	assert.Contains(t, string(res.out), "jsontext.Value(b).IsValid(jsonx.V1())")
	assert.Contains(t, string(res.out), "encoding/json/jsontext")
}

func TestRewriteNoV1Import(t *testing.T) {
	t.Parallel()

	res, err := rewrite("x.go", []byte("package p\n\nimport \"fmt\"\n\nvar _ = fmt.Sprint\n"))
	require.NoError(t, err)
	assert.Nil(t, res.out)
}
