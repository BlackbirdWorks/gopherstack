package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const header = "package p\n\nimport (\n\t\"encoding/json\"\n\t\"net/http\"\n\n\t\"github.com/blackbirdworks/gopherstack/pkgs/arn\"\n)\n\nvar _ = http.StatusOK\nvar _ = arn.X\n\n"

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
			body: "func f(b []byte) { _, _ = json.Marshal(b); _ = json.Unmarshal(b, &b) }\n",
			want: []string{"json.Marshal(b, jsonx.V1())", "json.Unmarshal(b, &b, jsonx.V1())", "\"encoding/json/v2\"", "pkgs/jsonx\""},
		},
		{
			name: "marshal indent",
			body: "func f(v any) { _, _ = json.MarshalIndent(v, \"\", \"  \") }\n",
			want: []string{"json.Marshal(v, jsonx.V1(), jsontext.Multiline(true), jsontext.WithIndent(\"  \"))", "encoding/json/jsontext"},
		},
		{
			name: "decode chain",
			body: "func f(r *http.Request, v any) error { return json.NewDecoder(r.Body).Decode(&v) }\n",
			want: []string{"json.UnmarshalDecode(jsontext.NewDecoder(r.Body), &v, jsonx.V1())"},
		},
		{
			name: "encode chain",
			body: "func f(w http.ResponseWriter, v any) { _ = json.NewEncoder(w).Encode(v) }\n",
			want: []string{"json.MarshalEncode(jsontext.NewEncoder(w, jsonx.V1()), v)"},
		},
		{
			name: "decoder var",
			body: "func f(r *http.Request, v any) error {\n\tdec := json.NewDecoder(r.Body)\n\tdec.UseNumber()\n\tdec.DisallowUnknownFields()\n\n\treturn dec.Decode(&v)\n}\n",
			want: []string{"dec := jsontext.NewDecoder(r.Body)", "json.UnmarshalDecode(dec, &v, jsonx.V1(), jsonx.UseNumber(), json.RejectUnknownMembers(true))"},
		},
		{
			name: "encoder var",
			body: "func f(w http.ResponseWriter, v any) error {\n\tenc := json.NewEncoder(w)\n\tenc.SetEscapeHTML(false)\n\tenc.SetIndent(\"\", \"\\t\")\n\n\treturn enc.Encode(v)\n}\n",
			want: []string{"jsontext.NewEncoder(w, jsonx.V1(), jsontext.EscapeForHTML(false), jsontext.Multiline(true), jsontext.WithIndent(\"\\t\"))", "json.MarshalEncode(enc, v)"},
		},
		{
			name: "types",
			body: "type T struct {\n\tR json.RawMessage\n\tN json.Number\n}\n\nvar _ json.Marshaler\n\nfunc f(b []byte) bool { return json.Valid(b) }\n",
			want: []string{"R jsontext.Value", "N jsonx.Number", "json.Marshaler", "jsontext.Value(b).IsValid(jsonx.V1())"},
		},
		{
			name: "bad stream stays untouched",
			body: "func f(r *http.Request, v any) {\n\tdec := json.NewDecoder(r.Body)\n\t_ = dec.Decode(&v)\n\t_ = dec.Buffered()\n}\n",
			want: []string{"_ = dec.Decode(&v)", "json.NewDecoder(r.Body)"}, wantFindings: 1,
		},
		{
			name: "conditional setindent is not hoisted",
			body: "func f(w http.ResponseWriter, v any, c bool) {\n\tenc := json.NewEncoder(w)\n\tif c {\n\t\tenc.SetIndent(\"\", \" \")\n\t}\n\t_ = enc.Encode(v)\n}\n",
			want: []string{"enc.SetIndent"}, wantFindings: 1,
		},
		{
			name:         "leftovers",
			body:         "func f(r *http.Request) { d := json.NewDecoder(r.Body); _ = d.More(); var e *json.SyntaxError; _ = e }\n",
			want:         []string{"json.NewDecoder(r.Body)"},
			wantFindings: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := rewrite("x.go", []byte(header+tt.body))
			require.NoError(t, err)
			assert.Len(t, res.findings, tt.wantFindings)
			require.NotNil(t, res.out)

			for _, w := range tt.want {
				assert.Contains(t, string(res.out), w)
			}
		})
	}
}

func TestRewriteNoV1Import(t *testing.T) {
	t.Parallel()

	res, err := rewrite("x.go", []byte("package p\n\nimport \"fmt\"\n\nvar _ = fmt.Sprint\n"))
	require.NoError(t, err)
	assert.Nil(t, res.out)
}
