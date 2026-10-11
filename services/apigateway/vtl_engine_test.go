package apigateway_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

func TestRenderTemplateStrict_Directives(t *testing.T) {
	t.Parallel()

	const body = `{"name":"Ann","age":31,"tags":["a","b","c"],"nested":{"k":"v","n":2},` +
		`"items":[{"id":1,"ok":true},{"id":2,"ok":false}]}`

	tests := []struct {
		name string
		tmpl string
		ctx  apigateway.VTLContext
		want string
	}{
		{
			name: "set and property",
			tmpl: "#set($root = $input.path('$'))\n{\"hello\":\"$root.name\",\"age\":$root.age}",
			want: `{"hello":"Ann","age":31}`,
		},
		{
			name: "foreach with hasNext",
			tmpl: "[#foreach($t in $input.path('$.tags'))\"$t\"#if($foreach.hasNext),#end#end]",
			want: `["a","b","c"]`,
		},
		{
			name: "foreach objects",
			tmpl: "#foreach($i in $input.path('$.items'))$i.id=$i.ok;#end",
			want: `1=true;2=false;`,
		},
		{
			name: "if elseif else",
			tmpl: "#set($a = $input.path('$.age'))#if($a < 18)minor#elseif($a < 65)adult#{else}senior#end",
			want: "adult",
		},
		{
			name: "string equality and logic",
			tmpl: "#if($input.path('$.name') == 'Ann' && !($input.path('$.age') > 40))yes#{else}no#end",
			want: "yes",
		},
		{
			name: "quiet reference",
			tmpl: "[$!missing][$missing]",
			want: "[][$missing]",
		},
		{
			name: "java style object",
			tmpl: "$input.path('$.nested') $input.path('$.tags')",
			want: "{k=v, n=2} [a, b, c]",
		},
		{
			name: "input json preserves order",
			tmpl: "$input.json('$.nested')",
			want: `{"k":"v","n":2}`,
		},
		{
			name: "input json string value",
			tmpl: "$input.json('$.name')",
			want: `"Ann"`,
		},
		{
			name: "wildcard path",
			tmpl: "$input.json('$.items[*].id')",
			want: `[1,2]`,
		},
		{
			name: "recursive descent",
			tmpl: "$input.json('$..n')",
			want: `[2]`,
		},
		{
			name: "params lookup order",
			tmpl: "$input.params('id')|$input.params('q')|$input.params('X-Trace')|[$input.params('none')]",
			ctx: apigateway.VTLContext{
				PathParams:  map[string]string{"id": "7"},
				QueryParams: map[string]string{"q": "hi"},
				Headers:     map[string]string{"X-Trace": "t1"},
			},
			want: "7|hi|t1|[]",
		},
		{
			name: "params object",
			tmpl: "$input.params().querystring.get('q') $input.params().header.get('X-Trace')",
			ctx: apigateway.VTLContext{
				QueryParams: map[string]string{"q": "hi"},
				Headers:     map[string]string{"X-Trace": "t1"},
			},
			want: "hi t1",
		},
		{
			name: "util parse json",
			tmpl: "#set($o = $util.parseJson('{\"a\":[1,2,3]}'))$o.a.size() $o.a.get(1) $o.a[2]",
			want: "3 2 3",
		},
		{
			name: "escape javascript on reference",
			tmpl: "\"$util.escapeJavaScript($input.path('$.name'))\"",
			ctx:  apigateway.VTLContext{Body: `{"name":"a\"b"}`},
			want: `"a\"b"`,
		},
		{
			name: "string interpolation and methods",
			tmpl: "#set($n = $input.path('$.name'))#set($g = \"hi $n\")$g.toUpperCase() $n.length() $n.substring(1)",
			want: "HI ANN 3 nn",
		},
		{
			name: "list building",
			tmpl: "#set($l = [])#foreach($i in [1..3])#set($d = $l.add($i * 2))#end$l.size() $l",
			want: "3 [2, 4, 6]",
		},
		{
			name: "map iteration",
			tmpl: "#foreach($e in $input.path('$.nested').entrySet())$e.key:$e.value,#end",
			want: "k:v,n:2,",
		},
		{
			name: "comments and literal",
			tmpl: "a## line comment\nb#* block *#c#[[ $raw ]]#",
			want: "abc $raw ",
		},
		{
			name: "macro",
			tmpl: "#macro(wrap $x)<$x>#end#wrap('a')#wrap($input.path('$.name'))",
			want: "<a><Ann>",
		},
		{
			name: "break",
			tmpl: "#foreach($i in [1..9])#if($i == 3)#break#end$i#end",
			want: "12",
		},
		{
			name: "context and stage",
			tmpl: "$context.httpMethod $context.identity.sourceIp ${stageVariables.env} [$stageVariables.nope]",
			ctx: apigateway.VTLContext{
				HTTPMethod: "POST", SourceIP: "1.2.3.4", StageVariables: map[string]string{"env": "prod"},
			},
			want: "POST 1.2.3.4 prod []",
		},
		{
			name: "hash and dollar literals",
			tmpl: "#fff costs $5 and $ alone",
			want: "#fff costs $5 and $ alone",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := tt.ctx
			if ctx.Body == "" {
				ctx.Body = body
			}

			got, err := apigateway.RenderTemplateStrict(tt.tmpl, ctx)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRenderTemplateStrict_SyntaxErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tmpl string
	}{
		{name: "unclosed if", tmpl: "#if(true)x"},
		{name: "stray end", tmpl: "x#end"},
		{name: "unclosed foreach", tmpl: "#foreach($i in [1,2])x"},
		{name: "bad set", tmpl: "#set(x = 1)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := apigateway.RenderTemplateStrict(tt.tmpl, apigateway.VTLContext{})
			require.Error(t, err)
		})
	}
}
