package appsync_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluateMappingTemplate_Engine(t *testing.T) {
	t.Parallel()

	const users = `{"arguments":{"users":[{"name":"a"},{"name":"b"}],"tags":["a","b","c","d"],"n":5,"flag":false}}`

	tests := []struct {
		name string
		tmpl string
		ctx  string
		want string
	}{
		{name: "set and ref", tmpl: `#set($x = 5)$x`, want: "5"},
		{
			name: "toJson of missing arg is null",
			tmpl: `{"limit":$util.toJson($ctx.args.limit)}`, ctx: users, want: `{"limit":null}`,
		},
		{
			name: "bare missing arg stays literal",
			tmpl: `{"limit":$ctx.args.limit}`,
			ctx:  users,
			want: `{"limit":$ctx.args.limit}`,
		},
		{name: "int division", tmpl: `#set($a = 7)#set($b = 2)#set($r = $a / $b)$r`, want: "3"},
		{name: "float division", tmpl: `#set($r = 7.0 / 2)$r`, want: "3.5"},
		{name: "string concat", tmpl: `#set($s = "a" + 1 + "b")$s`, want: "a1b"},
		{name: "if else", tmpl: `#if($ctx.args.n > 3)big#{else}small#end`, ctx: users, want: "big"},
		{name: "if else false branch", tmpl: `#if($ctx.args.n > 9)big#{else}small#end`, ctx: users, want: "small"},
		{name: "elseif", tmpl: `#set($x = 2)#if($x == 1)one#elseif($x == 2)two#else three#end`, want: "two"},
		{name: "not and logic", tmpl: `#if(!$ctx.args.flag && $ctx.args.n == 5)ok#end`, ctx: users, want: "ok"},
		{name: "or logic", tmpl: `#if($ctx.args.flag || $ctx.args.n < 2)yes#{else}no#end`, ctx: users, want: "no"},
		{
			name: "foreach range hasNext",
			tmpl: `#foreach($i in [1..3])$i#if($foreach.hasNext),#end#end`,
			want: "1,2,3",
		},
		{
			name: "foreach break",
			tmpl: `#foreach($t in $ctx.args.tags)#if($t == "c")#break#end$t#end`,
			ctx:  users, want: "ab",
		},
		{
			name: "foreach count and index",
			tmpl: `#foreach($t in $ctx.args.tags)$foreach.index:$foreach.count;#end`,
			ctx:  users, want: "0:1;1:2;2:3;3:4;",
		},
		{
			name: "multiline list build",
			tmpl: "#set($names = [])\n#foreach($u in $ctx.args.users)\n  #set($ignore = $names.add($u.name))\n#end\n" +
				"$util.toJson($names)",
			ctx: users, want: `["a","b"]`,
		},
		{
			name: "map literal and put",
			tmpl: `#set($m = {"a": 1})#set($ignore = $m.put("b", 2))$util.toJson($m)`,
			want: `{"a":1,"b":2}`,
		},
		{name: "string interpolation", tmpl: `#set($n = "Bob")#set($s = "hi $n!")$s`, want: "hi Bob!"},
		{name: "single quote no interpolation", tmpl: `#set($n = "Bob")#set($s = 'hi $n')$s`, want: "hi $n"},
		{name: "stash shared", tmpl: `$util.qr($ctx.stash.put("k", "v"))$ctx.stash.k`, want: "v"},
		{name: "return value", tmpl: `#return({"a": 1})after`, want: `{"a":1}`},
		{name: "line comment", tmpl: "a## c\nb", want: "a\nb"},
		{name: "block comment", tmpl: `#* block *#x`, want: "x"},
		{name: "unparsed block", tmpl: `#[[ $x ]]#`, want: " $x "},
		{name: "undefined reference stays literal", tmpl: `$nope`, want: "$nope"},
		{name: "quiet reference", tmpl: `a$!nope.b`, want: "a"},
		{name: "plain dollar and hash", tmpl: `cost: $5 #tag`, want: "cost: $5 #tag"},
		{
			name: "string methods",
			tmpl: `#set($s = "abc")$s.length() $s.substring(1) $s.toUpperCase()`,
			want: "3 bc ABC",
		},
		{name: "nested index", tmpl: `$ctx.args.users[1].name`, ctx: users, want: "b"},
		{name: "util str and null helpers", tmpl: `$util.str.toUpper("ab")$util.isNullOrEmpty("")`, want: "ABtrue"},
		{name: "default if null", tmpl: `$util.defaultIfNull($ctx.args.zz, "d")`, want: "d"},
		{
			name: "time epoch to iso",
			tmpl: `$util.time.epochMilliSecondsToISO8601(1700000000000)`,
			want: "2023-11-14T22:13:20.000Z",
		},
		{
			name: "time formatted",
			tmpl: `$util.time.epochMilliSecondsToFormatted(1700000000000, "yyyy-MM-dd HH:mm")`,
			want: "2023-11-14 22:13",
		},
		{name: "dynamodb json", tmpl: `$util.dynamodb.toDynamoDBJson("x")`, want: `{"S":"x"}`},
		{
			name: "dynamodb map values",
			tmpl: `$util.dynamodb.toMapValuesJson({"a": 1, "b": "t"})`,
			want: `{"a":{"N":"1"},"b":{"S":"t"}}`,
		},
		{name: "dynamodb string set", tmpl: `$util.dynamodb.toStringSetJson(["a","b"])`, want: `{"SS":["a","b"]}`},
		{name: "math round", tmpl: `$util.math.roundNum(2.6)`, want: "3"},
		{name: "base64", tmpl: `$util.base64Encode("hi")`, want: "aGk="},
		{
			name: "filter expression",
			tmpl: `$util.toJson($util.transform.toDynamoDBFilterExpression({"age": {"eq": 5}}))`,
			want: `{"expression":"(#age = :age_eq)","expressionNames":{"#age":"age"},` +
				`"expressionValues":{":age_eq":{"N":"5"}}}`,
		},
		{
			name: "list sort",
			tmpl: `$util.toJson($util.list.sortList([{"n":2},{"n":1}], false, "n"))`,
			want: `[{"n":1},{"n":2}]`,
		},
		{name: "typeOf", tmpl: `$util.typeOf([1])$util.typeOf({})$util.typeOf("s")`, want: "ListMapString"},
		{name: "map keySet size", tmpl: `#set($m = {"a":1,"b":2})$m.keySet().size()`, want: "2"},
		{name: "extensions recorded", tmpl: `$util.qr($extensions.evictFromApiCache("Query","f",{}))ok`, want: "ok"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := newTestBackend().EvaluateMappingTemplate(tt.tmpl, tt.ctx)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEvaluateMappingTemplate_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tmpl    string
		wantMsg string
	}{
		{name: "util error", tmpl: `$util.error("boom", "MyType")`, wantMsg: "boom"},
		{name: "validate false", tmpl: `$util.validate(1 > 2, "bad input")`, wantMsg: "bad input"},
		{name: "unauthorized", tmpl: `$util.unauthorized()`, wantMsg: "Unauthorized"},
		{name: "unterminated if", tmpl: `#if(true)x`, wantMsg: "#if without #end"},
		{name: "stray end", tmpl: `x#end`, wantMsg: "unexpected #end"},
		{name: "unknown util", tmpl: `$util.nosuch()`, wantMsg: "unknown util function"},
		{name: "runaway loop", tmpl: `#foreach($i in [1..99999])#foreach($j in [1..99999])x#end#end`, wantMsg: "limit"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := newTestBackend().EvaluateMappingTemplate(tt.tmpl, "")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantMsg)
		})
	}
}
