package wfinterp_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/wfinterp"
)

func TestScopeCheckAcceptsRealWorldSources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
	}{
		{"backward next loop", `
main:
  steps:
    - init:
        assign: [{i: 0}]
    - check:
        switch:
          - condition: ${i < 3}
            next: body
        next: done
    - body:
        assign: [{i: '${i + 1}'}]
        next: check
    - done:
        return: ${i}`},
		{"path write before textual init reached by jump", `
main:
  steps:
    - go:
        next: init
    - use:
        assign: [{m.k: 1}]
        next: end
    - init:
        assign: [{m: {}}]
        next: use`},
		{"list index and map key writes", `
main:
  steps:
    - a:
        assign:
          - l: [1, 2]
          - m: {a: 1}
          - l[0]: 5
          - m["k"]: 2
          - m.z: 3
          - m.list: [1]
          - m.list[0]: 9
    - r:
        return: ${l}`},
		{"for range and value index", `
main:
  params: [args]
  steps:
    - a:
        assign: [{n: 3}, {sum: 0}]
    - f:
        for:
          value: v
          range: [1, '${n}']
          steps:
            - s:
                assign: [{sum: '${sum + v}'}]
    - g:
        for:
          value: v
          index: ix
          in: ${args.items}
          steps:
            - s:
                assign: [{sum: '${sum + v + ix}'}]
    - r:
        return: ${sum}`},
		{"try except retry custom predicate and backoff", `
main:
  steps:
    - t:
        try:
          call: http.get
          args:
            url: https://example.com
          result: res
        retry:
          predicate: ${my_pred}
          max_retries: 3
          backoff:
            initial_delay: 1
            max_delay: 60
            multiplier: 2
        except:
          as: e
          steps:
            - chk:
                switch:
                  - condition: ${e.code == 404}
                    return: "nf"
            - rr:
                raise: ${e}
    - r:
        return: ${res.body}
my_pred:
  params: [e]
  steps:
    - r:
        return: ${e.code == 429 or e.code == 503}`},
		{"except-assigned var predeclared", `
main:
  steps:
    - pre:
        assign: [{x: 0}]
    - t:
        try:
          raise: boom
        except:
          as: e
          steps:
            - s:
                assign: [{x: '${e.message}'}]
    - r:
        return: ${x}`},
		{"parallel shared and branches", `
main:
  steps:
    - init:
        assign: [{total: 0}, {names: []}]
    - p:
        parallel:
          shared: [total, names]
          concurrency_limit: 2
          exception_policy: continueAll
          branches:
            - one:
                steps:
                  - s:
                      assign: [{total: '${total + 1}'}]
            - two:
                steps:
                  - s:
                      assign: [{local: 1}, {total: '${total + local}'}]
    - r:
        return: ${total}`},
		{"parallel for shared", `
main:
  steps:
    - init:
        assign: [{acc: {}}]
    - p:
        parallel:
          shared: [acc]
          for:
            value: u
            in: ['a', 'b']
            steps:
              - s:
                  assign:
                    - acc[u]: 1
    - r:
        return: ${acc}`},
		{"parallel branch writes to own key path of shared", `
main:
  steps:
    - init:
        assign: [{res: {}}]
    - p:
        parallel:
          shared: [res]
          branches:
            - b1:
                steps:
                  - s:
                      assign: [{res.a: 1}]
            - b2:
                steps:
                  - s:
                      assign: [{res.b: 1}]`},
		{"parallel branch reads parent var", `
main:
  steps:
    - init:
        assign: [{v: 1}]
    - p:
        parallel:
          branches:
            - b1:
                steps:
                  - s:
                      assign: [{loc: '${v}'}]
            - b2:
                steps:
                  - s:
                      assign: [{loc: 2}]`},
		{"subworkflow params with defaults", `
main:
  steps:
    - c:
        call: sub
        args:
          a: 1
        result: out
    - r:
        return: ${out}
sub:
  params: [a, b: 5, c: "x"]
  steps:
    - r:
        return: ${a + b}`},
		{"switch on every path", `
main:
  params: [input]
  steps:
    - s:
        switch:
          - condition: ${"a" in input}
            assign: [{z: 1}]
          - condition: true
            assign: [{z: 2}]
    - r:
        return: ${z}`},
		{"switch only some paths", `
main:
  params: [input]
  steps:
    - s:
        switch:
          - condition: ${"a" in input}
            assign: [{z: 1}]
    - r:
        return: ${default(map.get(input, "q"), 0)}`},
		{"switch nested steps", `
main:
  params: [input]
  steps:
    - s:
        switch:
          - condition: ${input.x == 1}
            steps:
              - a:
                  assign: [{z: 1}]
              - b:
                  return: ${z}
          - condition: true
            next: end`},
		{"env and expressions in nested maps", `
main:
  steps:
    - a:
        assign:
          - project: ${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}
          - loc: ${sys.get_env("GOOGLE_CLOUD_LOCATION")}
          - id: ${sys.get_env("GOOGLE_CLOUD_WORKFLOW_ID")}
          - ex: ${sys.get_env("GOOGLE_CLOUD_WORKFLOW_EXECUTION_ID")}
          - cfg:
              url: ${"https://" + project + ".example"}
              list:
                - ${loc}
                - k: ${id}
              "${not_an_expr}": 1
              "$${literal}": 2
    - r:
        return: ${cfg}`},
		{"string keys that look like expressions", `
main:
  steps:
    - a:
        assign:
          - m: {"${a}": 1, "$": 2}
    - r:
        return: ${m}`},
		{"callbacks", `
main:
  steps:
    - create:
        call: events.create_callback_endpoint
        args:
          http_callback_method: POST
        result: cb
    - log:
        call: sys.log
        args:
          text: ${cb.url}
    - wait:
        call: events.await_callback
        args:
          callback: ${cb}
          timeout: 60
        result: got
    - r:
        return: ${got.http_request.body}`},
		{"raise map and return in branch", `
main:
  params: [x]
  steps:
    - c:
        switch:
          - condition: ${x == 1}
            raise:
              code: 3
              message: bad
          - condition: ${x == 2}
            return: 2
    - r:
        return: 0`},
		{"http post with auth and body", `
main:
  steps:
    - p:
        call: http.post
        args:
          url: https://pubsub.googleapis.com/v1/projects/p/topics/t:publish
          auth:
            type: OAuth2
          body:
            messages:
              - data: ${base64.encode(text.encode("hi"))}
        result: r
    - out:
        return: ${r.body}`},
		{"call function with function-valued fields", `
main:
  steps:
    - a:
        call: experimental.executions.map
        args:
          workflow_id: w
          arguments: [{a: 1}]
        result: rs
    - r:
        return: ${rs}`},
		{"for loop var shadows then outer later", `
main:
  steps:
    - f:
        for:
          value: v
          in: [1]
          steps:
            - s:
                assign: [{t: '${v}'}]
    - g:
        for:
          value: v
          in: [2]
          steps:
            - s:
                assign: [{t: '${v}'}]`},
		{"loop var read via break/continue", `
main:
  steps:
    - f:
        for:
          value: v
          in: [1, 2]
          steps:
            - c:
                switch:
                  - condition: ${v == 1}
                    next: continue
                  - condition: ${v == 2}
                    next: break
    - r:
        return: 1`},
		{"try with steps and retry list default", `
main:
  steps:
    - t:
        try:
          steps:
            - a:
                assign: [{q: 1}]
            - b:
                call: sys.sleep
                args: {seconds: 1}
        retry: ${retry.default_backoff}
    - r:
        return: ${q}`},
		{"try retry policy with default backoff", `
main:
  steps:
    - t:
        try:
          call: http.get
          args: {url: "https://x.example"}
        retry:
          predicate: ${http.default_retry_predicate}
          max_retries: 5
          backoff: ${retry.default_backoff}`},
		{"read of name defined in later step textually", `
main:
  steps:
    - a:
        next: c
    - b:
        return: ${late}
    - c:
        assign: [{late: 1}]
        next: b`},
		{"param shadowed in subworkflow call result", `
main:
  params: [args]
  steps:
    - a:
        assign: [{items: '${args.items}'}]
    - c:
        call: sub
        args: {list: '${items}'}
        result: res
    - r:
        return: ${res}
sub:
  params: [list]
  steps:
    - r:
        return: ${len(list)}`},
		{"expression uses keys len type and in", `
main:
  params: [m]
  steps:
    - a:
        assign:
          - ks: ${keys(m)}
          - n: ${len(ks)}
          - t: ${get_type(m)}
          - s: ${string(n) + "x"}
          - ok: ${"k" in m}
          - mm: ${map.delete(m, "k")}
          - j: ${json.encode_to_string(m)}
    - r:
        return: ${j}`},
		{"call by expression-less named function with result in loop", `
main:
  steps:
    - f:
        for:
          value: v
          in: [1]
          steps:
            - c:
                call: sys.log
                args: {data: '${v}'}
                result: lr
            - u:
                assign: [{k: '${lr}'}]`},
		{"literals null true false", `
main:
  steps:
    - a:
        assign: [{x: '${null}'}, {y: '${true and not false}'}, {z: '${x == null}'}]
    - r:
        return: ${y}`},
		{"except without as and nested for", `
main:
  steps:
    - t:
        try:
          raise: x
        except:
          steps:
            - f:
                for:
                  value: v
                  in: [1]
                  steps:
                    - s:
                        assign: [{q: '${v}'}]
    - r:
        return: 1`},
		{"assign to name with dot path from call result", `
main:
  steps:
    - c:
        call: http.get
        args: {url: "https://x.example"}
        result: res
    - a:
        assign: [{res.body.extra: 1}]`},
	}

	for _, tt := range tests {
		t.Run(strings.ReplaceAll(tt.name, " ", "_"), func(t *testing.T) {
			t.Parallel()

			_, err := wfinterp.NewEngine().Compile(tt.src)
			require.NoError(t, err)
		})
	}
}

func TestScopeCheckMalformedInput(t *testing.T) {
	t.Parallel()

	deep := strings.Repeat("[", 5000) + strings.Repeat("]", 5000)
	parens := strings.Repeat("(", 5000) + "1" + strings.Repeat(")", 5000)
	aliasBomb := "a: &a [1,1,1,1,1,1,1,1,1]\n" +
		"b: &b [*a,*a,*a,*a,*a,*a,*a,*a,*a]\n" +
		"c: &c [*b,*b,*b,*b,*b,*b,*b,*b,*b]\n" +
		"d: [*c,*c,*c,*c,*c,*c,*c,*c,*c]"
	tests := []struct {
		name string
		src  string
	}{
		{"empty", ""},
		{"scalar", "x"},
		{"null steps", "main:\n  steps:\n"},
		{"deep flow seq", "main:\n  steps:\n    - a:\n        return: " + deep},
		{"deep expr parens", "main:\n  steps:\n    - a:\n        return: ${" + parens + "}"},
		{"alias bomb", aliasBomb},
		{"self ref name", "main:\n  steps:\n    - a:\n        assign: [{x: '${x}'}]"},
		{"bad lvalue", "main:\n  steps:\n    - a:\n        assign: [{'1[': 1}]"},
		{"unterminated expr", "main:\n  steps:\n    - a:\n        return: ${a."},
		{"empty expr", "main:\n  steps:\n    - a:\n        return: ${}"},
		{"steps not list", "main:\n  steps: {a: 1}"},
		{"huge name", "main:\n  steps:\n    - a:\n        return: ${" + strings.Repeat("a.", 20000) + "b}"},
		{"binary", "\x00\x01\xff"},
	}

	for _, tt := range tests {
		t.Run(strings.ReplaceAll(tt.name, " ", "_"), func(t *testing.T) {
			t.Parallel()

			require.NotPanics(t, func() { _, _ = wfinterp.NewEngine().Compile(tt.src) })
		})
	}
}
