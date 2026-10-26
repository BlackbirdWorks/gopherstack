package wfinterp_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/wfinterp"
)

func TestConstructs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want any
		arg  any
		name string
		src  string
	}{
		{
			name: "assign and return",
			src: `
main:
  steps:
    - a:
        assign:
          - x: 1
          - y: ${x + 41}
    - r:
        return: ${y}`,
			want: int64(42),
		},
		{
			name: "implicit main with list source",
			src: `
- r:
    return: "hi"`,
			want: "hi",
		},
		{
			name: "json source",
			src:  `{"main":{"steps":[{"r":{"return":"${1 + 2}"}}]}}`,
			want: int64(3),
		},
		{
			name: "params and argument",
			arg:  map[string]any{"name": "gopher"},
			src: `
main:
  params: [args]
  steps:
    - r:
        return: ${"hello " + args.name}`,
			want: "hello gopher",
		},
		{
			name: "nested map and list assignment",
			src: `
main:
  steps:
    - a:
        assign:
          - m: {a: {b: 1}, l: [1, 2, 3]}
          - m.a.b: 5
          - m["l"][1]: 20
          - m.c: "new"
    - r:
        return: ${m}`,
			want: map[string]any{
				"a": map[string]any{"b": int64(5)},
				"l": []any{int64(1), int64(20), int64(3)},
				"c": "new",
			},
		},
		{
			name: "switch picks first match and falls through to next",
			src: `
main:
  steps:
    - init:
        assign: [{n: 7}]
    - pick:
        switch:
          - condition: ${n < 5}
            return: "small"
          - condition: ${n < 10}
            assign: [{size: "medium"}]
          - condition: true
            return: "large"
        next: done
    - skipped:
        return: "skipped"
    - done:
        return: ${size}`,
			want: "medium",
		},
		{
			name: "switch without match continues",
			src: `
main:
  steps:
    - s:
        switch:
          - condition: ${1 > 2}
            return: "no"
    - r:
        return: "after"`,
			want: "after",
		},
		{
			name: "for over a list with index and break",
			src: `
main:
  steps:
    - init:
        assign: [{sum: 0}, {idxs: []}]
    - loop:
        for:
          value: v
          index: i
          in: [10, 20, 30, 40]
          steps:
            - stop:
                switch:
                  - condition: ${v == 40}
                    next: break
            - add:
                assign:
                  - sum: ${sum + v}
                  - idxs: ${list.concat(idxs, i)}
    - r:
        return: {sum: "${sum}", idxs: "${idxs}"}`,
			want: map[string]any{"sum": int64(60), "idxs": []any{int64(0), int64(1), int64(2)}},
		},
		{
			name: "for range with continue",
			src: `
main:
  steps:
    - init:
        assign: [{sum: 0}]
    - loop:
        for:
          value: v
          range: [1, 6]
          steps:
            - odd:
                switch:
                  - condition: ${v % 2 == 1}
                    next: continue
            - add:
                assign: [{sum: "${sum + v}"}]
    - r:
        return: ${sum}`,
			want: int64(12),
		},
		{
			name: "loop variables do not leak and outer variables update",
			src: `
main:
  steps:
    - init:
        assign: [{outer: 0}]
    - loop:
        for:
          value: v
          in: [1, 2]
          steps:
            - a:
                assign: [{outer: "${outer + v}"}, {inner: 1}]
    - check:
        try:
          steps:
            - read:
                return: ${inner}
        except:
          as: e
          steps:
            - out:
                return: {outer: "${outer}", tag: "${e.tags[0]}"}`,
			want: map[string]any{"outer": int64(3), "tag": "KeyError"},
		},
		{
			name: "subworkflow with params defaults and result",
			src: `
main:
  steps:
    - c:
        call: add
        args: {a: 2}
        result: r
    - done:
        return: ${r}
add:
  params: [a, b: 10]
  steps:
    - s:
        return: ${a + b}`,
			want: int64(12),
		},
		{
			name: "recursion through subworkflows",
			src: `
main:
  steps:
    - c:
        call: fact
        args: {n: 5}
        result: r
    - done:
        return: ${r}
fact:
  params: [n]
  steps:
    - base:
        switch:
          - condition: ${n <= 1}
            return: 1
    - rec:
        call: fact
        args: {n: "${n - 1}"}
        result: sub
    - mul:
        return: ${n * sub}`,
			want: int64(120),
		},
		{
			name: "jump with next and end",
			src: `
main:
  steps:
    - first:
        assign: [{log: "a"}]
        next: third
    - second:
        assign: [{log: "b"}]
    - third:
        assign: [{log: "${log + 'c'}"}]
        next: end
    - never:
        return: "unreachable"`,
			want: nil,
		},
		{
			name: "try except catches raise",
			src: `
main:
  steps:
    - t:
        try:
          steps:
            - boom:
                raise: {code: 7, message: "bad"}
        except:
          as: e
          steps:
            - r:
                return: ${e.code}`,
			want: int64(7),
		},
		{
			name: "raise a string",
			src: `
main:
  steps:
    - t:
        try:
          raise: "oops"
        except:
          as: e
          steps:
            - r:
                return: ${e}`,
			want: "oops",
		},
		{
			name: "runtime errors carry tags",
			src: `
main:
  steps:
    - t:
        try:
          assign: [{x: "${1 / 0}"}]
        except:
          as: e
          steps:
            - r:
                return: ${e.tags}`,
			want: []any{"ZeroDivisionError", "ArithmeticError"},
		},
		{
			name: "nested steps block",
			src: `
main:
  steps:
    - outer:
        steps:
          - a:
              assign: [{x: 1}]
          - b:
              assign: [{x: "${x + 1}"}]
    - r:
        return: ${x}`,
			want: int64(2),
		},
		{
			name: "next jumps out of a nested steps block",
			src: `
main:
  steps:
    - outer:
        steps:
          - inner:
              assign: [{x: 1}]
              next: target
          - skipped:
              assign: [{x: 99}]
    - skipped_too:
        assign: [{x: 100}]
    - target:
        return: ${x}`,
			want: int64(1),
		},
		{
			name: "return from inside a for loop",
			src: `
main:
  steps:
    - loop:
        for:
          value: v
          in: [1, 2, 3]
          steps:
            - hit:
                switch:
                  - condition: ${v == 2}
                    return: ${v * 10}
    - after:
        return: never`,
			want: int64(20),
		},
		{
			name: "next leaves a for loop for an outer step",
			src: `
main:
  steps:
    - loop:
        for:
          value: v
          in: [1, 2, 3]
          steps:
            - out:
                switch:
                  - condition: ${v == 2}
                    next: done
    - skipped:
        return: skipped
    - done:
        return: left`,
			want: "left",
		},
		{
			name: "call result into a map path",
			src: `
main:
  steps:
    - init:
        assign: [{m: {}}]
    - c:
        call: math.max
        args: {x: 3, y: 8}
        result: m.biggest
    - r:
        return: ${m}`,
			want: map[string]any{"biggest": int64(8)},
		},
		{
			name: "yaml integer map keys iterate as strings",
			src: `
main:
  steps:
    - init:
        assign:
          - m:
              1: 10
              2: 20
          - sum: 0
    - loop:
        for:
          value: k
          in: ${keys(m)}
          steps:
            - add:
                assign: [{sum: "${sum + m[k]}"}]
    - r:
        return: ${sum}`,
			want: int64(30),
		},
		{
			name: "switch next when no condition matches",
			src: `
main:
  steps:
    - s:
        switch:
          - condition: ${false}
            return: no
        next: end
    - never:
        return: unreachable`,
			want: nil,
		},
		{
			name: "map and list literals in expressions",
			src: `
main:
  steps:
    - r:
        return: '${{"a": [1, 2 + 3], "b": {"c": "d"}}}'`,
			want: map[string]any{"a": []any{int64(1), int64(5)}, "b": map[string]any{"c": "d"}},
		},
		{
			name: "get_env builtin",
			src: `
main:
  steps:
    - r:
        return: ${sys.get_env("GOOGLE_CLOUD_PROJECT_ID")}`,
			want: "p1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := run(t, tt.src, tt.arg, nil)
			require.NoError(t, r.err)
			assert.Equal(t, tt.want, r.value)
		})
	}
}

func TestRetryAndBackoff(t *testing.T) {
	t.Parallel()

	const failing = `
main:
  steps:
    - init:
        assign: [{n: 0}]
    - t:
        try:
          steps:
            - bump:
                assign: [{n: "${n + 1}"}]
            - maybe:
                switch:
                  - condition: ${n < %s}
                    raise: {code: 503, tags: ["HttpError"], message: "unavailable"}
        retry:
          predicate: ${http.default_retry_predicate}
          max_retries: 5
          backoff: {initial_delay: 1, max_delay: 60, multiplier: 2}
    - r:
        return: ${n}`

	tests := []struct {
		name       string
		failUntil  string
		wantResult any
		wantSleeps []time.Duration
		wantErr    bool
	}{
		{
			name: "succeeds on the third attempt", failUntil: "3", wantResult: int64(3),
			wantSleeps: []time.Duration{time.Second, 2 * time.Second},
		},
		{
			name: "gives up after max retries", failUntil: "100", wantErr: true,
			wantSleeps: []time.Duration{
				time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := sprintf(failing, tt.failUntil)
			r := run(t, src, nil, nil)

			if tt.wantErr {
				var werr *wfinterp.Error
				require.ErrorAs(t, r.err, &werr)
				assert.Contains(t, werr.Payload(), "unavailable")
			} else {
				require.NoError(t, r.err)
				assert.Equal(t, tt.wantResult, r.value)
			}

			assert.Equal(t, tt.wantSleeps, r.clock.slept())
		})
	}
}

func TestCustomRetryPredicateAndDefaultPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		src        string
		want       any
		wantSleeps []time.Duration
	}{
		{
			name: "custom predicate subworkflow",
			src: `
main:
  steps:
    - init:
        assign: [{n: 0}]
    - t:
        try:
          steps:
            - bump:
                assign: [{n: "${n + 1}"}]
            - maybe:
                switch:
                  - condition: ${n < 3}
                    raise: {code: 202}
        retry:
          predicate: ${again}
          max_retries: 4
          backoff: {initial_delay: 2, max_delay: 3, multiplier: 2}
    - r:
        return: ${n}
again:
  params: [e]
  steps:
    - s:
        return: ${e.code == 202}`,
			want:       int64(3),
			wantSleeps: []time.Duration{2 * time.Second, 3 * time.Second},
		},
		{
			name: "retry.never does not retry",
			src: `
main:
  steps:
    - t:
        try:
          raise: "x"
        retry:
          predicate: ${retry.never}
          max_retries: 3
        except:
          as: e
          steps:
            - r:
                return: "caught"`,
			want: "caught",
		},
		{
			name: "default policy retries a non-http error never",
			src: `
main:
  steps:
    - t:
        try:
          raise: "plain"
        retry: ${http.default_retry}
        except:
          as: e
          steps:
            - r:
                return: ${e}`,
			want: "plain",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := run(t, tt.src, nil, nil)
			require.NoError(t, r.err)
			assert.Equal(t, tt.want, r.value)
			assert.Equal(t, tt.wantSleeps, r.clock.slept())
		})
	}
}
