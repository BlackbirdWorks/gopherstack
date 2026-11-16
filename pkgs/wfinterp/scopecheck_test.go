package wfinterp_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/wfinterp"
)

func TestScopeCheckRejectsAtDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want string
	}{
		{
			name: "undefined variable",
			src: `
main:
  steps:
    - r:
        return: ${nothing}`,
			want: "variable 'nothing' is not defined",
		},
		{
			name: "undefined in member chain",
			src: `
main:
  steps:
    - r:
        return: ${ghost.field}`,
			want: "variable 'ghost' is not defined",
		},
		{
			name: "undefined in call args",
			src: `
main:
  steps:
    - c:
        call: sys.log
        args: {text: '${ghost}'}`,
			want: "variable 'ghost' is not defined",
		},
		{
			name: "loop variable leaks",
			src: `
main:
  steps:
    - l:
        for:
          value: v
          in: [1]
          steps:
            - a:
                assign: [{inner: 1}]
    - r:
        return: ${inner}`,
			want: "variable 'inner' is not defined",
		},
		{
			name: "loop value leaks",
			src: `
main:
  steps:
    - l:
        for:
          value: v
          in: [1]
          steps:
            - a:
                assign: [{x: 1}]
    - r:
        return: ${v}`,
			want: "variable 'v' is not defined",
		},
		{
			name: "except variable leaks",
			src: `
main:
  steps:
    - t:
        try:
          raise: x
        except:
          as: e
          steps:
            - a:
                assign: [{local: 1}]
    - r:
        return: ${e}`,
			want: "variable 'e' is not defined",
		},
		{
			name: "branch local read outside",
			src: `
main:
  steps:
    - p:
        parallel:
          branches:
            - one:
                steps:
                  - s:
                      assign: [{tmp: 1}]
            - two:
                steps:
                  - s:
                      assign: [{tmp: 2}]
    - c:
        assign: [{x: "${tmp}"}]`,
			want: "variable 'tmp' is not defined",
		},
		{
			name: "unshared parent write",
			src: `
main:
  steps:
    - init:
        assign: [{v: 0}]
    - p:
        parallel:
          branches:
            - one:
                steps:
                  - s:
                      assign: [{v: 1}]
            - two:
                steps:
                  - s:
                      assign: [{w: 1}]`,
			want: "variable 'v' belongs to a parent scope and is not shared",
		},
		{
			name: "subworkflow cannot see main variables",
			src: `
main:
  steps:
    - init:
        assign: [{v: 1}]
    - c:
        call: sub
sub:
  steps:
    - r:
        return: ${v}`,
			want: "variable 'v' is not defined",
		},
		{
			name: "path write to undefined variable",
			src: `
main:
  steps:
    - a:
        assign: [{ghost.key: 1}]`,
			want: "variable 'ghost' is not defined",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := wfinterp.NewEngine().Compile(tt.src)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestScopeCheckAcceptsValidScopes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
	}{
		{
			name: "loop var and outer var inside loop",
			src: `
main:
  steps:
    - i:
        assign: [{o: 0}]
    - l:
        for:
          value: v
          index: ix
          in: [1]
          steps:
            - a:
                assign: [{o: '${o + v + ix}'}]
    - r:
        return: ${o}`,
		},
		{
			name: "shared write",
			src: `
main:
  steps:
    - i:
        assign: [{v: 0}]
    - p:
        parallel:
          shared: [v]
          branches:
            - one:
                steps:
                  - s:
                      assign: [{v: 1}]
            - two:
                steps:
                  - s:
                      assign: [{w: 1}]`,
		},
		{
			name: "except variable and local",
			src: `
main:
  steps:
    - t:
        try:
          raise: x
        except:
          as: e
          steps:
            - a:
                assign: [{m: '${e}'}]
            - b:
                return: ${m}`,
		},
		{
			name: "params, subworkflow reference and library constants",
			src: `
main:
  params: [a]
  steps:
    - t:
        try:
          call: sys.log
          args: {text: '${a}'}
        retry: ${http.default_retry}
    - r:
        return: ${sys.now()}
sub:
  params: [q]
  steps:
    - r:
        return: ${q}`,
		},
		{
			name: "call result and later switch assignment",
			src: `
main:
  steps:
    - c:
        call: sub
        args: {q: 1}
        result: res
    - s:
        switch:
          - condition: ${res == 1}
            assign: [{z: 1}]
    - r:
        return: ${z}
sub:
  params: [q]
  steps:
    - r:
        return: ${q}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := wfinterp.NewEngine().Compile(tt.src)
			require.NoError(t, err)
		})
	}
}
