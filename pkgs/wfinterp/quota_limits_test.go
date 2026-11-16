package wfinterp_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/wfinterp"
)

func TestDocumentedCompileLimits(t *testing.T) {
	t.Parallel()

	manyAssigns := "main:\n  steps:\n    - a:\n        assign:\n" +
		strings.Repeat("          - v: 1\n", 51)
	okAssigns := "main:\n  steps:\n    - a:\n        assign:\n" +
		strings.Repeat("          - v: 1\n", 50) + "    - r:\n        return: 1\n"

	deep := `
main:
  steps:
    - p1:
        parallel:
          branches:
            - a:
                steps:
                  - p2:
                      parallel:
                        branches:
                          - a:
                              steps:
                                - p3:
                                    parallel:
                                      branches:
                                        - a:
                                            steps:
                                              - x:
                                                  assign: [{q: 1}]
                                        - b:
                                            steps:
                                              - y:
                                                  assign: [{q: 1}]
                          - b:
                              steps:
                                - z:
                                    assign: [{q: 1}]
            - b:
                steps:
                  - w:
                      assign: [{q: 1}]
`

	tests := []struct {
		name string
		src  string
		want string
	}{
		{
			"source too large", "main:\n  steps:\n    - r:\n        return: 1\n#" + strings.Repeat("x", 128<<10),
			"larger than",
		},
		{"too many assignments", manyAssigns, "at most 50 assignments"},
		{
			"expression too long",
			"main:\n  steps:\n    - r:\n        return: ${" + strings.Repeat("1+", 200) + "1}\n",
			"at most 400 characters",
		},
		{"parallel nested too deep", deep, "nested at most 2 levels"},
		{"fifty assignments are fine", okAssigns, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := wfinterp.NewEngine().Compile(tt.src)
			if tt.want == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestUnhandledBranchErrorReportsAtMostOneHundred(t *testing.T) {
	t.Parallel()

	src := `
main:
  steps:
    - p:
        try:
          parallel:
            for:
              value: n
              range: [1, 150]
              steps:
                - f:
                    raise: boom
        except:
          as: e
          steps:
            - r:
                return:
                  n: ${len(e.branches)}
                  truncated: ${e.truncated}
`
	r := run(t, src, nil, nil)
	require.NoError(t, r.err)
	assert.Equal(t, map[string]any{"n": int64(100), "truncated": true}, r.value)
}
