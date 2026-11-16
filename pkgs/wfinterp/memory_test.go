package wfinterp_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/wfinterp"
)

func TestVariableMemoryLimit(t *testing.T) {
	t.Parallel()

	const doubler = `
big:
  steps:
    - init:
        assign: [{s: "a"}]
    - grow:
        for:
          range: [1, %d]
          value: i
          steps:
            - dbl:
                assign: [{s: "${s + s}"}]
    - done:
        return: ${len(s)}
`

	const (
		callBig = "main:\n  steps:\n    - c:\n        call: big\n        result: n\n    - r:\n        return: ${n}\n"
		withArg = "main:\n  params: [a]\n  steps:\n    - c:\n        call: big\n    - r:\n        return: 1\n"
	)

	tests := []struct {
		arg     any
		name    string
		src     string
		wantErr string
	}{
		{
			name:    "one variable over the limit",
			src:     callBig + sprintf(doubler, 20),
			wantErr: "ResourceLimitError",
		},
		{
			name: "memory of ended routines is released",
			src: "main:\n  steps:\n" +
				"    - a:\n        call: big\n" +
				"    - b:\n        call: big\n" +
				"    - c:\n        call: big\n" +
				"    - d:\n        call: big\n" +
				"    - r:\n        return: 1\n" + sprintf(doubler, 18),
		},
		{
			name:    "argument counts",
			src:     withArg + sprintf(doubler, 18),
			arg:     fiveStrings(),
			wantErr: "ResourceLimitError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := run(t, tt.src, tt.arg, nil)
			if tt.wantErr == "" {
				require.NoError(t, r.err)

				return
			}

			var werr *wfinterp.Error
			require.ErrorAs(t, r.err, &werr)
			assert.Contains(t, werr.Payload(), tt.wantErr)
		})
	}
}

func fiveStrings() []any {
	out := make([]any, 5)
	for i := range out {
		out[i] = strings.Repeat("x", 60<<10)
	}

	return out
}

func TestStringLengthLimit(t *testing.T) {
	t.Parallel()

	src := "main:\n  steps:\n    - i:\n        assign: [{s: \"a\"}]\n    - g:\n        for:\n          range: [1, 19]\n" +
		"          value: n\n          steps:\n            - d:\n                assign: [{s: \"${s + s}\"}]\n" +
		"    - r:\n        return: ${len(s)}\n"

	r := run(t, src, nil, nil)

	var werr *wfinterp.Error
	require.ErrorAs(t, r.err, &werr)
	assert.Contains(t, werr.Error(), "string is longer")
}
